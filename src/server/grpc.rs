use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Json;
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_stream::{self as stream, Stream};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::proto::kv_store_server::{KvStore, KvStoreServer};
use crate::proto::{
    CompactRequest, CompactResponse, DeleteRequest, DeleteResponse, FlushRequest, FlushResponse,
    GetRequest, GetResponse, PutRequest, PutResponse, ScanRequest, ScanResponse, StatsRequest,
    StatsResponse, WatchRequest, WatchResponse,
};
use crate::storage::{StorageConfig, StorageEngine, StorageError};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub dashboard_addr: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:50051".to_string(),
            dashboard_addr: Some("127.0.0.1:8080".to_string()),
        }
    }
}

#[derive(Debug)]
pub struct GrpcKvStore {
    engine: Arc<Mutex<StorageEngine>>,
}

impl GrpcKvStore {
    pub fn new(engine: StorageEngine) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
        }
    }
}

pub async fn serve(
    server_config: ServerConfig,
    storage_config: StorageConfig,
) -> crate::storage::Result<()> {
    let grpc_addr = server_config
        .listen_addr
        .parse::<SocketAddr>()
        .map_err(|error| {
            StorageError::InvalidInput(format!(
                "invalid listen address {}: {error}",
                server_config.listen_addr
            ))
        })?;
    let engine = StorageEngine::open(storage_config)?;
    let shared_engine = Arc::new(Mutex::new(engine));
    let grpc_service = GrpcKvStore {
        engine: Arc::clone(&shared_engine),
    };

    let grpc_server = async move {
        Server::builder()
            .add_service(KvStoreServer::new(grpc_service))
            .serve(grpc_addr)
            .await
            .map_err(|error| StorageError::InvalidInput(format!("gRPC server failed: {error:?}")))
    };

    if let Some(dashboard_addr) = server_config.dashboard_addr {
        let dashboard_addr = dashboard_addr.parse::<SocketAddr>().map_err(|error| {
            StorageError::InvalidInput(format!(
                "invalid dashboard address {}: {error}",
                dashboard_addr
            ))
        })?;
        let dashboard = serve_dashboard(dashboard_addr, shared_engine);

        tokio::try_join!(grpc_server, dashboard)?;
    } else {
        grpc_server.await?;
    }

    Ok(())
}

#[tonic::async_trait]
impl KvStore for GrpcKvStore {
    type ScanStream = ResponseStream<ScanResponse>;
    type WatchStream = ResponseStream<WatchResponse>;

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;
        let engine = self.engine.lock().await;
        let value = engine
            .get(key.as_bytes())
            .map_err(storage_error_to_status)?;

        Ok(Response::new(GetResponse {
            value: value
                .as_ref()
                .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                .unwrap_or_default(),
            found: value.is_some(),
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request = request.into_inner();
        let mut engine = self.engine.lock().await;
        engine
            .put(request.key.as_bytes(), request.value.as_bytes())
            .map_err(storage_error_to_status)?;

        Ok(Response::new(PutResponse { success: true }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        let mut engine = self.engine.lock().await;
        engine
            .delete(request.key.as_bytes())
            .map_err(storage_error_to_status)?;

        Ok(Response::new(DeleteResponse { success: true }))
    }

    #[allow(clippy::result_large_err)]
    async fn scan(
        &self,
        request: Request<ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        let request = request.into_inner();
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit as usize
        };
        let engine = self.engine.lock().await;
        let rows = engine
            .scan_prefix(request.prefix.as_bytes(), limit)
            .map_err(storage_error_to_status)?;
        let responses = rows.into_iter().map(|(key, value)| {
            Ok(ScanResponse {
                key: String::from_utf8_lossy(&key).to_string(),
                value: String::from_utf8_lossy(&value).to_string(),
            })
        });

        Ok(Response::new(Box::pin(stream::iter(responses))))
    }

    async fn flush(
        &self,
        _request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let mut engine = self.engine.lock().await;
        let metadata = engine.flush_memtable().map_err(storage_error_to_status)?;

        Ok(Response::new(match metadata {
            Some(metadata) => FlushResponse {
                flushed: true,
                sstable_id: metadata.id.0,
                entries: metadata.entry_count,
            },
            None => FlushResponse {
                flushed: false,
                sstable_id: 0,
                entries: 0,
            },
        }))
    }

    async fn compact(
        &self,
        _request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        let mut engine = self.engine.lock().await;
        let report = engine.compact_all().map_err(storage_error_to_status)?;

        Ok(Response::new(CompactResponse {
            flushed_memtable: report.flushed_memtable,
            input_sstables: report.input_sstables as u64,
            output_sstables: report.output_sstables as u64,
            input_entries: report.input_entries,
            output_entries: report.output_entries,
            dropped_entries: report.dropped_entries,
        }))
    }

    async fn stats(
        &self,
        _request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        let engine = self.engine.lock().await;
        let stats = engine.stats();

        Ok(Response::new(StatsResponse {
            memtable_entries: stats.memtable_entries as u64,
            memtable_bytes: stats.memtable_bytes as u64,
            flush_threshold_bytes: stats.flush_threshold_bytes as u64,
            should_flush: stats.should_flush,
            sstable_count: stats.sstable_count as u64,
            bloom_filter_count: stats.bloom_filter_count as u64,
            next_sstable_id: stats.next_sstable_id,
        }))
    }

    async fn watch(
        &self,
        _request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("watch is not implemented yet"))
    }
}

fn storage_error_to_status(error: StorageError) -> Status {
    match error {
        StorageError::InvalidInput(message) => Status::invalid_argument(message),
        StorageError::CorruptWal(message) | StorageError::CorruptSstable(message) => {
            Status::data_loss(message)
        }
        StorageError::Io(error) => Status::internal(error.to_string()),
    }
}

async fn serve_dashboard(
    addr: SocketAddr,
    engine: Arc<Mutex<StorageEngine>>,
) -> crate::storage::Result<()> {
    let app = Router::new()
        .route("/", get(dashboard_home))
        .route("/api/stats", get(dashboard_stats))
        .route("/api/get", get(dashboard_get))
        .route("/api/scan", get(dashboard_scan))
        .route("/api/put", post(dashboard_put))
        .route("/api/delete", post(dashboard_delete))
        .route("/api/flush", post(dashboard_flush))
        .route("/api/compact", post(dashboard_compact))
        .route("/api/demo", post(dashboard_demo))
        .with_state(engine);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(|error| StorageError::InvalidInput(format!("dashboard server failed: {error}")))?;

    Ok(())
}

#[derive(Debug, Deserialize)]
struct KeyQuery {
    key: String,
}

#[derive(Debug, Deserialize)]
struct ScanQuery {
    prefix: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct PutQuery {
    key: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct ActionResponse {
    success: bool,
    message: String,
}

#[derive(Debug, Serialize)]
struct StatsResponseBody {
    memtable_entries: usize,
    memtable_bytes: usize,
    flush_threshold_bytes: usize,
    should_flush: bool,
    sstable_count: usize,
    bloom_filter_count: usize,
    next_sstable_id: u64,
}

#[derive(Debug, Serialize)]
struct GetResponseBody {
    found: bool,
    key: String,
    value: Option<String>,
}

#[derive(Debug, Serialize)]
struct RowResponseBody {
    key: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct ScanResponseBody {
    rows: Vec<RowResponseBody>,
}

async fn dashboard_home() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}

async fn dashboard_stats(State(engine): State<Arc<Mutex<StorageEngine>>>) -> impl IntoResponse {
    let engine = engine.lock().await;
    let stats = engine.stats();

    Json(StatsResponseBody {
        memtable_entries: stats.memtable_entries,
        memtable_bytes: stats.memtable_bytes,
        flush_threshold_bytes: stats.flush_threshold_bytes,
        should_flush: stats.should_flush,
        sstable_count: stats.sstable_count,
        bloom_filter_count: stats.bloom_filter_count,
        next_sstable_id: stats.next_sstable_id,
    })
}

async fn dashboard_get(
    State(engine): State<Arc<Mutex<StorageEngine>>>,
    Query(query): Query<KeyQuery>,
) -> impl IntoResponse {
    let engine = engine.lock().await;
    let value = engine.get(query.key.as_bytes());

    Json(match value {
        Ok(Some(value)) => GetResponseBody {
            found: true,
            key: query.key,
            value: Some(String::from_utf8_lossy(&value).to_string()),
        },
        Ok(None) => GetResponseBody {
            found: false,
            key: query.key,
            value: None,
        },
        Err(error) => GetResponseBody {
            found: false,
            key: query.key,
            value: Some(format!("error: {error}")),
        },
    })
}

async fn dashboard_scan(
    State(engine): State<Arc<Mutex<StorageEngine>>>,
    Query(query): Query<ScanQuery>,
) -> impl IntoResponse {
    let prefix = query.prefix.unwrap_or_default();
    let limit = query.limit.unwrap_or(100).min(500);
    let engine = engine.lock().await;
    let rows = engine.scan_prefix(prefix.as_bytes(), limit);

    Json(ScanResponseBody {
        rows: rows
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| RowResponseBody {
                key: String::from_utf8_lossy(&key).to_string(),
                value: String::from_utf8_lossy(&value).to_string(),
            })
            .collect(),
    })
}

async fn dashboard_put(
    State(engine): State<Arc<Mutex<StorageEngine>>>,
    Query(query): Query<PutQuery>,
) -> impl IntoResponse {
    let mut engine = engine.lock().await;
    Json(
        match engine.put(query.key.as_bytes(), query.value.as_bytes()) {
            Ok(()) => ActionResponse {
                success: true,
                message: format!("stored {}", query.key),
            },
            Err(error) => ActionResponse {
                success: false,
                message: error.to_string(),
            },
        },
    )
}

async fn dashboard_delete(
    State(engine): State<Arc<Mutex<StorageEngine>>>,
    Query(query): Query<KeyQuery>,
) -> impl IntoResponse {
    let mut engine = engine.lock().await;
    Json(match engine.delete(query.key.as_bytes()) {
        Ok(()) => ActionResponse {
            success: true,
            message: format!("deleted {}", query.key),
        },
        Err(error) => ActionResponse {
            success: false,
            message: error.to_string(),
        },
    })
}

async fn dashboard_flush(State(engine): State<Arc<Mutex<StorageEngine>>>) -> impl IntoResponse {
    let mut engine = engine.lock().await;
    Json(match engine.flush_memtable() {
        Ok(Some(metadata)) => ActionResponse {
            success: true,
            message: format!(
                "flushed SSTable {} with {} entries",
                metadata.id.0, metadata.entry_count
            ),
        },
        Ok(None) => ActionResponse {
            success: true,
            message: "nothing to flush".to_string(),
        },
        Err(error) => ActionResponse {
            success: false,
            message: error.to_string(),
        },
    })
}

async fn dashboard_compact(State(engine): State<Arc<Mutex<StorageEngine>>>) -> impl IntoResponse {
    let mut engine = engine.lock().await;
    Json(match engine.compact_all() {
        Ok(report) => ActionResponse {
            success: true,
            message: format!(
                "compacted {} SSTables into {}; dropped {} entries",
                report.input_sstables, report.output_sstables, report.dropped_entries
            ),
        },
        Err(error) => ActionResponse {
            success: false,
            message: error.to_string(),
        },
    })
}

async fn dashboard_demo(State(engine): State<Arc<Mutex<StorageEngine>>>) -> impl IntoResponse {
    let mut engine = engine.lock().await;
    let demo_rows = [
        ("user:1", "alice"),
        ("user:2", "bob"),
        ("user:3", "carol"),
        ("order:1001", "paid"),
        ("order:1002", "pending"),
        ("feature:bloom", "enabled"),
    ];

    for (key, value) in demo_rows {
        if let Err(error) = engine.put(key.as_bytes(), value.as_bytes()) {
            return Json(ActionResponse {
                success: false,
                message: error.to_string(),
            });
        }
    }

    Json(ActionResponse {
        success: true,
        message: "seeded demo keys".to_string(),
    })
}

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KV Store Dashboard</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #15211c;
      --muted: #65736f;
      --line: #dce4df;
      --panel: #ffffff;
      --page: #f5f7f4;
      --accent: #21845f;
      --accent-soft: #dff3ea;
      --accent-dark: #146047;
      --blue: #2d6cdf;
      --blue-soft: #e6eefc;
      --amber: #b86e18;
      --amber-soft: #fff1d8;
      --danger: #ad3f48;
      --danger-soft: #f9e5e8;
      --shadow: 0 16px 40px rgba(24, 43, 35, 0.10);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        linear-gradient(180deg, rgba(33, 132, 95, 0.13), rgba(45, 108, 223, 0.07) 280px, transparent 520px),
        var(--page);
    }
    header {
      position: sticky;
      top: 0;
      z-index: 10;
      padding: 18px 32px;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.92);
      backdrop-filter: blur(16px);
    }
    .topline {
      display: flex;
      gap: 16px;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      max-width: 1180px;
      margin: 0 auto;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .mark {
      width: 42px;
      height: 42px;
      display: grid;
      place-items: center;
      border-radius: 8px;
      color: #fff;
      font-weight: 800;
      background: linear-gradient(135deg, var(--accent), var(--blue));
      box-shadow: 0 10px 26px rgba(33, 132, 95, 0.22);
    }
    h1 { margin: 0; font-size: 26px; font-weight: 780; letter-spacing: 0; }
    header p { margin: 6px 0 0; color: var(--muted); }
    .status {
      min-width: 240px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--panel);
      color: var(--muted);
      box-shadow: 0 6px 16px rgba(24, 43, 35, 0.06);
    }
    .status.ok { background: var(--accent-soft); color: var(--accent-dark); }
    .status.err { background: var(--danger-soft); color: var(--danger); }
    main { max-width: 1180px; margin: 0 auto; padding: 26px 24px 40px; }
    section { margin-bottom: 22px; }
    .panel {
      border: 1px solid rgba(220, 228, 223, 0.92);
      border-radius: 8px;
      padding: 18px;
      background: rgba(255, 255, 255, 0.86);
      box-shadow: var(--shadow);
    }
    h2 { margin: 0 0 14px; font-size: 17px; letter-spacing: 0; }
    .grid {
      display: grid;
      grid-template-columns: minmax(300px, 390px) minmax(0, 1fr);
      gap: 22px;
      align-items: start;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 12px;
    }
    .pipeline {
      display: grid;
      grid-template-columns: repeat(4, minmax(150px, 1fr));
      gap: 12px;
    }
    .stage {
      position: relative;
      min-height: 110px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      background: #fff;
      overflow: hidden;
    }
    .stage::before,
    .stat::before {
      content: "";
      position: absolute;
      inset: 0 0 auto;
      height: 4px;
      background: var(--accent);
    }
    .stage:nth-child(2)::before,
    .stat:nth-child(2)::before { background: var(--blue); }
    .stage:nth-child(3)::before,
    .stat:nth-child(3)::before { background: var(--amber); }
    .stage:nth-child(4)::before,
    .stat:nth-child(4)::before { background: var(--danger); }
    .stat:nth-child(5)::before { background: #6b5bd6; }
    .stage + .stage::after {
      content: "→";
      position: absolute;
      left: -15px;
      top: 42px;
      color: var(--muted);
      font-weight: 800;
    }
    .stage strong {
      display: block;
      margin-bottom: 6px;
      font-size: 15px;
    }
    .stage span {
      color: var(--muted);
      font-size: 13px;
    }
    .meter {
      height: 8px;
      margin-top: 12px;
      border-radius: 999px;
      overflow: hidden;
      background: #e6ece8;
    }
    .meter > i {
      display: block;
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, var(--accent), var(--blue));
      transition: width 160ms ease;
    }
    .stat {
      position: relative;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      background: #fff;
      overflow: hidden;
      transition: transform 140ms ease, box-shadow 140ms ease;
    }
    .stat:hover { transform: translateY(-2px); box-shadow: 0 12px 28px rgba(24, 43, 35, 0.10); }
    .label { color: var(--muted); font-size: 12px; }
    .value { margin-top: 5px; font-size: 26px; font-weight: 780; }
    form {
      display: grid;
      gap: 10px;
      margin-bottom: 12px;
    }
    label { display: grid; gap: 4px; color: var(--muted); font-size: 12px; }
    input {
      width: 100%;
      height: 38px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      font: inherit;
      background: #fff;
      transition: border-color 140ms ease, box-shadow 140ms ease;
    }
    input:focus {
      outline: none;
      border-color: var(--blue);
      box-shadow: 0 0 0 3px rgba(45, 108, 223, 0.14);
    }
    button {
      height: 38px;
      padding: 0 14px;
      border: 0;
      border-radius: 8px;
      font: inherit;
      font-weight: 650;
      color: #fff;
      background: var(--accent);
      cursor: pointer;
      box-shadow: 0 8px 16px rgba(33, 132, 95, 0.16);
      transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease;
    }
    button:active { transform: translateY(1px); }
    .row { display: flex; gap: 10px; align-items: end; }
    .row > * { flex: 1; }
    .row button { flex: 0 0 auto; }
    button:hover { background: var(--accent-dark); }
    .secondary { background: #344c61; box-shadow: 0 8px 16px rgba(52, 76, 97, 0.15); }
    .danger { background: var(--danger); }
    .toolbar { display: flex; gap: 10px; flex-wrap: wrap; }
    .command-list {
      display: grid;
      gap: 8px;
    }
    code {
      display: block;
      overflow-x: auto;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #f9fbfa;
      color: #28342d;
      font-size: 13px;
      box-shadow: inset 4px 0 0 var(--blue-soft);
    }
    .activity {
      display: grid;
      gap: 8px;
      margin: 0;
      padding: 0;
      list-style: none;
    }
    .activity li {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 9px 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      color: var(--muted);
      font-size: 13px;
    }
    .activity b { color: var(--ink); }
    table {
      width: 100%;
      border-collapse: collapse;
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    th, td { padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: left; }
    tbody tr:hover { background: #f6faf8; }
    th { color: var(--muted); font-size: 12px; background: #f2f6f4; }
    .result { margin: 8px 0 14px; padding: 10px 12px; border-radius: 8px; }
    .ok { background: #e8f5ee; }
    .muted { background: #eef1ee; color: var(--muted); }
    .err { background: var(--danger-soft); color: var(--danger); }
    .pulse { animation: pulse 420ms ease; }
    @keyframes pulse {
      0% { transform: scale(1); }
      50% { transform: scale(1.015); }
      100% { transform: scale(1); }
    }
    @media (max-width: 800px) {
      main { padding: 16px; }
      header { padding: 20px 18px 16px; }
      .grid { grid-template-columns: 1fr; }
      .pipeline { grid-template-columns: 1fr; }
      .row { display: grid; }
      .row button { width: 100%; }
    }
  </style>
</head>
<body>
  <header>
    <div class="topline">
      <div class="brand">
        <div class="mark">KV</div>
        <div>
          <h1>KV Store Dashboard</h1>
          <p>Live HTTP dashboard for the local gRPC KV server. CLI on port 50051.</p>
        </div>
      </div>
      <div id="status" class="status">Loading...</div>
    </div>
  </header>
  <main>
    <section class="panel">
      <h2>Storage Stats</h2>
      <div class="stats">
        <div class="stat"><div class="label">MemTable Entries</div><div id="stat-memtable-entries" class="value">-</div></div>
        <div class="stat"><div class="label">MemTable Bytes</div><div id="stat-memtable-bytes" class="value">-</div></div>
        <div class="stat"><div class="label">SSTables</div><div id="stat-sstables" class="value">-</div></div>
        <div class="stat"><div class="label">Bloom Filters</div><div id="stat-bloom" class="value">-</div></div>
        <div class="stat"><div class="label">Next SSTable ID</div><div id="stat-next" class="value">-</div></div>
      </div>
    </section>

    <section class="panel">
      <h2>LSM Flow</h2>
      <div class="pipeline">
        <div class="stage">
          <strong>WAL</strong>
          <span id="flow-wal">Durable writes active</span>
        </div>
        <div class="stage">
          <strong>MemTable</strong>
          <span id="flow-memtable">-</span>
          <div class="meter"><i id="flush-meter"></i></div>
        </div>
        <div class="stage">
          <strong>SSTables</strong>
          <span id="flow-sstables">-</span>
        </div>
        <div class="stage">
          <strong>Bloom Filters</strong>
          <span id="flow-bloom">-</span>
        </div>
      </div>
    </section>

    <div class="grid">
      <div>
        <section class="panel">
          <h2>Write</h2>
          <form id="put-form">
            <label>Key<input id="put-key" autocomplete="off"></label>
            <label>Value<input id="put-value" autocomplete="off"></label>
            <button type="submit">Put</button>
          </form>
          <form id="delete-form">
            <label>Key<input id="delete-key" autocomplete="off"></label>
            <button class="danger" type="submit">Delete</button>
          </form>
        </section>

        <section class="panel">
          <h2>Lookup</h2>
          <form id="get-form">
            <div class="row">
              <label>Key<input id="get-key" autocomplete="off"></label>
              <button type="submit">Get</button>
            </div>
          </form>
          <div id="lookup-result" class="result muted">No lookup yet</div>
        </section>

        <section class="panel">
          <h2>Maintenance</h2>
          <div class="toolbar">
            <button id="flush-button" class="secondary" type="button">Flush MemTable</button>
            <button id="compact-button" class="secondary" type="button">Compact SSTables</button>
            <button id="demo-button" type="button">Seed Demo Data</button>
          </div>
        </section>

        <section class="panel">
          <h2>CLI Commands</h2>
          <div class="command-list">
            <code>cargo run -- --endpoint http://127.0.0.1:50051 put user:1 alice</code>
            <code>cargo run -- --endpoint http://127.0.0.1:50051 scan --prefix user: --limit 10</code>
            <code>cargo run -- --endpoint http://127.0.0.1:50051 flush</code>
          </div>
        </section>

        <section class="panel">
          <h2>Activity</h2>
          <ul id="activity-log" class="activity"></ul>
        </section>
      </div>

      <section class="panel">
        <h2>Scan Results</h2>
        <form id="scan-form">
          <div class="row">
            <label>Prefix<input id="scan-prefix" autocomplete="off"></label>
            <label>Limit<input id="scan-limit" type="number" value="100" min="1" max="500"></label>
            <button type="submit">Scan</button>
          </div>
        </form>
        <table>
          <thead><tr><th>Key</th><th>Value</th></tr></thead>
          <tbody id="scan-rows"><tr><td colspan="2">Loading...</td></tr></tbody>
        </table>
      </section>
    </div>
  </main>
  <script>
    const statusEl = document.querySelector('#status');
    const scanRowsEl = document.querySelector('#scan-rows');
    const lookupEl = document.querySelector('#lookup-result');
    const activityLogEl = document.querySelector('#activity-log');
    const activity = [];

    function setStatus(message, kind = 'ok') {
      statusEl.textContent = message;
      statusEl.className = `status ${kind}`;
    }

    function pushActivity(label, detail) {
      const time = new Date().toLocaleTimeString();
      activity.unshift({ label, detail, time });
      activity.splice(8);
      activityLogEl.innerHTML = activity
        .map((item) => `<li><span><b>${escapeHtml(item.label)}</b> ${escapeHtml(item.detail)}</span><span>${escapeHtml(item.time)}</span></li>`)
        .join('');
    }

    function encode(params) {
      return new URLSearchParams(params).toString();
    }

    async function requestJson(url, options = {}) {
      const response = await fetch(url, options);
      if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
      }
      return response.json();
    }

    function setMetric(selector, value) {
      const element = document.querySelector(selector);
      const nextValue = String(value);
      if (element.textContent === nextValue) {
        return;
      }
      element.textContent = nextValue;
      const card = element.closest('.stat');
      if (card) {
        card.classList.remove('pulse');
        void card.offsetWidth;
        card.classList.add('pulse');
      }
    }

    async function refreshStats() {
      const stats = await requestJson('/api/stats');
      setMetric('#stat-memtable-entries', stats.memtable_entries);
      setMetric('#stat-memtable-bytes', stats.memtable_bytes);
      setMetric('#stat-sstables', stats.sstable_count);
      setMetric('#stat-bloom', stats.bloom_filter_count);
      setMetric('#stat-next', stats.next_sstable_id);
      document.querySelector('#flow-memtable').textContent = `${stats.memtable_entries} entries, ${stats.memtable_bytes} bytes`;
      document.querySelector('#flow-sstables').textContent = `${stats.sstable_count} immutable files`;
      document.querySelector('#flow-bloom').textContent = `${stats.bloom_filter_count} filters loaded`;
      document.querySelector('#flow-wal').textContent = stats.memtable_entries > 0 ? 'WAL has unapplied-to-SSTable writes' : 'WAL is clean after flush';
      const pressure = Math.min(100, Math.round((stats.memtable_bytes / Math.max(1, stats.flush_threshold_bytes)) * 100));
      document.querySelector('#flush-meter').style.width = `${pressure}%`;
    }

    async function scan() {
      const prefix = document.querySelector('#scan-prefix').value;
      const limit = document.querySelector('#scan-limit').value || '100';
      const payload = await requestJson(`/api/scan?${encode({ prefix, limit })}`);
      if (payload.rows.length === 0) {
        scanRowsEl.innerHTML = '<tr><td colspan="2">No rows</td></tr>';
        return;
      }
      scanRowsEl.innerHTML = payload.rows
        .map((row) => `<tr><td>${escapeHtml(row.key)}</td><td>${escapeHtml(row.value)}</td></tr>`)
        .join('');
    }

    async function refreshAll(message = 'Ready') {
      await refreshStats();
      await scan();
      setStatus(message);
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    document.querySelector('#put-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const key = document.querySelector('#put-key').value;
      const value = document.querySelector('#put-value').value;
      try {
        const payload = await requestJson(`/api/put?${encode({ key, value })}`, { method: 'POST' });
        await refreshAll(payload.message);
        pushActivity('PUT', `${key}=${value}`);
        document.querySelector('#get-key').value = key;
        document.querySelector('#scan-prefix').value = key.includes(':') ? `${key.split(':')[0]}:` : '';
      } catch (error) {
        setStatus(error.message, 'err');
      }
    });

    document.querySelector('#delete-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const key = document.querySelector('#delete-key').value;
      try {
        const payload = await requestJson(`/api/delete?${encode({ key })}`, { method: 'POST' });
        await refreshAll(payload.message);
        pushActivity('DELETE', key);
      } catch (error) {
        setStatus(error.message, 'err');
      }
    });

    document.querySelector('#get-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const key = document.querySelector('#get-key').value;
      try {
        const payload = await requestJson(`/api/get?${encode({ key })}`);
        if (payload.found) {
          lookupEl.className = 'result ok';
          lookupEl.innerHTML = `<b>${escapeHtml(payload.key)}</b> = ${escapeHtml(payload.value)}`;
          pushActivity('GET', `${payload.key} found`);
        } else {
          lookupEl.className = 'result muted';
          lookupEl.innerHTML = `<b>${escapeHtml(payload.key)}</b> not found`;
          pushActivity('GET', `${payload.key} not found`);
        }
        setStatus('Lookup complete');
      } catch (error) {
        setStatus(error.message, 'err');
      }
    });

    document.querySelector('#scan-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      try {
        await scan();
        pushActivity('SCAN', document.querySelector('#scan-prefix').value || '(all)');
        setStatus('Scan refreshed');
      } catch (error) {
        setStatus(error.message, 'err');
      }
    });

    document.querySelector('#flush-button').addEventListener('click', async () => {
      try {
        const payload = await requestJson('/api/flush', { method: 'POST' });
        await refreshAll(payload.message);
        pushActivity('FLUSH', payload.message);
      } catch (error) {
        setStatus(error.message, 'err');
      }
    });

    document.querySelector('#compact-button').addEventListener('click', async () => {
      try {
        const payload = await requestJson('/api/compact', { method: 'POST' });
        await refreshAll(payload.message);
        pushActivity('COMPACT', payload.message);
      } catch (error) {
        setStatus(error.message, 'err');
      }
    });

    document.querySelector('#demo-button').addEventListener('click', async () => {
      try {
        const payload = await requestJson('/api/demo', { method: 'POST' });
        document.querySelector('#scan-prefix').value = 'user:';
        await refreshAll(payload.message);
        pushActivity('DEMO', payload.message);
      } catch (error) {
        setStatus(error.message, 'err');
      }
    });

    pushActivity('READY', 'dashboard connected');
    refreshAll();
    setInterval(refreshStats, 2000);
  </script>
</body>
</html>"#;
