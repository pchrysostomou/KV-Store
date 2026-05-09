use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Json;
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
use tokio_stream::{self as stream, Stream, StreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::proto::kv_store_server::{KvStore, KvStoreServer};
use crate::proto::{
    CompactRequest, CompactResponse, DeleteRequest, DeleteResponse, FlushRequest, FlushResponse,
    GetRequest, GetResponse, PutRequest, PutResponse, ScanRequest, ScanResponse, StatsRequest,
    StatsResponse, WatchEvent, WatchRequest, WatchResponse,
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
    watch_tx: broadcast::Sender<WatchResponse>,
}

#[derive(Clone)]
struct DashboardState {
    engine: Arc<Mutex<StorageEngine>>,
    watch_tx: broadcast::Sender<WatchResponse>,
}

impl GrpcKvStore {
    pub fn new(engine: StorageEngine) -> Self {
        let (watch_tx, _) = broadcast::channel(1024);
        Self {
            engine: Arc::new(Mutex::new(engine)),
            watch_tx,
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
    let (watch_tx, _) = broadcast::channel(1024);
    let grpc_service = GrpcKvStore {
        engine: Arc::clone(&shared_engine),
        watch_tx: watch_tx.clone(),
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
        let dashboard = serve_dashboard(dashboard_addr, shared_engine, watch_tx);

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
        emit_watch_event(&self.watch_tx, WatchEvent::Put, request.key, request.value);

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
        emit_watch_event(
            &self.watch_tx,
            WatchEvent::Delete,
            request.key,
            String::new(),
        );

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
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let prefix = request.into_inner().prefix;
        let stream =
            BroadcastStream::new(self.watch_tx.subscribe()).filter_map(move |event| match event {
                Ok(event) if event.key.starts_with(&prefix) => Some(Ok(event)),
                Ok(_) => None,
                Err(BroadcastStreamRecvError::Lagged(skipped)) => Some(Err(Status::data_loss(
                    format!("watch lagged by {skipped} events"),
                ))),
            });

        Ok(Response::new(Box::pin(stream)))
    }
}

fn emit_watch_event(
    watch_tx: &broadcast::Sender<WatchResponse>,
    event: WatchEvent,
    key: String,
    value: String,
) {
    let _ = watch_tx.send(WatchResponse {
        key,
        value,
        event: event as i32,
    });
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
    watch_tx: broadcast::Sender<WatchResponse>,
) -> crate::storage::Result<()> {
    let state = DashboardState { engine, watch_tx };
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
        .route("/api/import", post(dashboard_import))
        .route("/api/events", get(dashboard_events))
        .with_state(state);

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

#[derive(Debug, Deserialize, Serialize)]
struct RowResponseBody {
    key: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct ImportRequestBody {
    rows: Vec<RowResponseBody>,
}

#[derive(Debug, Serialize)]
struct ImportResponseBody {
    success: bool,
    imported: usize,
    message: String,
}

#[derive(Debug, Serialize)]
struct DashboardWatchEvent {
    key: String,
    value: String,
    event: String,
}

#[derive(Debug, Serialize)]
struct ScanResponseBody {
    rows: Vec<RowResponseBody>,
}

async fn dashboard_home() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}

async fn dashboard_stats(State(state): State<DashboardState>) -> impl IntoResponse {
    let engine = state.engine.lock().await;
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
    State(state): State<DashboardState>,
    Query(query): Query<KeyQuery>,
) -> impl IntoResponse {
    let engine = state.engine.lock().await;
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
    State(state): State<DashboardState>,
    Query(query): Query<ScanQuery>,
) -> impl IntoResponse {
    let prefix = query.prefix.unwrap_or_default();
    let limit = query.limit.unwrap_or(100).min(500);
    let engine = state.engine.lock().await;
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
    State(state): State<DashboardState>,
    Query(query): Query<PutQuery>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
    Json(
        match engine.put(query.key.as_bytes(), query.value.as_bytes()) {
            Ok(()) => {
                emit_watch_event(
                    &state.watch_tx,
                    WatchEvent::Put,
                    query.key.clone(),
                    query.value,
                );
                ActionResponse {
                    success: true,
                    message: format!("stored {}", query.key),
                }
            }
            Err(error) => ActionResponse {
                success: false,
                message: error.to_string(),
            },
        },
    )
}

async fn dashboard_delete(
    State(state): State<DashboardState>,
    Query(query): Query<KeyQuery>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
    Json(match engine.delete(query.key.as_bytes()) {
        Ok(()) => {
            emit_watch_event(
                &state.watch_tx,
                WatchEvent::Delete,
                query.key.clone(),
                String::new(),
            );
            ActionResponse {
                success: true,
                message: format!("deleted {}", query.key),
            }
        }
        Err(error) => ActionResponse {
            success: false,
            message: error.to_string(),
        },
    })
}

async fn dashboard_flush(State(state): State<DashboardState>) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
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

async fn dashboard_compact(State(state): State<DashboardState>) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
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

async fn dashboard_demo(State(state): State<DashboardState>) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
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
        emit_watch_event(
            &state.watch_tx,
            WatchEvent::Put,
            key.to_string(),
            value.to_string(),
        );
    }

    Json(ActionResponse {
        success: true,
        message: "seeded demo keys".to_string(),
    })
}

async fn dashboard_import(
    State(state): State<DashboardState>,
    Json(request): Json<ImportRequestBody>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
    let mut imported = 0;

    for row in request.rows {
        if row.key.is_empty() {
            continue;
        }

        if let Err(error) = engine.put(row.key.as_bytes(), row.value.as_bytes()) {
            return Json(ImportResponseBody {
                success: false,
                imported,
                message: error.to_string(),
            });
        }

        emit_watch_event(&state.watch_tx, WatchEvent::Put, row.key.clone(), row.value);
        imported += 1;
    }

    Json(ImportResponseBody {
        success: true,
        imported,
        message: format!("imported {imported} rows"),
    })
}

async fn dashboard_events(
    State(state): State<DashboardState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = BroadcastStream::new(state.watch_tx.subscribe()).filter_map(|event| match event {
        Ok(event) => {
            let payload = DashboardWatchEvent {
                key: event.key,
                value: event.value,
                event: watch_event_name(event.event).to_string(),
            };
            serde_json::to_string(&payload)
                .ok()
                .map(|data| Ok(Event::default().event("kv").data(data)))
        }
        Err(BroadcastStreamRecvError::Lagged(_)) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn watch_event_name(event: i32) -> &'static str {
    match WatchEvent::try_from(event).unwrap_or(WatchEvent::Unspecified) {
        WatchEvent::Put => "put",
        WatchEvent::Delete => "delete",
        WatchEvent::Unspecified => "unknown",
    }
}

const DASHBOARD_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KV Store Dashboard</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #13231d;
      --muted: #66766f;
      --line: #dce5df;
      --page: #f4f7f3;
      --panel: #ffffff;
      --accent: #21845f;
      --accent-soft: #dff3ea;
      --accent-dark: #146047;
      --blue: #2d6cdf;
      --blue-soft: #e7efff;
      --amber: #b86e18;
      --amber-soft: #fff2d7;
      --danger: #ad3f48;
      --danger-soft: #fae6e8;
      --purple: #6b5bd6;
      --shadow: 0 16px 38px rgba(21, 40, 32, 0.10);
    }
    body[data-theme="dark"] {
      color-scheme: dark;
      --ink: #eef7f1;
      --muted: #9dafaa;
      --line: #263a32;
      --page: #08110d;
      --panel: #101d18;
      --accent: #43b883;
      --accent-soft: #17382b;
      --accent-dark: #69d5a3;
      --blue: #79a7ff;
      --blue-soft: #172948;
      --amber: #e4a64f;
      --amber-soft: #3b2b12;
      --danger: #ff7a86;
      --danger-soft: #3b171c;
      --purple: #a59bff;
      --shadow: 0 18px 42px rgba(0, 0, 0, 0.28);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        linear-gradient(180deg, rgba(33, 132, 95, 0.13), rgba(45, 108, 223, 0.06) 290px, transparent 560px),
        radial-gradient(circle at 0 0, rgba(255, 255, 255, 0.88), transparent 390px),
        var(--page);
    }
    body[data-theme="dark"] {
      background:
        linear-gradient(180deg, rgba(67, 184, 131, 0.16), rgba(121, 167, 255, 0.08) 290px, transparent 560px),
        radial-gradient(circle at 0 0, rgba(255, 255, 255, 0.08), transparent 390px),
        var(--page);
    }
    header {
      position: sticky;
      top: 0;
      z-index: 10;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.94);
      backdrop-filter: blur(16px);
    }
    body[data-theme="dark"] header { background: rgba(10, 20, 15, 0.92); }
    .topline {
      max-width: 1320px;
      margin: 0 auto;
      padding: 20px 28px;
      display: flex;
      gap: 16px;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 13px;
      min-width: 0;
    }
    .mark {
      width: 42px;
      height: 42px;
      display: grid;
      place-items: center;
      border-radius: 8px;
      color: #fff;
      font-weight: 820;
      background: linear-gradient(135deg, var(--accent), var(--blue));
      box-shadow: 0 12px 28px rgba(33, 132, 95, 0.24);
    }
    h1 { margin: 0; font-size: 27px; font-weight: 800; letter-spacing: 0; }
    header p { margin: 5px 0 0; color: var(--muted); }
    .server-status {
      min-width: 220px;
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 10px 13px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #fff;
      color: var(--muted);
      box-shadow: 0 8px 18px rgba(24, 43, 35, 0.07);
    }
    .header-actions {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    .server-status::before {
      content: "";
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--muted);
    }
    .server-status.ok { background: var(--accent-soft); color: var(--accent-dark); }
    .server-status.ok::before { background: var(--accent); }
    .server-status.err { background: var(--danger-soft); color: var(--danger); }
    .server-status.err::before { background: var(--danger); }
    body[data-theme="dark"] .server-status { background: var(--panel); }
    main {
      max-width: 1320px;
      margin: 0 auto;
      padding: 26px 28px 44px;
      display: grid;
      gap: 22px;
    }
    .panel {
      border: 1px solid rgba(220, 229, 223, 0.95);
      border-radius: 8px;
      padding: 18px;
      background: rgba(255, 255, 255, 0.90);
      box-shadow: var(--shadow);
    }
    body[data-theme="dark"] .panel,
    body[data-theme="dark"] .stat,
    body[data-theme="dark"] .stage,
    body[data-theme="dark"] .form-card,
    body[data-theme="dark"] .table-wrap,
    body[data-theme="dark"] table,
    body[data-theme="dark"] input,
    body[data-theme="dark"] .ghost,
    body[data-theme="dark"] code,
    body[data-theme="dark"] .activity li {
      background: var(--panel);
    }
    .panel-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
    }
    .head-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    h2 { margin: 0; font-size: 17px; letter-spacing: 0; }
    .badge {
      min-height: 27px;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 9px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #f8fbf9;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
      white-space: nowrap;
    }
    body[data-theme="dark"] .badge { background: #14231d; }
    .stats {
      display: grid;
      grid-template-columns: repeat(5, minmax(140px, 1fr));
      gap: 12px;
    }
    .spark-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(160px, 1fr));
      gap: 12px;
    }
    .spark-card {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      background: #fff;
      min-height: 128px;
    }
    body[data-theme="dark"] .spark-card { background: var(--panel); }
    .spark {
      width: 100%;
      height: 58px;
      margin-top: 8px;
      overflow: visible;
    }
    .spark path {
      fill: none;
      stroke-width: 3;
      stroke-linecap: round;
      stroke-linejoin: round;
    }
    .spark-bg {
      stroke: var(--line);
      stroke-width: 1;
    }
    .spark-value {
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }
    .stat,
    .stage,
    .form-card {
      position: relative;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      overflow: hidden;
    }
    .stat {
      min-height: 78px;
      padding: 16px;
      transition: transform 140ms ease, box-shadow 140ms ease;
    }
    .stat:hover { transform: translateY(-2px); box-shadow: 0 12px 28px rgba(24, 43, 35, 0.10); }
    .stat::before,
    .stage::before {
      content: "";
      position: absolute;
      inset: 0 0 auto;
      height: 4px;
      background: var(--accent);
    }
    .stat:nth-child(2)::before,
    .stage:nth-child(2)::before { background: var(--blue); }
    .stat:nth-child(3)::before,
    .stage:nth-child(3)::before { background: var(--amber); }
    .stat:nth-child(4)::before,
    .stage:nth-child(4)::before { background: var(--danger); }
    .stat:nth-child(5)::before { background: var(--purple); }
    .label { color: var(--muted); font-size: 12px; }
    .value { margin-top: 5px; font-size: 27px; font-weight: 800; }
    .pipeline {
      display: grid;
      grid-template-columns: repeat(4, minmax(150px, 1fr));
      gap: 12px;
    }
    .stage {
      min-height: 116px;
      padding: 16px;
    }
    .stage strong { display: block; margin-bottom: 6px; font-size: 15px; }
    .stage span { color: var(--muted); font-size: 13px; }
    .meter {
      height: 8px;
      margin-top: 13px;
      border-radius: 999px;
      overflow: hidden;
      background: #e6ece8;
    }
    body[data-theme="dark"] .meter { background: #24372f; }
    .meter > i {
      display: block;
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, var(--accent), var(--blue));
      transition: width 180ms ease;
    }
    .workspace {
      display: grid;
      grid-template-columns: minmax(340px, 450px) minmax(0, 1fr);
      gap: 22px;
      align-items: start;
    }
    .stack {
      display: grid;
      gap: 18px;
      min-width: 0;
    }
    .operation-grid {
      display: grid;
      gap: 12px;
    }
    .form-card {
      padding: 14px;
      box-shadow: inset 4px 0 0 var(--accent-soft);
    }
    .form-card.danger-card { box-shadow: inset 4px 0 0 var(--danger-soft); }
    form {
      display: grid;
      gap: 10px;
      margin: 0;
    }
    .field-row,
    .scan-controls {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 10px;
      align-items: end;
    }
    .scan-controls {
      grid-template-columns: minmax(180px, 1fr) 110px auto;
      margin-bottom: 10px;
    }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; }
    input {
      width: 100%;
      height: 39px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      font: inherit;
      background: #fff;
      transition: border-color 140ms ease, box-shadow 140ms ease;
    }
    body[data-theme="dark"] input { color: var(--ink); }
    textarea {
      width: 100%;
      min-height: 130px;
      resize: vertical;
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      font: 13px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: var(--ink);
      background: #fff;
    }
    body[data-theme="dark"] textarea { background: var(--panel); }
    input:focus {
      outline: none;
      border-color: var(--blue);
      box-shadow: 0 0 0 3px rgba(45, 108, 223, 0.14);
    }
    button {
      min-height: 39px;
      padding: 0 14px;
      border: 0;
      border-radius: 8px;
      font: inherit;
      font-weight: 700;
      color: #fff;
      background: var(--accent);
      cursor: pointer;
      box-shadow: 0 8px 16px rgba(33, 132, 95, 0.16);
      transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease, opacity 120ms ease;
    }
    button:hover { background: var(--accent-dark); }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: wait; opacity: 0.68; }
    .secondary { background: #344c61; box-shadow: 0 8px 16px rgba(52, 76, 97, 0.15); }
    .danger { background: var(--danger); box-shadow: 0 8px 16px rgba(173, 63, 72, 0.15); }
    .ghost {
      min-height: 31px;
      padding: 0 10px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--muted);
      box-shadow: none;
      font-size: 12px;
    }
    .ghost:hover { background: #f4f8f6; color: var(--ink); }
    .wide { width: 100%; }
    .toolbar {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .toolbar button:last-child { grid-column: 1 / -1; }
    .bench-controls {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto;
      gap: 10px;
      align-items: end;
    }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin-top: 12px;
    }
    .kpi {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 11px;
      background: #fff;
    }
    body[data-theme="dark"] .kpi { background: var(--panel); }
    .kpi strong {
      display: block;
      margin-top: 4px;
      font-size: 20px;
    }
    .bench-bar {
      height: 8px;
      margin-top: 12px;
      border-radius: 999px;
      overflow: hidden;
      background: #e6ece8;
    }
    body[data-theme="dark"] .bench-bar { background: #24372f; }
    .bench-bar > i {
      display: block;
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, var(--blue), var(--accent));
      transition: width 120ms ease;
    }
    .import-actions {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: start;
      margin-top: 10px;
    }
    .result {
      margin-top: 10px;
      padding: 12px;
      border-radius: 8px;
      border: 1px solid transparent;
    }
    .ok { background: #e8f5ee; border-color: #cae6d6; }
    .muted { background: #eef1ee; color: var(--muted); }
    .err { background: var(--danger-soft); color: var(--danger); border-color: #efc4ca; }
    .selected { background: var(--blue-soft); border-color: #c7d9ff; }
    body[data-theme="dark"] .ok { background: #123525; border-color: #235c43; }
    body[data-theme="dark"] .muted { background: #17241e; }
    body[data-theme="dark"] .selected { border-color: #314f8a; }
    .prefix-pills {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }
    .table-wrap {
      max-height: 520px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 460px;
    }
    th, td {
      padding: 11px 13px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      color: var(--muted);
      font-size: 12px;
      background: #f2f6f4;
    }
    body[data-theme="dark"] th { background: #14231d; }
    tbody tr[data-key] { cursor: pointer; }
    tbody tr:hover { background: #f6faf8; }
    body[data-theme="dark"] tbody tr:hover { background: #14231d; }
    tbody tr:last-child td { border-bottom: 0; }
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
      max-height: 250px;
      overflow: auto;
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
    .watch-event-put b { color: var(--accent-dark); }
    .watch-event-delete b { color: var(--danger); }
    .pulse { animation: pulse 420ms ease; }
    @keyframes pulse {
      0% { transform: scale(1); }
      50% { transform: scale(1.015); }
      100% { transform: scale(1); }
    }
    @media (max-width: 980px) {
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .spark-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .pipeline { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .workspace { grid-template-columns: 1fr; }
      .scan-controls { grid-template-columns: 1fr; }
      .bench-controls { grid-template-columns: 1fr; }
      .import-actions { grid-template-columns: 1fr; }
      .scan-controls button { width: 100%; }
      .bench-controls button { width: 100%; }
    }
    @media (max-width: 620px) {
      .topline { padding: 18px 16px; }
      main { padding: 18px 16px 32px; }
      .brand { align-items: flex-start; }
      h1 { font-size: 23px; }
      .stats,
      .spark-grid,
      .pipeline,
      .field-row,
      .kpi-grid,
      .toolbar { grid-template-columns: 1fr; }
      .toolbar button:last-child { grid-column: auto; }
      .server-status { width: 100%; }
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
          <p>HTTP dashboard for the local gRPC KV server. CLI on port 50051.</p>
        </div>
      </div>
      <div class="header-actions">
        <button id="theme-toggle" class="ghost" type="button">Dark</button>
        <div id="server-status" class="server-status"><span id="status">Loading...</span></div>
      </div>
    </div>
  </header>
  <main>
    <section class="panel">
      <div class="panel-head">
        <h2>Storage Stats</h2>
        <span id="flush-state" class="badge">checking</span>
      </div>
      <div class="stats">
        <div class="stat"><div class="label">MemTable Entries</div><div id="stat-memtable-entries" class="value">-</div></div>
        <div class="stat"><div class="label">MemTable Bytes</div><div id="stat-memtable-bytes" class="value">-</div></div>
        <div class="stat"><div class="label">SSTables</div><div id="stat-sstables" class="value">-</div></div>
        <div class="stat"><div class="label">Bloom Filters</div><div id="stat-bloom" class="value">-</div></div>
        <div class="stat"><div class="label">Next SSTable ID</div><div id="stat-next" class="value">-</div></div>
      </div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <h2>Live Signals</h2>
        <span id="sample-count" class="badge">0 samples</span>
      </div>
      <div class="spark-grid">
        <div class="spark-card">
          <div class="label">MemTable Bytes</div>
          <svg class="spark" viewBox="0 0 220 58" preserveAspectRatio="none">
            <line class="spark-bg" x1="0" y1="54" x2="220" y2="54"></line>
            <path id="chart-memtable-bytes" stroke="#21845f"></path>
          </svg>
          <div id="chart-memtable-label" class="spark-value">waiting</div>
        </div>
        <div class="spark-card">
          <div class="label">SSTables</div>
          <svg class="spark" viewBox="0 0 220 58" preserveAspectRatio="none">
            <line class="spark-bg" x1="0" y1="54" x2="220" y2="54"></line>
            <path id="chart-sstables" stroke="#b86e18"></path>
          </svg>
          <div id="chart-sstable-label" class="spark-value">waiting</div>
        </div>
        <div class="spark-card">
          <div class="label">Bloom Filters</div>
          <svg class="spark" viewBox="0 0 220 58" preserveAspectRatio="none">
            <line class="spark-bg" x1="0" y1="54" x2="220" y2="54"></line>
            <path id="chart-bloom" stroke="#ad3f48"></path>
          </svg>
          <div id="chart-bloom-label" class="spark-value">waiting</div>
        </div>
        <div class="spark-card">
          <div class="label">MemTable Entries</div>
          <svg class="spark" viewBox="0 0 220 58" preserveAspectRatio="none">
            <line class="spark-bg" x1="0" y1="54" x2="220" y2="54"></line>
            <path id="chart-entries" stroke="#2d6cdf"></path>
          </svg>
          <div id="chart-entry-label" class="spark-value">waiting</div>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <h2>LSM Flow</h2>
        <span class="badge">WAL -> MemTable -> SSTable</span>
      </div>
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

    <div class="workspace">
      <div class="stack">
        <section class="panel">
          <div class="panel-head">
            <h2>Operations</h2>
            <span class="badge">read/write</span>
          </div>
          <div class="operation-grid">
            <form id="put-form" class="form-card">
              <div class="field-row">
                <label>Key<input id="put-key" autocomplete="off"></label>
                <label>Value<input id="put-value" autocomplete="off"></label>
              </div>
              <button class="wide" type="submit">Put</button>
            </form>
            <form id="delete-form" class="form-card danger-card">
              <label>Key<input id="delete-key" autocomplete="off"></label>
              <button class="danger wide" type="submit">Delete</button>
            </form>
          </div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h2>Lookup</h2>
            <span id="lookup-badge" class="badge">idle</span>
          </div>
          <form id="get-form">
            <div class="field-row">
              <label>Key<input id="get-key" autocomplete="off"></label>
              <button type="submit">Get</button>
            </div>
          </form>
          <div id="lookup-result" class="result muted">No lookup yet</div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h2>Maintenance</h2>
            <span class="badge">disk</span>
          </div>
          <div class="toolbar">
            <button id="flush-button" class="secondary" type="button">Flush MemTable</button>
            <button id="compact-button" class="secondary" type="button">Compact SSTables</button>
            <button id="demo-button" type="button">Seed Demo Data</button>
          </div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h2>Benchmark</h2>
            <span id="bench-badge" class="badge">idle</span>
          </div>
          <form id="bench-form">
            <div class="bench-controls">
              <label>Ops<input id="bench-ops" type="number" value="100" min="1" max="2000"></label>
              <label>Concurrency<input id="bench-concurrency" type="number" value="8" min="1" max="64"></label>
              <button type="submit">Run Bench</button>
            </div>
          </form>
          <div class="kpi-grid">
            <div class="kpi"><div class="label">Throughput</div><strong id="bench-throughput">-</strong></div>
            <div class="kpi"><div class="label">Avg Latency</div><strong id="bench-latency">-</strong></div>
            <div class="kpi"><div class="label">Errors</div><strong id="bench-errors">-</strong></div>
          </div>
          <div class="bench-bar"><i id="bench-progress"></i></div>
          <div id="bench-result" class="result muted">Run a small mixed PUT/GET workload from the browser.</div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h2>Activity</h2>
            <span id="activity-count" class="badge">0 events</span>
          </div>
          <ul id="activity-log" class="activity"></ul>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h2>Live Watch</h2>
            <span id="watch-count" class="badge">connecting</span>
          </div>
          <ul id="watch-log" class="activity"></ul>
        </section>
      </div>

      <div class="stack">
        <section class="panel">
          <div class="panel-head">
            <h2>Data Explorer</h2>
            <div class="head-actions">
              <button id="export-button" class="ghost" type="button" disabled>Export JSON</button>
              <span id="scan-count" class="badge">0 rows</span>
            </div>
          </div>
          <form id="scan-form">
            <div class="scan-controls">
              <label>Prefix<input id="scan-prefix" autocomplete="off"></label>
              <label>Limit<input id="scan-limit" type="number" value="100" min="1" max="500"></label>
              <button type="submit">Scan</button>
            </div>
          </form>
          <div class="prefix-pills">
            <button class="ghost" type="button" data-prefix="">All</button>
            <button class="ghost" type="button" data-prefix="user:">user:</button>
            <button class="ghost" type="button" data-prefix="order:">order:</button>
            <button class="ghost" type="button" data-prefix="feature:">feature:</button>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Key</th><th>Value</th></tr></thead>
              <tbody id="scan-rows"><tr><td colspan="2">Loading...</td></tr></tbody>
            </table>
          </div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h2>Snapshot Import</h2>
            <span id="import-badge" class="badge">paste JSON</span>
          </div>
          <textarea id="import-json" spellcheck="false" placeholder='{"rows":[{"key":"user:1","value":"alice"}]}'></textarea>
          <div class="import-actions">
            <div id="import-result" class="result muted">Paste an exported JSON snapshot or a raw rows array.</div>
            <button id="import-button" type="button">Import Rows</button>
          </div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h2>CLI Commands</h2>
            <span class="badge">same data</span>
          </div>
          <div class="command-list">
            <code>cargo run -- --endpoint http://127.0.0.1:50051 put user:1 alice</code>
            <code>cargo run -- --endpoint http://127.0.0.1:50051 scan --prefix user: --limit 10</code>
            <code>cargo run -- --endpoint http://127.0.0.1:50051 flush</code>
          </div>
        </section>
      </div>
    </div>
  </main>
  <script>
    const statusEl = document.querySelector('#status');
    const serverStatusEl = document.querySelector('#server-status');
    const scanRowsEl = document.querySelector('#scan-rows');
    const scanCountEl = document.querySelector('#scan-count');
    const lookupEl = document.querySelector('#lookup-result');
    const lookupBadgeEl = document.querySelector('#lookup-badge');
    const activityLogEl = document.querySelector('#activity-log');
    const activityCountEl = document.querySelector('#activity-count');
    const watchLogEl = document.querySelector('#watch-log');
    const watchCountEl = document.querySelector('#watch-count');
    const scanPrefixEl = document.querySelector('#scan-prefix');
    const scanLimitEl = document.querySelector('#scan-limit');
    const themeToggleEl = document.querySelector('#theme-toggle');
    const sampleCountEl = document.querySelector('#sample-count');
    const exportButtonEl = document.querySelector('#export-button');
    const benchBadgeEl = document.querySelector('#bench-badge');
    const benchProgressEl = document.querySelector('#bench-progress');
    const benchResultEl = document.querySelector('#bench-result');
    const benchThroughputEl = document.querySelector('#bench-throughput');
    const benchLatencyEl = document.querySelector('#bench-latency');
    const benchErrorsEl = document.querySelector('#bench-errors');
    const importJsonEl = document.querySelector('#import-json');
    const importButtonEl = document.querySelector('#import-button');
    const importBadgeEl = document.querySelector('#import-badge');
    const importResultEl = document.querySelector('#import-result');
    const statsHistory = [];
    let lastRows = [];
    const activity = [];
    const watchEvents = [];
    const maxSamples = 36;

    function applyTheme(theme) {
      document.body.dataset.theme = theme;
      themeToggleEl.textContent = theme === 'dark' ? 'Light' : 'Dark';
      localStorage.setItem('kv-dashboard-theme', theme);
    }

    function setStatus(message, kind = 'ok') {
      statusEl.textContent = message;
      serverStatusEl.className = `server-status ${kind}`;
    }

    function pushActivity(label, detail) {
      const time = new Date().toLocaleTimeString();
      activity.unshift({ label, detail, time });
      activity.splice(10);
      activityCountEl.textContent = `${activity.length} event${activity.length === 1 ? '' : 's'}`;
      activityLogEl.innerHTML = activity
        .map((item) => `<li><span><b>${escapeHtml(item.label)}</b> ${escapeHtml(item.detail)}</span><span>${escapeHtml(item.time)}</span></li>`)
        .join('');
    }

    function pushWatchEvent(event) {
      const time = new Date().toLocaleTimeString();
      watchEvents.unshift({ ...event, time });
      watchEvents.splice(12);
      watchCountEl.textContent = `${watchEvents.length} live`;
      watchLogEl.innerHTML = watchEvents
        .map((item) => {
          const label = item.event === 'delete' ? 'DELETE' : 'PUT';
          const detail = item.event === 'delete' ? item.key : `${item.key}=${item.value}`;
          return `<li class="watch-event-${escapeHtml(item.event)}"><span><b>${label}</b> ${escapeHtml(detail)}</span><span>${escapeHtml(item.time)}</span></li>`;
        })
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

    async function withBusy(button, label, action) {
      const original = button.textContent;
      button.disabled = true;
      button.textContent = label;
      try {
        await action();
      } finally {
        button.disabled = false;
        button.textContent = original;
      }
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

    function chartPath(values) {
      if (values.length === 0) {
        return '';
      }
      if (values.length === 1) {
        return `M 0 54 L 220 54`;
      }
      const min = Math.min(...values);
      const max = Math.max(...values);
      const span = Math.max(1, max - min);
      return values.map((value, index) => {
        const x = (index / (values.length - 1)) * 220;
        const y = 54 - ((value - min) / span) * 48;
        return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
      }).join(' ');
    }

    function setChart(pathSelector, labelSelector, values, suffix = '') {
      document.querySelector(pathSelector).setAttribute('d', chartPath(values));
      const current = values.at(-1) ?? 0;
      const previous = values.at(-2) ?? current;
      const delta = current - previous;
      const sign = delta > 0 ? '+' : '';
      document.querySelector(labelSelector).textContent = `${current}${suffix} (${sign}${delta})`;
    }

    function rememberStats(stats) {
      statsHistory.push({
        memtableBytes: stats.memtable_bytes,
        sstables: stats.sstable_count,
        bloom: stats.bloom_filter_count,
        entries: stats.memtable_entries,
      });
      statsHistory.splice(0, Math.max(0, statsHistory.length - maxSamples));
      sampleCountEl.textContent = `${statsHistory.length} sample${statsHistory.length === 1 ? '' : 's'}`;
      setChart('#chart-memtable-bytes', '#chart-memtable-label', statsHistory.map((item) => item.memtableBytes), ' bytes');
      setChart('#chart-sstables', '#chart-sstable-label', statsHistory.map((item) => item.sstables));
      setChart('#chart-bloom', '#chart-bloom-label', statsHistory.map((item) => item.bloom));
      setChart('#chart-entries', '#chart-entry-label', statsHistory.map((item) => item.entries));
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
      document.querySelector('#flush-state').textContent = stats.should_flush ? 'flush ready' : 'healthy';
      const pressure = Math.min(100, Math.round((stats.memtable_bytes / Math.max(1, stats.flush_threshold_bytes)) * 100));
      document.querySelector('#flush-meter').style.width = `${pressure}%`;
      rememberStats(stats);
    }

    function renderRows(rows) {
      lastRows = rows;
      exportButtonEl.disabled = rows.length === 0;
      scanCountEl.textContent = `${rows.length} row${rows.length === 1 ? '' : 's'}`;
      if (rows.length > 0 && importJsonEl.value.trim().length === 0) {
        importJsonEl.value = JSON.stringify({ rows: rows.slice(0, 20) }, null, 2);
      }
      if (rows.length === 0) {
        scanRowsEl.innerHTML = '<tr><td colspan="2">No rows</td></tr>';
        return;
      }
      scanRowsEl.innerHTML = rows
        .map((row) => `<tr data-key="${escapeHtml(row.key)}" data-value="${escapeHtml(row.value)}"><td>${escapeHtml(row.key)}</td><td>${escapeHtml(row.value)}</td></tr>`)
        .join('');
      scanRowsEl.querySelectorAll('tr[data-key]').forEach((row) => {
        row.addEventListener('click', () => selectRow(row.dataset.key, row.dataset.value));
      });
    }

    async function scan() {
      const prefix = scanPrefixEl.value;
      const limit = scanLimitEl.value || '100';
      const payload = await requestJson(`/api/scan?${encode({ prefix, limit })}`);
      renderRows(payload.rows);
    }

    function exportRows() {
      if (lastRows.length === 0) {
        return;
      }
      const payload = JSON.stringify({
        exported_at: new Date().toISOString(),
        prefix: scanPrefixEl.value,
        rows: lastRows,
      }, null, 2);
      const blob = new Blob([payload], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `kv-scan-${Date.now()}.json`;
      link.click();
      URL.revokeObjectURL(url);
      pushActivity('EXPORT', `${lastRows.length} rows`);
      setStatus('Export ready');
    }

    function parseImportPayload() {
      const parsed = JSON.parse(importJsonEl.value);
      const rows = Array.isArray(parsed) ? parsed : parsed.rows;
      if (!Array.isArray(rows)) {
        throw new Error('JSON must contain a rows array');
      }
      return rows.map((row) => ({
        key: String(row.key ?? ''),
        value: String(row.value ?? ''),
      })).filter((row) => row.key.length > 0);
    }

    async function importRows() {
      const rows = parseImportPayload();
      if (rows.length === 0) {
        throw new Error('No rows to import');
      }
      importBadgeEl.textContent = `${rows.length} rows`;
      importResultEl.className = 'result muted';
      importResultEl.textContent = `Importing ${rows.length} rows...`;
      const payload = await requestJson('/api/import', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ rows }),
      });
      if (!payload.success) {
        throw new Error(payload.message);
      }
      importBadgeEl.textContent = 'complete';
      importResultEl.className = 'result ok';
      importResultEl.textContent = payload.message;
      scanPrefixEl.value = rows[0].key.includes(':') ? `${rows[0].key.split(':')[0]}:` : '';
      await refreshAll(payload.message);
      pushActivity('IMPORT', `${payload.imported} rows`);
    }

    function updateBenchProgress(completed, total) {
      const percent = total === 0 ? 0 : Math.round((completed / total) * 100);
      benchProgressEl.style.width = `${percent}%`;
      benchBadgeEl.textContent = `${completed}/${total}`;
    }

    async function runBenchmark(totalOps, concurrency) {
      const total = Math.max(1, Math.min(2000, totalOps));
      const workers = Math.max(1, Math.min(64, concurrency, total));
      let cursor = 0;
      let completed = 0;
      let errors = 0;
      const startedAt = performance.now();
      const prefix = `bench:${Date.now()}:`;
      benchResultEl.className = 'result muted';
      benchResultEl.textContent = `Running ${total} ops with concurrency ${workers}...`;
      updateBenchProgress(0, total);

      async function worker() {
        while (true) {
          const index = cursor;
          cursor += 1;
          if (index >= total) {
            return;
          }
          const key = `${prefix}${index}`;
          const value = `value-${index}`;
          try {
            if (index > 0 && index % 4 === 0) {
              const readKey = `${prefix}${Math.floor(Math.random() * index)}`;
              await requestJson(`/api/get?${encode({ key: readKey })}`);
            } else {
              await requestJson(`/api/put?${encode({ key, value })}`, { method: 'POST' });
            }
          } catch (_error) {
            errors += 1;
          } finally {
            completed += 1;
            updateBenchProgress(completed, total);
          }
        }
      }

      await Promise.all(Array.from({ length: workers }, () => worker()));
      const elapsedMs = performance.now() - startedAt;
      const opsPerSecond = Math.round((completed / Math.max(1, elapsedMs)) * 1000);
      const avgLatency = elapsedMs / completed;
      benchThroughputEl.textContent = `${opsPerSecond}/s`;
      benchLatencyEl.textContent = `${avgLatency.toFixed(1)}ms`;
      benchErrorsEl.textContent = String(errors);
      benchBadgeEl.textContent = errors === 0 ? 'complete' : 'errors';
      benchResultEl.className = errors === 0 ? 'result ok' : 'result err';
      benchResultEl.textContent = `${completed} ops in ${(elapsedMs / 1000).toFixed(2)}s`;
      scanPrefixEl.value = 'bench:';
      await refreshAll('Benchmark complete');
      pushActivity('BENCH', `${opsPerSecond}/s, ${errors} errors`);
    }

    function selectRow(key, value) {
      document.querySelector('#get-key').value = key;
      document.querySelector('#delete-key').value = key;
      document.querySelector('#put-key').value = key;
      lookupBadgeEl.textContent = 'selected';
      lookupEl.className = 'result selected';
      lookupEl.innerHTML = `<b>${escapeHtml(key)}</b> = ${escapeHtml(value)}`;
      pushActivity('SELECT', key);
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
      const button = event.submitter;
      const key = document.querySelector('#put-key').value;
      const value = document.querySelector('#put-value').value;
      await withBusy(button, 'Putting', async () => {
        try {
          const payload = await requestJson(`/api/put?${encode({ key, value })}`, { method: 'POST' });
          scanPrefixEl.value = key.includes(':') ? `${key.split(':')[0]}:` : '';
          document.querySelector('#get-key').value = key;
          document.querySelector('#delete-key').value = key;
          await refreshAll(payload.message);
          pushActivity('PUT', `${key}=${value}`);
        } catch (error) {
          setStatus(error.message, 'err');
        }
      });
    });

    document.querySelector('#delete-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const button = event.submitter;
      const key = document.querySelector('#delete-key').value;
      await withBusy(button, 'Deleting', async () => {
        try {
          const payload = await requestJson(`/api/delete?${encode({ key })}`, { method: 'POST' });
          await refreshAll(payload.message);
          lookupBadgeEl.textContent = 'deleted';
          lookupEl.className = 'result muted';
          lookupEl.innerHTML = `<b>${escapeHtml(key)}</b> deleted`;
          pushActivity('DELETE', key);
        } catch (error) {
          setStatus(error.message, 'err');
        }
      });
    });

    document.querySelector('#get-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const button = event.submitter;
      const key = document.querySelector('#get-key').value;
      await withBusy(button, 'Getting', async () => {
        try {
          const payload = await requestJson(`/api/get?${encode({ key })}`);
          lookupBadgeEl.textContent = payload.found ? 'found' : 'missing';
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
    });

    document.querySelector('#scan-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const button = event.submitter;
      await withBusy(button, 'Scanning', async () => {
        try {
          await scan();
          pushActivity('SCAN', scanPrefixEl.value || '(all)');
          setStatus('Scan refreshed');
        } catch (error) {
          setStatus(error.message, 'err');
        }
      });
    });

    document.querySelectorAll('[data-prefix]').forEach((button) => {
      button.addEventListener('click', async () => {
        await withBusy(button, '...', async () => {
          try {
            scanPrefixEl.value = button.dataset.prefix;
            await scan();
            pushActivity('SCAN', scanPrefixEl.value || '(all)');
            setStatus('Scan refreshed');
          } catch (error) {
            setStatus(error.message, 'err');
          }
        });
      });
    });

    exportButtonEl.addEventListener('click', exportRows);

    importButtonEl.addEventListener('click', async () => {
      await withBusy(importButtonEl, 'Importing', async () => {
        try {
          await importRows();
        } catch (error) {
          importBadgeEl.textContent = 'error';
          importResultEl.className = 'result err';
          importResultEl.textContent = error.message;
          setStatus(error.message, 'err');
        }
      });
    });

    document.querySelector('#bench-form').addEventListener('submit', async (event) => {
      event.preventDefault();
      const button = event.submitter;
      const totalOps = Number(document.querySelector('#bench-ops').value || '100');
      const concurrency = Number(document.querySelector('#bench-concurrency').value || '8');
      await withBusy(button, 'Running', async () => {
        try {
          await runBenchmark(totalOps, concurrency);
        } catch (error) {
          benchBadgeEl.textContent = 'failed';
          benchResultEl.className = 'result err';
          benchResultEl.textContent = error.message;
          setStatus(error.message, 'err');
        }
      });
    });

    document.querySelector('#flush-button').addEventListener('click', async (event) => {
      await withBusy(event.currentTarget, 'Flushing', async () => {
        try {
          const payload = await requestJson('/api/flush', { method: 'POST' });
          await refreshAll(payload.message);
          pushActivity('FLUSH', payload.message);
        } catch (error) {
          setStatus(error.message, 'err');
        }
      });
    });

    document.querySelector('#compact-button').addEventListener('click', async (event) => {
      await withBusy(event.currentTarget, 'Compacting', async () => {
        try {
          const payload = await requestJson('/api/compact', { method: 'POST' });
          await refreshAll(payload.message);
          pushActivity('COMPACT', payload.message);
        } catch (error) {
          setStatus(error.message, 'err');
        }
      });
    });

    document.querySelector('#demo-button').addEventListener('click', async (event) => {
      await withBusy(event.currentTarget, 'Seeding', async () => {
        try {
          const payload = await requestJson('/api/demo', { method: 'POST' });
          scanPrefixEl.value = 'user:';
          await refreshAll(payload.message);
          pushActivity('DEMO', payload.message);
        } catch (error) {
          setStatus(error.message, 'err');
        }
      });
    });

    themeToggleEl.addEventListener('click', () => {
      applyTheme(document.body.dataset.theme === 'dark' ? 'light' : 'dark');
    });

    function connectWatchStream() {
      const events = new EventSource('/api/events');
      events.addEventListener('open', () => {
        watchCountEl.textContent = 'connected';
      });
      events.addEventListener('kv', (message) => {
        pushWatchEvent(JSON.parse(message.data));
      });
      events.addEventListener('error', () => {
        watchCountEl.textContent = 'reconnecting';
      });
    }

    applyTheme(localStorage.getItem('kv-dashboard-theme') || 'light');
    pushActivity('READY', 'dashboard connected');
    connectWatchStream();
    refreshAll().catch((error) => setStatus(error.message, 'err'));
    setInterval(() => refreshStats().catch((error) => setStatus(error.message, 'err')), 2000);
  </script>
</body>
</html>"##;
