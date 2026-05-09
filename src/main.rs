use std::error::Error;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use kv_store::proto::kv_store_client::KvStoreClient;
use kv_store::proto::{
    CompactRequest, CompactResponse, DeleteRequest, FlushRequest, FlushResponse, GetRequest,
    PutRequest, ScanRequest, StatsRequest, StatsResponse, WatchEvent, WatchRequest,
};
use kv_store::server::grpc::{serve, ServerConfig};
use kv_store::storage::{CompactionReport, StorageConfig, StorageEngine, StorageStats};

#[derive(Debug, Parser)]
#[command(name = "kvctl")]
#[command(about = "CLI and gRPC server for the distributed KV store prototype")]
struct Cli {
    #[arg(long, default_value = ".kvstore", global = true)]
    data_dir: PathBuf,

    #[arg(long, default_value_t = 64 * 1024 * 1024, global = true)]
    memtable_flush_threshold: usize,

    #[arg(long, global = true)]
    endpoint: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Server {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,

        #[arg(long, default_value = "127.0.0.1:8080")]
        dashboard_addr: String,
    },
    Put {
        key: String,
        value: String,
    },
    Get {
        key: String,
    },
    Delete {
        key: String,
    },
    Scan {
        #[arg(long, default_value = "")]
        prefix: String,

        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    Flush,
    Compact,
    Stats,
    Watch {
        #[arg(long, default_value = "")]
        prefix: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let mut config = StorageConfig::new(cli.data_dir);
    config.memtable_flush_threshold = cli.memtable_flush_threshold;

    match cli.command {
        Commands::Server {
            addr,
            dashboard_addr,
        } => {
            println!("gRPC listening on {addr}");
            println!("dashboard open at http://{dashboard_addr}/");
            serve(
                ServerConfig {
                    listen_addr: addr,
                    dashboard_addr: Some(dashboard_addr),
                },
                config,
            )
            .await?;
        }
        command => {
            if let Some(endpoint) = cli.endpoint {
                run_remote_command(endpoint, command).await?;
            } else {
                run_local_command(config, command)?;
            }
        }
    }

    Ok(())
}

fn run_local_command(config: StorageConfig, command: Commands) -> Result<(), Box<dyn Error>> {
    let mut engine = StorageEngine::open(config)?;

    match command {
        Commands::Server { .. } => unreachable!("server command is handled before local dispatch"),
        Commands::Put { key, value } => {
            engine.put(key.as_bytes(), value.as_bytes())?;
            println!("OK");
        }
        Commands::Get { key } => print_get(engine.get(key.as_bytes())?),
        Commands::Delete { key } => {
            engine.delete(key.as_bytes())?;
            println!("OK");
        }
        Commands::Scan { prefix, limit } => {
            for (key, value) in engine.scan_prefix(prefix.as_bytes(), limit)? {
                print_scan_row(&key, &value);
            }
        }
        Commands::Flush => print_local_flush(engine.flush_memtable()?),
        Commands::Compact => print_local_compact(engine.compact_all()?),
        Commands::Stats => print_local_stats(engine.stats()),
        Commands::Watch { .. } => {
            return Err("watch requires --endpoint because it streams server-side events".into());
        }
    }

    Ok(())
}

async fn run_remote_command(endpoint: String, command: Commands) -> Result<(), Box<dyn Error>> {
    let mut client = KvStoreClient::connect(endpoint).await?;

    match command {
        Commands::Server { .. } => unreachable!("server command is handled before remote dispatch"),
        Commands::Put { key, value } => {
            client.put(PutRequest { key, value }).await?;
            println!("OK");
        }
        Commands::Get { key } => {
            let response = client.get(GetRequest { key }).await?.into_inner();
            if response.found {
                println!("{}", response.value);
            } else {
                println!("(not found)");
            }
        }
        Commands::Delete { key } => {
            client.delete(DeleteRequest { key }).await?;
            println!("OK");
        }
        Commands::Scan { prefix, limit } => {
            let mut stream = client
                .scan(ScanRequest {
                    prefix,
                    limit: limit as u32,
                })
                .await?
                .into_inner();

            while let Some(row) = stream.message().await? {
                println!("{}={}", row.key, row.value);
            }
        }
        Commands::Flush => {
            let response = client.flush(FlushRequest {}).await?.into_inner();
            print_remote_flush(response);
        }
        Commands::Compact => {
            let response = client.compact(CompactRequest {}).await?.into_inner();
            print_remote_compact(response);
        }
        Commands::Stats => {
            let response = client.stats(StatsRequest {}).await?.into_inner();
            print_remote_stats(response);
        }
        Commands::Watch { prefix } => {
            let mut stream = client.watch(WatchRequest { prefix }).await?.into_inner();
            while let Some(event) = stream.message().await? {
                let event_name =
                    match WatchEvent::try_from(event.event).unwrap_or(WatchEvent::Unspecified) {
                        WatchEvent::Put => "PUT",
                        WatchEvent::Delete => "DELETE",
                        WatchEvent::Unspecified => "UNKNOWN",
                    };
                if event.value.is_empty() {
                    println!("{event_name} {}", event.key);
                } else {
                    println!("{event_name} {}={}", event.key, event.value);
                }
            }
        }
    }

    Ok(())
}

fn print_get(value: Option<Vec<u8>>) {
    match value {
        Some(value) => println!("{}", String::from_utf8_lossy(&value)),
        None => println!("(not found)"),
    }
}

fn print_scan_row(key: &[u8], value: &[u8]) {
    println!(
        "{}={}",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(value)
    );
}

fn print_local_flush(metadata: Option<kv_store::storage::SstableMetadata>) {
    match metadata {
        Some(metadata) => {
            println!(
                "flushed sstable_id={} entries={}",
                metadata.id.0, metadata.entry_count
            );
        }
        None => println!("nothing to flush"),
    }
}

fn print_remote_flush(response: FlushResponse) {
    if response.flushed {
        println!(
            "flushed sstable_id={} entries={}",
            response.sstable_id, response.entries
        );
    } else {
        println!("nothing to flush");
    }
}

fn print_local_compact(report: CompactionReport) {
    println!("flushed_memtable={}", report.flushed_memtable);
    println!("input_sstables={}", report.input_sstables);
    println!("output_sstables={}", report.output_sstables);
    println!("input_entries={}", report.input_entries);
    println!("output_entries={}", report.output_entries);
    println!("dropped_entries={}", report.dropped_entries);
}

fn print_remote_compact(response: CompactResponse) {
    println!("flushed_memtable={}", response.flushed_memtable);
    println!("input_sstables={}", response.input_sstables);
    println!("output_sstables={}", response.output_sstables);
    println!("input_entries={}", response.input_entries);
    println!("output_entries={}", response.output_entries);
    println!("dropped_entries={}", response.dropped_entries);
}

fn print_local_stats(stats: StorageStats) {
    println!("memtable_entries={}", stats.memtable_entries);
    println!("memtable_bytes={}", stats.memtable_bytes);
    println!("flush_threshold_bytes={}", stats.flush_threshold_bytes);
    println!("should_flush={}", stats.should_flush);
    println!("sstable_count={}", stats.sstable_count);
    println!("bloom_filter_count={}", stats.bloom_filter_count);
    println!("next_sstable_id={}", stats.next_sstable_id);
}

fn print_remote_stats(stats: StatsResponse) {
    println!("memtable_entries={}", stats.memtable_entries);
    println!("memtable_bytes={}", stats.memtable_bytes);
    println!("flush_threshold_bytes={}", stats.flush_threshold_bytes);
    println!("should_flush={}", stats.should_flush);
    println!("sstable_count={}", stats.sstable_count);
    println!("bloom_filter_count={}", stats.bloom_filter_count);
    println!("next_sstable_id={}", stats.next_sstable_id);
}
