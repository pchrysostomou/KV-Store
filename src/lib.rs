pub mod proto {
    tonic::include_proto!("kvstore.v1");
}

pub mod client;
pub mod raft;
pub mod server;
pub mod storage;

pub use storage::{StorageConfig, StorageEngine, StorageError};
