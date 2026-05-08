pub mod bloom;
pub mod compaction;
mod engine;
pub mod memtable;
pub mod sstable;
pub mod wal;

use std::io;

pub use bloom::BloomFilter;
pub use compaction::{CompactionConfig, CompactionReport};
pub use engine::{StorageConfig, StorageEngine, StorageStats};
pub use memtable::{MemTable, ValueRecord};
pub use sstable::{SstableId, SstableMetadata, SstableReader, SstableWriter};
pub use wal::{Wal, WalEntry, WalOp};

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("corrupt WAL: {0}")]
    CorruptWal(String),

    #[error("corrupt SSTable: {0}")]
    CorruptSstable(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),
}
