use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use super::compaction::CompactionReport;
use super::memtable::{MemTable, ValueRecord};
use super::sstable::{SstableId, SstableMetadata, SstableReader, SstableWriter};
use super::wal::{Wal, WalEntry, WalOp};
use super::{Result, StorageError};

const DEFAULT_MEMTABLE_FLUSH_THRESHOLD: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub memtable_flush_threshold: usize,
    pub sync_writes: bool,
}

impl StorageConfig {
    pub fn new<P>(data_dir: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            data_dir: data_dir.into(),
            memtable_flush_threshold: DEFAULT_MEMTABLE_FLUSH_THRESHOLD,
            sync_writes: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageStats {
    pub memtable_entries: usize,
    pub memtable_bytes: usize,
    pub flush_threshold_bytes: usize,
    pub should_flush: bool,
    pub sstable_count: usize,
    pub bloom_filter_count: usize,
    pub next_sstable_id: u64,
}

#[derive(Debug)]
pub struct StorageEngine {
    config: StorageConfig,
    memtable: MemTable,
    wal: Wal,
    sstables: Vec<SstableReader>,
    next_sstable_id: u64,
}

impl StorageEngine {
    pub fn open(config: StorageConfig) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;
        fs::create_dir_all(sstable_dir(&config.data_dir))?;

        let wal_path = active_wal_path(&config.data_dir);
        let sstables = load_sstables(&config.data_dir)?;
        let next_sstable_id = sstables
            .last()
            .map(|reader| reader.metadata().id.0 + 1)
            .unwrap_or(1);
        let entries = Wal::recover(&wal_path)?;
        let mut memtable = MemTable::new();

        for entry in entries {
            apply_wal_entry(&mut memtable, entry);
        }

        let wal = Wal::open(wal_path)?;

        Ok(Self {
            config,
            memtable,
            wal,
            sstables,
            next_sstable_id,
        })
    }

    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        validate_key(key.as_ref())?;

        let entry = WalEntry::put(key.as_ref(), value.as_ref());
        self.wal.append(&entry)?;
        if self.config.sync_writes {
            self.wal.sync()?;
        }

        self.memtable.put(entry.key, entry.value);
        if self
            .memtable
            .should_flush(self.config.memtable_flush_threshold)
        {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn delete<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        validate_key(key.as_ref())?;

        let entry = WalEntry::delete(key.as_ref());
        self.wal.append(&entry)?;
        if self.config.sync_writes {
            self.wal.sync()?;
        }

        self.memtable.delete(entry.key);
        if self
            .memtable
            .should_flush(self.config.memtable_flush_threshold)
        {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn get<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        validate_key(key.as_ref())?;

        if let Some(record) = self.memtable.get(key.as_ref()) {
            return Ok(match record {
                ValueRecord::Value(value) => Some(value.clone()),
                ValueRecord::Tombstone => None,
            });
        }

        for reader in self.sstables.iter().rev() {
            if let Some(record) = reader.get(key.as_ref())? {
                return Ok(match record {
                    ValueRecord::Value(value) => Some(value),
                    ValueRecord::Tombstone => None,
                });
            }
        }

        Ok(None)
    }

    pub fn scan_prefix<K>(&self, prefix: K, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        K: AsRef<[u8]>,
    {
        let prefix = prefix.as_ref();
        let mut merged = BTreeMap::new();

        for reader in &self.sstables {
            for (key, record) in reader.scan_prefix(prefix, usize::MAX)? {
                merged.insert(key, record);
            }
        }

        for (key, record) in self.memtable.scan_prefix(prefix, usize::MAX) {
            merged.insert(key, record);
        }

        Ok(merged
            .into_iter()
            .filter_map(|(key, record)| match record {
                ValueRecord::Value(value) => Some((key, value)),
                ValueRecord::Tombstone => None,
            })
            .take(limit)
            .collect())
    }

    pub fn flush_memtable(&mut self) -> Result<Option<SstableMetadata>> {
        if self.memtable.is_empty() {
            return Ok(None);
        }

        let id = SstableId(self.next_sstable_id);
        let entries = self.memtable.to_vec();
        let path = sstable_path(&self.config.data_dir, id);
        let metadata = SstableWriter::write(&path, id, &entries)?;
        let reader = SstableReader::open(&path)?;

        self.wal.reset()?;
        self.memtable.clear();
        self.sstables.push(reader);
        self.next_sstable_id += 1;

        Ok(Some(metadata))
    }

    pub fn compact_all(&mut self) -> Result<CompactionReport> {
        let flushed_memtable = self.flush_memtable()?.is_some();
        let input_sstables = self.sstables.len();

        if input_sstables == 0 {
            return Ok(CompactionReport {
                flushed_memtable,
                ..CompactionReport::empty()
            });
        }

        let input_entries = self
            .sstables
            .iter()
            .map(|reader| reader.metadata().entry_count)
            .sum();
        let old_paths = self
            .sstables
            .iter()
            .map(|reader| reader.path().to_path_buf())
            .collect::<Vec<_>>();
        let mut merged = BTreeMap::new();

        for reader in &self.sstables {
            for (key, record) in reader.entries()? {
                merged.insert(key, record);
            }
        }

        let output_entries = merged
            .into_iter()
            .filter(|(_, record)| matches!(record, ValueRecord::Value(_)))
            .collect::<Vec<_>>();
        let output_entry_count = output_entries.len() as u64;

        self.sstables.clear();

        if output_entries.is_empty() {
            for path in old_paths {
                remove_file_if_exists(path)?;
            }

            return Ok(CompactionReport {
                flushed_memtable,
                input_sstables,
                output_sstables: 0,
                input_entries,
                output_entries: 0,
                dropped_entries: input_entries,
            });
        }

        let id = SstableId(self.next_sstable_id);
        let path = sstable_path(&self.config.data_dir, id);
        SstableWriter::write(&path, id, &output_entries)?;
        let reader = SstableReader::open(&path)?;

        for old_path in old_paths {
            remove_file_if_exists(old_path)?;
        }

        self.sstables.push(reader);
        self.next_sstable_id += 1;

        Ok(CompactionReport {
            flushed_memtable,
            input_sstables,
            output_sstables: 1,
            input_entries,
            output_entries: output_entry_count,
            dropped_entries: input_entries.saturating_sub(output_entry_count),
        })
    }

    pub fn stats(&self) -> StorageStats {
        let memtable_bytes = self.memtable.approximate_bytes();
        StorageStats {
            memtable_entries: self.memtable.len(),
            memtable_bytes,
            flush_threshold_bytes: self.config.memtable_flush_threshold,
            should_flush: memtable_bytes >= self.config.memtable_flush_threshold,
            sstable_count: self.sstables.len(),
            bloom_filter_count: self
                .sstables
                .iter()
                .filter(|reader| reader.has_bloom_filter())
                .count(),
            next_sstable_id: self.next_sstable_id,
        }
    }
}

fn active_wal_path(data_dir: &Path) -> PathBuf {
    data_dir.join("active.wal")
}

fn sstable_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("sstables")
}

fn sstable_path(data_dir: &Path, id: SstableId) -> PathBuf {
    sstable_dir(data_dir).join(format!("{:020}.sst", id.0))
}

fn load_sstables(data_dir: &Path) -> Result<Vec<SstableReader>> {
    let dir = sstable_dir(data_dir);
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut readers = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension() != Some(OsStr::new("sst")) {
            continue;
        }

        readers.push(SstableReader::open(path)?);
    }

    readers.sort_by_key(|reader| reader.metadata().id);
    Ok(readers)
}

fn remove_file_if_exists(path: PathBuf) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(StorageError::InvalidInput(
            "keys must not be empty".to_string(),
        ));
    }

    Ok(())
}

fn apply_wal_entry(memtable: &mut MemTable, entry: WalEntry) {
    match entry.op {
        WalOp::Put => memtable.put(entry.key, entry.value),
        WalOp::Delete => memtable.delete(entry.key),
    }
}
