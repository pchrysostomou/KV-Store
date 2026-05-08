use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::bloom::BloomFilter;
use super::memtable::ValueRecord;
use super::{Result, StorageError};

const MAGIC_V1: u64 = 0x4b_56_53_53_54_41_42_4c;
const MAGIC_V2: u64 = 0x4b_56_53_53_54_42_4c_32;
const FOOTER_V1_LEN: u64 = 16;
const FOOTER_V2_LEN: u64 = 24;
const VALUE: u8 = 1;
const TOMBSTONE: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SstableId(pub u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SstableMetadata {
    pub id: SstableId,
    pub entry_count: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
}

#[derive(Clone, Debug)]
struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
}

#[derive(Debug)]
pub struct SstableWriter;

impl SstableWriter {
    pub fn write<P>(
        path: P,
        id: SstableId,
        entries: &[(Vec<u8>, ValueRecord)],
    ) -> Result<SstableMetadata>
    where
        P: AsRef<Path>,
    {
        if entries.is_empty() {
            return Err(StorageError::InvalidInput(
                "cannot write an empty SSTable".to_string(),
            ));
        }
        validate_sorted_entries(entries)?;

        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp_path = path.with_extension("sst.tmp");
        let mut file = File::create(&tmp_path)?;
        let mut index = Vec::with_capacity(entries.len());
        let mut bloom_filter = BloomFilter::new(entries.len());

        for (key, record) in entries {
            let offset = file.stream_position()?;
            write_data_record(&mut file, key, record)?;
            bloom_filter.insert(key);
            index.push(IndexEntry {
                key: key.clone(),
                offset,
            });
        }

        let index_offset = file.stream_position()?;
        write_index_block(&mut file, &index)?;
        let bloom_filter_offset = file.stream_position()?;
        write_bloom_filter_block(&mut file, &bloom_filter)?;
        file.write_all(&index_offset.to_le_bytes())?;
        file.write_all(&bloom_filter_offset.to_le_bytes())?;
        file.write_all(&MAGIC_V2.to_le_bytes())?;
        file.sync_all()?;
        drop(file);

        fs::rename(&tmp_path, path)?;

        Ok(SstableMetadata {
            id,
            entry_count: entries.len() as u64,
            smallest_key: entries[0].0.clone(),
            largest_key: entries[entries.len() - 1].0.clone(),
        })
    }
}

#[derive(Debug)]
pub struct SstableReader {
    path: PathBuf,
    metadata: SstableMetadata,
    index: Vec<IndexEntry>,
    bloom_filter: Option<BloomFilter>,
}

impl SstableReader {
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();
        let id = sstable_id_from_path(&path)?;
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        let footer = read_footer(&path, &mut file, file_len)?;

        if footer.index_offset >= file_len - footer.footer_len {
            return Err(StorageError::CorruptSstable(format!(
                "{} has an invalid index offset",
                path.display()
            )));
        }

        if let Some(bloom_filter_offset) = footer.bloom_filter_offset {
            if bloom_filter_offset <= footer.index_offset
                || bloom_filter_offset >= file_len - footer.footer_len
            {
                return Err(StorageError::CorruptSstable(format!(
                    "{} has an invalid Bloom filter offset",
                    path.display()
                )));
            }
        }

        file.seek(SeekFrom::Start(footer.index_offset))?;
        let index = read_index_block(&mut file)?;

        if index.is_empty() {
            return Err(StorageError::CorruptSstable(format!(
                "{} contains an empty index",
                path.display()
            )));
        }

        let metadata = SstableMetadata {
            id,
            entry_count: index.len() as u64,
            smallest_key: index[0].key.clone(),
            largest_key: index[index.len() - 1].key.clone(),
        };
        let bloom_filter = match footer.bloom_filter_offset {
            Some(offset) => {
                file.seek(SeekFrom::Start(offset))?;
                Some(read_bloom_filter_block(&mut file)?)
            }
            None => None,
        };

        Ok(Self {
            path,
            metadata,
            index,
            bloom_filter,
        })
    }

    pub fn metadata(&self) -> &SstableMetadata {
        &self.metadata
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn has_bloom_filter(&self) -> bool {
        self.bloom_filter.is_some()
    }

    pub fn might_contain<K>(&self, key: K) -> bool
    where
        K: AsRef<[u8]>,
    {
        self.bloom_filter
            .as_ref()
            .is_none_or(|filter| filter.might_contain(key))
    }

    pub fn get<K>(&self, key: K) -> Result<Option<ValueRecord>>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        if key < self.metadata.smallest_key.as_slice() || key > self.metadata.largest_key.as_slice()
        {
            return Ok(None);
        }
        if !self.might_contain(key) {
            return Ok(None);
        }

        let Ok(index) = self
            .index
            .binary_search_by(|entry| entry.key.as_slice().cmp(key))
        else {
            return Ok(None);
        };

        let record = self.read_record_at(self.index[index].offset)?;
        Ok(Some(record.record))
    }

    pub fn scan_prefix<K>(&self, prefix: K, limit: usize) -> Result<Vec<(Vec<u8>, ValueRecord)>>
    where
        K: AsRef<[u8]>,
    {
        let prefix = prefix.as_ref();
        let mut rows = Vec::new();
        let mut index = lower_bound(&self.index, prefix);

        while index < self.index.len() && rows.len() < limit {
            if !self.index[index].key.starts_with(prefix) {
                break;
            }

            let data_record = self.read_record_at(self.index[index].offset)?;
            rows.push((data_record.key, data_record.record));
            index += 1;
        }

        Ok(rows)
    }

    pub fn entries(&self) -> Result<Vec<(Vec<u8>, ValueRecord)>> {
        let mut rows = Vec::with_capacity(self.index.len());

        for entry in &self.index {
            let data_record = self.read_record_at(entry.offset)?;
            rows.push((data_record.key, data_record.record));
        }

        Ok(rows)
    }

    fn read_record_at(&self, offset: u64) -> Result<DataRecord> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        read_data_record(&mut file)
    }
}

#[derive(Debug)]
struct SstableFooter {
    index_offset: u64,
    bloom_filter_offset: Option<u64>,
    footer_len: u64,
}

#[derive(Debug)]
struct DataRecord {
    key: Vec<u8>,
    record: ValueRecord,
}

fn validate_sorted_entries(entries: &[(Vec<u8>, ValueRecord)]) -> Result<()> {
    for pair in entries.windows(2) {
        if pair[0].0 >= pair[1].0 {
            return Err(StorageError::InvalidInput(
                "SSTable entries must be strictly sorted by key".to_string(),
            ));
        }
    }

    Ok(())
}

fn read_footer(path: &Path, file: &mut File, file_len: u64) -> Result<SstableFooter> {
    if file_len < FOOTER_V1_LEN {
        return Err(StorageError::CorruptSstable(format!(
            "{} is too small to contain a footer",
            path.display()
        )));
    }

    file.seek(SeekFrom::End(-8))?;
    let magic = read_u64(file)?;

    match magic {
        MAGIC_V2 => {
            if file_len < FOOTER_V2_LEN {
                return Err(StorageError::CorruptSstable(format!(
                    "{} is too small to contain a v2 footer",
                    path.display()
                )));
            }

            file.seek(SeekFrom::End(-(FOOTER_V2_LEN as i64)))?;
            let index_offset = read_u64(file)?;
            let bloom_filter_offset = read_u64(file)?;
            let footer_magic = read_u64(file)?;

            if footer_magic != MAGIC_V2 {
                return Err(StorageError::CorruptSstable(format!(
                    "{} has a torn v2 footer",
                    path.display()
                )));
            }

            Ok(SstableFooter {
                index_offset,
                bloom_filter_offset: Some(bloom_filter_offset),
                footer_len: FOOTER_V2_LEN,
            })
        }
        MAGIC_V1 => {
            file.seek(SeekFrom::End(-(FOOTER_V1_LEN as i64)))?;
            let index_offset = read_u64(file)?;
            let footer_magic = read_u64(file)?;

            if footer_magic != MAGIC_V1 {
                return Err(StorageError::CorruptSstable(format!(
                    "{} has a torn v1 footer",
                    path.display()
                )));
            }

            Ok(SstableFooter {
                index_offset,
                bloom_filter_offset: None,
                footer_len: FOOTER_V1_LEN,
            })
        }
        _ => Err(StorageError::CorruptSstable(format!(
            "{} has an invalid magic number",
            path.display()
        ))),
    }
}

fn write_data_record(file: &mut File, key: &[u8], record: &ValueRecord) -> Result<()> {
    if key.is_empty() {
        return Err(StorageError::InvalidInput(
            "SSTable keys must not be empty".to_string(),
        ));
    }

    let key_len = u32::try_from(key.len()).map_err(|_| {
        StorageError::InvalidInput("key is too large to encode in SSTable".to_string())
    })?;
    let value = record.as_value().unwrap_or(&[]);
    let value_len = u32::try_from(value.len()).map_err(|_| {
        StorageError::InvalidInput("value is too large to encode in SSTable".to_string())
    })?;
    let kind = match record {
        ValueRecord::Value(_) => VALUE,
        ValueRecord::Tombstone => TOMBSTONE,
    };

    file.write_all(&key_len.to_le_bytes())?;
    file.write_all(&value_len.to_le_bytes())?;
    file.write_all(&[kind])?;
    file.write_all(key)?;
    file.write_all(value)?;

    Ok(())
}

fn read_data_record(file: &mut File) -> Result<DataRecord> {
    let key_len = read_u32(file)? as usize;
    let value_len = read_u32(file)? as usize;
    let mut kind = [0; 1];
    file.read_exact(&mut kind)?;

    if key_len == 0 {
        return Err(StorageError::CorruptSstable(
            "data record contains an empty key".to_string(),
        ));
    }

    let mut key = vec![0; key_len];
    let mut value = vec![0; value_len];
    file.read_exact(&mut key)?;
    file.read_exact(&mut value)?;

    let record = match kind[0] {
        VALUE => ValueRecord::Value(value),
        TOMBSTONE if value_len == 0 => ValueRecord::Tombstone,
        TOMBSTONE => {
            return Err(StorageError::CorruptSstable(
                "tombstone data record contains a value".to_string(),
            ))
        }
        unknown => {
            return Err(StorageError::CorruptSstable(format!(
                "unknown SSTable record kind {unknown}"
            )))
        }
    };

    Ok(DataRecord { key, record })
}

fn write_bloom_filter_block(file: &mut File, bloom_filter: &BloomFilter) -> Result<()> {
    let bytes_len = u32::try_from(bloom_filter.bits().len()).map_err(|_| {
        StorageError::InvalidInput("Bloom filter is too large to encode".to_string())
    })?;

    file.write_all(&bloom_filter.bit_len().to_le_bytes())?;
    file.write_all(&bloom_filter.hash_count().to_le_bytes())?;
    file.write_all(&bytes_len.to_le_bytes())?;
    file.write_all(bloom_filter.bits())?;

    Ok(())
}

fn read_bloom_filter_block(file: &mut File) -> Result<BloomFilter> {
    let bit_len = read_u64(file)?;
    let hash_count = read_u32(file)?;
    let bytes_len = read_u32(file)? as usize;
    let mut bits = vec![0; bytes_len];
    file.read_exact(&mut bits)?;

    BloomFilter::from_parts(bit_len, hash_count, bits)
        .ok_or_else(|| StorageError::CorruptSstable("invalid Bloom filter block".to_string()))
}

fn write_index_block(file: &mut File, index: &[IndexEntry]) -> Result<()> {
    let entry_count = u32::try_from(index.len())
        .map_err(|_| StorageError::InvalidInput("too many SSTable index entries".to_string()))?;
    file.write_all(&entry_count.to_le_bytes())?;

    for entry in index {
        let key_len = u32::try_from(entry.key.len()).map_err(|_| {
            StorageError::InvalidInput("key is too large to encode in SSTable index".to_string())
        })?;
        file.write_all(&key_len.to_le_bytes())?;
        file.write_all(&entry.key)?;
        file.write_all(&entry.offset.to_le_bytes())?;
    }

    Ok(())
}

fn read_index_block(file: &mut File) -> Result<Vec<IndexEntry>> {
    let entry_count = read_u32(file)? as usize;
    let mut index = Vec::with_capacity(entry_count);
    let mut previous_key: Option<Vec<u8>> = None;

    for _ in 0..entry_count {
        let key_len = read_u32(file)? as usize;
        if key_len == 0 {
            return Err(StorageError::CorruptSstable(
                "index entry contains an empty key".to_string(),
            ));
        }

        let mut key = vec![0; key_len];
        file.read_exact(&mut key)?;
        let offset = read_u64(file)?;

        if let Some(previous_key) = previous_key.as_ref() {
            if previous_key >= &key {
                return Err(StorageError::CorruptSstable(
                    "index keys are not strictly sorted".to_string(),
                ));
            }
        }

        previous_key = Some(key.clone());
        index.push(IndexEntry { key, offset });
    }

    Ok(index)
}

fn lower_bound(index: &[IndexEntry], key: &[u8]) -> usize {
    let mut left = 0;
    let mut right = index.len();

    while left < right {
        let mid = left + (right - left) / 2;
        match index[mid].key.as_slice().cmp(key) {
            Ordering::Less => left = mid + 1,
            Ordering::Equal | Ordering::Greater => right = mid,
        }
    }

    left
}

fn sstable_id_from_path(path: &Path) -> Result<SstableId> {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| {
            StorageError::CorruptSstable(format!("invalid SSTable path {}", path.display()))
        })?;

    let id = stem.parse::<u64>().map_err(|_| {
        StorageError::CorruptSstable(format!("invalid SSTable id in {}", path.display()))
    })?;

    Ok(SstableId(id))
}

fn read_u32(file: &mut File) -> Result<u32> {
    let mut bytes = [0; 4];
    file.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(file: &mut File) -> Result<u64> {
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}
