use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use super::{Result, StorageError};

const MAX_RECORD_BYTES: usize = 128 * 1024 * 1024;
const PUT: u8 = 1;
const DELETE: u8 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WalOp {
    Put,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalEntry {
    pub op: WalOp,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl WalEntry {
    pub fn put<K, V>(key: K, value: V) -> Self
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        Self {
            op: WalOp::Put,
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        }
    }

    pub fn delete<K>(key: K) -> Self
    where
        K: AsRef<[u8]>,
    {
        Self {
            op: WalOp::Delete,
            key: key.as_ref().to_vec(),
            value: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct Wal {
    file: File,
    path: PathBuf,
}

impl Wal {
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(Self { file, path })
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<u64> {
        let payload = encode_entry(entry)?;
        let length = u32::try_from(payload.len()).map_err(|_| {
            StorageError::InvalidInput("WAL record is too large to encode".to_string())
        })?;
        let checksum = checksum(&payload);
        let offset = self.file.seek(SeekFrom::End(0))?;

        self.file.write_all(&length.to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.write_all(&checksum.to_le_bytes())?;
        self.file.flush()?;

        Ok(offset)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn recover<P>(path: P) -> Result<Vec<WalEntry>>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(path)?;
        let mut entries = Vec::new();

        while let Some(length) = read_u32_or_eof(&mut file)? {
            let length = length as usize;
            if length > MAX_RECORD_BYTES {
                return Err(StorageError::CorruptWal(format!(
                    "record length {length} exceeds max {MAX_RECORD_BYTES}"
                )));
            }

            let mut payload = vec![0; length];
            if !read_exact_or_trailing_eof(&mut file, &mut payload)? {
                break;
            }

            let Some(expected_checksum) = read_u32_or_eof(&mut file)? else {
                break;
            };

            let actual_checksum = checksum(&payload);
            if actual_checksum != expected_checksum {
                return Err(StorageError::CorruptWal(format!(
                    "checksum mismatch: expected {expected_checksum}, got {actual_checksum}"
                )));
            }

            entries.push(decode_entry(&payload)?);
        }

        Ok(entries)
    }
}

fn encode_entry(entry: &WalEntry) -> Result<Vec<u8>> {
    if entry.key.is_empty() {
        return Err(StorageError::InvalidInput(
            "WAL entries require a non-empty key".to_string(),
        ));
    }

    let op = match entry.op {
        WalOp::Put => PUT,
        WalOp::Delete => DELETE,
    };

    let value = match entry.op {
        WalOp::Put => entry.value.as_slice(),
        WalOp::Delete => &[],
    };

    let key_len = u32::try_from(entry.key.len())
        .map_err(|_| StorageError::InvalidInput("key is too large to encode in WAL".to_string()))?;
    let value_len = u32::try_from(value.len()).map_err(|_| {
        StorageError::InvalidInput("value is too large to encode in WAL".to_string())
    })?;

    let mut payload = Vec::with_capacity(1 + 4 + 4 + entry.key.len() + value.len());
    payload.push(op);
    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(&value_len.to_le_bytes());
    payload.extend_from_slice(&entry.key);
    payload.extend_from_slice(value);

    Ok(payload)
}

fn decode_entry(payload: &[u8]) -> Result<WalEntry> {
    if payload.len() < 9 {
        return Err(StorageError::CorruptWal(
            "record is too short to contain header".to_string(),
        ));
    }

    let op = match payload[0] {
        PUT => WalOp::Put,
        DELETE => WalOp::Delete,
        unknown => {
            return Err(StorageError::CorruptWal(format!(
                "unknown operation code {unknown}"
            )))
        }
    };

    let key_len = read_u32_from_payload(payload, 1) as usize;
    let value_len = read_u32_from_payload(payload, 5) as usize;
    let expected_len = 9usize
        .checked_add(key_len)
        .and_then(|length| length.checked_add(value_len))
        .ok_or_else(|| StorageError::CorruptWal("record length overflow".to_string()))?;

    if payload.len() != expected_len {
        return Err(StorageError::CorruptWal(format!(
            "record length {} does not match encoded key/value lengths {expected_len}",
            payload.len()
        )));
    }

    if key_len == 0 {
        return Err(StorageError::CorruptWal(
            "record contains an empty key".to_string(),
        ));
    }

    if op == WalOp::Delete && value_len != 0 {
        return Err(StorageError::CorruptWal(
            "delete record contains a value".to_string(),
        ));
    }

    let key_start = 9;
    let key_end = key_start + key_len;
    let value_end = key_end + value_len;

    Ok(WalEntry {
        op,
        key: payload[key_start..key_end].to_vec(),
        value: payload[key_end..value_end].to_vec(),
    })
}

fn checksum(payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(payload);
    hasher.finalize()
}

fn read_u32_from_payload(payload: &[u8], start: usize) -> u32 {
    let mut bytes = [0; 4];
    bytes.copy_from_slice(&payload[start..start + 4]);
    u32::from_le_bytes(bytes)
}

fn read_u32_or_eof(file: &mut File) -> io::Result<Option<u32>> {
    let mut buf = [0; 4];
    match file.read_exact(&mut buf) {
        Ok(()) => Ok(Some(u32::from_le_bytes(buf))),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_exact_or_trailing_eof(file: &mut File, buf: &mut [u8]) -> io::Result<bool> {
    match file.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}
