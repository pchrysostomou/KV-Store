use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueRecord {
    Value(Vec<u8>),
    Tombstone,
}

impl ValueRecord {
    pub fn as_value(&self) -> Option<&[u8]> {
        match self {
            Self::Value(value) => Some(value),
            Self::Tombstone => None,
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::Value(value) => value.len(),
            Self::Tombstone => 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct MemTable {
    entries: BTreeMap<Vec<u8>, ValueRecord>,
    approximate_bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.insert(
            key.as_ref().to_vec(),
            ValueRecord::Value(value.as_ref().to_vec()),
        );
    }

    pub fn delete<K>(&mut self, key: K)
    where
        K: AsRef<[u8]>,
    {
        self.insert(key.as_ref().to_vec(), ValueRecord::Tombstone);
    }

    pub fn get<K>(&self, key: K) -> Option<&ValueRecord>
    where
        K: AsRef<[u8]>,
    {
        self.entries.get(key.as_ref())
    }

    pub fn get_value<K>(&self, key: K) -> Option<&[u8]>
    where
        K: AsRef<[u8]>,
    {
        self.get(key).and_then(ValueRecord::as_value)
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&[u8], &ValueRecord)> {
        self.entries
            .iter()
            .map(|(key, record)| (key.as_slice(), record))
    }

    pub fn to_vec(&self) -> Vec<(Vec<u8>, ValueRecord)> {
        self.entries
            .iter()
            .map(|(key, record)| (key.clone(), record.clone()))
            .collect()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.approximate_bytes = 0;
    }

    pub fn scan_prefix<K>(&self, prefix: K, limit: usize) -> Vec<(Vec<u8>, ValueRecord)>
    where
        K: AsRef<[u8]>,
    {
        let prefix = prefix.as_ref();

        self.entries
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, record)| (key.clone(), record.clone()))
            .take(limit)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn approximate_bytes(&self) -> usize {
        self.approximate_bytes
    }

    pub fn should_flush(&self, threshold_bytes: usize) -> bool {
        self.approximate_bytes >= threshold_bytes
    }

    fn insert(&mut self, key: Vec<u8>, record: ValueRecord) {
        let new_size = entry_size(&key, &record);
        let old_size = self
            .entries
            .get(key.as_slice())
            .map(|old_record| entry_size(&key, old_record))
            .unwrap_or(0);

        self.entries.insert(key, record);
        self.approximate_bytes += new_size;
        self.approximate_bytes = self.approximate_bytes.saturating_sub(old_size);
    }
}

fn entry_size(key: &[u8], record: &ValueRecord) -> usize {
    key.len() + record.heap_bytes() + 1
}
