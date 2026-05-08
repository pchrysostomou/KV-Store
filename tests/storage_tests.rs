use std::fs;

use kv_store::storage::memtable::ValueRecord;
use kv_store::storage::{
    BloomFilter, MemTable, SstableId, SstableReader, SstableWriter, StorageConfig, StorageEngine,
    Wal, WalEntry, WalOp,
};

#[test]
fn memtable_put_get_delete() {
    let mut memtable = MemTable::new();

    memtable.put("alpha", "one");
    memtable.put("beta", "two");

    assert_eq!(memtable.get_value("alpha"), Some("one".as_bytes()));
    assert_eq!(memtable.get_value("beta"), Some("two".as_bytes()));
    assert_eq!(memtable.get_value("missing"), None);

    memtable.delete("alpha");
    assert_eq!(memtable.get("alpha"), Some(&ValueRecord::Tombstone));
    assert_eq!(memtable.get_value("alpha"), None);
}

#[test]
fn memtable_scan_prefix_is_sorted_and_limited() {
    let mut memtable = MemTable::new();

    memtable.put("user:2", "b");
    memtable.put("order:1", "ignored");
    memtable.put("user:1", "a");
    memtable.put("user:3", "c");

    let rows = memtable.scan_prefix("user:", 2);
    let keys: Vec<_> = rows.into_iter().map(|(key, _)| key).collect();

    assert_eq!(keys, vec![b"user:1".to_vec(), b"user:2".to_vec()]);
}

#[test]
fn wal_recovers_puts_and_deletes_in_order() {
    let temp = tempfile::tempdir().expect("tempdir");
    let wal_path = temp.path().join("active.wal");

    {
        let mut wal = Wal::open(&wal_path).expect("open wal");
        wal.append(&WalEntry::put("alpha", "one"))
            .expect("append put");
        wal.append(&WalEntry::delete("alpha"))
            .expect("append delete");
        wal.sync().expect("sync wal");
    }

    let entries = Wal::recover(&wal_path).expect("recover wal");

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], WalEntry::put("alpha", "one"));
    assert_eq!(entries[1].op, WalOp::Delete);
    assert_eq!(entries[1].key, b"alpha".to_vec());
}

#[test]
fn bloom_filter_accepts_inserted_keys_and_rejects_some_missing_key() {
    let mut filter = BloomFilter::new(3);

    filter.insert("alpha");
    filter.insert("beta");
    filter.insert("gamma");

    assert!(filter.might_contain("alpha"));
    assert!(filter.might_contain("beta"));
    assert!(filter.might_contain("gamma"));

    let rejected_key = (0..1_000)
        .map(|index| format!("missing:{index}"))
        .find(|key| !filter.might_contain(key.as_bytes()))
        .expect("expected at least one non-member to be rejected");

    assert!(!filter.might_contain(rejected_key.as_bytes()));
}

#[test]
fn storage_engine_recovers_memtable_from_wal() {
    let temp = tempfile::tempdir().expect("tempdir");

    {
        let mut engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("open engine");
        engine.put("alpha", "one").expect("put alpha");
        engine.put("beta", "two").expect("put beta");
        engine.delete("alpha").expect("delete alpha");
    }

    let engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("reopen engine");

    assert_eq!(engine.get("alpha").expect("get alpha"), None);
    assert_eq!(engine.get("beta").expect("get beta"), Some(b"two".to_vec()));
}

#[test]
fn sstable_writer_reader_get_and_scan() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("00000000000000000001.sst");
    let entries = vec![
        (b"order:1".to_vec(), ValueRecord::Value(b"ignored".to_vec())),
        (b"user:1".to_vec(), ValueRecord::Value(b"alice".to_vec())),
        (b"user:2".to_vec(), ValueRecord::Value(b"bob".to_vec())),
    ];

    SstableWriter::write(&path, SstableId(1), &entries).expect("write sstable");
    let reader = SstableReader::open(&path).expect("open sstable");

    assert!(reader.has_bloom_filter());
    assert!(reader.might_contain("user:1"));

    let rejected_key = (0..1_000)
        .map(|index| format!("missing:{index}"))
        .find(|key| !reader.might_contain(key.as_bytes()))
        .expect("expected Bloom filter to reject at least one non-member");
    assert_eq!(
        reader
            .get(rejected_key.as_bytes())
            .expect("get rejected key"),
        None
    );

    assert_eq!(
        reader.get("user:1").expect("get user:1"),
        Some(ValueRecord::Value(b"alice".to_vec()))
    );
    assert_eq!(reader.get("missing").expect("get missing"), None);

    let rows = reader.scan_prefix("user:", 10).expect("scan prefix");
    let keys: Vec<_> = rows.into_iter().map(|(key, _)| key).collect();

    assert_eq!(keys, vec![b"user:1".to_vec(), b"user:2".to_vec()]);
}

#[test]
fn storage_engine_flushes_to_sstable_and_recovers() {
    let temp = tempfile::tempdir().expect("tempdir");

    {
        let mut engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("open engine");
        engine.put("alpha", "one").expect("put alpha");
        engine.put("beta", "two").expect("put beta");
        let metadata = engine.flush_memtable().expect("flush").expect("metadata");

        assert_eq!(metadata.entry_count, 2);
        assert_eq!(engine.stats().memtable_entries, 0);
        assert_eq!(engine.stats().sstable_count, 1);
        assert_eq!(engine.stats().bloom_filter_count, 1);
    }

    let engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("reopen engine");

    assert_eq!(
        engine.get("alpha").expect("get alpha"),
        Some(b"one".to_vec())
    );
    assert_eq!(engine.get("beta").expect("get beta"), Some(b"two".to_vec()));
    assert_eq!(engine.stats().sstable_count, 1);
}

#[test]
fn newest_sstable_tombstone_hides_older_value() {
    let temp = tempfile::tempdir().expect("tempdir");

    {
        let mut engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("open engine");
        engine.put("alpha", "one").expect("put alpha");
        engine.flush_memtable().expect("flush value");
        engine.delete("alpha").expect("delete alpha");
        engine.flush_memtable().expect("flush tombstone");
    }

    let engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("reopen engine");

    assert_eq!(engine.get("alpha").expect("get alpha"), None);
    assert_eq!(engine.stats().sstable_count, 2);
}

#[test]
fn scan_merges_sstables_and_tombstones() {
    let temp = tempfile::tempdir().expect("tempdir");

    let mut engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("open engine");
    engine.put("user:1", "alice").expect("put user 1");
    engine.flush_memtable().expect("flush user 1");
    engine.put("user:2", "bob").expect("put user 2");
    engine.delete("user:1").expect("delete user 1");
    engine.flush_memtable().expect("flush user 2");

    let rows = engine.scan_prefix("user:", 10).expect("scan users");

    assert_eq!(rows, vec![(b"user:2".to_vec(), b"bob".to_vec())]);
}

#[test]
fn compaction_collapses_sstables_and_drops_old_versions() {
    let temp = tempfile::tempdir().expect("tempdir");

    {
        let mut engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("open engine");
        engine.put("user:1", "alice-v1").expect("put user 1 v1");
        engine.flush_memtable().expect("flush user 1 v1");

        engine.put("user:1", "alice-v2").expect("put user 1 v2");
        engine.put("user:2", "bob").expect("put user 2");
        engine.flush_memtable().expect("flush user 1 v2");

        engine.delete("user:2").expect("delete user 2");
        engine.flush_memtable().expect("flush user 2 tombstone");

        assert_eq!(engine.stats().sstable_count, 3);

        let report = engine.compact_all().expect("compact");

        assert!(!report.flushed_memtable);
        assert_eq!(report.input_sstables, 3);
        assert_eq!(report.output_sstables, 1);
        assert_eq!(report.input_entries, 4);
        assert_eq!(report.output_entries, 1);
        assert_eq!(report.dropped_entries, 3);
        assert_eq!(engine.stats().sstable_count, 1);
        assert_eq!(
            engine.get("user:1").expect("get user 1"),
            Some(b"alice-v2".to_vec())
        );
        assert_eq!(engine.get("user:2").expect("get user 2"), None);
    }

    assert_eq!(sstable_file_count(temp.path()), 1);

    let engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("reopen engine");

    assert_eq!(
        engine.get("user:1").expect("get user 1"),
        Some(b"alice-v2".to_vec())
    );
    assert_eq!(engine.get("user:2").expect("get user 2"), None);
    assert_eq!(engine.stats().sstable_count, 1);
}

#[test]
fn compaction_flushes_memtable_first() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut engine = StorageEngine::open(StorageConfig::new(temp.path())).expect("open engine");

    engine.put("alpha", "one").expect("put alpha");
    let report = engine.compact_all().expect("compact");

    assert!(report.flushed_memtable);
    assert_eq!(report.input_sstables, 1);
    assert_eq!(report.output_sstables, 1);
    assert_eq!(report.input_entries, 1);
    assert_eq!(report.output_entries, 1);
    assert_eq!(engine.stats().memtable_entries, 0);
    assert_eq!(engine.stats().sstable_count, 1);
    assert_eq!(
        engine.get("alpha").expect("get alpha"),
        Some(b"one".to_vec())
    );
}

fn sstable_file_count(path: &std::path::Path) -> usize {
    fs::read_dir(path.join("sstables"))
        .expect("read sstables dir")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .count()
}
