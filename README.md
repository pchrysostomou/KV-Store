# Distributed KV Store

Rust prototype for a mini-etcd style distributed key-value store.

## Current Status

Implemented first storage slice:

- In-memory sorted `MemTable` backed by `BTreeMap`
- Tombstone deletes
- Append-only WAL with checksummed binary records
- WAL recovery into the MemTable on startup
- Immutable SSTable files with an in-file index and footer
- SSTable Bloom filters for fast negative lookups
- Manual and threshold-based MemTable flush
- Full SSTable compaction that keeps the newest key versions and drops tombstones
- Local `kvctl` CLI for `put`, `get`, `delete`, and `stats`
- Proto API draft for the future gRPC server
- Initial Raft/server/client module skeletons

The current WAL record format is:

```text
[length u32][op u8][key_len u32][value_len u32][key][value][crc32 u32]
```

That extends the roadmap's minimal format with an operation byte for deletes and a checksum for safer recovery.

## Run Locally

Install the Rust toolchain, then run:

```bash
cargo test
cargo run -- put mykey "hello world"
cargo run -- get mykey
cargo run -- delete mykey
cargo run -- stats
cargo run -- scan --prefix user: --limit 100
cargo run -- flush
cargo run -- compact
```

Use a custom data directory with:

```bash
cargo run -- --data-dir /tmp/kv-store put mykey value
```

## Run As A gRPC Server

Start the server:

```bash
cargo run -- --data-dir /tmp/kv-server server --addr 127.0.0.1:50051
```

Use another terminal as a remote client:

```bash
cargo run -- --endpoint http://127.0.0.1:50051 put user:1 alice
cargo run -- --endpoint http://127.0.0.1:50051 get user:1
cargo run -- --endpoint http://127.0.0.1:50051 scan --prefix user: --limit 10
cargo run -- --endpoint http://127.0.0.1:50051 stats
cargo run -- --endpoint http://127.0.0.1:50051 flush
```

Port `50051` is a gRPC endpoint, not a browser page. Use `kvctl --endpoint` or a gRPC client.

## Next Milestone

Next storage improvements:

- Add block-level compression/checksums
- Replace full compaction with level-based compaction triggers
