# roble

An embedded, ordered key-value store in Rust whose files are
[Pebble](https://github.com/cockroachdb/pebble)'s: a database roble writes is
one Go Pebble opens, and the other way round.

> **Status:** pre-1.0. Built to replace Pebble under
> [Oxia](https://github.com/oxia-db/oxia); reads and writes databases Go
> Pebble v2.1.7 opens. The API may change before 1.0, and it has not run in
> production yet.

## What it is

- **An LSM tree**: a write-ahead log, memtables, sstables in levels, and
  leveled compaction, with Pebble's file formats byte for byte.
- **Ordered**: point lookups, floor and ceiling lookups (`Comparison`), range
  scans forwards and backwards, snapshots, and batches that can be read back
  through before they commit.
- **Thread-per-core**: a store belongs to one thread and is `!Send`. It runs
  on [compio](https://github.com/compio-rs/compio), flushes and compacts in
  tasks of its own on that thread, and hands the thread back every so often
  while it does, so reads and writes beside it wait a slice rather than a
  merge. Many stores can share one thread and one cache budget.
- **Built to be tested**: all I/O goes through an `Fs` trait, and the `sim`
  feature's in-memory filesystem can fail or crash at any single operation.

## Example

```rust
use roble::{Comparison, DefaultEnv, Durability, Options};

#[compio::main]
async fn main() -> roble::Result<()> {
    let options = Options::new().create_if_missing(true);
    let db = roble::open_with(&DefaultEnv, "data/shard-0", options).await?;

    // One write, synced to the log before it returns.
    db.put(b"/users/alice", b"1", Durability::Sync).await?;

    // Several that land together, readable through the batch before they do.
    let mut batch = db.indexed_batch();
    batch.put(b"/users/bob", b"2")?;
    batch.delete(b"/users/carol")?;
    batch.commit(Durability::NoSync).await?;

    let alice = db.get(b"/users/alice", Comparison::Equal).await?;
    assert_eq!(alice.map(|f| f.value), Some(b"1".to_vec()));

    // The greatest key at or below `/users/b`, which is alice.
    let floor = db.get(b"/users/b", Comparison::Floor).await?;
    assert_eq!(floor.map(|f| f.key), Some(b"/users/alice".to_vec()));

    let mut scan = db.range(&b"/users/"[..]..&b"/users0"[..])?;
    scan.first().await?;
    while scan.is_valid() {
        println!("{:?} = {:?}", scan.key(), scan.value());
        scan.next().await?;
    }

    // Waits for the background flush or merge in flight, then lets go of
    // the directory.
    db.close().await
}
```

## Compatibility

roble follows Pebble v2.1.7's formats, and refuses what is above them rather
than half-reading it:

| | roble reads | roble writes |
|---|---|---|
| Format major version | 13 to 16 | 16 |
| sstables | RocksDBv2 to Pebblev4 | Pebblev4 |
| Compression | Snappy, Zstd | none in L0, Snappy below (configurable) |
| WAL and MANIFEST | Pebble's record framing, batches and version edits | the same |

A comparer other than Pebble's bytewise one, range keys, blob files and WAL
failover are refused with an error naming what was found. The cross-language
tests in `compat/` hold both directions to Go Pebble itself.

## Crates

| Crate | |
|---|---|
| [`roble`](https://docs.rs/roble) | The engine. |
| [`roble-format`](https://docs.rs/roble-format) | Pebble's on-disk formats, encoded and decoded with no I/O. |
| [`roble-cli`](https://crates.io/crates/roble-cli) | `roble`, a command line tool to put, get, scan and delete entries and to inspect Pebble's files. |

roble's features:

- `opentelemetry` (default): metrics through OpenTelemetry, listed in
  [METRICS.md](METRICS.md).
- `sim`: the in-memory filesystem that fails and crashes on command, for an
  application's own crash tests.

The minimum supported Rust version is 1.99.

## Design

[DESIGN.md](DESIGN.md) is the design: the workload it is built for, what
compatibility costs, the formats, and how reads, writes, flushes,
compaction, caching and testing work. The `§N` references in the code and
its docs are to its sections.

## License

Apache-2.0.
