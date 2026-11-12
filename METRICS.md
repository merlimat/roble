# roble — metrics

A store exports metrics through OpenTelemetry once its options name a meter
provider. They are recorded with [nanometer](https://github.com/merlimat/nanometer)
handles, which a store creates when it opens: a record takes a few nanoseconds and
never allocates. Without a provider a store records nothing, reads no clock, and
does no work on their behalf.

## Setup

```rust
use roble::{DefaultEnv, Options, open_with};
use opentelemetry::KeyValue;

let options = Options::new()
    .meter_provider(&provider)
    .metric_attributes(&[KeyValue::new("shard", "7")]);
let db = open_with(&DefaultEnv, "data/shard-7".as_ref(), options).await?;
```

- The metrics come under the instrumentation scope `roble`.
- roble uses `opentelemetry` 0.33.1, the version nanometer pins, so the app's
  provider must come from that version too.
- Counters and up-down counters export through any provider. Histograms export
  only through nanometer's `Producer`, which the app registers with its metric
  reader: see nanometer's README, under **Exporting histograms**.
- nanometer's registry is process-wide and keyed by (scope, metric name), and a
  metric is registered with the provider of the **first** store to create it. A
  process with two providers exports through the first; give every store the same
  one and tell them apart by attributes.

## Attributes

Every series carries `roble.store`, the directory the store was opened at,
followed by the options' `metric_attributes`. The last value of a repeated key
wins, so an attribute named `roble.store` replaces the directory with a name of
the caller's choosing.

| Attribute | On | Values |
|---|---|---|
| `roble.store` | everything | the store's directory, or the caller's name for it |
| `roble.operation.type` | `roble.write.operations` | `put`, `delete`, `delete_range` |
| `roble.level` | `roble.level.*` | `0`–`6` |
| `disk.io.direction` | `roble.compaction.*` | `read`, `write` |
| `error.type` | `roble.*.errors` | `io`, `locked`, `format`, `unsupported`, `corrupt`, `not_found` |
| `disk.io.direction` | `roble.disk.*` | `read`, `write` |
| `roble.cache` | `roble.cache.*` | `block`, `table` |
| `roble.cache.result` | `roble.cache.lookups` | `hit`, `miss` |
| `roble.stall.reason` | `roble.write.stall*` | `memtable`, `l0` |

A handle exists for every value of `error.type` from the moment a store opens,
so the series are there at zero before anything has gone wrong.

## Writing

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.write.duration` | latency | s | Time a successful write takes, from the call until its batch is in the memtable, the flush it paid for included when it found the store at `memtable_size` |
| `roble.write.operations` | counter | `{operation}` | Operations in the batches written successfully, by kind |
| `roble.write.size` | histogram | By | Bytes of the batch representation of each write |
| `roble.write.bytes` | counter | By | Bytes of the batch representations written successfully |
| `roble.write.entries` | histogram | `{entry}` | Operations in each batch written successfully |
| `roble.write.errors` | counter | `{error}` | Writes that failed, by what went wrong |
| `roble.wal.append.duration` | latency | s | Time appending a batch to the log takes, including its sync when one was asked for |
| `roble.write.stalls` | counter | {stall} | Writes that waited for room instead of making it themselves, by `roble.stall.reason` |
| `roble.write.stall.duration` | latency | s | Time such a write spent waiting, by `roble.stall.reason` |
| `roble.memtable.size` | up-down counter | By | Bytes of writes that no table holds yet |

A store opened with `disable_wal` records no `roble.wal.append.duration` at all,
because nothing is appended: its durability is `flush`'s (§4).

## Reading

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.get.duration` | latency | s | Time a lookup takes, found or not |
| `roble.scans` | counter | `{scan}` | Scans created |
| `roble.scan.entries` | counter | `{entry}` | Keys that scans stepped onto |
| `roble.scans.open` | up-down counter | `{scan}` | Scans created and not yet dropped |
| `roble.read.bytes` | counter | By | Bytes of the keys and values that lookups and scans returned |
| `roble.read.errors` | counter | `{error}` | Lookups that failed, by what went wrong |

A `get` under any comparison but `Equal` is a scan, positioned once: it counts one
scan and one entry. `Equal` is a point lookup and counts neither.

## Flushing and compacting

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.flushes` | counter | `{flush}` | Flushes that wrote a table |
| `roble.compactions` | counter | `{compaction}` | Compactions run |
| `roble.compaction.errors` | counter | `{error}` | Compactions that failed, by what went wrong |
| `roble.flush.duration` | latency | s | Time a flush takes, from the call until its version edit is durable |
| `roble.flush.size` | histogram | By | Bytes of the table each flush wrote |
| `roble.flush.bytes` | counter | By | Bytes that flushes wrote |
| `roble.compaction.duration` | latency | s | Time a compaction takes, from the call until its version edit is durable |
| `roble.compaction.tables` | counter | `{table}` | Tables that compactions read and wrote |
| `roble.compaction.io` | counter | By | Bytes that compactions read and wrote |

A compaction a flush ran for itself is a compaction like any other: it counts in
`roble.compactions`, and its time is `roble.compaction.duration` and not part of
`roble.flush.duration`, which ends where the flush does (§9).

**Write amplification** is every byte written over the bytes a flush had to write:

```
(roble.flush.bytes + roble.compaction.io{disk.io.direction="write"}) / roble.flush.bytes
```

Both halves are counters rather than histograms so that the ratio can be taken.

## What the store holds

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.level.tables` | up-down counter | `{table}` | Tables in each level of the store's current version |
| `roble.level.size` | up-down counter | By | Bytes of the tables in each level |
| `roble.l0.sublevels` | up-down counter | `{sublevel}` | Layers L0's tables form, which is what a read of L0 costs |
| `roble.disk.size` | up-down counter | By | Bytes of every file in the store's directory |
| `roble.compaction.debt` | up-down counter | By | Bytes the levels hold above what their allowances let them, L0's counted in layers |
| `roble.records.estimate` | up-down counter | `{record}` | Records the store holds, counting every version of a key and every tombstone |

`roble.records.estimate` is an estimate for one reason: the count is in each
table's properties block, which is read when the table is opened, and no
MANIFEST records it. A store learns a table's count the first time something
has reason to read it, rather than opening every table at once and making an
open pay for a metric — so after a reopen the number climbs to the truth as
the store is used. A flush and a compaction know their own outputs outright,
so a store that writes is exact from the start.

## Disk

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.disk.io` | counter | By | Bytes the store read from and wrote to its files |
| `roble.disk.operations` | counter | `{operation}` | Read and write requests the store made |
| `roble.disk.read.duration` | latency | s | Time a read of a file takes |
| `roble.disk.sync.duration` | latency | s | Time a sync of a file takes |

Both carry `disk.io.direction`, so IOPS is the rate of
`roble.disk.operations` and throughput the rate of `roble.disk.io`. These count
what the store asked the filesystem for, which is what a cache miss costs; a
read served from the block cache makes no request and appears in
`roble.cache.lookups` instead.

## Caches

A `Caches` is shared by every store on a thread (§8), so what it holds and what
it evicts are facts about the thread, and it exports them through its own
provider:

```rust
let caches = Caches::new(128, 32 << 20)
    .meter_provider(&provider, &[KeyValue::new("thread", "0")]);
```

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.cache.usage` | up-down counter | — | What the caches hold: bytes for blocks, open readers for tables |
| `roble.cache.limit` | up-down counter | — | What they may hold |
| `roble.cache.evictions` | counter | `{entry}` | Entries evicted to make room |

Which store *looked* in a cache is a different question, and the store answers
that one:

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.cache.lookups` | counter | `{lookup}` | Tables and blocks the store looked for, by `roble.cache` and `roble.cache.result` |

A **compaction's** block reads are not counted here, and do not fill the cache
either. A compaction reads every block of its inputs exactly once and never
comes back, so its hit rate is zero by construction and counting its lookups
would bury the only hit rate anybody asks about — whether the reads that come
from outside found what they needed. It still *takes* a block that is already
there, which costs the foreground nothing.

A `Caches` with no provider of its own records nothing, whatever the stores
drawing on it do.

`roble.disk.size` is measured from the directory, so it counts the MANIFEST, the
logs, the markers and anything obsolete that the sweep has not collected yet — not
only the tables the version names. It is remeasured where files appear or
disappear, which is a flush, a compaction and an open.

These are up-down counters rather than gauges so that the stores of a process add
up to one series per set of attributes. Each store adds what changes and takes its
share back out when it closes, so a store that goes away stops being counted.

## What Oxia graphs

Oxia's data server exports a fixed set of Pebble's metrics per shard, in
`kv_pebble.go`, plus some of its own around the `kvstore.KV` calls. This is
what each becomes here.

| Oxia | roble |
|---|---|
| `kv_batch_commit_latency` | `roble.write.duration` |
| `kv_read_latency` | `roble.get.duration` |
| `kv_write` | `roble.write.bytes` |
| `kv_write_ops` | `roble.write.operations` |
| `kv_read` | `roble.read.bytes` |
| `kv_read_ops` | the count of `roble.get.duration`, or `roble.scan.entries` |
| `kv_write_errors` | `roble.write.errors`, by `error.type` |
| `kv_read_errors` | `roble.read.errors`, by `error.type` |
| `kv_batch_size` | `roble.write.size` |
| `kv_batch_count` | `roble.write.entries` |
| `pebble_flush_total` | `roble.flushes` |
| `pebble_flush` | `roble.flush.bytes` |
| `pebble_compactions_total` | `roble.compactions` |
| `pebble_compaction_debt` | `roble.compaction.debt` |
| `pebble_memtable_size` | `roble.memtable.size` |
| `pebble_disk_space` | `roble.disk.size` |
| `pebble_num_files_total` | `roble.level.tables`, summed over levels |
| `pebble_write_amplification_percent` | the ratio above |
| `pebble_read_iterators` | `roble.scans.open` |
| `pebble_block_cache_used` | `roble.cache.usage{roble.cache="block"}` |
| `pebble_max_cache_size` | `roble.cache.limit{roble.cache="block"}` |
| `pebble_block_cache_hits` / `_misses` | `roble.cache.lookups`, by `roble.cache.result` |
| `pebble_read` | `roble.disk.io{disk.io.direction="read"}` |
| `pebble_l0_sublevels` | `roble.l0.sublevels` |
| `kv_pebble_write_stalls`, `kv_pebble_write_stall_time` | `roble.write.stalls`, `roble.write.stall.duration` |

Pebble's gauges are read from one `db.Metrics()` call that Oxia memoizes for
five seconds; roble's are recorded where the thing happens, so there is nothing
to poll and nothing to memoize.

## Not here yet

- **A stall of roble's own making**, which is the one thing the two stall
  series do not separate. A write that waits for a merge a *background* task is
  running would be worth telling apart from one that waits for a merge another
  write is paying for, and today there is no background task to tell apart
  (§9). The attribute gains a value when it does.
- **Per-level read and write amplification**, which Pebble reports and Oxia
  does not graph. `roble.l0.sublevels` is the read amplification of the one
  level where it is not simply one; the rest would be a ratio of counters
  that are already here.
