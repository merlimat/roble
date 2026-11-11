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

## Writing

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.write.duration` | latency | s | Time a successful write takes, from the call until its batch is in the memtable |
| `roble.write.operations` | counter | `{operation}` | Operations in the batches written successfully, by kind |
| `roble.write.size` | histogram | By | Bytes of the batch representation of each write |
| `roble.wal.append.duration` | latency | s | Time appending a batch to the log takes, including its sync when one was asked for |
| `roble.memtable.size` | up-down counter | By | Bytes of writes that no table holds yet |

A store opened with `disable_wal` records no `roble.wal.append.duration` at all,
because nothing is appended: its durability is `flush`'s (§4).

## Reading

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.get.duration` | latency | s | Time a lookup takes, found or not |
| `roble.scans` | counter | `{scan}` | Scans created |
| `roble.scan.entries` | counter | `{entry}` | Keys that scans stepped onto |

A `get` under any comparison but `Equal` is a scan, positioned once: it counts one
scan and one entry. `Equal` is a point lookup and counts neither.

## Flushing and compacting

| Metric | Instrument | Unit | What it says |
|---|---|---|---|
| `roble.flush.duration` | latency | s | Time a flush takes, from the call until its version edit is durable |
| `roble.flush.size` | histogram | By | Bytes of the table each flush wrote |
| `roble.flush.bytes` | counter | By | Bytes that flushes wrote |
| `roble.compaction.duration` | latency | s | Time a compaction takes, from the call until its version edit is durable |
| `roble.compaction.tables` | counter | `{table}` | Tables that compactions read and wrote |
| `roble.compaction.io` | counter | By | Bytes that compactions read and wrote |

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
| `roble.disk.size` | up-down counter | By | Bytes of every file in the store's directory |

`roble.disk.size` is measured from the directory, so it counts the MANIFEST, the
logs, the markers and anything obsolete that the sweep has not collected yet — not
only the tables the version names. It is remeasured where files appear or
disappear, which is a flush, a compaction and an open.

These are up-down counters rather than gauges so that the stores of a process add
up to one series per set of attributes. Each store adds what changes and takes its
share back out when it closes, so a store that goes away stops being counted.

## Not here yet

- **The caches' usage, and their hits and misses.** A `Caches` is shared by every
  store on a thread (§8), so these belong to it rather than to any one store, and
  it has no meter of its own yet.
- **Disk I/O: bytes, operations and their latency.** Every read and write a store
  issues, including a compaction's, which is what gives IOPS and read
  amplification.
- **An estimate of the records a store holds.** The count is in each table's
  properties block, which is read when a table is opened; summing it means opening
  every table of the version, which a store does not do today.
- **The two write stalls** of §9 — a write waiting for a memtable, and a write
  waiting for L0 to drain. Nothing stalls yet, and a counter for an event that
  cannot happen says nothing; they arrive with the waiting they count.
