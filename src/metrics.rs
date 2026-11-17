// Copyright 2026 Matteo Merli <matteo.merli@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The metrics a store exports through OpenTelemetry, recorded with
//! [nanometer] handles: a few nanoseconds a record, without allocating.
//!
//! A store creates its handles when it opens, with its attributes:
//! `roble.store`, the directory it was opened at, then those its options add,
//! which can replace it. Without a meter provider in its options it creates
//! none, and recording does nothing — which is what every test in this
//! repository but `tests/metrics.rs` runs with.
//!
//! Amounts a store holds, such as the bytes of its levels, are up-down
//! counters rather than gauges, so that the stores of a process add up to one
//! series per set of attributes. Each store adds what changes and takes its
//! share back out when it closes.
//!
//! What is **not** here yet: the cache's hits and misses, because a `Caches`
//! is shared by every store on a thread (§8) and a handle created per store
//! would describe a budget none of them owns; and the two write stalls §9
//! names, because nothing stalls yet — a counter for an event that cannot
//! happen says nothing, and it arrives with the waiting it counts.
//!
//! Without the `opentelemetry` feature none of this can be given a provider:
//! [`off`] stands in for nanometer with handles that cannot exist, so every
//! `Option` of one is `None` and the code below is the same either way.
//!
//! [nanometer]: https://docs.rs/nanometer

use std::cell::Cell;
use std::path::Path;

#[cfg(feature = "opentelemetry")]
pub(crate) use nanometer::Meter as NanoMeter;
#[cfg(feature = "opentelemetry")]
use nanometer::{
    CounterDef, HistogramDef, LatencyDef, LocalCounter, LocalHistogram, LocalLatency,
    LocalUpDownCounter, Timer, UpDownCounterDef,
};
#[cfg(feature = "opentelemetry")]
pub(crate) use opentelemetry::KeyValue;

#[cfg(not(feature = "opentelemetry"))]
use off::{
    CounterDef, HistogramDef, LatencyDef, LocalCounter, LocalHistogram, LocalLatency,
    LocalUpDownCounter, Timer, UpDownCounterDef,
};
#[cfg(not(feature = "opentelemetry"))]
pub(crate) use off::{KeyValue, Meter as NanoMeter};

use crate::env::Fs;
use crate::options::Options;

/// The instrumentation scope of roble's metrics.
#[cfg(feature = "opentelemetry")]
pub(crate) const SCOPE: &str = "roble";

const WRITE_DURATION: LatencyDef = LatencyDef::new("roble.write.duration")
    .description("Time a successful write takes, from the call until its batch is in the memtable");
const WRITE_OPERATIONS: CounterDef = CounterDef::new("roble.write.operations")
    .unit("{operation}")
    .description("Operations in the batches written successfully");
const WRITE_SIZE: HistogramDef = HistogramDef::bytes("roble.write.size")
    .description("Bytes of the batch representation of each write");
const WRITE_BYTES: CounterDef = CounterDef::new("roble.write.bytes")
    .unit("By")
    .description("Bytes of the batch representations written successfully");
const WRITE_ENTRIES: HistogramDef = HistogramDef::counts("roble.write.entries")
    .unit("{entry}")
    .description("Operations in each batch written successfully");
const WRITE_ERRORS: CounterDef = CounterDef::new("roble.write.errors")
    .unit("{error}")
    .description("Writes that failed, by what went wrong");
const READ_BYTES: CounterDef = CounterDef::new("roble.read.bytes")
    .unit("By")
    .description("Bytes of the keys and values that lookups and scans returned");
const READ_ERRORS: CounterDef = CounterDef::new("roble.read.errors")
    .unit("{error}")
    .description("Lookups that failed, by what went wrong");
const SCANS_OPEN: UpDownCounterDef = UpDownCounterDef::new("roble.scans.open")
    .unit("{scan}")
    .description("Scans that have been created and not dropped");
const SNAPSHOTS_OPEN: UpDownCounterDef = UpDownCounterDef::new("roble.snapshots.open")
    .unit("{snapshot}")
    .description("Snapshots that have been created and not dropped");
const GET_DURATION: LatencyDef =
    LatencyDef::new("roble.get.duration").description("Time a lookup takes, found or not");
const SCANS: CounterDef = CounterDef::new("roble.scans")
    .unit("{scan}")
    .description("Scans created");
const SCAN_ENTRIES: CounterDef = CounterDef::new("roble.scan.entries")
    .unit("{entry}")
    .description("Keys that scans stepped onto");
const MEMTABLE_SIZE: UpDownCounterDef = UpDownCounterDef::new("roble.memtable.size")
    .unit("By")
    .description("Bytes of writes that no table holds yet");
const FLUSHES: CounterDef = CounterDef::new("roble.flushes")
    .unit("{flush}")
    .description("Flushes that wrote a table");
const COMPACTIONS: CounterDef = CounterDef::new("roble.compactions")
    .unit("{compaction}")
    .description("Compactions run");
const COMPACTION_DEBT: UpDownCounterDef = UpDownCounterDef::new("roble.compaction.debt")
    .unit("By")
    .description("Bytes the levels hold above what their allowances let them");
const FLUSH_DURATION: LatencyDef = LatencyDef::new("roble.flush.duration")
    .description("Time a flush takes, from the call until its version edit is durable");
const FLUSH_SIZE: HistogramDef =
    HistogramDef::bytes("roble.flush.size").description("Bytes of the table each flush wrote");
const FLUSH_BYTES: CounterDef = CounterDef::new("roble.flush.bytes").unit("By").description(
    "Bytes that flushes wrote. The denominator of write amplification: \
         (roble.flush.bytes + roble.compaction.io{write}) / roble.flush.bytes",
);
const WRITE_STALLS: CounterDef = CounterDef::new("roble.write.stalls")
    .unit("{stall}")
    .description("Writes that waited for room instead of making it themselves");
const WRITE_STALL_DURATION: LatencyDef = LatencyDef::new("roble.write.stall.duration")
    .description("Time a write spent waiting for room it did not make itself");
const WAL_DURATION: LatencyDef = LatencyDef::new("roble.wal.append.duration").description(
    "Time appending a batch to the log takes, including its sync when one was asked for",
);
const L0_SUBLEVELS: UpDownCounterDef = UpDownCounterDef::new("roble.l0.sublevels")
    .unit("{sublevel}")
    .description("Layers L0's tables form, which is what a read of L0 costs");
const DISK_SIZE: UpDownCounterDef = UpDownCounterDef::new("roble.disk.size")
    .unit("By")
    .description("Bytes of every file in the store's directory");
const COMPACTION_ERRORS: CounterDef = CounterDef::new("roble.compaction.errors")
    .unit("{error}")
    .description("Compactions that failed, by what went wrong");
const COMPACTION_DURATION: LatencyDef = LatencyDef::new("roble.compaction.duration")
    .description("Time a compaction takes, from the call until its version edit is durable");
const COMPACTION_TABLES: CounterDef = CounterDef::new("roble.compaction.tables")
    .unit("{table}")
    .description("Tables that compactions read and wrote");
const COMPACTION_IO: CounterDef = CounterDef::new("roble.compaction.io")
    .unit("By")
    .description("Bytes that compactions read and wrote");
const DISK_IO: CounterDef = CounterDef::new("roble.disk.io")
    .unit("By")
    .description("Bytes the store read from and wrote to its files");
const DISK_OPERATIONS: CounterDef = CounterDef::new("roble.disk.operations")
    .unit("{operation}")
    .description("Read and write requests the store made to its files");
const DISK_READ_DURATION: LatencyDef =
    LatencyDef::new("roble.disk.read.duration").description("Time a read of a file takes");
const DISK_SYNC_DURATION: LatencyDef =
    LatencyDef::new("roble.disk.sync.duration").description("Time a sync of a file takes");
const CACHE_LOOKUPS: CounterDef = CounterDef::new("roble.cache.lookups")
    .unit("{lookup}")
    .description("Tables and blocks the store looked for in a cache");
#[cfg(feature = "opentelemetry")]
const CACHE_USAGE: UpDownCounterDef = UpDownCounterDef::new("roble.cache.usage")
    .unit("{by}")
    .description("What the caches hold: bytes for blocks, open readers for tables");
#[cfg(feature = "opentelemetry")]
const CACHE_LIMIT: UpDownCounterDef = UpDownCounterDef::new("roble.cache.limit")
    .unit("{by}")
    .description("What the caches may hold: bytes for blocks, open readers for tables");
#[cfg(feature = "opentelemetry")]
const CACHE_EVICTIONS: CounterDef = CounterDef::new("roble.cache.evictions")
    .unit("{entry}")
    .description("Entries evicted from a cache to make room");
const RECORDS: UpDownCounterDef = UpDownCounterDef::new("roble.records.estimate")
    .unit("{record}")
    .description(
        "Records the store holds, counting every version of a key and every tombstone: \
         the memtable's, plus those of each table whose count is known",
    );
const LEVEL_TABLES: UpDownCounterDef = UpDownCounterDef::new("roble.level.tables")
    .unit("{table}")
    .description("Tables in each level of the store's current version");
const LEVEL_SIZE: UpDownCounterDef = UpDownCounterDef::new("roble.level.size")
    .unit("By")
    .description("Bytes of the tables in each level of the store's current version");

const STORE: &str = "roble.store";
const OPERATION_TYPE: &str = "roble.operation.type";
const LEVEL: &str = "roble.level";
const DIRECTION: &str = "disk.io.direction";
const CACHE: &str = "roble.cache";
const CACHE_RESULT: &str = "roble.cache.result";
const STALL_REASON: &str = "roble.stall.reason";
const ERROR_TYPE: &str = "error.type";

/// The value of `error.type` for each way an operation can fail.
///
/// OpenTelemetry asks for a low-cardinality classification rather than a
/// message, and roble's error kinds already are one.
const ERROR_TYPES: [&str; 8] = [
    "io",
    "locked",
    "format",
    "unsupported",
    "corrupt",
    "no_database",
    "read_only",
    "stale",
];

/// Which of [`ERROR_TYPES`] an error is.
fn error_type(error: &crate::error::Error) -> usize {
    use crate::error::Error::*;
    match error {
        Io(_) => 0,
        Locked => 1,
        Format(_) => 2,
        Unsupported(_) => 3,
        Corrupt(_) => 4,
        NoDatabase(_) => 5,
        ReadOnly => 6,
        Stale(_) => 7,
    }
}

/// How many levels a version has, and so how many series the level metrics
/// carry. Named here rather than imported so the array sizes below are one
/// constant, not two that could drift.
const LEVELS: usize = roble_format::manifest::NUM_LEVELS as usize;

/// Creates a store's metric handles, with its attributes.
#[derive(Clone, Default)]
pub(crate) struct Meter {
    meter: Option<NanoMeter>,
    attributes: Vec<KeyValue>,
}

impl Meter {
    /// The meter of a store opened at `dir` with `options`.
    pub(crate) fn new<F: Fs>(options: &Options<F>, dir: &Path) -> Meter {
        let Some(meter) = options.meter.clone() else {
            return Meter::default();
        };
        let store = KeyValue::new(STORE, dir.to_string_lossy().into_owned());
        // The options' attributes go last: of a key given twice, the last
        // value wins, so a caller can name the store itself.
        let attributes = std::iter::once(store)
            .chain(options.metric_attributes.iter().cloned())
            .collect();
        Meter {
            meter: Some(meter),
            attributes,
        }
    }

    /// The store's attributes, then `extra`.
    fn attributes(&self, extra: &[KeyValue]) -> Vec<KeyValue> {
        let mut attributes = self.attributes.clone();
        attributes.extend_from_slice(extra);
        attributes
    }

    fn counter(&self, def: CounterDef, extra: &[KeyValue]) -> Counter {
        Counter(
            self.meter
                .as_ref()
                .map(|m| m.counter(def, &self.attributes(extra))),
        )
    }

    fn amount(&self, def: UpDownCounterDef, extra: &[KeyValue]) -> Amount {
        Amount {
            counter: self
                .meter
                .as_ref()
                .map(|m| m.up_down_counter(def, &self.attributes(extra))),
            value: Cell::new(0),
        }
    }

    fn histogram(&self, def: HistogramDef) -> Histogram {
        Histogram(
            self.meter
                .as_ref()
                .map(|m| m.histogram(def, &self.attributes)),
        )
    }

    fn latency(&self, def: LatencyDef, extra: &[KeyValue]) -> Latency {
        Latency(
            self.meter
                .as_ref()
                .map(|m| m.latency(def, &self.attributes(extra))),
        )
    }

    /// One handle per level, each with the level as an attribute.
    fn per_level(&self, def: UpDownCounterDef) -> [Amount; LEVELS] {
        std::array::from_fn(|level| self.amount(def, &[KeyValue::new(LEVEL, level as i64)]))
    }
}

/// The handles a store records with.
pub(crate) struct Metrics {
    pub(crate) write: Latency,
    /// Puts, deletes and range deletes.
    pub(crate) write_operations: [Counter; 3],
    pub(crate) write_size: Histogram,
    pub(crate) write_bytes: Counter,
    pub(crate) write_entries: Histogram,
    pub(crate) write_errors: Errors,
    pub(crate) read_bytes: Counter,
    pub(crate) read_errors: Errors,
    pub(crate) scans_open: Amount,
    pub(crate) snapshots_open: Amount,
    pub(crate) get: Latency,
    pub(crate) scans: Counter,
    pub(crate) scan_entries: Counter,
    pub(crate) memtable_size: Amount,
    pub(crate) flushes: Counter,
    pub(crate) compactions: Counter,
    pub(crate) compaction_debt: Amount,
    pub(crate) flush: Latency,
    pub(crate) flush_size: Histogram,
    pub(crate) flush_bytes: Counter,
    pub(crate) wal: Latency,
    /// Writes that waited for a change already underway instead of making
    /// room themselves, and the time they spent waiting — by which of the
    /// two things they were short of (§9).
    pub(crate) write_stalls: [Counter; 2],
    pub(crate) write_stall: [Latency; 2],
    pub(crate) disk_size: Amount,
    /// Layers L0's tables form (§9, `crate::version::Version::l0_sublevels`).
    pub(crate) l0_sublevels: Amount,
    pub(crate) compaction: Latency,
    /// Compactions that failed. Worth its own series because a compaction
    /// often has nobody to return an error to: a flush swallows the one it
    /// scheduled, and a task running one in the background was told to by a
    /// flush that has already gone (§9).
    pub(crate) compaction_errors: Errors,
    /// Tables read and written, in that order.
    pub(crate) compaction_tables: [Counter; 2],
    /// Bytes read and written, in that order.
    pub(crate) compaction_io: [Counter; 2],
    pub(crate) read: Io,
    pub(crate) written: Io,
    pub(crate) read_duration: Latency,
    pub(crate) sync_duration: Latency,
    /// Block-cache hits and misses, then table-cache hits and misses.
    pub(crate) cache_lookups: [Counter; 4],
    pub(crate) records: Amount,
    pub(crate) level_tables: [Amount; LEVELS],
    pub(crate) level_size: [Amount; LEVELS],
}

impl Metrics {
    /// The handles of a store, from `meter`.
    pub(crate) fn new(meter: &Meter) -> Metrics {
        let io = |direction| [KeyValue::new(DIRECTION, direction)];
        Metrics {
            write: meter.latency(WRITE_DURATION, &[]),
            write_operations: ["put", "delete", "delete_range"].map(|kind| {
                meter.counter(WRITE_OPERATIONS, &[KeyValue::new(OPERATION_TYPE, kind)])
            }),
            write_size: meter.histogram(WRITE_SIZE),
            write_bytes: meter.counter(WRITE_BYTES, &[]),
            write_entries: meter.histogram(WRITE_ENTRIES),
            write_errors: Errors::new(meter, WRITE_ERRORS),
            read_bytes: meter.counter(READ_BYTES, &[]),
            read_errors: Errors::new(meter, READ_ERRORS),
            scans_open: meter.amount(SCANS_OPEN, &[]),
            snapshots_open: meter.amount(SNAPSHOTS_OPEN, &[]),
            get: meter.latency(GET_DURATION, &[]),
            scans: meter.counter(SCANS, &[]),
            scan_entries: meter.counter(SCAN_ENTRIES, &[]),
            memtable_size: meter.amount(MEMTABLE_SIZE, &[]),
            flushes: meter.counter(FLUSHES, &[]),
            compactions: meter.counter(COMPACTIONS, &[]),
            compaction_debt: meter.amount(COMPACTION_DEBT, &[]),
            flush: meter.latency(FLUSH_DURATION, &[]),
            flush_size: meter.histogram(FLUSH_SIZE),
            flush_bytes: meter.counter(FLUSH_BYTES, &[]),
            wal: meter.latency(WAL_DURATION, &[]),
            write_stalls: ["memtable", "l0"]
                .map(|why| meter.counter(WRITE_STALLS, &[KeyValue::new(STALL_REASON, why)])),
            write_stall: ["memtable", "l0"].map(|why| {
                meter.latency(WRITE_STALL_DURATION, &[KeyValue::new(STALL_REASON, why)])
            }),
            disk_size: meter.amount(DISK_SIZE, &[]),
            l0_sublevels: meter.amount(L0_SUBLEVELS, &[]),
            compaction: meter.latency(COMPACTION_DURATION, &[]),
            compaction_errors: Errors::new(meter, COMPACTION_ERRORS),
            compaction_tables: ["read", "write"].map(|d| meter.counter(COMPACTION_TABLES, &io(d))),
            compaction_io: ["read", "write"].map(|d| meter.counter(COMPACTION_IO, &io(d))),
            read: Io::new(meter, "read"),
            written: Io::new(meter, "write"),
            read_duration: meter.latency(DISK_READ_DURATION, &[]),
            sync_duration: meter.latency(DISK_SYNC_DURATION, &[]),
            cache_lookups: [
                ("block", "hit"),
                ("block", "miss"),
                ("table", "hit"),
                ("table", "miss"),
            ]
            .map(|(cache, result)| {
                meter.counter(
                    CACHE_LOOKUPS,
                    &[
                        KeyValue::new(CACHE, cache),
                        KeyValue::new(CACHE_RESULT, result),
                    ],
                )
            }),
            records: meter.amount(RECORDS, &[]),
            level_tables: meter.per_level(LEVEL_TABLES),
            level_size: meter.per_level(LEVEL_SIZE),
        }
    }

    /// Whether anything is being recorded at all.
    ///
    /// The one question worth asking before doing work *for* a metric rather
    /// than beside one: measuring the directory means a call into the
    /// filesystem, and a store without a provider should not make it.
    pub(crate) fn recording(&self) -> bool {
        self.disk_size.counter.is_some()
    }

    /// Restates how far the levels are past what they may hold.
    ///
    /// Pebble calls this the compaction debt and Oxia graphs it: the number
    /// that says the store is falling behind before anything stalls.
    pub(crate) fn saw_debt(&self, debt: u64) {
        self.compaction_debt.set(debt as i64);
    }

    /// Restates what each level holds.
    ///
    /// Called wherever the version changes rather than computed on export,
    /// because an up-down counter is a store's *share* of a series: what it
    /// last said, until it says otherwise.
    pub(crate) fn saw_version(&self, version: &crate::version::Version) {
        for (level, tables) in version.levels() {
            self.level_tables[level as usize].set(tables.len() as i64);
            self.level_size[level as usize].set(tables.iter().map(|t| t.size as i64).sum());
        }
        // Dropped rather than reported if it cannot be worked out: a table
        // whose bounds do not decode has already broken everything that
        // reads them, and a metric is not the place to find that out.
        if let Ok(sublevels) = version.l0_sublevels() {
            self.l0_sublevels.set(sublevels as i64);
        }
    }
}

/// Which of the two caches a handle describes.
#[cfg(feature = "opentelemetry")]
pub(crate) const BLOCK_CACHE: usize = 0;
#[cfg(feature = "opentelemetry")]
pub(crate) const TABLE_CACHE: usize = 1;

/// Which of [`Metrics::write_stalls`] a wait was: a write short of room in a
/// memtable, and a write short of a shallow enough L0 (§9).
pub(crate) const STALL_MEMTABLE: usize = 0;
pub(crate) const STALL_L0: usize = 1;

/// Which of [`Metrics::cache_lookups`] a lookup was.
pub(crate) const BLOCK_HIT: usize = 0;
pub(crate) const BLOCK_MISS: usize = 1;
pub(crate) const TABLE_HIT: usize = 2;
pub(crate) const TABLE_MISS: usize = 3;

/// The bytes and requests of one direction of I/O.
#[derive(Default)]
pub(crate) struct Io {
    bytes: Counter,
    operations: Counter,
}

impl Io {
    fn new(meter: &Meter, direction: &'static str) -> Io {
        let attributes = [KeyValue::new(DIRECTION, direction)];
        Io {
            bytes: meter.counter(DISK_IO, &attributes),
            operations: meter.counter(DISK_OPERATIONS, &attributes),
        }
    }

    /// Counts one request of `bytes` bytes.
    pub(crate) fn add(&self, bytes: usize) {
        self.bytes.add(bytes as u64);
        self.operations.add(1);
    }
}

/// What the caches themselves export.
///
/// Theirs rather than any store's: a `Caches` is shared by every store on a
/// thread (§8), so its budget and what it holds describe the thread. Which
/// store *looked* in it is a different question, and
/// [`Metrics::cache_lookups`] answers that one.
#[derive(Default)]
pub struct CacheMetrics {
    /// Blocks, then tables.
    usage: [Amount; 2],
    limit: [Amount; 2],
    evictions: [Counter; 2],
}

impl CacheMetrics {
    /// Handles for caches exporting through `provider`, with `attributes`.
    #[cfg(feature = "opentelemetry")]
    pub(crate) fn new<P: opentelemetry::metrics::MeterProvider + ?Sized>(
        provider: &P,
        attributes: &[KeyValue],
    ) -> CacheMetrics {
        let meter = Meter {
            meter: Some(nanometer::Meter::new(provider, SCOPE)),
            attributes: attributes.to_vec(),
        };
        let of = |kind| [KeyValue::new(CACHE, kind)];
        CacheMetrics {
            usage: ["block", "table"].map(|k| meter.amount(CACHE_USAGE, &of(k))),
            limit: ["block", "table"].map(|k| meter.amount(CACHE_LIMIT, &of(k))),
            evictions: ["block", "table"].map(|k| meter.counter(CACHE_EVICTIONS, &of(k))),
        }
    }

    /// The handles for one of the two caches: blocks are index 0, tables 1.
    pub(crate) fn of(&self, which: usize) -> (&Amount, &Amount, &Counter) {
        (
            &self.usage[which],
            &self.limit[which],
            &self.evictions[which],
        )
    }
}

/// One counter per kind of error, so a failure is classified rather than
/// merely counted.
#[derive(Default)]
pub(crate) struct Errors([Counter; ERROR_TYPES.len()]);

impl Errors {
    fn new(meter: &Meter, def: CounterDef) -> Errors {
        Errors(ERROR_TYPES.map(|kind| meter.counter(def, &[KeyValue::new(ERROR_TYPE, kind)])))
    }

    /// Counts `result`'s error, and hands it back unchanged.
    pub(crate) fn saw<T>(
        &self,
        result: Result<T, crate::error::Error>,
    ) -> Result<T, crate::error::Error> {
        if let Err(e) = &result {
            self.0[error_type(e)].add(1);
        }
        result
    }
}

/// A counter, or nothing for a store without metrics.
#[derive(Default)]
pub(crate) struct Counter(Option<LocalCounter>);

impl Counter {
    pub(crate) fn add(&self, n: u64) {
        if let Some(counter) = &self.0 {
            counter.add(n);
        }
    }
}

/// A store's share of an up-down counter: an amount the store holds, which it
/// takes back out when dropped.
#[derive(Default)]
pub(crate) struct Amount {
    counter: Option<LocalUpDownCounter>,
    value: Cell<i64>,
}

impl Amount {
    pub(crate) fn add(&self, n: i64) {
        if let Some(counter) = &self.counter {
            self.value.set(self.value.get() + n);
            counter.add(n);
        }
    }

    pub(crate) fn set(&self, value: i64) {
        if let Some(counter) = &self.counter {
            counter.add(value - self.value.get());
            self.value.set(value);
        }
    }
}

impl Drop for Amount {
    fn drop(&mut self) {
        self.set(0);
    }
}

/// A histogram, or nothing.
#[derive(Default)]
pub(crate) struct Histogram(Option<LocalHistogram>);

impl Histogram {
    pub(crate) fn record(&self, value: u64) {
        if let Some(histogram) = &self.0 {
            histogram.record(value);
        }
    }
}

/// A latency, or nothing.
#[derive(Default)]
pub(crate) struct Latency(Option<LocalLatency>);

impl Latency {
    /// Times until the guard it returns is dropped.
    ///
    /// `None` without metrics, so the clock is not read at all: §10 keeps a
    /// store off the clock, and timing what a meter provider asked for is
    /// the one place that reads it. A seeded run without a meter provider
    /// reads no clock anywhere.
    pub(crate) fn start(&self) -> Option<Timer<'_>> {
        self.0.as_ref().map(|l| l.start())
    }
}

/// What stands in for nanometer without the `opentelemetry` feature.
///
/// The same names, so the code above compiles unchanged, and handles that are
/// uninhabited: no value of one can be made, so every `Option` of one is
/// `None`, every record is a branch on that, and none of it reads a clock.
//
// Its builders mirror nanometer's whole, so some go unused here.
#[cfg(not(feature = "opentelemetry"))]
#[allow(dead_code)]
mod off {
    use std::convert::Infallible;
    use std::marker::PhantomData;

    /// An attribute, which nothing will ever read.
    #[derive(Clone, Debug)]
    pub(crate) struct KeyValue;

    impl KeyValue {
        pub(crate) fn new<K, V>(_key: K, _value: V) -> Self {
            KeyValue
        }
    }

    /// A metric's name, unit and description, kept nowhere.
    macro_rules! def {
        ($($name:ident),*) => {$(
            #[derive(Clone, Copy)]
            pub(crate) struct $name;

            impl $name {
                pub(crate) const fn new(_name: &'static str) -> Self {
                    $name
                }
                pub(crate) const fn unit(self, _unit: &'static str) -> Self {
                    self
                }
                pub(crate) const fn description(self, _text: &'static str) -> Self {
                    self
                }
            }
        )*};
    }
    def!(CounterDef, HistogramDef, LatencyDef, UpDownCounterDef);

    impl HistogramDef {
        pub(crate) const fn bytes(_name: &'static str) -> Self {
            HistogramDef
        }
        pub(crate) const fn counts(_name: &'static str) -> Self {
            HistogramDef
        }
    }

    /// A meter, of which there are none.
    #[derive(Clone)]
    pub(crate) enum Meter {}

    impl Meter {
        pub(crate) fn counter(&self, _: CounterDef, _: &[KeyValue]) -> LocalCounter {
            match *self {}
        }
        pub(crate) fn up_down_counter(
            &self,
            _: UpDownCounterDef,
            _: &[KeyValue],
        ) -> LocalUpDownCounter {
            match *self {}
        }
        pub(crate) fn histogram(&self, _: HistogramDef, _: &[KeyValue]) -> LocalHistogram {
            match *self {}
        }
        pub(crate) fn latency(&self, _: LatencyDef, _: &[KeyValue]) -> LocalLatency {
            match *self {}
        }
    }

    pub(crate) enum LocalCounter {}
    pub(crate) enum LocalUpDownCounter {}
    pub(crate) enum LocalHistogram {}
    pub(crate) enum LocalLatency {}
    pub(crate) struct Timer<'a>(Infallible, PhantomData<&'a ()>);

    // As nanometer's does, so that ending one early reads the same here.
    impl Drop for Timer<'_> {
        fn drop(&mut self) {
            match self.0 {}
        }
    }

    impl LocalCounter {
        pub(crate) fn add(&self, _: u64) {
            match *self {}
        }
    }
    impl LocalUpDownCounter {
        pub(crate) fn add(&self, _: i64) {
            match *self {}
        }
    }
    impl LocalHistogram {
        pub(crate) fn record(&self, _: u64) {
            match *self {}
        }
    }
    impl LocalLatency {
        pub(crate) fn start(&self) -> Timer<'_> {
            match *self {}
        }
    }
}
