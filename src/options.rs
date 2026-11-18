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

//! What a store is told when it opens (§4, §11).
//!
//! Most of these are budgets, and say only what a store spends. Two are
//! policies, which is to say they change what a call means rather than what
//! it costs: [`Options::wal`] changes where durability comes from, and so
//! changes what `write` promises, and `create_if_missing` decides whether an
//! `open` of a directory with no database in it is an error or a database
//! (§6, `crate::create`).

#[cfg(feature = "opentelemetry")]
use opentelemetry::metrics::MeterProvider;

use crate::metrics::{KeyValue, NanoMeter};

use crate::cache::Caches;
use crate::env::Fs;
use roble_format::block::Codec;

/// How a store's writes reach its write-ahead log, and so how durable a
/// write is when it returns ([`Options::wal`]).
///
/// The store's, not each write's: every [`put`](crate::Db::put),
/// [`delete`](crate::Db::delete), [`delete_range`](crate::Db::delete_range)
/// and [`commit`](crate::IndexedBatch::commit) is made the same way.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Wal {
    /// No log. A write reaches the memtable and nothing else, and is durable
    /// only once a [`flush`](crate::Db::flush) has written it to a table:
    /// Oxia's configuration, since its own replication log is the source of
    /// truth.
    Off,
    /// Every write is appended to the log before it is applied, and the log
    /// is left to the OS to sync. A write survives the process going away,
    /// not the machine. Pebble's `pebble.NoSync`, RocksDB's default.
    Buffered,
    /// Every write is appended to the log and the log synced before the
    /// write returns, so it survives the machine going away too. Pebble's
    /// `pebble.Sync`; the default, as the choice that loses nothing.
    #[default]
    Synced,
}

/// How a store is opened.
pub struct Options<F: Fs = crate::FileFs> {
    /// The caches it draws on, shared with every other store on the thread
    /// that was given the same handle (§8).
    pub(crate) caches: Caches<F>,
    /// How writes reach the log ([`Options::wal`]).
    pub(crate) wal: Wal,
    /// Whether an open with no database to find writes one.
    pub(crate) create_if_missing: bool,
    /// Whether a flush leaves the compaction the picker chose for the
    /// caller to run.
    pub(crate) disable_automatic_compactions: bool,
    /// Whether a store opened to write runs its flushes and merges in tasks
    /// of its own.
    pub(crate) background_tasks: bool,
    /// How many bytes a memtable holds before a write writes it out.
    pub(crate) memtable_size: u64,
    /// How large the MANIFEST may grow before a flush replaces it.
    pub(crate) max_manifest_bytes: u64,
    /// How many tables L0 may hold before the picker wants it compacted.
    pub(crate) l0_compaction_threshold: u64,
    /// How many tables L0 may hold before a write waits for it to drain.
    pub(crate) l0_stop_writes_threshold: u64,
    /// How many bytes L1 may hold; each level below it ten times more.
    pub(crate) lbase_max_bytes: u64,
    /// How many entries a compaction merges before it offers the thread
    /// back; zero to merge straight through.
    pub(crate) compaction_unit: u64,
    /// How large a table a compaction aims to write into L0; each level
    /// below it twice as large.
    pub(crate) target_file_size: u64,
    /// What a compaction compresses the data and index blocks of the tables
    /// it writes with.
    pub(crate) compression: Codec,
    /// Where the store's metrics go, if anywhere (`crate::metrics`).
    pub(crate) meter: Option<NanoMeter>,
    /// Attributes added to every series, after `roble.store`.
    pub(crate) metric_attributes: Vec<KeyValue>,
}

impl<F: Fs> Default for Options<F> {
    fn default() -> Self {
        Options {
            caches: Caches::default(),
            wal: Wal::Synced,
            create_if_missing: false,
            disable_automatic_compactions: false,
            background_tasks: true,
            memtable_size: crate::memtable::MEMTABLE_SIZE,
            max_manifest_bytes: crate::rotate::MAX_MANIFEST_BYTES,
            l0_compaction_threshold: crate::pick::L0_COMPACTION_THRESHOLD,
            l0_stop_writes_threshold: crate::pick::L0_STOP_WRITES_THRESHOLD,
            lbase_max_bytes: crate::pick::LBASE_MAX_BYTES,
            compaction_unit: crate::work::COMPACTION_UNIT,
            target_file_size: crate::compact::TARGET_FILE_SIZE,
            compression: Codec::Snappy,
            meter: None,
            metric_attributes: Vec::new(),
        }
    }
}

impl<F: Fs> Options<F> {
    /// The defaults: caches of this store's own, and a WAL.
    pub fn new() -> Self {
        Options::default()
    }

    /// Draws on `caches` rather than on caches of this store's own.
    ///
    /// What makes one budget cover every store on a thread (§8), and what
    /// makes the budget mean anything: a hundred shards with a cache each is
    /// a hundred budgets.
    pub fn caches(mut self, caches: Caches<F>) -> Self {
        self.caches = caches;
        self
    }

    /// How writes reach the write-ahead log, and so how durable a write is
    /// when it returns: [`Wal::Synced`] by default.
    ///
    /// One setting for the store rather than a flag on every write, as Oxia
    /// configures Pebble: its `UseWAL` and `SyncData` are the store's, and
    /// every commit is made with them.
    ///
    /// [`Wal::Off`] is Oxia's configuration (§1): its replication log is the
    /// source of truth, so the store need only be durable at
    /// [`Db::flush`](crate::Db::flush), and Oxia replays everything after
    /// that. A write then reaches the memtable and nothing else, and a crash
    /// takes the database back to its last flush.
    ///
    /// It does not stop a log being *read*. A database written with a WAL and
    /// reopened without one still has its unflushed logs replayed, because
    /// those records were acknowledged and dropping them silently is the one
    /// thing an open may not do (§6).
    ///
    /// That crossover leaves one wart, kept rather than fixed because the
    /// fix would be machinery for a configuration nobody runs: a store with
    /// no WAL never moves the minimum unflushed log number, so a log it
    /// replayed is replayed again at every later open, even once a flush has
    /// put those records in a table. Re-applying them costs time and nothing
    /// else — every entry arrives with the sequence number it was written
    /// with, so a newer value or tombstone still wins.
    pub fn wal(mut self, wal: Wal) -> Self {
        self.wal = wal;
        self
    }

    /// Creates the database when the directory holds none, as Pebble's
    /// `CreateIfMissing` does (`crate::create`).
    ///
    /// Off by default, so that an `open` of the wrong path says so instead
    /// of quietly handing back an empty store. On, an open makes the
    /// directory if it is not there, writes the least a database can consist
    /// of, and then opens that — one call, under one lock, which is why this
    /// is a setting rather than a `create` of its own: Oxia's own call site
    /// cannot tell a shard arriving from a shard already here, since
    /// `newKVPebble` builds a path and opens it either way.
    ///
    /// It does not make an open forgiving of anything else. A directory
    /// holding a `marker.` file is refused rather than written into,
    /// because what is there is either a database this would overwrite or a
    /// creation that did not finish, and one marker cannot say which.
    pub fn create_if_missing(mut self, create: bool) -> Self {
        self.create_if_missing = create;
        self
    }

    /// Stops the store from compacting on its own.
    ///
    /// Pebble's `DisableAutomaticCompactions`. A flush no longer runs the
    /// merge the picker chose, a write no longer waits for one at L0's
    /// depth, and the store spawns no task to run them: what a test that
    /// drives compaction by hand needs, so that the store holds still.
    ///
    /// A store with this set still *reports* that it has fallen behind, in
    /// `roble.compaction.debt`. It simply does nothing about it.
    ///
    /// Not part of the API: the only way left to compact is `compact_once`,
    /// which is not part of it either.
    #[doc(hidden)]
    pub fn disable_automatic_compactions(mut self, disable: bool) -> Self {
        self.disable_automatic_compactions = disable;
        self
    }

    /// Whether the store flushes and compacts in tasks of its own: true by
    /// default.
    ///
    /// Not part of the API. Off, nothing is spawned, and the store flushes
    /// and merges inside the writes that need it, as it does for any work
    /// its tasks are not there to take: what a test that counts operations
    /// or drives the store by hand needs, and what the CLI and the
    /// benchmark's inline mode run.
    #[doc(hidden)]
    pub fn background_tasks(mut self, on: bool) -> Self {
        self.background_tasks = on;
        self
    }

    /// How many bytes a memtable holds before a write writes it out (§4).
    ///
    /// Pebble's `MemTableSize`, and the same default of 4 MiB. A write that
    /// finds the memtable at or past this flushes it before applying its own
    /// batch, so **this is the only thing that bounds a store's memory**: a
    /// caller that never calls `flush` itself still gets one.
    ///
    /// It bounds the store rather than fixing the memtable's size. The batch
    /// that takes it over the line is applied whole, so what a store holds
    /// across every memtable it has — the one writes go to, and one a flush
    /// is writing out (§5) — is at most this plus one batch. That holds
    /// after a flush that failed as well, because the write that follows
    /// such a flush is the one that retries it.
    ///
    /// **This line is the backstop.** A store with a task waiting on
    /// [`crate::Db::flush_wanted`] is told there is a memtable to write
    /// out at seven eighths of it, and that
    /// task writes it out while the writes carry on into the last eighth —
    /// so the only writes that come down here are the ones a store whose
    /// flushes have stopped keeping up, or one with nobody to hand them to,
    /// has nowhere else to put. The bound above is the same either way: the
    /// reserve is inside the budget, not beside it.
    ///
    /// The flush is the caller's to pay for, in the write that asked for the
    /// room, and its time is in `roble.write.duration` because that is what
    /// the write cost. `u64::MAX` turns it off, which is what a caller that
    /// does its own flushing on its own schedule wants.
    pub fn memtable_size(mut self, bytes: u64) -> Self {
        self.memtable_size = bytes;
        self
    }

    /// How large the MANIFEST may grow before a flush replaces it
    /// (`crate::rotate`).
    pub fn max_manifest_bytes(mut self, bytes: u64) -> Self {
        self.max_manifest_bytes = bytes;
        self
    }

    /// How deep L0 may grow, in layers, before the picker wants it compacted
    /// (`crate::pick`).
    pub fn l0_compaction_threshold(mut self, tables: u64) -> Self {
        self.l0_compaction_threshold = tables;
        self
    }

    /// How deep L0 may grow, in layers, before a write waits for it to drain
    /// (§9, `crate::pick`).
    ///
    /// Pebble's `L0StopWritesThreshold`, and the same default of twelve.
    /// Reaching it means a write pays for the L0 merge, or waits for the one
    /// already running, before its batch is accepted: every read consults
    /// all of L0, so a store that let it grow without limit would be trading
    /// write latency it does not have for read latency it cannot bound.
    ///
    /// Above [`Self::l0_compaction_threshold`], which is where the picker
    /// starts *wanting* L0 compacted. Between the two a flush runs the merge
    /// and nothing waits; at this one a write does.
    ///
    /// A store told not to compact on its own ignores this entirely, because
    /// then L0's depth is not its to manage: a write that waited for a merge
    /// nobody was going to run would wait for ever.
    pub fn l0_stop_writes_threshold(mut self, tables: u64) -> Self {
        self.l0_stop_writes_threshold = tables;
        self
    }

    /// How many bytes L1 may hold, each level below it ten times more
    /// (`crate::pick`).
    pub fn lbase_max_bytes(mut self, bytes: u64) -> Self {
        self.lbase_max_bytes = bytes;
        self
    }

    /// How many entries one of a store's long jobs gets through before it
    /// hands the thread back to whatever else is ready (§9, `crate::work`).
    ///
    /// Two of them, and the same unit: the merge a compaction runs, and the
    /// walk of the memtable a flush writes out. Both are work rather than
    /// waiting, and both run beside the writes rather than inside one, which
    /// is what makes the pacing load-bearing.
    ///
    /// Zero goes straight through, which is what a caller wants when
    /// nothing else shares the thread — a bulk load, a repair tool — and what
    /// nothing else should want: a job that does not yield stops every
    /// read and write on its core until it finishes.
    pub fn compaction_unit(mut self, entries: u64) -> Self {
        self.compaction_unit = entries;
        self
    }

    /// How large a table a compaction into L0 aims to write, each level
    /// below it twice as large (`crate::compact`).
    pub fn target_file_size(mut self, bytes: u64) -> Self {
        self.target_file_size = bytes;
        self
    }

    /// What the tables a compaction writes — L1 and below — compress their
    /// data and index blocks with. Snappy by default.
    ///
    /// A flush always writes L0 uncompressed. Both halves are Oxia's
    /// configuration (DESIGN.md §1): L0 tables are the ones written most
    /// often and read for the shortest time, so not worth the CPU, while the
    /// levels below hold nearly all the data. Snappy is the cheapest codec Go
    /// Pebble reads — a quarter to a third of Zstd's CPU to compress a block
    /// that compresses, and about a thirteenth of Zstd 3's on one that does not —
    /// and on compressible values it saves most of the space Zstd would. Changing this
    /// changes only tables written from now on; a store reads every table
    /// whatever it was written with.
    pub fn compression(mut self, codec: Codec) -> Self {
        self.compression = codec;
        self
    }

    /// Exports the store's metrics through `provider`, under the
    /// instrumentation scope `roble`. Without a provider, the store records
    /// none and reads no clock.
    ///
    /// [METRICS.md](https://github.com/merlimat/roble/blob/main/METRICS.md)
    /// lists them. Each series carries `roble.store`, the directory the store
    /// was opened at, and those of
    /// [`metric_attributes`](Self::metric_attributes). Histograms export only
    /// through nanometer's `Producer`, which the app registers with its
    /// metric reader: see [nanometer](https://docs.rs/nanometer).
    #[cfg(feature = "opentelemetry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "opentelemetry")))]
    pub fn meter_provider<P: MeterProvider + ?Sized>(mut self, provider: &P) -> Self {
        self.meter = Some(nanometer::Meter::new(provider, crate::metrics::SCOPE));
        self
    }

    /// Adds `attributes` to every series of the store's metrics, after
    /// `roble.store`: one of them with that key replaces the store's
    /// directory.
    #[cfg(feature = "opentelemetry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "opentelemetry")))]
    pub fn metric_attributes(mut self, attributes: &[KeyValue]) -> Self {
        self.metric_attributes.extend_from_slice(attributes);
        self
    }
}

// Written out rather than derived: the caches are a handle whose own line
// says what they hold, and a meter provider has nothing worth printing
// beyond whether there is one.
impl<F: Fs> std::fmt::Debug for Options<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Options");
        d.field("caches", &self.caches)
            .field("wal", &self.wal)
            .field("create_if_missing", &self.create_if_missing)
            .field(
                "disable_automatic_compactions",
                &self.disable_automatic_compactions,
            )
            .field("background_tasks", &self.background_tasks)
            .field("memtable_size", &self.memtable_size)
            .field("max_manifest_bytes", &self.max_manifest_bytes)
            .field("l0_compaction_threshold", &self.l0_compaction_threshold)
            .field("l0_stop_writes_threshold", &self.l0_stop_writes_threshold)
            .field("lbase_max_bytes", &self.lbase_max_bytes)
            .field("compaction_unit", &self.compaction_unit)
            .field("target_file_size", &self.target_file_size);
        #[cfg(feature = "opentelemetry")]
        d.field("metered", &self.meter.is_some())
            .field("metric_attributes", &self.metric_attributes);
        d.finish()
    }
}
