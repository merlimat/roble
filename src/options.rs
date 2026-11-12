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
//! Three settings, and only one of them is a policy: `disable_wal` changes
//! where durability comes from, and so changes what `write` promises. The
//! other two are budgets.

use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider;

use crate::cache::Caches;
use crate::env::Fs;

/// How a store is opened.
pub struct Options<F: Fs> {
    /// The caches it draws on, shared with every other store on the thread
    /// that was given the same handle (§8).
    pub(crate) caches: Caches<F>,
    /// Whether to skip the log entirely.
    pub(crate) disable_wal: bool,
    /// Whether a flush leaves the compaction the picker chose for the
    /// caller to run.
    pub(crate) disable_automatic_compactions: bool,
    /// How many bytes a memtable holds before a write writes it out.
    pub(crate) memtable_size: u64,
    /// How large the MANIFEST may grow before a flush replaces it.
    pub(crate) max_manifest_bytes: u64,
    /// How many tables L0 may hold before the picker wants it compacted.
    pub(crate) l0_compaction_threshold: u64,
    /// How many bytes L1 may hold; each level below it ten times more.
    pub(crate) lbase_max_bytes: u64,
    /// How many entries a compaction merges before it offers the thread
    /// back; zero to merge straight through.
    pub(crate) compaction_unit: u64,
    /// How large a table a compaction aims to write into L0; each level
    /// below it twice as large.
    pub(crate) target_file_size: u64,
    /// Where the store's metrics go, if anywhere (`crate::metrics`).
    pub(crate) meter: Option<nanometer::Meter>,
    /// Attributes added to every series, after `roble.store`.
    pub(crate) metric_attributes: Vec<KeyValue>,
}

impl<F: Fs> Default for Options<F> {
    fn default() -> Self {
        Options {
            caches: Caches::default(),
            disable_wal: false,
            disable_automatic_compactions: false,
            memtable_size: crate::memtable::MEMTABLE_SIZE,
            max_manifest_bytes: crate::rotate::MAX_MANIFEST_BYTES,
            l0_compaction_threshold: crate::pick::L0_COMPACTION_THRESHOLD,
            lbase_max_bytes: crate::pick::LBASE_MAX_BYTES,
            compaction_unit: crate::work::COMPACTION_UNIT,
            target_file_size: crate::compact::TARGET_FILE_SIZE,
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

    /// Turns the WAL off, making `flush` the only durability point.
    ///
    /// This is Oxia's configuration (§1): its replication log is the source
    /// of truth, so the store need only be durable at `flush`, and Oxia
    /// replays everything after that. A write then reaches the memtable and
    /// nothing else, and a crash takes the database back to its last flush.
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
    pub fn disable_wal(mut self, disable: bool) -> Self {
        self.disable_wal = disable;
        self
    }

    /// Leaves compaction entirely to the caller.
    ///
    /// Pebble's `DisableAutomaticCompactions`, and wanted for the same two
    /// reasons: a test that drives compaction by hand needs the store to
    /// hold still, and a caller that would rather compact when it knows it
    /// is idle needs the store not to decide for it. `compact_once` is
    /// still there for both.
    ///
    /// A store with this set still *reports* that it has fallen behind, in
    /// `roble.compaction.debt`. It simply does nothing about it.
    pub fn disable_automatic_compactions(mut self, disable: bool) -> Self {
        self.disable_automatic_compactions = disable;
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

    /// How many tables L0 may hold before the picker wants it compacted
    /// (`crate::pick`).
    pub fn l0_compaction_threshold(mut self, tables: u64) -> Self {
        self.l0_compaction_threshold = tables;
        self
    }

    /// How many bytes L1 may hold, each level below it ten times more
    /// (`crate::pick`).
    pub fn lbase_max_bytes(mut self, bytes: u64) -> Self {
        self.lbase_max_bytes = bytes;
        self
    }

    /// How many entries a compaction merges before it hands the thread back
    /// to whatever else is ready (§9, `crate::work`).
    ///
    /// Zero merges straight through, which is what a caller wants when
    /// nothing else shares the thread — a bulk load, a repair tool — and what
    /// nothing else should want: a compaction that does not yield stops every
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
    pub fn meter_provider<P: MeterProvider + ?Sized>(mut self, provider: &P) -> Self {
        self.meter = Some(nanometer::Meter::new(provider, crate::metrics::SCOPE));
        self
    }

    /// Adds `attributes` to every series of the store's metrics, after
    /// `roble.store`: one of them with that key replaces the store's
    /// directory.
    pub fn metric_attributes(mut self, attributes: &[KeyValue]) -> Self {
        self.metric_attributes.extend_from_slice(attributes);
        self
    }
}
