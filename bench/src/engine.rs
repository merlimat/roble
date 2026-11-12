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

//! The engines, behind one trait.
//!
//! Every phase is written once against [`Store`] and run once per engine, so
//! what differs between two columns is the engine and not the loop that drove
//! it. Each engine still runs its own batch and its own iteration, because
//! those are not the same shape and a shared abstraction over them would be a
//! lie about at least one: roble and Pebble seek and step a position, RocksDB
//! hands out an iterator that owns its direction, and encina's only goes
//! forward. So the trait is drawn at the phase, not at the cursor — a batch
//! method is given the range of records to write and builds the engine's own
//! batch from the shared [`crate::key_for`] and [`crate::value_for`], which is
//! what keeps the bytes identical without flattening the APIs into one.
//!
//! What an engine cannot do it says so rather than pretending: see
//! [`Store::reverse`] and [`Store::checkpoint`].

use std::path::Path;
use std::time::Instant;

use crate::report::Report;
use crate::{Config, fail, key_for, value_for};

/// One store of one engine — a shard, in Oxia's terms.
#[allow(async_fn_in_trait)]
pub trait Store: Sized {
    /// What to call this engine on the command line and in the output.
    const NAME: &'static str;

    /// Opens `cfg.shards` stores under `dir`, sharing one cache budget
    /// between them as Oxia shares one between the shards on a node (§8).
    async fn open(cfg: &Config, dir: &Path) -> Vec<Self>;

    /// Writes records `from..from + count` of shard `shard` as one batch, and
    /// commits it, durably if `sync`.
    async fn write_batch(&self, cfg: &Config, shard: usize, from: usize, count: usize, sync: bool);

    /// Leaves the store in a shape the read phases can be compared over, and
    /// says how many pieces of work it did.
    ///
    /// For an engine that compacts on the caller's thread this does the work;
    /// for one that compacts in the background it waits. The `cpu s` column
    /// is what makes those two comparable.
    async fn settle(&self) -> u64;

    /// How many tables sit at each level, or `None` for an engine with no
    /// levels to report.
    fn levels(&self) -> Option<[usize; 7]>;

    /// Whether this engine can iterate backwards at all.
    ///
    /// encina cannot: its `Iter` has `next` and nothing else, so `scan-rev`
    /// has nothing to call. Worth saying out loud rather than quietly
    /// reporting a zero, because reverse iteration is on an Oxia serving path
    /// (§14) and an engine without it could not serve that path.
    fn reverse(&self) -> bool {
        true
    }

    /// Steps every record, forward or backward, sampling steps into `report`,
    /// and returns how many it saw.
    async fn scan(&self, reverse: bool, report: &mut Report) -> u64;

    /// The value at `key`, if any.
    async fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// The greatest key at or below `probe`, if any.
    ///
    /// The probes this is given do not exist, which is what makes it a seek:
    /// see the `floor` phase.
    async fn floor(&self, probe: &[u8]) -> Option<Vec<u8>>;

    /// Writes a checkpoint into `at`, or `false` if the engine has none.
    async fn checkpoint(&self, at: &Path) -> bool;

    /// What the store takes on disk.
    async fn size(&self) -> u64;
}

/// Steps a cursor that has already been positioned, sampling as it goes.
///
/// The sampling rule lives here so that all four engines are charged the same
/// measurement overhead: `step` is called once per record and times one call
/// in [`Report::SAMPLE`].
macro_rules! step_loop {
    ($report:expr, $valid:expr, $read:expr, $step:expr) => {{
        let mut seen = 0u64;
        while $valid {
            $read;
            seen += 1;
            let at = $report.sampling(seen).then(Instant::now);
            $step;
            if let Some(at) = at {
                $report.saw(at.elapsed());
            }
        }
        seen
    }};
}

// --------------------------------------------------------------------- roble

/// roble, the subject.
pub struct Roble {
    db: roble::Opened<roble::FileFs>,
}

impl Store for Roble {
    const NAME: &'static str = "roble";

    async fn open(cfg: &Config, dir: &Path) -> Vec<Roble> {
        // One cache handle for every shard, which is the arrangement Oxia
        // runs and the one §8 is built for: hundreds of shards in one budget.
        let caches = roble::Caches::new(
            roble::cache::DEFAULT_OPEN_TABLES,
            cfg.cache_mb as usize * 1024 * 1024,
        );
        let mut shards = Vec::with_capacity(cfg.shards);
        for i in 0..cfg.shards {
            let at = dir.join(format!("shard-{i:04}"));
            let db = roble::open_with(
                &roble::DefaultEnv,
                &at,
                roble::Options::new()
                    .create_if_missing(true)
                    .caches(caches.clone())
                    // Oxia's configuration: its replication log is the source
                    // of truth, so the store need only be durable at a flush.
                    .disable_wal(!cfg.wal),
            )
            .await
            .unwrap_or_else(|e| fail(&format!("open {}: {e}", at.display())));
            shards.push(Roble { db });
        }
        shards
    }

    async fn write_batch(&self, cfg: &Config, shard: usize, from: usize, count: usize, sync: bool) {
        // An *indexed* batch, which is the one Oxia uses: its `PebbleBatch`
        // reads back through itself before committing, so the repr measured
        // here is the one built under an index.
        let mut batch = self.db.batch();
        for i in from..from + count {
            batch
                .put(&key_for(shard, i), &value_for(cfg, shard, i))
                .unwrap_or_else(|e| fail(&format!("put: {e}")));
        }
        batch
            .commit(sync)
            .await
            .unwrap_or_else(|e| fail(&format!("commit: {e}")));
    }

    async fn settle(&self) -> u64 {
        self.db
            .flush()
            .await
            .unwrap_or_else(|e| fail(&format!("flush: {e}")));
        // roble compacts cooperatively on the thread that owns the store
        // (§9), so this is the caller's to drive and its cost is counted here
        // rather than in a background total.
        let mut ran = 0;
        while self.db.pick().is_some() {
            self.db
                .compact_once()
                .await
                .unwrap_or_else(|e| fail(&format!("compact: {e}")));
            ran += 1;
            if ran > 100_000 {
                fail("the picker never settled");
            }
        }
        ran
    }

    fn levels(&self) -> Option<[usize; 7]> {
        let mut counts = [0usize; 7];
        for (level, tables) in self.db.versions().version.levels() {
            counts[level as usize] = tables.len();
        }
        Some(counts)
    }

    async fn scan(&self, reverse: bool, report: &mut Report) -> u64 {
        let mut c = self
            .db
            .scan()
            .await
            .unwrap_or_else(|e| fail(&format!("scan: {e}")));
        if reverse {
            c.last().await
        } else {
            c.first().await
        }
        .unwrap_or_else(|e| fail(&format!("seek: {e}")));
        step_loop!(
            report,
            c.is_valid(),
            std::hint::black_box((c.key(), c.value())),
            if reverse {
                c.prev().await
            } else {
                c.next().await
            }
            .unwrap_or_else(|e| fail(&format!("step: {e}")))
        )
    }

    async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db
            .get(key, roble::Comparison::Equal)
            .await
            .unwrap_or_else(|e| fail(&format!("get: {e}")))
            .map(|f| f.value)
    }

    async fn floor(&self, probe: &[u8]) -> Option<Vec<u8>> {
        self.db
            .get(probe, roble::Comparison::Floor)
            .await
            .unwrap_or_else(|e| fail(&format!("floor: {e}")))
            .map(|f| f.key)
    }

    async fn checkpoint(&self, at: &Path) -> bool {
        self.db
            .checkpoint(at)
            .await
            .unwrap_or_else(|e| fail(&format!("checkpoint: {e}")));
        true
    }

    async fn size(&self) -> u64 {
        self.db.disk_usage().await.unwrap_or(0)
    }
}

// -------------------------------------------------------------------- encina

/// encina: a copy-on-write B+tree, and deliberately not apples-to-apples.
///
/// §2 of DESIGN.md claims the price of Pebble compatibility is worth paying
/// without putting a number on it, and §12 asks for this comparison for that
/// reason: no compaction, no tombstones to step over, a format chosen to suit
/// its own data, and one file rather than a directory of them. The gap is the
/// price.
#[cfg(feature = "encina")]
pub struct Encina {
    db: encina::Db,
    path: std::path::PathBuf,
}

#[cfg(feature = "encina")]
impl Store for Encina {
    const NAME: &'static str = "encina";

    async fn open(cfg: &Config, dir: &Path) -> Vec<Encina> {
        let mut shards = Vec::with_capacity(cfg.shards);
        for i in 0..cfg.shards {
            // A file, not a directory: encina is one file per store. The
            // cache budget is divided between the shards rather than shared,
            // because an encina store's cache is its own (§8 says this is
            // where roble differs on purpose), so the total held is the same.
            let at = dir.join(format!("shard-{i:04}.encina"));
            let per_shard = (cfg.cache_mb as usize * 1024 * 1024) / cfg.shards;
            let db = encina::Db::open(
                &encina::DefaultEnv,
                &at,
                encina::Options::new().cache_size(per_shard),
            )
            .await
            .unwrap_or_else(|e| fail(&format!("open {}: {e}", at.display())));
            shards.push(Encina {
                db,
                path: at.clone(),
            });
        }
        shards
    }

    async fn write_batch(
        &self,
        cfg: &Config,
        shard: usize,
        from: usize,
        count: usize,
        _sync: bool,
    ) {
        // encina's write is durable by itself: there is no sync flag to pass,
        // so a `--no-wal` run asks it for less than it gives. That is in its
        // favour on the load row and against it nowhere, which the README
        // says rather than leaving to be noticed.
        let mut batch = encina::WriteBatch::new();
        for i in from..from + count {
            batch.put(&key_for(shard, i), &value_for(cfg, shard, i));
        }
        self.db
            .write(&mut batch)
            .await
            .unwrap_or_else(|e| fail(&format!("write: {e}")));
    }

    /// encina does nothing in the background, so there is nothing to settle.
    async fn settle(&self) -> u64 {
        0
    }

    /// No levels: a B+tree has one structure, not seven.
    fn levels(&self) -> Option<[usize; 7]> {
        None
    }

    /// encina's `Iter` has `next` and no `prev`.
    fn reverse(&self) -> bool {
        false
    }

    async fn scan(&self, reverse: bool, report: &mut Report) -> u64 {
        if reverse {
            fail("encina cannot iterate backwards; the phase should have been skipped");
        }
        let mut iter = self.db.range(..);
        let mut seen = 0u64;
        loop {
            let at = report.sampling(seen + 1).then(Instant::now);
            let next = iter
                .next()
                .await
                .unwrap_or_else(|e| fail(&format!("scan: {e}")));
            if let Some(at) = at {
                report.saw(at.elapsed());
            }
            match next {
                Some((k, v)) => {
                    std::hint::black_box((k, v));
                    seen += 1;
                }
                None => break,
            }
        }
        seen
    }

    async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db
            .get(key, encina::Comparison::Equal, |_, v| v.to_vec())
            .await
            .unwrap_or_else(|e| fail(&format!("get: {e}")))
    }

    async fn floor(&self, probe: &[u8]) -> Option<Vec<u8>> {
        self.db
            .get(probe, encina::Comparison::Floor, |k, _| k.to_vec())
            .await
            .unwrap_or_else(|e| fail(&format!("floor: {e}")))
    }

    /// encina has no checkpoint of its own.
    async fn checkpoint(&self, _at: &Path) -> bool {
        false
    }

    async fn size(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }
}

// ------------------------------------------------------------------- rocksdb

/// RocksDB, the engine Pebble was written to replace and the one most of this
/// workload's prior art is measured against.
#[cfg(feature = "rocksdb")]
pub struct Rocks {
    db: rocksdb::DB,
    path: std::path::PathBuf,
}

#[cfg(feature = "rocksdb")]
impl Store for Rocks {
    const NAME: &'static str = "rocksdb";

    async fn open(cfg: &Config, dir: &Path) -> Vec<Rocks> {
        // The settings Oxia gives Pebble, as closely as RocksDB spells them:
        // 16 KiB blocks, Snappy, 10-bit Bloom filters, and one LRU block
        // cache shared by every shard.
        let cache = rocksdb::Cache::new_lru_cache(cfg.cache_mb as usize * 1024 * 1024);
        let mut shards = Vec::with_capacity(cfg.shards);
        for i in 0..cfg.shards {
            let at = dir.join(format!("shard-{i:04}"));
            let mut table = rocksdb::BlockBasedOptions::default();
            table.set_block_size(16 * 1024);
            table.set_block_cache(&cache);
            table.set_bloom_filter(10.0, false);
            let mut options = rocksdb::Options::default();
            options.create_if_missing(true);
            options.set_block_based_table_factory(&table);
            options.set_compression_type(rocksdb::DBCompressionType::Snappy);
            let db = rocksdb::DB::open(&options, &at)
                .unwrap_or_else(|e| fail(&format!("open {}: {e}", at.display())));
            shards.push(Rocks {
                db,
                path: at.clone(),
            });
        }
        shards
    }

    async fn write_batch(&self, cfg: &Config, shard: usize, from: usize, count: usize, sync: bool) {
        let mut batch = rocksdb::WriteBatch::default();
        for i in from..from + count {
            batch.put(key_for(shard, i), value_for(cfg, shard, i));
        }
        let mut write = rocksdb::WriteOptions::default();
        write.set_sync(sync);
        self.db
            .write_opt(batch, &write)
            .unwrap_or_else(|e| fail(&format!("write: {e}")));
    }

    async fn settle(&self) -> u64 {
        self.db
            .flush()
            .unwrap_or_else(|e| fail(&format!("flush: {e}")));
        // RocksDB compacts on threads of its own, so this waits rather than
        // doing the work — the same asymmetry Pebble has against roble, and
        // the reason to read `cpu s` and not the rate.
        let mut options = rocksdb::WaitForCompactOptions::default();
        options.set_flush(true);
        self.db
            .wait_for_compact(&options)
            .unwrap_or_else(|e| fail(&format!("wait for compactions: {e}")));
        1
    }

    fn levels(&self) -> Option<[usize; 7]> {
        let mut counts = [0usize; 7];
        for (level, count) in counts.iter_mut().enumerate() {
            let name = format!("rocksdb.num-files-at-level{level}");
            *count = self
                .db
                .property_int_value(&name)
                .unwrap_or(None)
                .unwrap_or(0) as usize;
        }
        Some(counts)
    }

    async fn scan(&self, reverse: bool, report: &mut Report) -> u64 {
        let mode = if reverse {
            rocksdb::IteratorMode::End
        } else {
            rocksdb::IteratorMode::Start
        };
        let mut iter = self.db.iterator(mode);
        let mut seen = 0u64;
        loop {
            let at = report.sampling(seen + 1).then(Instant::now);
            let next = iter.next();
            if let Some(at) = at {
                report.saw(at.elapsed());
            }
            match next {
                Some(Ok((k, v))) => {
                    std::hint::black_box((&k, &v));
                    seen += 1;
                }
                Some(Err(e)) => fail(&format!("scan: {e}")),
                None => break,
            }
        }
        seen
    }

    async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db
            .get(key)
            .unwrap_or_else(|e| fail(&format!("get: {e}")))
    }

    async fn floor(&self, probe: &[u8]) -> Option<Vec<u8>> {
        // `From(.., Reverse)` is RocksDB's `SeekForPrev`: the last key at or
        // below the probe. Oxia reaches the same place in Pebble with an
        // iterator bounded above by the probe and `Last()` called on it.
        let mut iter = self.db.iterator(rocksdb::IteratorMode::From(
            probe,
            rocksdb::Direction::Reverse,
        ));
        match iter.next() {
            Some(Ok((k, _))) => Some(k.to_vec()),
            Some(Err(e)) => fail(&format!("floor: {e}")),
            None => None,
        }
    }

    /// RocksDB's checkpoint, which is where Pebble's came from.
    async fn checkpoint(&self, at: &Path) -> bool {
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db)
            .unwrap_or_else(|e| fail(&format!("checkpoint: {e}")));
        checkpoint
            .create_checkpoint(at)
            .unwrap_or_else(|e| fail(&format!("checkpoint: {e}")));
        true
    }

    async fn size(&self) -> u64 {
        let mut total = 0;
        let mut stack = vec![self.path.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                match entry.metadata() {
                    Ok(m) if m.is_dir() => stack.push(entry.path()),
                    Ok(m) => total += m.len(),
                    Err(_) => {}
                }
            }
        }
        total
    }
}
