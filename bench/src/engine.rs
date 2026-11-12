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

use roble_format::files as roble_files;

use crate::report::Report;
use crate::{Config, fail, key_for, value_for};

/// One table of one level, as an engine describes its own.
///
/// The columns are the intersection of what the three LSMs expose, which is
/// the point: a layout printed from this is diffable between them. RocksDB
/// also reports an entry count per file and roble would have to open a table
/// to learn one, so that column is left out rather than filled in for some
/// engines and dashed for others.
pub struct TableInfo {
    pub level: usize,
    /// What the engine calls the file, as it names it on disk.
    pub name: String,
    pub size: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

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

    /// Every table, with its level, size and key range — what `--layout`
    /// prints. `None` for an engine with no tables.
    ///
    /// The counts in [`Store::levels`] say how much merging a scan does; this
    /// says what it merges, which is what makes two engines' shapes
    /// comparable rather than just differently-sized.
    fn layout(&self) -> Option<Vec<TableInfo>> {
        None
    }

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
    /// The length of the value for `key`, or `None` if it is not there.
    ///
    /// A length and not the value, because **every engine here can produce a
    /// value without handing back an owned copy** and they do it differently:
    /// roble reads into a `Lookup` the caller keeps, Pebble's `Get` borrows
    /// the value from the block it is in and hands back a closer, RocksDB has
    /// `get_pinned`, and encina passes the borrowed value to a closure. An
    /// owned return would charge some of them for an allocation the
    /// comparison is not about while others borrowed, which is what this row
    /// used to do.
    ///
    /// A length still proves the value was read, which `Some(())` would not,
    /// and the phase checks it against `--value-bytes`.
    async fn get(&self, key: &[u8]) -> Option<usize>;

    /// The greatest key at or below `probe`, if any.
    ///
    /// The probes this is given do not exist, which is what makes it a seek:
    /// see the `floor` phase.
    async fn floor(&self, probe: &[u8]) -> Option<Vec<u8>>;

    /// Closes every store and opens them again, so the next phase reads
    /// nothing the last one warmed.
    ///
    /// What a cold `get` needs, and the reason it is a trait method rather
    /// than a second process: every cache an engine holds has to go, and only
    /// the engine knows what it holds. Each implementation drops its stores —
    /// which for roble is also what releases the directory lock — and opens
    /// them as `open` did, with caches of their own.
    async fn reopen(shards: Vec<Self>, cfg: &Config, dir: &Path) -> Vec<Self> {
        drop(shards);
        Self::open(cfg, dir).await
    }

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
    /// The working space `get` hands back on every call. One per store,
    /// as Oxia holds one store per shard and reads them from one thread.
    ///
    /// A `Cell` and not a `RefCell`: it is moved out for the call and back
    /// after, so there is no borrow to hold across the await inside. Clippy
    /// refuses that borrow, and is right to — it is the hazard that decided
    /// a `Lookup` belongs to the caller in the first place.
    lookup: std::cell::Cell<roble::Lookup>,
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
                    .memtable_size(cfg.memtable_mb * 1024 * 1024)
                    // Oxia's configuration: its replication log is the source
                    // of truth, so the store need only be durable at a flush.
                    .disable_wal(!cfg.wal),
            )
            .await
            .unwrap_or_else(|e| fail(&format!("open {}: {e}", at.display())));
            shards.push(Roble {
                db,
                lookup: std::cell::Cell::new(roble::Lookup::new()),
            });
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

    fn layout(&self) -> Option<Vec<TableInfo>> {
        let versions = self.db.versions();
        let mut out = Vec::new();
        for (level, tables) in versions.version.levels() {
            for t in tables {
                out.push(TableInfo {
                    level: level as usize,
                    name: roble_files::table_filename(t.file_num),
                    size: t.size,
                    smallest: t.smallest_user_key().unwrap_or_default().to_vec(),
                    largest: t.largest_user_key().unwrap_or_default().to_vec(),
                });
            }
        }
        Some(out)
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

    async fn get(&self, key: &[u8]) -> Option<usize> {
        // Moved out for the call and put back after. Moving it is a few
        // pointers either way, and the buffers keep the capacity they
        // reached, which is the whole point of handing one back.
        let mut buf = self.lookup.take();
        let found = self
            .db
            .get_into(key, roble::Comparison::Equal, &mut buf)
            .await
            .unwrap_or_else(|e| fail(&format!("get_into: {e}")));
        let len = found.then(|| buf.value().len());
        self.lookup.set(buf);
        len
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

    async fn get(&self, key: &[u8]) -> Option<usize> {
        // The closure is handed the value borrowed, so the length comes out
        // without a copy; `|_, v| v.to_vec()` was the copy this row used to
        // charge encina for.
        self.db
            .get(key, encina::Comparison::Equal, |_, v| v.len())
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
            // Every knob roble and Pebble agree on — roble took Pebble's
            // defaults (§4, §9) — set on RocksDB too, because its own are
            // different enough to decide the result rather than inform it.
            //
            // The memtable was the first: RocksDB's default is 64 MiB against
            // their 4, so left alone it buffered this whole workload and
            // flushed nothing during the load while the other two wrote
            // 30 MiB of tables.
            //
            // The target file size matters more. RocksDB's default is 64 MiB
            // against their 2, so it settled this store into a *single* table
            // where roble leaves eleven and Pebble nine — and a point lookup
            // searching one table instead of three is most of why RocksDB
            // looked quickest at it. That is a setting, not an engine.
            options.set_write_buffer_size(cfg.memtable_mb as usize * 1024 * 1024);
            options.set_target_file_size_base(roble::compact::TARGET_FILE_SIZE);
            options.set_max_bytes_for_level_base(roble::pick::LBASE_MAX_BYTES);
            options.set_level_zero_file_num_compaction_trigger(
                roble::pick::L0_COMPACTION_THRESHOLD as i32,
            );
            options
                .set_level_zero_stop_writes_trigger(roble::pick::L0_STOP_WRITES_THRESHOLD as i32);
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
        // Both, because `--no-wal` means what it says for the other two:
        // roble takes `disable_wal` and Pebble `DisableWAL`, so setting only
        // the sync flag here left RocksDB writing a journal the others were
        // not — which showed up as its directory being 5 MiB larger than
        // theirs during the load, and as a `journal off` rate measured with a
        // journal still being written.
        write.set_sync(sync);
        write.disable_wal(!sync);
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

    fn layout(&self) -> Option<Vec<TableInfo>> {
        let files = self
            .db
            .live_files()
            .unwrap_or_else(|e| fail(&format!("live files: {e}")));
        Some(
            files
                .into_iter()
                .map(|f| TableInfo {
                    level: f.level.max(0) as usize,
                    name: f.name.trim_start_matches('/').to_string(),
                    size: f.size as u64,
                    smallest: f.start_key.unwrap_or_default(),
                    largest: f.end_key.unwrap_or_default(),
                })
                .collect(),
        )
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

    async fn get(&self, key: &[u8]) -> Option<usize> {
        // `get_pinned` borrows the value out of the block it is in, where
        // `get` copies it into a fresh `Vec`. The same distinction as roble's
        // `get_into` and Pebble's closer, reached a third way.
        self.db
            .get_pinned(key)
            .unwrap_or_else(|e| fail(&format!("get: {e}")))
            .map(|v| v.len())
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
