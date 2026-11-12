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

//! Benchmarks roble, encina and RocksDB on the workload Oxia runs, in the
//! phases and the output format `harness/bench` reports for Go Pebble, so all
//! four can be read side by side. See README.md.
//!
//! A run benchmarks one engine in a process of its own, so the CPU time and
//! the size it reports are that engine's alone.
//!
//! **What this exists to answer.** §14 of DESIGN.md leaves several questions
//! waiting on a measurement, and the sharpest is reverse iteration: Oxia's
//! `KeyRangeScanReverse` is on a serving path, an LSM pays more for `prev`
//! than for `next`, and the design says so without saying by how much. The
//! `scan-rev` phase is that number, beside the `scan` phase that is its
//! control. §12 asks for the encina comparison for a different reason: §2
//! claims the price of Pebble compatibility is worth paying without ever
//! putting a number on it, and the gap between roble and a B+tree that owes
//! nobody compatibility is that price.

mod engine;
mod report;

// Behind a feature, so the default build is measured on the platform's own
// allocator — which is what a consumer gets unless it chooses otherwise.
#[cfg(all(feature = "mimalloc", feature = "count-alloc"))]
compile_error!(
    "`mimalloc` and `count-alloc` each install a global allocator, so only one \
     can be on at a time: build twice and compare the two runs"
);

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "count-alloc")]
mod counting {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    pub static ALLOCS: AtomicU64 = AtomicU64::new(0);
    pub static BYTES: AtomicU64 = AtomicU64::new(0);

    pub struct Counting;

    // SAFETY: every method forwards to `System` with the layout it was given
    // and returns what `System` returned, so the allocator contract is
    // whatever `System`'s is. The counters are the only addition.
    #[allow(unsafe_code)]
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(layout.size() as u64, Relaxed);
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(new.saturating_sub(layout.size()) as u64, Relaxed);
            unsafe { System.realloc(ptr, layout, new) }
        }
    }
}

#[cfg(feature = "count-alloc")]
#[global_allocator]
static COUNTING: counting::Counting = counting::Counting;

use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, Instant};

use engine::Store;
use report::{Output, Report};

const USAGE: &str = "\
usage: roble-bench --dir DIR [--engine roble|encina|rocksdb] [options]

Creates the store at DIR/<engine>, which must not exist yet.

  --engine NAME   which engine to benchmark (roble). encina and rocksdb have
                  to be compiled in: --features encina, --features rocksdb
  --shards N      stores to open, as Oxia opens one per shard (1)
  --batches N     batches written to each shard (200)
  --records N     records in each batch (100)
  --value-bytes N bytes in each value (128)
  --reads N       lookups in each get and floor phase (100000)
  --cache-mb N    block cache for every shard together, in MiB (64)
  --value-chunk N bytes of random data each value is built from, repeated to
                  fill it (default: the whole value, so incompressible). 16
                  makes a 128-byte value eight copies of one chunk, which
                  Snappy halves — the dial between the two extremes §8 needs
  --memtable-mb N bytes a memtable holds before a flush, in MiB (4). Set on
                  every engine that has one, because their own defaults differ
                  by sixteen times and the load phase then measures different
                  amounts of work
  --seed N        seed for the keys, the values and the lookup order (1)
  --no-wal        turn the WAL off, as Oxia runs it: a flush is then the
                  only durability point, and a commit syncs nothing
  --reuse         open the store that is already at DIR/<engine> instead of
                  refusing it, so that a load and the reads over it can be two
                  processes with something done in between — dropping the
                  page cache, which needs root and so cannot be done from
                  here. Without it a run insists on a directory of its own.
  --layout        print every table of every level, with its size and key
                  range, after the store has settled
  --tsv           one tab-separated line per phase instead of a table, for a
                  script that puts several engines side by side
  --phases LIST   comma-separated subset of
                  load,settle,scan-rev,scan,get,floor,get-cold,checkpoint
                  (always run in that order, whatever order they are named in)";

fn main() {
    let cfg = Config::parse();
    let path = cfg.dir.join(&cfg.engine);
    if path.exists() && !cfg.reuse {
        fail(&format!(
            "{} exists: remove it first, or pass --reuse to read the store already there",
            path.display()
        ));
    }
    if cfg.reuse && !path.exists() {
        fail(&format!(
            "--reuse was given but {} does not exist: load it first",
            path.display()
        ));
    }
    std::fs::create_dir_all(&path).expect("create the directory");
    let runtime = compio::runtime::Runtime::new().expect("start the runtime");
    runtime.block_on(async {
        match cfg.engine.as_str() {
            "roble" => run::<engine::Roble>(&cfg, &path).await,
            #[cfg(feature = "encina")]
            "encina" => run::<engine::Encina>(&cfg, &path).await,
            #[cfg(feature = "rocksdb")]
            "rocksdb" => run::<engine::Rocks>(&cfg, &path).await,
            // Named but not compiled in, which is a different thing from
            // misspelled and worth a different message. One arm each, so
            // that enabling one feature does not make the other's message
            // unreachable.
            #[cfg(not(feature = "encina"))]
            "encina" => fail("encina was not compiled in: rebuild with --features encina"),
            #[cfg(not(feature = "rocksdb"))]
            "rocksdb" => fail("rocksdb was not compiled in: rebuild with --features rocksdb"),
            other => fail(&format!("unknown engine {other}\n\n{USAGE}")),
        }
    });
}

async fn run<S: Store>(cfg: &Config, path: &Path) {
    let out = Output {
        tsv: cfg.tsv,
        engine: S::NAME,
    };
    // The header and the notes are for a person reading one run; the table
    // across engines is assembled from `--tsv` and needs none of them.
    if cfg.tsv {
        return run_phases::<S>(cfg, path, out).await;
    }
    println!(
        "{}: {} shards x {} batches x {} records, {}-byte values, {} MiB cache, \
         wal={}, seed={}",
        S::NAME,
        cfg.shards,
        cfg.batches,
        cfg.records,
        cfg.value_bytes,
        cfg.cache_mb,
        if cfg.wal { "on" } else { "off" },
        cfg.seed,
    );
    // Said here rather than only in the README, so it travels with the
    // numbers. Go's `os.File.Sync` is `fcntl(F_FULLFSYNC)` on macOS, which
    // flushes the drive's own write cache; compio's and RocksDB's are
    // `fsync`, which does not. On Linux — which is what Oxia runs — they all
    // are, and the load rows are comparable.
    if cfg.wal && cfg.wants("load") && cfg!(target_os = "macos") {
        println!("note: on macOS Pebble's sync is F_FULLFSYNC where this engine's is fsync, so");
        println!("note: the load row is not comparable with Pebble's. Run every engine with");
        println!("note: --no-wal, which is Oxia's own configuration, for a load row that is.");
    }
    println!("{}", Report::header());
    run_phases::<S>(cfg, path, out).await;
}

async fn run_phases<S: Store>(cfg: &Config, path: &Path, out: Output) {
    let shards = S::open(cfg, path).await;

    if cfg.wants("load") {
        load(cfg, &shards, out).await;
    }
    if cfg.wants("settle") {
        settle(&shards, out).await;
    }
    // What shape the load and the settle left, because a scan's cost is
    // mostly how many iterators it has to merge: one table in one level is
    // not the same work as ten in L0, and without this line a reader cannot
    // tell a slow iterator from a differently-shaped store.
    if (cfg.wants("scan") || cfg.wants("scan-rev") || cfg.layout) && !cfg.tsv {
        println!("levels {}", level_shape(&shards));
    }
    if cfg.layout && !cfg.tsv {
        print_layout(&shards);
    }
    // Reverse before forward, deliberately. The phases share a cache and run
    // in order, so whichever goes second reads one the first warmed — and the
    // question §14 asks is whether reverse iteration is *worse*. Putting it
    // first points that bias against it, so a `scan-rev` that keeps up here
    // is keeping up from behind. A cold comparison of either needs its own
    // process: `--phases load,scan` and `--phases load,scan-rev`.
    if cfg.wants("scan-rev") {
        scan(cfg, &shards, true, out).await;
    }
    if cfg.wants("scan") {
        scan(cfg, &shards, false, out).await;
    }
    if cfg.wants("get") {
        lookups(cfg, &shards, false, out).await;
    }
    if cfg.wants("floor") {
        lookups(cfg, &shards, true, out).await;
    }
    // Last of the read phases, because it closes the stores and opens them
    // again: everything above it wants the caches the one before left.
    let shards = if cfg.wants("get-cold") {
        let shards = S::reopen(shards, cfg, path).await;
        cold_get(cfg, &shards, out).await;
        shards
    } else {
        shards
    };
    if cfg.wants("checkpoint") {
        checkpoint(&shards, path, out).await;
    }

    if !cfg.tsv {
        println!("peak rss {} MiB", peak_rss_mib());
    } else {
        println!("{}\tpeak-rss-mib\t{}", out.engine, peak_rss_mib());
    }
    #[cfg(feature = "count-alloc")]
    {
        use std::sync::atomic::Ordering::Relaxed;
        let (allocs, bytes) = (
            counting::ALLOCS.load(Relaxed),
            counting::BYTES.load(Relaxed),
        );
        if cfg.tsv {
            println!("{}\tallocs\t{allocs}\t{bytes}", out.engine);
        } else {
            println!(
                "{allocs} allocations, {:.1} MiB asked for",
                bytes as f64 / (1024.0 * 1024.0)
            );
        }
    }
}

/// Writes every batch of every shard, timing each commit.
async fn load<S: Store>(cfg: &Config, shards: &[S], out: Output) {
    let mut report = Report::new("load", out);
    for (s, store) in shards.iter().enumerate() {
        for b in 0..cfg.batches {
            let at = Instant::now();
            store
                .write_batch(cfg, s, b * cfg.records, cfg.records, cfg.wal)
                .await;
            report.saw(at.elapsed());
            report.count += cfg.records as u64;
        }
    }
    report.finish(size_of(shards).await);
}

/// Leaves every shard in a shape the read phases can be compared over.
async fn settle<S: Store>(shards: &[S], out: Output) {
    let mut report = Report::new("settle", out);
    for store in shards {
        let at = Instant::now();
        let did = store.settle().await;
        report.saw(at.elapsed());
        report.count += did;
    }
    report.finish(size_of(shards).await);
}

/// Steps every record of every shard, forward or backward.
async fn scan<S: Store>(cfg: &Config, shards: &[S], reverse: bool, out: Output) {
    let name = if reverse { "scan-rev" } else { "scan" };
    if reverse && !shards.first().is_some_and(|s| s.reverse()) {
        Report::skipped(name, "this engine cannot iterate backwards", out);
        return;
    }
    let mut report = Report::new(name, out);
    for store in shards {
        let seen = store.scan(reverse, &mut report).await;
        report.count += seen;
    }
    let expected = (cfg.shards * cfg.batches * cfg.records) as u64;
    if report.count != expected {
        fail(&format!(
            "the {name} saw {} records, not the {expected} that were written",
            report.count
        ));
    }
    report.finish(size_of(shards).await);
}

/// Looks up random keys, every one of which has an answer.
///
/// `get` is Oxia's `Get` with `ComparisonEqual`, and the one comparison a
/// bloom filter can answer.
///
/// `floor` probes keys that are **not there**, one byte past a stored key:
/// Oxia's `getFloor` tries an exact `Get` first and only falls through to a
/// seek when that misses, so probing keys that exist would measure the `get`
/// phase over again. What this times is the seek, which no filter can help.
async fn lookups<S: Store>(cfg: &Config, shards: &[S], floor: bool, out: Output) {
    let name = if floor { "floor" } else { "get" };
    let mut report = Report::new(name, out);
    let total = cfg.batches * cfg.records;
    let mut rng = Rng::new(cfg.seed ^ 0x9e37_79b9);
    for _ in 0..cfg.reads {
        let s = (rng.next() as usize) % cfg.shards;
        let i = (rng.next() as usize) % total;
        let key = key_for(s, i);
        let mut probe = key.clone();
        if floor {
            // Past `key-%010d` and below the next one, since every byte here
            // sorts above the digits the padding ends in.
            probe.push(b'~');
        }
        let at = Instant::now();
        let answer = if floor {
            shards[s].floor(&probe).await
        } else {
            shards[s].get(&probe).await
        };
        report.saw(at.elapsed());
        let answer = answer.unwrap_or_else(|| {
            fail(&format!(
                "{name} found no answer for {:?}",
                String::from_utf8_lossy(&probe)
            ))
        });
        if floor && answer != key {
            fail(&format!(
                "{name} answered {:?}, not the key below the probe",
                String::from_utf8_lossy(&answer)
            ));
        }
        report.count += 1;
    }
    report.finish(size_of(shards).await);
}

/// One point lookup per shard, on a store just reopened.
///
/// What the `get` phase cannot say. That one runs `--reads` lookups in a row,
/// after the scans have walked every block, so it reports a warm steady state:
/// the filters are cached, the indexes are cached, and most of the tables are
/// open. A shard Oxia has just been handed is in none of those states, and the
/// first read of it pays for the footer, the index and the filter of whatever
/// table holds the key.
///
/// Exactly one lookup per shard, which is the point — a second would be warm.
/// So `--shards 8` gives eight samples and `--shards 1` gives one, and the
/// percentile columns collapse onto it.
///
/// It is the engine's caches this empties, not the operating system's: the
/// blocks may still be in the page cache, so what this measures is the work
/// above the filesystem and not a seek to a spinning disk.
async fn cold_get<S: Store>(cfg: &Config, shards: &[S], out: Output) {
    let mut report = Report::new("get-cold", out);
    let total = cfg.batches * cfg.records;
    let mut rng = Rng::new(cfg.seed ^ 0x51ed_270b);
    for (s, store) in shards.iter().enumerate() {
        let key = key_for(s, (rng.next() as usize) % total);
        let at = Instant::now();
        let found = store.get(&key).await;
        report.saw(at.elapsed());
        if found.is_none() {
            fail(&format!(
                "get-cold found no answer for {:?}. A store opened without the WAL keeps only \
                 what a flush wrote, so this phase needs `settle` before it",
                String::from_utf8_lossy(&key)
            ));
        }
        report.count += 1;
    }
    report.finish(size_of(shards).await);
}

/// Checkpoints every shard, which is how Oxia moves one.
async fn checkpoint<S: Store>(shards: &[S], path: &Path, out: Output) {
    let mut report = Report::new("checkpoint", out);
    for (s, store) in shards.iter().enumerate() {
        let at = path.join(format!("checkpoint-{s:04}"));
        let started = Instant::now();
        if !store.checkpoint(&at).await {
            Report::skipped("checkpoint", "this engine has none", out);
            return;
        }
        report.saw(started.elapsed());
        report.count += 1;
    }
    report.finish(size_of(shards).await);
}

/// What every shard takes on disk, together.
async fn size_of<S: Store>(shards: &[S]) -> u64 {
    let mut total = 0;
    for store in shards {
        total += store.size().await;
    }
    total
}

/// Prints every table of every level: which file, how large, and what range
/// of keys it holds.
///
/// The `levels` line says how many iterators a scan merges; this says what it
/// merges them over, which is the difference between knowing that two engines
/// settled differently and seeing how. Sorted by level and then by key, not by
/// file number, because what matters is whether a level's tables partition the
/// key space or overlap it — the first is a scan that reads one table at a
/// time, the second is a scan that merges them all.
fn print_layout<S: Store>(shards: &[S]) {
    let mut tables: Vec<engine::TableInfo> =
        shards.iter().filter_map(|s| s.layout()).flatten().collect();
    if tables.is_empty() {
        println!("layout — this engine has no tables");
        return;
    }
    tables.sort_by(|a, b| {
        a.level
            .cmp(&b.level)
            .then_with(|| a.smallest.cmp(&b.smallest))
    });
    println!("layout {} tables", tables.len());
    for t in &tables {
        println!(
            "  L{} {:<16} {:>8.1} MiB  {} .. {}",
            t.level,
            t.name,
            t.size as f64 / (1024.0 * 1024.0),
            String::from_utf8_lossy(&t.smallest),
            String::from_utf8_lossy(&t.largest),
        );
    }
    // Whether each level below L0 really partitions the key space, which is
    // the property that lets a scan read its tables one after another instead
    // of merging them. L0 is allowed to overlap by design: it holds whole
    // flushed memtables.
    for level in 1..7 {
        let mut at: Vec<&engine::TableInfo> = tables.iter().filter(|t| t.level == level).collect();
        at.sort_by(|a, b| a.smallest.cmp(&b.smallest));
        let overlaps = at
            .windows(2)
            .filter(|p| p[1].smallest <= p[0].largest)
            .count();
        if overlaps > 0 {
            println!("  L{level}: {overlaps} overlapping pair(s), which a scan has to merge");
        }
    }
}

/// How many tables sit at each level, summed over the shards.
fn level_shape<S: Store>(shards: &[S]) -> String {
    let mut counts = [0usize; 7];
    let mut any = false;
    for store in shards {
        if let Some(levels) = store.levels() {
            any = true;
            for (level, n) in levels.iter().enumerate() {
                counts[level] += n;
            }
        }
    }
    if !any {
        return "— this engine has none".to_string();
    }
    counts
        .iter()
        .enumerate()
        .filter(|(_, n)| **n > 0)
        .map(|(l, n)| format!("L{l}={n}"))
        .collect::<Vec<_>>()
        .join(" ")
}

// -------------------------------------------------------- the keys and values

/// The key of record `i` in shard `s`.
///
/// Oxia's shape, with roble's one constraint on it: the keys are raw and
/// bytewise ordered, because the `oxia-slash-spans` comparer is one roble
/// refuses (§3), so a shard roble can serve is one using Oxia's *natural*
/// encoder. The id is zero-padded so that bytewise order is numeric order,
/// which is what makes the floor probes land where a reader expects.
pub fn key_for(s: usize, i: usize) -> Vec<u8> {
    let bucket = i % 64;
    format!("/shard-{s:04}/bucket-{bucket:02}/key-{i:010}").into_bytes()
}

/// The value of record `i` in shard `s`: pseudo-random bytes.
///
/// Random rather than a repeated byte, and this is not a detail. A value of
/// one byte over and over compresses to nothing, so a run with it would have
/// the size column say that Snappy is free and the uncompressed default of §8
/// is costing 2.5x — a measurement of the generator, not of Oxia's data. The
/// same generator fills them in every engine, so all the stores hold the same
/// bytes.
pub fn value_for(cfg: &Config, s: usize, i: usize) -> Vec<u8> {
    let mut rng = Rng::new(cfg.seed ^ ((s as u64) << 40) ^ (i as u64));
    // `--value-chunk` is the dial between incompressible and compressible: a
    // value is one random chunk repeated to fill it, so a chunk the size of
    // the value gives Snappy nothing to find, and a smaller one gives it an
    // exact repeat every `chunk` bytes. Real serialized records sit between
    // the two, sharing field tags and common strings without repeating
    // wholesale, which is why §8's question needs a dial rather than a
    // choice of two.
    let chunk = if cfg.value_chunk == 0 {
        cfg.value_bytes
    } else {
        cfg.value_chunk.min(cfg.value_bytes)
    };
    let mut pattern = Vec::with_capacity(chunk + 8);
    while pattern.len() < chunk {
        pattern.extend_from_slice(&rng.next().to_le_bytes());
    }
    pattern.truncate(chunk);
    let mut value = Vec::with_capacity(cfg.value_bytes);
    while value.len() < cfg.value_bytes {
        let take = chunk.min(cfg.value_bytes - value.len());
        value.extend_from_slice(&pattern[..take]);
    }
    value
}

// ------------------------------------------------------------------- the rest

/// xorshift64*, so the lookup order is the same on every run and in every
/// engine without pulling in a dependency for it. `harness/bench` has the
/// same generator, seeded the same way.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

/// What the run was asked for. The fields an engine needs to open itself or
/// to build a record are public; the rest only the phases read.
pub struct Config {
    dir: PathBuf,
    engine: String,
    /// Stores to open, as Oxia opens one per shard.
    pub shards: usize,
    batches: usize,
    records: usize,
    /// Bytes in each value.
    pub value_bytes: usize,
    /// Bytes of random data each value is built from, repeated to fill it.
    pub value_chunk: usize,
    reads: usize,
    /// Cache for every shard together, in MiB.
    pub cache_mb: u64,
    /// Bytes a memtable holds before a flush, in MiB.
    pub memtable_mb: u64,
    /// Seed for the keys, the values and the lookup order.
    pub seed: u64,
    /// Whether the WAL is on, which is also whether a commit syncs.
    pub wal: bool,
    layout: bool,
    reuse: bool,
    tsv: bool,
    phases: Option<Vec<String>>,
}

impl Config {
    fn parse() -> Self {
        let mut cfg = Config {
            dir: PathBuf::new(),
            engine: "roble".to_string(),
            shards: 1,
            batches: 200,
            records: 100,
            value_bytes: 128,
            value_chunk: 0,
            reads: 100_000,
            cache_mb: 64,
            memtable_mb: 4,
            seed: 1,
            wal: true,
            layout: false,
            reuse: false,
            tsv: false,
            phases: None,
        };
        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut saw_dir = false;
        let mut i = 0;
        while i < args.len() {
            let arg = args[i].as_str();
            // Every flag but the two that stand alone takes the next word.
            let value = |name: &str| -> &str {
                args.get(i + 1)
                    .map(String::as_str)
                    .unwrap_or_else(|| fail(&format!("{name} needs a value")))
            };
            let mut took_value = true;
            match arg {
                "--dir" => {
                    cfg.dir = PathBuf::from(value(arg));
                    saw_dir = true;
                }
                "--engine" => cfg.engine = value(arg).to_string(),
                "--shards" => cfg.shards = number(value(arg)) as usize,
                "--batches" => cfg.batches = number(value(arg)) as usize,
                "--records" => cfg.records = number(value(arg)) as usize,
                "--value-bytes" => cfg.value_bytes = number(value(arg)) as usize,
                "--value-chunk" => cfg.value_chunk = number(value(arg)) as usize,
                "--reads" => cfg.reads = number(value(arg)) as usize,
                "--cache-mb" => cfg.cache_mb = number(value(arg)),
                "--memtable-mb" => cfg.memtable_mb = number(value(arg)),
                "--seed" => cfg.seed = number(value(arg)),
                "--phases" => {
                    cfg.phases = Some(
                        value(arg)
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .collect(),
                    );
                }
                "--no-wal" => {
                    cfg.wal = false;
                    took_value = false;
                }
                "--layout" => {
                    cfg.layout = true;
                    took_value = false;
                }
                "--reuse" => {
                    cfg.reuse = true;
                    took_value = false;
                }
                "--tsv" => {
                    cfg.tsv = true;
                    took_value = false;
                }
                "-h" | "--help" => {
                    println!("{USAGE}");
                    process::exit(0);
                }
                other => fail(&format!("unknown argument {other}\n\n{USAGE}")),
            }
            i += if took_value { 2 } else { 1 };
        }
        if !saw_dir {
            fail(&format!("--dir is required\n\n{USAGE}"));
        }
        if cfg.shards == 0 || cfg.batches == 0 || cfg.records == 0 {
            fail("--shards, --batches and --records all have to be at least 1");
        }
        cfg
    }

    fn wants(&self, phase: &str) -> bool {
        match &self.phases {
            None => true,
            Some(chosen) => chosen.iter().any(|c| c == phase),
        }
    }
}

fn number(s: &str) -> u64 {
    s.parse()
        .unwrap_or_else(|_| fail(&format!("{s:?} is not a number")))
}

/// What the kernel says this process has used: CPU time and peak memory.
///
/// One call site for both, because both come out of one `getrusage`. On a
/// platform without it the columns read zero rather than being left out, so
/// that one engine's output is the shape of the others' wherever it runs.
#[cfg(unix)]
fn rusage() -> Option<libc::rusage> {
    // SAFETY: `getrusage` writes into the `rusage` it is given and reads
    // nothing else. The struct is zeroed first, so every field is initialised
    // whether or not the call fills it, and the pointer is to a live local.
    #[allow(unsafe_code)]
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        (libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0).then_some(usage)
    }
}

/// This process's CPU time, user and system, its threads included.
///
/// The column that makes the engines comparable at all: roble compacts on the
/// thread that owns the store (§9) while Pebble and RocksDB compact on
/// threads of their own, so wall-clock time alone would credit them with work
/// they still paid for.
pub fn cpu_time() -> Duration {
    #[cfg(unix)]
    {
        let Some(usage) = rusage() else {
            return Duration::ZERO;
        };
        let secs = |t: libc::timeval| {
            Duration::new(t.tv_sec as u64, (t.tv_usec as u32).saturating_mul(1000))
        };
        secs(usage.ru_utime) + secs(usage.ru_stime)
    }
    #[cfg(not(unix))]
    Duration::ZERO
}

/// Peak resident memory, in MiB.
fn peak_rss_mib() -> u64 {
    #[cfg(unix)]
    {
        let Some(usage) = rusage() else { return 0 };
        // `ru_maxrss` is bytes on macOS and kilobytes everywhere else.
        let bytes = if cfg!(target_os = "macos") {
            usage.ru_maxrss as u64
        } else {
            (usage.ru_maxrss as u64).saturating_mul(1024)
        };
        bytes / (1024 * 1024)
    }
    #[cfg(not(unix))]
    0
}

/// Says what went wrong and stops. A benchmark that cannot do what it was
/// asked has no number to report, so there is nothing to carry on with.
pub fn fail(message: &str) -> ! {
    eprintln!("roble-bench: {message}");
    process::exit(1);
}
