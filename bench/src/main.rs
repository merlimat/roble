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

//! Benchmarks roble, encina and RocksDB on the workload Oxia runs, so the
//! three can be read side by side. See README.md.
//!
//! Go Pebble had a driver of its own here, written to these phases and this
//! key shape, and it has been removed. Nothing replaces it: `cmd/pebble`'s
//! benchmarks build their own keys, so they measure Pebble on their workload
//! rather than on Oxia's. The README says what that costs.
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

// mimalloc by default, and `bench/Cargo.toml` says why: the platform's own
// allocator can decide a result by itself, and roble never chooses one.
// `--no-default-features` measures the system allocator instead.
//
// Counting installs its own `#[global_allocator]` and forwards to this one, so
// only one of the two statics may exist.
#[cfg(all(feature = "mimalloc", not(feature = "count-alloc")))]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "count-alloc")]
mod counting {
    use std::alloc::{GlobalAlloc, Layout};
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    // The allocator under the count is the one the build would use without
    // it, so counting never changes which allocator is being measured.
    #[cfg(feature = "mimalloc")]
    use mimalloc::MiMalloc as Base;
    #[cfg(not(feature = "mimalloc"))]
    use std::alloc::System as Base;

    pub static ALLOCS: AtomicU64 = AtomicU64::new(0);
    pub static BYTES: AtomicU64 = AtomicU64::new(0);

    pub struct Counting;

    // SAFETY: every method forwards to `Base` with the layout it was given and
    // returns what `Base` returned, so the allocator contract is whatever
    // `Base`'s is. The counters are the only addition.
    #[allow(unsafe_code)]
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(layout.size() as u64, Relaxed);
            unsafe { Base.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { Base.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(new.saturating_sub(layout.size()) as u64, Relaxed);
            unsafe { Base.realloc(ptr, layout, new) }
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
use report::{Latencies, Output, Report};

const USAGE: &str = "\
usage: roble-bench --dir DIR [--engine roble|encina|rocksdb] [options]

Creates the store at DIR/<engine>, which must not exist yet.

  --engine NAME   which engine to benchmark (roble). encina and rocksdb have
                  to be compiled in: --features encina, --features rocksdb
  --shards N      stores to open, as Oxia opens one per shard (1)
  --batches N     batches written to each shard (200)
  --records N     records in each batch (10000). The size matters more than it
                  looks: a copy-on-write B+tree writes a new root per commit,
                  so a load driven one record at a time is hours and gigabytes
                  where the same records batched are seconds. A thousand is
                  the floor below which per-commit cost starts to dominate
  --value-bytes N bytes in each value (128)
  --reads N       lookups in each get and floor phase (100000)
  --cache-mb N    block cache for every shard together, in MiB (64)
  --value-chunk N bytes of random data each value is built from, repeated to
                  fill it (default: the whole value, so incompressible). 16
                  makes a 128-byte value eight copies of one chunk, which
                  Snappy halves — the dial between the two extremes §8 needs
  --compression C none, snappy, lz4 or zstd[:level]. roble compresses L1 and
                  below with it and leaves L0 uncompressed, as Oxia
                  configures it (default snappy); it refuses lz4, which Go
                  Pebble cannot read. RocksDB compresses every level with it
                  (default snappy)
  --memtable-mb N bytes a memtable holds before a flush, in MiB (64). Set on
                  every engine that has one, because their own defaults differ
                  by sixteen times and the load phase then measures different
                  amounts of work. 64 is RocksDB's own default and the size
                  `pebble bench ycsb` hardcodes. **A memtable this big needs a
                  dataset well past it**: with less, the whole load fits in
                  one and nothing flushes, so the load row measures a skiplist
                  and the read phases run over whatever `settle` had to write
                  all at once
  --seed N        seed for the keys, the values and the lookup order (1)
  --distribution D  which keys the get and floor phases ask for: uniform, zipf
                  or latest (zipf). zipf is YCSB's skew at theta=0.99, drawn
                  and scrambled as `pebble bench ycsb` draws it, so the two
                  are comparable; latest is YCSB D's, where the newest records
                  are the hottest. uniform reproduces a number measured before
                  there was a choice
  --no-wal        turn the WAL off, as Oxia runs it: a flush is then the
                  only durability point, and a commit syncs nothing
  --no-compactions  compact nothing, so the read phases see L0 exactly as
                  the flushes left it. Without it the shape a run reads
                  depends on how far a clock-paced compaction got before
                  the reads began, and two runs of one binary read
                  different stores. Not with settle, whose job it undoes
  --reuse         open the store that is already at DIR/<engine> instead of
                  refusing it, so that a load and the reads over it can be two
                  processes with something done in between — dropping the
                  page cache, which needs root and so cannot be done from
                  here. Without it a run insists on a directory of its own.
  --layout        print every table of every level, with its size and key
                  range, after the store has settled
  --tsv           one tab-separated line per phase instead of a table, for a
                  script that puts several engines side by side
  --scans SPEC    how long a scan-short scan is, as
                  [{zipf,uniform}:]min[-max] (zipf:1-1000), which is
                  `pebble bench ycsb`'s spelling of the same thing
  --min-seconds N how long a timed phase runs for, at least (10). The scans,
                  get, floor, scan-short and ycsb run until both --reads and
                  this are satisfied; 0 turns it off and leaves --reads alone.
                  It is here because --reads alone was not enough: at 200,000
                  operations the mixes finished in under a second and the
                  scans in tens of milliseconds, which is not long enough for
                  a flush to land inside the measurement
  --workload X    which YCSB mix the ycsb phase runs, A to F (B). A is
                  50/50 read and update, B 95/5, C all reads, D 95/5 read and
                  insert over the latest distribution, E 95/5 scan and insert,
                  F 50/50 read and read-modify-write. D and F are YCSB's
                  definitions and not `pebble bench ycsb`'s, which has D on
                  the default distribution and F as all inserts
  --delete-range LO:HI  range-delete records LO..HI of every shard, as
                  fractions of the records loaded (0.45:0.55), after settle and
                  before the read phases, so the tombstones are in a memtable.
                  get then has to find nothing for a deleted record and floor
                  the live key below the range. One deletion per bucket, since
                  a bucket's records are what lie together in key order. Only
                  get and floor account for it, so --phases has to leave out
                  scan, scan-rev, scan-short, get-cold and ycsb
  --delete-before-settle  make the --delete-range deletions before settle
                  instead, so that settle flushes and compacts them with the
                  records they delete
  --phases LIST   comma-separated subset of
                  load,settle,scan-rev,scan,get,floor,scan-short,get-cold,
                  ycsb,checkpoint. **ycsb writes**, so it runs last and a
                  --reuse after it finds more records than were loaded
                  (always run in that order, whatever order they are named in)";

fn main() {
    let cfg = Config::parse();
    let path = cfg.dir.join(&cfg.engine);
    // Before anything is written, because what this catches makes every row
    // below it meaningless.
    warn_if_in_memory(&cfg.dir);
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
         wal={}, seed={}, keys={}, compression={}{}",
        S::NAME,
        cfg.shards,
        cfg.batches,
        cfg.records,
        cfg.value_bytes,
        cfg.cache_mb,
        if cfg.wal { "on" } else { "off" },
        cfg.seed,
        cfg.distribution.name(),
        cfg.compression
            .map_or("the engine's default".to_string(), |c| format!("{c:?}")),
        if cfg.compactions {
            ""
        } else {
            ", compactions=off"
        },
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
    if cfg.delete_before_settle {
        delete(cfg, &shards).await;
    }
    if cfg.wants("settle") {
        settle(&shards, out).await;
    }
    if !cfg.delete_before_settle {
        delete(cfg, &shards).await;
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
    if cfg.wants("scan-short") {
        short_scans(cfg, &shards, out).await;
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
    // After everything that wants a store of a known size, because this one
    // writes: A and B update, D and E insert, F writes back what it read.
    if cfg.wants("ycsb") {
        ycsb(cfg, &shards, out).await;
    }
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
    let mut report = Report::new("load", out).max_only();
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
    let expected = (cfg.shards * cfg.batches * cfg.records) as u64;
    // One sweep over a 42 MiB store is tens of milliseconds, which is not a
    // measurement. Sweeping again until `--min-seconds` is met is: the first
    // pass reads what `settle` left in the cache and the ones after it read
    // a cache the pass before warmed, so what this reports is a **steady
    // state** scan rate rather than a first scan. `run.sh` keeps a pass that
    // gives each direction a freshly loaded store of its own, which is where
    // a first scan is still measured.
    let started = Instant::now();
    let mut passes = 0u64;
    loop {
        for store in shards {
            let seen = store.scan(reverse, &mut report).await;
            report.count += seen;
        }
        passes += 1;
        if started.elapsed().as_secs_f64() >= cfg.min_seconds {
            break;
        }
    }
    if report.count != expected * passes {
        fail(&format!(
            "the {name} saw {} records over {passes} passes, not the {} that were written",
            report.count,
            expected * passes
        ));
    }
    report.finish(size_of(shards).await);
}

/// Range-deletes the records `--delete-range` names, in every shard.
///
/// One deletion per bucket: a range of record numbers is not a range of keys,
/// since the keys sort by bucket first (`key_into`), but within one bucket it
/// is — from the bucket's first record at or past the range's start to its
/// first one at or past the range's end.
async fn delete<S: Store>(cfg: &Config, shards: &[S]) {
    let Some(deleted) = cfg.deleted() else {
        return;
    };
    let started = Instant::now();
    let mut deletions = 0;
    for (s, store) in shards.iter().enumerate() {
        for span in deleted_spans(&deleted) {
            store
                .delete_range(&key_for(s, span.start), &key_for(s, span.end), cfg.wal)
                .await;
            deletions += 1;
        }
    }
    if !cfg.tsv {
        println!(
            "deleted records {}..{} of every shard {} settle: {deletions} range deletions in {:.3}s",
            deleted.start,
            deleted.end,
            if cfg.delete_before_settle {
                "before"
            } else {
                "after"
            },
            started.elapsed().as_secs_f64()
        );
    }
}

/// For each bucket the deleted records fall in, the records whose keys bound
/// them: from its first record at or past `deleted.start` to its first at or
/// past `deleted.end`, which need not exist to be a key.
fn deleted_spans(deleted: &std::ops::Range<usize>) -> impl Iterator<Item = std::ops::Range<usize>> {
    let (start, end) = (deleted.start, deleted.end);
    (0..BUCKETS)
        .map(move |bucket| {
            let first = |i: usize| i + (bucket + BUCKETS - i % BUCKETS) % BUCKETS;
            first(start)..first(end)
        })
        .filter(|span| !span.is_empty())
}

/// The record a floor probe of deleted record `i` lands on: the last one of
/// its bucket below the deleted range, which starts at `start`.
fn live_below(i: usize, start: usize) -> usize {
    start - 1 - (start - 1 - i % BUCKETS) % BUCKETS
}

/// Looks up random keys, every one of which has an answer — unless
/// `--delete-range` deleted it, and then `get` has to find nothing and `floor`
/// the live key below the deleted range.
///
/// `get` is Oxia's `Get` with `ComparisonEqual`, and the one comparison a
/// bloom filter can answer. Each engine reads it the best way it offers —
/// see [`Store::get`] — and reports the value's length, because an owned
/// return would charge some of them for an allocation the comparison is not
/// about and leave others borrowing.
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
    // Reused across every probe, so what the phase reports is the engine's
    // allocations and not the harness's.
    let mut key = Vec::new();
    let mut probe = Vec::new();
    let mut below = Vec::new();
    let keys = Keys::new(cfg.distribution, total);
    let deleted = cfg.deleted();
    let started = Instant::now();
    while more(cfg, report.count, started) {
        let s = (rng.next() as usize) % cfg.shards;
        let i = keys.next(&mut rng);
        let gone = deleted.as_ref().filter(|d| d.contains(&i));
        key_into(s, i, &mut key);
        probe.clear();
        probe.reserve(key.len() + 1);
        probe.extend_from_slice(&key);
        if floor {
            // Past `key-%010d` and below the next one, since every byte here
            // sorts above the digits the padding ends in.
            probe.push(b'~');
        }
        if floor {
            let at = Instant::now();
            let answer = shards[s].floor(&probe).await;
            report.saw(at.elapsed());
            let answer = answer.unwrap_or_else(|| {
                fail(&format!(
                    "{name} found no answer for {:?}",
                    String::from_utf8_lossy(&probe)
                ))
            });
            let want = match gone {
                Some(d) => {
                    key_into(s, live_below(i, d.start), &mut below);
                    &below
                }
                None => &key,
            };
            if answer != *want {
                fail(&format!(
                    "{name} answered {:?}, not the key below the probe",
                    String::from_utf8_lossy(&answer)
                ));
            }
            report.count += 1;
            continue;
        }
        let at = Instant::now();
        let len = shards[s].get(&probe).await;
        report.saw(at.elapsed());
        if gone.is_some() {
            if len.is_some() {
                fail(&format!(
                    "{name} found {:?}, which --delete-range deleted",
                    String::from_utf8_lossy(&probe)
                ));
            }
            report.count += 1;
            continue;
        }
        // Checked against the value this record was written with, not merely
        // for being there: a read that answered without producing the value
        // would otherwise be the fastest engine in the table.
        let len = len.unwrap_or_else(|| {
            fail(&format!(
                "{name} found no answer for {:?}",
                String::from_utf8_lossy(&probe)
            ))
        });
        if len != cfg.value_bytes {
            fail(&format!(
                "{name} answered {len} bytes for {:?}, not the {} written",
                String::from_utf8_lossy(&probe),
                cfg.value_bytes,
            ));
        }
        report.count += 1;
    }
    report.finish(size_of(shards).await);
}

/// `--reads` short range scans, each one a seek and then a few steps.
///
/// This is YCSB E's read, and the phase the harness most needed. A whole
/// table scan divides its seek and its per-source setup across every record
/// in the store, so both vanish; a scan of twenty records is mostly those two
/// things. Oxia's `KeyRangeScan` is the second kind, and until now nothing
/// here measured it.
///
/// Each scan is **one** timed operation, as YCSB counts it, and not a sample
/// per step.
async fn short_scans<S: Store>(cfg: &Config, shards: &[S], out: Output) {
    let mut report = Report::new("scan-short", out);
    let total = cfg.batches * cfg.records;
    let mut rng = Rng::new(cfg.seed ^ 0x2545_f491);
    let keys = Keys::new(cfg.distribution, total);
    let lengths = Lengths::parse(&cfg.scans);
    let mut key = Vec::new();
    let started = Instant::now();
    while more(cfg, report.count, started) {
        let s = (rng.next() as usize) % cfg.shards;
        let i = keys.next(&mut rng);
        key_into(s, i, &mut key);
        let want = lengths.next(&mut rng);
        // What is left between this key and the end of the shard, which is
        // what a scan can see when it asks for more than that.
        let left = (total - sorted_position(i, total)) as u64;
        let at = Instant::now();
        let saw = shards[s].scan_from(&key, want).await;
        report.saw(at.elapsed());
        // Checked, because a seek that landed in the wrong place would
        // otherwise be the fastest engine in the table: a scan sees what it
        // asked for, or everything left if that is less.
        let expected = want.min(left);
        if saw != expected {
            fail(&format!(
                "scan-short asked for {want} records from {:?} and saw {saw}, not {expected}",
                String::from_utf8_lossy(&key)
            ));
        }
        report.count += 1;
    }
    report.finish(size_of(shards).await);
}

/// `--reads` operations drawn from one of YCSB's six mixes.
///
/// **This phase writes**, which no other read phase does: A and B update, D
/// and E insert, F reads and writes back. So it is run last, after everything
/// that wants a store of a known size, and a `--reuse` of the directory
/// afterwards will find more records than `--batches x --records` and fail
/// the count checks. That is the phase working, not breaking.
///
/// Each shard keeps **its own** keyspace, because each grows by its own
/// inserts: a read on shard 3 has to draw from the records shard 3 has, and
/// drawing from a total some other shard grew would ask for a key that was
/// never written.
async fn ycsb<S: Store>(cfg: &Config, shards: &[S], out: Output) {
    let w = Workload::parse(&cfg.workload);
    let mut report = Report::new(w.row, out);
    let total = cfg.batches * cfg.records;
    let mut rng = Rng::new(cfg.seed ^ 0x7f4a_7c15);
    let mut keys: Vec<Keys> = (0..cfg.shards).map(|_| Keys::new(w.dist, total)).collect();
    let mut next_insert: Vec<usize> = vec![total; cfg.shards];
    let lengths = Lengths::parse(&cfg.scans);
    let mut key = Vec::new();
    let mut counts = [0u64; 5];
    let mut latencies: [Latencies; 5] = std::array::from_fn(|_| Latencies::default());
    let started = Instant::now();
    while more(cfg, report.count, started) {
        let s = (rng.next() as usize) % cfg.shards;
        let op = w.pick(&mut rng);
        let at = Instant::now();
        match op {
            Op::Read | Op::Rmw => {
                let i = keys[s].next(&mut rng);
                key_into(s, i, &mut key);
                let len = shards[s].get(&key).await.unwrap_or_else(|| {
                    fail(&format!(
                        "{} found no answer for {:?}",
                        w.row,
                        String::from_utf8_lossy(&key)
                    ))
                });
                if len != cfg.value_bytes {
                    fail(&format!(
                        "{} answered {len} bytes, not the {} written",
                        w.row, cfg.value_bytes
                    ));
                }
                // The write half of a read-modify-write, which is the only
                // thing separating F from a plain read.
                if op == Op::Rmw {
                    shards[s].write_batch(cfg, s, i, 1, false).await;
                }
            }
            // The value is the one this record already had, because
            // `value_for` is a function of the record number. An update is a
            // new version of the key either way, which is the cost being
            // measured; no engine here deduplicates identical values.
            Op::Update => {
                let i = keys[s].next(&mut rng);
                shards[s].write_batch(cfg, s, i, 1, false).await;
            }
            Op::Insert => {
                let i = next_insert[s];
                next_insert[s] += 1;
                shards[s].write_batch(cfg, s, i, 1, false).await;
                // Only now, so that nothing reads the key before it is there.
                keys[s].grow();
            }
            Op::Scan => {
                let i = keys[s].next(&mut rng);
                key_into(s, i, &mut key);
                let want = lengths.next(&mut rng);
                let saw = shards[s].scan_from(&key, want).await;
                if saw > want {
                    fail(&format!(
                        "{} scanned {saw} records, more than the {want} asked for",
                        w.row
                    ));
                }
            }
        }
        latencies[op.index()].saw(at.elapsed());
        counts[op.index()] += 1;
        report.count += 1;
    }
    for l in &latencies {
        report.absorb(l);
    }
    report.finish(size_of(shards).await);
    // A mix's own p50 is two distributions laid on top of each other, so it
    // says very little; the per-operation line is what a mixed row is read
    // against. Left out of `--tsv`, which is one line per phase by contract.
    //
    // It carries p99.9 and max as well as p50 and p99, because on this phase
    // those are orders apart and the distance between them is what the
    // scheduling costs: roble does its flushes and merges on the thread that
    // owns the store (§9), paced in slices, so the tail is where a slice
    // landing on an operation shows up. It used to be four orders, when the
    // write that found the memtable full paid for the whole merge; a column
    // that stops at p99 reported none of that and would report none of this.
    if !cfg.tsv {
        let mut parts = Vec::new();
        for i in 0..counts.len() {
            if counts[i] == 0 {
                continue;
            }
            let q = |f: f64| latencies[i].us(f);
            parts.push(format!(
                "{} {} p50 {}us p99 {}us p99.9 {}us max {}us",
                Op::at(i).name(),
                counts[i],
                q(0.50),
                q(0.99),
                q(0.999),
                q(1.0)
            ));
        }
        println!("  {}", parts.join(",  "));
        // What the writes left behind, which is the evidence that the
        // compaction kept up. roble compacts cooperatively on the thread
        // that owns the store (§9), in the tasks this driver spawns beside
        // each shard — so a mix that writes drives flushing and compaction
        // by writing, and does not need the phase to yield to either. L0
        // stays inside its allowance of four through 800,000 operations,
        // ending at one or two depending on where the phase stops in a
        // flush cycle. Printed anyway, because the day that stops being
        // true is the day this number is the first to say so.
        println!("  levels {}", level_shape(shards));
        if !w.pebble_agrees {
            println!(
                "  note: `pebble bench ycsb` defines {} differently, so it has no row here",
                w.row
            );
        }
    }
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

/// How many buckets a shard's records are spread over (`key_into`).
const BUCKETS: usize = 64;

/// The key of record `i` in shard `s`.
///
/// Oxia's shape, with roble's one constraint on it: the keys are raw and
/// bytewise ordered, because the `oxia-slash-spans` comparer is one roble
/// refuses (§3), so a shard roble can serve is one using Oxia's *natural*
/// encoder. The id is zero-padded so that bytewise order is numeric order,
/// which is what makes the floor probes land where a reader expects.
pub fn key_for(s: usize, i: usize) -> Vec<u8> {
    let mut out = Vec::new();
    key_into(s, i, &mut out);
    out
}

/// The same, into a buffer the caller keeps.
///
/// The read phases probe tens of thousands of keys in a loop, and a `format!`
/// per probe put two allocations of the harness's own into every measurement
/// of an engine's lookup. At three and a half allocations for a buffered
/// `get`, that was most of the number.
pub fn key_into(s: usize, i: usize, out: &mut Vec<u8>) {
    use std::io::Write;
    let bucket = i % BUCKETS;
    out.clear();
    // Enough for the whole key and the `~` a floor probe appends, so neither
    // the write nor the push grows the buffer.
    out.reserve(48);
    write!(out, "/shard-{s:04}/bucket-{bucket:02}/key-{i:010}").expect("writing to a Vec");
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
    // `i << 1`, not `i`: `Rng::new` sets bit 0, so a bare `i` gave records 2k
    // and 2k+1 one value between them. With keys in record order the two sit
    // side by side, and Snappy halved RocksDB's store while roble, which
    // writes no compression, kept it whole.
    let mut rng = Rng::new(cfg.seed ^ ((s as u64) << 40) ^ ((i as u64) << 1));
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
/// engine without pulling in a dependency for it.
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

    /// A float in `[0, 1)`, which is what the Zipf draw is defined on.
    fn unit(&mut self) -> f64 {
        // 53 bits, the mantissa, so every representable value is reachable
        // and none is reachable twice.
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// zeta(n, theta) = sum of `1 / i^theta` for i in `1..=n`.
///
/// Computed once per run. At six million keys it is six million `powf`s,
/// which is some tens of milliseconds before the first operation and nothing
/// after it.
fn zeta(n: u64, theta: f64) -> f64 {
    (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
}

/// A Zipf generator over `0..n`, drawn exactly as YCSB's and Pebble's are.
///
/// theta is 0.99, which is YCSB's default and the one `pebble bench ycsb`
/// uses, and the three-branch draw below is Pebble's `randvar.Zipf` line for
/// line. Both of those are deliberate: a roble number and a `pebble bench
/// ycsb` number are only comparable if the keys they ask for are drawn from
/// the same distribution, and the cheapest way to be sure of that is to run
/// the same arithmetic.
struct Zipf {
    alpha: f64,
    zeta_n: f64,
    half_pow_theta: f64,
    eta: f64,
    n: u64,
}

/// YCSB's default skew, and `pebble bench ycsb`'s.
const THETA: f64 = 0.99;

impl Zipf {
    fn new(n: u64) -> Self {
        let zeta_n = zeta(n, THETA);
        Zipf {
            alpha: 1.0 / (1.0 - THETA),
            eta: (1.0 - (2.0 / n as f64).powf(1.0 - THETA)) / (1.0 - zeta(2, THETA) / zeta_n),
            zeta_n,
            half_pow_theta: 1.0 + 0.5f64.powf(THETA),
            n,
        }
    }

    fn next(&self, rng: &mut Rng) -> u64 {
        self.at(rng.unit())
    }

    /// Extends the draw to cover `n` values.
    ///
    /// zeta is a sum over `1..=n`, so growing it needs only the new terms —
    /// which is what makes a workload that inserts affordable: YCSB D and E
    /// add a record every twentieth operation, and recomputing zeta from
    /// scratch each time would be the benchmark measuring itself. Pebble's
    /// `computeZetaIncrementally` exists for the same reason.
    fn grow(&mut self, n: u64) {
        if n <= self.n {
            return;
        }
        for i in self.n + 1..=n {
            self.zeta_n += 1.0 / (i as f64).powf(THETA);
        }
        self.n = n;
        self.eta =
            (1.0 - (2.0 / n as f64).powf(1.0 - THETA)) / (1.0 - zeta(2, THETA) / self.zeta_n);
    }

    /// The draw for a given point of the distribution, which is where the
    /// bound can be checked.
    ///
    /// `u` is below 1 and so the third branch is below `n` in exact
    /// arithmetic — but `eta * u - eta + 1` is an f64 and rounds, and at a
    /// `u` near enough to 1 it rounds *to* 1, which would put the draw one
    /// past the end. The clamp is that rounding's only backstop, and taking
    /// `u` as an argument is what lets a test reach it.
    fn at(&self, u: f64) -> u64 {
        let uz = u * self.zeta_n;
        let drawn = if uz < 1.0 {
            0
        } else if uz < self.half_pow_theta {
            1
        } else {
            (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64
        };
        drawn.min(self.n - 1)
    }
}

/// Pebble's key hash, which is an FNV-1 variant and not textbook FNV-1a: it
/// multiplies before it mixes each byte in, where FNV-1a mixes first.
///
/// Copied rather than corrected, because its job here is to scramble the same
/// way `pebble bench ycsb` scrambles.
fn hash_key(mut key: u64) -> u64 {
    let mut h: u64 = 14_695_981_039_346_656_037;
    for _ in 0..8 {
        h = h.wrapping_mul(1_099_511_628_211);
        h ^= key & 0xff;
        key >>= 8;
    }
    h
}

/// Which keys a read phase asks for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Dist {
    /// Every key equally often. What the phases did before there was a
    /// choice, and what reproduces a number measured then.
    Uniform,
    /// YCSB's skew: some 1% of the keys take about half the traffic.
    Zipf,
    /// YCSB D's: the most recently written keys are the hottest.
    Latest,
}

impl Dist {
    fn parse(name: &str) -> Self {
        match name {
            "uniform" => Dist::Uniform,
            "zipf" | "zipfian" => Dist::Zipf,
            "latest" => Dist::Latest,
            other => fail(&format!(
                "--distribution takes uniform, zipf or latest, not {other:?}"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Dist::Uniform => "uniform",
            Dist::Zipf => "zipf",
            Dist::Latest => "latest",
        }
    }
}

/// Which filesystem `path` sits on, on Linux, and `None` anywhere else.
///
/// `/proc/mounts` rather than a `statfs` binding, because this crate has no
/// libc dependency and parsing one file is cheaper than acquiring one.
#[cfg(target_os = "linux")]
fn filesystem_at(path: &Path) -> Option<String> {
    // The directory does not exist yet — this runs before anything is
    // written, which is the whole point of it — so resolve the nearest
    // ancestor that does. Canonicalizing the path itself just fails, which
    // is how the first version of this check silently did nothing.
    let mut probe = path.to_path_buf();
    let path = loop {
        if let Ok(resolved) = std::fs::canonicalize(&probe) {
            break resolved;
        }
        if !probe.pop() || probe.as_os_str().is_empty() {
            break std::env::current_dir().ok()?;
        }
    };
    let mounts = std::fs::read_to_string("/proc/mounts").ok()?;
    let mut best: Option<(usize, String)> = None;
    for line in mounts.lines() {
        let mut f = line.split_whitespace();
        let (Some(_dev), Some(point), Some(fstype)) = (f.next(), f.next(), f.next()) else {
            continue;
        };
        // `/proc/mounts` escapes a space in a mount point as `\040`. Missing
        // one costs this note, not a wrong answer, which is the right way
        // round for something advisory.
        let point = point.replace("\\040", " ");
        if path.starts_with(&point) && best.as_ref().is_none_or(|(n, _)| point.len() > *n) {
            best = Some((point.len(), fstype.to_string()));
        }
    }
    best.map(|(_, fs)| fs)
}

#[cfg(not(target_os = "linux"))]
fn filesystem_at(_path: &Path) -> Option<String> {
    None
}

/// Says so, loudly, if the store is about to be written to memory.
///
/// `mktemp -d` lands in `/tmp`, and `/tmp` is a **tmpfs on Ubuntu 24.04 and
/// later** — so the obvious way to run this puts the store in RAM, where
/// `fsync` is a no-op and no write reaches a device. The load, the settle and
/// both journal rows then measure memory, and nothing in the output would
/// have said so. It cost this harness two full runs before the check existed.
///
/// On stderr rather than stdout, so that it reaches someone reading a run
/// and someone piping `--tsv` into a table alike, without putting a line in
/// the table.
fn warn_if_in_memory(path: &Path) {
    let Some(fs) = filesystem_at(path) else {
        return;
    };
    if fs != "tmpfs" && fs != "ramfs" {
        return;
    }
    eprintln!(
        "note: {} is on {fs}, so this store is in memory and not on disk.",
        path.display()
    );
    eprintln!("note: fsync there is a no-op and no write reaches a device, so the load,");
    eprintln!("note: settle and journal rows measure RAM rather than storage. `mktemp -d`");
    eprintln!("note: lands in /tmp, which is a tmpfs on Ubuntu 24.04 and later: pass --dir");
    eprintln!("note: (or BENCH_DIR, for run.sh) a path on real storage instead.");
}

/// Whether a timed phase should go round again.
///
/// `--reads` says how many operations and `--min-seconds` says how long, and
/// a phase runs until **both** are satisfied. The second one exists because
/// the first was not enough on its own: at 200,000 operations the YCSB mixes
/// finished in 0.14 to 0.77 seconds on a Linux VM, and the scans in 7 to 81
/// *milli*seconds. A tenth of a second is not long enough for a flush to land
/// inside the measurement, for a compaction to be provoked, or for a rate to
/// be anything but the variance between two runs.
///
/// Ten seconds by default, which is also `pebble bench ycsb`'s `--duration`.
fn more(cfg: &Config, done: u64, started: Instant) -> bool {
    done < cfg.reads as u64 || started.elapsed().as_secs_f64() < cfg.min_seconds
}

/// One operation of a YCSB mix.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Op {
    /// Read a record that is there.
    Read,
    /// Write a record that is there again, making a new version of it.
    Update,
    /// Write a record that was not there, growing the keyspace.
    Insert,
    /// Seek and step, as `scan-short` does.
    Scan,
    /// Read a record and write it back, which is YCSB F.
    Rmw,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Read => "read",
            Op::Update => "update",
            Op::Insert => "insert",
            Op::Scan => "scan",
            Op::Rmw => "rmw",
        }
    }

    fn at(i: usize) -> Op {
        [Op::Read, Op::Update, Op::Insert, Op::Scan, Op::Rmw][i]
    }

    fn index(self) -> usize {
        match self {
            Op::Read => 0,
            Op::Update => 1,
            Op::Insert => 2,
            Op::Scan => 3,
            Op::Rmw => 4,
        }
    }
}

/// A YCSB workload: which operations, in what proportion, over which keys.
pub struct Workload {
    row: &'static str,
    /// Operations and their percentages, which sum to 100.
    ops: &'static [(Op, u32)],
    /// **The workload picks this, not `--distribution`.** D is defined as
    /// the latest distribution and the rest as zipfian, and a YCSB workload
    /// that drew from somewhere else would not be that workload.
    dist: Dist,
    /// Whether `pebble bench ycsb` defines this one the same way, and so
    /// whether a Pebble number belongs beside it.
    pebble_agrees: bool,
}

impl Workload {
    /// YCSB's six, as the paper and the `workload*` files define them.
    ///
    /// **D and F are not Pebble's.** `cmd/pebble` has D on the default
    /// distribution with a `TODO(peter): default to skewed-latest`, and F as
    /// 100% inserts with a `TODO(peter): the real workload is
    /// read-modify-write`. Both are carried here as YCSB defines them, and
    /// both are marked as having no Pebble counterpart rather than being
    /// quietly compared against a different workload.
    fn parse(letter: &str) -> Self {
        match letter {
            "A" | "a" => Workload {
                row: "ycsb-A",
                ops: &[(Op::Read, 50), (Op::Update, 50)],
                dist: Dist::Zipf,
                pebble_agrees: true,
            },
            "B" | "b" => Workload {
                row: "ycsb-B",
                ops: &[(Op::Read, 95), (Op::Update, 5)],
                dist: Dist::Zipf,
                pebble_agrees: true,
            },
            "C" | "c" => Workload {
                row: "ycsb-C",
                ops: &[(Op::Read, 100)],
                dist: Dist::Zipf,
                pebble_agrees: true,
            },
            "D" | "d" => Workload {
                row: "ycsb-D",
                ops: &[(Op::Read, 95), (Op::Insert, 5)],
                dist: Dist::Latest,
                pebble_agrees: false,
            },
            "E" | "e" => Workload {
                row: "ycsb-E",
                ops: &[(Op::Scan, 95), (Op::Insert, 5)],
                dist: Dist::Zipf,
                pebble_agrees: true,
            },
            "F" | "f" => Workload {
                row: "ycsb-F",
                ops: &[(Op::Read, 50), (Op::Rmw, 50)],
                dist: Dist::Zipf,
                pebble_agrees: false,
            },
            other => fail(&format!("--workload takes A to F, not {other:?}")),
        }
    }

    /// Draws the next operation.
    fn pick(&self, rng: &mut Rng) -> Op {
        let roll = (rng.next() % 100) as u32;
        let mut seen = 0;
        for &(op, weight) in self.ops {
            seen += weight;
            if roll < seen {
                return op;
            }
        }
        // The weights sum to 100 and `roll` is below 100, so this is the
        // last arm of the loop reached by a rounding that cannot happen; a
        // test asserts the sum.
        self.ops[self.ops.len() - 1].0
    }
}

/// How long a short range scan is.
///
/// `[{zipf,uniform}:]min[-max]`, which is `pebble bench ycsb`'s `--scans`
/// spelling, so that a scan length spec can be pasted between the two.
///
/// **Not scrambled, where the key draw is.** A scan length is a magnitude and
/// not a key: short scans being the common case is the shape YCSB E has and
/// the shape Oxia has, and an unscrambled Zipf draw is exactly that. The
/// scramble exists only to stop hot *keys* collecting in one table.
pub struct Lengths {
    min: u64,
    zipf: Option<Zipf>,
    spread: u64,
}

impl Lengths {
    fn parse(spec: &str) -> Self {
        let (kind, range) = match spec.split_once(':') {
            Some((k, r)) => (k, r),
            None => ("uniform", spec),
        };
        let (min, max) = match range.split_once('-') {
            Some((a, b)) => (number(a), number(b)),
            None => (number(range), number(range)),
        };
        if min == 0 || max < min {
            fail(&format!("--scans wants 1 <= min <= max, not {spec:?}"));
        }
        let spread = max + 1 - min;
        Lengths {
            min,
            spread,
            zipf: match kind {
                "uniform" => None,
                "zipf" | "zipfian" => Some(Zipf::new(spread)),
                other => fail(&format!("--scans takes zipf: or uniform:, not {other:?}")),
            },
        }
    }

    fn next(&self, rng: &mut Rng) -> u64 {
        self.min
            + match &self.zipf {
                Some(z) => z.next(rng),
                None => rng.next() % self.spread,
            }
    }
}

/// Where record `i` falls in key order, among `total` of them.
///
/// The keys are `/shard-%04d/bucket-%02d/key-%010d` and bucket is `i % 64`,
/// so they sort by bucket first and by id within it — which is *not* the
/// order the record numbers are in. A short scan starting at record `i` can
/// therefore see `total - this` records and no more, and that is what lets
/// the phase check the count it got back.
fn sorted_position(i: usize, total: usize) -> usize {
    let bucket = i % BUCKETS;
    // How many records fall in the buckets before this one: bucket `b` holds
    // every `i` with `i % BUCKETS == b`.
    let before: usize = (0..bucket)
        .map(|b| (total + BUCKETS - 1 - b) / BUCKETS)
        .sum();
    before + i / BUCKETS
}

/// Draws the record a read phase probes next.
///
/// **Zipf is scrambled and `latest` is not**, which is the one thing about
/// this that is easy to get wrong. A raw Zipf draw makes the *lowest* record
/// numbers the hottest, and record numbers are ordered the way the keys are —
/// so an unscrambled Zipf would put every hot key in the first table of the
/// store and measure a shape no workload has. Hashing the draw across the
/// keyspace is what YCSB's `ScrambledZipfianGenerator` is for, and Pebble
/// does the same thing by formatting the hash as the key. `latest` means
/// recent, and recency *is* the record number, so scrambling it would destroy
/// the only property it has.
struct Keys {
    dist: Dist,
    zipf: Zipf,
    total: u64,
}

impl Keys {
    fn new(dist: Dist, total: usize) -> Self {
        Keys {
            dist,
            zipf: Zipf::new(total as u64),
            total: total as u64,
        }
    }

    /// Takes one more record into the keyspace, for a workload that inserts.
    fn grow(&mut self) {
        self.total += 1;
        self.zipf.grow(self.total);
    }

    fn next(&self, rng: &mut Rng) -> usize {
        let i = match self.dist {
            Dist::Uniform => rng.next() % self.total,
            Dist::Zipf => hash_key(self.zipf.next(rng)) % self.total,
            Dist::Latest => self.total - 1 - self.zipf.next(rng).min(self.total - 1),
        };
        i as usize
    }
}

/// What `--compression` asked for, before an engine says whether it can.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    /// Nothing compressed.
    None,
    /// Snappy.
    Snappy,
    /// LZ4, which RocksDB writes and Go Pebble cannot read.
    Lz4,
    /// Zstd at a level.
    Zstd(i32),
}

impl Compression {
    /// `none`, `snappy`, `lz4`, `zstd` or `zstd:<level>`.
    fn parse(s: &str) -> Self {
        match s.split_once(':') {
            None if s == "none" => Compression::None,
            None if s == "snappy" => Compression::Snappy,
            None if s == "lz4" => Compression::Lz4,
            None if s == "zstd" => Compression::Zstd(roble::Codec::ZSTD_DEFAULT_LEVEL),
            Some(("zstd", level)) => Compression::Zstd(
                level
                    .parse()
                    .unwrap_or_else(|_| fail(&format!("--compression: bad zstd level {level:?}"))),
            ),
            _ => fail(&format!(
                "--compression: {s:?} is not none, snappy, lz4, zstd or zstd:<level>"
            )),
        }
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
    /// What `--compression` asked for, if anything: each engine has its own
    /// default.
    pub compression: Option<Compression>,
    reads: usize,
    /// Cache for every shard together, in MiB.
    pub cache_mb: u64,
    /// Bytes a memtable holds before a flush, in MiB.
    pub memtable_mb: u64,
    /// Seed for the keys, the values and the lookup order.
    pub seed: u64,
    /// Which keys the read phases ask for.
    pub distribution: Dist,
    /// How long a `scan-short` scan is.
    pub scans: String,
    /// Which YCSB mix the `ycsb` phase runs.
    pub workload: String,
    /// How long a timed phase runs for, at least, in seconds.
    pub min_seconds: f64,
    /// Whether the WAL is on, which is also whether a commit syncs.
    pub wal: bool,
    /// Whether the engine may compact on its own, rather than only when
    /// `settle` drives it.
    pub compactions: bool,
    layout: bool,
    reuse: bool,
    tsv: bool,
    phases: Option<Vec<String>>,
    /// The records `--delete-range` deletes, as fractions of those loaded.
    delete: Option<(f64, f64)>,
    delete_before_settle: bool,
}

impl Config {
    fn parse() -> Self {
        Self::parse_from(std::env::args().skip(1).collect())
    }

    fn parse_from(args: Vec<String>) -> Self {
        let mut cfg = Config {
            dir: PathBuf::new(),
            engine: "roble".to_string(),
            shards: 1,
            batches: 200,
            records: 10000,
            value_bytes: 128,
            value_chunk: 0,
            compression: None,
            reads: 100_000,
            cache_mb: 64,
            memtable_mb: 64,
            seed: 1,
            distribution: Dist::Zipf,
            scans: "zipf:1-1000".to_string(),
            workload: "B".to_string(),
            min_seconds: 10.0,
            wal: true,
            compactions: true,
            layout: false,
            reuse: false,
            tsv: false,
            phases: None,
            delete: None,
            delete_before_settle: false,
        };
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
                "--compression" => cfg.compression = Some(Compression::parse(value(arg))),
                "--reads" => cfg.reads = number(value(arg)) as usize,
                "--cache-mb" => cfg.cache_mb = number(value(arg)),
                "--memtable-mb" => cfg.memtable_mb = number(value(arg)),
                "--seed" => cfg.seed = number(value(arg)),
                "--distribution" => cfg.distribution = Dist::parse(value(arg)),
                "--scans" => cfg.scans = value(arg).to_string(),
                "--workload" => cfg.workload = value(arg).to_string(),
                "--min-seconds" => cfg.min_seconds = number(value(arg)) as f64,
                "--delete-range" => {
                    let spec = value(arg);
                    let fraction = |f: &str| {
                        f.parse::<f64>().unwrap_or_else(|_| {
                            fail(&format!("--delete-range {spec}: {f:?} is not a fraction"))
                        })
                    };
                    let (lo, hi) = spec
                        .split_once(':')
                        .unwrap_or_else(|| fail(&format!("--delete-range {spec}: not LO:HI")));
                    cfg.delete = Some((fraction(lo), fraction(hi)));
                }
                "--delete-before-settle" => {
                    cfg.delete_before_settle = true;
                    took_value = false;
                }
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
                "--no-compactions" => {
                    cfg.compactions = false;
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
        // Refused rather than given a meaning, because it would have a
        // different one per engine: roble's `settle` drives the merges
        // itself and would compact regardless, while RocksDB's waits for
        // its own threads, which this flag has stopped, and would leave L0
        // as it found it. Two rows that look alike and are not is the thing
        // this harness exists to avoid.
        if !cfg.compactions && cfg.wants("settle") {
            fail(
                "--no-compactions reads a store as the load left it, and settle compacts it: \
                 leave settle out of --phases",
            );
        }
        if let Some((lo, hi)) = cfg.delete {
            if !(0.0..=1.0).contains(&lo) || !(0.0..=1.0).contains(&hi) || lo >= hi {
                fail("--delete-range LO:HI needs 0 <= LO < HI <= 1");
            }
            // So that every bucket keeps a record below the range, which is
            // what a floor probe inside it answers with.
            if cfg.deleted().is_some_and(|d| d.start < BUCKETS) {
                fail(&format!(
                    "--delete-range has to start past the first {BUCKETS} records, \
                     so that a floor inside it has a record below it to find"
                ));
            }
            for phase in ["scan", "scan-rev", "scan-short", "get-cold", "ycsb"] {
                if cfg.wants(phase) {
                    fail(&format!(
                        "{phase} expects every record loaded, and --delete-range deletes some: \
                         leave it out of --phases"
                    ));
                }
            }
        } else if cfg.delete_before_settle {
            fail("--delete-before-settle says when to make the --delete-range deletions: give one");
        }
        cfg
    }

    /// The records `--delete-range` deletes in every shard, if it was given.
    fn deleted(&self) -> Option<std::ops::Range<usize>> {
        let total = (self.batches * self.records) as f64;
        self.delete
            .map(|(lo, hi)| (lo * total) as usize..(hi * total) as usize)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// How often each record was drawn, over `draws` draws of `total` keys.
    fn counts(dist: Dist, total: usize, draws: usize) -> Vec<u32> {
        let keys = Keys::new(dist, total);
        let mut rng = Rng::new(7);
        let mut c = vec![0u32; total];
        for _ in 0..draws {
            c[keys.next(&mut rng)] += 1;
        }
        c
    }

    /// What share of the draws went to `of`, as a percentage.
    fn share(c: &[u32], of: &[u32]) -> u64 {
        let part: u64 = of.iter().map(|&x| x as u64).sum();
        let all: u64 = c.iter().map(|&x| x as u64).sum();
        part * 100 / all
    }

    /// The hottest keys, by how often they were drawn.
    fn hottest(c: &[u32], n: usize) -> Vec<u32> {
        let mut sorted = c.to_vec();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        sorted[..n].to_vec()
    }

    const TOTAL: usize = 100_000;
    const DRAWS: usize = 400_000;

    /// The property that makes a skewed read phase worth running: some keys
    /// are much hotter than others, and **which** keys they are is spread
    /// across the store rather than sitting in its first table.
    ///
    /// The second half is the one that breaks silently. A raw Zipf draw makes
    /// the lowest record numbers the hottest, record numbers are ordered the
    /// way the keys are, and so every hot key would land in one table — a
    /// shape no workload has, and one that would make every cache number look
    /// far better than it is. Scrambling is what prevents it, and this is the
    /// test that would notice it being dropped.
    #[test]
    fn zipf_is_concentrated_but_not_clustered() {
        let c = counts(Dist::Zipf, TOTAL, DRAWS);
        let hot = share(&c, &hottest(&c, TOTAL / 100));
        assert!(hot > 40, "the hottest 1% of keys took only {hot}%");
        let first = share(&c, &c[..TOTAL / 100]);
        assert!(
            first < 5,
            "the first 1% of the keyspace took {first}%, so the draw is clustered"
        );
    }

    /// `latest` is the one distribution that **is** meant to be clustered: it
    /// is YCSB D's, where the newest records are the hottest, and recency is
    /// the record number. Scrambling it would destroy the only property it
    /// has, so this asserts the heat is at the end of the keyspace.
    #[test]
    fn latest_favours_the_newest_records() {
        let c = counts(Dist::Latest, TOTAL, DRAWS);
        let newest = share(&c, &c[TOTAL - TOTAL / 100..]);
        assert!(newest > 40, "the newest 1% of keys took only {newest}%");
    }

    /// And uniform stays uniform, which is what reproduces a number measured
    /// before there was a choice.
    #[test]
    fn uniform_favours_nothing() {
        let c = counts(Dist::Uniform, TOTAL, DRAWS);
        let hot = share(&c, &hottest(&c, TOTAL / 100));
        assert!(
            hot < 5,
            "the hottest 1% of keys took {hot}%, which is not uniform"
        );
    }

    /// `sorted_position` is what `scan-short` checks its answers against, so
    /// it is checked against the only thing that cannot be wrong: sorting the
    /// keys. Totals either side of a multiple of 64, because the buckets are
    /// uneven when 64 does not divide the total and that is where an
    /// off-by-one lives.
    #[test]
    fn sorted_position_agrees_with_sorting_the_keys() {
        for total in [1, 2, 63, 64, 65, 100, 1000, 1003] {
            let mut keys: Vec<(Vec<u8>, usize)> = (0..total).map(|i| (key_for(0, i), i)).collect();
            keys.sort();
            for (pos, (_, i)) in keys.iter().enumerate() {
                assert_eq!(sorted_position(*i, total), pos, "record {i} of {total}");
            }
        }
    }

    /// `--delete-range`'s deletions, one per bucket, cover exactly the records
    /// it names, and a floor probe of each deleted one lands where
    /// `live_below` says: on the greatest live key below the probe.
    #[test]
    fn deleted_spans_cover_the_deleted_records_and_live_below_is_the_floor() {
        for (total, deleted) in [
            (1000, 300..700),
            (1000, 64..65),
            (1003, 100..1003),
            (5000, 64..4999),
        ] {
            let keys: Vec<(Vec<u8>, usize)> = (0..total).map(|i| (key_for(0, i), i)).collect();
            let spans: Vec<(Vec<u8>, Vec<u8>)> = deleted_spans(&deleted)
                .map(|span| (key_for(0, span.start), key_for(0, span.end)))
                .collect();
            let gone = |key: &[u8]| {
                spans
                    .iter()
                    .any(|(lo, hi)| lo.as_slice() <= key && key < hi.as_slice())
            };
            for (key, i) in &keys {
                assert_eq!(
                    gone(key),
                    deleted.contains(i),
                    "record {i}, deleting {deleted:?} of {total}"
                );
            }
            let mut live: Vec<&Vec<u8>> = keys
                .iter()
                .filter(|(k, _)| !gone(k))
                .map(|(k, _)| k)
                .collect();
            live.sort();
            for i in deleted.clone() {
                let mut probe = key_for(0, i);
                probe.push(b'~');
                let floor = live.iter().rev().find(|k| k.as_slice() <= probe.as_slice());
                assert_eq!(
                    floor.map(|k| k.as_slice()),
                    Some(key_for(0, live_below(i, deleted.start)).as_slice()),
                    "floor of deleted record {i}, deleting {deleted:?} of {total}"
                );
            }
        }
    }

    /// A scan length stays in its range, and the zipf spec makes short scans
    /// the common case — which is the shape YCSB E has and the reason the
    /// lengths are *not* scrambled.
    #[test]
    fn scan_lengths_are_short_far_more_often_than_long() {
        let lengths = Lengths::parse("zipf:1-1000");
        let mut rng = Rng::new(3);
        let mut short = 0;
        for _ in 0..10_000 {
            let n = lengths.next(&mut rng);
            assert!((1..=1000).contains(&n), "drew {n}");
            if n <= 10 {
                short += 1;
            }
        }
        assert!(
            short > 3_000,
            "only {short} of 10000 scans were 10 records or fewer"
        );
    }

    /// And the other two spellings mean what they say.
    #[test]
    fn a_scan_length_spec_means_what_it_says() {
        let mut rng = Rng::new(5);
        let fixed = Lengths::parse("7");
        let spread = Lengths::parse("uniform:3-4");
        let mut threes = 0;
        for _ in 0..1_000 {
            assert_eq!(fixed.next(&mut rng), 7);
            let n = spread.next(&mut rng);
            assert!(n == 3 || n == 4, "drew {n}");
            threes += (n == 3) as u32;
        }
        assert!(
            (300..700).contains(&threes),
            "{threes} of 1000 were 3, which is not uniform"
        );
    }

    /// The clamp in `Zipf::at`, which nothing reachable through `Rng` can
    /// exercise: `unit()` is `[0, 1)`, and the branch needs a `u` whose
    /// arithmetic rounds to exactly 1. Driven directly, because the
    /// alternative is a guard nobody has ever run.
    #[test]
    fn a_zipf_draw_stays_below_its_bound_even_at_one() {
        for n in [2u64, 3, 1000, 1_000_000] {
            let z = Zipf::new(n);
            for u in [0.0, 0.5, 1.0 - f64::EPSILON, 1.0 - f64::EPSILON / 4.0, 1.0] {
                let drawn = z.at(u);
                assert!(drawn < n, "n={n} u={u} drew {drawn}");
            }
        }
    }

    /// Growing a Zipf has to leave it exactly where building it at the
    /// larger size would have. Checked against that, rather than against
    /// itself, because the incremental sum is the whole claim.
    #[test]
    fn growing_a_zipf_matches_building_one() {
        let mut grown = Zipf::new(500);
        for n in [501, 502, 700, 2000] {
            grown.grow(n);
            let fresh = Zipf::new(n);
            assert!(
                (grown.zeta_n - fresh.zeta_n).abs() < 1e-9,
                "n={n}: zeta {} against {}",
                grown.zeta_n,
                fresh.zeta_n
            );
            assert!((grown.eta - fresh.eta).abs() < 1e-9, "n={n}: eta");
            for u in [0.0, 0.25, 0.5, 0.75, 0.999] {
                assert_eq!(grown.at(u), fresh.at(u), "n={n} u={u}");
            }
        }
    }

    /// Growing never shrinks: a smaller `n` is ignored rather than
    /// corrupting the sum.
    #[test]
    fn growing_a_zipf_backwards_does_nothing() {
        let mut z = Zipf::new(1000);
        let (zeta, eta) = (z.zeta_n, z.eta);
        z.grow(10);
        z.grow(1000);
        assert_eq!(z.n, 1000);
        assert!((z.zeta_n - zeta).abs() < 1e-12 && (z.eta - eta).abs() < 1e-12);
    }

    /// The mixes are YCSB's, and `pick` hands them back in those
    /// proportions.
    ///
    /// The percentages are **written out here** rather than read from the
    /// table under test. A test that asks the table what it claims and then
    /// checks the draws against that would pass whatever the table said,
    /// which is what the first version of this did.
    #[test]
    fn the_mixes_are_ycsbs_mixes() {
        let expected: &[(&str, &[(Op, u32)])] = &[
            ("A", &[(Op::Read, 50), (Op::Update, 50)]),
            ("B", &[(Op::Read, 95), (Op::Update, 5)]),
            ("C", &[(Op::Read, 100)]),
            ("D", &[(Op::Read, 95), (Op::Insert, 5)]),
            ("E", &[(Op::Scan, 95), (Op::Insert, 5)]),
            ("F", &[(Op::Read, 50), (Op::Rmw, 50)]),
        ];
        // Enough draws that 0.3 of a percentage point is six sigma, so the
        // tolerance catches a one-in-a-hundred shift without ever flaking.
        const DRAWS: u32 = 1_000_000;
        for (letter, want) in expected {
            let w = Workload::parse(letter);
            assert_eq!(w.ops, *want, "{letter} is not YCSB {letter}");
            let sum: u32 = want.iter().map(|&(_, p)| p).sum();
            assert_eq!(sum, 100, "{letter} sums to {sum}");

            let mut rng = Rng::new(13);
            let mut seen = [0u32; 5];
            for _ in 0..DRAWS {
                seen[w.pick(&mut rng).index()] += 1;
            }
            for &(op, pct) in *want {
                let got = seen[op.index()] as f64 * 100.0 / DRAWS as f64;
                assert!(
                    (got - pct as f64).abs() < 0.3,
                    "{letter}: {} came out {got:.2}%, not {pct}%",
                    op.name()
                );
            }
            for (i, &n) in seen.iter().enumerate() {
                assert!(
                    want.iter().any(|&(op, _)| op.index() == i) || n == 0,
                    "{letter} drew {} {n} times and should never",
                    Op::at(i).name()
                );
            }
        }
    }

    /// The two YCSB definitions `cmd/pebble` does not share, and the one
    /// distribution a workload fixes for itself.
    #[test]
    fn d_is_latest_and_d_and_f_have_no_pebble_row() {
        assert_eq!(Workload::parse("D").dist, Dist::Latest);
        for letter in ["A", "B", "C", "E"] {
            assert_eq!(Workload::parse(letter).dist, Dist::Zipf, "{letter}");
            assert!(Workload::parse(letter).pebble_agrees, "{letter}");
        }
        assert!(!Workload::parse("D").pebble_agrees);
        assert!(!Workload::parse("F").pebble_agrees);
    }

    /// Every record gets a value of its own. When two neighbouring records
    /// shared one, an engine that compresses stored half the bytes of one
    /// that does not, but only with keys in record order — which made roble's
    /// ordered-key reads look twice as slow as RocksDB's.
    #[test]
    fn records_do_not_share_values() {
        let cfg = Config::parse_from(vec!["--dir".into(), "unused".into()]);
        let values: std::collections::HashSet<Vec<u8>> =
            (0..10_000).map(|i| value_for(&cfg, 0, i)).collect();
        assert_eq!(values.len(), 10_000);
    }

    /// Both skewed draws stay inside the keyspace at its edges, where the
    /// `min`/`max` arithmetic is easiest to get wrong by one.
    #[test]
    fn a_draw_never_leaves_the_keyspace() {
        for dist in [Dist::Uniform, Dist::Zipf, Dist::Latest] {
            for total in [1, 2, 3, 1000] {
                let keys = Keys::new(dist, total);
                let mut rng = Rng::new(11);
                for _ in 0..10_000 {
                    let i = keys.next(&mut rng);
                    assert!(i < total, "{} drew {i} of {total}", dist.name());
                }
            }
        }
    }
}
