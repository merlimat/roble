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

//! Benchmarks roble on the workload Oxia runs, in the phases and the output
//! format `harness/bench` reports for Go Pebble, so the two can be read side
//! by side. See README.md.
//!
//! A run benchmarks one engine in a process of its own, so the CPU time and
//! the size it reports are that engine's alone.
//!
//! **What this exists to answer.** §14 of DESIGN.md leaves four questions
//! waiting on a measurement, and the sharpest is reverse iteration: Oxia's
//! `KeyRangeScanReverse` is on a serving path, an LSM pays more for `prev`
//! than for `next`, and the design says so without saying by how much. The
//! `scan-rev` phase is that number, beside the `scan` phase that is its
//! control.

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, Instant};

use roble::{Comparison, DefaultEnv, Options};

const USAGE: &str = "\
usage: roble-bench --dir DIR [options]

Creates the store at DIR/roble, which must not exist yet.

  --shards N      stores to open, as Oxia opens one per shard (1)
  --batches N     batches written to each shard (200)
  --records N     records in each batch (100)
  --value-bytes N bytes in each value (128)
  --reads N       lookups in each get and floor phase (100000)
  --cache-mb N    block cache for every shard together, in MiB (64)
  --seed N        seed for the keys and the lookup order (1)
  --no-wal        turn the WAL off, as Oxia runs it: a flush is then the
                  only durability point, and a commit syncs nothing
  --phases LIST   comma-separated subset of
                  load,settle,scan-rev,scan,get,floor,checkpoint
                  (always run in that order, whatever order they are named in)";

fn main() {
    let cfg = Config::parse();
    let path = cfg.dir.join("roble");
    if path.exists() {
        fail(&format!("{} exists: remove it first", path.display()));
    }
    std::fs::create_dir_all(&path).expect("create the directory");
    compio::runtime::Runtime::new()
        .expect("start the runtime")
        .block_on(run(&cfg, &path));
}

async fn run(cfg: &Config, path: &Path) {
    println!(
        "roble: {} shards x {} batches x {} records, {}-byte values, {} MiB cache, \
         wal={}, seed={}",
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
    // flushes the drive's own write cache; compio's is `fsync`, which does
    // not. On Linux — which is what Oxia runs — both are `fsync` and the two
    // are comparable.
    if cfg.wal && cfg.wants("load") && cfg.cfg_is_macos() {
        println!(
            "note: on macOS Pebble's sync is F_FULLFSYNC and roble's is fsync, so the load \n\
             note: row is not comparable between the two. Run both with --no-wal, which is \n\
             note: Oxia's own configuration, for a load comparison that is."
        );
    }
    println!("{}", Report::header());

    // One cache handle for every shard, which is the arrangement Oxia runs
    // and the one §8 is built for: hundreds of shards inside one budget.
    let caches = roble::Caches::new(
        roble::cache::DEFAULT_OPEN_TABLES,
        cfg.cache_mb as usize * 1024 * 1024,
    );
    let mut shards = Vec::with_capacity(cfg.shards);
    for i in 0..cfg.shards {
        let dir = path.join(format!("shard-{i:04}"));
        let db = roble::open_with(
            &DefaultEnv,
            &dir,
            Options::new()
                .create_if_missing(true)
                .caches(caches.clone())
                // Oxia's configuration: its replication log is the source of
                // truth, so the store need only be durable at a flush (§1).
                .disable_wal(!cfg.wal),
        )
        .await
        .unwrap_or_else(|e| fail(&format!("open {}: {e}", dir.display())));
        shards.push(db);
    }

    if cfg.wants("load") {
        load(cfg, &shards).await;
    }
    if cfg.wants("settle") {
        settle(&shards).await;
    }
    // Reverse before forward, deliberately. The phases share a block cache
    // and run in order, so whichever goes second reads a cache the first one
    // warmed — and the question §14 asks is whether reverse iteration is
    // *worse*. Putting it first points that bias against it, so a `scan-rev`
    // that keeps up with `scan` here is keeping up from behind. A cold
    // comparison of either needs its own process: `--phases load,scan` and
    // `--phases load,scan-rev`.
    // What shape the load and the settle left, because a scan's cost is
    // mostly how many iterators it has to merge: one table in one level is
    // not the same work as ten in L0, and without this line a reader cannot
    // tell a slow iterator from a differently-shaped LSM.
    if cfg.wants("scan") || cfg.wants("scan-rev") {
        println!("levels {}", level_shape(&shards));
    }
    if cfg.wants("scan-rev") {
        scan(cfg, &shards, Direction::Reverse).await;
    }
    if cfg.wants("scan") {
        scan(cfg, &shards, Direction::Forward).await;
    }
    if cfg.wants("get") {
        lookups(cfg, &shards, Comparison::Equal, "get").await;
    }
    if cfg.wants("floor") {
        lookups(cfg, &shards, Comparison::Floor, "floor").await;
    }
    if cfg.wants("checkpoint") {
        checkpoint(&shards, path).await;
    }

    println!("peak rss {} MiB", peak_rss_mib());
}

/// Writes every batch of every shard, timing each commit.
///
/// Through an indexed batch, which is the one Oxia uses — it reads back
/// through the batch before committing it — so what is measured includes the
/// index the repr is built under.
async fn load<F: roble::Fs>(cfg: &Config, shards: &[roble::Opened<F>]) {
    let mut report = Report::new("load");
    for (s, db) in shards.iter().enumerate() {
        for b in 0..cfg.batches {
            let mut batch = db.batch();
            for r in 0..cfg.records {
                let i = b * cfg.records + r;
                let key = key_for(s, i);
                batch
                    .put(&key, &value_for(cfg, s, i))
                    .unwrap_or_else(|e| fail(&format!("put: {e}")));
            }
            let at = Instant::now();
            batch
                // Synced when there is a log to sync: that is what makes
                // the load phase's latency a durability number rather than a
                // memtable-insert number.
                .commit(cfg.wal)
                .await
                .unwrap_or_else(|e| fail(&format!("commit: {e}")));
            report.saw(at.elapsed());
            report.count += cfg.records as u64;
        }
    }
    report.finish(shards).await;
}

/// Runs every compaction the picker wants, so the phases that follow read a
/// settled database rather than one in the middle of being shaped.
///
/// roble compacts cooperatively on the owning thread (§9), so this is the
/// caller's to drive and its cost belongs to whoever drives it. Go Pebble's
/// counterpart waits for its background compactions instead; the CPU column
/// is what makes the two comparable.
async fn settle<F: roble::Fs>(shards: &[roble::Opened<F>]) {
    let mut report = Report::new("settle");
    for db in shards {
        db.flush()
            .await
            .unwrap_or_else(|e| fail(&format!("flush: {e}")));
        while db.pick().is_some() {
            let at = Instant::now();
            let out = db
                .compact_once()
                .await
                .unwrap_or_else(|e| fail(&format!("compact: {e}")));
            report.saw(at.elapsed());
            report.count += out.len() as u64;
            if report.count > 100_000 {
                fail("the picker never settled");
            }
        }
    }
    report.finish(shards).await;
}

enum Direction {
    Forward,
    Reverse,
}

/// One step in this many is timed, in the scan phases. See `scan`.
const STEP_SAMPLE: u64 = 64;

/// Steps every record of every shard, forward or backward.
///
/// The latency recorded is one step, not the whole scan: what the reverse
/// question is about is the cost of `prev` against `next`, and a total hides
/// a tail.
async fn scan<F: roble::Fs>(cfg: &Config, shards: &[roble::Opened<F>], dir: Direction) {
    let mut report = Report::new(match dir {
        Direction::Forward => "scan",
        Direction::Reverse => "scan-rev",
    });
    for db in shards {
        let mut cursor = db
            .scan()
            .await
            .unwrap_or_else(|e| fail(&format!("scan: {e}")));
        match dir {
            Direction::Forward => cursor.first().await,
            Direction::Reverse => cursor.last().await,
        }
        .unwrap_or_else(|e| fail(&format!("seek: {e}")));
        while cursor.is_valid() {
            black_box(cursor.key());
            black_box(cursor.value());
            report.count += 1;
            // Sampled, not every step. A step here costs a few hundred
            // nanoseconds and a clock reading costs tens, so timing all of
            // them would charge the faster engine the larger share of a
            // measurement overhead and make the comparison say the opposite
            // of what it should. The rate comes from the phase's own wall
            // clock, which this leaves alone; the percentiles come from one
            // step in `STEP_SAMPLE`, which is enough for a tail.
            let sampled = report.count.is_multiple_of(STEP_SAMPLE);
            let at = sampled.then(Instant::now);
            match dir {
                Direction::Forward => cursor.next().await,
                Direction::Reverse => cursor.prev().await,
            }
            .unwrap_or_else(|e| fail(&format!("step: {e}")));
            if let Some(at) = at {
                report.saw(at.elapsed());
            }
        }
    }
    let expected = (cfg.shards * cfg.batches * cfg.records) as u64;
    if report.count != expected {
        fail(&format!(
            "the scan saw {} records, not the {expected} that were written",
            report.count
        ));
    }
    report.finish(shards).await;
}

/// Looks up random keys, every one of which has an answer.
///
/// `Equal` is Oxia's `Get`, and the one comparison a bloom filter can answer.
///
/// `Floor` is Oxia's too, and its probes are deliberately keys that are **not
/// there**: Oxia's `getFloor` tries an exact `Get` first and only falls
/// through to a seek when that misses, so a floor lookup of a key that exists
/// measures the `get` phase over again. A probe one byte past a stored key
/// misses the exact lookup and is answered by the key below it, which is the
/// seek this is here to time — no filter can help it, so it reads the index
/// of every table whose bounds take the key in (§6).
async fn lookups<F: roble::Fs>(
    cfg: &Config,
    shards: &[roble::Opened<F>],
    how: Comparison,
    name: &'static str,
) {
    let mut report = Report::new(name);
    let total = cfg.batches * cfg.records;
    let mut rng = Rng::new(cfg.seed ^ 0x9e37_79b9);
    for _ in 0..cfg.reads {
        let s = (rng.next() as usize) % cfg.shards;
        let i = (rng.next() as usize) % total;
        let mut key = key_for(s, i);
        if how != Comparison::Equal {
            // Past `key-%010d` and below the next one, since every byte here
            // sorts above the digits the padding ends in.
            key.push(b'~');
        }
        let at = Instant::now();
        let found = shards[s]
            .get(&key, how)
            .await
            .unwrap_or_else(|e| fail(&format!("{name}: {e}")));
        report.saw(at.elapsed());
        let answer = found.unwrap_or_else(|| {
            fail(&format!(
                "{name} found no answer for {:?}",
                String::from_utf8_lossy(&key)
            ))
        });
        if how != Comparison::Equal && answer.key != key_for(s, i) {
            fail(&format!(
                "{name} answered {:?}, not the key below the probe",
                String::from_utf8_lossy(&answer.key)
            ));
        }
        report.count += 1;
    }
    report.finish(shards).await;
}

/// Checkpoints every shard, which is how Oxia moves one.
async fn checkpoint<F: roble::Fs>(shards: &[roble::Opened<F>], path: &Path) {
    let mut report = Report::new("checkpoint");
    for (s, db) in shards.iter().enumerate() {
        let at = path.join(format!("checkpoint-{s:04}"));
        let started = Instant::now();
        db.checkpoint(&at)
            .await
            .unwrap_or_else(|e| fail(&format!("checkpoint: {e}")));
        report.saw(started.elapsed());
        report.count += 1;
    }
    report.finish(shards).await;
}

// ------------------------------------------------------------------ reporting

/// One phase's line: how much it did, how fast, and what it cost.
struct Report {
    name: &'static str,
    count: u64,
    started: Instant,
    cpu_at_start: Duration,
    latencies: Vec<Duration>,
}

impl Report {
    fn header() -> String {
        format!(
            "{:<11} {:>10} {:>12} {:>9} {:>9} {:>9} {:>8} {:>9}",
            "phase", "count", "rate/s", "p50 us", "p99 us", "max us", "cpu s", "size MiB"
        )
    }

    fn new(name: &'static str) -> Self {
        Report {
            name,
            count: 0,
            started: Instant::now(),
            cpu_at_start: cpu_time(),
            latencies: Vec::new(),
        }
    }

    fn saw(&mut self, took: Duration) {
        self.latencies.push(took);
    }

    async fn finish<F: roble::Fs>(mut self, shards: &[roble::Opened<F>]) {
        let wall = self.started.elapsed();
        let cpu = cpu_time().saturating_sub(self.cpu_at_start);
        let mut bytes = 0u64;
        for db in shards {
            bytes += db.disk_usage().await.unwrap_or(0);
        }
        self.latencies.sort_unstable();
        let us = |q: f64| -> u128 {
            if self.latencies.is_empty() {
                return 0;
            }
            let i = ((self.latencies.len() - 1) as f64 * q).round() as usize;
            self.latencies[i].as_micros()
        };
        println!(
            "{:<11} {:>10} {:>12.0} {:>9} {:>9} {:>9} {:>8.2} {:>9.1}",
            self.name,
            self.count,
            self.count as f64 / wall.as_secs_f64().max(f64::MIN_POSITIVE),
            us(0.50),
            us(0.99),
            us(1.0),
            cpu.as_secs_f64(),
            bytes as f64 / (1024.0 * 1024.0),
        );
    }
}

// --------------------------------------------------------------------- the key

/// The key of record `i` in shard `s`.
///
/// Oxia's shape, with roble's one constraint on it: the keys are raw and
/// bytewise ordered, because the `oxia-slash-spans` comparer is one roble
/// refuses (§3), so a shard roble can serve is one using Oxia's *natural*
/// encoder. The id is zero-padded so that bytewise order is numeric order,
/// which is what makes the floor lookups land where a reader expects.
fn key_for(s: usize, i: usize) -> Vec<u8> {
    let bucket = i % 64;
    format!("/shard-{s:04}/bucket-{bucket:02}/key-{i:010}").into_bytes()
}

/// How many tables sit at each level, summed over the shards.
fn level_shape<F: roble::Fs>(shards: &[roble::Opened<F>]) -> String {
    let mut counts = [0usize; 7];
    for db in shards {
        for (level, tables) in db.versions().version.levels() {
            counts[level as usize] += tables.len();
        }
    }
    counts
        .iter()
        .enumerate()
        .filter(|(_, n)| **n > 0)
        .map(|(l, n)| format!("L{l}={n}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The value of record `i` in shard `s`: pseudo-random bytes.
///
/// Random rather than a repeated byte, and this is not a detail. A value of
/// one byte over and over compresses to nothing, so a run with it would have
/// the size column say that Snappy is free and the uncompressed default of §8
/// is costing 2.5x — a measurement of the generator, not of Oxia's data. The
/// same generator fills them on both sides, so the two stores hold the same
/// bytes.
fn value_for(cfg: &Config, s: usize, i: usize) -> Vec<u8> {
    let mut rng = Rng::new(cfg.seed ^ ((s as u64) << 40) ^ (i as u64));
    let mut value = Vec::with_capacity(cfg.value_bytes);
    while value.len() < cfg.value_bytes {
        value.extend_from_slice(&rng.next().to_le_bytes());
    }
    value.truncate(cfg.value_bytes);
    value
}

// ------------------------------------------------------------------- the rest

/// xorshift64*, so the lookup order is the same on every run and in both
/// engines without pulling in a dependency for it.
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

struct Config {
    dir: PathBuf,
    shards: usize,
    batches: usize,
    records: usize,
    value_bytes: usize,
    reads: usize,
    cache_mb: u64,
    seed: u64,
    wal: bool,
    phases: Option<Vec<String>>,
}

impl Config {
    fn parse() -> Self {
        let mut cfg = Config {
            dir: PathBuf::new(),
            shards: 1,
            batches: 200,
            records: 100,
            value_bytes: 128,
            reads: 100_000,
            cache_mb: 64,
            seed: 1,
            wal: true,
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
                "--shards" => cfg.shards = number(value(arg)) as usize,
                "--batches" => cfg.batches = number(value(arg)) as usize,
                "--records" => cfg.records = number(value(arg)) as usize,
                "--value-bytes" => cfg.value_bytes = number(value(arg)) as usize,
                "--reads" => cfg.reads = number(value(arg)) as usize,
                "--cache-mb" => cfg.cache_mb = number(value(arg)),
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

    fn cfg_is_macos(&self) -> bool {
        cfg!(target_os = "macos")
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
/// that one engine's output is the shape of the other's wherever it runs.
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
/// The column that makes the two engines comparable at all: roble compacts on
/// the thread that owns the store (§9) and Pebble compacts on threads of its
/// own, so wall-clock time alone would credit Pebble with work it still paid
/// for.
fn cpu_time() -> Duration {
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

fn fail(message: &str) -> ! {
    eprintln!("roble-bench: {message}");
    process::exit(1);
}
