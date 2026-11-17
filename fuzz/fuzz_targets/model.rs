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

//! A store against a `BTreeMap`, over operations the fuzzer chose.
//!
//! `tests/model.rs` does this from four fixed seeds, one operation to a
//! batch, through the whole store only. Here the sequence is the input, so a
//! coverage-guided search can go looking for the one that disagrees, and the
//! store is reached every way a caller reaches it: batches of several
//! operations, bounded scans moved by seeks and by changes of direction,
//! snapshots read after the store has moved on, checkpoints opened as
//! stores of their own, and reopens — with the WAL on or off, which is
//! Oxia's configuration and a different promise.
//!
//! Unlike `open_corrupt`, everything here is compared, and nothing is
//! allowed to fail: the filesystem is a healthy one and every call is one a
//! caller may make. A wrong answer, an error, a panic or a hang all fail it.

#![no_main]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use roble::sim::SimEnv;
use roble::{Comparison, Db, Disk, Fs, Options, Scan, Snapshot, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Arbitrary, Debug)]
struct Case {
    /// One of the golden databases, or past their number an empty store.
    fixture: u8,
    config: Config,
    steps: Vec<Step>,
}

#[derive(Arbitrary, Debug)]
struct Config {
    wal: bool,
    automatic_compactions: bool,
    l0_compaction_threshold: u8,
    target_file_size: u16,
    lbase_max_bytes: u16,
    /// A memtable small enough that writes flush themselves. Only with the
    /// WAL on: with it off a flush is a durability point, and the model has
    /// to know where each one fell.
    small_memtable: Option<u16>,
}

#[derive(Arbitrary, Debug)]
enum Step {
    Write(Vec<Op>),
    Flush,
    Compact,
    Reopen,
    Get(Probe, How),
    Scan {
        lower: Option<Probe>,
        upper: Option<Probe>,
        moves: Vec<Move>,
    },
    Snapshot,
    ReadSnapshot {
        which: u8,
        lower: Option<Probe>,
        upper: Option<Probe>,
        moves: Vec<Move>,
        lookups: Vec<Probe>,
    },
    Checkpoint,
    Verify,
}

#[derive(Arbitrary, Debug)]
enum Op {
    Put(u8, u16),
    Delete(u8),
    DeleteRange(u8, u8),
}

#[derive(Arbitrary, Debug, Clone, Copy)]
enum How {
    Equal,
    Floor,
    Ceiling,
    Lower,
    Higher,
}

#[derive(Arbitrary, Debug)]
enum Move {
    First,
    Last,
    Next,
    Prev,
    SeekGe(Probe),
    SeekLt(Probe),
}

/// A key of the alphabet, or one just beside it.
#[derive(Arbitrary, Debug)]
struct Probe(u8, u8);

/// Forty keys, so that writes land on each other and a range covers many,
/// and some are prefixes of others (`k1`, `k10`).
fn key(n: u8) -> Vec<u8> {
    format!("/oxia/k{}", n % 40).into_bytes()
}

impl Probe {
    fn bytes(&self) -> Vec<u8> {
        let mut k = key(self.0);
        match self.1 % 3 {
            0 => {}
            1 => k.push(b'!'),
            _ => {
                k.pop();
            }
        }
        k
    }
}

impl How {
    fn comparison(self) -> Comparison {
        match self {
            How::Equal => Comparison::Equal,
            How::Floor => Comparison::Floor,
            How::Ceiling => Comparison::Ceiling,
            How::Lower => Comparison::Lower,
            How::Higher => Comparison::Higher,
        }
    }

    fn expected(self, model: &Model, p: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        let p = p.to_vec();
        let pair = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
        match self {
            How::Equal => model.get(&p).map(|v| (p.clone(), v.clone())),
            How::Floor => model.range(..=p).next_back().map(pair),
            How::Ceiling => model.range(p..).next().map(pair),
            How::Lower => model.range(..p).next_back().map(pair),
            How::Higher => model
                .range((Bound::Excluded(p), Bound::Unbounded))
                .next()
                .map(pair),
        }
    }
}

type Files = Vec<(String, Vec<u8>)>;

/// The golden databases, read once.
fn fixtures() -> &'static [Files] {
    static FIXTURES: OnceLock<Vec<Files>> = OnceLock::new();
    FIXTURES.get_or_init(|| {
        let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/golden");
        let mut dbs: Vec<PathBuf> = std::fs::read_dir(&golden)
            .expect("tests/golden")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.is_dir())
            .collect();
        dbs.sort();
        dbs.iter()
            .map(|db| {
                std::fs::read_dir(db)
                    .expect("a golden database")
                    .map(|e| {
                        let e = e.expect("entry");
                        let name = e.file_name().into_string().expect("UTF-8 name");
                        (name, std::fs::read(e.path()).expect("read"))
                    })
                    .collect()
            })
            .collect()
    })
}

fn options(c: &Config) -> Options<roble::sim::SimFs> {
    let mut o = Options::new()
        .create_if_missing(true)
        .disable_wal(!c.wal)
        .disable_automatic_compactions(!c.automatic_compactions)
        .l0_compaction_threshold(1 + u64::from(c.l0_compaction_threshold % 4))
        .target_file_size(256 + u64::from(c.target_file_size % 8192))
        .lbase_max_bytes(512 + u64::from(c.lbase_max_bytes));
    if let (true, Some(bytes)) = (c.wal, c.small_memtable) {
        o = o.memtable_size(16 * 1024 + u64::from(bytes));
    }
    o
}

/// The `[lower, upper)` a scan's bounds leave of `model`.
fn bounded(model: &Model, lower: &Option<Vec<u8>>, upper: &Option<Vec<u8>>) -> Model {
    let lo = lower.clone().map_or(Bound::Unbounded, Bound::Included);
    let hi = upper.clone().map_or(Bound::Unbounded, Bound::Excluded);
    if let (Some(l), Some(u)) = (lower, upper) {
        if l >= u {
            return Model::new();
        }
    }
    model
        .range((lo, hi))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Moves `scan` and checks every position against what `view` says it
/// should be on.
async fn drive<D: Disk>(scan: &mut Scan<D>, view: &Model, moves: &[Move]) {
    let pair = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
    let mut at: Option<(Vec<u8>, Vec<u8>)> = None;
    for m in moves.iter().take(32) {
        at = match m {
            Move::First => {
                scan.first().await.expect("first");
                view.iter().next().map(pair)
            }
            Move::Last => {
                scan.last().await.expect("last");
                view.iter().next_back().map(pair)
            }
            Move::Next => {
                scan.next().await.expect("next");
                at.and_then(|(k, _)| {
                    view.range((Bound::Excluded(k), Bound::Unbounded))
                        .next()
                        .map(pair)
                })
            }
            Move::Prev => {
                scan.prev().await.expect("prev");
                at.and_then(|(k, _)| view.range(..k).next_back().map(pair))
            }
            Move::SeekGe(p) => {
                let p = p.bytes();
                scan.seek_ge(&p).await.expect("seek_ge");
                view.range(p..).next().map(pair)
            }
            Move::SeekLt(p) => {
                let p = p.bytes();
                scan.seek_lt(&p).await.expect("seek_lt");
                view.range(..p).next_back().map(pair)
            }
        };
        let got = scan
            .is_valid()
            .then(|| (scan.key().to_vec(), scan.value().to_vec()));
        assert_eq!(got, at, "after {m:?}");
    }
}

/// Every entry, forward and then backward.
async fn whole<D: Disk>(mut scan: Scan<D>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut forward = Vec::new();
    scan.first().await.expect("first");
    while scan.is_valid() {
        forward.push((scan.key().to_vec(), scan.value().to_vec()));
        scan.next().await.expect("next");
    }
    let mut backward = Vec::new();
    scan.last().await.expect("last");
    while scan.is_valid() {
        backward.push((scan.key().to_vec(), scan.value().to_vec()));
        scan.prev().await.expect("prev");
    }
    backward.reverse();
    assert_eq!(
        forward, backward,
        "a reverse scan disagrees with the forward one"
    );
    forward
}

fn entries(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
    model.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

async fn verify<F: Fs>(db: &Db<F>, model: &Model, what: &str) {
    let scan = db.scan().await.expect("scan");
    assert_eq!(whole(scan).await, entries(model), "{what}");
}

fn bounds(lower: &Option<Probe>, upper: &Option<Probe>) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    (
        lower.as_ref().map(Probe::bytes),
        upper.as_ref().map(Probe::bytes),
    )
}

async fn run(case: Case) {
    let all = fixtures();
    let env = SimEnv::new();
    let fs = env.sim();
    let dir = PathBuf::from("/db");
    if let Some(files) = all.get(usize::from(case.fixture) % (all.len() + 1)) {
        fs.mkdir_all(&dir).await.expect("mkdir");
        for (name, bytes) in files {
            let disk = fs.create(&dir.join(name)).await.expect("create");
            let (r, _) = disk.write(WriteBuf::new(bytes.clone()), 0, ()).await;
            r.expect("write");
            disk.sync(()).await.expect("sync");
            disk.close().await.expect("close");
        }
        fs.sync_dir(&dir).await.expect("sync dir");
    }

    let config = &case.config;
    let mut db = roble::open_with(&env, &dir, options(config))
        .await
        .expect("open");
    // What the store holds before anything is done to it is taken from the
    // store: the golden databases are checked against Pebble elsewhere.
    let scan = db.scan().await.expect("scan");
    let mut model: Model = whole(scan).await.into_iter().collect();
    // What a reopen comes back to: everything with the WAL on, and with it
    // off the last flush.
    let mut durable = model.clone();
    let mut snapshots: Vec<(Snapshot<_>, Model)> = Vec::new();
    let mut checkpoints = 0;

    for (step, s) in case.steps.iter().take(64).enumerate() {
        match s {
            Step::Write(ops) => {
                let mut batch = Batch::new();
                let mut next = model.clone();
                for op in ops.iter().take(8) {
                    match op {
                        Op::Put(k, len) => {
                            let k = key(*k);
                            let mut v = format!("v{step}.").into_bytes();
                            v.resize(v.len() + usize::from(*len % 1024), b'x');
                            batch.put(&k, &v).expect("put");
                            next.insert(k, v);
                        }
                        Op::Delete(k) => {
                            let k = key(*k);
                            batch.delete(&k).expect("delete");
                            next.remove(&k);
                        }
                        Op::DeleteRange(a, b) => {
                            let (a, b) = (key(*a), key(*b));
                            if a < b {
                                batch.delete_range(&a, &b).expect("delete_range");
                                next.retain(|k, _| !(*k >= a && *k < b));
                            }
                        }
                    }
                }
                if batch.is_empty() {
                    continue;
                }
                // With the WAL off a synced write is refused: there is
                // nothing to sync.
                db.write(&mut batch, config.wal).await.expect("write");
                model = next;
                if config.wal {
                    durable = model.clone();
                }
            }
            Step::Flush => {
                db.flush().await.expect("flush");
                durable = model.clone();
            }
            Step::Compact => {
                db.compact_once().await.expect("compact_once");
            }
            Step::Reopen => {
                snapshots.clear();
                drop(db);
                db = roble::open_with(&env, &dir, options(config))
                    .await
                    .expect("reopen");
                model = durable.clone();
                verify(&db, &model, "a reopen disagrees with the model").await;
            }
            Step::Get(p, how) => {
                let p = p.bytes();
                let found = db
                    .get(&p, how.comparison())
                    .await
                    .expect("get")
                    .map(|f| (f.key, f.value));
                assert_eq!(found, how.expected(&model, &p), "{how:?} of {p:?}");
            }
            Step::Scan {
                lower,
                upper,
                moves,
            } => {
                let (lo, hi) = bounds(lower, upper);
                let mut scan = db
                    .scan_range(lo.as_deref(), hi.as_deref())
                    .await
                    .expect("scan_range");
                drive(&mut scan, &bounded(&model, &lo, &hi), moves).await;
            }
            Step::Snapshot => {
                if snapshots.len() == 4 {
                    snapshots.remove(0);
                }
                snapshots.push((db.snapshot().await.expect("snapshot"), model.clone()));
            }
            Step::ReadSnapshot {
                which,
                lower,
                upper,
                moves,
                lookups,
            } => {
                if snapshots.is_empty() {
                    continue;
                }
                let (snap, seen) = &snapshots[usize::from(*which) % snapshots.len()];
                assert_eq!(
                    whole(snap.scan().expect("scan")).await,
                    entries(seen),
                    "a snapshot disagrees with the model as it was"
                );
                let (lo, hi) = bounds(lower, upper);
                let mut scan = snap
                    .scan_range(lo.as_deref(), hi.as_deref())
                    .expect("scan_range");
                drive(&mut scan, &bounded(seen, &lo, &hi), moves).await;
                for p in lookups.iter().take(8) {
                    let p = p.bytes();
                    assert_eq!(
                        snap.lookup(&p).await.expect("lookup"),
                        seen.get(&p).cloned(),
                        "a snapshot's lookup of {p:?}"
                    );
                }
            }
            Step::Checkpoint => {
                let to = PathBuf::from(format!("/checkpoint-{checkpoints}"));
                checkpoints += 1;
                db.checkpoint(&to).await.expect("checkpoint");
                // It flushes first.
                durable = model.clone();
                let copy = roble::open_with(&env, &to, options(config))
                    .await
                    .expect("open a checkpoint");
                verify(&copy, &model, "a checkpoint disagrees with the model").await;
            }
            Step::Verify => verify(&db, &model, "the store disagrees with the model").await,
        }
    }

    verify(&db, &model, "the store disagrees with the model").await;
    drop(snapshots);
    drop(db);
    let db = roble::open_with(&env, &dir, options(config))
        .await
        .expect("a final reopen");
    verify(&db, &durable, "a final reopen disagrees with the model").await;
}

thread_local! {
    static RUNTIME: compio::runtime::Runtime =
        compio::runtime::Runtime::new().expect("a compio runtime");
}

fuzz_target!(|case: Case| {
    RUNTIME.with(|rt| rt.block_on(run(case)));
});
