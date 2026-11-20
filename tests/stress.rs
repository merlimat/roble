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

//! Many callers at once, on a real filesystem, with the store's own tasks
//! beside them.
//!
//! Every other test of the whole store makes one call at a time, or two at
//! a place it chose (`SimFs::pause_at`). A store is not used that way: a
//! shard has writers, readers, snapshots and iterators all suspended at
//! once, each at whatever await its I/O put it, while the store flushes and
//! merges in tasks of its own. This runs that, on `FileFs`, where an await
//! is a real wait and what runs during it is up to the runtime.
//!
//! The oracle has to survive not knowing the order. Each writer owns a
//! slice of the keys and writes nothing outside it, so its history is a
//! sequence, and every batch it writes also sets the slice's counter to the
//! batch's number. A read that sees a point in time — one scan, or any
//! reads through one snapshot or one checkpoint — sees the slice exactly as
//! it was after the batch the counter names: no part of a later one, none
//! missing from an earlier one. And that number is bounded by what the test
//! knows of the clock, which on one thread it knows exactly: no lower than
//! the last batch acknowledged before the read was made, and no higher than
//! the last one begun by then. A single `get` sees one key, so it is held
//! to the slice as it was at some batch between the two.
//!
//! Beside them a task flushes, compacts and takes checkpoints, which it
//! opens as stores of their own and reads the same way. At the end the
//! store is closed, reopened, and must hold every slice as its writer left
//! it.
//!
//! A test of overlap that ran its callers one after another would pass
//! having tested nothing, so it counts: the most calls in flight at once,
//! and how many held reads had a write land underneath them.
//!
//! `ROBLE_STRESS_SECS` sets how long each pass runs (2 by default), and
//! `ROBLE_STRESS_SEED` replays a run's operations — not its interleaving,
//! which is the runtime's.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::path::Path;
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};

use compio::runtime::ResumeUnwind as _;
use roble::{Caches, Comparison, Db, Error, Found, Options, Scan, Wal};
use roble_format::batch::Batch;

const WRITERS: u64 = 4;
const READERS: u64 = 4;
/// Keys in a slice: few, so that writes pile up on them and a range
/// deletion covers several.
const KEYS: u64 = 48;
/// How often a writer's history keeps the whole slice, so that the slice
/// at any batch is a replay of at most this many.
const MARK: u64 = 256;

fn lo(w: u64) -> Vec<u8> {
    format!("w{w}-").into_bytes()
}

fn hi(w: u64) -> Vec<u8> {
    format!("w{w}.").into_bytes()
}

/// The slice's counter, which sorts before its keys.
fn counter(w: u64) -> Vec<u8> {
    format!("w{w}-#").into_bytes()
}

fn key(w: u64, i: u64) -> Vec<u8> {
    format!("w{w}-k{i:03}").into_bytes()
}

/// A small, seeded generator: no dependency, and a seed that replays a
/// run's operations.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// The value op `slot` of batch `n` puts: its origin, then up to a few KiB,
/// so that memtables fill and tables split.
fn value(w: u64, n: u64, slot: u64) -> Vec<u8> {
    let mut v = format!("w{w}/{n}/{slot}/").into_bytes();
    let len = Rng::new(w << 48 ^ n << 8 ^ slot).below(4) * Rng::new(n ^ slot).below(1500);
    v.resize(v.len() + len as usize, b'a' + (n % 26) as u8);
    v
}

enum Op {
    Put(u64, u64),
    Delete(u64),
    DeleteRange(u64, u64),
}

/// A slice: each key's index, and which batch and op wrote it.
type Slice = BTreeMap<u64, (u64, u64)>;

/// One writer's batches, and the slice after every `MARK`th.
#[derive(Default)]
struct History {
    batches: Vec<Vec<Op>>,
    marks: Vec<Slice>,
}

impl History {
    fn push(&mut self, ops: Vec<Op>) {
        self.batches.push(ops);
        if (self.batches.len() as u64).is_multiple_of(MARK) {
            let at = self.slice(self.batches.len() as u64);
            self.marks.push(at);
        }
    }

    /// The slice after batch `n`, the first being 1.
    fn slice(&self, n: u64) -> Slice {
        let mark = (n / MARK).min(self.marks.len() as u64);
        let mut slice = match mark {
            0 => Slice::new(),
            m => self.marks[m as usize - 1].clone(),
        };
        for b in mark * MARK..n {
            for (slot, op) in self.batches[b as usize].iter().enumerate() {
                match *op {
                    Op::Put(i, _) => {
                        slice.insert(i, (b + 1, slot as u64));
                    }
                    Op::Delete(i) => {
                        slice.remove(&i);
                    }
                    Op::DeleteRange(a, z) => slice.retain(|i, _| !(a <= *i && *i < z)),
                }
            }
        }
        slice
    }
}

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// Slice `w` after batch `n`, as a scan of it reads it.
fn entries(w: u64, n: u64, slice: &Slice) -> Entries {
    let mut out = Vec::with_capacity(slice.len() + 1);
    if n > 0 {
        out.push((counter(w), n.to_string().into_bytes()));
    }
    for (&i, &(b, slot)) in slice {
        out.push((key(w, i), value(w, b, slot)));
    }
    out
}

fn show(k: &[u8], v: &[u8]) -> String {
    let v = &v[..v.len().min(24)];
    format!(
        "{}={}",
        String::from_utf8_lossy(k),
        String::from_utf8_lossy(v)
    )
}

/// Where two lists of entries first differ.
fn difference(got: &Entries, want: &Entries) -> Option<String> {
    let at = got.iter().zip(want).position(|(g, w)| g != w);
    let at = match at {
        Some(at) => at,
        None if got.len() == want.len() => return None,
        None => got.len().min(want.len()),
    };
    let name = |e: Option<&(Vec<u8>, Vec<u8>)>| e.map_or("nothing".into(), |(k, v)| show(k, v));
    Some(format!(
        "entry {at}: read {}, want {} ({} read, {} wanted)",
        name(got.get(at)),
        name(want.get(at)),
        got.len(),
        want.len()
    ))
}

/// The batch number a slice's entries say they are at.
fn batch_of(w: u64, got: &Entries) -> u64 {
    match got.first() {
        Some((k, v)) if *k == counter(w) => std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("slice {w}'s counter holds {}", show(k, v))),
        _ => 0,
    }
}

/// What a `get` should find in `want`, a slice's entries, or `None` when the
/// answer lies outside the slice and could be another writer's.
fn expect<'a>(
    want: &'a Entries,
    probe: &[u8],
    how: Comparison,
) -> Option<Option<&'a (Vec<u8>, Vec<u8>)>> {
    let found = match how {
        Comparison::Equal => return Some(want.iter().find(|(k, _)| k == probe)),
        Comparison::Floor => want.iter().rev().find(|(k, _)| k.as_slice() <= probe),
        Comparison::Lower => want.iter().rev().find(|(k, _)| k.as_slice() < probe),
        Comparison::Ceiling => want.iter().find(|(k, _)| k.as_slice() >= probe),
        Comparison::Higher => want.iter().find(|(k, _)| k.as_slice() > probe),
    };
    found.map(Some)
}

/// Whether `got` is a right answer to `get(probe, how)` on slice `w` at
/// `want`.
fn answers(w: u64, want: &Entries, probe: &[u8], how: Comparison, got: &Option<Found>) -> bool {
    match expect(want, probe, how) {
        Some(e) => got.as_ref().map(|f| (&f.key, &f.value)) == e.map(|(k, v)| (k, v)),
        // Nothing in the slice on that side, so the answer is past its
        // edge, in the direction asked, or nothing at all.
        None => match got {
            None => true,
            Some(f) => match how {
                Comparison::Floor | Comparison::Lower => f.key < lo(w),
                _ => f.key >= hi(w),
            },
        },
    }
}

fn comparison(rng: &mut Rng) -> Comparison {
    [
        Comparison::Equal,
        Comparison::Floor,
        Comparison::Ceiling,
        Comparison::Lower,
        Comparison::Higher,
    ][rng.below(5) as usize]
}

/// A key to read slice `w` at: one of its keys, one just after, its
/// counter, or either edge.
fn probe(rng: &mut Rng, w: u64) -> Vec<u8> {
    match rng.below(8) {
        0 => counter(w),
        1 => lo(w),
        2 => hi(w),
        3 | 4 => {
            let mut k = key(w, rng.below(KEYS));
            k.push(b'+');
            k
        }
        _ => key(w, rng.below(KEYS)),
    }
}

/// Hands the thread to whatever else is ready, once.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

async fn pause(rng: &mut Rng) {
    for _ in 0..rng.below(4) {
        yield_now().await;
    }
}

/// What the tasks share: each writer's history and where it has got to, and
/// the counts that say the run overlapped.
struct Shared {
    histories: Vec<RefCell<History>>,
    /// The last batch each writer began, and the last the store
    /// acknowledged.
    started: Vec<Cell<u64>>,
    acked: Vec<Cell<u64>>,
    until: Instant,
    in_flight: Cell<usize>,
    most_in_flight: Cell<usize>,
    /// Held reads that a write landed underneath.
    landed: Cell<u64>,
    reads: Cell<u64>,
}

impl Shared {
    fn done(&self) -> bool {
        Instant::now() >= self.until
    }

    fn acked(&self) -> Vec<u64> {
        self.acked.iter().map(Cell::get).collect()
    }

    fn started(&self) -> Vec<u64> {
        self.started.iter().map(Cell::get).collect()
    }

    fn call(&self) -> Call<'_> {
        let n = self.in_flight.get() + 1;
        self.in_flight.set(n);
        self.most_in_flight.set(self.most_in_flight.get().max(n));
        Call(self)
    }

    /// Requires `got` to be slice `w` as it was after some batch from
    /// `floor` to `ceil`, and returns which.
    fn check_cut(&self, w: u64, got: &Entries, floor: u64, ceil: u64, what: &str) -> u64 {
        let n = batch_of(w, got);
        if floor == ceil {
            assert_eq!(
                n, floor,
                "{what}: slice {w} read at batch {n}, where it is at {floor}"
            );
        }
        assert!(
            floor <= n && n <= ceil,
            "{what}: slice {w} read at batch {n}, but {floor} had been acknowledged and {ceil} begun"
        );
        let want = entries(w, n, &self.histories[w as usize].borrow().slice(n));
        if let Some(diff) = difference(got, &want) {
            panic!("{what}: slice {w} at batch {n}: {diff}");
        }
        self.reads.set(self.reads.get() + 1);
        n
    }

    /// Slice `w`'s entries after batch `n`.
    fn entries(&self, w: u64, n: u64) -> Entries {
        entries(w, n, &self.histories[w as usize].borrow().slice(n))
    }
}

/// A call in flight, counted while it lives.
struct Call<'a>(&'a Shared);

impl Drop for Call<'_> {
    fn drop(&mut self) {
        self.0.in_flight.set(self.0.in_flight.get() - 1);
    }
}

async fn writer(db: Rc<Db>, sh: Rc<Shared>, w: u64, seed: u64) {
    let mut rng = Rng::new(seed);
    let mut n = 0;
    while !sh.done() {
        n += 1;
        let mut ops = Vec::new();
        for slot in 0..1 + rng.below(4) {
            ops.push(match rng.below(10) {
                0..6 => Op::Put(rng.below(KEYS), slot),
                6..9 => Op::Delete(rng.below(KEYS)),
                _ => {
                    let a = rng.below(KEYS);
                    Op::DeleteRange(a, (a + 1 + rng.below(8)).min(KEYS))
                }
            });
        }
        let mut batch = Batch::new();
        for op in &ops {
            match *op {
                Op::Put(i, slot) => batch.put(&key(w, i), &value(w, n, slot)),
                Op::Delete(i) => batch.delete(&key(w, i)),
                Op::DeleteRange(a, z) => batch.delete_range(&key(w, a), &key(w, z)),
            }
            .expect("batch");
        }
        batch
            .put(&counter(w), n.to_string().as_bytes())
            .expect("batch");
        sh.histories[w as usize].borrow_mut().push(ops);
        sh.started[w as usize].set(n);
        {
            let _call = sh.call();
            db.write_raw(&mut batch, false)
                .await
                .unwrap_or_else(|e| panic!("writer {w}, batch {n}: {e}"));
        }
        sh.acked[w as usize].set(n);
        pause(&mut rng).await;
    }
}

/// Reads all of `scan` forwards, or backwards, giving the thread up between
/// steps now and then.
async fn read_all(
    scan: &mut Scan,
    forward: bool,
    rng: &mut Rng,
    sh: &Shared,
) -> roble::Result<Entries> {
    let mut out = Vec::new();
    {
        let _call = sh.call();
        if forward {
            scan.first().await?;
        } else {
            scan.last().await?;
        }
    }
    while scan.is_valid() {
        out.push((scan.key().to_vec(), scan.value().to_vec()));
        if rng.below(4) == 0 {
            pause(rng).await;
        }
        let _call = sh.call();
        if forward {
            scan.next().await?;
        } else {
            scan.prev().await?;
        }
    }
    if !forward {
        out.reverse();
    }
    Ok(out)
}

/// A scan of one slice, read end to end with the writers going on beside it.
async fn read_scan(db: &Db, sh: &Shared, rng: &mut Rng, r: u64) {
    let w = rng.below(WRITERS);
    let forward = rng.below(2) == 0;
    let floor = sh.acked[w as usize].get();
    let ceil = sh.started[w as usize].get();
    let (lo, hi) = (lo(w), hi(w));
    let mut scan = db.range(lo.as_slice()..hi.as_slice()).expect("a scan");
    let got = read_all(&mut scan, forward, rng, sh)
        .await
        .unwrap_or_else(|e| panic!("reader {r}'s scan of slice {w}: {e}"));
    if sh.acked[w as usize].get() > ceil {
        sh.landed.set(sh.landed.get() + 1);
    }
    let dir = if forward { "forward" } else { "reverse" };
    sh.check_cut(w, &got, floor, ceil, &format!("reader {r}'s {dir} scan"));
}

/// Where a list of entries says a scan should be.
fn at(want: &Entries, i: Option<usize>) -> Option<&(Vec<u8>, Vec<u8>)> {
    i.map(|i| &want[i])
}

/// A scan of one slice kept and moved about, a step or a seek at a time,
/// with the writers going on between its moves.
async fn read_moves(db: &Db, sh: &Shared, rng: &mut Rng, r: u64) {
    let w = rng.below(WRITERS);
    let floor = sh.acked[w as usize].get();
    let ceil = sh.started[w as usize].get();
    let (lo, hi) = (lo(w), hi(w));
    let mut scan = db.range(lo.as_slice()..hi.as_slice()).expect("a scan");
    let what = format!("reader {r}'s held scan of slice {w}");
    let fail = |e: Error| -> ! { panic!("{what}: {e}") };
    // The first move reads the counter, which says what the rest must be.
    {
        let _call = sh.call();
        scan.first().await.unwrap_or_else(|e| fail(e));
    }
    let head: Entries = if scan.is_valid() {
        vec![(scan.key().to_vec(), scan.value().to_vec())]
    } else {
        Vec::new()
    };
    let n = batch_of(w, &head);
    assert!(
        floor <= n && n <= ceil,
        "{what}: read at batch {n}, but {floor} had been acknowledged and {ceil} begun"
    );
    let want = sh.entries(w, n);
    let mut pos = (!want.is_empty()).then_some(0);
    for step in 0..12 + rng.below(20) {
        pause(rng).await;
        let _call = sh.call();
        let name;
        match rng.below(6) {
            0 => {
                let p = probe(rng, w);
                name = format!("seek_ge({})", String::from_utf8_lossy(&p));
                scan.seek_ge(&p).await.unwrap_or_else(|e| fail(e));
                pos = want.iter().position(|(k, _)| *k >= p);
            }
            1 => {
                let p = probe(rng, w);
                name = format!("seek_lt({})", String::from_utf8_lossy(&p));
                scan.seek_lt(&p).await.unwrap_or_else(|e| fail(e));
                pos = want.iter().rposition(|(k, _)| *k < p);
            }
            2 => {
                name = "first".into();
                scan.first().await.unwrap_or_else(|e| fail(e));
                pos = (!want.is_empty()).then_some(0);
            }
            3 => {
                name = "last".into();
                scan.last().await.unwrap_or_else(|e| fail(e));
                pos = want.len().checked_sub(1);
            }
            // A step from nothing is left alone: where it lands is not
            // something this is about.
            4 if pos.is_some() => {
                name = "next".into();
                scan.next().await.unwrap_or_else(|e| fail(e));
                pos = pos.map(|i| i + 1).filter(|&i| i < want.len());
            }
            5 if pos.is_some() => {
                name = "prev".into();
                scan.prev().await.unwrap_or_else(|e| fail(e));
                pos = pos.and_then(|i| i.checked_sub(1));
            }
            _ => continue,
        }
        let got = scan
            .is_valid()
            .then(|| (scan.key().to_vec(), scan.value().to_vec()));
        let want_here = at(&want, pos);
        if got.as_ref() != want_here {
            let name_of =
                |e: Option<&(Vec<u8>, Vec<u8>)>| e.map_or("nothing".into(), |(k, v)| show(k, v));
            panic!(
                "{what} at batch {n}, move {step} ({name}): on {}, want {}",
                name_of(got.as_ref()),
                name_of(want_here)
            );
        }
    }
    if sh.acked[w as usize].get() > ceil {
        sh.landed.set(sh.landed.get() + 1);
    }
    sh.reads.set(sh.reads.get() + 1);
}

/// A snapshot, read a few times while the writers go on: every slice must
/// be one point in time, the same one every time it is asked.
async fn read_snapshot(db: &Db, sh: &Shared, rng: &mut Rng, r: u64) {
    let floor = sh.acked();
    let ceil = sh.started();
    let snap = db.snapshot().expect("a snapshot");
    let what = format!("reader {r}'s snapshot");
    let mut at = Vec::new();
    for w in 0..WRITERS {
        let _call = sh.call();
        let found = snap
            .get(&counter(w), Comparison::Equal)
            .await
            .unwrap_or_else(|e| panic!("{what}: {e}"));
        let got: Entries = found.into_iter().map(|f| (f.key, f.value)).collect();
        let n = batch_of(w, &got);
        assert!(
            floor[w as usize] <= n && n <= ceil[w as usize],
            "{what}: slice {w} read at batch {n}, but {} had been acknowledged and {} begun",
            floor[w as usize],
            ceil[w as usize]
        );
        at.push(n);
    }
    for _ in 0..4 + rng.below(12) {
        pause(rng).await;
        let w = rng.below(WRITERS);
        let n = at[w as usize];
        if rng.below(4) == 0 {
            let (lo, hi) = (lo(w), hi(w));
            let mut scan = snap.range(lo.as_slice()..hi.as_slice()).expect("a scan");
            let got = read_all(&mut scan, rng.below(2) == 0, rng, sh)
                .await
                .unwrap_or_else(|e| panic!("{what}, scanning slice {w}: {e}"));
            sh.check_cut(w, &got, n, n, &what);
        } else {
            let p = probe(rng, w);
            let how = comparison(rng);
            let _call = sh.call();
            let got = snap
                .get(&p, how)
                .await
                .unwrap_or_else(|e| panic!("{what}: {e}"));
            let want = sh.entries(w, n);
            assert!(
                answers(w, &want, &p, how, &got),
                "{what}: slice {w} at batch {n}: get({}, {how:?}) found {:?}, want {:?}",
                String::from_utf8_lossy(&p),
                got.as_ref().map(|f| show(&f.key, &f.value)),
                expect(&want, &p, how).map(|e| e.map(|(k, v)| show(k, v)))
            );
            sh.reads.set(sh.reads.get() + 1);
        }
    }
    if sh.acked().iter().zip(&ceil).any(|(a, c)| a > c) {
        sh.landed.set(sh.landed.get() + 1);
    }
}

/// A `get` on the store as it is: one key, so held to the slice as it was
/// at some batch between the last acknowledged and the last begun.
async fn read_get(db: &Db, sh: &Shared, rng: &mut Rng, r: u64) {
    let w = rng.below(WRITERS);
    let p = probe(rng, w);
    let how = comparison(rng);
    let floor = sh.acked[w as usize].get();
    let got = {
        let _call = sh.call();
        db.get(&p, how)
            .await
            .unwrap_or_else(|e| panic!("reader {r}'s get: {e}"))
    };
    let ceil = sh.started[w as usize].get();
    let fits = (floor..=ceil).any(|n| answers(w, &sh.entries(w, n), &p, how, &got));
    assert!(
        fits,
        "reader {r}: get({}, {how:?}) found {:?}, which slice {w} held at no batch from {floor} to {ceil}",
        String::from_utf8_lossy(&p),
        got.as_ref().map(|f| show(&f.key, &f.value))
    );
    sh.reads.set(sh.reads.get() + 1);
}

async fn reader(db: Rc<Db>, sh: Rc<Shared>, r: u64, seed: u64) {
    let mut rng = Rng::new(seed);
    while !sh.done() {
        match rng.below(8) {
            0 | 1 => read_scan(&db, &sh, &mut rng, r).await,
            2 | 3 => read_moves(&db, &sh, &mut rng, r).await,
            4 => read_snapshot(&db, &sh, &mut rng, r).await,
            _ => read_get(&db, &sh, &mut rng, r).await,
        }
        pause(&mut rng).await;
    }
}

/// Reads a whole store, slice by slice, and requires each to be a point in
/// time between `floor` and `ceil`.
async fn check_store(db: &Db, sh: &Shared, floor: &[u64], ceil: &[u64], what: &str) {
    let mut rng = Rng::new(0);
    for w in 0..WRITERS {
        let (lo, hi) = (lo(w), hi(w));
        let mut scan = db.range(lo.as_slice()..hi.as_slice()).expect("a scan");
        let got = read_all(&mut scan, true, &mut rng, sh)
            .await
            .unwrap_or_else(|e| panic!("{what}: {e}"));
        sh.check_cut(w, &got, floor[w as usize], ceil[w as usize], what);
    }
}

/// Flushes, compacts and takes checkpoints beside everything else.
async fn disturber(db: Rc<Db>, sh: Rc<Shared>, dir: &Path, seed: u64) {
    let mut rng = Rng::new(seed);
    let mut checkpoints = 0;
    while !sh.done() {
        for _ in 0..50 + rng.below(200) {
            yield_now().await;
        }
        match rng.below(4) {
            0 => {
                let _call = sh.call();
                db.flush().await.expect("a flush");
            }
            1 | 2 => {
                // L0 whole, as the store's own compactions take it: a part
                // of it could put newer keys below older ones.
                let version = db.versions().version;
                let levels: Vec<u32> = (0..6).filter(|&l| !version.level(l).is_empty()).collect();
                if levels.is_empty() {
                    continue;
                }
                let level = levels[rng.below(levels.len() as u64) as usize];
                let tables = version.level(level);
                let files: Vec<u64> = if level == 0 {
                    tables.iter().map(|t| t.file_num).collect()
                } else {
                    vec![tables[rng.below(tables.len() as u64) as usize].file_num]
                };
                let _call = sh.call();
                match db.compact(level, &files).await {
                    // Somebody, likely the store, is merging some of them.
                    Ok(_) | Err(Error::Unsupported(_)) => {}
                    Err(e) => panic!("compacting {files:?} out of L{level}: {e}"),
                }
            }
            _ => {
                checkpoints += 1;
                let cp = dir.join(format!("checkpoint-{checkpoints}"));
                let floor = sh.acked();
                {
                    let _call = sh.call();
                    db.checkpoint(&cp).await.expect("a checkpoint");
                }
                let ceil = sh.started();
                let opened = roble::open_with(&roble::DefaultEnv, &cp, Options::new())
                    .await
                    .expect("the checkpoint opens");
                check_store(
                    &opened,
                    &sh,
                    &floor,
                    &ceil,
                    &format!("checkpoint {checkpoints}"),
                )
                .await;
                opened.close().await.expect("the checkpoint closes");
                std::fs::remove_dir_all(&cp).expect("remove the checkpoint");
            }
        }
    }
}

fn options(wal: Wal) -> Options {
    let options = Options::new()
        .create_if_missing(true)
        .wal(wal)
        .memtable_size(32 << 10)
        .l0_compaction_threshold(2)
        .lbase_max_bytes(128 << 10)
        .target_file_size(32 << 10)
        .max_manifest_bytes(16 << 10)
        // Small enough that reads go to the disk, and wait there.
        .caches(Caches::new(16, 64 << 10));
    match wal {
        // A write with no log does no I/O, so it would never wait unless
        // the store makes it.
        Wal::Off => options.l0_stop_writes_threshold(4),
        _ => options,
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|s| s.parse().ok())
}

async fn run(wal: Wal) {
    let secs = env_u64("ROBLE_STRESS_SECS").unwrap_or(2);
    let seed = env_u64("ROBLE_STRESS_SEED").unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos() as u64
    });
    eprintln!("stress, {wal:?}: {secs} s, ROBLE_STRESS_SEED={seed}");

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db");
    let db = Rc::new(
        roble::open_with(&roble::DefaultEnv, &dir, options(wal))
            .await
            .expect("open"),
    );
    let sh = Rc::new(Shared {
        histories: (0..WRITERS).map(|_| RefCell::default()).collect(),
        started: (0..WRITERS).map(|_| Cell::new(0)).collect(),
        acked: (0..WRITERS).map(|_| Cell::new(0)).collect(),
        until: Instant::now() + Duration::from_secs(secs),
        in_flight: Cell::new(0),
        most_in_flight: Cell::new(0),
        landed: Cell::new(0),
        reads: Cell::new(0),
    });

    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        tasks.push(compio::runtime::spawn(writer(
            Rc::clone(&db),
            Rc::clone(&sh),
            w,
            seed ^ w,
        )));
    }
    for r in 0..READERS {
        tasks.push(compio::runtime::spawn(reader(
            Rc::clone(&db),
            Rc::clone(&sh),
            r,
            seed ^ (r + 100),
        )));
    }
    let disturbing = compio::runtime::spawn({
        let (db, sh, tmp) = (Rc::clone(&db), Rc::clone(&sh), tmp.path().to_path_buf());
        async move { disturber(db, sh, &tmp, seed ^ 200).await }
    });
    for task in tasks {
        task.await.resume_unwind().expect("a task ran to its end");
    }
    disturbing
        .await
        .resume_unwind()
        .expect("the disturber ran to its end");

    let last = sh.acked();
    assert_eq!(last, sh.started(), "every write begun was acknowledged");
    eprintln!(
        "stress, {wal:?}: batches {last:?}, {} reads, at most {} calls in flight, \
         {} held reads with a write landing underneath",
        sh.reads.get(),
        sh.most_in_flight.get(),
        sh.landed.get()
    );
    // Overlap is the point: a run without it tested nothing this test is
    // for.
    assert!(sh.most_in_flight.get() > 2, "the calls never overlapped");
    assert!(sh.landed.get() > 0, "no write landed under a held read");

    check_store(&db, &sh, &last, &last, "the store at the end").await;
    if wal == Wal::Off {
        db.flush().await.expect("the last flush");
    }
    let Ok(db) = Rc::try_unwrap(db) else {
        panic!("a task still holds the store");
    };
    db.close().await.expect("close");
    let db = roble::open_with(&roble::DefaultEnv, &dir, options(wal))
        .await
        .expect("reopen");
    check_store(&db, &sh, &last, &last, "the store reopened").await;
    db.close().await.expect("close");
}

#[compio::test]
async fn many_callers_at_once_with_the_log_on() {
    run(Wal::Buffered).await;
}

#[compio::test]
async fn many_callers_at_once_with_the_log_off() {
    run(Wal::Off).await;
}
