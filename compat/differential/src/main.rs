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

//! One random history, run through roble and Go Pebble in turn, on one
//! directory.
//!
//! Every other round trip is a fixed scenario: roble writes something
//! particular and Pebble reads it, or the reverse. This hands the directory
//! back and forth. `gen` writes a script of segments — puts of keys from a
//! small alphabet with values from a few bytes to past a log block, deletes,
//! range deletions, flushes, compactions and checks — and `run.sh` gives
//! each segment to the engine whose turn it is: this program for roble,
//! `harness/differential` for Pebble. Each opens what the other left — its
//! tables, the shapes its compactions chose, its MANIFEST, the log it did or
//! did not flush — carries on writing, checks, and closes.
//!
//! The oracle is the script itself. Each runner replays the segments before
//! its own into a `BTreeMap`, applies its own to the store and the map
//! together, and at every `check` reads the store every way it can — both
//! scans end to end, and every [`Comparison`] at every key of the alphabet
//! and either side of it — and requires the map's answers. A `walk` makes a
//! scan over a range and moves it the way the script says, seeking, stepping
//! and turning round, and requires the map's answer after every move.
//! `snap` takes a snapshot, up to three at once until `unsnap` lets one go,
//! and `snapcheck` and `snapwalk` read one as `check` and `walk` read the
//! store, against the map as it was when the snapshot was taken, while the
//! writes, flushes and compactions after it go on underneath. `batch` opens
//! an indexed batch; `bput`, `bdel` and `bdelrange` write into it while the
//! store's own writes go on beside it, `bcheck` and `bwalk` read through it
//! against the map with the batch laid over it, and `commit` or `abort` ends
//! it. So a
//! disagreement names the engine that read wrongly, the segment, and what the
//! other had written before it.
//!
//! ```text
//! differential gen <seed> <segments> <ops-per-segment>   script on stdout
//! differential run <dir> <script> <segment>              roble runs one segment
//! ```

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use roble::Comparison;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// How many keys the alphabet holds: few enough that writes pile up on the
/// same keys, and every one is read at each check.
const KEYS: u64 = 1000;

fn key(n: u64) -> String {
    format!("k{n:04}")
}

/// A put's value: its step's tag, then bytes drawn from the step, to `len`
/// bytes. The tag means no two puts of a key hold the same value, so a read
/// of a stale one shows; the drawn bytes mean compression cannot shrink a
/// large value away, so tables really fill and split. `harness/differential`
/// builds the same bytes.
fn value(step: u64, len: usize) -> Vec<u8> {
    let mut out = format!("v{step}.").into_bytes();
    let mut x = step.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push(b'a' + (x % 26) as u8);
    }
    out.truncate(len);
    out
}

/// One line of a script.
enum Op {
    Put(String, u64, usize),
    Del(String),
    DelRange(String, String),
    Flush,
    Compact,
    /// A check of what `At` names.
    Check(At),
    /// A scan over `[lower, upper)`, either side open when `None`, and the
    /// moves to make with it, over what `At` names.
    Walk(At, Option<Vec<u8>>, Option<Vec<u8>>, Vec<Move>),
    /// A snapshot, appended to the live ones.
    Snap,
    /// The live snapshot at this index, let go.
    Unsnap(usize),
    /// An indexed batch, opened. One at a time.
    Batch,
    /// A `Put`, `Del` or `DelRange` written into the open batch.
    Batched(Box<Op>),
    /// The open batch, committed.
    Commit,
    /// The open batch, dropped.
    Abort,
}

/// What a check or a walk reads.
#[derive(Clone, Copy)]
enum At {
    Store,
    /// The live snapshot at this index.
    Snap(usize),
    /// The store through the open batch.
    Batch,
}

/// One move of a walk.
enum Move {
    First,
    Last,
    Next,
    Prev,
    SeekGe(Vec<u8>),
    SeekLt(Vec<u8>),
}

/// A key of a script line: a key of the alphabet, or, ending in `+`, the
/// key just after it (the key with a zero byte appended), so that seeks land
/// between keys as well as on them.
fn script_key(word: &str) -> Vec<u8> {
    match word.strip_suffix('+') {
        Some(k) => [k.as_bytes(), &[0]].concat(),
        None => word.as_bytes().to_vec(),
    }
}

fn parse_move(word: &str) -> Move {
    match word {
        "f" => Move::First,
        "l" => Move::Last,
        "n" => Move::Next,
        "p" => Move::Prev,
        _ => match word.split_once(':') {
            Some(("g", k)) => Move::SeekGe(script_key(k)),
            Some(("t", k)) => Move::SeekLt(script_key(k)),
            _ => fail(&format!("cannot read the move {word:?}")),
        },
    }
}

fn parse(line: &str) -> Op {
    let w: Vec<&str> = line.split_whitespace().collect();
    match w.as_slice() {
        ["put", k, step, len] => Op::Put(
            k.to_string(),
            step.parse().expect("step"),
            len.parse().expect("len"),
        ),
        ["del", k] => Op::Del(k.to_string()),
        ["delrange", a, b] => Op::DelRange(a.to_string(), b.to_string()),
        ["flush"] => Op::Flush,
        ["compact"] => Op::Compact,
        ["check"] => Op::Check(At::Store),
        ["snapcheck", i] => Op::Check(At::Snap(i.parse().expect("snapshot"))),
        ["bcheck"] => Op::Check(At::Batch),
        ["snap"] => Op::Snap,
        ["unsnap", i] => Op::Unsnap(i.parse().expect("snapshot")),
        ["walk", lower, upper, moves @ ..] => walk_op(At::Store, lower, upper, moves),
        ["snapwalk", i, lower, upper, moves @ ..] => {
            walk_op(At::Snap(i.parse().expect("snapshot")), lower, upper, moves)
        }
        ["bwalk", lower, upper, moves @ ..] => walk_op(At::Batch, lower, upper, moves),
        ["batch"] => Op::Batch,
        ["commit"] => Op::Commit,
        ["abort"] => Op::Abort,
        [w, rest @ ..] if w.starts_with('b') && ["bput", "bdel", "bdelrange"].contains(w) => {
            Op::Batched(Box::new(parse(
                &[&w[1..]]
                    .iter()
                    .chain(rest)
                    .copied()
                    .collect::<Vec<_>>()
                    .join(" "),
            )))
        }
        _ => fail(&format!("cannot read {line:?}")),
    }
}

fn walk_op(at: At, lower: &str, upper: &str, moves: &[&str]) -> Op {
    let bound = |w: &str| (w != "-").then(|| script_key(w));
    Op::Walk(
        at,
        bound(lower),
        bound(upper),
        moves.iter().map(|w| parse_move(w)).collect(),
    )
}

/// The script's segments, each a list of operations.
fn segments(script: &str) -> Vec<Vec<Op>> {
    let mut out: Vec<Vec<Op>> = Vec::new();
    for line in script.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if line == "segment" {
            out.push(Vec::new());
        } else {
            out.last_mut()
                .unwrap_or_else(|| fail("an operation before the first segment"))
                .push(parse(line));
        }
    }
    out
}

/// The history up to some point: the map of what is committed, and the
/// writes of a batch that is open.
#[derive(Default)]
struct History<'s> {
    model: Model,
    batch: Option<Vec<&'s Op>>,
}

impl<'s> History<'s> {
    fn apply(&mut self, op: &'s Op) {
        match op {
            Op::Batch => self.batch = Some(Vec::new()),
            Op::Batched(write) => self.batch.as_mut().expect("no batch is open").push(write),
            Op::Commit => {
                for write in self.batch.take().expect("no batch is open") {
                    apply(&mut self.model, write);
                }
            }
            Op::Abort => self.batch = None,
            _ => apply(&mut self.model, op),
        }
    }

    /// The map as a read through the open batch sees it: the batch's writes
    /// over what is committed, since on a commit they will be numbered above
    /// everything before.
    fn through_batch(&self) -> Model {
        let mut model = self.model.clone();
        for write in self.batch.iter().flatten() {
            apply(&mut model, write);
        }
        model
    }
}

/// One store write, applied to a map.
fn apply(model: &mut Model, op: &Op) {
    match op {
        Op::Put(k, step, len) => {
            model.insert(k.as_bytes().to_vec(), value(*step, *len));
        }
        Op::Del(k) => {
            model.remove(k.as_bytes());
        }
        Op::DelRange(a, b) => {
            model.retain(|k, _| !(a.as_bytes() <= k.as_slice() && k.as_slice() < b.as_bytes()))
        }
        Op::Flush | Op::Compact | Op::Check(_) | Op::Walk(..) | Op::Snap | Op::Unsnap(_) => {}
        Op::Batch | Op::Batched(_) | Op::Commit | Op::Abort => {
            unreachable!("a batch's lines go through History")
        }
    }
}

/// A small seeded generator, the same history from the same seed.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn generate(seed: u64, segments: u64, ops: u64) {
    let mut rng = Rng(seed | 1);
    let mut step = 0;
    let mut out = String::new();
    for _ in 0..segments {
        // A check first, of what the other engine left, and one last.
        out.push_str("segment\ncheck\n");
        // Snapshots live in one engine's turn: it closes the store at the end.
        let mut snaps = 0;
        // So does an open batch, ended before the turn is.
        let mut batch = false;
        for _ in 0..ops {
            step += 1;
            let line = match rng.below(100) {
                0..=55 => {
                    // Mostly short, now and then past a 4 KiB block, and
                    // rarely past a 32 KiB log block.
                    let len = match rng.below(100) {
                        0..=89 => 1 + rng.below(200),
                        90..=98 => 4_000 + rng.below(4_000),
                        _ => 33_000 + rng.below(8_000),
                    };
                    let line = format!("put {} {step} {len}", key(rng.below(KEYS)));
                    into_batch(&mut rng, batch, line)
                }
                56..=75 => {
                    let line = format!("del {}", key(rng.below(KEYS)));
                    into_batch(&mut rng, batch, line)
                }
                76..=80 => {
                    let a = rng.below(KEYS);
                    let b = a + 1 + rng.below(30);
                    let line = format!("delrange {} {}", key(a), key(b));
                    into_batch(&mut rng, batch, line)
                }
                81..=88 => "flush".to_string(),
                // Rare, since it flattens the store into its last level, and
                // the layouts worth reading are the ones before that.
                89 if rng.below(4) == 0 => "compact".to_string(),
                89 => "flush".to_string(),
                90..=93 => gen_walk(&mut rng),
                // Taken now and then and read a while later, so that writes,
                // flushes and compactions land between.
                94..=95 if snaps == 0 || (snaps < 3 && rng.below(3) == 0) => {
                    snaps += 1;
                    "snap".to_string()
                }
                94..=95 => {
                    let i = rng.below(snaps);
                    match rng.below(3) {
                        0 => format!("snapcheck {i}"),
                        1 => format!(
                            "snap{}",
                            gen_walk(&mut rng).replacen("walk", &format!("walk {i}"), 1)
                        ),
                        _ => {
                            snaps -= 1;
                            format!("unsnap {i}")
                        }
                    }
                }
                // A batch lives for a couple of hundred operations, most of
                // the writes in its time going into it, and is read through
                // along the way.
                96..=97 if !batch => {
                    batch = true;
                    "batch".to_string()
                }
                96..=97 => match rng.below(20) {
                    0..=3 => {
                        batch = false;
                        "commit".to_string()
                    }
                    4 => {
                        batch = false;
                        "abort".to_string()
                    }
                    5..=11 => "bcheck".to_string(),
                    _ => format!("b{}", gen_walk(&mut rng)),
                },
                _ => "check".to_string(),
            };
            out.push_str(&line);
            out.push('\n');
        }
        if batch {
            out.push_str(if rng.below(4) == 0 {
                "abort\n"
            } else {
                "commit\n"
            });
        }
        // Half the segments end flushed, and half leave their last writes
        // in the log for the other engine to replay.
        if rng.below(2) == 0 {
            out.push_str("flush\n");
        }
        out.push_str("check\n");
    }
    print!("{out}");
}

/// A write, into the open batch seven times in ten while one is open.
fn into_batch(rng: &mut Rng, batch: bool, write: String) -> String {
    if batch && rng.below(10) < 7 {
        format!("b{write}")
    } else {
        write
    }
}

/// A walk: a range, a quarter of the time the whole alphabet and otherwise
/// up to a few hundred keys with either side now and then open, and forty
/// moves over it. Seeks aim at keys and between keys, a few of them outside
/// the range, where a scan clamps them to its bounds.
fn gen_walk(rng: &mut Rng) -> String {
    let (mut lower, mut upper) = ("-".to_string(), "-".to_string());
    let (mut from, mut to) = (0, KEYS);
    if rng.below(4) != 0 {
        from = rng.below(KEYS);
        to = from + 1 + rng.below(300);
        if rng.below(5) != 0 {
            lower = key(from);
        }
        if rng.below(5) != 0 {
            upper = key(to);
        }
    }
    let mut line = format!("walk {lower} {upper}");
    for _ in 0..40 {
        let mv = match rng.below(10) {
            0..=2 => "n".to_string(),
            3..=5 => "p".to_string(),
            6 => ["f", "l"][rng.below(2) as usize].to_string(),
            _ => {
                let at = from.saturating_sub(3) + rng.below(to - from.saturating_sub(3) + 3);
                let plus = ["", "+"][rng.below(2) as usize];
                let how = ["g", "t"][rng.below(2) as usize];
                format!("{how}:{}{plus}", key(at))
            }
        };
        line.push(' ');
        line.push_str(&mv);
    }
    line
}

/// Every probe a check reads at: each key, and the keys either side of it,
/// and either end of the alphabet.
fn probes() -> Vec<Vec<u8>> {
    let mut out = vec![b"a".to_vec(), b"z".to_vec()];
    for n in 0..KEYS {
        let k = key(n).into_bytes();
        out.push([k.clone(), vec![0]].concat());
        out.push(k);
    }
    out
}

fn show(found: &Option<(Vec<u8>, Vec<u8>)>) -> String {
    match found {
        None => "nothing".to_string(),
        Some((k, v)) => format!(
            "{} = {} bytes starting {:?}",
            String::from_utf8_lossy(k),
            v.len(),
            String::from_utf8_lossy(&v[..v.len().min(12)])
        ),
    }
}

/// What a check or a walk reads: the store, or a snapshot of it.
enum Src<'a, F: roble::Fs> {
    Db(&'a roble::Db<F>),
    Snap(&'a roble::Snapshot<F>),
    Batch(&'a roble::IndexedBatch<'a, F>),
}

impl<F: roble::Fs> Src<'_, F> {
    fn range<'k>(
        &self,
        range: impl std::ops::RangeBounds<&'k [u8]>,
    ) -> roble::Result<roble::Scan<F>> {
        match self {
            Src::Db(db) => db.range(range),
            Src::Snap(snap) => snap.range(range),
            Src::Batch(batch) => batch.range(range),
        }
    }

    async fn get(&self, key: &[u8], how: Comparison) -> roble::Result<Option<roble::Found>> {
        match self {
            Src::Db(db) => db.get(key, how).await,
            Src::Snap(snap) => snap.get(key, how).await,
            Src::Batch(batch) => batch.get(key, how).await,
        }
    }
}

async fn check<F: roble::Fs>(db: &Src<'_, F>, model: &Model, at: &str) {
    let mut scan = db
        .range(..)
        .unwrap_or_else(|e| fail(&format!("{at}: scan: {e}")));
    let mut forward = Model::new();
    scan.first()
        .await
        .unwrap_or_else(|e| fail(&format!("{at}: first: {e}")));
    while scan.is_valid() {
        forward.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next()
            .await
            .unwrap_or_else(|e| fail(&format!("{at}: next: {e}")));
    }
    if forward != *model {
        let first = model
            .iter()
            .zip(forward.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(model.len().min(forward.len()));
        fail(&format!(
            "{at}: a forward scan found {} keys, the history says {}; first difference at entry {first}: \
             roble {}, history {}",
            forward.len(),
            model.len(),
            show(
                &forward
                    .iter()
                    .nth(first)
                    .map(|(k, v)| (k.clone(), v.clone()))
            ),
            show(&model.iter().nth(first).map(|(k, v)| (k.clone(), v.clone()))),
        ));
    }
    let mut backward = Vec::new();
    scan.last()
        .await
        .unwrap_or_else(|e| fail(&format!("{at}: last: {e}")));
    while scan.is_valid() {
        backward.push((scan.key().to_vec(), scan.value().to_vec()));
        scan.prev()
            .await
            .unwrap_or_else(|e| fail(&format!("{at}: prev: {e}")));
    }
    backward.reverse();
    if backward
        != model
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
    {
        fail(&format!("{at}: a reverse scan disagrees with the history"));
    }
    drop(scan);

    let pair = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
    for p in probes() {
        for (how, want) in [
            (
                Comparison::Equal,
                model.get(&p).map(|v| (p.clone(), v.clone())),
            ),
            (
                Comparison::Ceiling,
                model.range(p.clone()..).next().map(pair),
            ),
            (
                Comparison::Floor,
                model
                    .range((Bound::Unbounded, Bound::Included(p.clone())))
                    .next_back()
                    .map(pair),
            ),
            (
                Comparison::Higher,
                model
                    .range((Bound::Excluded(p.clone()), Bound::Unbounded))
                    .next()
                    .map(pair),
            ),
            (
                Comparison::Lower,
                model.range(..p.clone()).next_back().map(pair),
            ),
        ] {
            let got = db
                .get(&p, how)
                .await
                .unwrap_or_else(|e| {
                    fail(&format!(
                        "{at}: {how:?} of {:?}: {e}",
                        String::from_utf8_lossy(&p)
                    ))
                })
                .map(|f| (f.key, f.value));
            if got != want {
                fail(&format!(
                    "{at}: {how:?} of {:?}: roble found {}, the history {}",
                    String::from_utf8_lossy(&p),
                    show(&got),
                    show(&want)
                ));
            }
        }
    }
}

/// Where a walk's scan should be after a move, by the map: the move's key
/// among the map's keys in `[lower, upper)`, or nothing. `at` is where the
/// scan is before the move; a step off the end of the range leaves it on
/// nothing, and a step from nothing is not made (see [`walk`]).
fn walk_want<'m>(
    model: &'m Model,
    lower: &Option<Vec<u8>>,
    upper: &Option<Vec<u8>>,
    at: Option<&[u8]>,
    mv: &Move,
) -> Option<(&'m Vec<u8>, &'m Vec<u8>)> {
    let lo = |b: Bound<Vec<u8>>| match (lower, b) {
        (Some(l), Bound::Included(k)) if k < *l => Bound::Included(l.clone()),
        (Some(l), Bound::Unbounded) => Bound::Included(l.clone()),
        (_, b) => b,
    };
    let hi = |b: Bound<Vec<u8>>| match (upper, b) {
        (Some(u), Bound::Excluded(k)) if k > *u => Bound::Excluded(u.clone()),
        (Some(u), Bound::Unbounded) => Bound::Excluded(u.clone()),
        (_, b) => b,
    };
    let ge = |from: Bound<Vec<u8>>| {
        let (from, to) = (lo(from), hi(Bound::Unbounded));
        // A range whose start is past its end is empty, where BTreeMap panics.
        match (&from, &to) {
            (Bound::Included(a) | Bound::Excluded(a), Bound::Excluded(b)) if a >= b => None,
            _ => model.range((from, to)).next(),
        }
    };
    let lt = |to: Bound<Vec<u8>>| {
        let (from, to) = (lo(Bound::Unbounded), hi(to));
        match (&from, &to) {
            (Bound::Included(a), Bound::Excluded(b)) if a >= b => None,
            _ => model.range((from, to)).next_back(),
        }
    };
    match mv {
        Move::First => ge(Bound::Unbounded),
        Move::Last => lt(Bound::Unbounded),
        Move::SeekGe(k) => ge(Bound::Included(k.clone())),
        Move::SeekLt(k) => lt(Bound::Excluded(k.clone())),
        Move::Next => ge(Bound::Excluded(at?.to_vec())),
        Move::Prev => lt(Bound::Excluded(at?.to_vec())),
    }
}

/// A scan over the walk's range, moved as the script says, and required to
/// be where the map says after every move. A step from nothing is skipped:
/// roble leaves the scan on nothing and Pebble turns round onto the range's
/// end, and which is right is not what the walk is asking.
async fn walk<F: roble::Fs>(
    db: &Src<'_, F>,
    model: &Model,
    lower: &Option<Vec<u8>>,
    upper: &Option<Vec<u8>>,
    moves: &[Move],
    at: &str,
) {
    let start = match lower {
        Some(k) => Bound::Included(k.as_slice()),
        None => Bound::Unbounded,
    };
    let end = match upper {
        Some(k) => Bound::Excluded(k.as_slice()),
        None => Bound::Unbounded,
    };
    let mut scan = db
        .range((start, end))
        .unwrap_or_else(|e| fail(&format!("{at}: walk: {e}")));
    let mut done = Vec::new();
    for mv in moves {
        let now = scan.is_valid().then(|| scan.key().to_vec());
        if now.is_none() && matches!(mv, Move::Next | Move::Prev) {
            continue;
        }
        let want =
            walk_want(model, lower, upper, now.as_deref(), mv).map(|(k, v)| (k.clone(), v.clone()));
        let (name, result) = match mv {
            Move::First => ("f".to_string(), scan.first().await),
            Move::Last => ("l".to_string(), scan.last().await),
            Move::Next => ("n".to_string(), scan.next().await),
            Move::Prev => ("p".to_string(), scan.prev().await),
            Move::SeekGe(k) => (
                format!("g:{}", String::from_utf8_lossy(k)),
                scan.seek_ge(k).await,
            ),
            Move::SeekLt(k) => (
                format!("t:{}", String::from_utf8_lossy(k)),
                scan.seek_lt(k).await,
            ),
        };
        done.push(name);
        result.unwrap_or_else(|e| fail(&format!("{at}: walk {}: {e}", done.join(" "))));
        let got = scan
            .is_valid()
            .then(|| (scan.key().to_vec(), scan.value().to_vec()));
        if got != want {
            fail(&format!(
                "{at}: walk over [{}, {}) after {}: roble is on {}, the history {}",
                lower.as_deref().map_or("-".into(), String::from_utf8_lossy),
                upper.as_deref().map_or("-".into(), String::from_utf8_lossy),
                done.join(" "),
                show(&got),
                show(&want)
            ));
        }
    }
}

/// Every table down to the last level, as `harness/differential`'s
/// `compact` has Pebble do.
async fn compact_all<F: roble::Fs>(db: &roble::Db<F>, at: &str) {
    for level in 0..6 {
        let tables: Vec<u64> = db
            .versions()
            .version
            .level(level)
            .iter()
            .map(|t| t.file_num)
            .collect();
        if !tables.is_empty() {
            db.compact(level, &tables)
                .await
                .unwrap_or_else(|e| fail(&format!("{at}: compact L{level}: {e}")));
        }
    }
}

async fn run(dir: &Path, script: &Path, segment: usize) {
    let text = std::fs::read_to_string(script)
        .unwrap_or_else(|e| fail(&format!("{}: {e}", script.display())));
    let all = segments(&text);
    let mut history = History::default();
    for op in all[..segment].iter().flatten() {
        history.apply(op);
    }

    // Small, so flushes and compactions keep happening, and inline, so they
    // happen inside the writes that need them and are done when this exits.
    // Writes are buffered, not synced: what the other engine opens next is
    // what this one closed, through the page cache, and durability is not
    // the subject (SimFs and tests/crash_loop.rs are).
    let options = roble::Options::new()
        .create_if_missing(true)
        .wal(roble::Wal::Buffered)
        .memtable_size(64 << 10)
        .target_file_size(16 << 10)
        .lbase_max_bytes(64 << 10)
        .background_tasks(false);
    let db = roble::open_with(&roble::DefaultEnv, dir, options)
        .await
        .unwrap_or_else(|e| {
            fail(&format!(
                "segment {segment}: roble could not open what Pebble left: {e}"
            ))
        });

    // The live snapshots, each with the map as it was when it was taken.
    let mut snaps: Vec<(roble::Snapshot<_>, Model)> = Vec::new();
    let mut batch: Option<roble::IndexedBatch<_>> = None;
    for (i, op) in all[segment].iter().enumerate() {
        let at = format!("segment {segment} (roble), operation {i}");
        let model = &history.model;
        match op {
            Op::Put(k, step, len) => db.put(k.as_bytes(), &value(*step, *len)).await,
            Op::Del(k) => db.delete(k.as_bytes()).await,
            Op::DelRange(a, b) => db.delete_range(a.as_bytes(), b.as_bytes()).await,
            Op::Flush => db.flush().await.map(|_| ()),
            Op::Compact => {
                compact_all(&db, &at).await;
                Ok(())
            }
            Op::Check(on) | Op::Walk(on, ..) => {
                let through;
                let (src, model, at) = match on {
                    At::Store => (Src::Db(&db), model, at.clone()),
                    At::Snap(i) => (
                        Src::Snap(&snaps[*i].0),
                        &snaps[*i].1,
                        format!("{at}, snapshot {i}"),
                    ),
                    At::Batch => {
                        through = history.through_batch();
                        let batch = batch.as_ref().expect("no batch is open");
                        (
                            Src::Batch(batch),
                            &through,
                            format!("{at}, through the batch"),
                        )
                    }
                };
                match op {
                    Op::Walk(_, lower, upper, moves) => {
                        walk(&src, model, lower, upper, moves, &at).await
                    }
                    _ => check(&src, model, &at).await,
                }
                Ok(())
            }
            Op::Snap => db.snapshot().map(|snap| snaps.push((snap, model.clone()))),
            Op::Unsnap(i) => {
                snaps.remove(*i);
                Ok(())
            }
            Op::Batch => {
                batch = Some(db.indexed_batch());
                Ok(())
            }
            Op::Batched(write) => {
                let b = batch.as_mut().expect("no batch is open");
                match &**write {
                    Op::Put(k, step, len) => b.put(k.as_bytes(), &value(*step, *len)),
                    Op::Del(k) => b.delete(k.as_bytes()),
                    Op::DelRange(a, z) => b.delete_range(a.as_bytes(), z.as_bytes()),
                    _ => unreachable!("only writes go into a batch"),
                }
            }
            Op::Commit => batch.take().expect("no batch is open").commit().await,
            Op::Abort => {
                batch = None;
                Ok(())
            }
        }
        .unwrap_or_else(|e| fail(&format!("{at}: {e}")));
        history.apply(op);
    }
    drop(batch);
    drop(snaps);
    db.close()
        .await
        .unwrap_or_else(|e| fail(&format!("segment {segment}: close: {e}")));
}

fn fail(message: &str) -> ! {
    eprintln!("differential: {message}");
    std::process::exit(1);
}

#[compio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let num = |s: &str| {
        s.parse::<u64>()
            .unwrap_or_else(|_| fail(&format!("not a number: {s}")))
    };
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["gen", seed, segments, ops] => generate(num(seed), num(segments), num(ops)),
        ["run", dir, script, segment] => {
            run(
                &PathBuf::from(dir),
                &PathBuf::from(script),
                num(segment) as usize,
            )
            .await
        }
        _ => fail("usage: differential gen <seed> <segments> <ops> | run <dir> <script> <segment>"),
    }
}
