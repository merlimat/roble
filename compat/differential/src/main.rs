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
//! and turning round, and requires the map's answer after every move. So a
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
    Check,
    /// A scan over `[lower, upper)`, either side open when `None`, and the
    /// moves to make with it.
    Walk(Option<Vec<u8>>, Option<Vec<u8>>, Vec<Move>),
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
        ["check"] => Op::Check,
        ["walk", lower, upper, moves @ ..] => {
            let bound = |w: &str| (w != "-").then(|| script_key(w));
            Op::Walk(
                bound(lower),
                bound(upper),
                moves.iter().map(|w| parse_move(w)).collect(),
            )
        }
        _ => fail(&format!("cannot read {line:?}")),
    }
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
        Op::Flush | Op::Compact | Op::Check | Op::Walk(..) => {}
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
        for _ in 0..ops {
            step += 1;
            let line = match rng.below(100) {
                0..=57 => {
                    // Mostly short, now and then past a 4 KiB block, and
                    // rarely past a 32 KiB log block.
                    let len = match rng.below(100) {
                        0..=89 => 1 + rng.below(200),
                        90..=98 => 4_000 + rng.below(4_000),
                        _ => 33_000 + rng.below(8_000),
                    };
                    format!("put {} {step} {len}", key(rng.below(KEYS)))
                }
                58..=77 => format!("del {}", key(rng.below(KEYS))),
                78..=82 => {
                    let a = rng.below(KEYS);
                    let b = a + 1 + rng.below(30);
                    format!("delrange {} {}", key(a), key(b))
                }
                83..=90 => "flush".to_string(),
                // Rare, since it flattens the store into its last level, and
                // the layouts worth reading are the ones before that.
                91 if rng.below(4) == 0 => "compact".to_string(),
                91 => "flush".to_string(),
                92..=95 => gen_walk(&mut rng),
                _ => "check".to_string(),
            };
            out.push_str(&line);
            out.push('\n');
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

async fn check<F: roble::Fs>(db: &roble::Db<F>, model: &Model, at: &str) {
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
    db: &roble::Db<F>,
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
    let mut model = Model::new();
    for op in all[..segment].iter().flatten() {
        apply(&mut model, op);
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

    for (i, op) in all[segment].iter().enumerate() {
        let at = format!("segment {segment} (roble), operation {i}");
        match op {
            Op::Put(k, step, len) => db.put(k.as_bytes(), &value(*step, *len)).await,
            Op::Del(k) => db.delete(k.as_bytes()).await,
            Op::DelRange(a, b) => db.delete_range(a.as_bytes(), b.as_bytes()).await,
            Op::Flush => db.flush().await.map(|_| ()),
            Op::Compact => {
                compact_all(&db, &at).await;
                Ok(())
            }
            Op::Check => {
                check(&db, &model, &at).await;
                Ok(())
            }
            Op::Walk(lower, upper, moves) => {
                walk(&db, &model, lower, upper, moves, &at).await;
                Ok(())
            }
        }
        .unwrap_or_else(|e| fail(&format!("{at}: {e}")));
        apply(&mut model, op);
    }
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
