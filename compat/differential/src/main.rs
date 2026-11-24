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
//! against the map with the batch laid over it, `biter` holds an iterator
//! through it that is to read the batch as it was when the iterator was
//! made while the batch takes more writes, and `commit` or `abort` ends it.
//! Beside the map, the store's own order: as it opens and closes, at every
//! `check`, and in every checkpoint it opens, the runner requires that what
//! lies above in the store is newer than what it overlaps below
//! (`Db::check_levels`), which compactions and both engines' scans rely on.
//! So a disagreement names the engine that read wrongly, the segment, and
//! what the other had written before it.
//!
//! Half the turns write with the WAL off, as Oxia runs Pebble: roble's with
//! [`roble::Wal::Off`], Pebble's with `DisableWAL`. Such a segment starts
//! `segment wal-off` and always ends with a `flush`, since with the WAL off
//! nothing else makes a write durable and Oxia flushes before it closes; a
//! checkpoint's store is flushed before it is closed for the same reason. A
//! turn with the WAL off opens what one with it on left, the log included,
//! and the reverse, so each replays and retires logs the other wrote or did
//! not.
//!
//! Some turns are killed. `run.sh` names an operation, or the open, and a
//! delay in microseconds; the turn starts a timer as it begins that one and
//! aborts the process when it fires, wherever it has got to. `run.sh` reads
//! how far the turn got from the `done` lines it printed after each
//! operation, and `truncate` cuts the script's segment there: the operations
//! done stay, an open batch is aborted, and a write that was under way when
//! the kill came becomes `maybe`, since it may or may not have landed. With
//! the WAL off, so does every write after the last flush or checkpoint the
//! turn finished: a kill loses the memtable, and what lasts is the writes up
//! to wherever the last flush to finish, the script's or the store's own,
//! cut them off. The next turn, the other engine's, opens what the killed one
//! left, reads it whole, finds how many of the `maybe`s, taken in order, make
//! what it holds, prints that, and `resolve` writes it into the script before
//! anything else reads it.
//!
//! ```text
//! differential gen <seed> <segments> <ops-per-segment>   script on stdout
//! differential run <dir> <script> <segment> [kill <op|open> <micros>]
//! differential truncate <script> <segment> <done>        cut a killed segment
//! differential resolve <script> <segment> <landed>       how many maybes landed
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    /// An iterator over `[lower, upper)` of what `At` names, held: appended
    /// to the live ones and moved by later operations.
    Iter(At, Option<Vec<u8>>, Option<Vec<u8>>),
    /// Moves of the held iterator at this index, from wherever the last ones
    /// left it.
    IterMove(usize, Vec<Move>),
    /// The held iterator at this index, closed.
    Uniter(usize),
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
    /// A write that was under way when the turn was killed, which may or may
    /// not have landed: the next turn decides which and `resolve` says so.
    Maybe(Box<Op>),
    /// A checkpoint of the store into the directory beside it named by this
    /// id, which no other checkpoint of the script has.
    Checkpoint(usize),
    /// The checkpoint with this id, opened, checked against the map as it
    /// stood when it was taken with the puts of earlier `CpCheck`s on top,
    /// given these puts, and closed: a checkpoint is a store of its own once
    /// opened, and the one who opens it writes to it.
    CpCheck(usize, Vec<(String, u64, usize)>),
}

/// What a check, a walk or a held iterator reads.
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

/// A key as the script spells it: `script_key` backwards, so that a message
/// names the move the script made and holds no zero byte.
fn spell(key: &[u8]) -> String {
    match key.strip_suffix(&[0]) {
        Some(k) => format!("{}+", String::from_utf8_lossy(k)),
        None => String::from_utf8_lossy(key).into_owned(),
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
        [w @ ("iter" | "biter"), lower, upper] => {
            let bound = |w: &str| (w != "-").then(|| script_key(w));
            let at = if *w == "biter" { At::Batch } else { At::Store };
            Op::Iter(at, bound(lower), bound(upper))
        }
        ["itermove", i, moves @ ..] => Op::IterMove(
            i.parse().expect("iterator"),
            moves.iter().map(|w| parse_move(w)).collect(),
        ),
        ["uniter", i] => Op::Uniter(i.parse().expect("iterator")),
        ["batch"] => Op::Batch,
        ["commit"] => Op::Commit,
        ["abort"] => Op::Abort,
        ["checkpoint", id] => Op::Checkpoint(id.parse().expect("checkpoint")),
        ["cpcheck", id, puts @ ..] => Op::CpCheck(
            id.parse().expect("checkpoint"),
            puts.iter()
                .map(|p| match p.split(':').collect::<Vec<_>>().as_slice() {
                    [k, step, len] => (
                        k.to_string(),
                        step.parse().expect("step"),
                        len.parse().expect("len"),
                    ),
                    _ => fail(&format!("cannot read the checkpoint put {p:?}")),
                })
                .collect(),
        ),
        ["maybe", rest @ ..] => Op::Maybe(Box::new(parse(&rest.join(" ")))),
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

/// One turn's part of the script.
struct Segment {
    /// Whether the turn writes with the WAL off.
    wal_off: bool,
    ops: Vec<Op>,
}

/// Whether `line` starts a segment, and if it does, whether the segment's
/// turn writes with the WAL off.
fn segment_header(line: &str) -> Option<bool> {
    match line {
        "segment" => Some(false),
        "segment wal-off" => Some(true),
        _ => None,
    }
}

/// The script's segments.
fn segments(script: &str) -> Vec<Segment> {
    let mut out: Vec<Segment> = Vec::new();
    for line in script.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if let Some(wal_off) = segment_header(line) {
            out.push(Segment {
                wal_off,
                ops: Vec::new(),
            });
        } else {
            out.last_mut()
                .unwrap_or_else(|| fail("an operation before the first segment"))
                .ops
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
    /// The `maybe`s not yet decided, in order, each with the batch that was
    /// open at it: the next turn finds how many of them landed.
    maybes: Vec<(&'s Op, Option<Vec<&'s Op>>)>,
    /// The checkpoints taken, each with the segment it was taken in and the
    /// map as it stood: what is committed, without an open batch's writes.
    checkpoints: HashMap<usize, (usize, Model)>,
    /// The segment the operations being applied are in.
    segment: usize,
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
            Op::Maybe(write) => {
                self.maybes.push((write, self.batch.clone()));
                // A commit ends its batch, whether or not it landed.
                if matches!(**write, Op::Commit) {
                    self.batch = None;
                }
            }
            Op::Checkpoint(id) => {
                self.checkpoints
                    .insert(*id, (self.segment, self.model.clone()));
            }
            Op::CpCheck(id, puts) => {
                if let Some((_, model)) = self.checkpoints.get_mut(id) {
                    for (k, step, len) in puts {
                        model.insert(k.as_bytes().to_vec(), value(*step, *len));
                    }
                }
            }
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
        Op::Iter(..) | Op::IterMove(..) | Op::Uniter(_) => {}
        Op::Checkpoint(_) | Op::CpCheck(..) => {}
        Op::Batch | Op::Batched(_) | Op::Commit | Op::Abort | Op::Maybe(_) => {
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
    // Checkpoints outlive the turn that took them, unlike snapshots: the
    // ones still read from now and then, and the ones the last turn took.
    let mut checkpoints: Vec<usize> = Vec::new();
    let mut taken: Vec<usize> = Vec::new();
    let mut next_checkpoint = 0;
    for _ in 0..segments {
        // Half the turns write with the WAL off, as Oxia runs Pebble.
        let wal_off = rng.below(2) == 0;
        out.push_str(if wal_off {
            "segment wal-off\n"
        } else {
            "segment\n"
        });
        // A check first, of what the other engine left, and one last.
        out.push_str("check\n");
        // Then every checkpoint the last turn took, so that the other engine
        // is the first to open each, as the engine took it.
        for id in taken.drain(..) {
            out.push_str(&cpcheck(&mut rng, &mut step, id));
            out.push('\n');
        }
        // Snapshots live in one engine's turn: it closes the store at the end.
        let mut snaps = 0;
        // So does an open batch, ended before the turn is.
        let mut batch = false;
        // And held iterators: the keys each spans, for its moves to seek in,
        // and whether it reads through the open batch, since those are
        // closed before the batch ends.
        let mut iters: Vec<(u64, u64, bool)> = Vec::new();
        for _ in 0..ops {
            step += 1;
            let line = match rng.below(100) {
                // Through the open batch, and moved while the batch takes
                // writes. Made a while after the batch is, so that the batch
                // holds writes from before an iterator was made as well as
                // after.
                0..=2 if batch => gen_held(&mut rng, &mut iters, true),
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
                90..=91 => gen_walk(&mut rng),
                // Held for a while and moved now and then, so that writes,
                // flushes and compactions land between its moves.
                92..=93 => gen_held(&mut rng, &mut iters, false),
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
                        format!("{}commit", close_batch_iters(&mut iters))
                    }
                    4 => {
                        batch = false;
                        format!("{}abort", close_batch_iters(&mut iters))
                    }
                    5..=11 => "bcheck".to_string(),
                    _ => format!("b{}", gen_walk(&mut rng)),
                },
                // Read again later by either engine, after it or the other
                // has opened them, and four at most in reach.
                // Not one this turn took: the other engine opens that first.
                98 if checkpoints.iter().all(|id| taken.contains(id)) || rng.below(2) == 0 => {
                    let id = next_checkpoint;
                    next_checkpoint += 1;
                    if checkpoints.len() == 4 {
                        checkpoints.remove(0);
                    }
                    checkpoints.push(id);
                    taken.push(id);
                    format!("checkpoint {id}")
                }
                98 => {
                    let older: Vec<usize> = checkpoints
                        .iter()
                        .copied()
                        .filter(|id| !taken.contains(id))
                        .collect();
                    let id = older[rng.below(older.len() as u64) as usize];
                    cpcheck(&mut rng, &mut step, id)
                }
                _ => "check".to_string(),
            };
            out.push_str(&line);
            out.push('\n');
        }
        if batch {
            out.push_str(&close_batch_iters(&mut iters));
            out.push_str(if rng.below(4) == 0 {
                "abort\n"
            } else {
                "commit\n"
            });
        }
        // With the WAL off a segment always ends flushed, as Oxia flushes
        // before it closes: nothing else would keep its last writes. With it
        // on, half the segments end flushed, and half leave their last writes
        // in the log for the other engine to replay.
        if wal_off || rng.below(2) == 0 {
            out.push_str("flush\n");
        }
        out.push_str("check\n");
    }
    print!("{out}");
}

/// A read of checkpoint `id`, and up to four short puts into it after.
fn cpcheck(rng: &mut Rng, step: &mut u64, id: usize) -> String {
    let mut line = format!("cpcheck {id}");
    for _ in 0..rng.below(5) {
        *step += 1;
        line.push_str(&format!(
            " {}:{step}:{}",
            key(rng.below(KEYS)),
            1 + rng.below(200)
        ));
    }
    line
}

/// An operation on a held iterator over the store, or through the open
/// batch: a new one when none of its kind is held, or a quarter of the time
/// when fewer than three over the store or two through the batch are, and
/// otherwise moves of one of its kind, or a sixth of the time its close.
fn gen_held(rng: &mut Rng, iters: &mut Vec<(u64, u64, bool)>, through: bool) -> String {
    let held: Vec<usize> = (0..iters.len())
        .filter(|&i| iters[i].2 == through)
        .collect();
    let most = if through { 2 } else { 3 };
    if held.is_empty() || (held.len() < most && rng.below(4) == 0) {
        let (bounds, from, to) = gen_bounds(rng);
        iters.push((from, to, through));
        format!("{}iter {bounds}", if through { "b" } else { "" })
    } else {
        let i = held[rng.below(held.len() as u64) as usize];
        if rng.below(6) == 0 {
            iters.remove(i);
            format!("uniter {i}")
        } else {
            let (from, to, _) = iters[i];
            let n = 2 + rng.below(8);
            format!("itermove {i}{}", gen_moves(rng, from, to, n))
        }
    }
}

/// A `uniter` for each held iterator through the open batch, to close them
/// before the batch is committed or aborted: the last first, so that each
/// close leaves the indices the next lines name as they were.
fn close_batch_iters(iters: &mut Vec<(u64, u64, bool)>) -> String {
    let mut lines = String::new();
    for i in (0..iters.len()).rev() {
        if iters[i].2 {
            iters.remove(i);
            lines.push_str(&format!("uniter {i}\n"));
        }
    }
    lines
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
    let (bounds, from, to) = gen_bounds(rng);
    format!("walk {bounds}{}", gen_moves(rng, from, to, 40))
}

/// A range to walk, as the script spells it, and the keys it spans: mostly a
/// few hundred keys, with either side now and then open, and now and then
/// the whole alphabet.
fn gen_bounds(rng: &mut Rng) -> (String, u64, u64) {
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
    (format!("{lower} {upper}"), from, to)
}

/// `n` moves, each with a space before it, seeking on keys of `[from, to)`
/// and just outside it.
fn gen_moves(rng: &mut Rng, from: u64, to: u64, n: u64) -> String {
    let mut line = String::new();
    for _ in 0..n {
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

/// Fails unless what lies above in the store is newer than what it overlaps
/// below (`Db::check_levels`): what compactions and both engines' scans rely
/// on, whether or not a read has gone wrong yet.
async fn levels<F: roble::Fs>(db: &roble::Db<F>, at: &str) {
    db.check_levels()
        .await
        .unwrap_or_else(|e| fail(&format!("{at}: the levels are out of order: {e}")));
}

async fn check<F: roble::Fs>(db: &Src<'_, F>, model: &Model, at: &str) {
    if let Src::Db(store) = db {
        levels(store, at).await;
    }
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
    let mut walker = Walker::new(db, model.clone(), lower.clone(), upper.clone(), at);
    walker.walk(moves, at).await;
}

/// An iterator over a range, the map it is to agree with, and the moves made
/// with it so far: a walk's, or a held iterator's, which keeps its position
/// and its map from one operation to the next while the store changes under
/// it.
struct Walker<F: roble::Fs> {
    scan: roble::Scan<F>,
    model: Model,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    done: Vec<String>,
}

impl<F: roble::Fs> Walker<F> {
    fn new(
        db: &Src<'_, F>,
        model: Model,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        at: &str,
    ) -> Self {
        let start = match &lower {
            Some(k) => Bound::Included(k.as_slice()),
            None => Bound::Unbounded,
        };
        let end = match &upper {
            Some(k) => Bound::Excluded(k.as_slice()),
            None => Bound::Unbounded,
        };
        let scan = db
            .range((start, end))
            .unwrap_or_else(|e| fail(&format!("{at}: walk: {e}")));
        Walker {
            scan,
            model,
            lower,
            upper,
            done: Vec::new(),
        }
    }

    async fn walk(&mut self, moves: &[Move], at: &str) {
        let (scan, model, lower, upper) = (&mut self.scan, &self.model, &self.lower, &self.upper);
        for mv in moves {
            let now = scan.is_valid().then(|| scan.key().to_vec());
            if now.is_none() && matches!(mv, Move::Next | Move::Prev) {
                continue;
            }
            let want = walk_want(model, lower, upper, now.as_deref(), mv)
                .map(|(k, v)| (k.clone(), v.clone()));
            let (name, result) = match mv {
                Move::First => ("f".to_string(), scan.first().await),
                Move::Last => ("l".to_string(), scan.last().await),
                Move::Next => ("n".to_string(), scan.next().await),
                Move::Prev => ("p".to_string(), scan.prev().await),
                Move::SeekGe(k) => (format!("g:{}", spell(k)), scan.seek_ge(k).await),
                Move::SeekLt(k) => (format!("t:{}", spell(k)), scan.seek_lt(k).await),
            };
            self.done.push(name);
            result.unwrap_or_else(|e| fail(&format!("{at}: walk {}: {e}", self.done.join(" "))));
            let got = scan
                .is_valid()
                .then(|| (scan.key().to_vec(), scan.value().to_vec()));
            if got != want {
                fail(&format!(
                    "{at}: walk over [{}, {}) after {}: roble is on {}, the history {}",
                    lower.as_deref().map_or("-".into(), String::from_utf8_lossy),
                    upper.as_deref().map_or("-".into(), String::from_utf8_lossy),
                    self.done.join(" "),
                    show(&got),
                    show(&want)
                ));
            }
        }
    }
}

/// Every table down to the last level, as `harness/differential`'s
/// `compact` has Pebble do.
async fn compact_all<F: roble::Fs>(db: &roble::Db<F>, background: bool, at: &str) {
    let mut level = 0;
    while level < 6 {
        let tables: Vec<u64> = db
            .versions()
            .version
            .level(level)
            .iter()
            .map(|t| t.file_num)
            .collect();
        if tables.is_empty() {
            level += 1;
            continue;
        }
        match db.compact(level, &tables).await {
            Ok(_) => level += 1,
            // The store's own merge task has some of them: wait for it, and
            // take the level again as it then stands.
            Err(roble::Error::Unsupported(_)) if background => db.compaction_settled().await,
            Err(e) => fail(&format!("{at}: compact L{level}: {e}")),
        }
    }
}

/// Decides the `maybe`s the killed turn before this one left: the write it
/// had under way and, with the WAL off, every one it made after the last
/// flush or checkpoint it finished. What survived the kill is some first
/// ones of them, in order, since a flush writes a memtable out whole and
/// the older ones first. Reads the store whole, finds how many of them make
/// what it holds, and prints that for `run.sh` to `resolve` into the script.
/// When several counts do, since one write can undo another, the most is
/// taken: each leaves the same map.
async fn decide<F: roble::Fs>(db: &roble::Db<F>, history: &mut History<'_>, segment: usize) {
    if history.maybes.is_empty() {
        return;
    }
    let at = format!("segment {segment}: deciding what the killed turn left");
    let held = contents(db, &at).await;
    let maybes = std::mem::take(&mut history.maybes);
    let mut model = history.model.clone();
    let mut landed = None;
    // The count whose map differs from the store's at the fewest keys, for
    // the message if none matches.
    let mut closest: Option<(usize, usize, Model)> = None;
    for n in 0..=maybes.len() {
        if n > 0 {
            let (write, batch) = &maybes[n - 1];
            let mut then = History {
                model,
                batch: batch.clone(),
                ..History::default()
            };
            then.apply(write);
            model = then.model;
        }
        let differ = differing(&model, &held).count();
        if differ == 0 {
            landed = Some(n);
        } else if closest.as_ref().is_none_or(|c| differ < c.0) {
            closest = Some((differ, n, model.clone()));
        }
    }
    let Some(n) = landed else {
        let (differ, n, model) = closest.expect("a count that does not match");
        let k = differing(&model, &held)
            .next()
            .expect("a key they differ at");
        fail(&format!(
            "{at}: the store holds none of the {} maps its {} writes in doubt could have left; \
             the closest, with the first {n} landed, differs from it at {differ} keys, first at \
             {}: the store holds {}, the history {}",
            maybes.len() + 1,
            maybes.len(),
            spell(k),
            show(&held.get(k).map(|v| (k.clone(), v.clone()))),
            show(&model.get(k).map(|v| (k.clone(), v.clone())))
        ));
    };
    history.model = held;
    println!("resolved {n}");
}

/// Everything the store holds, read with one forward scan.
async fn contents<F: roble::Fs>(db: &roble::Db<F>, at: &str) -> Model {
    let mut scan = db
        .range(..)
        .unwrap_or_else(|e| fail(&format!("{at}: scan: {e}")));
    let mut held = Model::new();
    scan.first()
        .await
        .unwrap_or_else(|e| fail(&format!("{at}: first: {e}")));
    while scan.is_valid() {
        held.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next()
            .await
            .unwrap_or_else(|e| fail(&format!("{at}: next: {e}")));
    }
    held
}

/// The keys two maps disagree on, in order.
fn differing<'m>(a: &'m Model, b: &'m Model) -> impl Iterator<Item = &'m Vec<u8>> {
    let keys: BTreeSet<&Vec<u8>> = a.keys().chain(b.keys()).collect();
    keys.into_iter().filter(move |k| a.get(*k) != b.get(*k))
}

/// The script's lines, split into the lines before segment `segment`, its
/// own, and the lines after.
fn split_segment(script: &str, segment: usize) -> (Vec<&str>, Vec<&str>, Vec<&str>) {
    let lines: Vec<&str> = script.lines().collect();
    let starts: Vec<usize> = (0..lines.len())
        .filter(|&i| segment_header(lines[i]).is_some())
        .collect();
    let start = starts[segment] + 1;
    let end = starts.get(segment + 1).copied().unwrap_or(lines.len());
    (
        lines[..start].to_vec(),
        lines[start..end].to_vec(),
        lines[end..].to_vec(),
    )
}

fn rewrite(script: &Path, before: Vec<&str>, own: Vec<String>, after: Vec<&str>) {
    let mut out = before.join("\n");
    for line in own.iter().map(String::as_str).chain(after) {
        out.push('\n');
        out.push_str(line);
    }
    out.push('\n');
    std::fs::write(script, out).unwrap_or_else(|e| fail(&format!("{}: {e}", script.display())));
}

/// The first word of a script line: what it does.
fn verb(line: &str) -> &str {
    line.split_whitespace().next().unwrap_or("")
}

/// Whether a script line writes to the store.
fn writes(line: &str) -> bool {
    ["put", "del", "delrange", "commit"].contains(&verb(line))
}

/// Cuts segment `segment`, killed after `done` operations: those stay, any
/// open batch is aborted, and a checkpoint whose `cpcheck` was under way is
/// not read again. A write under way becomes `maybe`, since it may or may
/// not have landed, and with the WAL off so does every write after the last
/// flush or checkpoint the turn finished. Prints how many writes are in
/// doubt.
fn truncate(script: &Path, segment: usize, done: usize) {
    let text = std::fs::read_to_string(script).unwrap_or_else(|e| fail(&format!("{e}")));
    let (before, own, mut after) = split_segment(&text, segment);
    let done = done.min(own.len());
    let wal_off = before.last().copied().and_then(segment_header) == Some(true);
    // With the WAL off a write is sure to have landed only once a flush
    // has written it out: one of the script's, or a checkpoint's. Others
    // may have, the store's own, and the next turn finds out.
    let sure = if wal_off {
        own[..done]
            .iter()
            .rposition(|l| ["flush", "checkpoint"].contains(&verb(l)))
            .map_or(0, |i| i + 1)
    } else {
        done
    };
    let mut kept: Vec<String> = own[..done]
        .iter()
        .enumerate()
        .map(|(i, l)| {
            if i >= sure && writes(l) {
                format!("maybe {l}")
            } else {
                l.to_string()
            }
        })
        .collect();
    if let Some(next) = own.get(done) {
        let word = verb(next);
        if writes(next) {
            kept.push(format!("maybe {next}"));
        }
        // Its puts may or may not have landed, and nothing reads one to
        // decide: the checkpoint is not read again.
        if word == "cpcheck" {
            let id = next.split_whitespace().nth(1);
            after.retain(|l| !(l.starts_with("cpcheck ") && l.split_whitespace().nth(1) == id));
        }
    }
    let doubt = kept.iter().filter(|l| l.starts_with("maybe ")).count();
    kept.push("abort".to_string());
    rewrite(script, before, kept, after);
    println!("{doubt}");
}

/// Writes the next turn's decision into segment `segment`: the first
/// `landed` of its `maybe`s become their writes, and the rest go.
fn resolve(script: &Path, segment: usize, landed: usize) {
    let text = std::fs::read_to_string(script).unwrap_or_else(|e| fail(&format!("{e}")));
    let (before, own, after) = split_segment(&text, segment);
    let mut seen = 0;
    let own = own
        .iter()
        .filter_map(|l| match l.strip_prefix("maybe ") {
            Some(write) => {
                seen += 1;
                (seen <= landed).then(|| write.to_string())
            }
            None => Some(l.to_string()),
        })
        .collect();
    rewrite(script, before, own, after);
}

/// Where a turn that is to be killed dies: as it begins this operation, or
/// the open, plus this many microseconds.
#[derive(Clone, Copy)]
struct Kill {
    at: Option<usize>,
    micros: u64,
}

impl Kill {
    /// Starts the timer if `at` is where it was asked for. The process ends
    /// as a kill ends it: no destructor runs, nothing buffered is written,
    /// and what the OS already holds stays.
    fn arm(kill: Option<Kill>, at: Option<usize>) {
        if let Some(k) = kill.filter(|k| k.at == at) {
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_micros(k.micros));
                std::process::abort();
            });
        }
    }
}

async fn run(dir: &Path, script: &Path, segment: usize, kill: Option<Kill>) {
    let sync = kill.is_some();
    let text = std::fs::read_to_string(script)
        .unwrap_or_else(|e| fail(&format!("{}: {e}", script.display())));
    let all = segments(&text);
    let mut history = History::default();
    for (i, earlier) in all[..segment].iter().enumerate() {
        history.segment = i;
        for op in &earlier.ops {
            history.apply(op);
        }
    }
    history.segment = segment;
    let wal_off = all[segment].wal_off;

    // Small, so flushes and compactions keep happening. Every other roble
    // turn runs them inline, inside the writes that need them, and the rest
    // in the store's own tasks, as a store is opened by default: there a
    // flush or a merge runs beside the turn's writes and reads, and a kill
    // may find one under way that no operation of the script asked for.
    // Either way they are done when the turn closes, which waits for the
    // tasks.
    // Writes are buffered, not synced: what the other engine opens next is
    // what this one left, through the page cache, which a kill does not
    // lose either, and a machine crash is not the subject (SimFs is). A turn
    // that may be killed syncs anyway, as Pebble's has to (its unsynced
    // writes wait in its own buffers), so that both paths are run. With the
    // WAL off there is nothing to sync: a write lasts once a flush has
    // written it out, and a kill loses what none has.
    let background = segment / 2 % 2 == 1;
    let options = || {
        roble::Options::new()
            .wal(if wal_off {
                roble::Wal::Off
            } else if sync {
                roble::Wal::Synced
            } else {
                roble::Wal::Buffered
            })
            .memtable_size(64 << 10)
            .target_file_size(16 << 10)
            .lbase_max_bytes(64 << 10)
            .background_tasks(background)
    };
    Kill::arm(kill, None);
    let db = roble::open_with(&roble::DefaultEnv, dir, options().create_if_missing(true))
        .await
        .unwrap_or_else(|e| {
            fail(&format!(
                "segment {segment}: roble could not open what Pebble left: {e}"
            ))
        });

    // A kill before this line was a kill inside the open.
    println!("opened");
    levels(&db, &format!("segment {segment} (roble), as opened")).await;
    decide(&db, &mut history, segment).await;

    // The live snapshots, each with the map as it was when it was taken.
    let mut snaps: Vec<(roble::Snapshot<_>, Model)> = Vec::new();
    // The held iterators, each with the map as it was when it was made.
    let mut iters: Vec<Walker<_>> = Vec::new();
    let mut batch: Option<roble::IndexedBatch<_>> = None;
    for (i, op) in all[segment].ops.iter().enumerate() {
        let at = format!("segment {segment} (roble), operation {i}");
        Kill::arm(kill, Some(i));
        let model = &history.model;
        match op {
            Op::Put(k, step, len) => db.put(k.as_bytes(), &value(*step, *len)).await,
            Op::Del(k) => db.delete(k.as_bytes()).await,
            Op::DelRange(a, b) => db.delete_range(a.as_bytes(), b.as_bytes()).await,
            Op::Flush => db.flush().await.map(|_| ()),
            Op::Compact => {
                compact_all(&db, background, &at).await;
                Ok(())
            }
            Op::Check(on) | Op::Walk(on, ..) | Op::Iter(on, ..) => {
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
                    // With the map as it is now: whatever the store or the
                    // batch is given later, the iterator is not to see.
                    Op::Iter(_, lower, upper) => {
                        iters.push(Walker::new(&src, model.clone(), lower.clone(), upper.clone(), &at))
                    }
                    _ => check(&src, model, &at).await,
                }
                Ok(())
            }
            Op::IterMove(i, moves) => {
                iters[*i]
                    .walk(moves, &format!("{at}, held iterator {i}"))
                    .await;
                Ok(())
            }
            Op::Uniter(i) => {
                iters.remove(*i);
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
            Op::Checkpoint(id) => db.checkpoint(checkpoint_dir(dir, *id)).await,
            Op::CpCheck(id, puts) => {
                // Not taken if the turn that was to take it was killed first.
                let Some((taken, model)) = history.checkpoints.get(id) else {
                    eprintln!("{at}: checkpoint {id} was cut with the turn that was to take it");
                    println!("done {i}");
                    continue;
                };
                // Not created if missing: an empty store would pass for a
                // checkpoint of an empty map.
                let cp = roble::open_with(&roble::DefaultEnv, checkpoint_dir(dir, *id), options())
                    .await
                    .unwrap_or_else(|e| {
                        fail(&format!(
                            "{at}: roble could not open checkpoint {id}, taken in segment {taken}: {e}"
                        ))
                    });
                check(&Src::Db(&cp), model, &format!("{at}, checkpoint {id}")).await;
                for (k, step, len) in puts {
                    cp.put(k.as_bytes(), &value(*step, *len))
                        .await
                        .unwrap_or_else(|e| fail(&format!("{at}, checkpoint {id}: {e}")));
                }
                // With the WAL off its puts are in its memtable alone, which
                // a close does not write out: flushed first, as Oxia closes.
                if wal_off {
                    cp.flush()
                        .await
                        .unwrap_or_else(|e| fail(&format!("{at}, checkpoint {id}: flush: {e}")));
                }
                cp.close().await
            }
            Op::Maybe(_) => fail(&format!("{at}: a maybe in a turn's own segment")),
        }
        .unwrap_or_else(|e| fail(&format!("{at}: {e}")));
        history.apply(op);
        // How far a killed turn got: `run.sh` reads the last of these.
        println!("done {i}");
    }
    drop(batch);
    drop(snaps);
    drop(iters);
    levels(&db, &format!("segment {segment} (roble), at the close")).await;
    db.close()
        .await
        .unwrap_or_else(|e| fail(&format!("segment {segment}: close: {e}")));
}

/// Where the checkpoint with this id goes: beside the store, so that a run's
/// directory holds both and is removed with them.
fn checkpoint_dir(dir: &Path, id: usize) -> PathBuf {
    let mut name = dir.as_os_str().to_owned();
    name.push(format!("-cp{id}"));
    PathBuf::from(name)
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
        ["run", dir, script, segment, kill @ ..] => {
            let kill = match kill {
                [] => None,
                ["kill", at, micros] => Some(Kill {
                    at: (*at != "open").then(|| num(at) as usize),
                    micros: num(micros),
                }),
                _ => fail("run takes kill <op|open> <micros> or nothing after the segment"),
            };
            run(
                &PathBuf::from(dir),
                &PathBuf::from(script),
                num(segment) as usize,
                kill,
            )
            .await
        }
        ["truncate", script, segment, done] => truncate(
            &PathBuf::from(script),
            num(segment) as usize,
            num(done) as usize,
        ),
        ["resolve", script, segment, landed] => resolve(
            &PathBuf::from(script),
            num(segment) as usize,
            num(landed) as usize,
        ),
        _ => fail(
            "usage: differential gen <seed> <segments> <ops> | run <dir> <script> <segment> \
             [kill <op|open> <micros>] \
             | truncate <script> <segment> <done> | resolve <script> <segment> <landed>",
        ),
    }
}
