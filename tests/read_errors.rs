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

//! Reads when the disk does not cooperate (DESIGN.md §12).
//!
//! `tests/failure.rs` fails every operation a write, a flush or a compaction
//! makes, one at a time, and checks what survives. This does the same to
//! reading. Each read a lookup, a scan or a compaction makes is failed in
//! turn, and every answer is checked against a model of what the store holds.
//!
//! The rule is the one a read can keep whatever the disk does: **an answer is
//! right or it is an error.** A lookup or a step that meets a failed read
//! returns the error, and nothing returned before it is wrong. This is the
//! class of bug RocksDB has shipped more than once. An iterator went on past
//! a file it could not read, and a compaction lost the keys of one
//! (`RangeDelReseekAfterFileReadError`, `ErrorWhenReadFile`, and two fixes in
//! its 8.7.0). The shape it takes is a key quietly missing, not an error.
//!
//! What met the error has to be usable afterwards. The store answers the
//! next lookup correctly, through the same buffer. A scan is positioned
//! nowhere until it is positioned again, and then it reads correctly.
//!
//! Nothing is cached, so every read a call makes reaches the disk and any of
//! them can be the one that fails. A guard checks that failures landed on
//! table opens and on block reads, in seeks and in steps. A sweep whose
//! failures all landed in one place would pass while proving much less.

use std::collections::{BTreeMap, BTreeSet};

use roble::sim::{SimEnv, SimFs};
use roble::{Caches, Comparison, GetBuf, Options, Scan};
use roble_format::sst::WriterOptions;

mod lsm;

type Db = roble::Db<SimFs>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const DIR: &str = "/db";

/// Long and random enough that a table of forty keys takes several data
/// blocks: Snappy, which compresses the tables below L0, cannot shrink it.
const VALUE_LEN: usize = 1000;

fn key(i: usize) -> Vec<u8> {
    format!("k{i:03}").into_bytes()
}

/// A value naming the write it came from, padded out with bytes that do not
/// compress.
fn value(tag: &str, i: usize) -> Vec<u8> {
    let mut out = format!("{tag}:{i:03}:").into_bytes();
    let mut x = tag
        .bytes()
        .fold(0x9e37_79b9_7f4a_7c15_u64 ^ i as u64, |h, b| {
            h.wrapping_mul(0x100_0000_01b3).wrapping_add(u64::from(b))
        })
        | 1;
    while out.len() < VALUE_LEN {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push(x as u8);
    }
    out
}

fn show(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

fn options() -> Options<SimFs> {
    Options::new()
        .create_if_missing(true)
        .disable_automatic_compactions(true)
        .background_tasks(false)
        .caches(Caches::new(0, 0))
}

async fn open(env: &SimEnv) -> Db {
    reopen(env, "open").await
}

async fn reopen(env: &SimEnv, what: &str) -> Db {
    roble::open_with(env, DIR, options())
        .await
        .unwrap_or_else(|e| panic!("{what}: {e}"))
}

/// Writes to a store and to its model together.
struct Writer<'a> {
    db: &'a Db,
    model: Model,
}

impl Writer<'_> {
    async fn put(&mut self, i: usize, tag: &str) {
        let (k, v) = (key(i), value(tag, i));
        self.db.put(&k, &v).await.expect("put");
        self.model.insert(k, v);
    }

    async fn delete(&mut self, i: usize) {
        self.db.delete(&key(i)).await.expect("delete");
        self.model.remove(&key(i));
    }

    async fn delete_range(&mut self, from: usize, to: usize) {
        let (from, to) = (key(from), key(to));
        self.db
            .delete_range(&from, &to)
            .await
            .expect("delete_range");
        self.model.retain(|k, _| !(&from <= k && k < &to));
    }
}

/// Flushes the memtable and compacts its table down to `level`.
async fn down(db: &Db, level: u32) {
    let mut tables = vec![db.flush().await.expect("flush").expect("a table")];
    for from in 0..level {
        tables = db.compact(from, &tables).await.expect("compact");
    }
}

fn tables_at(db: &Db, level: u32) -> Vec<u64> {
    db.versions()
        .version
        .level(level)
        .iter()
        .map(|t| t.file_num)
        .collect()
}

/// A store whose reads cross every boundary a failed read can land on.
///
/// - **L2** has three tables side by side, forty keys and several data blocks
///   each. A scan crosses from block to block, and from table to table, which
///   opens the next table in the middle of a step.
/// - **L1** has one table over all three, holding a range deletion that spans
///   them. A scan that meets it skips the levels below with a seek of their
///   own.
/// - **L0** has two tables that overlap. They bring keys back inside the
///   deleted span, and delete keys the levels below hold.
/// - **The memtable** holds a write and a deletion of its own.
async fn build() -> (SimEnv, Db, Model) {
    let env = SimEnv::new();
    let db = open(&env).await;
    let mut w = Writer {
        db: &db,
        model: Model::new(),
    };
    for part in 0..3 {
        for i in part * 40..(part + 1) * 40 {
            w.put(i, "base").await;
        }
        down(&db, 2).await;
    }
    w.put(10, "l1").await;
    w.put(95, "l1").await;
    w.delete(100).await;
    w.delete_range(30, 90).await;
    down(&db, 1).await;
    w.put(5, "old").await;
    w.delete(15).await;
    w.put(50, "back").await;
    down(&db, 0).await;
    w.put(5, "new").await;
    w.delete_range(110, 115).await;
    w.put(60, "back").await;
    down(&db, 0).await;
    w.put(1, "mem").await;
    w.delete(2).await;
    w.put(70, "mem").await;
    let model = w.model;

    let shape: Vec<usize> = (0..3).map(|l| tables_at(&db, l).len()).collect();
    assert_eq!(shape, [2, 1, 3], "tables at L0, L1 and L2");
    (env, db, model)
}

/// Failures by where they landed, so that a sweep can show it reached what it
/// set out to.
#[derive(Default, Debug)]
struct Tally(BTreeMap<String, usize>);

impl Tally {
    fn saw(&mut self, what: &str, e: &roble::Error) {
        let text = e.to_string();
        let op = if text.contains("(Open)") {
            "open"
        } else if text.contains("(Read)") {
            "read"
        } else {
            "other"
        };
        *self.0.entry(format!("{what} {op}")).or_default() += 1;
    }

    fn require(&self, buckets: &[&str]) {
        for bucket in buckets {
            assert!(
                self.0.get(*bucket).is_some_and(|&n| n > 0),
                "no failure landed on a {bucket}, so the sweep did not test it: {:?}",
                self.0
            );
        }
    }
}

/// Everything a scan of the whole store finds, forward or backward.
async fn contents(db: &Db, backward: bool) -> roble::Result<Model> {
    let mut scan = db.range(..)?;
    let mut out = Model::new();
    if backward {
        scan.last().await?;
    } else {
        scan.first().await?;
    }
    while scan.is_valid() {
        out.insert(scan.key().to_vec(), scan.value().to_vec());
        if backward {
            scan.prev().await?;
        } else {
            scan.next().await?;
        }
    }
    Ok(out)
}

/// That the store holds exactly `model`, read both ways.
async fn holds(db: &Db, model: &Model, what: &str) {
    for backward in [false, true] {
        let got = contents(db, backward)
            .await
            .unwrap_or_else(|e| panic!("{what}: reading it back: {e}"));
        if &got != model {
            let missing: Vec<String> = model
                .keys()
                .filter(|k| got.get(*k) != model.get(*k))
                .map(|k| show(k))
                .collect();
            let extra: Vec<String> = got
                .keys()
                .filter(|k| !model.contains_key(*k))
                .map(|k| show(k))
                .collect();
            panic!(
                "{what}: read {}, it holds the wrong things; missing or changed {missing:?}, \
                 extra {extra:?}",
                if backward { "backward" } else { "forward" }
            );
        }
    }
}

const PROBES: &[&str] = &[
    "k000", "k001", "k002", "k005", "k015", "k029", "k030", "k045", "k050", "k070", "k089", "k090",
    "k095", "k100", "k112", "k119", "k120",
];

const COMPARISONS: [Comparison; 5] = [
    Comparison::Equal,
    Comparison::Floor,
    Comparison::Ceiling,
    Comparison::Lower,
    Comparison::Higher,
];

/// The model's answer to a lookup.
fn answer(model: &Model, probe: &[u8], how: Comparison) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut keys = model.iter();
    let hit = match how {
        Comparison::Equal => model.get_key_value(probe),
        Comparison::Floor => keys.rev().find(|(k, _)| k.as_slice() <= probe),
        Comparison::Ceiling => keys.find(|(k, _)| k.as_slice() >= probe),
        Comparison::Lower => keys.rev().find(|(k, _)| k.as_slice() < probe),
        Comparison::Higher => keys.find(|(k, _)| k.as_slice() > probe),
    };
    hit.map(|(k, v)| (k.clone(), v.clone()))
}

/// That a lookup through `buf` answers as the model does.
async fn looks_up(
    db: &Db,
    buf: &mut GetBuf,
    model: &Model,
    probe: &str,
    how: Comparison,
    what: &str,
) {
    let found = db
        .get_into(probe.as_bytes(), how, buf)
        .await
        .unwrap_or_else(|e| panic!("{what}: {how:?} {probe} failed: {e}"));
    let got = found.then(|| (buf.key().to_vec(), buf.value().to_vec()));
    let want = answer(model, probe.as_bytes(), how);
    assert!(
        got == want,
        "{what}: {how:?} {probe} answered {:?}, where the store holds {:?}{}",
        got.as_ref().map(|(k, _)| show(k)),
        want.as_ref().map(|(k, _)| show(k)),
        if got.as_ref().map(|(k, _)| k) == want.as_ref().map(|(k, _)| k) {
            " with another value"
        } else {
            ""
        }
    );
}

#[compio::test]
async fn a_lookup_that_meets_a_failed_read_fails_and_the_next_one_is_right() {
    let (env, db, model) = build().await;
    let mut tally = Tally::default();
    // One buffer for every lookup, the failed ones included: it keeps a scan
    // between lookups, and what a failed lookup left in it is what the next
    // one starts from.
    let mut buf = GetBuf::new();

    for probe in PROBES {
        for how in COMPARISONS {
            let before = env.sim().ops();
            looks_up(&db, &mut buf, &model, probe, how, "nothing failing").await;
            let total = env.sim().ops() - before;

            for n in 1..=total {
                let what = format!("{how:?} {probe}, failing read {n} of {total}");
                env.sim().fail_at(n);
                let result = db.get_into(probe.as_bytes(), how, &mut buf).await;
                env.sim().stop_failing();
                match result {
                    Ok(_) => panic!("{what}: answered as though nothing had failed"),
                    Err(e) => tally.saw("lookup", &e),
                }
                for again in COMPARISONS {
                    looks_up(&db, &mut buf, &model, probe, again, &what).await;
                }
            }
        }
    }

    tally.require(&["lookup open", "lookup read"]);
    holds(&db, &model, "after every lookup").await;
}

/// One call on a scan.
#[derive(Clone, Copy, Debug)]
enum Move {
    First,
    Last,
    SeekGe(usize),
    SeekLt(usize),
    Next,
    Prev,
}

impl Move {
    /// Whether it positions the scan from scratch, rather than stepping from
    /// where it is.
    fn positions(self) -> bool {
        !matches!(self, Move::Next | Move::Prev)
    }
}

async fn make(scan: &mut Scan<SimFs>, mv: Move) -> roble::Result<()> {
    match mv {
        Move::First => scan.first().await,
        Move::Last => scan.last().await,
        Move::SeekGe(i) => scan.seek_ge(&key(i)).await,
        Move::SeekLt(i) => scan.seek_lt(&key(i)).await,
        Move::Next => scan.next().await,
        Move::Prev => scan.prev().await,
    }
}

/// A scan's bounds, and what it does.
struct Walk {
    lower: Option<usize>,
    upper: Option<usize>,
    moves: Vec<Move>,
}

impl Walk {
    fn scan(&self, db: &Db) -> Scan<SimFs> {
        let (lower, upper) = (self.lower.map(key), self.upper.map(key));
        match (&lower, &upper) {
            (None, None) => db.range(..),
            (Some(l), None) => db.range(l.as_slice()..),
            (None, Some(u)) => db.range(..u.as_slice()),
            (Some(l), Some(u)) => db.range(l.as_slice()..u.as_slice()),
        }
        .expect("range")
    }

    /// Where the model says `mv` leaves a scan that was on `at`, after a
    /// last move that went back if `back` says so.
    fn expect(&self, model: &Model, at: Option<&[u8]>, back: bool, mv: Move) -> Option<Vec<u8>> {
        let mut keys = model.keys().filter(|k| {
            self.lower.is_none_or(|l| **k >= key(l)) && self.upper.is_none_or(|u| **k < key(u))
        });
        match (mv, at) {
            (Move::First, _) => keys.next(),
            (Move::Last, _) => keys.next_back(),
            (Move::SeekGe(i), _) => keys.find(|k| **k >= key(i)),
            (Move::SeekLt(i), _) => keys.rev().find(|k| **k < key(i)),
            (Move::Next, Some(p)) => keys.find(|k| k.as_slice() > p),
            (Move::Prev, Some(p)) => keys.rev().find(|k| k.as_slice() < p),
            // From no key a step against the way the scan went turns round
            // onto the end it ran off, and one the same way stays off.
            (Move::Next, None) if back => keys.next(),
            (Move::Prev, None) if !back => keys.next_back(),
            (Move::Next | Move::Prev, None) => None,
        }
        .cloned()
    }
}

/// Walks that start everywhere a scan can start, and run off both ends.
fn walks() -> Vec<Walk> {
    let walk = |lower, upper, start, step, steps| Walk {
        lower,
        upper,
        moves: std::iter::once(start)
            .chain(std::iter::repeat_n(step, steps))
            .collect(),
    };
    vec![
        walk(None, None, Move::First, Move::Next, 120),
        walk(None, None, Move::Last, Move::Prev, 120),
        walk(Some(20), Some(100), Move::First, Move::Next, 70),
        walk(Some(20), Some(100), Move::Last, Move::Prev, 70),
        // Ending inside the span L0 deleted, so that finding the last key
        // reads on past the seek: it skips back over the deletion.
        walk(Some(100), Some(115), Move::Last, Move::Prev, 20),
        // Turning round re-seeks every source the other way, and a seek
        // lands inside the deleted span, inside an L2 table, and past the
        // end of one.
        Walk {
            lower: None,
            upper: None,
            moves: vec![
                Move::SeekGe(45),
                Move::Next,
                Move::Next,
                Move::Prev,
                Move::Prev,
                Move::Prev,
                Move::Next,
                Move::SeekLt(95),
                Move::Prev,
                Move::Prev,
                Move::Next,
                Move::Next,
                Move::Next,
                Move::SeekGe(85),
                Move::Prev,
                Move::Next,
                Move::SeekLt(40),
                Move::Next,
                Move::Prev,
            ],
        },
        // Turning round from no key at both ends, which seeks every source
        // afresh as `first` and `last` do: from past every key, from off the
        // end, from below every key, and from off the start.
        Walk {
            lower: Some(20),
            upper: Some(100),
            moves: vec![
                Move::SeekGe(100),
                Move::Prev,
                Move::Prev,
                Move::Next,
                Move::Next,
                Move::Prev,
                Move::SeekLt(20),
                Move::Next,
                Move::Prev,
                Move::Next,
            ],
        },
    ]
}

/// That a scan is where the model says, holding the model's value.
fn on(scan: &Scan<SimFs>, model: &Model, at: Option<&[u8]>, what: &str) {
    let got = scan.is_valid().then(|| scan.key().to_vec());
    assert!(
        got.as_deref() == at,
        "{what}: the scan is on {:?}, where the store holds {:?}",
        got.as_deref().map(show),
        at.map(show)
    );
    if let Some(at) = at {
        assert!(
            scan.value() == model[at].as_slice(),
            "{what}: the scan is on {} with another value",
            show(at)
        );
    }
}

/// Makes `walk`'s moves on `scan`, checking every one that succeeds, up to
/// the first that fails. Says which that was.
async fn walk_on(
    scan: &mut Scan<SimFs>,
    model: &Model,
    walk: &Walk,
    what: &str,
) -> Option<(usize, roble::Error)> {
    let (mut at, mut back) = (None, false);
    for (i, &mv) in walk.moves.iter().enumerate() {
        if let Err(e) = make(scan, mv).await {
            return Some((i, e));
        }
        at = walk.expect(model, at.as_deref(), back, mv);
        back = matches!(mv, Move::Last | Move::SeekLt(_) | Move::Prev);
        on(
            scan,
            model,
            at.as_deref(),
            &format!("{what}, move {i} ({mv:?})"),
        );
    }
    None
}

#[compio::test]
async fn a_scan_that_meets_a_failed_read_fails_and_reads_right_once_positioned_again() {
    let (env, db, model) = build().await;
    let mut tally = Tally::default();

    for (w, walk) in walks().iter().enumerate() {
        let before = env.sim().ops();
        let mut scan = walk.scan(&db);
        let failed = walk_on(&mut scan, &model, walk, &format!("walk {w}")).await;
        assert!(failed.is_none(), "walk {w} failed with nothing failing");
        let total = env.sim().ops() - before;

        for n in 1..=total {
            let what = format!("walk {w}, failing read {n} of {total}");
            let mut scan = walk.scan(&db);
            env.sim().fail_at(n);
            let failed = walk_on(&mut scan, &model, walk, &what).await;
            env.sim().stop_failing();
            let Some((i, e)) = failed else {
                panic!("{what}: the walk went on as though nothing had failed")
            };
            let mv = walk.moves[i];
            tally.saw(if mv.positions() { "seek" } else { "step" }, &e);
            let what = format!("{what}, failed at move {i} ({mv:?})");

            // Positioned nowhere, and saying so: a scan that met an error
            // cannot vouch for where it is, so it is on no key, and a step
            // from there refuses rather than ending as though the range
            // had run out, or turning round as though it had.
            assert!(
                !scan.is_valid(),
                "{what}: the scan still says it is on {}",
                show(scan.key())
            );
            for again in [Move::Next, Move::Prev] {
                assert!(
                    make(&mut scan, again).await.is_err(),
                    "{what}: a {again:?} afterwards carried on, and left the scan {}",
                    if scan.is_valid() {
                        format!("on {}", show(scan.key()))
                    } else {
                        "on nothing, as though the range had run out".to_string()
                    }
                );
                assert!(
                    !scan.is_valid(),
                    "{what}: a refused {again:?} left the scan on a key"
                );
            }

            // Positioned again, by each call that can do it in turn, it reads
            // right to the end of its range, and off it, where a step on the
            // same way does nothing.
            let (start, step) = match n % 4 {
                0 => (Move::First, Move::Next),
                1 => (Move::Last, Move::Prev),
                2 => (Move::SeekGe(45), Move::Next),
                _ => (Move::SeekLt(95), Move::Prev),
            };
            let again = Walk {
                lower: walk.lower,
                upper: walk.upper,
                moves: std::iter::once(start)
                    .chain(std::iter::repeat_n(step, 121))
                    .collect(),
            };
            let failed = walk_on(
                &mut scan,
                &model,
                &again,
                &format!("{what}, then {start:?}"),
            )
            .await;
            if let Some((i, e)) = failed {
                panic!("{what}, then {start:?}: move {i} failed: {e}");
            }
        }
    }

    tally.require(&["seek open", "seek read", "step open", "step read"]);
    holds(&db, &model, "after every scan").await;
}

fn describe<T>(result: &roble::Result<T>) -> String {
    match result {
        Ok(_) => "succeeded".to_string(),
        Err(e) => format!("failed: {e}"),
    }
}

#[compio::test]
async fn a_compaction_that_meets_a_failed_read_leaves_the_store_as_it_was() {
    let mut tally = Tally::default();
    for (level, name) in [(0u32, "L0 into L1"), (1, "L1 into L2")] {
        let total = {
            let (env, db, _) = build().await;
            let tables = tables_at(&db, level);
            let before = env.sim().ops();
            db.compact(level, &tables).await.expect("compact");
            env.sim().ops() - before
        };

        // Every operation, the writes included: a compaction that fails has
        // to leave the store as it was however it failed. Each failure ends
        // two ways, carried on from and reopened, or crashed.
        for n in 1..=total {
            for crash in [false, true] {
                let (env, db, model) = build().await;
                let tables = tables_at(&db, level);
                env.sim().fail_at(n);
                let result = db.compact(level, &tables).await;
                env.sim().stop_failing();
                let what = format!(
                    "compacting {name}, failing operation {n} of {total}, which {}",
                    describe(&result)
                );

                if crash {
                    drop(db);
                    env.sim().crash();
                    let what = format!("{what}, crashed and reopened");
                    let db = reopen(&env, &what).await;
                    holds(&db, &model, &what).await;
                    continue;
                }

                holds(&db, &model, &what).await;
                if let Err(e) = &result {
                    tally.saw("compaction", e);
                    // Its tables are still the version's, so the same
                    // compaction can run again, unless what failed was the
                    // MANIFEST, after which the store records no more
                    // (`crate::flush`) until it is reopened.
                    match db.compact(level, &tables).await {
                        Ok(_) => holds(&db, &model, &format!("{what}, compacted again")).await,
                        Err(again) => assert!(
                            again.to_string().contains("records no more"),
                            "{what}: compacting again failed: {again}"
                        ),
                    }
                }
                drop(db);
                let what = format!("{what}, reopened");
                let db = reopen(&env, &what).await;
                holds(&db, &model, &what).await;
            }
        }
    }
    tally.require(&["compaction open", "compaction read"]);
}

/// A step back through one key's versions, a block each, fails where it
/// cannot read the newest: it does not answer the version below it.
///
/// Pebble's `testdata/iter_histories/errors` at line 107, which a fix of
/// Pebble's came with: going back from `d`, a failed read of the block
/// holding `c#13` left its iterator answering `c#12`. Only a store holding
/// snapshots keeps a key's older versions, so roble never writes such a
/// table, but it reads the ones Pebble writes: `c` at 13 down to 8, then `d`
/// and `e`, an entry to a block. The table here is written by roble's own
/// writer, in the layout Pebble's would have.
///
/// roble's backward step walks every version of a key before it answers, so
/// each read the walks make is failed in turn, and each answer before the
/// failure has to be right; so does each lookup that answers with `c`.
#[compio::test]
async fn a_step_back_through_a_keys_versions_that_cannot_read_the_newest_fails() {
    // Pebble's `c`, `d` and `e`, spelled as this file spells keys.
    let (c, d, e) = (key(2), key(3), key(4));
    let layout: Vec<String> = std::iter::once("L1".to_string())
        .chain((8..=13).rev().map(|s| format!("{}.SET.{s}:c{s}", show(&c))))
        .chain([
            format!("{}.SET.9:d9", show(&d)),
            format!("{}.SET.9:e9", show(&e)),
        ])
        .collect();
    let groups = lsm::define(&layout).expect("a layout");
    let (env, _) = lsm::build(&groups, u64::MAX, WriterOptions::default().block_size(1)).await;
    let db = reopen(&env, "the layout").await;
    let model = Model::from([
        (c, b"c13".to_vec()),
        (d, b"d9".to_vec()),
        (e, b"e9".to_vec()),
    ]);
    holds(&db, &model, "with nothing failing").await;

    // Pebble's walk first, then a turn after it, and a seek that lands on `c`
    // from above.
    let walks = [
        vec![Move::Last, Move::Prev, Move::Prev],
        vec![Move::Last, Move::Prev, Move::Prev, Move::Next],
        vec![Move::SeekLt(3), Move::Prev],
    ];
    // The failed reads that stopped Pebble's step back onto `c`. It reads a
    // block for each version above `c#8`, whose block the step onto `d` read
    // already to see past `d`: five, and fewer would mean the versions did
    // not each get a block.
    let mut onto_c = BTreeSet::new();
    for (w, moves) in walks.into_iter().enumerate() {
        let walk = Walk {
            lower: None,
            upper: None,
            moves,
        };
        let before = env.sim().ops();
        let mut scan = walk.scan(&db);
        let failed = walk_on(&mut scan, &model, &walk, &format!("walk {w}")).await;
        assert!(failed.is_none(), "walk {w} failed with nothing failing");
        let total = env.sim().ops() - before;

        for n in 1..=total {
            let what = format!("walk {w}, failing read {n} of {total}");
            let mut scan = walk.scan(&db);
            env.sim().fail_at(n);
            let failed = walk_on(&mut scan, &model, &walk, &what).await;
            env.sim().stop_failing();
            let Some((i, _)) = failed else {
                panic!("{what}: the walk went on as though nothing had failed")
            };
            if w == 0 && i == 2 {
                onto_c.insert(n);
            }
        }
    }
    assert!(
        onto_c.len() >= 5,
        "the step back onto c failed at {} reads, {onto_c:?}, not one a version",
        onto_c.len()
    );

    let mut buf = GetBuf::new();
    for (probe, how) in [
        ("k002", Comparison::Equal),
        ("k002~", Comparison::Floor),
        ("k003", Comparison::Lower),
    ] {
        let before = env.sim().ops();
        looks_up(&db, &mut buf, &model, probe, how, "nothing failing").await;
        let total = env.sim().ops() - before;
        for n in 1..=total {
            let what = format!("{how:?} {probe}, failing read {n} of {total}");
            env.sim().fail_at(n);
            let result = db.get_into(probe.as_bytes(), how, &mut buf).await;
            env.sim().stop_failing();
            assert!(
                result.is_err(),
                "{what}: answered as though nothing had failed"
            );
            looks_up(&db, &mut buf, &model, probe, how, &what).await;
        }
    }
}
