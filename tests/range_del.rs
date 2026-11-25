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

//! Range deletions over a hand-built LSM, judged against Pebble's own cases.
//!
//! `tests/pebble/range_del` is Pebble's `testdata/range_del` at the pin,
//! unchanged (`tests/pebble/README.md`). Each `define` lays out an LSM entry
//! by entry: memtables, and tables at named levels, holding sets and range
//! deletions at sequence numbers the case chooses, so that a tombstone sits
//! exactly where a reader has to step over or around it — across memtables,
//! across levels, newer and older than the keys it covers. Then `get` and
//! `iter` read it, and the case gives Pebble's answers.
//!
//! The same LSM is built for roble: the memtables as a log of one batch per
//! entry, each at its own sequence number, which the open replays; and each
//! level's tables written with the format crate's writer and named in a
//! MANIFEST at that level.
//!
//! **Every read in the file is at a sequence number**, `get seq=N`, which sees
//! only what is numbered below N. roble has no read at an arbitrary past
//! number, and never needs one: its snapshots hold the version they were made
//! on, so no table a snapshot reads holds anything newer than it. What a
//! read at N sees, though, is by definition what a read of the newest state
//! sees when everything numbered N or above was never written. So each read
//! builds the LSM without those entries — the same tables at the same levels,
//! less their newer entries — and reads that at the newest sequence number,
//! which is the read roble does. Answers are compared line for line.
//!
//! Skipped: a `define` holding a MERGE, which roble refuses (§3); and what
//! follows a `compact`, which is Pebble's compaction rewriting the LSM.
//!
//! Instead, every state Pebble prints after a `compact` is written out by
//! hand and given the reads Pebble made of it (`LAYOUTS`), beside layouts from
//! RocksDB's range-deletion tests and Pebble's iterator tests. Each layout is
//! also read every way there is, against what its own entries leave visible
//! (`walk`).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::path::Path;

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Options};
use roble_format::InternalKey;
use roble_format::key::{Kind, SEQ_NUM_MAX};
use roble_format::sst::WriterOptions;

mod datadriven;
use datadriven::cases;
mod lsm;
use lsm::{Entry, Place, build, define};

/// Whether every layer of the LSM holds only sequence numbers above those of
/// every layer older than it: later memtables over earlier ones, memtables
/// over tables, and each level over the ones below.
///
/// Pebble's merging iterator relies on this, letting a range deletion delete
/// every older layer's keys without comparing sequence numbers. A `define`
/// can break it, putting a tombstone numbered 13 in a newer memtable than a
/// key numbered 14, and Pebble's answer then follows the layers and not the
/// numbers. No store writes that: a newer memtable or level holds what was
/// written later. roble replays the memtables as one, ordered by sequence
/// number alone, so such a `define` is not one it can be built as.
fn layered(groups: &[(Place, Vec<Entry>)]) -> bool {
    // Newer first: the last memtable, then the others, then L0, L1, ...
    let rank = |i: usize, p: &Place| match p {
        Place::Mem => (0, usize::MAX - i),
        Place::Level(l) => (1 + *l as usize, 0),
    };
    let span = |es: &[Entry]| {
        let seqs = es.iter().map(|e| e.seq);
        (seqs.clone().min(), seqs.max())
    };
    for (i, (p, a)) in groups.iter().enumerate() {
        for (j, (q, b)) in groups.iter().enumerate() {
            if rank(i, p) < rank(j, q)
                && let ((Some(newer), _), (_, Some(older))) = (span(a), span(b))
                && newer <= older
            {
                return false;
            }
        }
    }
    true
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// What `get` answers, spelled as Pebble's test spells it.
async fn get(db: &roble::Db<SimFs>, keys: &[String]) -> String {
    let mut out = Vec::new();
    for k in keys {
        let k = k.trim();
        out.push(
            match db.get(k.as_bytes(), Comparison::Equal).await.expect("get") {
                Some(f) => format!("{k}:{}", text(&f.value)),
                None => format!("{k}: pebble: not found"),
            },
        );
    }
    out.join("\n")
}

/// What an iterator's steps answer, spelled as Pebble's `runIterCmd` spells
/// them: `key: (value, .)` on a key, the `.` being the range key roble does
/// not have, and `.` off the end.
///
/// Two steps Pebble's file does not use, for the layouts written out by hand
/// below: RocksDB's `seek-for-prev`, to the last key at or before its key;
/// and Pebble's `set-bounds`, which roble has as a new scan over the bounds,
/// unpositioned as Pebble's iterator is left.
async fn iter(db: &roble::Db<SimFs>, steps: &[String]) -> String {
    let mut scan = db.range(..).expect("scan");
    let mut out = Vec::new();
    for step in steps {
        let words: Vec<&str> = step.split_whitespace().collect();
        let result = match words.as_slice() {
            ["first"] => scan.first().await,
            ["last"] => scan.last().await,
            ["next"] => scan.next().await,
            ["prev"] => scan.prev().await,
            ["seek-ge", k] => scan.seek_ge(k.as_bytes()).await,
            ["seek-lt", k] => scan.seek_lt(k.as_bytes()).await,
            ["seek-for-prev", k] => scan.seek_lt(&[k.as_bytes(), b"\0"].concat()).await,
            ["set-bounds", bounds @ ..] => {
                let edge = |name: &str| {
                    bounds
                        .iter()
                        .find_map(|b| b.strip_prefix(name))
                        .map(str::as_bytes)
                };
                scan = db
                    .range((
                        edge("lower=").map_or(Bound::Unbounded, Bound::Included),
                        edge("upper=").map_or(Bound::Unbounded, Bound::Excluded),
                    ))
                    .expect("scan");
                out.push(".".to_string());
                continue;
            }
            other => panic!("a step this runner does not know: {other:?}"),
        };
        result.expect("step");
        out.push(if scan.is_valid() {
            format!("{}: ({}, .)", text(scan.key()), text(scan.value()))
        } else {
            ".".to_string()
        });
    }
    out.join("\n")
}

#[compio::test]
async fn roble_reads_pebbles_range_deletion_cases_as_pebble_does() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pebble/range_del");
    let text = std::fs::read_to_string(&path).expect("Pebble's range_del test data");

    let mut lsm: Option<Vec<(Place, Vec<Entry>)>> = None;
    let mut judged = 0;
    let mut unlayered = Vec::new();
    for case in cases(&text) {
        let mut words = case.command.split_whitespace();
        let command = words.next().expect("a command");
        let seq: Option<u64> = words
            .find_map(|w| w.strip_prefix("seq="))
            .map(|s| s.parse().expect("a sequence number"));
        match command {
            "define" => {
                lsm = define(&case.input);
                if lsm.as_ref().is_some_and(|g| !layered(g)) {
                    unlayered.push(case.line);
                    lsm = None;
                }
                continue;
            }
            // Pebble rewrites the LSM, and the reads after it are of that.
            "compact" => {
                lsm = None;
                continue;
            }
            "get" | "iter" => {}
            _ => continue,
        }
        let Some(groups) = &lsm else { continue };
        let below = seq.expect("every read in this file is at a sequence number");

        let (env, dir) = build(groups, below, WriterOptions::default()).await;
        let db = roble::open_with(
            &env,
            &dir,
            Options::new()
                .disable_automatic_compactions(true)
                .background_tasks(false),
        )
        .await
        .unwrap_or_else(|e| panic!("line {}: the store did not open: {e}", case.line));
        let got = if command == "get" {
            get(&db, &case.input).await
        } else {
            iter(&db, &case.input).await
        };
        assert_eq!(
            got,
            case.expected,
            "line {}: {}\n{}",
            case.line,
            case.command,
            case.input.join("\n")
        );
        judged += 1;
    }
    // Every read outside the skipped defines, so a runner that lost some fails
    // here rather than passing on fewer.
    // Of the file's 67 reads: 26 in defines holding a MERGE, 6 after a
    // compact, and 11 in the two defines whose layers are out of order —
    // the second of which Pebble's own comment calls an LSM "not generated
    // naturally". The rest are judged, so a runner that lost some fails here.
    assert_eq!(
        unlayered,
        [459, 973],
        "the defines skipped as out of layer order"
    );
    assert_eq!(judged, 24, "the reads judged in {}", path.display());
}

/// A reverse seek that steps back out of a table holding only a range
/// deletion below the target keeps that deletion in play.
///
/// The layout `compat/differential` met in a database Pebble wrote (seed
/// 509). Pebble split a compaction's output inside a range deletion at
/// `k0403`, so in L5 the first table ends at the exclusive sentinel there and
/// the second starts there with nothing but the deletion's fragment: its
/// first point key is `k0411`. Beneath, L6 still holds `k0403` from before
/// the deletion.
///
/// A `Floor` of `k0403` seeks below `k0403\0`. In L5 that lands in the
/// second table, finds no point key below the target and waits at the first
/// table's bound — still holding the second, whose fragment covers `k0403`.
/// L6's `k0403` was taken first, and then that bound, at the same user key,
/// came up, and the merge opened the first table to see what it held: which
/// put the level on it and out of reach of the fragment, and the deleted
/// `k0403` came back. A table whose bound is the exclusive sentinel at a key
/// holds nothing there, so it waits until the key is decided.
#[compio::test]
async fn a_reverse_seek_keeps_the_deletion_of_the_table_it_stepped_back_out_of() {
    let entry = |user: &str, kind, seq, value: &str| Entry {
        user: user.as_bytes().to_vec(),
        kind,
        seq,
        value: value.as_bytes().to_vec(),
    };
    let groups = vec![
        (
            Place::Level(5),
            vec![
                entry("k0390", Kind::Set, 300, "new"),
                entry("k0392", Kind::RangeDelete, 339, "k0403"),
            ],
        ),
        (
            Place::Level(5),
            vec![
                entry("k0403", Kind::RangeDelete, 339, "k0416"),
                entry("k0411", Kind::Set, 340, "after"),
            ],
        ),
        (
            Place::Level(6),
            vec![
                entry("k0380", Kind::Set, 85, "deep"),
                entry("k0403", Kind::Set, 85, "deleted"),
                entry("k0420", Kind::Set, 85, "deep"),
            ],
        ),
    ];
    let (env, dir) = build(&groups, u64::MAX, WriterOptions::default()).await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .background_tasks(false),
    )
    .await
    .expect("open");
    let l5 = db.versions().version.level(5).to_vec();
    assert!(
        l5.len() == 2 && l5[0].largest_is_exclusive().expect("bound"),
        "the layout should be two L5 tables meeting at an exclusive bound"
    );

    let answer = |f: Option<roble::Found>| f.map(|f| text(&f.key));
    for (probe, how) in [
        (&b"k0403"[..], Comparison::Floor),
        (b"k0403\0", Comparison::Floor),
        (b"k0404", Comparison::Lower),
        (b"k0410", Comparison::Floor),
    ] {
        assert_eq!(
            answer(db.get(probe, how).await.expect("get")),
            Some("k0390".to_string()),
            "{how:?} of {:?} should pass the deleted k0403",
            text(probe)
        );
    }
    assert_eq!(
        answer(db.get(b"k0403", Comparison::Equal).await.expect("get")),
        None
    );

    let mut scan = db.range(..).expect("scan");
    scan.seek_lt(b"k0403\0").await.expect("seek");
    assert_eq!(text(scan.key()), "k0390", "a scan's reverse seek, too");
    scan.prev().await.expect("prev");
    assert_eq!(text(scan.key()), "k0380");
    scan.seek_lt(b"k0412").await.expect("seek");
    assert_eq!(text(scan.key()), "k0411");
    scan.prev().await.expect("prev");
    assert_eq!(text(scan.key()), "k0390", "and a step back across it");

    // Every other read too, forward ones included: a seek at `k0403`, or into
    // the first table's part of the deletion, has to land on `k0411`.
    db.check_levels().await.expect("the levels are in order");
    walk(&db, &groups, "seed 509").await;
}

/// The keys `groups` leaves visible, with their values, worked out from its
/// entries alone: a key's newest version, when it is a SET and no range
/// deletion numbered above it covers it.
///
/// That is what every read of the layout answers only when its levels are in
/// order, newer above whatever older they overlap, as a store keeps them. A
/// layout written by hand can break that where no store would, and then the
/// levels decide and not the numbers, so each layout is checked with
/// `check_levels` before it is read.
fn visible(groups: &[(Place, Vec<Entry>)]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let entries: Vec<&Entry> = groups.iter().flat_map(|(_, es)| es).collect();
    let mut newest: BTreeMap<&[u8], &Entry> = BTreeMap::new();
    for &e in entries.iter().filter(|e| e.kind != Kind::RangeDelete) {
        let n = newest.entry(e.user.as_slice()).or_insert(e);
        if e.seq > n.seq {
            *n = e;
        }
    }
    newest
        .into_iter()
        .filter(|(user, e)| {
            e.kind == Kind::Set
                && !entries.iter().any(|t| {
                    t.kind == Kind::RangeDelete
                        && t.seq > e.seq
                        && t.user.as_slice() <= *user
                        && *user < t.value.as_slice()
                })
        })
        .map(|(user, e)| (user.to_vec(), e.value.clone()))
        .collect()
}

/// A key and its value, or none, as text a failure can show.
fn shown(found: Option<(&[u8], &[u8])>) -> Option<(String, String)> {
    found.map(|(k, v)| (text(k), text(v)))
}

/// A move of [`walk`]'s.
#[derive(Clone, Copy)]
enum Move<'a> {
    SeekGe(&'a [u8]),
    SeekLt(&'a [u8]),
    First,
    Last,
    Next,
    Prev,
}

impl std::fmt::Display for Move<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Move::SeekGe(k) => write!(f, "seek_ge({})", k.escape_ascii()),
            Move::SeekLt(k) => write!(f, "seek_lt({})", k.escape_ascii()),
            Move::First => write!(f, "first"),
            Move::Last => write!(f, "last"),
            Move::Next => write!(f, "next"),
            Move::Prev => write!(f, "prev"),
        }
    }
}

/// Reads `db` every way a store can be read, and checks every answer against
/// what `groups` leaves [`visible`].
///
/// The probes are every key the layout names, the key just past each, and
/// the two ends of the keyspace. Every comparison is asked of `get` at every
/// probe. Then, under every pair of bounds the probes make, a scan seeks to
/// every probe either way and to its first and last keys, and from each of
/// those steps on and turns back, or turns and steps on. No step is taken
/// from no key, as the differential takes none.
async fn walk(db: &roble::Db<SimFs>, groups: &[(Place, Vec<Entry>)], context: &str) {
    let want = visible(groups);
    let mut probes = BTreeSet::from([Vec::new(), vec![0xff]]);
    for e in groups.iter().flat_map(|(_, es)| es) {
        let named = [
            Some(&e.user),
            (e.kind == Kind::RangeDelete).then_some(&e.value),
        ];
        for k in named.into_iter().flatten() {
            probes.insert(k.clone());
            probes.insert([k.as_slice(), b"\0"].concat());
        }
    }

    let from = |lo: Bound<&[u8]>| want.range::<[u8], _>((lo, Bound::Unbounded)).next();
    let to = |hi: Bound<&[u8]>| want.range::<[u8], _>((Bound::Unbounded, hi)).next_back();
    for p in &probes {
        let p = p.as_slice();
        for how in [
            Comparison::Equal,
            Comparison::Floor,
            Comparison::Ceiling,
            Comparison::Lower,
            Comparison::Higher,
        ] {
            let expect = match how {
                Comparison::Equal => want.get_key_value(p),
                Comparison::Floor => to(Bound::Included(p)),
                Comparison::Ceiling => from(Bound::Included(p)),
                Comparison::Lower => to(Bound::Excluded(p)),
                Comparison::Higher => from(Bound::Excluded(p)),
            };
            let got = db.get(p, how).await.expect("get");
            assert_eq!(
                got.as_ref().map(|f| (text(&f.key), text(&f.value))),
                shown(expect.map(|(k, v)| (k.as_slice(), v.as_slice()))),
                "{context}: {how:?} of {}",
                p.escape_ascii()
            );
        }
    }

    let edges: Vec<Option<&[u8]>> = std::iter::once(None)
        .chain(probes.iter().map(|p| Some(p.as_slice())))
        .collect();
    let starts: Vec<Move> = probes
        .iter()
        .flat_map(|p| [Move::SeekGe(p), Move::SeekLt(p)])
        .chain([Move::First, Move::Last])
        .collect();
    for &lower in &edges {
        for &upper in &edges {
            if let (Some(l), Some(u)) = (lower, upper)
                && l >= u
            {
                continue;
            }
            let view: Vec<(&[u8], &[u8])> = want
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .filter(|(k, _)| lower.is_none_or(|l| *k >= l) && upper.is_none_or(|u| *k < u))
                .collect();
            let mut scan = db
                .range((
                    lower.map_or(Bound::Unbounded, Bound::Included),
                    upper.map_or(Bound::Unbounded, Bound::Excluded),
                ))
                .expect("scan");
            for &start in &starts {
                for then in [[Move::Next, Move::Prev], [Move::Prev, Move::Next]] {
                    let mut at: Option<usize> = None;
                    let mut made = Vec::new();
                    for m in std::iter::once(start).chain(then) {
                        at = match m {
                            Move::SeekGe(p) => {
                                scan.seek_ge(p).await.expect("seek_ge");
                                view.iter().position(|(k, _)| *k >= p)
                            }
                            Move::SeekLt(p) => {
                                scan.seek_lt(p).await.expect("seek_lt");
                                view.iter().rposition(|(k, _)| *k < p)
                            }
                            Move::First => {
                                scan.first().await.expect("first");
                                (!view.is_empty()).then_some(0)
                            }
                            Move::Last => {
                                scan.last().await.expect("last");
                                view.len().checked_sub(1)
                            }
                            Move::Next => {
                                scan.next().await.expect("next");
                                at.map(|i| i + 1).filter(|&i| i < view.len())
                            }
                            Move::Prev => {
                                scan.prev().await.expect("prev");
                                at.and_then(|i| i.checked_sub(1))
                            }
                        };
                        made.push(m.to_string());
                        assert_eq!(
                            shown(scan.is_valid().then(|| (scan.key(), scan.value()))),
                            shown(at.map(|i| view[i])),
                            "{context}: within [{}, {}): {}",
                            lower.map_or("-".to_string(), |l| l.escape_ascii().to_string()),
                            upper.map_or("-".to_string(), |u| u.escape_ascii().to_string()),
                            made.join(" ")
                        );
                        if at.is_none() {
                            break;
                        }
                    }
                }
            }
        }
    }
}

/// Every table's bounds, a line each, spelled as Pebble prints them:
/// `L1: a#11,SET-c#inf,RANGEDEL`.
fn levels(db: &roble::Db<SimFs>) -> String {
    let spell = |encoded: &[u8]| {
        let k = InternalKey::decode(encoded).expect("a bound");
        let seq = match k.seq_num {
            SEQ_NUM_MAX => "inf".to_string(),
            seq => seq.to_string(),
        };
        format!(
            "{}#{seq},{}",
            text(k.user_key),
            k.kind.name().to_uppercase()
        )
    };
    let versions = db.versions();
    let mut out = Vec::new();
    for (level, tables) in versions.version.levels() {
        for t in tables {
            out.push(format!(
                "L{level}: {}-{}",
                spell(&t.smallest),
                spell(&t.largest)
            ));
        }
    }
    out.join("\n")
}

/// The lines of `s`, trimmed, without the blank ones.
fn lines(s: &str) -> Vec<String> {
    s.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect()
}

/// A layout written out by hand, where it comes from, and what reading it has
/// to give besides what [`walk`] asks.
struct Layout {
    /// Where it comes from, which a failure names.
    source: &'static str,
    /// Its entries, spelled as a `define`'s and split on whitespace: `mem`,
    /// or `L<n>` for a table at level n, each followed by what it holds.
    entries: &'static str,
    /// Its tables' bounds, as [`levels`] spells them. They follow from the
    /// entries, so this is the check that the entries are what the source
    /// printed or described.
    bounds: &'static str,
    /// The reads its source made of it, with their answers: a command,
    /// `iter` or `get` with `seq=N` when it reads below N, as Pebble's file
    /// spells one; its input; its answer.
    reads: &'static [(&'static str, &'static str, &'static str)],
}

/// Layouts from Pebble's and RocksDB's tests, written out by hand and pinned
/// with the answers their sources give. Some of their shapes are built
/// elsewhere in this suite too (`tests/scan.rs`, `tests/compact.rs`); these
/// are the sources' own.
///
/// **Pebble's `range_del` after a `compact`**, which the runner above skips.
/// Each of the four cases at lines 990, 1031, 1071 and 1133 splits a range
/// deletion across tables by compacting it into L1 with a one-byte target,
/// then moves the parts to different levels. Every state Pebble prints after a
/// `compact` is here, its tables holding what their printed bounds say,
/// with the reads Pebble made of it. Where `compact d-e` meets the snapshot
/// at 1, the deletion's last part also holds `d#0`. That is inferred rather
/// than printed: the bounds are the same either way, and the snapshot still
/// sees `d#0`, so Pebble's compaction keeps it. No read depends on it.
///
/// **RocksDB's `db_range_del_test.cc`**, whose layouts put a range deletion
/// at or across a table's edge, alone in a table or a level, or one in each
/// kind of source, abutting. They are written as the tables roble and Pebble
/// would hold: a deletion split across tables is cut at the split, where
/// RocksDB keeps it whole in each and cuts it when reading. The sequence
/// numbers follow the order each test writes in, and RocksDB's `Key(n)` is
/// spelled `kn`.
///
/// **Pebble's iterator tests**: `iterator_next_prev`, and `merging_iter`'s
/// two cases of a turn at a range deletion's edge under bounds. Their MERGEs,
/// which roble refuses (§3), are SETs here, which changes no answer: every
/// key holds one version. The third case of `iterator_next_prev` reads at a
/// sequence number below every entry of a table, and no roble read sees part
/// of a table, so it is not here.
const LAYOUTS: &[Layout] = &[
    Layout {
        source: "Pebble's range_del, lines 1011 and 1049: the defines at 996 and 1034 after \
                 `compact a-e`",
        entries: "L1 a.SET.11:v a.RANGEDEL.10:c
                  L1 c.SET.12:v c.RANGEDEL.10:e",
        bounds: "L1: a#11,SET-c#inf,RANGEDEL
                 L1: c#12,SET-e#inf,RANGEDEL",
        reads: &[],
    },
    Layout {
        source: "Pebble's range_del, line 1017: the define at 996 after `compact d-e`",
        entries: "L1 a.SET.11:v a.RANGEDEL.10:c
                  L2 c.SET.12:v c.RANGEDEL.10:e",
        bounds: "L1: a#11,SET-c#inf,RANGEDEL
                 L2: c#12,SET-e#inf,RANGEDEL",
        reads: &[("iter seq=13", "seek-ge b\nnext", "c: (v, .)\n.")],
    },
    Layout {
        source: "Pebble's range_del, line 1055: the define at 1034 after `compact a-b`",
        entries: "L1 c.SET.12:v c.RANGEDEL.10:e
                  L2 a.SET.11:v a.RANGEDEL.10:c",
        bounds: "L1: c#12,SET-e#inf,RANGEDEL
                 L2: a#11,SET-c#inf,RANGEDEL",
        reads: &[(
            "iter seq=13",
            "seek-lt d\nprev\nprev",
            "c: (v, .)\na: (v, .)\n.",
        )],
    },
    Layout {
        source: "Pebble's range_del, line 1097: the define at 1078 after `compact a-b`",
        entries: "L1 a.SET.11:v a.RANGEDEL.10:c
                  L1 c.SET.12:v c.RANGEDEL.10:d
                  L1 d.RANGEDEL.10:e
                  L2 d.SET.0:v",
        bounds: "L1: a#11,SET-c#inf,RANGEDEL
                 L1: c#12,SET-d#inf,RANGEDEL
                 L1: d#10,RANGEDEL-e#inf,RANGEDEL
                 L2: d#0,SET-d#0,SET",
        reads: &[],
    },
    Layout {
        source: "Pebble's range_del, line 1106: the define at 1078 after `compact d-e`",
        entries: "L1 a.SET.11:v a.RANGEDEL.10:c
                  L1 c.SET.12:v c.RANGEDEL.10:d
                  L3 d.RANGEDEL.10:e d.SET.0:v",
        bounds: "L1: a#11,SET-c#inf,RANGEDEL
                 L1: c#12,SET-d#inf,RANGEDEL
                 L3: d#10,RANGEDEL-e#inf,RANGEDEL",
        reads: &[("get seq=13", "c", "c:v")],
    },
    Layout {
        source: "Pebble's range_del, line 1119: the define at 1078 after `compact a-b L1`",
        entries: "L1 c.SET.12:v c.RANGEDEL.10:d
                  L2 a.SET.11:v a.RANGEDEL.10:c
                  L3 d.RANGEDEL.10:e d.SET.0:v",
        bounds: "L1: c#12,SET-d#inf,RANGEDEL
                 L2: a#11,SET-c#inf,RANGEDEL
                 L3: d#10,RANGEDEL-e#inf,RANGEDEL",
        reads: &[("get seq=13", "c", "c:v")],
    },
    Layout {
        source: "Pebble's range_del, line 1158: the define at 1136 after `compact a-b`",
        entries: "L0 f.SET.13:v
                  L1 a.SET.11:v a.RANGEDEL.10:c
                  L1 c.SET.12:v c.RANGEDEL.10:d
                  L1 d.RANGEDEL.10:e
                  L2 d.SET.0:v",
        bounds: "L0: f#13,SET-f#13,SET
                 L1: a#11,SET-c#inf,RANGEDEL
                 L1: c#12,SET-d#inf,RANGEDEL
                 L1: d#10,RANGEDEL-e#inf,RANGEDEL
                 L2: d#0,SET-d#0,SET",
        reads: &[],
    },
    Layout {
        source: "Pebble's range_del, line 1169: the define at 1136 after `compact d-e`",
        entries: "L0 f.SET.13:v
                  L1 a.SET.11:v a.RANGEDEL.10:c
                  L1 c.SET.12:v c.RANGEDEL.10:d
                  L3 d.RANGEDEL.10:e d.SET.0:v",
        bounds: "L0: f#13,SET-f#13,SET
                 L1: a#11,SET-c#inf,RANGEDEL
                 L1: c#12,SET-d#inf,RANGEDEL
                 L3: d#10,RANGEDEL-e#inf,RANGEDEL",
        reads: &[("get seq=13", "c", "c:v")],
    },
    Layout {
        source: "Pebble's range_del, line 1184: the define at 1136 after `compact f-f L0`",
        entries: "L1 a.SET.11:v a.RANGEDEL.10:c
                  L1 c.SET.12:v c.RANGEDEL.10:d
                  L1 f.SET.13:v
                  L3 d.RANGEDEL.10:e d.SET.0:v",
        bounds: "L1: a#11,SET-c#inf,RANGEDEL
                 L1: c#12,SET-d#inf,RANGEDEL
                 L1: f#13,SET-f#13,SET
                 L3: d#10,RANGEDEL-e#inf,RANGEDEL",
        reads: &[],
    },
    Layout {
        source: "Pebble's range_del, line 1193: the define at 1136 after `compact a-f L1`",
        entries: "L2 a.SET.11:v a.RANGEDEL.10:c
                  L2 c.SET.12:v c.RANGEDEL.10:d
                  L2 f.SET.13:v
                  L3 d.RANGEDEL.10:e d.SET.0:v",
        bounds: "L2: a#11,SET-c#inf,RANGEDEL
                 L2: c#12,SET-d#inf,RANGEDEL
                 L2: f#13,SET-f#13,SET
                 L3: d#10,RANGEDEL-e#inf,RANGEDEL",
        reads: &[("get seq=13", "c", "c:v")],
    },
    Layout {
        source: "RocksDB's NonOverlappingTombstonAtBoundary: a deletion that ends the first \
                 of two tables past its last key covers L2's key under that end",
        entries: "L1 k1.SET.4:v k3.SET.5:v k4.RANGEDEL.6:k6
                  L1 k6.RANGEDEL.6:k7 k6.SET.2:v k8.SET.3:v
                  L2 k5.SET.1:v",
        bounds: "L1: k1#4,SET-k6#inf,RANGEDEL
                 L1: k6#6,RANGEDEL-k8#3,SET
                 L2: k5#1,SET-k5#1,SET",
        reads: &[(
            "iter",
            "seek-ge k3\nnext\nseek-ge k4\nseek-ge k5\nseek-ge k6",
            "k3: (v, .)\nk8: (v, .)\nk8: (v, .)\nk8: (v, .)\nk8: (v, .)",
        )],
    },
    Layout {
        source: "RocksDB's TombstoneAcrossFileBoundary: a deletion split across two tables \
                 covers L2's key under the second part",
        entries: "L1 k1.SET.4:v k2.RANGEDEL.6:k5 k3.SET.5:v
                  L1 k5.RANGEDEL.6:k6 k5.SET.2:v k7.SET.3:v
                  L2 k5.SET.1:v",
        bounds: "L1: k1#4,SET-k5#inf,RANGEDEL
                 L1: k5#6,RANGEDEL-k7#3,SET
                 L2: k5#1,SET-k5#1,SET",
        reads: &[("iter", "seek-ge k1\nnext", "k1: (v, .)\nk7: (v, .)")],
    },
    Layout {
        source: "RocksDB's TombstoneOnlyFile: a table of nothing but a deletion, after \
                 another in the same level",
        entries: "L1 k1.SET.2:v k2.SET.3:v
                  L1 k3.RANGEDEL.4:k5
                  L2 k3.SET.1:v",
        bounds: "L1: k1#2,SET-k2#3,SET
                 L1: k3#4,RANGEDEL-k5#inf,RANGEDEL
                 L2: k3#1,SET-k3#1,SET",
        reads: &[(
            "iter",
            "seek-ge k2\nnext\nseek-for-prev k4\nnext",
            "k2: (v, .)\n.\nk2: (v, .)\n.",
        )],
    },
    Layout {
        source: "RocksDB's TombstoneOnlyWithOlderVisibleKey: a level of nothing but a \
                 deletion, over keys at both its ends",
        entries: "L1 k3.RANGEDEL.4:k5
                  L2 k2.SET.1:v k4.SET.2:v k5.SET.3:v",
        bounds: "L1: k3#4,RANGEDEL-k5#inf,RANGEDEL
                 L2: k2#1,SET-k5#3,SET",
        reads: &[(
            "iter",
            "seek-ge k4\nprev\nprev
             seek-for-prev k4\nnext\nprev\nprev
             seek-ge k4\nprev\nnext\nnext
             seek-for-prev k4\nnext\nnext
             first\nnext\nnext
             last\nprev\nprev",
            "k5: (v, .)\nk2: (v, .)\n.
             k2: (v, .)\nk5: (v, .)\nk2: (v, .)\n.
             k5: (v, .)\nk2: (v, .)\nk5: (v, .)\n.
             k2: (v, .)\nk5: (v, .)\n.
             k2: (v, .)\nk5: (v, .)\n.
             k5: (v, .)\nk2: (v, .)\n.",
        )],
    },
    Layout {
        // The two above as the survey put them together: L2 holds two keys
        // under the deletion, so that the skip past it has one to pass, and
        // one at its end, which the skip must not.
        source: "RocksDB's TombstoneOnlyFile and TombstoneOnlyWithOlderVisibleKey in one: a \
                 table of nothing but a deletion over two keys and one at its end",
        entries: "L1 k1.SET.5:v k2.SET.6:v
                  L1 k3.RANGEDEL.7:k5
                  L2 k2.SET.1:v k3.SET.2:v k4.SET.3:v k5.SET.4:v",
        bounds: "L1: k1#5,SET-k2#6,SET
                 L1: k3#7,RANGEDEL-k5#inf,RANGEDEL
                 L2: k2#1,SET-k5#4,SET",
        reads: &[(
            "iter",
            "seek-ge k2\nnext\nprev\nseek-for-prev k4\nnext",
            "k2: (v, .)\nk5: (v, .)\nk2: (v, .)\nk2: (v, .)\nk5: (v, .)",
        )],
    },
    Layout {
        source: "RocksDB's TombstoneSentinelDirectionChange: a seek inside a level of nothing \
                 but a deletion, then a turn",
        entries: "L1 k7.SET.3:v
                  L2 k4.RANGEDEL.2:k6
                  L3 k4.SET.1:v",
        bounds: "L1: k7#3,SET-k7#3,SET
                 L2: k4#2,RANGEDEL-k6#inf,RANGEDEL
                 L3: k4#1,SET-k4#1,SET",
        reads: &[("iter", "seek-ge k5\nprev", "k7: (v, .)\n.")],
    },
    Layout {
        source: "RocksDB's TombstoneSentinelDirectionChange, after its put of k5",
        entries: "L1 k5.SET.4:v
                  L1 k7.SET.3:v
                  L2 k4.RANGEDEL.2:k6
                  L3 k4.SET.1:v",
        bounds: "L1: k5#4,SET-k5#4,SET
                 L1: k7#3,SET-k7#3,SET
                 L2: k4#2,RANGEDEL-k6#inf,RANGEDEL
                 L3: k4#1,SET-k4#1,SET",
        reads: &[("iter", "seek-ge k5\nprev", "k5: (v, .)\n.")],
    },
    Layout {
        // RocksDB's deletions hide nothing, so every read of its layout ends
        // at once, and what it counts is the reseeks. roble skips past a
        // deletion once it has taken a key it hides, and only levels still
        // inside it, so here a level below holds two keys under each deletion
        // and one either side, and the L0 and L1 tables each hold a key
        // written after their own deletion, which their skip must not pass.
        // The keys are one up from RocksDB's to make room for the one before,
        // and the answers are this layout's. Its immutable memtable's deletion
        // is in the memtable here, since a log replays into one.
        source: "RocksDB's IteratorReseek: abutting deletions, one in each kind of source \
                 a scan merges, over a level holding two keys under each",
        entries: "mem k1.RANGEDEL.16:k2 k2.RANGEDEL.15:k3
                  L0 k3.RANGEDEL.13:k4 k3b.SET.14:v
                  L1 k4.RANGEDEL.11:k5 k4b.SET.12:v
                  L2 k0.SET.1:v k1.SET.2:v k1a.SET.3:v k2.SET.4:v k2a.SET.5:v k3.SET.6:v
                     k3a.SET.7:v k4.SET.8:v k4a.SET.9:v k5.SET.10:v",
        bounds: "L0: k3#13,RANGEDEL-k4#inf,RANGEDEL
                 L1: k4#11,RANGEDEL-k5#inf,RANGEDEL
                 L2: k0#1,SET-k5#10,SET",
        reads: &[(
            "iter",
            "seek-ge k1\nseek-for-prev k2\nfirst\nnext\nnext\nnext\nlast\nprev\nprev\nprev",
            "k3b: (v, .)\nk0: (v, .)
             k0: (v, .)\nk3b: (v, .)\nk4b: (v, .)\nk5: (v, .)
             k5: (v, .)\nk4b: (v, .)\nk3b: (v, .)\nk0: (v, .)",
        )],
    },
    Layout {
        source: "RocksDB's ReseekDuringNextAndPrev: deletions in the memtable over L0 and L1, \
                 stepped across both ways",
        entries: "mem k0.RANGEDEL.5:k1 k2.RANGEDEL.6:k3
                  L0 k2.SET.4:v
                  L1 k1.SET.1:v k2.SET.2:v k3.SET.3:v",
        bounds: "L0: k2#4,SET-k2#4,SET
                 L1: k1#1,SET-k3#3,SET",
        reads: &[(
            "iter",
            "seek-ge k0\nnext\nprev\nnext\nnext
             seek-ge k1\nnext\nprev\nnext\nnext
             seek-for-prev k2\nnext\nprev\nnext\nnext
             seek-for-prev k1\nnext\nprev\nnext\nnext
             first\nnext\nprev\nnext\nnext
             last\nprev\nnext\nprev\nnext\nnext",
            "k1: (v, .)\nk3: (v, .)\nk1: (v, .)\nk3: (v, .)\n.
             k1: (v, .)\nk3: (v, .)\nk1: (v, .)\nk3: (v, .)\n.
             k1: (v, .)\nk3: (v, .)\nk1: (v, .)\nk3: (v, .)\n.
             k1: (v, .)\nk3: (v, .)\nk1: (v, .)\nk3: (v, .)\n.
             k1: (v, .)\nk3: (v, .)\nk1: (v, .)\nk3: (v, .)\n.
             k3: (v, .)\nk1: (v, .)\nk3: (v, .)\nk1: (v, .)\nk3: (v, .)\n.",
        )],
    },
    Layout {
        source: "RocksDB's TombstoneFromCurrentLevel: an L0 table's deletion over its own key",
        entries: "mem k0.RANGEDEL.6:k1
                  L0 k2.SET.4:v k2.RANGEDEL.5:k3
                  L1 k1.SET.1:v k2.SET.2:v k3.SET.3:v",
        bounds: "L0: k2#5,RANGEDEL-k3#inf,RANGEDEL
                 L1: k1#1,SET-k3#3,SET",
        reads: &[("iter", "seek-ge k0\nnext", "k1: (v, .)\nk3: (v, .)")],
    },
    Layout {
        source: "Pebble's iterator_next_prev, line 42: a turn back from the first key past \
                 an L0 table of nothing but a deletion",
        entries: "L0 b.RANGEDEL.11:c
                  L6 a.SET.10:1 c.SET.10:2",
        bounds: "L0: b#11,RANGEDEL-c#inf,RANGEDEL
                 L6: a#10,SET-c#10,SET",
        reads: &[("iter", "first\nprev", "a: (1, .)\n.")],
    },
    Layout {
        source: "Pebble's iterator_next_prev, line 93: a turn on from the last key past an L0 \
                 table of nothing but a deletion",
        entries: "L0 x.RANGEDEL.11:y
                  L6 t.SET.10:1 z.SET.10:2",
        bounds: "L0: x#11,RANGEDEL-y#inf,RANGEDEL
                 L6: t#10,SET-z#10,SET",
        reads: &[("iter", "last\nnext", "z: (2, .)\n.")],
    },
    Layout {
        // Pebble's case goes on to `prev` from no key, which roble's scan does
        // not turn round from; `walk` makes every turn it does make here.
        source: "Pebble's merging_iter, line 526: the upper bound held at a range deletion's \
                 edge",
        entries: "L1 kq.RANGEDEL.100:p
                  L2 b.SET.90:90 cat.SET.70:70 g.SET.80:80 o.SET.65:65
                  L3 a.SET.41:41 koujdlp.SET.37:37 ok.SET.46:46 v.SET.43:43 v.RANGEDEL.19:z",
        bounds: "L1: kq#100,RANGEDEL-p#inf,RANGEDEL
                 L2: b#90,SET-o#65,SET
                 L3: a#41,SET-z#inf,RANGEDEL",
        reads: &[("iter", "set-bounds upper=n\nseek-ge krgywquurww", ".\n.")],
    },
    Layout {
        source: "Pebble's merging_iter, line 556: the lower bound held at a range deletion's \
                 edge",
        entries: "L1 a.SET.103:103 c.RANGEDEL.101:jyk imd.SET.793:793 iwoeionch.SET.792:792
                  L2 b.SET.90:90 cat.SET.70:70 g.SET.80:80 o.SET.65:65
                  L3 all.SET.0:0 c.SET.0:0 zk.SET.722:722",
        bounds: "L1: a#103,SET-jyk#inf,RANGEDEL
                 L2: b#90,SET-o#65,SET
                 L3: all#0,SET-zk#722,SET",
        reads: &[(
            "iter",
            "set-bounds lower=cz upper=jd\nseek-lt jd\nnext",
            ".\niwoeionch: (792, .)\n.",
        )],
    },
];

/// The store a layout describes, less every entry numbered `below` or above,
/// opened as the runner above opens one.
async fn open_layout(groups: &[(Place, Vec<Entry>)], below: u64) -> (SimEnv, roble::Db<SimFs>) {
    let (env, dir) = build(groups, below, WriterOptions::default()).await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .background_tasks(false),
    )
    .await
    .expect("the layout opens");
    (env, db)
}

/// Range deletions at and across tables' edges, in the layouts of Pebble's
/// and RocksDB's tests ([`LAYOUTS`]), read as their sources read them and
/// then every other way ([`walk`]).
#[compio::test]
async fn layouts_written_out_by_hand_read_as_their_sources_and_their_entries_say() {
    for layout in LAYOUTS {
        let context = layout.source;
        let tokens: Vec<String> = layout
            .entries
            .split_whitespace()
            .map(String::from)
            .collect();
        let groups = define(&tokens).expect("a layout without MERGE");
        let (_env, db) = open_layout(&groups, u64::MAX).await;
        db.check_levels()
            .await
            .unwrap_or_else(|e| panic!("{context}: the levels are out of order: {e}"));
        assert_eq!(
            levels(&db),
            lines(layout.bounds).join("\n"),
            "{context}: the tables' bounds"
        );
        walk(&db, &groups, context).await;

        for (command, input, expected) in layout.reads {
            let below = command
                .split_whitespace()
                .find_map(|w| w.strip_prefix("seq="))
                .map_or(u64::MAX, |s| s.parse().expect("a sequence number"));
            let (_env, db) = open_layout(&groups, below).await;
            let steps = lines(input);
            let got = if command.starts_with("get") {
                get(&db, &steps).await
            } else {
                iter(&db, &steps).await
            };
            assert_eq!(
                got,
                lines(expected).join("\n"),
                "{context}: {command}\n{}",
                steps.join("\n")
            );
        }
    }
}
