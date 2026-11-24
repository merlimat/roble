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

//! What a version may and may not look like.
//!
//! The rule under test is the one Pebble states in `manifest.CheckOrdering`:
//!
//! ```go
//! if v := cmp(prev.Largest().UserKey, f.Smallest().UserKey);
//!     v > 0 || (v == 0 && !prev.Largest().IsExclusiveSentinel()) { … }
//! ```
//!
//! Two tables of one level may **touch** at a user key — the ordinary result
//! of a compaction split inside a range deletion — but only when the first
//! ends there exclusively. Reading that bound as inclusive refuses databases
//! Pebble writes; reading every touching bound as allowed accepts databases
//! Pebble would refuse. Both mistakes are here, one per test.
//!
//! The same function checks the **order** first: walking a level as it is
//! held, each table has to start after the one before it, comparing smallest
//! bounds as internal keys. A lookup binary-searches a level as it is held, so
//! a level out of order hides keys even though none of its tables overlap.
//! Where one user key is a prefix of another, that order is not the order of
//! the bytes the bounds are encoded in.
//!
//! Across levels the rule is that what lies above is newer, which
//! `Db::check_levels` checks. It is judged against the cases of Pebble's own
//! check, the states two of roble's bugs left, the databases Pebble wrote,
//! and a store's own flushes and compactions.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, VersionSet, WriteBuf};
use roble_format::InternalKey;
use roble_format::batch::Batch;
use roble_format::files::{MARKER_FORMAT_VERSION, MARKER_MANIFEST, marker_filename};
use roble_format::key::{self, Kind, SEQ_NUM_MAX};
use roble_format::manifest::{NewTable, VersionEdit};
use roble_format::rangedel::{self, Tombstone};
use roble_format::record::RecordWriter;
use roble_format::sst::{TableWriter, WriterOptions};

const BYTEWISE: &str = "leveldb.BytewiseComparator";

/// A table of `level` spanning `smallest`..`largest`, with `largest`'s
/// trailer given outright so a bound can be made exclusive or not.
fn table(
    level: u32,
    file_num: u64,
    smallest: &[u8],
    largest: &[u8],
    largest_seq: u64,
    largest_kind: Kind,
) -> NewTable {
    NewTable {
        level,
        file_num,
        size: 100,
        smallest: key::encode(smallest, 10, Kind::Set),
        largest: key::encode(largest, largest_seq, largest_kind),
        smallest_seq_num: 10,
        largest_seq_num: if largest_seq == SEQ_NUM_MAX {
            10
        } else {
            largest_seq
        },
        marked_for_compaction: false,
        creation_time: 0,
    }
}

/// Replays one edit holding `tables`, as an open would.
fn replay(tables: Vec<NewTable>) -> roble::Result<VersionSet> {
    VersionSet::replay(&[VersionEdit {
        comparer_name: Some(BYTEWISE.to_string()),
        min_unflushed_log_num: Some(0),
        obsolete_prev_log_num: None,
        next_file_num: Some(100),
        last_seq_num: Some(1000),
        deleted_tables: Vec::new(),
        new_tables: tables,
    }])
}

#[compio::test]
async fn tables_touching_at_an_exclusive_bound_are_accepted() {
    // What a compaction split inside a range deletion leaves: the first
    // table ends at the tombstone's end key at the largest sequence number
    // there is, which excludes that key, and the second begins there.
    let set = replay(vec![
        table(1, 1, b"a", b"m", SEQ_NUM_MAX, Kind::RangeDelete),
        table(1, 2, b"m", b"z", 20, Kind::Set),
    ])
    .expect("a level whose tables touch at an exclusive bound is not an overlap");
    assert_eq!(set.version.level(1).len(), 2);
    assert!(
        set.version.level(1)[0]
            .largest_is_exclusive()
            .expect("exclusive"),
        "the first table's bound should be the exclusive one"
    );
}

#[compio::test]
async fn tables_touching_at_an_inclusive_bound_are_refused() {
    // The same shape with an ordinary key at the bound: both tables would
    // hold `m`, which Pebble prohibits — a user key is never split across
    // two tables of a level.
    let err = replay(vec![
        table(1, 1, b"a", b"m", 20, Kind::Set),
        table(1, 2, b"m", b"z", 20, Kind::Set),
    ])
    .expect_err("two tables both holding `m` is an overlap");
    assert!(format!("{err}").contains("overlap"), "got: {err}");
}

#[compio::test]
async fn an_edit_a_running_store_applies_is_refused_as_a_replayed_one_is() {
    // `apply` is how a store takes the edits it makes as it runs. A comparer
    // other than the set's is refused there as on replay, and before any of
    // the edit is applied: the set is left as it was.
    let mut set = replay(vec![table(1, 1, b"a", b"m", 20, Kind::Set)]).expect("replay");
    let err = set
        .apply(&VersionEdit {
            comparer_name: Some("another.Comparator".to_string()),
            new_tables: vec![table(1, 2, b"n", b"z", 20, Kind::Set)],
            ..VersionEdit::default()
        })
        .expect_err("a second comparer");
    assert!(format!("{err}").contains("comparer changes"), "got: {err}");
    assert_eq!(set.version.level(1).len(), 1, "none of the edit applied");
}

#[compio::test]
async fn a_bound_is_exclusive_only_at_the_largest_sequence_number() {
    // A range deletion that ends at an ordinary sequence number is not a
    // sentinel: it is a key the table holds.
    let err = replay(vec![
        table(1, 1, b"a", b"m", 20, Kind::RangeDelete),
        table(1, 2, b"m", b"z", 20, Kind::Set),
    ])
    .expect_err("a range deletion below the sentinel does not exclude its key");
    assert!(format!("{err}").contains("overlap"), "got: {err}");
}

#[compio::test]
async fn a_bound_is_exclusive_only_for_a_range_deletion() {
    // The largest sequence number with any other kind is not a bound Pebble
    // writes, and roble refuses the kinds that would make it one (§3).
    let err = replay(vec![
        table(1, 1, b"a", b"m", SEQ_NUM_MAX, Kind::Set),
        table(1, 2, b"m", b"z", 20, Kind::Set),
    ])
    .expect_err("a set at the largest sequence number is not an exclusive bound");
    assert!(format!("{err}").contains("overlap"), "got: {err}");
}

#[compio::test]
async fn tables_that_truly_overlap_are_refused_either_way() {
    for (seq, kind) in [(SEQ_NUM_MAX, Kind::RangeDelete), (20, Kind::Set)] {
        let err = replay(vec![
            table(1, 1, b"a", b"n", seq, kind),
            table(1, 2, b"m", b"z", 20, Kind::Set),
        ])
        .expect_err("ranges that cross are an overlap whatever the bound's kind");
        assert!(format!("{err}").contains("overlap"), "got: {err}");
    }
}

/// A key, and the same key extended by a byte below its kind byte.
///
/// An encoded bound is the user key followed by the trailer, and the trailer's
/// first byte is the kind (a Set's is 1). Bytewise, that 1 meets the longer
/// key's 0, which puts `w0\0\0` first. As internal keys, `w0` comes first.
const PREFIX: &[u8] = b"w0";
const EXTENDED: &[u8] = b"w0\x00\x00";

#[compio::test]
async fn a_level_holds_a_key_before_its_extension_whichever_was_added_first() {
    let prefix = || table(1, 1, PREFIX, PREFIX, 10, Kind::Set);
    let extended = || table(1, 2, EXTENDED, b"w1\xff\x00", 20, Kind::Set);
    for tables in [vec![prefix(), extended()], vec![extended(), prefix()]] {
        let set = replay(tables).expect("two tables that do not overlap");
        let order: Vec<u64> = set.version.level(1).iter().map(|t| t.file_num).collect();
        assert_eq!(order, [1, 2], "`w0`'s table comes first");
    }
}

/// Writes `key` into a table of its own in L1: flushed, then compacted out
/// of L0.
async fn alone_in_l1(db: &roble::Db<SimFs>, key: &[u8]) {
    let mut batch = Batch::new();
    batch.put(key, b"v").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");
    db.compact(0, &[table]).await.expect("compact");
}

/// Checks that a store holding [`PREFIX`] and [`EXTENDED`] finds both, under
/// every comparison that should land on one of them and in a scan each way.
async fn assert_reads(db: &roble::Db<SimFs>, when: &str) {
    let between: &[u8] = b"w0\x00";
    for (probe, how, expected) in [
        (PREFIX, Comparison::Equal, PREFIX),
        (PREFIX, Comparison::Higher, EXTENDED),
        (EXTENDED, Comparison::Lower, PREFIX),
        (between, Comparison::Floor, PREFIX),
        (between, Comparison::Ceiling, EXTENDED),
    ] {
        let found = db.get(probe, how).await.expect("get").map(|f| f.key);
        assert_eq!(
            found.as_deref(),
            Some(expected),
            "{when}: {how:?} of {}",
            probe.escape_ascii()
        );
    }

    let mut scan = db.range(..).expect("scan");
    let mut forward = Vec::new();
    scan.first().await.expect("first");
    while scan.is_valid() {
        forward.push(scan.key().to_vec());
        scan.next().await.expect("next");
    }
    let mut backward = Vec::new();
    scan.last().await.expect("last");
    while scan.is_valid() {
        backward.push(scan.key().to_vec());
        scan.prev().await.expect("prev");
    }
    backward.reverse();
    assert_eq!(forward, [PREFIX, EXTENDED], "{when}: a forward scan");
    assert_eq!(backward, [PREFIX, EXTENDED], "{when}: a reverse scan");
}

#[compio::test]
async fn a_store_finds_a_key_whose_extension_starts_the_next_table_of_its_level() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let options = || {
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
    };
    let db = roble::open_with(&env, &dir, options()).await.expect("open");
    alone_in_l1(&db, PREFIX).await;
    alone_in_l1(&db, EXTENDED).await;
    assert_eq!(
        db.versions().version.level(1).len(),
        2,
        "each key in a table of its own in L1"
    );
    assert_reads(&db, "as written").await;

    // A reopen builds the level again, from the MANIFEST.
    drop(db);
    let db = roble::open_with(&env, &dir, options())
        .await
        .expect("reopen");
    assert_reads(&db, "after a reopen").await;
}

#[compio::test]
async fn l0_may_overlap_as_much_as_it_likes() {
    // L0 holds whole flushed memtables, which overlap by design.
    replay(vec![
        table(0, 1, b"a", b"z", 20, Kind::Set),
        table(0, 2, b"a", b"z", 20, Kind::Set),
    ])
    .expect("L0 overlap is not an error");
}

/// An L0 table spanning `smallest`..`largest`, written at `seq`.
///
/// L0's tables are ordered by sequence number rather than by key and are
/// allowed to overlap, which is what makes their sublevels a question.
fn in_l0(file_num: u64, smallest: &[u8], largest: &[u8], seq: u64) -> NewTable {
    NewTable {
        level: 0,
        file_num,
        size: 100,
        smallest: key::encode(smallest, seq, Kind::Set),
        largest: key::encode(largest, seq, Kind::Set),
        smallest_seq_num: seq,
        largest_seq_num: seq,
        marked_for_compaction: false,
        creation_time: 0,
    }
}

/// How many layers L0's tables form in the version `tables` replays to.
fn sublevels(tables: Vec<NewTable>) -> usize {
    replay(tables)
        .expect("the version")
        .version
        .l0_sublevels()
        .expect("the sublevels")
}

#[compio::test]
async fn an_l0_with_nothing_in_it_is_no_sublevels() {
    assert_eq!(sublevels(vec![table(1, 1, b"a", b"b", 20, Kind::Set)]), 0);
}

#[compio::test]
async fn l0_tables_that_do_not_overlap_are_one_sublevel() {
    // The shape a store writing in key order produces, and the whole reason
    // the count of tables is the wrong number: four tables here cost one
    // seek, not four.
    assert_eq!(
        sublevels(vec![
            in_l0(1, b"a0", b"a9", 10),
            in_l0(2, b"b0", b"b9", 20),
            in_l0(3, b"c0", b"c9", 30),
            in_l0(4, b"d0", b"d9", 40),
        ]),
        1
    );
}

#[compio::test]
async fn l0_tables_written_in_descending_key_order_are_still_one_sublevel() {
    // The same four tables as above, written the other way round: each one
    // starts *before* every table already placed ends, and ends before any
    // of them starts. An overlap test that only asked the first of those two
    // questions would stack all four.
    assert_eq!(
        sublevels(vec![
            in_l0(1, b"d0", b"d9", 10),
            in_l0(2, b"c0", b"c9", 20),
            in_l0(3, b"b0", b"b9", 30),
            in_l0(4, b"a0", b"a9", 40),
        ]),
        1
    );
}

#[compio::test]
async fn l0_tables_that_all_overlap_are_a_sublevel_each() {
    // The other extreme: the same key range written four times over, which
    // is four seeks and the number the thresholds are really about.
    assert_eq!(
        sublevels(vec![
            in_l0(1, b"a0", b"a9", 10),
            in_l0(2, b"a0", b"a9", 20),
            in_l0(3, b"a0", b"a9", 30),
            in_l0(4, b"a0", b"a9", 40),
        ]),
        4
    );
}

#[compio::test]
async fn a_sublevel_fills_up_sideways_before_it_stacks() {
    // Two that do not overlap each other share a layer, and the one that
    // overlaps both goes above the pair rather than above each of them.
    assert_eq!(
        sublevels(vec![
            in_l0(1, b"a0", b"a9", 10),
            in_l0(2, b"c0", b"c9", 20),
            in_l0(3, b"a5", b"c5", 30),
        ]),
        2
    );
}

#[compio::test]
async fn tables_that_only_touch_are_not_stacked() {
    // §5 lets the tables of a level share a user key at their bounds when
    // the first ends there *exclusively*, which is what a compaction split
    // inside a range deletion leaves. Reading that as an overlap would count
    // a seek that no read makes.
    let touching = vec![
        table(0, 1, b"a0", b"b0", SEQ_NUM_MAX, Kind::RangeDelete),
        in_l0(2, b"b0", b"c0", 30),
    ];
    assert_eq!(sublevels(touching), 1);

    // And the same two bounds with an ordinary trailer on the first do
    // overlap, because then it holds keys at `b0` as well.
    let overlapping = vec![
        table(0, 1, b"a0", b"b0", 10, Kind::Set),
        in_l0(2, b"b0", b"c0", 30),
    ];
    assert_eq!(sublevels(overlapping), 2);
}

/// One table, to ask about its bounds.
fn bounded(smallest: &[u8], largest: &[u8], largest_seq: u64, largest_kind: Kind) -> VersionSet {
    replay(vec![table(
        0,
        1,
        smallest,
        largest,
        largest_seq,
        largest_kind,
    )])
    .expect("the version")
}

#[compio::test]
async fn a_table_meets_a_scans_bounds_as_the_bounds_are_written() {
    // A scan's bounds are half-open as Pebble's are — lower included, upper
    // excluded — and a table's own largest bound is excluded too when it is
    // the sentinel a range deletion leaves (§5). Every assertion here is one
    // of those three exclusions, because every one of them is an off-by-one
    // that loses keys or holds files.
    let plain = bounded(b"b", b"d", 20, Kind::Set);
    let plain = &plain.version.level(0)[0];
    let sentinel = bounded(b"b", b"d", SEQ_NUM_MAX, Kind::RangeDelete);
    let sentinel = &sentinel.version.level(0)[0];

    assert!(
        plain.overlaps_range(Some(b"d"), None).expect("overlaps"),
        "the scan's lower bound is included, and this table holds a key at it"
    );
    assert!(
        !sentinel.overlaps_range(Some(b"d"), None).expect("overlaps"),
        "but a table ending at the sentinel holds nothing at that key"
    );
    assert!(
        !plain.overlaps_range(Some(b"e"), None).expect("overlaps"),
        "and a table that ends before the scan starts holds nothing it can see"
    );

    assert!(
        !plain.overlaps_range(None, Some(b"b")).expect("overlaps"),
        "the scan's upper bound is excluded, so a table starting at it is out of reach"
    );
    assert!(
        plain.overlaps_range(None, Some(b"c")).expect("overlaps"),
        "and one starting below it is not"
    );

    assert!(
        plain.overlaps_range(None, None).expect("overlaps"),
        "a scan with no bounds reaches everything"
    );
    assert!(
        plain
            .overlaps_range(Some(b"c"), Some(b"c0"))
            .expect("overlaps"),
        "and a scan inside the table's range reaches it"
    );
}

/// One command of a datadriven file: the command line, its input lines, and
/// the output written under it.
struct Case {
    line: usize,
    command: String,
    input: Vec<String>,
    expected: String,
}

/// Splits a file in the format of cockroachdb/datadriven, the one Pebble's
/// test data is written in. An output runs to the first blank line, or,
/// when it opens with a second `----`, to a closing `----` pair.
fn cases(text: &str) -> Vec<Case> {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i].trim_end();
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }
        let at = i + 1;
        let command = line.to_string();
        i += 1;
        let mut input = Vec::new();
        while i < lines.len() && lines[i] != "----" {
            input.push(lines[i].to_string());
            i += 1;
        }
        i += 1;
        let mut expected = Vec::new();
        if lines.get(i) == Some(&"----") {
            i += 1;
            while i + 1 < lines.len() && !(lines[i] == "----" && lines[i + 1] == "----") {
                expected.push(lines[i]);
                i += 1;
            }
            i += 2;
        } else {
            while i < lines.len() && !lines[i].trim().is_empty() {
                expected.push(lines[i]);
                i += 1;
            }
        }
        out.push(Case {
            line: at,
            command,
            input,
            expected: expected.join("\n"),
        });
    }
    out
}

/// An internal key as Pebble's `ParseInternalKey` writes it: `user.KIND.seq`,
/// with `inf` for the largest sequence number.
fn pebble_key(s: &str) -> (Vec<u8>, Kind, u64) {
    let mut parts = s.rsplitn(3, '.');
    let seq = parts.next().expect("a sequence number");
    let kind = parts.next().expect("a kind");
    let user = parts.next().expect("a user key");
    let kind = match kind {
        "SET" => Kind::Set,
        "DEL" => Kind::Delete,
        "RANGEDEL" => Kind::RangeDelete,
        other => panic!("a kind this test does not know: {other}"),
    };
    let seq = if seq == "inf" {
        SEQ_NUM_MAX
    } else {
        seq.parse().expect("a sequence number")
    };
    (user.as_bytes().to_vec(), kind, seq)
}

/// A table as Pebble's L0 sublevel test reads one, `000009:a.SET.10-b.SET.10`
/// and any flags after it: the sequence numbers are the bounds' own, except
/// that an exclusive end takes the start's, as there (`parseMeta`).
fn pebble_table(line: &str) -> NewTable {
    let (num, rest) = line.trim().split_once(':').expect("a table is num:bounds");
    let bounds = rest.split_whitespace().next().expect("the bounds");
    let (smallest, largest) = bounds.split_once('-').expect("smallest-largest");
    let (lo, lo_kind, lo_seq) = pebble_key(smallest);
    let (hi, hi_kind, hi_seq) = pebble_key(largest);
    let exclusive = hi_kind == Kind::RangeDelete && hi_seq == SEQ_NUM_MAX;
    NewTable {
        level: 0,
        file_num: num.parse().expect("a file number"),
        size: 256,
        smallest: key::encode(&lo, lo_seq, lo_kind),
        largest: key::encode(&hi, hi_seq, hi_kind),
        smallest_seq_num: lo_seq,
        largest_seq_num: if exclusive { lo_seq } else { hi_seq },
        marked_for_compaction: false,
        creation_time: 0,
    }
}

/// L0's sublevels, judged against Pebble's own test cases.
///
/// `tests/pebble/l0_sublevels` is Pebble's
/// `internal/manifest/testdata/l0_sublevels` at the pin, unchanged
/// (`tests/pebble/README.md`). Every `define` and `add-l0-files` prints, first,
/// how many sublevels Pebble's `newL0Sublevels` makes of L0, and the count is
/// all of it roble uses: the write stall and the picker are charged by it
/// (§9). So each is replayed as a version and `l0_sublevels` has to give the
/// same number. The levels below L0 are left out, since a sublevel is a
/// matter of L0 alone and the rest of each case is about compaction picking,
/// which roble does its own way.
///
/// Skipped: a `define no_initialize`, whose sublevels the case writes out by
/// hand rather than having Pebble compute them, and the two `add-l0-files`
/// whose output is Pebble refusing its incremental shortcut. Their files
/// still join L0, as in Pebble's driver, so the cases after them are judged
/// with them there.
#[compio::test]
async fn roble_counts_the_sublevels_pebble_counts() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pebble/l0_sublevels");
    let text = std::fs::read_to_string(&path).expect("Pebble's L0 sublevel test data");

    let mut l0: Vec<NewTable> = Vec::new();
    let mut judged = 0;
    for case in cases(&text) {
        let initialize = match case.command.split_whitespace().next() {
            Some("define") => {
                l0.clear();
                !case.command.contains("no_initialize")
            }
            Some("add-l0-files") => true,
            _ => continue,
        };
        // Only L0's tables, under `L0` or an `L0.n` heading; an
        // `add-l0-files` has no heading and is all L0.
        let mut in_l0 = case.command.starts_with("add-l0-files");
        for line in &case.input {
            let line = line.trim();
            if line.starts_with('L') && line.len() <= 4 {
                in_l0 = line.starts_with("L0");
            } else if in_l0 {
                l0.push(pebble_table(line));
            }
        }
        if !initialize || case.expected.starts_with("pebble: ") {
            continue;
        }

        let want: usize = case
            .expected
            .split("sublevels: ")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                panic!(
                    "line {}: no sublevel count in {:?}",
                    case.line, case.expected
                )
            });
        let got = VersionSet::replay(&[VersionEdit {
            comparer_name: Some(BYTEWISE.to_string()),
            min_unflushed_log_num: Some(0),
            obsolete_prev_log_num: None,
            next_file_num: Some(l0.iter().map(|t| t.file_num).max().unwrap_or(0) + 1),
            last_seq_num: Some(
                l0.iter()
                    .map(|t| t.smallest_seq_num.max(t.largest_seq_num))
                    .max()
                    .unwrap_or(0),
            ),
            deleted_tables: Vec::new(),
            new_tables: l0.clone(),
        }])
        .unwrap_or_else(|e| panic!("line {}: the version: {e}", case.line))
        .version
        .l0_sublevels()
        .expect("the sublevels");
        assert_eq!(
            got,
            want,
            "line {}: {} over\n{}",
            case.line,
            case.command,
            case.input.join("\n")
        );
        judged += 1;
    }
    // Every case but the skipped ones, so a parser that lost some fails here.
    assert_eq!(judged, 37, "the cases of {}", path.display());
}

/// A file in the simulated filesystem, written and synced.
async fn put_file(fs: &SimFs, path: &Path, bytes: Vec<u8>) {
    let disk = fs.create(path).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
    result.expect("write");
    disk.sync(()).await.expect("sync");
    disk.close().await.expect("close");
}

/// One entry of a store laid out by hand: a point key, `user.KIND.seq:value`,
/// or a range deletion, `Span:start-end:{(#seq,RANGEDEL)}`.
enum Laid {
    Point(Vec<u8>, Kind, u64, Vec<u8>),
    Span(Tombstone),
}

fn laid(token: &str) -> Laid {
    if let Some(span) = token.strip_prefix("Span:") {
        let (range, keys) = span.split_once(':').expect("start-end:keys");
        let (start, end) = range.split_once('-').expect("start-end");
        let seq = keys
            .strip_prefix("{(#")
            .and_then(|s| s.strip_suffix(",RANGEDEL)}"))
            .unwrap_or_else(|| panic!("one range deletion, not {keys}"));
        return Laid::Span(Tombstone {
            start: start.as_bytes().to_vec(),
            end: end.as_bytes().to_vec(),
            seq_num: seq.parse().expect("a sequence number"),
        });
    }
    let (k, value) = token.split_once(':').expect("key:value");
    let (user, kind, seq) = pebble_key(k);
    Laid::Point(user, kind, seq, value.as_bytes().to_vec())
}

/// The store `input` lays out, as Pebble's `testdata/level_checker` lays one
/// out: `L` starts the next level, from L1, and each table is two lines, its
/// bounds as written and then its entries in order. roble's own cases add
/// `L<n>`, a level by number, and `mem`, entries a log holds for the open to
/// replay, each in a batch of its own at its own sequence number.
async fn lay_out(input: &[String]) -> (SimEnv, PathBuf) {
    const LOG: u64 = 2;
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let fs: &SimFs = env.sim();
    fs.mkdir_all(&dir).await.expect("mkdir");

    let mut log = RecordWriter::recyclable(LOG as u32);
    let mut tables = Vec::new();
    let mut next = LOG + 1;
    let mut last_seq = 0;
    // `None` while the lines are a memtable's.
    let mut level: Option<u32> = None;
    let mut lines = input.iter().map(|l| l.trim());
    while let Some(line) = lines.next() {
        if line == "mem" {
            level = None;
            continue;
        }
        if let Some(n) = line.strip_prefix('L')
            && (n.is_empty() || n.parse::<u32>().is_ok())
        {
            level = Some(n.parse().unwrap_or_else(|_| level.map_or(1, |l| l + 1)));
            continue;
        }
        let Some(level) = level else {
            for token in line.split_whitespace() {
                let mut batch = Batch::new();
                let seq = match laid(token) {
                    Laid::Point(user, Kind::Set, seq, value) => {
                        batch.put(&user, &value).expect("put");
                        seq
                    }
                    Laid::Span(t) => {
                        batch.delete_range(&t.start, &t.end).expect("delete_range");
                        t.seq_num
                    }
                    Laid::Point(_, kind, ..) => panic!("{kind:?} in a memtable"),
                };
                batch.set_seq_num(seq);
                log.write_record(batch.as_bytes()).expect("frame");
                last_seq = last_seq.max(seq);
            }
            continue;
        };

        let mut bounds = line.split_whitespace().map(pebble_key);
        let (lo, lo_kind, lo_seq) = bounds.next().expect("the smallest bound");
        let (hi, hi_kind, hi_seq) = bounds.next().expect("the largest bound");
        let mut writer = TableWriter::new(WriterOptions::default()).expect("a writer");
        let mut spans = Vec::new();
        let mut seqs = Vec::new();
        for token in lines.next().expect("a table's entries").split_whitespace() {
            match laid(token) {
                Laid::Point(user, kind, seq, value) => {
                    let k = InternalKey {
                        user_key: &user,
                        seq_num: seq,
                        kind,
                        obsolete: false,
                    };
                    writer.add(k, &value).expect("add");
                    seqs.push(seq);
                }
                Laid::Span(t) => {
                    seqs.push(t.seq_num);
                    spans.push(t);
                }
            }
        }
        // Fragmented, as Pebble's driver writes them.
        for t in rangedel::fragment(&spans.iter().collect::<Vec<_>>()) {
            writer
                .add_range_deletion(&t.start, &t.end, t.seq_num)
                .expect("add a range deletion");
        }
        let bytes = writer.finish().expect("finish");
        let largest_seq = *seqs.iter().max().expect("an entry");
        last_seq = last_seq.max(largest_seq);
        tables.push(NewTable {
            level,
            file_num: next,
            size: bytes.len() as u64,
            smallest: key::encode(&lo, lo_seq, lo_kind),
            largest: key::encode(&hi, hi_seq, hi_kind),
            smallest_seq_num: *seqs.iter().min().expect("an entry"),
            largest_seq_num: largest_seq,
            marked_for_compaction: false,
            creation_time: 0,
        });
        put_file(fs, &dir.join(format!("{next:06}.sst")), bytes).await;
        next += 1;
    }
    put_file(fs, &dir.join(format!("{LOG:06}.log")), log.finish()).await;

    let edit = VersionEdit {
        comparer_name: Some(BYTEWISE.to_string()),
        min_unflushed_log_num: Some(LOG),
        next_file_num: Some(next),
        last_seq_num: Some(last_seq),
        new_tables: tables,
        ..Default::default()
    };
    let mut manifest = RecordWriter::legacy();
    manifest
        .write_record(&edit.encode())
        .expect("frame the edit");
    put_file(fs, &dir.join("MANIFEST-000001"), manifest.finish()).await;
    for marker in [
        marker_filename(MARKER_FORMAT_VERSION, 1, "016"),
        marker_filename(MARKER_MANIFEST, 1, "MANIFEST-000001"),
    ] {
        put_file(fs, &dir.join(marker), Vec::new()).await;
    }
    fs.sync_dir(&dir).await.expect("sync dir");
    (env, dir)
}

/// What `Db::check_levels` finds wrong with the store `input` lays out, if
/// anything. Opened read-only, so that the open writes nothing over it.
async fn level_fault(input: &[String]) -> Option<String> {
    let (env, dir) = lay_out(input).await;
    let db = roble::open_read_only(&env, &dir, Options::new())
        .await
        .expect("the layout opens");
    db.check_levels().await.err().map(|e| e.to_string())
}

/// The level invariant, judged against the cases of Pebble's own check.
///
/// `tests/pebble/level_checker` is Pebble's `testdata/level_checker` at the
/// pin, unchanged (`tests/pebble/README.md`). Each `define` lays out tables at
/// L1, L2 and so on, and each `check` gives what Pebble's `CheckLevels` found
/// there: nothing, or its first fault, which names the keys and range
/// deletions at fault, each with its level. roble has to find a fault where
/// Pebble does, and the same one: each of those, at the same level.
///
/// Skipped, with the `check` after each: a `define` that turns off the
/// writer's order checks or writes range deletions unfragmented, whose faults
/// are in a table's own order, which is the table reader's to refuse; one
/// holding a MERGE, which roble refuses (§3); and one whose keys are ordered
/// by Pebble's test comparer, under which `a@9` sorts before `a@6`.
#[compio::test]
async fn roble_finds_the_level_faults_pebble_finds() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pebble/level_checker");
    let text = std::fs::read_to_string(&path).expect("Pebble's level checker test data");

    let mut layout: Option<Vec<String>> = None;
    let mut judged = 0;
    for case in cases(&text) {
        if case.command.starts_with("define") {
            let plain = case.command == "define"
                && !case
                    .input
                    .iter()
                    .any(|l| l.contains(".MERGE.") || l.contains('@'));
            layout = plain.then(|| case.input.clone());
            continue;
        }
        assert!(
            case.command.starts_with("check"),
            "line {}: {}",
            case.line,
            case.command
        );
        let Some(input) = &layout else { continue };
        let fault = level_fault(input).await;
        let context = format!("line {}: over\n{}", case.line, input.join("\n"));
        if case.expected.is_empty() {
            assert_eq!(fault, None, "{context}");
        } else {
            let fault = fault.unwrap_or_else(|| {
                panic!(
                    "{context}\nno fault, where Pebble's has {:?}",
                    case.expected
                )
            });
            // Each one Pebble names, with the level it names: `c#27,SET in L1:`.
            let words: Vec<&str> = case.expected.split_whitespace().collect();
            let mut named = 0;
            for (i, word) in words.iter().enumerate() {
                if word.contains('#') && words.get(i + 1) == Some(&"in") {
                    let at = format!("{word} in {} table", words[i + 2].trim_end_matches(':'));
                    assert!(
                        fault.contains(&at),
                        "{context}\nroble's fault {fault:?} has no {at:?}, as Pebble's {:?} has",
                        case.expected
                    );
                    named += 1;
                }
            }
            assert!(named > 0, "{context}\nnothing named in {:?}", case.expected);
        }
        judged += 1;
    }
    // Every check but the skipped ones, so a runner that lost some fails here.
    assert_eq!(judged, 10, "the checks of {}", path.display());
}

/// The states two of roble's bugs left, and the rule Pebble's check leaves
/// out, laid out the same way.
#[compio::test]
async fn roble_finds_the_level_faults_its_own_bugs_left() {
    let cases: &[(&[&str], Option<&str>)] = &[
        // merlimat/roble#226: a log the open replayed beneath tables already
        // holding its records, which a flush with the WAL off left unflushed.
        // Its put is above itself, in the table the first replay was flushed
        // into,
        (
            &["mem", "a.SET.5:v", "L6", "a.SET.5 a.SET.5", "a.SET.5:v"],
            Some(
                "corrupt: a#5,SET in a memtable is above a#5,SET in L6 table 3, which is \
                 numbered the same",
            ),
        ),
        // above a newer version of its key,
        (
            &[
                "mem",
                "a.SET.5:old",
                "L6",
                "a.SET.10 a.SET.10",
                "a.SET.10:new",
            ],
            Some("corrupt: a#5,SET in a memtable is above a#10,SET in L6 table 3, which is newer"),
        ),
        // above a newer range deletion over it,
        (
            &[
                "mem",
                "b.SET.5:old",
                "L6",
                "a.RANGEDEL.10 c.RANGEDEL.inf",
                "Span:a-c:{(#10,RANGEDEL)}",
            ],
            Some(
                "corrupt: b#5,SET in a memtable is above a-c:{(#10,RANGEDEL)} in L6 table 3, \
                 which is newer",
            ),
        ),
        // and its range deletion above a newer key it covers, which only the
        // rule Pebble's check leaves out finds.
        (
            &[
                "mem",
                "Span:a-c:{(#5,RANGEDEL)}",
                "L6",
                "b.SET.10 b.SET.10",
                "b.SET.10:new",
            ],
            Some(
                "corrupt: a-c:{(#5,RANGEDEL)} in a memtable is above b#10,SET in L6 table 3, \
                 which is newer",
            ),
        ),
        // merlimat/roble#234: a compaction of part of L0 left an older table
        // above the deletion it moved into L1.
        (
            &[
                "L0",
                "a.SET.5 a.SET.5",
                "a.SET.5:old",
                "L1",
                "a.DEL.10 a.DEL.10",
                "a.DEL.10:",
            ],
            Some("corrupt: a#5,SET in L0 table 3 is above a#10,DEL in L1 table 4, which is newer"),
        ),
        // The rule Pebble's check leaves out, where a scan relies on it: from
        // L1 down, it passes over every deeper key in a range deletion's span.
        (
            &[
                "L1",
                "a.RANGEDEL.5 c.RANGEDEL.inf",
                "Span:a-c:{(#5,RANGEDEL)}",
                "L2",
                "b.SET.10 b.SET.10",
                "b.SET.10:new",
            ],
            Some(
                "corrupt: a-c:{(#5,RANGEDEL)} in L1 table 3 is above b#10,SET in L2 table 4, \
                 which is newer",
            ),
        ),
        // L0 stacks by sequence number, not by file number: a table numbered
        // below another can hold newer keys, as Pebble's compactions within
        // L0 leave.
        (
            &[
                "L0",
                "a.SET.20 a.SET.20",
                "a.SET.20:newer",
                "L0",
                "a.SET.10 a.SET.10",
                "a.SET.10:older",
            ],
            None,
        ),
    ];
    for (input, want) in cases {
        let input: Vec<String> = input.iter().map(|l| l.to_string()).collect();
        assert_eq!(
            level_fault(&input).await.as_deref(),
            *want,
            "over\n{}",
            input.join("\n")
        );
    }
}

/// A store's own flushes and compactions keep the levels in order: rounds of
/// writes flushed over older tables, L0 compacted into L1 under later ones,
/// then everything down to L6, with snapshots keeping older versions and a
/// round left in the memtable at each step.
#[compio::test]
async fn a_store_keeps_its_levels_in_order_as_it_flushes_and_compacts() {
    let env = SimEnv::new();
    let db = roble::open_with(
        &env,
        PathBuf::from("/db"),
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open");
    let tables = |db: &roble::Db<SimFs>, level: u32| -> Vec<u64> {
        let versions = db.versions();
        versions
            .version
            .level(level)
            .iter()
            .map(|t| t.file_num)
            .collect()
    };

    let mut snapshots = Vec::new();
    for round in 0..6u8 {
        for i in 0..40u8 {
            db.put(&[b'k', i], &[round]).await.expect("put");
        }
        db.delete(&[b'k', round]).await.expect("delete");
        db.delete_range(&[b'k', 10 + 4 * round], &[b'k', 13 + 4 * round])
            .await
            .expect("delete_range");
        snapshots.push(db.snapshot().expect("snapshot"));
        db.check_levels()
            .await
            .unwrap_or_else(|e| panic!("round {round}, in the memtable: {e}"));
        db.flush().await.expect("flush");
        db.check_levels()
            .await
            .unwrap_or_else(|e| panic!("round {round}, flushed: {e}"));
        if round % 2 == 1 {
            db.compact(0, &tables(&db, 0)).await.expect("compact L0");
            db.check_levels()
                .await
                .unwrap_or_else(|e| panic!("round {round}, L0 compacted: {e}"));
        }
    }

    db.put(b"k\x05", b"in the memtable").await.expect("put");
    db.delete_range(b"k\x00", b"k\x03")
        .await
        .expect("delete_range");
    for level in 0..6 {
        let from = tables(&db, level);
        if !from.is_empty() {
            db.compact(level, &from).await.expect("compact");
            db.check_levels()
                .await
                .unwrap_or_else(|e| panic!("L{level} compacted: {e}"));
        }
    }
    assert!(!tables(&db, 6).is_empty(), "the compactions reached L6");
    drop(snapshots);
}

/// The databases Pebble wrote keep the levels in order, as roble reads them:
/// every golden fixture, with its unflushed log replayed.
#[compio::test]
async fn pebbles_databases_keep_their_levels_in_order() {
    let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let mut checked = 0;
    for fixture in std::fs::read_dir(&golden).expect("the fixtures") {
        let src = fixture.expect("a fixture").path();
        if !src.is_dir() {
            continue;
        }
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        let fs: &SimFs = env.sim();
        fs.mkdir_all(&dir).await.expect("mkdir");
        for file in std::fs::read_dir(&src).expect("a fixture's files") {
            let file = file.expect("a file");
            let bytes = std::fs::read(file.path()).expect("read");
            put_file(fs, &dir.join(file.file_name()), bytes).await;
        }
        fs.sync_dir(&dir).await.expect("sync dir");
        let db = roble::open_read_only(&env, &dir, Options::new())
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", src.display()));
        db.check_levels()
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", src.display()));
        checked += 1;
    }
    assert!(checked >= 7, "the fixtures of {}", golden.display());
}
