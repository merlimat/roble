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

use roble::VersionSet;
use roble_format::key::{self, Kind, SEQ_NUM_MAX};
use roble_format::manifest::{NewTable, VersionEdit};

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
