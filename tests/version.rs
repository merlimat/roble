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
