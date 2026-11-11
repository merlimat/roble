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

//! Stepping through a table, forwards and backwards, across block boundaries.
//!
//! The oracle is the table's own entry list, read with `for_each_entry` —
//! which `format/tests/golden.rs` already pins against Go Pebble's reader. So
//! comparing against it is comparing against Pebble, without a second corpus
//! to keep fresh.
//!
//! It is also **exhaustive** where a probe corpus is sampled: every entry of
//! every fixture is sought, stepped away from in both directions, and stepped
//! back to. Crossing a block boundary is the thing that breaks, and with the
//! `multiblock` fixture's two-level index and many small blocks, most of these
//! steps cross one.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::table::OpenTable;
use roble::{Access, Caches, Disk, Fs, WriteBuf};
use roble_format::block::Value;
use roble_format::sst::Table;

/// The fixtures roble accepts, as `format/tests/golden.rs` lists them.
const FIXTURES: &[&str] = &[
    "simple",
    "kinds",
    "multiblock",
    "snappy",
    "pebblev2",
    "zstd",
    "xxhash64",
    "rangedel",
    "bloom",
    "valueblocks",
];

fn sst_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("format/tests/golden")
        .join(format!("{name}.sst"))
}

/// Every entry of a table, as the format crate's pinned full scan reads them.
fn expected(bytes: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let table = Table::open(bytes).expect("open");
    let value_blocks = table.value_blocks().expect("value blocks");
    let mut out = Vec::new();
    table
        .for_each_entry(|k, v| {
            let value = match v {
                Value::InPlace(b) => b.to_vec(),
                Value::ValueHandle(h) => table.resolve_value(&value_blocks, h)?,
                Value::BlobHandle(_) => panic!("a blob handle is above roble's pin"),
            };
            // Re-encoded with the **raw** trailer, obsolete bit included.
            // `InternalKey` splits that bit out into a field of its own, so
            // rebuilding the key from the kind alone drops it — and the
            // `valueblocks` fixture has keys that carry it. The iterator hands
            // back the key as stored, which is what the ordering is defined
            // on, so the oracle has to do the same.
            let kind = k.kind.to_byte()
                | if k.obsolete {
                    roble_format::key::OBSOLETE_BIT
                } else {
                    0
                };
            out.push((
                roble_format::key::encode_with_trailer(k.user_key, (k.seq_num << 8) | kind as u64),
                value,
            ));
            Ok(())
        })
        .expect("scan");
    out
}

/// Puts a fixture into a simulated filesystem and opens it as a table.
async fn open(name: &str) -> (SimEnv, std::rc::Rc<OpenTable<roble::sim::SimDisk>>) {
    let bytes = std::fs::read(sst_path(name)).expect("read fixture");
    let env = SimEnv::new();
    let fs: &SimFs = env.sim();
    fs.mkdir_all(Path::new("/t")).await.expect("mkdir");
    let path = PathBuf::from("/t/000001.sst");
    let disk = fs.create(&path).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
    result.expect("write");
    disk.close().await.expect("close");

    let caches: Caches<SimFs> = Caches::default();
    let disk = fs.open(&path, Access::ReadOnly).await.expect("open");
    let table = OpenTable::open(1, disk, caches.blocks(0))
        .await
        .expect("open table");
    (env, std::rc::Rc::new(table))
}

#[compio::test]
async fn an_iterator_walks_a_whole_table_both_ways() {
    for name in FIXTURES {
        let bytes = std::fs::read(sst_path(name)).expect("read fixture");
        let want = expected(&bytes);
        let (_env, table) = open(name).await;

        let mut forward = Vec::new();
        let mut it = table.iter();
        it.first().await.expect("first");
        while it.is_valid() {
            let e = it.entry().await.expect("entry");
            forward.push((e.key, e.value));
            it.next().await.expect("next");
        }
        assert_eq!(forward, want, "{name}: forward");

        let mut backward = Vec::new();
        let mut it = table.iter();
        it.last().await.expect("last");
        while it.is_valid() {
            let e = it.entry().await.expect("entry");
            backward.push((e.key, e.value));
            it.prev().await.expect("prev");
        }
        backward.reverse();
        assert_eq!(backward, want, "{name}: reverse");
    }
}

#[compio::test]
async fn every_entry_can_be_sought_and_stepped_away_from() {
    // Exhaustive rather than sampled: for each entry, seek to it, step
    // forward and back, and step back and forward. Crossing a block boundary
    // is what breaks, and the entries either side of one are only reachable
    // this way.
    for name in FIXTURES {
        let bytes = std::fs::read(sst_path(name)).expect("read fixture");
        let want = expected(&bytes);
        let (_env, table) = open(name).await;
        let mut it = table.iter();

        for (i, (key, _)) in want.iter().enumerate() {
            it.seek_ge(key).await.expect("seek_ge");
            assert!(it.is_valid(), "{name}: seek to entry {i} found nothing");
            assert_eq!(it.key(), &key[..], "{name}: seek landed wrong at {i}");

            it.next().await.expect("next");
            match want.get(i + 1) {
                Some((k, _)) => assert_eq!(it.key(), &k[..], "{name}: forward from {i}"),
                None => assert!(!it.is_valid(), "{name}: past the last entry"),
            }

            it.seek_ge(key).await.expect("seek_ge");
            it.prev().await.expect("prev");
            match i.checked_sub(1).and_then(|j| want.get(j)) {
                Some((k, _)) => assert_eq!(it.key(), &k[..], "{name}: backward from {i}"),
                None => assert!(!it.is_valid(), "{name}: before the first entry"),
            }

            // And `seek_lt` of an entry is the entry before it.
            it.seek_lt(key).await.expect("seek_lt");
            match i.checked_sub(1).and_then(|j| want.get(j)) {
                Some((k, _)) => assert_eq!(it.key(), &k[..], "{name}: seek_lt from {i}"),
                None => assert!(!it.is_valid(), "{name}: seek_lt before the first entry"),
            }

            // A target that is **not** a stored key, falling just past every
            // version of this one. This is the only way to reach the case
            // where a block holds nothing at or after the target and the
            // answer is the next block's first entry: a seek to a key that
            // exists always lands inside the block that holds it.
            let user = roble_format::key::split_encoded(key).expect("split").0;
            let past = roble_format::key::encode_with_trailer(user, 0);
            it.seek_ge(&past).await.expect("seek_ge past");
            let expect_at = want
                .iter()
                .position(|(k, _)| roble_format::key::compare(k, &past).expect("cmp").is_ge());
            match expect_at {
                Some(j) => {
                    assert!(it.is_valid(), "{name}: seek past {i} found nothing");
                    assert_eq!(it.key(), &want[j].0[..], "{name}: seek past {i}");
                }
                None => assert!(!it.is_valid(), "{name}: seek past the last key"),
            }
        }
    }
}

#[compio::test]
async fn stepping_back_and_forth_returns_to_the_same_place() {
    // A step that lands correctly but leaves the cursor unable to carry on is
    // the bug reverse iteration actually has, and a one-step check cannot see
    // it. This walks the whole table in a zig-zag.
    for name in FIXTURES {
        let bytes = std::fs::read(sst_path(name)).expect("read fixture");
        let want = expected(&bytes);
        if want.len() < 3 {
            continue;
        }
        let (_env, table) = open(name).await;
        let mut it = table.iter();
        it.first().await.expect("first");

        for (i, (key, _)) in want.iter().enumerate() {
            assert_eq!(it.key(), &key[..], "{name}: zig-zag lost its place at {i}");
            // Forward then back lands where it started, except at the ends.
            it.next().await.expect("next");
            if it.is_valid() {
                it.prev().await.expect("prev");
                assert_eq!(it.key(), &key[..], "{name}: there and back again at {i}");
                it.next().await.expect("next");
            }
        }
    }
}
