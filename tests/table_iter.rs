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

use roble::__private::OpenTable;
use roble::sim::{SimEnv, SimFs};
use roble::{Access, Caches, Disk, Fs, WriteBuf};
use roble_format::block::Value;
use roble_format::sst::{Table, TableWriter, WriterOptions};

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
    open_bytes(std::fs::read(sst_path(name)).expect("read fixture")).await
}

/// The same, for a table built in the test rather than read from the corpus.
async fn open_bytes(bytes: Vec<u8>) -> (SimEnv, std::rc::Rc<OpenTable<roble::sim::SimDisk>>) {
    let (env, opened) = try_open_bytes(bytes).await;
    (env, std::rc::Rc::new(opened.expect("open table")))
}

/// The same again, handing back whatever opening the table said, for the
/// fixtures whose point is that it says no.
async fn try_open_bytes(bytes: Vec<u8>) -> (SimEnv, roble::Result<OpenTable<roble::sim::SimDisk>>) {
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
    (
        env,
        OpenTable::open(1, disk, roble::__private::block_cache(&caches, 0)).await,
    )
}

/// An index names its blocks by handle, and a handle is read from the table.
/// One that reaches past the end of the file has to be refused when the table
/// is opened: a read sized from it would ask for that many bytes, and a
/// length past what memory holds aborts the process rather than failing.
///
/// The index block is checksummed, so the bad handle is put in a block of its
/// own with a checksum that matches, appended, and named by a new footer. That
/// way what reaches the engine is a table every check before this one passes.
#[compio::test]
async fn an_index_naming_a_block_past_the_end_is_refused_at_open() {
    use roble_format::{BlockBuilder, FOOTER_LEN, InternalKey, Kind};

    let mut w = TableWriter::new(WriterOptions::default()).expect("writer");
    for (i, k) in [b"/oxia/a", b"/oxia/b"].iter().enumerate() {
        let key = InternalKey {
            user_key: &k[..],
            seq_num: i as u64 + 1,
            kind: Kind::Set,
            obsolete: false,
        };
        w.add(key, b"value").expect("add");
    }
    let bytes = w.finish().expect("finish");
    let table = Table::open(&bytes).expect("a table roble wrote");
    let footer = *table.footer();
    let separator = table.index_entries().expect("index")[0].separator.clone();

    // One entry, naming a block at offset zero far longer than the file.
    let mut handle = Vec::new();
    for v in [0u64, 1 << 60] {
        put_uvarint(&mut handle, v);
    }
    let mut index = BlockBuilder::new(1);
    index.add(&separator, &handle, 0, None, false);
    let index = index.finish().to_vec();

    let at = bytes.len() - FOOTER_LEN;
    let mut patched = bytes[..at].to_vec();
    patched.extend_from_slice(&index);
    patched.push(0); // no compression
    let sum = footer.checksum.compute(&index, 0).expect("checksum");
    patched.extend_from_slice(&sum.to_le_bytes());

    // The footer as it was, with the index handle pointing at the new block.
    let mut tail = bytes[at..].to_vec();
    let mut handles = Vec::new();
    put_uvarint(&mut handles, footer.metaindex.offset);
    put_uvarint(&mut handles, footer.metaindex.length);
    put_uvarint(&mut handles, at as u64);
    put_uvarint(&mut handles, index.len() as u64);
    let version_at = FOOTER_LEN - 12;
    tail[1..version_at].fill(0);
    tail[1..1 + handles.len()].copy_from_slice(&handles);
    patched.extend_from_slice(&tail);

    let (_env, opened) = try_open_bytes(patched).await;
    let err = match opened {
        Ok(_) => panic!("a table whose index reaches past its end opened"),
        Err(e) => e,
    };
    // A format error, and underneath it the format crate's own corruption
    // report, which is where the kind is.
    use std::error::Error as _;
    assert!(matches!(err, roble::Error::Format(_)), "got {err:?}");
    assert!(
        matches!(
            err.source()
                .and_then(|s| s.downcast_ref::<roble_format::Error>()),
            Some(roble_format::Error::Corrupt(_))
        ),
        "got {err:?}"
    );
}

fn put_uvarint(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push(v as u8 | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
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

/// A reverse walk with nothing in the cache, which is where a backward
/// read-ahead window is built.
///
/// `an_iterator_walks_a_whole_table_both_ways` walks forward before it turns
/// round, so by then every block is cached and a window is never needed. A
/// window built for a walk going backwards holds the block asked for at its
/// *end* rather than its start, and handing back the wrong one of them is
/// invisible to every other test here — it reads correctly forwards.
#[compio::test]
async fn a_reverse_walk_on_a_cold_cache_reads_its_window_the_right_way_round() {
    for name in FIXTURES {
        let bytes = std::fs::read(sst_path(name)).expect("read fixture");
        let want = expected(&bytes);
        let (_env, table) = open(name).await;

        let mut backward = Vec::new();
        let mut it = table.iter();
        it.last().await.expect("last");
        while it.is_valid() {
            let e = it.entry().await.expect("entry");
            backward.push((e.key, e.value));
            it.prev().await.expect("prev");
        }
        backward.reverse();
        assert_eq!(backward, want, "{name}: reverse, cache cold");
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

/// A table with range deletions and no point key has nowhere to go.
///
/// A flush whose every write was a `DeleteRange` leaves exactly this, which
/// `compat/sst-roundtrip` round-trips as `rangedel-only` and Go Pebble reads
/// back tombstone for tombstone. Its index is empty, so every way of
/// positioning has to come back invalid rather than reach for a block that is
/// not there — and the tombstones are still readable, which is the whole
/// reason the table exists.
#[compio::test]
async fn an_empty_table_has_nowhere_to_go_but_still_holds_its_tombstones() {
    let mut w = TableWriter::new(WriterOptions::default()).expect("writer");
    w.add_range_deletion(b"/oxia/a", b"/oxia/z", 5)
        .expect("tombstone");
    let bytes = w.finish().expect("finish");

    let (_env, table) = open_bytes(bytes).await;
    assert_eq!(table.tombstones.len(), 1, "the tombstone survived");

    let mut it = table.iter();
    assert!(!it.is_valid(), "a fresh iterator is unpositioned");

    it.first().await.expect("first");
    assert!(!it.is_valid(), "first");
    it.last().await.expect("last");
    assert!(!it.is_valid(), "last");

    let target = roble_format::key::search_key(b"/oxia/m");
    it.seek_ge(&target).await.expect("seek_ge");
    assert!(!it.is_valid(), "seek_ge");
    it.seek_lt(&target).await.expect("seek_lt");
    assert!(!it.is_valid(), "seek_lt");

    // And stepping an unpositioned iterator stays unpositioned rather than
    // running off either end.
    it.next().await.expect("next");
    assert!(!it.is_valid(), "next");
    it.prev().await.expect("prev");
    assert!(!it.is_valid(), "prev");
}

/// A table written with another comparer is refused, by name, at open.
///
/// §3 pins the ordering to the bytewise comparer, and Oxia's own
/// `oxia-slash-spans` is the one a real database could carry: it splits keys
/// on slashes, so a reader that ignored the name would compare them in an
/// order the table was not written in and answer every seek slightly wrong.
/// The fixture's keys are in bytewise order and its ordering is unchanged —
/// only the name differs — so what is refused is the claim rather than the
/// bytes, which is the refusal that has to hold.
#[compio::test]
async fn a_table_written_with_another_comparer_is_refused() {
    let bytes = std::fs::read(sst_path("refuse-comparer")).expect("read fixture");
    let (_env, opened) = try_open_bytes(bytes).await;
    let err = opened.err().expect("a foreign comparer must be refused");
    let text = format!("{err}");
    assert!(
        text.contains("oxia-slash-spans") && text.contains("leveldb.BytewiseComparator"),
        "the refusal should name both comparers, got {text:?}"
    );
    assert!(
        matches!(err, roble::Error::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );
}

/// One cursor, handed to tables of **different formats**, reads both right.
///
/// `seek_ge_into` takes the caller's cursor so that a point lookup does not
/// build one per table it searches, and keeps beside it the format it was
/// built for. That is not bookkeeping: the format decides whether a `SET`
/// value carries a prefix byte — Pebblev3 added it — so a cursor built for
/// one format and reused on a table in another would read every value
/// shifted by a byte. A database Pebble upgraded over time holds tables at
/// more than one format, so this is reachable and not hypothetical.
///
/// Pebblev2 and Pebblev3 are that pair, and the loop goes round twice: the
/// second pass hands each table a cursor built for the *other* format, which
/// is the only arrangement that reaches the check. One pass would not.
#[compio::test]
async fn one_cursor_serves_tables_of_different_formats() {
    let mut cursor = None;
    for pass in 0..2 {
        for name in ["pebblev2", "pebblev3"] {
            let bytes = std::fs::read(sst_path(name)).expect("read fixture");
            let (want_key, want_value) = expected(&bytes).first().expect("a first entry").clone();

            let (_env, table) = open(name).await;
            let (mut key, mut value) = (Vec::new(), Vec::new());
            let target = roble_format::key::search_key(b"");
            assert!(
                table
                    .seek_ge_into(&target, &mut cursor, &mut key, &mut value)
                    .await
                    .expect("seek"),
                "{name} should hold an entry at or after the empty key"
            );
            assert_eq!(key, want_key, "pass {pass}, {name}: the key it read");
            assert_eq!(value, want_value, "pass {pass}, {name}: the value it read");
        }
    }
}
