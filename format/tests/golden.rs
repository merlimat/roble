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

//! Reads the tables Go Pebble wrote and checks roble's dump against Pebble's.
//!
//! The dumps in `tests/golden` were produced by `harness/golden-sst`, which
//! writes each table with Go Pebble and then dumps what *Pebble's own reader*
//! sees in it. So this test does not check roble against the data the harness
//! fed in; it checks roble against Go Pebble's reading of the bytes on disk,
//! which is the thing compatibility actually means (DESIGN.md §12).
//!
//! Regenerate with `cd harness && go run ./golden-sst ../format/tests/golden`.

use std::path::{Path, PathBuf};

use roble_format::sst::Table;

const FIXTURES: &[&str] = &[
    "simple",
    "kinds",
    "multiblock",
    "snappy",
    "pebblev2",
    "zstd",
    "xxhash64",
    "rangedel",
];

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

#[test]
fn roble_dump_matches_go_pebble() {
    let dir = golden_dir();
    for name in FIXTURES {
        let sst = dir.join(format!("{name}.sst"));
        let expected_path = dir.join(format!("{name}.dump"));
        let bytes = std::fs::read(&sst).unwrap_or_else(|e| {
            panic!("{}: {e}; regenerate with harness/golden-sst", sst.display())
        });
        let expected = std::fs::read_to_string(&expected_path)
            .unwrap_or_else(|e| panic!("{}: {e}", expected_path.display()));

        let actual =
            roble_format::dump_table(&bytes).unwrap_or_else(|e| panic!("{name}: dump: {e}"));
        if let Some(diff) = roble_format::first_difference(&expected, &actual) {
            panic!("{name}: roble's dump differs from Go Pebble's\n{diff}");
        }
    }
}

/// The fixtures that exist to exercise one algorithm must actually contain it.
/// Every algorithm decodes to the same entries, so a fixture that silently got
/// Snappy instead of Zstd would pass `roble_dump_matches_go_pebble` while
/// leaving the Zstd path untested.
#[test]
fn the_compression_and_checksum_fixtures_contain_what_they_claim() {
    use roble_format::ChecksumType;
    use roble_format::block::Compression;

    let cases: &[(&str, Compression)] = &[
        ("simple", Compression::None),
        ("snappy", Compression::Snappy),
        ("zstd", Compression::Zstd),
    ];
    for (name, want) in cases {
        let bytes = std::fs::read(golden_dir().join(format!("{name}.sst")))
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let table = Table::open(&bytes).unwrap_or_else(|e| panic!("{name}: open: {e}"));
        let handles = table.data_block_handles().expect("index");
        let found: Vec<Compression> = handles
            .iter()
            .map(|h| table.block_compression(*h).expect("trailer"))
            .collect();
        assert!(
            found.contains(want),
            "{name} was supposed to exercise {}, but its {} data blocks are {:?}",
            want.name(),
            handles.len(),
            found
        );
    }

    let bytes = std::fs::read(golden_dir().join("xxhash64.sst")).expect("read xxhash64.sst");
    let table = Table::open(&bytes).expect("open xxhash64.sst");
    assert_eq!(
        table.footer().checksum,
        ChecksumType::XxHash64,
        "the xxhash64 fixture is not actually checksummed with xxhash64"
    );
}

#[test]
fn a_truncated_table_errors_rather_than_panicking() {
    let bytes = std::fs::read(golden_dir().join("simple.sst")).expect("read simple.sst");
    // Every prefix of a real table: none of them may panic, and none may be
    // read as if it were whole.
    for cut in 0..bytes.len() {
        match Table::open(&bytes[..cut]) {
            Err(_) => {}
            Ok(table) => {
                // Opening can legitimately succeed only if the footer and the
                // blocks it names survived the cut; walking must then fail.
                let walked = table.for_each_entry(|_, _| Ok(()));
                assert!(
                    walked.is_err(),
                    "a table truncated to {cut} of {} bytes read as if whole",
                    bytes.len()
                );
            }
        }
    }
}

#[test]
fn a_corrupt_block_fails_its_checksum() {
    let mut bytes = std::fs::read(golden_dir().join("simple.sst")).expect("read simple.sst");
    // Flip a bit inside the first data block, which starts at offset 0.
    bytes[8] ^= 0x01;
    let table = Table::open(&bytes).expect("footer and metaindex are untouched");
    let err = table
        .for_each_entry(|_, _| Ok(()))
        .expect_err("a flipped bit in a data block must not read as data");
    assert!(
        matches!(err, roble_format::Error::ChecksumMismatch { .. }),
        "expected a checksum mismatch, got {err}"
    );
}

/// What roble wrote has to read back as what it was handed: the cheap half of
/// the round trip, with no Go toolchain in the loop. The expensive half — Go
/// Pebble reading it — is `compat/sst-roundtrip`.
#[test]
fn roble_reads_back_what_roble_wrote() {
    use roble_format::block::Value;
    use roble_format::sst::{TableWriter, WriterOptions};
    use roble_format::{InternalKey, Kind};

    // Small blocks so the index is exercised rather than a single block.
    let opts = WriterOptions {
        block_size: 512,
        ..Default::default()
    };
    let mut w = TableWriter::new(opts).expect("writer");

    let mut expected = Vec::new();
    for i in 0..300u64 {
        let user_key = format!("/oxia/default/shard-0003/users/{i:08}").into_bytes();
        let kind = match i % 4 {
            0 => Kind::Set,
            1 => Kind::Delete,
            2 => Kind::SetWithDelete,
            _ => Kind::DeleteSized,
        };
        let value: Vec<u8> = if matches!(kind, Kind::Set | Kind::SetWithDelete) {
            format!("{{\"v\":{i}}}").into_bytes()
        } else {
            Vec::new()
        };
        let key = InternalKey {
            user_key: &user_key,
            seq_num: 1000 + i,
            kind,
            obsolete: false,
        };
        w.add(key, &value).expect("add");
        expected.push((user_key, 1000 + i, kind, value));
    }
    // Tombstones too: they live in their own block, so a reader that walks
    // data blocks alone would report none of them.
    let tombstones: Vec<(Vec<u8>, Vec<u8>, u64)> = vec![
        (b"/t/a".to_vec(), b"/t/b".to_vec(), 50),
        (b"/t/c".to_vec(), b"/t/d".to_vec(), 40),
        (b"/t/c".to_vec(), b"/t/d".to_vec(), 30),
    ];
    for (start, end, seq) in &tombstones {
        w.add_range_deletion(start, end, *seq).expect("tombstone");
    }

    let bytes = w.finish().expect("finish");

    let table = Table::open(&bytes).expect("roble reopens its own table");
    assert_eq!(table.footer().format.name(), "pebblev4");
    assert!(!table.has_two_level_index());
    assert!(
        table.data_block_handles().expect("index").len() > 1,
        "the fixture is meant to span several data blocks"
    );

    let mut got = Vec::new();
    table
        .for_each_entry(|key, value| {
            let v = match value {
                Value::InPlace(v) => v.to_vec(),
                other => panic!("roble wrote a {} value", other.kind_name()),
            };
            got.push((key.user_key.to_vec(), key.seq_num, key.kind, v));
            Ok(())
        })
        .expect("walk");
    assert_eq!(got, expected);

    let read_back: Vec<(Vec<u8>, Vec<u8>, u64)> = table
        .range_deletions()
        .expect("range deletions")
        .into_iter()
        .map(|t| (t.start, t.end, t.seq_num))
        .collect();
    assert_eq!(read_back, tombstones);
}

#[test]
fn the_writer_refuses_what_roble_does_not_write() {
    use roble_format::sst::{TableWriter, WriterOptions};
    use roble_format::{InternalKey, Kind};

    let key = |kind, obsolete| InternalKey {
        user_key: b"/k",
        seq_num: 1,
        kind,
        obsolete,
    };
    for kind in [Kind::Merge, Kind::RangeKeySet, Kind::Excise] {
        let mut w = TableWriter::new(WriterOptions::default()).expect("writer");
        assert!(
            w.add(key(kind, false), b"v").is_err(),
            "{} should be refused",
            kind.name()
        );
    }
    // The obsolete bit is never set, because setting it wrongly makes a Go
    // reader skip a live key.
    let mut w = TableWriter::new(WriterOptions::default()).expect("writer");
    assert!(w.add(key(Kind::Set, true), b"v").is_err());

    // Out-of-order keys are caught at write time, not discovered at read time.
    let mut w = TableWriter::new(WriterOptions::default()).expect("writer");
    w.add(key(Kind::Set, false), b"v").expect("first key");
    let earlier = InternalKey {
        user_key: b"/a",
        seq_num: 1,
        kind: Kind::Set,
        obsolete: false,
    };
    assert!(w.add(earlier, b"v").is_err(), "descending keys must fail");
}
