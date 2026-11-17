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

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use roble_format::sst::Table;

const FIXTURES: &[&str] = &[
    "simple",
    "kinds",
    "multiblock",
    "snappy",
    "pebblev1",
    "pebblev2",
    "pebblev3",
    "zstd",
    "xxhash64",
    "rangedel",
    "bloom",
    "valueblocks",
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
        if let Some(diff) = first_difference(&expected, &actual) {
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

/// The filter block Go Pebble wrote, rebuilt from the same keys, must come out
/// byte for byte identical.
///
/// This is the only check that pins the hash. A filter that merely "works" —
/// says yes to its own keys at a believable false-positive rate — can still
/// disagree with Go on every key, and the disagreement surfaces as a point
/// lookup that answers "not found" for a key that is really there.
#[test]
fn robles_bloom_filter_is_byte_identical_to_go_pebbles() {
    use roble_format::filter::{DEFAULT_BITS_PER_KEY, Filter, FilterBuilder};

    let bytes = std::fs::read(golden_dir().join("bloom.sst")).expect("read bloom.sst");
    let table = Table::open(&bytes).expect("open bloom.sst");

    let block = table
        .filter_block()
        .expect("read the filter block")
        .expect("the bloom fixture has a filter block");
    let go_filter = block.as_slice();

    // Pebble adds each point key's prefix as it writes, in table order, which
    // under the bytewise comparer is the whole user key.
    let mut builder = FilterBuilder::new(DEFAULT_BITS_PER_KEY);
    let mut keys: Vec<Vec<u8>> = Vec::new();
    table
        .for_each_entry(|key, _| {
            builder.add_key(key.user_key);
            keys.push(key.user_key.to_vec());
            Ok(())
        })
        .expect("walk");
    let roble_filter = builder.finish();

    assert_eq!(
        roble_filter.len(),
        go_filter.len(),
        "filter lengths differ: roble {} bytes, Go {} bytes",
        roble_filter.len(),
        go_filter.len()
    );
    if roble_filter != go_filter {
        let differing = roble_filter
            .iter()
            .zip(go_filter)
            .enumerate()
            .filter(|(_, (a, b))| a != b)
            .count();
        let first = roble_filter
            .iter()
            .zip(go_filter)
            .position(|(a, b)| a != b)
            .expect("they differ somewhere");
        panic!(
            "filter bits differ in {differing} of {} bytes, first at {first}:              roble {:#04x} vs Go {:#04x}",
            go_filter.len(),
            roble_filter[first],
            go_filter[first]
        );
    }

    // And the filter Go wrote must answer yes to every key in the table.
    let f = Filter::new(go_filter).expect("Go's filter validates");
    assert_eq!(f.probes(), 6, "10 bits per key means 6 probes");
    for k in &keys {
        assert!(
            f.may_contain(k),
            "Go's own filter excludes {:?}",
            String::from_utf8_lossy(k)
        );
    }
}

/// The value-block fixture must really contain value blocks, and roble must
/// resolve every handle to the same bytes Pebble's reader produces.
///
/// The dump comparison already proves the values match, because Go's dump
/// resolves them through Pebble. What this adds is that the path was taken at
/// all: a fixture that stopped producing value blocks would still pass the
/// dump comparison while leaving resolution untested.
#[test]
fn the_value_block_fixture_really_exercises_resolution() {
    use roble_format::block::Value;

    let bytes = std::fs::read(golden_dir().join("valueblocks.sst")).expect("read it");
    let table = Table::open(&bytes).expect("open");

    let blocks = table.value_blocks().expect("value index");
    assert!(
        !blocks.is_empty(),
        "the fixture has no value blocks, so resolution is untested"
    );

    let mut handles = 0usize;
    let mut in_place = 0usize;
    table
        .for_each_entry(|_, value| {
            match value {
                Value::ValueHandle(h) => {
                    let resolved = table.resolve_value(&blocks, h).expect("resolve");
                    assert!(!resolved.is_empty(), "a resolved value should not be empty");
                    handles += 1;
                }
                Value::InPlace(_) => in_place += 1,
                other => panic!("unexpected {} value", other.kind_name()),
            }
            Ok(())
        })
        .expect("walk");

    assert!(
        handles > 0,
        "no entry used a value handle, so {in_place} in-place values proved nothing"
    );
}

/// A table Pebble wrote with range keys must be refused, by name.
///
/// §3 declines range keys, and until this fixture existed that refusal was
/// only tested against synthetic bytes. Pebble writes them through a public
/// API, so the real thing is cheap to produce — and the point of refusing is
/// that roble declines the table rather than reading past a structure it does
/// not understand.
#[test]
fn a_real_range_key_table_is_refused() {
    let bytes = std::fs::read(golden_dir().join("refuse-rangekeys.sst"))
        .expect("read refuse-rangekeys.sst");
    let err = Table::open(&bytes).expect_err("a range-key table must be refused");
    let text = format!("{err}");
    assert!(
        text.contains("range-key"),
        "the refusal should name range keys, got {text:?}"
    );
    // And it must be a refusal, not a corruption report: the file is valid.
    assert!(
        matches!(err, roble_format::Error::Unsupported(_)),
        "expected Unsupported, got {err:?}"
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

/// `block_compression` takes any handle a caller has, not only one this table
/// decoded, so it cannot rely on the decoder having bounded it.
#[test]
fn a_handle_past_the_end_of_u64_is_refused_by_block_compression() {
    let bytes = std::fs::read(golden_dir().join("simple.sst")).expect("read simple.sst");
    let table = Table::open(&bytes).expect("open");
    let handle = roble_format::BlockHandle {
        offset: u64::MAX,
        length: 1,
    };
    let err = table
        .block_compression(handle)
        .expect_err("a handle no table reaches");
    assert!(
        matches!(err, roble_format::Error::Corrupt(_)),
        "got {err:?}"
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
    // The caller's mistake, not damage: nothing was read.
    assert!(
        matches!(
            w.add(earlier, b"v"),
            Err(roble_format::Error::InvalidArgument(_))
        ),
        "descending keys must fail, as an invalid argument"
    );

    // As does a sequence number past the largest the trailer holds.
    let mut w = TableWriter::new(WriterOptions::default()).expect("writer");
    let too_large = InternalKey {
        seq_num: roble_format::key::SEQ_NUM_MAX + 1,
        ..key(Kind::Set, false)
    };
    assert!(matches!(
        w.add(too_large, b"v"),
        Err(roble_format::Error::InvalidArgument(_))
    ));
}

/// A MERGE key is refused wherever it is met, and for its own reason.
///
/// `refuse-merge.sst` is ordinary apart from one entry: the table opens, since
/// nothing about its structure is unsupported, and the kind is only met when
/// an entry is read. That is the point — the refusal has to come from reading,
/// not from the header.
///
/// All three read paths are checked because `check_kind` is public for exactly
/// this: a kind refused when a table is scanned and served when it is sought
/// would be worse than either.
#[test]
fn a_merge_key_is_refused_by_every_path_that_reads_one() {
    let bytes =
        std::fs::read(golden_dir().join("refuse-merge.sst")).expect("read refuse-merge.sst");
    let table = Table::open(&bytes).expect("the table itself is well formed");

    let merge_key = b"/oxia/default/shard-0009/users/00000003";
    let target = roble_format::key::search_key(merge_key);

    let scanned = table
        .for_each_entry(|_, _| Ok(()))
        .expect_err("a scan must refuse the MERGE key");
    let sought = table
        .seek_ge(&target, |_, _| Ok(()))
        .expect_err("a seek_ge onto the MERGE key must refuse it");
    // seek_lt from just past the key lands on it.
    let after = roble_format::key::search_key(b"/oxia/default/shard-0009/users/00000004");
    let backwards = table
        .seek_lt(&after, |_, _| Ok(()))
        .expect_err("a seek_lt onto the MERGE key must refuse it");

    for (path, err) in [
        ("scan", &scanned),
        ("seek_ge", &sought),
        ("seek_lt", &backwards),
    ] {
        let text = format!("{err}");
        assert!(
            text.contains("MERGE keys: Oxia configures no merger"),
            "{path} should name the merger, got {text:?}"
        );
        assert!(
            matches!(err, roble_format::Error::Unsupported(_)),
            "{path}: expected Unsupported, got {err:?}"
        );
    }

    // The keys around it are ordinary, so what is refused is the kind and not
    // the file: a seek that stops short of the MERGE key still answers.
    let before = roble_format::key::search_key(b"/oxia/default/shard-0009/users/00000001");
    table
        .seek_ge(&before, |_, _| Ok(()))
        .expect("a seek that does not reach the MERGE key still answers");
}

/// A table above roble's pin is refused whole, by name.
///
/// Pebblev5 is columnar: the blocks are laid out differently, so reading part
/// of it would not be reading it slowly but reading it wrongly (§3). The
/// fixture has no dump beside it for that reason — a dump exists to be
/// compared against roble's reading, and roble has none to offer.
#[test]
fn a_table_above_the_pin_is_refused_whole() {
    let bytes =
        std::fs::read(golden_dir().join("refuse-pebblev5.sst")).expect("read refuse-pebblev5.sst");
    let err = Table::open(&bytes).expect_err("pebblev5 is above the pin");
    let text = format!("{err}");
    assert!(
        text.contains("pebblev5") && text.contains("pin"),
        "the refusal should name the format and the pin, got {text:?}"
    );
    assert!(
        matches!(err, roble_format::Error::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );
}

/// The format crate refuses the same table on its own account.
///
/// `Table::open` and the engine's `OpenTable::open` each read the properties
/// and each check the comparer, because they are reached by different callers
/// — the dump command goes through one and a database read through the other.
/// A check in only one of them would leave a reader that served keys in an
/// order the table was never written in.
#[test]
fn the_reader_refuses_a_foreign_comparer_too() {
    let bytes =
        std::fs::read(golden_dir().join("refuse-comparer.sst")).expect("read refuse-comparer.sst");
    let err = Table::open(&bytes).expect_err("a foreign comparer must be refused");
    let text = format!("{err}");
    assert!(
        text.contains("oxia-slash-spans"),
        "the refusal should name the comparer, got {text:?}"
    );
    assert!(
        matches!(err, roble_format::Error::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );
}

/// A table with no point key answers every read, rather than refusing them.
///
/// The shape is the one `compat/sst-roundtrip` round-trips as `rangedel-only`
/// and [#24] gave the engine a test for: a flush whose every write was a
/// `DeleteRange`. The reader has its own copies of the same three questions,
/// reached by the dump command rather than by a database read, and its index
/// is empty for all of them.
///
/// "Answers" rather than "finds": a seek returns `false` for having found
/// nothing, which is not the same as an error, and the tombstones are still
/// there to be read.
#[test]
fn a_table_with_no_point_key_answers_every_read() {
    use roble_format::sst::{TableWriter, WriterOptions};

    let mut w = TableWriter::new(WriterOptions::default()).expect("writer");
    w.add_range_deletion(b"/oxia/a", b"/oxia/z", 5)
        .expect("tombstone");
    let bytes = w.finish().expect("finish");

    let table = Table::open(&bytes).expect("an empty table still opens");
    assert_eq!(
        table.range_deletions().expect("tombstones").len(),
        1,
        "the tombstone survived"
    );

    let target = roble_format::key::search_key(b"/oxia/m");
    assert!(
        !table.seek_ge(&target, |_, _| Ok(())).expect("seek_ge"),
        "seek_ge found nothing"
    );
    assert!(
        !table.seek_lt(&target, |_, _| Ok(())).expect("seek_lt"),
        "seek_lt found nothing"
    );

    let mut seen = 0;
    table
        .for_each_entry(|_, _| {
            seen += 1;
            Ok(())
        })
        .expect("scan");
    assert_eq!(seen, 0, "there is nothing to scan");
}

/// A table with no filter block says so, rather than reporting an empty one.
///
/// Most of the corpus has no filter — only `bloom` is written with one — and
/// the difference between "no filter" and "a filter that matches nothing" is
/// the difference between reading a table and skipping it (§6). Pebble's
/// `rocksdb.filter.policy` property is absent here, and `filter_block` has to
/// agree with it.
#[test]
fn a_table_without_a_filter_has_no_filter_block() {
    let bytes = std::fs::read(golden_dir().join("simple.sst")).expect("read simple.sst");
    let table = Table::open(&bytes).expect("open simple.sst");
    assert!(
        table.filter_block().expect("read the metaindex").is_none(),
        "simple.sst is written without a filter policy"
    );
    assert!(
        table.properties().string("rocksdb.filter.policy").is_none(),
        "and says so in its properties"
    );
}

/// The first differing line between two dumps, which is far more use than two
/// several-hundred-line blobs.
fn first_difference(expected: &str, actual: &str) -> Option<String> {
    let mut out = String::new();
    for (i, (e, a)) in expected.lines().zip(actual.lines()).enumerate() {
        if e != a {
            writeln!(out, "  line {}:", i + 1).expect("String");
            writeln!(out, "    expected: {e}").expect("String");
            writeln!(out, "    actual:   {a}").expect("String");
            return Some(out);
        }
    }
    let (e, a) = (expected.lines().count(), actual.lines().count());
    if e == a {
        return None;
    }
    writeln!(out, "  expected {e} lines, got {a}").expect("String");
    let (label, extra) = if a > e {
        ("actual only", actual.lines().skip(e).take(3))
    } else {
        ("expected only", expected.lines().skip(a).take(3))
    };
    for line in extra {
        writeln!(out, "    {label}: {line}").expect("String");
    }
    Some(out)
}

/// `first_difference` explains a failure, so these run it on purpose: the
/// golden test only ever takes its "no difference" path while it passes.
mod first_difference_explains {
    use super::first_difference;

    /// Two dumps that agree have no first difference.
    ///
    /// This is the only path the golden test takes while it passes, which is why
    /// everything below it had never run: a function whose whole job is to
    /// explain a failure is dead code until something fails, and then it is
    /// the only thing anyone reads.
    #[test]
    fn dumps_that_agree_have_no_difference() {
        let text = "format pebblev4\nentries 2\n";
        assert_eq!(first_difference(text, text), None);
        assert_eq!(first_difference("", ""), None);
    }

    #[test]
    fn a_differing_line_is_named_by_its_number() {
        let expected = "format pebblev4\nchecksum crc32c\nentries 2\n";
        let actual = "format pebblev4\nchecksum xxhash64\nentries 2\n";
        let diff = first_difference(expected, actual).expect("they differ");
        // One-based, and the second line is line 2 rather than 1 or 3. An
        // off-by-one here sends whoever is debugging to the wrong line of a
        // several-hundred-line dump.
        assert!(diff.contains("line 2:"), "got {diff}");
        assert!(diff.contains("expected: checksum crc32c"), "got {diff}");
        assert!(diff.contains("actual:   checksum xxhash64"), "got {diff}");
        // And it stops at the first one rather than listing everything.
        assert!(!diff.contains("entries"), "got {diff}");
    }

    #[test]
    fn the_very_first_line_is_line_one() {
        let diff = first_difference("a\nb\n", "z\nb\n").expect("they differ");
        assert!(diff.contains("line 1:"), "got {diff}");
    }

    #[test]
    fn a_dump_that_is_longer_says_so_and_shows_what_is_extra() {
        let expected = "a\nb\n";
        let actual = "a\nb\nc\nd\ne\nf\n";
        let diff = first_difference(expected, actual).expect("they differ in length");
        assert!(diff.contains("expected 2 lines, got 6"), "got {diff}");
        // Three lines of the extra, which is enough to recognise what arrived
        // without reprinting the blob this function exists to avoid.
        for line in ["actual only: c", "actual only: d", "actual only: e"] {
            assert!(diff.contains(line), "got {diff}");
        }
        assert!(!diff.contains("actual only: f"), "only three, got {diff}");
    }

    #[test]
    fn a_dump_that_is_shorter_says_what_is_missing() {
        let expected = "a\nb\nc\nd\n";
        let actual = "a\nb\n";
        let diff = first_difference(expected, actual).expect("they differ in length");
        assert!(diff.contains("expected 4 lines, got 2"), "got {diff}");
        assert!(diff.contains("expected only: c"), "got {diff}");
        assert!(diff.contains("expected only: d"), "got {diff}");
    }

    #[test]
    fn a_differing_line_is_reported_ahead_of_a_differing_length() {
        // Both are wrong at once, which is the usual case when a dump has
        // gone off the rails partway through. The line is the useful fact —
        // the length follows from it — so it is the one reported.
        let expected = "a\nb\nc\n";
        let actual = "a\nZ\n";
        let diff = first_difference(expected, actual).expect("they differ");
        assert!(diff.contains("line 2:"), "got {diff}");
        assert!(!diff.contains("lines, got"), "got {diff}");
    }

    #[test]
    fn a_trailing_newline_is_not_a_difference() {
        // `lines()` drops it either way, and a dump built with `writeln!`
        // always has one. Reporting it as a mismatch would make every
        // hand-written expectation fail for a reason nobody could see.
        assert_eq!(first_difference("a\nb\n", "a\nb"), None);
    }
}
