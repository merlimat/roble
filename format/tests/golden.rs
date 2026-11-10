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

const FIXTURES: &[&str] = &["simple", "kinds", "multiblock", "snappy", "pebblev2"];

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").expect("writing to a String cannot fail");
    }
    s
}

/// Produces the same dump the Go harness does, from roble's reader alone.
fn dump(name: &str, table_bytes: &[u8]) -> String {
    let table = Table::open(table_bytes).unwrap_or_else(|e| panic!("{name}: open: {e}"));
    let footer = *table.footer();

    let data_blocks = table
        .data_block_handles()
        .unwrap_or_else(|e| panic!("{name}: read index: {e}"))
        .len();
    // The index walk and the properties block have to agree about how many
    // data blocks there are: a reader that stopped after the first block would
    // still produce a well-formed, shorter dump, and this catches that before
    // the entry diff does.
    if let Some(claimed) = table
        .properties()
        .num_data_blocks()
        .expect("num data blocks")
    {
        assert_eq!(
            claimed as usize, data_blocks,
            "properties claim {claimed} data blocks, index walk found {data_blocks}"
        );
    }

    let mut entries = Vec::new();
    table
        .for_each_entry(|key, value| {
            let bytes = value
                .in_place()
                .expect("fixtures hold no value blocks, as the harness asserts");
            entries.push(format!(
                "entry {:016x} {} {} {}",
                key.seq_num,
                key.kind.name(),
                hex(key.user_key),
                hex(bytes)
            ));
            Ok(())
        })
        .unwrap_or_else(|e| panic!("{name}: walk entries: {e}"));

    let mut out = String::new();
    writeln!(out, "format {}", footer.format.name()).unwrap();
    writeln!(out, "checksum {}", footer.checksum.name()).unwrap();
    writeln!(out, "datablocks {data_blocks}").unwrap();
    writeln!(out, "entries {}", entries.len()).unwrap();
    for e in entries {
        writeln!(out, "{e}").unwrap();
    }
    out
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

        let actual = dump(name, &bytes);
        if actual != expected {
            let diff = first_difference(&expected, &actual);
            panic!("{name}: roble's dump differs from Go Pebble's\n{diff}");
        }
    }
}

/// The first differing line, which is far more use than two 400-line dumps.
fn first_difference(expected: &str, actual: &str) -> String {
    let mut out = String::new();
    for (i, (e, a)) in expected.lines().zip(actual.lines()).enumerate() {
        if e != a {
            writeln!(out, "  line {}:", i + 1).unwrap();
            writeln!(out, "    go:    {e}").unwrap();
            writeln!(out, "    roble: {a}").unwrap();
            return out;
        }
    }
    let (e, a) = (expected.lines().count(), actual.lines().count());
    writeln!(out, "  go produced {e} lines, roble {a}").unwrap();
    if a > e {
        for line in actual.lines().skip(e).take(3) {
            writeln!(out, "    roble only: {line}").unwrap();
        }
    } else {
        for line in expected.lines().skip(a).take(3) {
            writeln!(out, "    go only:    {line}").unwrap();
        }
    }
    out
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
