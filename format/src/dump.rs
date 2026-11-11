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

//! A canonical textual dump of a table, which both roble and the Go harness
//! produce so that the two can be diffed (DESIGN.md §12).
//!
//! The format is deliberately dull and line-oriented, so that a mismatch
//! points at one line rather than at a blob:
//!
//! ```text
//!   format pebblev4
//!   checksum crc32c
//!   datablocks 3
//!   entries 42
//!   tombstones 2
//!   entry 000000000000000a set 2f61 76616c7565
//!   tombstone 0000000000000005 2f61 2f62
//! ```
//!
//! `entries` counts point entries only. Pebble's own `rocksdb.num.entries`
//! includes range deletions, which is a different number and deliberately not
//! the one dumped here.
//!
//! Sequence numbers are 16 hex digits, keys and values hex, kinds the names in
//! [`crate::Kind::name`]. This is the output `roble sst dump` will print.

use std::fmt::Write as _;

use crate::block::Value;
use crate::error::{Error, Result};
use crate::sst::Table;

/// Dumps a table's structure and every point entry.
///
/// Cross-checks the index walk against `rocksdb.num.data.blocks`: a reader
/// that stopped early would otherwise produce a well-formed, shorter dump, and
/// a dump that disagrees with the table's own properties is not worth printing.
pub fn dump_table(bytes: &[u8]) -> Result<String> {
    let table = Table::open(bytes)?;
    let footer = *table.footer();
    let data_blocks = table.data_block_handles()?.len();

    if let Some(claimed) = table.properties().num_data_blocks()?
        && claimed as usize != data_blocks
    {
        // Untested, with a reason rather than a test (§12). Every writer
        // counts the blocks it wrote, so the two disagreeing needs a table
        // whose properties were edited after its checksum was computed, or
        // the block check fires first. It is here because a dump is read as
        // evidence, and a dump that quietly disagreed with the file's own
        // account of itself would be evidence of the wrong thing.
        return Error::corrupt(format!(
            "properties claim {claimed} data blocks, the index walk found {data_blocks}"
        ));
    }

    let tombstones = table.range_deletions()?;

    // Decoded once rather than per value: a table with value blocks has few
    // of them, but the index would otherwise be re-read for every entry.
    let value_blocks = table.value_blocks()?;

    let mut entries: Vec<String> = Vec::new();
    table.for_each_entry(|key, value| {
        let resolved;
        let bytes = match value {
            Value::InPlace(v) => v,
            Value::ValueHandle(h) => {
                resolved = table.resolve_value(&value_blocks, h)?;
                &resolved
            }
            // Untested, with a reason (§12), and the reason is that an
            // earlier refusal gets there first: a blob handle arrives with
            // Pebblev6, and the footer refuses anything above v4 before a
            // block is read. The backstop stays for the day the pin moves.
            other => {
                return Error::unsupported(format!(
                    "{} values arrive above roble's pin and cannot be dumped",
                    other.kind_name()
                ));
            }
        };
        let mut line = String::new();
        write!(
            line,
            "entry {:016x} {} {} {}",
            key.seq_num,
            key.kind.name(),
            hex(key.user_key),
            hex(bytes)
        )
        .expect("writing to a String cannot fail");
        entries.push(line);
        Ok(())
    })?;

    let mut out = String::new();
    writeln!(out, "format {}", footer.format.name()).expect("String");
    writeln!(out, "checksum {}", footer.checksum.name()).expect("String");
    writeln!(out, "datablocks {data_blocks}").expect("String");
    writeln!(out, "entries {}", entries.len()).expect("String");
    writeln!(out, "tombstones {}", tombstones.len()).expect("String");
    for e in entries {
        writeln!(out, "{e}").expect("String");
    }
    for t in &tombstones {
        writeln!(
            out,
            "tombstone {:016x} {} {}",
            t.seq_num,
            hex(&t.start),
            hex(&t.end)
        )
        .expect("String");
    }
    Ok(out)
}

/// The first differing line between two dumps, which is far more use than two
/// several-hundred-line blobs.
pub fn first_difference(expected: &str, actual: &str) -> Option<String> {
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

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").expect("writing to a String cannot fail");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two dumps that agree have no first difference.
    ///
    /// This is the only path `golden.rs` takes while it passes, which is why
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
