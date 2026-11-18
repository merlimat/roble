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

//! Fragmenting range deletions, judged against Pebble's own test cases.
//!
//! `tests/pebble/fragmenter` is Pebble's `internal/keyspan/testdata/fragmenter`
//! at the pin, unchanged (`tests/pebble/README.md`). Each `build` gives
//! tombstones as `seq: start---end` and the fragments Pebble's `Fragmenter`
//! emits for them, in the order it emits them. roble's `rangedel::fragment`
//! is what a compaction builds its range-deletion block with, so the two
//! have to agree fragment for fragment: a block of different fragments is
//! one Pebble's reader walks differently.
//!
//! The drawing is not compared, only what it draws: the sequence number and
//! the two bounds of each line, read the way Pebble's test reads its input.

use std::path::Path;

use roble_format::rangedel::{Tombstone, fragment};

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

/// A line as Pebble's `spanRe` reads it, `(\d+):\s*(\w+)-*(\w+)\w*(.*)`:
/// the sequence number, then the start and end keys. Two letters with no
/// dash between them, `ab`, are a start and an end; whatever follows the
/// end, after a space, is a value, which a range deletion does not have.
fn span(line: &str) -> (u64, String, String, String) {
    let (seq, rest) = line.split_once(':').expect("a span is seq: start-end");
    let seq = seq.trim().parse().expect("a sequence number");
    let rest = rest.trim_start();
    let word = |s: &str| {
        s.find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(s.len())
    };
    let first = word(rest);
    let after = rest[first..].trim_start_matches('-');
    let (start, end, tail) = if after.len() < rest.len() - first {
        // Dashes between the two: each is a word of its own.
        let second = word(after);
        (&rest[..first], &after[..second], &after[second..])
    } else {
        // None, so the regex's first group gives the last character up.
        (&rest[..first - 1], &rest[first - 1..first], &rest[first..])
    };
    (
        seq,
        start.to_string(),
        end.to_string(),
        tail.trim().to_string(),
    )
}

#[test]
fn roble_fragments_as_pebbles_fragmenter_does() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pebble/fragmenter");
    let text = std::fs::read_to_string(&path).expect("Pebble's fragmenter test data");

    let mut built = 0;
    for case in cases(&text) {
        if case.command != "build" {
            continue;
        }
        // Pebble's `Fragmenter.Add` refuses tombstones out of start order
        // and the case pins that it does. `fragment` sorts what it is given,
        // since a compaction gathers tombstones from several tables, so
        // there is nothing of roble's for that case to judge.
        if case
            .expected
            .starts_with("pebble: keys must be added in order")
        {
            continue;
        }
        let input: Vec<Tombstone> = case
            .input
            .iter()
            .map(|l| {
                let (seq_num, start, end, value) = span(l);
                assert!(
                    value.is_empty(),
                    "line {}: a range deletion with a value",
                    case.line
                );
                Tombstone {
                    start: start.into_bytes(),
                    end: end.into_bytes(),
                    seq_num,
                }
            })
            .collect();
        let want: Vec<(u64, String, String)> = case
            .expected
            .lines()
            .map(|l| {
                let (seq, start, end, _) = span(l);
                (seq, start, end)
            })
            .collect();

        let got: Vec<(u64, String, String)> = fragment(&input.iter().collect::<Vec<_>>())
            .into_iter()
            .map(|t| {
                (
                    t.seq_num,
                    String::from_utf8(t.start).expect("text"),
                    String::from_utf8(t.end).expect("text"),
                )
            })
            .collect();
        assert_eq!(
            got,
            want,
            "line {}: the fragments of\n{}",
            case.line,
            case.input.join("\n")
        );
        built += 1;
    }
    // Every build but the refusal, so a parser that lost cases fails here.
    assert_eq!(built, 17, "the builds of {}", path.display());
}
