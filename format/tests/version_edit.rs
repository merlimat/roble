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

//! Decoding version edits, judged against Pebble's own test cases.
//!
//! `tests/pebble/version_edit_decode` is Pebble's
//! `internal/manifest/testdata/version_edit_decode` at the pin, unchanged
//! (`tests/pebble/README.md`). Each `decode` gives a MANIFEST record as
//! annotated hex and prints what Pebble's `VersionEdit.Decode` reads from it.
//! roble's `VersionEdit::decode` is what an open replays a MANIFEST with, so it
//! has to read the same fields from the same bytes: the last of a repeated
//! tag, and a table's creation time. The two edits of blob files, which
//! arrive above roble's pin (DESIGN.md §3), have to be refused, by name.
//!
//! Each edit roble reads is also encoded again and has to decode to the same
//! edit, which is the round trip Pebble's test checks of its own. The fields
//! are compared, not Pebble's text: it leaves out a field that is zero, which
//! roble holds as zero or as absent alike.
//!
//! Skipped: the `encode` case, which writes an edit of blob files.

use std::path::Path;

use roble_format::InternalKey;
use roble_format::key::{Kind, SEQ_NUM_MAX};
use roble_format::manifest::VersionEdit;

/// One command of a datadriven file: the command line, its input lines, and
/// the output written under it.
struct Case {
    line: usize,
    command: String,
    input: Vec<String>,
    expected: Vec<String>,
}

/// Splits a file in the format of cockroachdb/datadriven, the one Pebble's
/// test data is written in. An output runs to the first blank line.
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
        while i < lines.len() && !lines[i].trim().is_empty() {
            expected.push(lines[i].to_string());
            i += 1;
        }
        out.push(Case {
            line: at,
            command,
            input,
            expected,
        });
    }
    out
}

/// The bytes an annotated input spells, read as Pebble's test reads them:
/// each line up to a `#`, its whitespace dropped, is a quoted string or hex.
fn annotated(input: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    for line in input {
        let code: String = line
            .split('#')
            .next()
            .unwrap_or("")
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        match code.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
            Some(quoted) => out.extend_from_slice(quoted.as_bytes()),
            None => out.extend(unhex(&code)),
        }
    }
    out
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(
        s.len().is_multiple_of(2),
        "an odd number of hex digits: {s}"
    );
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// An internal key as Pebble prints one, `user#seq,KIND`.
fn spelled(encoded: &[u8]) -> String {
    let k = InternalKey::decode(encoded).expect("a key");
    let seq = match k.seq_num {
        SEQ_NUM_MAX => "inf".to_string(),
        n => n.to_string(),
    };
    let kind = match k.kind {
        Kind::Set => "SET",
        Kind::Delete => "DEL",
        Kind::RangeDelete => "RANGEDEL",
        other => panic!("a kind this test does not spell: {other:?}"),
    };
    format!("{}#{seq},{kind}", k.user_key.escape_ascii())
}

/// Unix seconds of a time Pebble prints as `2023-12-04T17:57:24Z`, worked out
/// by the civil-calendar arithmetic, there being no date library here.
fn unix_seconds(at: &str) -> u64 {
    let (date, time) = at
        .strip_suffix('Z')
        .and_then(|t| t.split_once('T'))
        .expect("a UTC time");
    let n = |s: &str| -> i64 { s.parse().expect("a number") };
    let ymd: Vec<i64> = date.split('-').map(n).collect();
    let hms: Vec<i64> = time.split(':').map(n).collect();
    let (y, m, d) = (ymd[0], ymd[1], ymd[2]);
    // Days since 1970-01-01, counting years from March so that the leap day
    // ends one.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    u64::try_from(days * 86_400 + hms[0] * 3_600 + hms[1] * 60 + hms[2]).expect("after 1970")
}

/// The fields roble's edit holds, printed one per line as Pebble's
/// `VersionEdit.String` prints them, leaving out what is zero or absent.
fn fields(edit: &VersionEdit) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(name) = &edit.comparer_name {
        out.push(format!("comparer: {name}"));
    }
    for (name, value) in [
        ("log-num", edit.min_unflushed_log_num),
        ("prev-log-num", edit.obsolete_prev_log_num),
        ("next-file-num", edit.next_file_num),
        ("last-seq-num", edit.last_seq_num),
    ] {
        if let Some(n) = value.filter(|&n| n != 0) {
            out.push(format!("{name}: {n}"));
        }
    }
    for (level, file_num) in &edit.deleted_tables {
        out.push(format!("del-table: L{level} {file_num:06}"));
    }
    for t in &edit.new_tables {
        let mut line = format!(
            "add-table: L{} {:06}:[{}-{}]",
            t.level,
            t.file_num,
            spelled(&t.smallest),
            spelled(&t.largest)
        );
        if t.creation_time != 0 {
            line.push_str(&format!(" ({})", t.creation_time));
        }
        out.push(line);
    }
    out
}

/// Pebble's printed fields, spaced as roble's are and with a creation time
/// as Unix seconds.
fn pebbles_fields(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .map(|l| {
            let (name, value) = l.trim().split_once(':').expect("name: value");
            let mut value = value.trim().to_string();
            if let Some(open) = value.rfind(" (")
                && value.ends_with("Z)")
            {
                let at = unix_seconds(&value[open + 2..value.len() - 1]);
                value = format!("{} ({at})", &value[..open]);
            }
            format!("{name}: {value}")
        })
        .collect()
}

#[test]
fn roble_decodes_the_version_edits_pebble_decodes() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pebble/version_edit_decode");
    let text = std::fs::read_to_string(&path).expect("Pebble's version edit test data");

    let (mut read, mut refused, mut skipped) = (0, 0, 0);
    for case in cases(&text) {
        let context = format!("line {}", case.line);
        if case.command != "decode" {
            assert_eq!(case.command, "encode", "{context}");
            skipped += 1;
            continue;
        }
        let bytes = annotated(&case.input);
        // Pebble prints the bytes it read, thirty to a line, then the fields.
        let shown = case.expected.iter().take_while(|l| !l.starts_with(' '));
        let printed: String = shown.clone().map(|l| l.trim()).collect();
        assert_eq!(
            hex(&bytes),
            printed,
            "{context}: the bytes the input spells"
        );
        let printed_fields = &case.expected[shown.count()..];

        let decoded = VersionEdit::decode(&bytes);
        if printed_fields.iter().any(|l| l.contains("blob")) {
            let err = decoded.expect_err(&format!("{context}: an edit of blob files"));
            assert!(
                err.to_string().contains("blob"),
                "{context}: refused, but not for its blob files: {err}"
            );
            refused += 1;
            continue;
        }
        let edit = decoded.unwrap_or_else(|e| panic!("{context}: {e}"));
        assert_eq!(fields(&edit), pebbles_fields(printed_fields), "{context}");
        assert_eq!(
            VersionEdit::decode(&edit.encode()).expect("its own encoding"),
            edit,
            "{context}: the edit encoded again"
        );
        read += 1;
    }
    assert_eq!(
        (read, refused, skipped),
        (3, 2, 1),
        "the cases of {}",
        path.display()
    );
}
