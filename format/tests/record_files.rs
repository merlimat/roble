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

//! roble's record framing, against files Go Pebble really wrote.
//!
//! `harness/record-files` opens a database with its WAL enabled and copies out
//! the resulting `.log` — whose chunks are recyclable and carry a log number —
//! and `MANIFEST-*`, whose chunks are legacy. Pebble's own `record.NewReader`
//! and `record.NewWriter` cannot be driven from outside the module, since
//! their log-number parameter is `base.DiskFileNum`, so these real files take
//! the place of a Go-side harness.
//!
//! Two directions are checked against them: roble decodes every record, and
//! roble re-frames the same payloads into the same bytes.

use std::path::{Path, PathBuf};

use roble_format::record::{BLOCK_SIZE, RecordReader, RecordWriter};

fn golden() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

/// The WAL's file name and log number, from the harness's index.
fn wal_log_num() -> u32 {
    let index = std::fs::read_to_string(golden().join("records.txt"))
        .unwrap_or_else(|e| panic!("records.txt: {e}; regenerate with harness/record-files"));
    for line in index.lines() {
        if let Some(rest) = line.strip_prefix("wal ") {
            for field in rest.split_whitespace() {
                if let Some(n) = field.strip_prefix("lognum=") {
                    return n.parse().expect("lognum");
                }
            }
        }
    }
    panic!("records.txt has no wal line");
}

fn read_records(bytes: &[u8], log_num: u32) -> Vec<Vec<u8>> {
    let mut r = RecordReader::new(bytes, log_num);
    let mut out = Vec::new();
    while let Some(rec) = r.next() {
        out.push(rec.unwrap_or_else(|e| panic!("record {}: {e}", out.len())));
    }
    out
}

#[test]
fn roble_reads_the_wal_go_pebble_wrote() {
    let bytes = std::fs::read(golden().join("records-wal.log")).expect("read the WAL");
    let records = read_records(&bytes, wal_log_num());

    assert!(
        records.len() >= 20,
        "expected the fixture to hold many records, got {}",
        records.len()
    );
    assert!(
        bytes.len() > 3 * BLOCK_SIZE,
        "the fixture should span several blocks, it is {} bytes",
        bytes.len()
    );
    // One record is a 200 KiB batch, so at least one must span blocks.
    assert!(
        records.iter().any(|r| r.len() > BLOCK_SIZE),
        "no record spans a block boundary, so multi-chunk records are untested"
    );

    // Every record in a WAL is a batch representation, which roble can parse:
    // this is the framing and the batch repr agreeing with each other on real
    // Pebble output rather than only on roble's own.
    for (i, rec) in records.iter().enumerate() {
        let mut r = roble_format::batch::Batch::read(rec)
            .unwrap_or_else(|e| panic!("record {i} is not a batch repr: {e}"));
        let mut entries = 0;
        while let Some(e) = r.next() {
            e.unwrap_or_else(|e| panic!("record {i}, entry {entries}: {e}"));
            entries += 1;
        }
        assert!(entries > 0, "record {i} decoded to an empty batch");
    }
}

#[test]
fn roble_reframes_the_wal_into_the_same_bytes() {
    let log_num = wal_log_num();
    let bytes = std::fs::read(golden().join("records-wal.log")).expect("read the WAL");
    let records = read_records(&bytes, log_num);

    let mut w = RecordWriter::recyclable(log_num);
    for rec in &records {
        w.write_record(rec).expect("write");
    }
    // A closed log ends with Pebble's EOF trailer, so re-framing has to write
    // one too for the bytes to match.
    w.write_eof_trailer().expect("eof trailer");
    let reframed = w.finish();

    compare("records-wal.log", &bytes, &reframed);
}

#[test]
fn roble_reads_and_reframes_the_manifest_go_pebble_wrote() {
    let bytes = std::fs::read(golden().join("records-manifest")).expect("read the MANIFEST");
    // A MANIFEST's chunks are legacy and carry no log number; the value is
    // passed anyway so a file that turned out recyclable would be checked.
    let records = read_records(&bytes, 0);
    assert!(
        records.len() > 1,
        "expected several version edits, got {}",
        records.len()
    );

    let mut w = RecordWriter::legacy();
    for rec in &records {
        w.write_record(rec).expect("write");
    }
    compare("records-manifest", &bytes, &w.finish());
}

/// Compares two framings, reporting the first difference in context.
///
/// Pebble pads a block's tail with zeros once it cannot hold another header,
/// so a re-framing may stop short of the original's final padding; every byte
/// they both have must match exactly.
fn compare(name: &str, original: &[u8], reframed: &[u8]) {
    let common = original.len().min(reframed.len());
    if let Some(at) = (0..common).find(|&i| original[i] != reframed[i]) {
        let from = at.saturating_sub(8);
        panic!(
            "{name}: re-framing differs at byte {at} (block {}, offset {} within it)\n  \
             go:    {:02x?}\n  roble: {:02x?}",
            at / BLOCK_SIZE,
            at % BLOCK_SIZE,
            &original[from..(at + 8).min(original.len())],
            &reframed[from..(at + 8).min(reframed.len())],
        );
    }
    if original.len() != reframed.len() {
        let (longer, label) = if original.len() > reframed.len() {
            (&original[common..], "go")
        } else {
            (&reframed[common..], "roble")
        };
        assert!(
            longer.iter().all(|&b| b == 0),
            "{name}: {label} has {} extra bytes and they are not all padding",
            longer.len()
        );
    }
}
