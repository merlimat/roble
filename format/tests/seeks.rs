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

//! Seeks, judged against Pebble's own iterator.
//!
//! `harness/seek-probes` runs `SeekGE` and `SeekLT` over every table in the
//! golden corpus and writes down what Pebble answers. This regenerates that
//! file from roble's own seeks, using only the probe keys from it, and
//! compares the whole text. A full scan — which `golden.rs` already pins —
//! cannot catch a seek that lands on the wrong side of a block boundary, or
//! one that is right but cannot be stepped away from; this can.

use std::path::{Path, PathBuf};

use roble_format::block::Value;
use roble_format::sst::Table;
use roble_format::{InternalKey, key};

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd-length hex {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Spells one seek result exactly as `harness/seek-probes` does.
fn render(table: &Table<'_>, key: InternalKey<'_>, value: Value<'_>) -> String {
    // A value stored in a value block has to be resolved before it can be
    // compared, since Pebble's iterator hands back the resolved bytes.
    let resolved: Vec<u8> = match value {
        Value::InPlace(b) => b.to_vec(),
        Value::ValueHandle(h) => {
            let blocks = table.value_blocks().expect("value blocks");
            table.resolve_value(&blocks, h).expect("resolve value")
        }
        Value::BlobHandle(_) => panic!("a blob handle is above roble's pin"),
    };
    const LEAD: usize = 8;
    let head = &resolved[..resolved.len().min(LEAD)];
    format!(
        "{} #{} {} v={}:{}",
        hex(key.user_key),
        key.seq_num,
        key.kind.name(),
        resolved.len(),
        hex(head)
    )
}

#[test]
fn roble_seeks_where_go_pebble_seeks() {
    let dir = golden_dir();
    let path = dir.join("seeks.txt");
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{}: {e}; regenerate with 'cd harness && go run ./seek-probes ../format/tests/golden'",
            path.display()
        )
    });

    // The probe keys are input, taken from the file; the answers are what this
    // test produces. Parsed first so each table is opened once.
    let mut sections: Vec<(String, Vec<Vec<u8>>)> = Vec::new();
    for line in expected.lines() {
        if let Some(name) = line.strip_prefix("fixture ") {
            sections.push((name.to_string(), Vec::new()));
        } else if let Some(probe) = line.strip_prefix("probe ") {
            sections
                .last_mut()
                .expect("a probe before any fixture")
                .1
                .push(unhex(probe));
        }
    }
    assert!(!sections.is_empty(), "no fixtures in {}", path.display());

    let mut out = String::new();
    for (name, probes) in &sections {
        out.push_str(&format!("fixture {name}\n"));
        let bytes = std::fs::read(dir.join(format!("{name}.sst"))).expect("read fixture");
        let table = Table::open(&bytes).expect("open fixture");

        for probe in probes {
            out.push_str(&format!("probe {}\n", hex(probe)));
            // Pebble's SeekGE and SeekLT take a user key; the internal key
            // that finds it is the search key, whose trailer sorts before
            // every real version of that user key.
            let target = key::search_key(probe);

            let mut line = "none".to_string();
            table
                .seek_ge(&target, |k, v| {
                    line = render(&table, k, v);
                    Ok(())
                })
                .expect("seek_ge");
            out.push_str(&format!("  ge {line}\n"));

            let mut line = "none".to_string();
            table
                .seek_lt(&target, |k, v| {
                    line = render(&table, k, v);
                    Ok(())
                })
                .expect("seek_lt");
            out.push_str(&format!("  lt {line}\n"));
        }
    }

    if out != expected {
        let (a, b) = (
            out.lines().collect::<Vec<_>>(),
            expected.lines().collect::<Vec<_>>(),
        );
        let at = a
            .iter()
            .zip(&b)
            .position(|(x, y)| x != y)
            .unwrap_or(a.len().min(b.len()));
        let from = at.saturating_sub(4);
        panic!(
            "seeks differ at line {}:\n  roble: {:?}\n  pebble: {:?}\ncontext:\n{}",
            at + 1,
            a.get(at),
            b.get(at),
            b[from..(at + 2).min(b.len())].join("\n")
        );
    }
}
