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

//! roble's batch representation must be byte-identical to Go Pebble's.
//!
//! Not "equivalent": identical. Oxia checksums the whole repr, header
//! included, and compares the checksum across replicas, so one differing byte
//! is a replica mismatch rather than a format error (DESIGN.md §6).
//!
//! The fixture is built by `harness/batch-repr`, which runs the same operation
//! sequences through Go Pebble and dumps the uncommitted reprs. Regenerate
//! with `cd harness && go run ./batch-repr ../format/tests/golden/batches.txt`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble_format::batch::Batch;

fn golden() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/batches.txt")
}

/// `name -> (count, repr)` as Go Pebble produced them.
fn go_batches() -> BTreeMap<String, (u32, Vec<u8>)> {
    let text = std::fs::read_to_string(golden()).unwrap_or_else(|e| {
        panic!(
            "{}: {e}; regenerate with harness/batch-repr",
            golden().display()
        )
    });
    let mut out = BTreeMap::new();
    let mut pending: Option<(String, u32, usize)> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("batch ") {
            let mut parts = rest.split_whitespace();
            let name = parts.next().expect("batch name").to_string();
            let count: u32 = parts
                .next()
                .and_then(|p| p.strip_prefix("count="))
                .and_then(|n| n.parse().ok())
                .expect("count=");
            let len: usize = parts
                .next()
                .and_then(|p| p.strip_prefix("len="))
                .and_then(|n| n.parse().ok())
                .expect("len=");
            pending = Some((name, count, len));
        } else if let Some(hex) = line.strip_prefix("repr ") {
            let (name, count, len) = pending.take().expect("repr without a batch line");
            let bytes = unhex(hex);
            assert_eq!(
                bytes.len(),
                len,
                "{name}: declared length disagrees with the hex"
            );
            out.insert(name, (count, bytes));
        }
    }
    assert!(!out.is_empty(), "the fixture holds no batches");
    out
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd-length hex");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex digit"))
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Rebuilds each of the harness's programs with roble.
///
/// Kept deliberately parallel to `harness/batch-repr`'s `programs()`: if the
/// two drift, the names stop matching and the test says so rather than
/// quietly checking fewer cases.
fn roble_batch(name: &str) -> Option<Batch> {
    let mut b = Batch::new();
    let long = vec![b'v'; 300];
    match name {
        "empty" => {}
        "one_set" => b.put(b"/a", b"1").unwrap(),
        "one_delete" => b.delete(b"/a").unwrap(),
        "set_empty_value" => b.put(b"/a", b"").unwrap(),
        "empty_key" => b.put(b"", b"x").unwrap(),
        "mixed" => {
            b.put(b"/users/alice", b"a").unwrap();
            b.delete(b"/users/bob").unwrap();
            b.put(b"/users/carol", &long).unwrap();
            b.delete_range(b"/sessions/", b"/sessions0").unwrap();
            b.single_delete(b"/tmp/x").unwrap();
        }
        "varint_boundary" => {
            b.put(&[b'k'; 127], &[b'v'; 128]).unwrap();
            b.put(&[b'K'; 128], &[b'V'; 127]).unwrap();
            b.put(&[b'q'; 300], &[b'w'; 16384]).unwrap();
        }
        "high_bytes" => {
            b.put(b"\xff\xfe\x00\x80", b"\x00\xff\x7f\x80").unwrap();
            b.delete_range(b"\x00", b"\xff\xff").unwrap();
        }
        "many" => {
            for i in 0..200 {
                let key = format!("/oxia/default/shard-0001/users/{i:08}");
                let value = format!("{{\"version\":{i}}}");
                b.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }
        _ => return None,
    }
    Some(b)
}

#[test]
fn robles_batch_repr_is_byte_identical_to_go_pebbles() {
    let go = go_batches();
    let mut checked = 0;
    for (name, (count, expected)) in &go {
        let b = roble_batch(name)
            .unwrap_or_else(|| panic!("no roble program for the fixture's {name:?} batch"));
        assert_eq!(b.count(), *count, "{name}: entry count");
        if b.as_bytes() != expected.as_slice() {
            let first = b.as_bytes().iter().zip(expected).position(|(a, c)| a != c);
            panic!(
                "{name}: repr differs from Go Pebble's\n  first difference at {first:?}\n  \
                 roble ({} bytes): {}\n  go    ({} bytes): {}",
                b.len(),
                hex(&b.as_bytes()[..b.len().min(64)]),
                expected.len(),
                hex(&expected[..expected.len().min(64)])
            );
        }
        checked += 1;
    }
    assert_eq!(checked, go.len());
    assert!(checked >= 9, "expected the fixture to cover more cases");
}

#[test]
fn roble_reads_every_batch_go_pebble_wrote() {
    for (name, (count, repr)) in go_batches() {
        let r = Batch::read(&repr).unwrap_or_else(|e| panic!("{name}: read: {e}"));
        let mut seen = 0u32;
        for e in r {
            e.unwrap_or_else(|e| panic!("{name}: entry {seen}: {e}"));
            seen += 1;
        }
        assert_eq!(
            seen, count,
            "{name}: read {seen} entries, header said {count}"
        );
    }
}

/// A repr that roble built must survive a round trip through its own reader,
/// and a sequence number stamped at commit must not disturb the entries.
#[test]
fn stamping_a_sequence_number_changes_only_the_header() {
    let (_, repr) = go_batches().remove("mixed").expect("the mixed fixture");
    let mut b = roble_batch("mixed").expect("program");
    assert_eq!(b.as_bytes(), repr.as_slice());

    b.set_seq_num(0x0102_0304_0506);
    assert_eq!(b.seq_num(), 0x0102_0304_0506);
    assert_eq!(
        &b.as_bytes()[12..],
        &repr[12..],
        "stamping the sequence number must not move the entries"
    );
    assert_eq!(b.count(), 5, "nor change the count");
}
