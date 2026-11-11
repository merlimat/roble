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

//! Scanning, judged against Pebble's iterator.
//!
//! `harness/read-probes` walks the same database end to end in both
//! directions, bounded and unbounded, and then — the part a seek corpus
//! cannot reach — seeks and *steps*: three forward, three back, and a turn
//! that goes one step forward and one back again. A merging iterator's real
//! failure is a seek that lands correctly and then cannot be stepped away
//! from, or a direction change that loses its place.
//!
//! This reproduces that file from roble's own scan and compares the whole
//! text.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, Scan, WriteBuf};

async fn load(name: &str) -> (SimEnv, PathBuf) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let fs: &SimFs = env.sim();
    fs.mkdir_all(&dir).await.expect("mkdir");
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        let bytes = std::fs::read(entry.path()).expect("read file");
        let disk = fs
            .create(&dir.join(entry.file_name()))
            .await
            .expect("create");
        let (result, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
        result.expect("write");
        disk.close().await.expect("close");
    }
    (env, dir)
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

/// Where a scan is, as the harness spells it.
fn where_it_is<D: Disk>(scan: &Scan<D>) -> String {
    if !scan.is_valid() {
        return "none".to_string();
    }
    format!("{} {}", hex(scan.key()), hex(scan.value()))
}

#[compio::test]
async fn roble_scans_where_go_pebble_scans() {
    let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let path = golden.join("db-reads.scans");
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{}: {e}; regenerate with 'cd harness && go run ./read-probes ../tests/golden'",
            path.display()
        )
    });

    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let mut out = String::new();

    // The four whole-database walks.
    for (name, lower, upper, forward) in [
        ("forward", None, None, true),
        ("reverse", None, None, false),
        ("bounded-forward", Some(&b"b2"[..]), Some(&b"d3"[..]), true),
        ("bounded-reverse", Some(&b"b2"[..]), Some(&b"d3"[..]), false),
    ] {
        out.push_str(&format!("{name}\n"));
        let mut scan = db.scan_range(lower, upper).await.expect("scan");
        if forward {
            scan.first().await.expect("first");
        } else {
            scan.last().await.expect("last");
        }
        while scan.is_valid() {
            out.push_str(&format!("  {} {}\n", hex(scan.key()), hex(scan.value())));
            if forward {
                scan.next().await.expect("next");
            } else {
                scan.prev().await.expect("prev");
            }
        }
    }

    // Seeks that fall outside the bounds, which a walk never makes.
    {
        let mut scan = db
            .scan_range(Some(&b"b2"[..]), Some(&b"d3"[..]))
            .await
            .expect("scan");
        out.push_str("bounded-seeks\n");
        for (name, seek_ge, key) in [
            ("below", true, &b"a0"[..]),
            ("above", false, &b"zz"[..]),
            ("at-lower", true, &b"b2"[..]),
            ("at-upper", false, &b"d3"[..]),
        ] {
            if seek_ge {
                scan.seek_ge(key).await.expect("seek_ge");
            } else {
                scan.seek_lt(key).await.expect("seek_lt");
            }
            out.push_str(&format!("  {name} {}\n", where_it_is(&scan)));
        }
    }

    // Seek, then step. The probe list is taken from the file itself.
    let probes: Vec<Vec<u8>> = expected
        .lines()
        .filter_map(|l| l.strip_prefix("from "))
        .map(unhex)
        .collect();
    assert!(!probes.is_empty(), "no probes in {}", path.display());

    let mut scan = db.scan().await.expect("scan");
    for probe in &probes {
        out.push_str(&format!("from {}\n", hex(probe)));

        scan.seek_ge(probe).await.expect("seek_ge");
        for _ in 0..3 {
            out.push_str(&format!("  fwd {}\n", where_it_is(&scan)));
            if !scan.is_valid() {
                break;
            }
            scan.next().await.expect("next");
        }

        scan.seek_lt(probe).await.expect("seek_lt");
        for _ in 0..3 {
            out.push_str(&format!("  rev {}\n", where_it_is(&scan)));
            if !scan.is_valid() {
                break;
            }
            scan.prev().await.expect("prev");
        }

        // One forward, then back again: the direction change.
        scan.seek_ge(probe).await.expect("seek_ge");
        if scan.is_valid() {
            scan.next().await.expect("next");
        }
        if scan.is_valid() {
            scan.prev().await.expect("prev");
        }
        out.push_str(&format!("  turn {}\n", where_it_is(&scan)));

        // And the other way round, which is the only path through the
        // re-seek `next` does after a reverse step.
        scan.seek_lt(probe).await.expect("seek_lt");
        if scan.is_valid() {
            scan.prev().await.expect("prev");
        }
        if scan.is_valid() {
            scan.next().await.expect("next");
        }
        out.push_str(&format!("  turn-back {}\n", where_it_is(&scan)));
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
        let from = at.saturating_sub(8);
        panic!(
            "scans differ at line {}:\n  roble:  {:?}\n  pebble: {:?}\ncontext:\n{}",
            at + 1,
            a.get(at),
            b.get(at),
            b[from..(at + 2).min(b.len())].join("\n")
        );
    }
}
