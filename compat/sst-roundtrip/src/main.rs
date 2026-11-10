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

//! Writes sstables with roble and dumps each one, so that `harness/verify-sst`
//! can have Go Pebble read them and `run.sh` can diff the two dumps.
//!
//! The fixtures are chosen for the parts of the format a Go reader is most
//! likely to disagree about, rather than for coverage of roble's own code.

use std::path::Path;

use roble_format::sst::{TableWriter, WriterOptions};
use roble_format::{InternalKey, Kind, dump_table};

struct Fixture {
    name: &'static str,
    block_size: usize,
    entries: Vec<(Vec<u8>, u64, Kind, Vec<u8>)>,
}

fn oxia_key(shard: u32, i: u32) -> Vec<u8> {
    format!("/oxia/default/shard-{shard:04}/users/{i:08}").into_bytes()
}

fn fixtures() -> Vec<Fixture> {
    let mut out = Vec::new();

    // One data block, small enough to read by eye in a hex dump.
    out.push(Fixture {
        name: "simple",
        block_size: 4096,
        entries: (0..6u32)
            .map(|i| {
                (
                    format!("/k{i}").into_bytes(),
                    10 + u64::from(i),
                    Kind::Set,
                    format!("value-{i}").into_bytes(),
                )
            })
            .collect(),
    });

    // Every point kind roble writes, including a value of zero length and one
    // larger than a block would like.
    out.push(Fixture {
        name: "kinds",
        block_size: 4096,
        entries: vec![
            (b"/a".to_vec(), 100, Kind::Set, b"set".to_vec()),
            (b"/b".to_vec(), 101, Kind::Delete, Vec::new()),
            (b"/c".to_vec(), 102, Kind::SetWithDelete, b"swd".to_vec()),
            (b"/d".to_vec(), 103, Kind::SingleDelete, Vec::new()),
            (b"/e".to_vec(), 104, Kind::DeleteSized, Vec::new()),
            (b"/f".to_vec(), 105, Kind::Set, Vec::new()),
            (b"/g".to_vec(), 106, Kind::Set, vec![b'x'; 5000]),
        ],
    });

    // Many small blocks, so the index has to be right rather than incidental.
    out.push(Fixture {
        name: "multiblock",
        block_size: 512,
        entries: (0..400u32)
            .map(|i| {
                (
                    oxia_key(3, i),
                    1000 + u64::from(i),
                    Kind::Set,
                    format!("{{\"version\":{i}}}").into_bytes(),
                )
            })
            .collect(),
    });

    // The same user key at several sequence numbers. This is the case that
    // sets `setHasSameKeyPrefix` and so the 0x20 bit in the value prefix, and
    // the restart array's high bit — the one place a Go reader would diverge
    // on a detail roble's own reader would happily agree with itself about.
    let mut dups = Vec::new();
    for seq in (1..=8u64).rev() {
        dups.push((
            b"/dup".to_vec(),
            seq,
            Kind::Set,
            format!("v{seq}").into_bytes(),
        ));
    }
    for seq in (1..=4u64).rev() {
        dups.push((
            b"/dup2".to_vec(),
            seq,
            if seq % 2 == 0 {
                Kind::Set
            } else {
                Kind::Delete
            },
            if seq % 2 == 0 {
                format!("w{seq}").into_bytes()
            } else {
                Vec::new()
            },
        ));
    }
    out.push(Fixture {
        name: "dups",
        block_size: 4096,
        entries: dups,
    });

    // Long shared prefixes, which is what Oxia's keys look like, so prefix
    // compression is doing real work and the shared-prefix cap matters.
    out.push(Fixture {
        name: "prefixes",
        block_size: 1024,
        entries: (0..200u32)
            .map(|i| {
                (
                    oxia_key(11, i),
                    5000 + u64::from(i),
                    Kind::Set,
                    b"p".to_vec(),
                )
            })
            .collect(),
    });

    out
}

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: sst-roundtrip <output-dir>");
    let dir = Path::new(&dir);
    std::fs::create_dir_all(dir).expect("create output dir");

    println!(
        "{:<14} {:>8} {:>10} {:>8}",
        "fixture", "entries", "bytes", "blocks"
    );
    for f in fixtures() {
        let opts = WriterOptions {
            block_size: f.block_size,
            ..Default::default()
        };
        let mut w = TableWriter::new(opts).expect("writer");
        for (user_key, seq_num, kind, value) in &f.entries {
            w.add(
                InternalKey {
                    user_key,
                    seq_num: *seq_num,
                    kind: *kind,
                    obsolete: false,
                },
                value,
            )
            .unwrap_or_else(|e| {
                panic!(
                    "{}: add {:?}: {e}",
                    f.name,
                    String::from_utf8_lossy(user_key)
                )
            });
        }
        let bytes = w
            .finish()
            .unwrap_or_else(|e| panic!("{}: finish: {e}", f.name));

        let dump = dump_table(&bytes).unwrap_or_else(|e| panic!("{}: dump: {e}", f.name));
        std::fs::write(dir.join(format!("{}.sst", f.name)), &bytes).expect("write sst");
        std::fs::write(dir.join(format!("{}.dump", f.name)), &dump).expect("write dump");

        let blocks = dump
            .lines()
            .find_map(|l| l.strip_prefix("datablocks "))
            .unwrap_or("?");
        println!(
            "{:<14} {:>8} {:>10} {:>8}",
            f.name,
            f.entries.len(),
            bytes.len(),
            blocks
        );
    }
}
