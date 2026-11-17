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

//! The restart cursor against a sorted `Vec`, over any sequence of moves.
//!
//! Two blocks are built from arbitrary keys at an arbitrary restart interval,
//! and one cursor is moved between them — `first`, `last` and the seeks pick
//! a block, `next` and `prev` step within the one it is on — because the
//! entries a step back keeps (`Behind`) are only right while they belong to
//! the block the cursor is on. After every move the cursor must be on the
//! entry the `Vec` says, or on none.

#![no_main]

use std::cmp::Ordering;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use roble_format::block::Block;
use roble_format::block::restart::Cursor;
use roble_format::key;
use roble_format::{BlockBuilder, TableFormat};

#[derive(Arbitrary, Debug)]
struct Case {
    blocks: [BlockCase; 2],
    ops: Vec<Op>,
}

#[derive(Arbitrary, Debug)]
struct BlockCase {
    restart_interval: u8,
    /// User key, trailer, value.
    entries: Vec<(Vec<u8>, u64, Vec<u8>)>,
}

#[derive(Arbitrary, Debug)]
enum Op {
    First(bool),
    Last(bool),
    Next,
    Prev,
    /// Seek to a stored key exactly, picked by index.
    SeekGeStored(bool, u8),
    SeekLtStored(bool, u8),
    /// Seek to a user key with an arbitrary trailer.
    SeekGe(bool, Vec<u8>, u64),
    SeekLt(bool, Vec<u8>, u64),
}

struct Built {
    bytes: Vec<u8>,
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
}

fn build(case: BlockCase) -> Built {
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = case
        .entries
        .into_iter()
        .map(|(k, trailer, v)| (key::encode_with_trailer(&k, trailer), v))
        .collect();
    entries.sort_by(|a, b| key::compare(&a.0, &b.0).expect("internal keys"));
    entries.dedup_by(|a, b| a.0 == b.0);

    let mut b = BlockBuilder::new(usize::from(case.restart_interval));
    let mut prev: &[u8] = &[];
    for (k, v) in &entries {
        let max_shared = roble_format::block::max_shared_len(TableFormat::Pebblev4, prev, k);
        b.add(k, v, max_shared, None, false);
        prev = k;
    }
    let bytes = b.finish().to_vec();
    let (keys, values) = entries.into_iter().unzip();
    Built {
        bytes,
        keys,
        values,
    }
}

fuzz_target!(|case: Case| {
    let [a, b] = case.blocks;
    let built = [build(a), build(b)];
    let blocks = [
        Block::new(&built[0].bytes).expect("a built block"),
        Block::new(&built[1].bytes).expect("a built block"),
    ];

    let mut cursor = Cursor::new();
    // Which block the cursor is on, and which entry of it, if any.
    let mut on = 0usize;
    let mut want: Option<usize> = None;

    for op in case.ops {
        let stored = |which: bool, i: u8| {
            let keys = &built[usize::from(which)].keys;
            if keys.is_empty() {
                // Nothing stored to pick: any internal key will do.
                key::search_key(b"")
            } else {
                keys[usize::from(i) % keys.len()].clone()
            }
        };
        // A seek: which block, the target, and whether it is `seek_ge`.
        let seek = match op {
            Op::SeekGeStored(w, i) => Some((w, stored(w, i), true)),
            Op::SeekLtStored(w, i) => Some((w, stored(w, i), false)),
            Op::SeekGe(w, ref k, t) => Some((w, key::encode_with_trailer(k, t), true)),
            Op::SeekLt(w, ref k, t) => Some((w, key::encode_with_trailer(k, t), false)),
            _ => None,
        };
        let (what, r) = match op {
            Op::First(w) => {
                on = usize::from(w);
                want = (!built[on].keys.is_empty()).then_some(0);
                ("first", cursor.first(&blocks[on]))
            }
            Op::Last(w) => {
                on = usize::from(w);
                want = built[on].keys.len().checked_sub(1);
                ("last", cursor.last(&blocks[on]))
            }
            Op::Next => {
                want = want.and_then(|i| (i + 1 < built[on].keys.len()).then_some(i + 1));
                ("next", cursor.next(&blocks[on]))
            }
            Op::Prev => {
                want = want.and_then(|i| i.checked_sub(1));
                ("prev", cursor.prev(&blocks[on]))
            }
            _ => {
                let (which, target, ge) = seek.expect("every other op is a seek");
                on = usize::from(which);
                let keys = &built[on].keys;
                let cmp = |k: &Vec<u8>| key::compare(k, &target).expect("internal keys");
                if ge {
                    want = keys.iter().position(|k| cmp(k) != Ordering::Less);
                    ("seek_ge", cursor.seek_ge(&blocks[on], &target))
                } else {
                    want = keys.iter().rposition(|k| cmp(k) == Ordering::Less);
                    ("seek_lt", cursor.seek_lt(&blocks[on], &target))
                }
            }
        };
        r.unwrap_or_else(|e| panic!("{what} on a built block: {e}"));
        match want {
            None => assert!(!cursor.is_valid(), "{what}: on an entry, want none"),
            Some(i) => {
                assert!(cursor.is_valid(), "{what}: on no entry, want {i}");
                assert_eq!(
                    cursor.key(),
                    &built[on].keys[i][..],
                    "{what}: key of entry {i}"
                );
                assert_eq!(
                    cursor.value(&blocks[on]),
                    &built[on].values[i][..],
                    "{what}: value of entry {i}"
                );
            }
        }
    }
});
