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

//! One block as the engine fetches it, read as every kind of block there is.
//!
//! Layout: one byte giving how many cursor operations follow, those
//! operations, then the fetched block with its five-byte trailer. The
//! trailer's type byte picks the decompressor, so Snappy and Zstd are reached
//! as well. The same bytes are also read as an uncompressed block directly,
//! which is the shorter way to the parsers behind decompression.
//!
//! Nothing is compared here: the bytes are garbage and any answer is allowed.
//! What is not allowed is a panic, an abort, a hang or an allocation the
//! input cannot justify.

#![no_main]

use libfuzzer_sys::fuzz_target;
use roble_format::block::{Block, decode_fetched};
use roble_format::filter::Filter;
use roble_format::sst::{Metaindex, Properties, index_entries_of};
use roble_format::{BlockHandle, ChecksumType, InternalKey, TableFormat, rangedel, value_block};

const FORMATS: [TableFormat; 5] = [
    TableFormat::RocksDBv2,
    TableFormat::Pebblev1,
    TableFormat::Pebblev2,
    TableFormat::Pebblev3,
    TableFormat::Pebblev4,
];

fn walk(bytes: &[u8], ops: &[u8]) {
    let _ = Filter::from_block(bytes).map(|f| f.map(|f| f.may_contain(b"k")));
    let _ = InternalKey::decode(bytes);
    let _ = BlockHandle::decode(bytes);
    let _ = value_block::Handle::decode(bytes);

    let Ok(block) = Block::new(bytes) else {
        return;
    };
    let mut it = block.iter();
    while let Some(Ok(_)) = it.next() {}
    for format in FORMATS {
        let mut it = block.iter_data(format);
        while let Some(Ok(_)) = it.next() {}
    }
    let _ = rangedel::decode(&block);
    let _ = index_entries_of(&block);
    if let Ok(meta) = Metaindex::parse(&block)
        && let Some(index) = meta.value_index
    {
        let _ = index.decode_rows(bytes);
    }
    if let Ok(props) = Properties::decode(&block) {
        let _ = props.is_two_level_index();
        let _ = props.num_entries();
        let _ = props.num_data_blocks();
    }

    // Cursor operations, stopping at the first error: a cursor that has
    // refused a block is not stepped again by any caller.
    let mut cursor = block.data_cursor(FORMATS[ops.len() % FORMATS.len()]);
    for &op in ops {
        // A seek target cut from the block itself, so it has a chance of
        // sharing prefixes with what is stored.
        let at = (op as usize >> 3) * 7 % bytes.len();
        let target = &bytes[at..bytes.len().min(at + 24)];
        let r = match op & 7 {
            0 => cursor.first(&block),
            1 => cursor.last(&block),
            2 | 6 => cursor.next(&block),
            3 | 7 => cursor.prev(&block),
            4 => cursor.seek_ge(&block, target),
            _ => cursor.seek_lt(&block, target),
        };
        if r.is_err() {
            return;
        }
        if cursor.is_valid() && cursor.entry(&block).is_err() {
            return;
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&n, rest)) = data.split_first() else {
        return;
    };
    let (ops, fetched) = rest.split_at((n as usize).min(rest.len()));
    if let Ok(decoded) = decode_fetched(fetched, 0, ChecksumType::Crc32c) {
        walk(decoded.as_slice(), ops);
    }
    walk(fetched, ops);
});
