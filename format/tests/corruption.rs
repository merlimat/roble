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

//! A damaged block is refused by its checksum, never read as something else.
//!
//! The fuzz targets turn checksums off (`cfg(fuzzing)`) so that damage
//! reaches the parsers behind them, and allow any error. This is the other
//! half, with checksums on: one byte flipped inside each block of every
//! golden table, in turn, and the whole table read. The read has to fail,
//! with a checksum mismatch, naming the block that was damaged. Every table
//! format in the corpus is covered, both checksum types and all three
//! compressions, so each way a block is stored is shown to be covered by the
//! checksum before it is decoded.

use std::collections::BTreeSet;
use std::path::Path;

use roble_format::block::Block;
use roble_format::sst::{Table, index_entries_of};
use roble_format::{BlockHandle, ChecksumType, Error};

/// Every block of a table, named for the failure message: the footer's two,
/// the metaindex's, the partitions of a two-level index, the data blocks and
/// the value blocks.
fn blocks(bytes: &[u8]) -> Vec<(String, BlockHandle)> {
    let table = Table::open(bytes).expect("the undamaged table opens");
    let footer = table.footer();
    let mut out = vec![
        ("the metaindex".to_string(), footer.metaindex),
        ("the index".to_string(), footer.index),
    ];
    let meta = table.metaindex();
    for (name, handle) in [
        ("the properties", meta.properties),
        ("the range deletions", meta.range_del),
        ("the filter", meta.filter),
        (
            "the value index",
            meta.value_index.as_ref().map(|v| v.handle),
        ),
    ] {
        if let Some(h) = handle {
            out.push((name.to_string(), h));
        }
    }
    if table.has_two_level_index() {
        let top = table.decode_block(footer.index).expect("the index");
        let top = Block::new(top.as_slice()).expect("the index block");
        for (i, e) in index_entries_of(&top)
            .expect("the partitions")
            .into_iter()
            .enumerate()
        {
            out.push((format!("index partition {i}"), e.handle));
        }
    }
    for (i, h) in table
        .data_block_handles()
        .expect("the data blocks")
        .into_iter()
        .enumerate()
    {
        out.push((format!("data block {i}"), h));
    }
    for (i, h) in table
        .value_blocks()
        .expect("the value blocks")
        .into_iter()
        .enumerate()
    {
        out.push((format!("value block {i}"), h));
    }
    out
}

/// Everything a reader can get out of a table: the dump, which walks every
/// entry, resolves every value and lists the range deletions, and the
/// filter's bytes, which the dump does not read.
fn read_all(bytes: &[u8]) -> roble_format::Result<String> {
    let mut out = roble_format::dump_table(bytes)?;
    let table = Table::open(bytes)?;
    if let Some(filter) = table.filter_block()? {
        out.push_str(&format!("filter {:02x?}\n", filter.as_slice()));
    }
    Ok(out)
}

#[test]
fn a_flipped_byte_in_any_block_is_refused_by_its_checksum() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let mut names: Vec<_> = std::fs::read_dir(&dir)
        .expect("the golden corpus")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "sst"))
        // Refused whole, before any block is read for its content.
        .filter(|p| {
            !p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("refuse-")
        })
        .collect();
    names.sort();

    let mut checksums = BTreeSet::new();
    let mut damaged = 0;
    for path in &names {
        let name = path.file_name().unwrap().to_string_lossy();
        let bytes = std::fs::read(path).expect("read the table");
        read_all(&bytes).unwrap_or_else(|e| panic!("{name} undamaged: {e}"));
        checksums.insert(Table::open(&bytes).expect("open").footer().checksum.name());

        for (block, handle) in blocks(&bytes) {
            // Inside the block's stored bytes, so the trailer is intact and
            // only the checksum can tell. A block of no bytes has nothing to
            // flip but its compression byte, which the checksum covers too.
            let at = if handle.length == 0 {
                handle.offset
            } else {
                handle.offset + handle.length / 2
            } as usize;
            let mut copy = bytes.clone();
            copy[at] ^= 0x10;
            match read_all(&copy) {
                Err(Error::ChecksumMismatch { offset, .. }) => assert_eq!(
                    offset, handle.offset,
                    "{name}: damage to {block} was reported against another block"
                ),
                Err(e) => {
                    panic!("{name}: damage to {block} was refused, but not by its checksum: {e}")
                }
                Ok(_) => {
                    panic!("{name}: damage to {block} at byte {at} was read without complaint")
                }
            }
            damaged += 1;
        }
    }
    assert_eq!(
        checksums,
        BTreeSet::from([ChecksumType::Crc32c.name(), ChecksumType::XxHash64.name()]),
        "both checksum types should be in the corpus"
    );
    assert!(
        damaged > 50,
        "only {damaged} blocks damaged across {} tables",
        names.len()
    );
}
