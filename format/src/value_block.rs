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

//! Value blocks and their index (`sstable/valblk`).
//!
//! From Pebblev3 on, a `SET`'s value can live outside its data block. The data
//! block then holds a **handle** — three uvarints: the value's length, which
//! value block holds it, and where in that block it starts.
//!
//! Finding the value block means reading the value index, whose own handle is
//! the metaindex entry `pebble.value_index`. That entry is not just a block
//! handle: it is a block handle followed by **three width bytes**, giving the
//! byte widths of the three columns in the index. Reading only the handle and
//! ignoring the widths leaves the index unreadable.
//!
//! The index is a fixed-width table, one row per value block, each row three
//! little-endian integers truncated to those widths: block number, offset,
//! length. Row `i` always describes block `i`, so a lookup is arithmetic
//! rather than a search.
//!
//! roble reads these and never writes them (DESIGN.md §3). Pebble produces one
//! only when `setHasSamePrefix` holds — under the bytewise comparer, when the
//! same user key repeats inside one output table — so they are rare in an Oxia
//! database but entirely legal, and a reader that cannot resolve a handle
//! cannot read such a table at all.

use crate::block::BlockHandle;
use crate::error::{Error, Result};
use crate::varint;

/// A pointer from a data block entry to a value in a value block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handle {
    /// The value's length in bytes.
    pub value_len: u32,
    /// Which value block holds it.
    pub block_num: u32,
    /// Where in that block the value starts.
    pub offset_in_block: u32,
}

impl Handle {
    /// Decodes a handle from the bytes stored beside the key.
    ///
    /// The three fields are uvarints in this order: length, block number,
    /// offset. Reading them in any other order yields a plausible-looking
    /// handle that points somewhere else.
    pub fn decode(src: &[u8]) -> Result<Self> {
        let (value_len, n1) = varint::uvarint32(src)?;
        let (block_num, n2) = varint::uvarint32(&src[n1..])?;
        let (offset_in_block, n3) = varint::uvarint32(&src[n1 + n2..])?;
        let used = n1 + n2 + n3;
        if used != src.len() {
            return Error::corrupt(format!(
                "value handle is {} bytes but decoded {used}",
                src.len()
            ));
        }
        Ok(Handle {
            value_len,
            block_num,
            offset_in_block,
        })
    }
}

/// The value index: where each value block is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueIndex {
    /// Where the index block itself is.
    pub handle: BlockHandle,
    block_num_len: u8,
    offset_len: u8,
    length_len: u8,
}

impl ValueIndex {
    /// Decodes the metaindex value stored under `pebble.value_index`.
    ///
    /// That value is a block handle *plus* three width bytes
    /// (`valblk.EncodeIndexHandle`), which is why it cannot be read with
    /// [`BlockHandle::decode`] alone.
    pub fn decode_metaindex_value(src: &[u8]) -> Result<Self> {
        let (handle, n) = BlockHandle::decode(src)?;
        let widths = &src[n..];
        if widths.len() < 3 {
            return Error::truncated("value index widths", n + 3, src.len());
        }
        let index = ValueIndex {
            handle,
            block_num_len: widths[0],
            offset_len: widths[1],
            length_len: widths[2],
        };
        for (what, w) in [
            ("block number", index.block_num_len),
            ("offset", index.offset_len),
            ("length", index.length_len),
        ] {
            if w == 0 || w > 8 {
                return Error::corrupt(format!("value index {what} column is {w} bytes wide"));
            }
        }
        Ok(index)
    }

    /// The bytes each row of the index occupies.
    pub fn row_width(&self) -> usize {
        self.block_num_len as usize + self.offset_len as usize + self.length_len as usize
    }

    /// Decodes every row of the index block into a block handle per value block.
    ///
    /// Row `i` describes value block `i`; Pebble writes the block number in
    /// each row anyway, so it is checked rather than assumed.
    pub fn decode_rows(&self, index_block: &[u8]) -> Result<Vec<BlockHandle>> {
        let width = self.row_width();
        if !index_block.len().is_multiple_of(width) {
            return Error::corrupt(format!(
                "value index of {} bytes is not a whole number of {width}-byte rows",
                index_block.len()
            ));
        }
        let mut out = Vec::with_capacity(index_block.len() / width);
        for (i, row) in index_block.chunks_exact(width).enumerate() {
            let (num, rest) = row.split_at(self.block_num_len as usize);
            let (offset, length) = rest.split_at(self.offset_len as usize);
            let num = little_endian(num);
            if num != i as u64 {
                return Error::corrupt(format!("value index row {i} describes block {num}"));
            }
            out.push(BlockHandle {
                offset: little_endian(offset),
                length: little_endian(length),
            });
        }
        Ok(out)
    }
}

/// Reads an integer stored little-endian in as many bytes as it needed.
fn little_endian(b: &[u8]) -> u64 {
    let mut v = 0u64;
    for (i, &byte) in b.iter().enumerate() {
        v |= u64::from(byte) << (8 * i);
    }
    v
}

/// Takes a value out of a decoded value block.
pub fn value_in_block(block: &[u8], handle: Handle) -> Result<&[u8]> {
    let start = handle.offset_in_block as usize;
    let end = start
        .checked_add(handle.value_len as usize)
        .ok_or_else(|| Error::Corrupt("value handle overflows".to_string()))?;
    if end > block.len() {
        return Error::truncated("value in value block", end, block.len());
    }
    Ok(&block[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_handle_decodes_its_three_fields_in_order() {
        // value_len = 300, block_num = 2, offset_in_block = 1000.
        let mut bytes = Vec::new();
        for v in [300u32, 2, 1000] {
            let mut x = v;
            while x >= 0x80 {
                bytes.push((x as u8) | 0x80);
                x >>= 7;
            }
            bytes.push(x as u8);
        }
        let h = Handle::decode(&bytes).expect("decode");
        assert_eq!(h.value_len, 300);
        assert_eq!(h.block_num, 2);
        assert_eq!(h.offset_in_block, 1000);
    }

    #[test]
    fn trailing_bytes_after_a_handle_are_refused() {
        let mut bytes = vec![1u8, 0, 0];
        bytes.push(9);
        assert!(Handle::decode(&bytes).is_err());
    }

    #[test]
    fn the_metaindex_value_carries_widths_after_the_handle() {
        // handle offset 1000, length 12, then widths 1, 2, 1.
        let mut bytes = Vec::new();
        for v in [1000u64, 12] {
            let mut x = v;
            while x >= 0x80 {
                bytes.push((x as u8) | 0x80);
                x >>= 7;
            }
            bytes.push(x as u8);
        }
        bytes.extend_from_slice(&[1, 2, 1]);
        let index = ValueIndex::decode_metaindex_value(&bytes).expect("decode");
        assert_eq!(index.handle.offset, 1000);
        assert_eq!(index.handle.length, 12);
        assert_eq!(index.row_width(), 4);

        // Missing widths must not read as a bare block handle.
        assert!(ValueIndex::decode_metaindex_value(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn rows_are_fixed_width_little_endian_and_self_checking() {
        let index = ValueIndex {
            handle: BlockHandle {
                offset: 0,
                length: 0,
            },
            block_num_len: 1,
            offset_len: 2,
            length_len: 1,
        };
        // Two rows: block 0 at offset 0x0102 length 5, block 1 at 0x0304 len 6.
        let rows = vec![0, 0x02, 0x01, 5, 1, 0x04, 0x03, 6];
        let got = index.decode_rows(&rows).expect("decode");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].offset, 0x0102);
        assert_eq!(got[0].length, 5);
        assert_eq!(got[1].offset, 0x0304);

        // A row whose block number is not its position is corruption, not a
        // hint to search.
        let mut wrong = rows.clone();
        wrong[0] = 7;
        assert!(index.decode_rows(&wrong).is_err());

        // A partial row is refused rather than truncated.
        assert!(index.decode_rows(&rows[..rows.len() - 1]).is_err());
    }

    #[test]
    fn a_value_past_the_end_of_its_block_is_refused() {
        let block = b"abcdefgh";
        let good = Handle {
            value_len: 3,
            block_num: 0,
            offset_in_block: 2,
        };
        assert_eq!(value_in_block(block, good).expect("in range"), b"cde");
        let past = Handle {
            value_len: 10,
            block_num: 0,
            offset_in_block: 2,
        };
        assert!(value_in_block(block, past).is_err());
    }
}
