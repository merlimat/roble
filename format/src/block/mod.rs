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

//! Row-oriented blocks (`sstable/rowblk`) and their handles and trailers
//! (`sstable/block`: `Handle`, `TrailerLen`).
//!
//! A block is a run of entries followed by a restart array:
//!
//! ```text
//!   entry*                     shared, unshared, value-length as uvarints,
//!                              then the unshared key bytes, then the value
//!   restart: u32 * n           little-endian offsets into the block
//!   n: u32                     little-endian count of restarts
//! ```
//!
//! and on disk every block is followed by a five-byte trailer: one byte of
//! block type (the compression algorithm) and four bytes of checksum.
//!
//! **A block's keys are opaque bytes.** Whether a key carries an 8-byte
//! internal-key trailer is a convention of the block's *role*, not of this
//! encoding: data and index blocks store internal keys, while the metaindex
//! and properties blocks store bare names. So the low-level iterator yields
//! raw keys, and [`Block::iter_data`] is the only one that decodes them.
//!
//! The high bit of a restart offset is likewise not part of the offset: from
//! Pebblev3 on it carries `setHasSameKeyPrefixSinceLastRestart`
//! (`rowblk_writer.go`: `setHasSameKeyPrefixRestartMask`).

mod builder;
mod compression;
pub mod restart;

pub use builder::{BlockBuilder, DEFAULT_RESTART_INTERVAL, INDEX_RESTART_INTERVAL, max_shared_len};
pub(crate) use compression::decode as decode_compressed;
pub use compression::{Compression, Decoded};

use crate::error::{Error, Result};
use crate::key::InternalKey;
use crate::varint;

/// `block.TrailerLen`: one byte of block type, four of checksum.
pub const TRAILER_SIZE: usize = 5;

/// `rowblk.EmptySize`: the four-byte restart count every block ends with.
pub const EMPTY_BLOCK_SIZE: usize = 4;

/// `setHasSameKeyPrefixRestartMask`, the stolen high bit of a restart offset.
const SET_HAS_SAME_KEY_PREFIX_MASK: u32 = 1 << 31;

/// Where a block is in a table (`sstable/block`: `Handle`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BlockHandle {
    /// Byte offset of the block's data, not counting the trailer.
    pub offset: u64,
    /// Length of the block's data, not counting the trailer.
    pub length: u64,
}

impl BlockHandle {
    /// Decodes a handle, returning it and the bytes consumed.
    pub fn decode(src: &[u8]) -> Result<(Self, usize)> {
        let (offset, n1) = varint::uvarint(src)?;
        let (length, n2) = varint::uvarint(&src[n1..])?;
        Ok((BlockHandle { offset, length }, n1 + n2))
    }

    /// The span the block occupies on disk, data plus trailer.
    pub fn span_len(self) -> u64 {
        self.length + TRAILER_SIZE as u64
    }
}

/// The value stored beside a key in a data block.
///
/// From Pebblev3 on, a `SET` value carries a one-byte prefix saying whether the
/// bytes that follow are the value itself or a handle into a value block. roble
/// writes only in-place values but must read both (DESIGN.md §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Value<'a> {
    /// The bytes are the value.
    InPlace(&'a [u8]),
    /// The bytes are an encoded value-block handle, not yet resolved.
    ValueHandle(&'a [u8]),
    /// The bytes are an encoded blob handle. Above the pin, so the table reader
    /// refuses it; decoded here only so the refusal can name it.
    BlobHandle(&'a [u8]),
}

impl<'a> Value<'a> {
    /// The in-place bytes, or `None` when the value lives elsewhere.
    pub fn in_place(self) -> Option<&'a [u8]> {
        match self {
            Value::InPlace(v) => Some(v),
            _ => None,
        }
    }

    /// The name used in dumps.
    pub fn kind_name(self) -> &'static str {
        match self {
            Value::InPlace(_) => "inplace",
            Value::ValueHandle(_) => "valuehandle",
            Value::BlobHandle(_) => "blobhandle",
        }
    }

    /// The raw bytes, whatever they mean.
    pub fn bytes(self) -> &'a [u8] {
        match self {
            Value::InPlace(v) | Value::ValueHandle(v) | Value::BlobHandle(v) => v,
        }
    }
}

// sstable/block/kv.go: ValuePrefix.
const VALUE_KIND_MASK: u8 = 0xc0;
const VALUE_KIND_IN_PLACE: u8 = 0x00;
const VALUE_KIND_VALUE_BLOCK_HANDLE: u8 = 0x80;
const VALUE_KIND_BLOB_HANDLE: u8 = 0x40;

/// A decoded row block: its entries and its restart array.
///
/// Borrows the block's bytes; nothing is copied and no entry is decoded until
/// it is walked, so an uncompressed block stays the bytes that came off the
/// disk (DESIGN.md §8).
#[derive(Debug, Clone, Copy)]
pub struct Block<'a> {
    data: &'a [u8],
    restarts_at: usize,
    num_restarts: usize,
}

impl<'a> Block<'a> {
    /// Splits a block's bytes into entries and restart array.
    ///
    /// `data` is the block without its five-byte trailer.
    pub fn new(data: &'a [u8]) -> Result<Self> {
        if data.len() < EMPTY_BLOCK_SIZE {
            return Error::truncated("block", EMPTY_BLOCK_SIZE, data.len());
        }
        let count_at = data.len() - 4;
        let num_restarts =
            u32::from_le_bytes(data[count_at..].try_into().expect("4 bytes")) as usize;
        if num_restarts == 0 {
            // rowblk.Finish always writes at least one restart, even for an
            // empty block.
            return Error::corrupt("block has no restart points");
        }
        let restarts_len = num_restarts
            .checked_mul(4)
            .ok_or_else(|| Error::Corrupt(format!("{num_restarts} restart points")))?;
        if restarts_len + 4 > data.len() {
            return Error::corrupt(format!(
                "block of {} bytes claims {num_restarts} restart points",
                data.len()
            ));
        }
        Ok(Block {
            data,
            restarts_at: count_at - restarts_len,
            num_restarts,
        })
    }

    /// The number of restart points.
    pub fn num_restarts(&self) -> usize {
        self.num_restarts
    }

    /// The offset of restart point `i`, with the stolen high bit masked off.
    pub fn restart_offset(&self, i: usize) -> Result<usize> {
        if i >= self.num_restarts {
            return Error::corrupt(format!(
                "restart {i} of a block with {} restarts",
                self.num_restarts
            ));
        }
        let at = self.restarts_at + i * 4;
        let raw = u32::from_le_bytes(self.data[at..at + 4].try_into().expect("4 bytes"));
        let offset = (raw & !SET_HAS_SAME_KEY_PREFIX_MASK) as usize;
        if offset > self.restarts_at {
            return Error::corrupt(format!(
                "restart {i} points to {offset}, past the {} bytes of entries",
                self.restarts_at
            ));
        }
        Ok(offset)
    }

    /// The entry region, the restart array and its length.
    ///
    /// For [`restart::Cursor`], which reads restart offsets and jumps within
    /// the entries rather than walking them.
    pub(crate) fn parts(&self) -> (&'a [u8], &'a [u8], usize) {
        (
            &self.data[..self.restarts_at],
            &self.data[self.restarts_at..self.restarts_at + self.num_restarts * 4],
            self.num_restarts,
        )
    }

    /// A cursor that can seek and step either way over this block.
    pub fn cursor(&self) -> restart::Cursor<'a> {
        restart::Cursor::new(self)
    }

    /// A cursor over this **data** block, decoding internal keys and value
    /// prefixes.
    pub fn data_cursor(&self, format: crate::TableFormat) -> restart::DataCursor<'a> {
        restart::DataCursor::new(self, format)
    }

    /// Walks the block's entries with keys left as raw bytes.
    ///
    /// This is the right iterator for the metaindex and properties blocks,
    /// whose keys are bare names, and for walking an index block when only the
    /// handles are wanted.
    pub fn iter(&self) -> BlockIter<'a> {
        BlockIter {
            entries: &self.data[..self.restarts_at],
            pos: 0,
            key: Vec::new(),
        }
    }

    /// Walks a **data** block, decoding internal keys and value prefixes.
    ///
    /// `format` decides whether a `SET` value carries a prefix byte, so passing
    /// the wrong one shifts every value by one byte.
    pub fn iter_data(&self, format: crate::TableFormat) -> DataBlockIter<'a> {
        DataBlockIter {
            inner: self.iter(),
            format,
        }
    }
}

/// One entry of a block, with its key still opaque.
#[derive(Debug)]
pub struct RawEntry<'a, 'k> {
    /// The key as stored, assembled from the previous key's shared prefix.
    pub key: &'k [u8],
    /// The value as stored, a subslice of the block.
    pub value: &'a [u8],
}

/// Walks a block's entries, assembling each prefix-compressed key.
///
/// The key is assembled into a buffer the iterator owns and lent out per
/// entry; the value is lent straight from the block.
#[derive(Debug)]
pub struct BlockIter<'a> {
    entries: &'a [u8],
    pos: usize,
    key: Vec<u8>,
}

impl<'a> BlockIter<'a> {
    /// Decodes the next entry, or `None` at the end of the block.
    ///
    /// Borrows `self` for the key, which lives in the iterator's buffer, so an
    /// entry cannot outlive the step that produced it.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<RawEntry<'a, '_>>> {
        if self.pos >= self.entries.len() {
            return None;
        }
        Some(self.next_inner())
    }

    fn next_inner(&mut self) -> Result<RawEntry<'a, '_>> {
        let src = &self.entries[self.pos..];
        let (shared, n1) = varint::uvarint32(src)?;
        let (unshared, n2) = varint::uvarint32(&src[n1..])?;
        let (value_len, n3) = varint::uvarint32(&src[n1 + n2..])?;
        let header = n1 + n2 + n3;
        let shared = shared as usize;
        let unshared = unshared as usize;
        let value_len = value_len as usize;

        let need = header
            .checked_add(unshared)
            .and_then(|n| n.checked_add(value_len))
            .ok_or_else(|| Error::Corrupt("block entry lengths overflow".to_string()))?;
        if need > src.len() {
            return Error::truncated("block entry", need, src.len());
        }
        if shared > self.key.len() {
            return Error::corrupt(format!(
                "block entry shares {shared} bytes with a {}-byte previous key",
                self.key.len()
            ));
        }
        // Prefix compression: the key is the previous key's first `shared`
        // bytes followed by `unshared` fresh ones, so it has to be assembled
        // rather than borrowed.
        self.key.truncate(shared);
        self.key.extend_from_slice(&src[header..header + unshared]);

        let value = &src[header + unshared..need];
        self.pos += need;
        Ok(RawEntry {
            key: &self.key,
            value,
        })
    }
}

/// One entry of a data block.
#[derive(Debug)]
pub struct Entry<'a, 'k> {
    /// The internal key, assembled from the previous key's shared prefix.
    pub key: InternalKey<'k>,
    /// The value, which is a subslice of the block.
    pub value: Value<'a>,
}

/// Walks a data block, decoding internal keys and value prefixes.
#[derive(Debug)]
pub struct DataBlockIter<'a> {
    inner: BlockIter<'a>,
    format: crate::TableFormat,
}

impl<'a> DataBlockIter<'a> {
    /// Decodes the next entry, or `None` at the end of the block.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<Entry<'a, '_>>> {
        let format = self.format;
        let raw = match self.inner.next()? {
            Ok(raw) => raw,
            Err(e) => return Some(Err(e)),
        };
        let key = match InternalKey::decode(raw.key) {
            Ok(k) => k,
            Err(e) => return Some(Err(e)),
        };
        let value = match decode_value(format, key, raw.value) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(Entry { key, value }))
    }
}

pub(crate) fn decode_value<'a>(
    format: crate::TableFormat,
    key: InternalKey<'_>,
    bytes: &'a [u8],
) -> Result<Value<'a>> {
    // rowblk_writer.go: addPrefixToValueStoredWithKey = key.Kind() == Set, and
    // only from Pebblev3 on. Every other kind stores its value bare.
    if !(format.has_value_prefix() && key.kind == crate::Kind::Set) {
        return Ok(Value::InPlace(bytes));
    }
    let Some((&prefix, rest)) = bytes.split_first() else {
        return Error::corrupt(format!(
            "a {} SET value needs at least a prefix byte",
            format.name()
        ));
    };
    Ok(match prefix & VALUE_KIND_MASK {
        VALUE_KIND_IN_PLACE => Value::InPlace(rest),
        VALUE_KIND_VALUE_BLOCK_HANDLE => Value::ValueHandle(rest),
        VALUE_KIND_BLOB_HANDLE => Value::BlobHandle(rest),
        other => return Error::corrupt(format!("value prefix kind {other:#04x}")),
    })
}
