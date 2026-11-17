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

/// Verifies and decompresses a block whose bytes have already been fetched.
///
/// `fetched` is the block's data **and** its five-byte trailer — exactly the
/// span [`BlockHandle::span_len`] measures — which is what an engine reads
/// when it fetches one block instead of a whole table. `offset` only names the
/// block in a checksum error.
///
/// This is the seam a block cache needs: without it, reading a block means
/// having the whole file in memory, and a cache keyed by (file, offset) has
/// nothing to cache.
pub fn decode_fetched(
    fetched: &[u8],
    offset: u64,
    checksum: crate::ChecksumType,
) -> Result<Decoded<'_>> {
    if fetched.len() < TRAILER_SIZE {
        return Error::truncated("block", TRAILER_SIZE, fetched.len());
    }
    let (data, trailer) = fetched.split_at(fetched.len() - TRAILER_SIZE);
    let block_type = trailer[0];
    let stored = u32::from_le_bytes(trailer[1..5].try_into().expect("4 bytes"));

    let computed = checksum.compute(data, block_type)?;
    if computed != stored {
        return Err(Error::ChecksumMismatch {
            offset,
            stored,
            computed,
        });
    }
    decode_compressed(Compression::from_byte(block_type)?, data)
}

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
    ///
    /// Refuses one whose end, trailer included, is past `u64::MAX`. The two
    /// varints are read from a table, and every reader adds them, so this is
    /// where an end no file can have is caught rather than at each sum.
    pub fn decode(src: &[u8]) -> Result<(Self, usize)> {
        let (offset, n1) = varint::uvarint(src)?;
        let (length, n2) = varint::uvarint(&src[n1..])?;
        let handle = BlockHandle { offset, length };
        if handle.end().is_none() {
            return Error::corrupt(format!("block handle {handle:?} ends past u64::MAX"));
        }
        Ok((handle, n1 + n2))
    }

    /// Where the block's trailer ends, or `None` past `u64::MAX`.
    fn end(self) -> Option<u64> {
        self.length
            .checked_add(TRAILER_SIZE as u64)
            .and_then(|span| self.offset.checked_add(span))
    }

    /// The span the block occupies on disk, data plus trailer.
    ///
    /// Cannot overflow for a handle [`decode`](Self::decode) returned.
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
    /// The name used in dumps.
    pub fn kind_name(self) -> &'static str {
        match self {
            Value::InPlace(_) => "inplace",
            Value::ValueHandle(_) => "valuehandle",
            Value::BlobHandle(_) => "blobhandle",
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
    ///
    /// The cursor does not borrow the block: every operation takes the block
    /// again, so that an iterator can own the bytes and the cursor together.
    pub fn cursor(&self) -> restart::Cursor {
        restart::Cursor::new()
    }

    /// A cursor over this **data** block, decoding internal keys and value
    /// prefixes.
    pub fn data_cursor(&self, format: crate::TableFormat) -> restart::DataCursor {
        restart::DataCursor::new(format)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::builder::BlockBuilder;

    /// A block of three entries, as a writer leaves it.
    fn a_block() -> Vec<u8> {
        let mut b = BlockBuilder::new(2);
        for (k, v) in [
            (&b"/oxia/a"[..], &b"one"[..]),
            (&b"/oxia/b"[..], &b"two"[..]),
            (&b"/oxia/c"[..], &b"three"[..]),
        ] {
            b.add(k, v, 0, None, false);
        }
        b.finish().to_vec()
    }

    /// A handle is two varints read from a table, so nothing stops one from
    /// naming a block whose end, trailer included, is past `u64::MAX`. Every
    /// reader adds the two, so the handle is refused where it is decoded.
    #[test]
    fn a_handle_whose_end_overflows_is_refused() {
        let mut src = Vec::new();
        for v in [u64::MAX - TRAILER_SIZE as u64, 1] {
            put_uvarint(&mut src, v);
        }
        let err = BlockHandle::decode(&src).expect_err("an end past u64::MAX");
        assert!(matches!(err, Error::Corrupt(_)), "got {err:?}");

        // One byte less and it fits, so the bound is exact.
        let mut src = Vec::new();
        for v in [u64::MAX - TRAILER_SIZE as u64 - 1, 1] {
            put_uvarint(&mut src, v);
        }
        BlockHandle::decode(&src).expect("an end at u64::MAX");
    }

    fn put_uvarint(dst: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            dst.push(v as u8 | 0x80);
            v >>= 7;
        }
        dst.push(v as u8);
    }

    #[test]
    fn a_value_names_the_kind_it_is() {
        // These names reach a dump and an error message, both of which are
        // read by someone deciding what a table holds.
        assert_eq!(Value::InPlace(b"x").kind_name(), "inplace");
        assert_eq!(Value::ValueHandle(b"x").kind_name(), "valuehandle");
        assert_eq!(Value::BlobHandle(b"x").kind_name(), "blobhandle");
    }

    #[test]
    fn a_fetched_block_shorter_than_its_trailer_is_truncated() {
        // The trailer carries the compression byte and the checksum, so there
        // is nothing to verify against without it. An engine that fetched a
        // short read rather than a block arrives here.
        for n in 0..TRAILER_SIZE {
            let err = decode_fetched(&vec![0u8; n], 0, crate::ChecksumType::Crc32c)
                .expect_err("shorter than a trailer");
            assert!(
                matches!(err, Error::Truncated { .. }),
                "{n} bytes: got {err:?}"
            );
        }
    }

    #[test]
    fn a_degenerate_block_is_refused_rather_than_indexed_into() {
        // Everything `Block::new` checks is what lets the cursor index without
        // bounds checks afterwards, so each of these has to be caught here.
        for n in 0..EMPTY_BLOCK_SIZE {
            assert!(Block::new(&vec![0u8; n]).is_err(), "{n} bytes");
        }
        // A restart count of zero: Pebble never writes one, and a block with
        // no restart point has no place for a cursor to start.
        assert!(
            Block::new(&0u32.to_le_bytes()).is_err(),
            "zero restart points"
        );
        // More restart points than the block could hold.
        let mut claims_many = vec![0u8; 8];
        claims_many[4..8].copy_from_slice(&1000u32.to_le_bytes());
        assert!(Block::new(&claims_many).is_err(), "1000 restart points");
    }

    /// Cutting a real block anywhere errors rather than panicking.
    ///
    /// The same shape as `a_truncated_table_errors_rather_than_panicking` one
    /// level up, and the reason is the same: these decoders are handed bytes
    /// off a disk, so every prefix of a good block is a thing that can arrive.
    /// Opening may legitimately succeed — the restart array is at the end, so
    /// a cut usually destroys it, but not always — and then walking must fail
    /// rather than read past what is there.
    #[test]
    fn a_block_cut_anywhere_is_refused_rather_than_panicking() {
        // The restart array and its count sit at the end, so in practice
        // every cut destroys them and `Block::new` is where this stops. That
        // is worth saying rather than leaving a walk below that never runs:
        // the entry-level checks are reached by the malformed blocks in the
        // test after this one, where the restart array is intact and the
        // entries are not.
        let whole = a_block();
        for cut in 0..whole.len() {
            assert!(Block::new(&whole[..cut]).is_err(), "a block cut at {cut}");
        }
        assert!(Block::new(&whole).is_ok(), "the whole block still opens");
    }

    /// An entry that lies about its own lengths, in a block that is otherwise
    /// well formed.
    ///
    /// These are the two checks between a cursor and reading past the block:
    /// `Block::new` has already agreed the restart array is sound, so nothing
    /// before this point looks at the entries at all.
    #[test]
    fn an_entry_that_overruns_its_block_is_refused() {
        // One restart point at offset 0, as every block has.
        fn with_entries(entries: &[u8]) -> Vec<u8> {
            let mut out = entries.to_vec();
            out.extend_from_slice(&0u32.to_le_bytes()); // restart[0] = 0
            out.extend_from_slice(&1u32.to_le_bytes()); // one restart point
            out
        }

        // shared = 0, unshared = 200, value_len = 0, and then nothing.
        let truncated = with_entries(&[0, 200, 1, 0]);
        let block = Block::new(&truncated).expect("the restart array is sound");
        let err = block
            .iter()
            .next()
            .expect("an entry")
            .expect_err("an entry that runs past the block");
        assert!(matches!(err, Error::Truncated { .. }), "got {err:?}");

        // shared = 5 on the first entry, where there is no previous key to
        // share with.
        let overshared = with_entries(&[5, 1, 0, b'x']);
        let block = Block::new(&overshared).expect("the restart array is sound");
        let err = block
            .iter()
            .next()
            .expect("an entry")
            .expect_err("sharing with a key that is not there");
        assert!(
            format!("{err}").contains("shares 5 bytes with a 0-byte previous key"),
            "got {err:?}"
        );
    }

    #[test]
    fn a_value_prefix_is_decoded_by_its_kind() {
        use crate::{InternalKey, Kind, TableFormat};

        let key = InternalKey {
            user_key: b"/oxia/a",
            seq_num: 7,
            kind: Kind::Set,
            obsolete: false,
        };
        let v4 = TableFormat::Pebblev4;

        // A blob handle: legal bytes, and a value roble refuses one level up
        // because blob files arrive above its pin.
        let blob = [VALUE_KIND_BLOB_HANDLE, 1, 2, 3];
        assert!(matches!(
            decode_value(v4, key, &blob).expect("decodes"),
            Value::BlobHandle(b"\x01\x02\x03")
        ));

        // A kind the mask leaves that Pebble does not define.
        let unknown = [VALUE_KIND_MASK, 1, 2, 3];
        let err = decode_value(v4, key, &unknown).expect_err("no such kind");
        assert!(matches!(err, Error::Corrupt(_)), "got {err:?}");

        // And a SET with no prefix byte at all, in a format that requires one.
        let err = decode_value(v4, key, &[]).expect_err("no prefix byte");
        assert!(
            format!("{err}").contains("prefix byte"),
            "got {err:?}, which should name the missing prefix"
        );

        // Before v3 the value is stored bare, so the same empty slice is an
        // empty value rather than a missing prefix.
        let v2 = TableFormat::Pebblev2;
        assert!(matches!(
            decode_value(v2, key, &[]).expect("bare"),
            Value::InPlace(b"")
        ));
    }

    /// A restart point that does not exist, or points outside the entries.
    ///
    /// `Block::new` counts the restart array and checks it fits; it does not
    /// read the offsets in it. A cursor asks for them one at a time, so this
    /// is where a corrupt one has to be caught — before it is used to index.
    #[test]
    fn a_restart_point_outside_the_block_is_refused() {
        let whole = a_block();
        let block = Block::new(&whole).expect("a good block");
        let n = block.num_restarts();
        let err = block
            .restart_offset(n)
            .expect_err("one past the last restart point");
        assert!(matches!(err, Error::Corrupt(_)), "got {err:?}");

        // An offset past the entries, in a block whose restart array is
        // otherwise well formed.
        let mut data = vec![0u8, 1, 0, b'x']; // one entry, four bytes
        data.extend_from_slice(&100u32.to_le_bytes()); // restart[0] = 100
        data.extend_from_slice(&1u32.to_le_bytes());
        let block = Block::new(&data).expect("the array fits");
        let err = block.restart_offset(0).expect_err("past the entries");
        assert!(
            format!("{err}").contains("past the 4 bytes of entries"),
            "got {err:?}"
        );
    }

    /// Each of the three ways decoding one data-block entry can fail.
    ///
    /// `DataBlockIter` is the cursor the engine reads through, and it layers
    /// two decodes on the raw entry — the internal key, then the value for
    /// the table's format. All three failures have to come back as an error
    /// from `next` rather than as a panic or a half-decoded entry.
    #[test]
    fn a_data_block_entry_reports_each_way_it_can_fail() {
        use crate::TableFormat;

        fn with_entries(entries: &[u8]) -> Vec<u8> {
            let mut out = entries.to_vec();
            out.extend_from_slice(&0u32.to_le_bytes());
            out.extend_from_slice(&1u32.to_le_bytes());
            out
        }

        fn first_error(data: &[u8]) -> Error {
            let block = Block::new(data).expect("the restart array is sound");
            block
                .iter_data(TableFormat::Pebblev4)
                .next()
                .expect("an entry")
                .expect_err("an entry that cannot be decoded")
        }

        // The raw entry itself overruns the block.
        let err = first_error(&with_entries(&[0, 200, 1, 0]));
        assert!(matches!(err, Error::Truncated { .. }), "raw: {err:?}");

        // A key too short to hold the eight-byte trailer every internal key
        // ends with.
        let err = first_error(&with_entries(&[0, 3, 0, b'a', b'b', b'c']));
        assert!(matches!(err, Error::Truncated { .. }), "key: {err:?}");

        // A well-formed SET key, and a value with no prefix byte, which
        // Pebblev4 requires on every SET.
        let mut entry = vec![0u8, 9, 0, b'k'];
        entry.extend_from_slice(&1u64.to_le_bytes()); // seq 0, kind Set
        let err = first_error(&with_entries(&entry));
        assert!(
            format!("{err}").contains("prefix byte"),
            "value: {err:?}, which should name the missing prefix"
        );
    }
}
