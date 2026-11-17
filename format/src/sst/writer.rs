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

//! The table writer (`sstable/rowblk_writer.go`).
//!
//! roble writes the narrowest table Go Pebble accepts (DESIGN.md §3):
//! Pebblev4, row blocks, crc32c, a single-level index, values in place, and
//! the obsolete bit never set. Blocks are uncompressed unless
//! [`WriterOptions::compression`] says otherwise, and then it is the data and
//! index blocks that are, as in Pebble's `layoutWriter`: the filter,
//! range-deletion, properties and metaindex blocks are always stored as built. No value blocks, no range keys, no
//! two-level index — all of which it *reads*, and none of which it needs to
//! produce.
//!
//! The index separator for a data block is that block's last internal key.
//! Pebble shortens it; a reader only needs it to be an upper bound for its own
//! block and below the next block's first key, which the last key satisfies.
//!
//! **A finished block is not kept.** Pebble's writer hands each data block to
//! its write queue as soon as it is full, and
//! `layoutWriter.writePrecompressedBlock` copies it into a `bufio.Writer` over
//! the file — so the bytes of a table leave the process while the table is
//! still being written, and what stays is the list the comment on
//! `RawRowWriter.EstimatedSize` gives: the open data block, the open index
//! block, the open range-deletion block. This writer does the same with the
//! push inverted into a pull: finished bytes gather in a buffer the caller
//! takes with [`TableWriter::take_ready`] whenever it chooses. The pull is
//! what lets this crate stay synchronous — roble's `Disk::write` is async and
//! takes an owned buffer, and a writer that called it would drag both of those
//! across a crate boundary that has neither.

use std::collections::BTreeMap;

use crate::block::{
    BlockBuilder, BlockHandle, Codec, Compressor, DEFAULT_RESTART_INTERVAL, INDEX_RESTART_INTERVAL,
    max_shared_len,
};
use crate::checksum::ChecksumType;
use crate::error::{Error, Result};
use crate::filter::{self, FilterBuilder};
use crate::footer::{FOOTER_LEN, PEBBLE_MAGIC, TableFormat};
use crate::key::{InternalKey, Kind, TRAILER_LEN};
use crate::rangedel;
use crate::sst::reader::BYTEWISE_COMPARER;

/// The merger name Pebble records by default, which Oxia leaves alone.
pub const CONCATENATE_MERGER: &str = "pebble.concatenate";

/// Oxia's block size, and so roble's default (DESIGN.md §1).
pub const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;

/// How a table is written.
#[derive(Debug, Clone, Copy)]
pub struct WriterOptions {
    /// The target size of a data block, before its trailer.
    pub block_size: usize,
    /// The table format. Only [`TableFormat::Pebblev4`] is written.
    pub format: TableFormat,
    /// Bits per key for the bloom filter, or `None` to write no filter block.
    ///
    /// A table without one is format-legal and Go Pebble reads it, but it
    /// throws away the point-lookup filtering Oxia configures, so the default
    /// matches Oxia's `bloom.FilterPolicy(10)` (DESIGN.md §1).
    pub filter_bits_per_key: Option<usize>,
    /// What data and index blocks are compressed with. A block compression
    /// does not shrink by 12% is stored uncompressed anyway, as Pebble does.
    pub compression: Codec,
}

impl Default for WriterOptions {
    fn default() -> Self {
        WriterOptions {
            block_size: DEFAULT_BLOCK_SIZE,
            format: TableFormat::Pebblev4,
            filter_bits_per_key: Some(filter::DEFAULT_BITS_PER_KEY),
            compression: Codec::None,
        }
    }
}

/// Builds a Pebble sstable, a block at a time.
///
/// Keys must be added in increasing internal-key order: increasing user key,
/// and for equal user keys decreasing sequence number. The writer checks this
/// rather than trusting it, because a table that is out of order is accepted
/// by a writer and then silently misread by every seek.
///
/// Finished bytes accumulate until they are taken. A caller with a file to
/// write to drains them as it goes — [`TableWriter::ready_len`] says how many
/// are waiting, [`TableWriter::take_ready`] hands them over — and appends
/// each chunk at the offset the ones before it ended at. A caller with
/// nowhere to stream to takes none and lets [`TableWriter::finish`] hand over
/// the whole table, which is what it returns when nothing was taken earlier.
#[derive(Debug)]
pub struct TableWriter {
    opts: WriterOptions,
    /// Finished bytes the caller has not taken yet.
    buf: Vec<u8>,
    /// How many it has, which is what keeps a block handle's offset an offset
    /// in the *file* rather than in what is left of the buffer.
    taken: u64,
    data: BlockBuilder,
    index: BlockBuilder,
    prev_key: Vec<u8>,
    block_last_key: Vec<u8>,
    num_entries: u64,
    num_data_blocks: u64,
    num_deletions: u64,
    raw_key_size: u64,
    raw_value_size: u64,
    data_size: u64,
    range_del: rangedel::Builder,
    filter: Option<FilterBuilder>,
    compressor: Compressor,
}

impl TableWriter {
    /// A writer with the given options.
    pub fn new(opts: WriterOptions) -> Result<Self> {
        if opts.format != TableFormat::Pebblev4 {
            return Error::unsupported(format!(
                "roble writes {} only, not {}",
                TableFormat::Pebblev4.name(),
                opts.format.name()
            ));
        }
        Ok(TableWriter {
            opts,
            buf: Vec::new(),
            taken: 0,
            data: BlockBuilder::new(DEFAULT_RESTART_INTERVAL),
            index: BlockBuilder::new(INDEX_RESTART_INTERVAL),
            prev_key: Vec::new(),
            block_last_key: Vec::new(),
            num_entries: 0,
            num_data_blocks: 0,
            num_deletions: 0,
            raw_key_size: 0,
            raw_value_size: 0,
            data_size: 0,
            range_del: rangedel::Builder::new(),
            filter: opts.filter_bits_per_key.map(FilterBuilder::new),
            compressor: Compressor::new(opts.compression)?,
        })
    }

    /// Adds a point entry.
    pub fn add(&mut self, key: InternalKey<'_>, value: &[u8]) -> Result<()> {
        if !key.kind.is_accepted() {
            return Error::unsupported(format!("roble does not write {} keys", key.kind.name()));
        }
        if key.obsolete {
            // Always-zero is correct and legal; setting it wrongly makes a Go
            // reader skip a live key (DESIGN.md §3).
            return Error::unsupported("roble never sets the obsolete bit");
        }
        if key.seq_num > crate::key::SEQ_NUM_MAX {
            return Error::corrupt(format!("sequence number {} is too large", key.seq_num));
        }

        let mut encoded = Vec::with_capacity(key.user_key.len() + TRAILER_LEN);
        encoded.extend_from_slice(key.user_key);
        let trailer = (key.seq_num << 8) | u64::from(key.kind.to_byte());
        encoded.extend_from_slice(&trailer.to_le_bytes());

        self.check_order(&encoded, key)?;

        // Under the bytewise comparer Split is the whole key, so "same prefix"
        // means the same user key.
        let same_key_prefix = prev_user_key(&self.prev_key) == Some(key.user_key);

        if !self.data.is_empty()
            && self.data.estimated_size() + entry_cost(&encoded, value) > self.opts.block_size
        {
            self.flush_data_block()?;
        }

        let value_prefix = if self.opts.format.has_value_prefix() && key.kind == Kind::Set {
            // kv.go: valueKindIsInPlaceValue, plus setHasSameKeyPrefixMask.
            Some(if same_key_prefix { 0x20 } else { 0x00 })
        } else {
            None
        };
        let max_shared = max_shared_len(self.opts.format, &self.prev_key, &encoded);
        self.data
            .add(&encoded, value, max_shared, value_prefix, same_key_prefix);

        // maybeAddToFilter hashes key[:Split(key)], which under the bytewise
        // comparer is the whole user key, and only for point keys.
        if let Some(f) = self.filter.as_mut() {
            f.add_key(key.user_key);
        }

        self.num_entries += 1;
        self.raw_key_size += encoded.len() as u64;
        self.raw_value_size += value.len() as u64;
        if matches!(
            key.kind,
            Kind::Delete | Kind::DeleteSized | Kind::SingleDelete
        ) {
            self.num_deletions += 1;
        }
        self.prev_key.clear();
        self.prev_key.extend_from_slice(&encoded);
        self.block_last_key.clear();
        self.block_last_key.extend_from_slice(&encoded);
        Ok(())
    }

    /// Adds a range tombstone covering `[start, end)` as of `seq_num`.
    ///
    /// Tombstones are kept apart from point keys and written to their own
    /// block, so they may be added in any interleaving with [`TableWriter::add`];
    /// among themselves they must arrive fragmented and in order, which
    /// [`rangedel::Builder`] checks.
    pub fn add_range_deletion(&mut self, start: &[u8], end: &[u8], seq_num: u64) -> Result<()> {
        self.range_del.add(start, end, seq_num)
    }

    fn check_order(&self, encoded: &[u8], key: InternalKey<'_>) -> Result<()> {
        if self.prev_key.is_empty() {
            return Ok(());
        }
        let prev = InternalKey::decode(&self.prev_key)?;
        let ordered = match key.user_key.cmp(prev.user_key) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => key.seq_num < prev.seq_num,
            std::cmp::Ordering::Less => false,
        };
        if !ordered {
            return Error::corrupt(format!(
                "keys out of order: {:?}#{} after {:?}#{}",
                String::from_utf8_lossy(key.user_key),
                key.seq_num,
                String::from_utf8_lossy(prev.user_key),
                prev.seq_num
            ));
        }
        debug_assert_eq!(encoded.len(), key.user_key.len() + TRAILER_LEN);
        Ok(())
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        let block = self.data.finish().to_vec();
        self.data.reset();
        let handle = self.write_block(&block, true)?;
        self.data_size += handle.length;
        self.num_data_blocks += 1;

        // The separator is this block's last key; the index's own entries are
        // keyed by it, so the index block needs no value prefix.
        let mut encoded_handle = Vec::new();
        put_uvarint(&mut encoded_handle, handle.offset);
        put_uvarint(&mut encoded_handle, handle.length);
        let sep = std::mem::take(&mut self.block_last_key);
        let max_shared = sep.len().saturating_sub(TRAILER_LEN);
        self.index
            .add(&sep, &encoded_handle, max_shared, None, false);
        Ok(())
    }

    /// About how large the table would be if finished now: every block
    /// written so far as stored, compressed or not, plus the open data block
    /// and the index as built (`RawRowWriter.EstimatedSize`).
    ///
    /// What a caller splitting its output at a target size should compare
    /// against. Counting the keys and values that went in instead overshoots
    /// by whatever compression saved, so a table of compressible values
    /// would end at a fraction of its target.
    pub fn estimated_size(&self) -> u64 {
        self.taken
            + (self.buf.len() + self.data.estimated_size() + self.index.estimated_size()) as u64
    }

    /// How many finished bytes are waiting to be taken.
    ///
    /// What a caller streaming to a file watches to decide a write is worth
    /// making. The threshold is the caller's: this crate has no disk and so
    /// no opinion about how large a write should be.
    pub fn ready_len(&self) -> usize {
        self.buf.len()
    }

    /// Takes the finished bytes, for the caller to append to the file.
    ///
    /// They belong at the offset where everything taken before them ended, so
    /// a caller appends them in the order it took them and nothing else
    /// writes to that file. What is taken is gone from the writer, which is
    /// the whole point: it is why a 64 MiB table costs a block and not 64 MiB
    /// to write, which is what §8 asks of a node running hundreds of stores.
    pub fn take_ready(&mut self) -> Vec<u8> {
        let ready = std::mem::take(&mut self.buf);
        self.taken += ready.len() as u64;
        ready
    }

    /// Takes a drained buffer back, to build the next chunk in.
    ///
    /// Not an optimisation of allocation *count* but of what the process
    /// holds: a flush that allocated a fresh megabyte for every chunk and
    /// freed it after the write left the allocator a cache of large free
    /// regions, which is resident memory by another name and measured as
    /// 30 MiB of it on a 64 MiB table. One buffer, handed round, is what the
    /// chunking is supposed to cost (§8).
    ///
    /// roble's `Disk::write` hands its buffer back, which is what makes this
    /// possible: the buffer that went to the disk is the one the next chunk
    /// wants, and the disk has finished with it. Anything added since it was
    /// taken is kept, ahead of whatever comes next, so the bytes stay in
    /// order however the caller interleaves the two.
    pub fn reuse(&mut self, mut buf: Vec<u8>) {
        buf.clear();
        buf.append(&mut self.buf);
        self.buf = buf;
    }

    /// Appends a block and its trailer, returning its handle.
    ///
    /// `compress` is for the data and index blocks, the ones Pebble's
    /// `layoutWriter.writeBlock` compresses; the rest go through its
    /// `writeBlockUncompressed`. The handle and the checksum cover the bytes
    /// as stored, compressed or not.
    fn write_block(&mut self, block: &[u8], compress: bool) -> Result<BlockHandle> {
        // The offset in the file, which is what a handle means: the bytes
        // already taken, then the ones still here.
        let offset = self.taken + self.buf.len() as u64;
        let (algorithm, stored) = if compress {
            self.compressor.compress(block)?
        } else {
            (crate::block::Compression::None, block)
        };
        let block_type = algorithm.to_byte();
        let checksum = ChecksumType::Crc32c.compute(stored, block_type)?;
        self.buf.extend_from_slice(stored);
        self.buf.push(block_type);
        self.buf.extend_from_slice(&checksum.to_le_bytes());
        Ok(BlockHandle {
            offset,
            length: stored.len() as u64,
        })
    }

    /// Finishes the table and returns the bytes not already taken.
    ///
    /// For a caller that never called [`TableWriter::take_ready`] that is the
    /// whole table, which is what the tools and the round-trip checks want.
    /// For one that drained as it went it is the rest: whatever blocks are
    /// still buffered, then the index, the range-deletion and filter blocks,
    /// the properties, the metaindex and the footer — appended at the offset
    /// the last chunk ended at.
    pub fn finish(mut self) -> Result<Vec<u8>> {
        self.flush_data_block()?;

        let index_block = self.index.finish().to_vec();
        let index_handle = self.write_block(&index_block, true)?;

        // Pebble writes this block uncompressed, always, and so do the
        // filter, properties and metaindex below (`WriteRangeDeletionBlock`,
        // `WriteFilterBlock`, `WritePropertiesBlock` below Pebblev7, and
        // `Finish`).
        let range_del_handle = if self.range_del.is_empty() {
            None
        } else {
            let block = self.range_del.finish();
            Some(self.write_block(&block, false)?)
        };

        let filter_handle = match self.filter.as_ref() {
            Some(f) if !f.is_empty() => {
                let block = f.finish();
                Some(self.write_block(&block, false)?)
            }
            _ => None,
        };

        let props = self.properties(index_handle.length, filter_handle);
        let mut props_builder = BlockBuilder::new(usize::MAX);
        let mut prev: Vec<u8> = Vec::new();
        for (name, value) in &props {
            let max_shared = prev.len();
            props_builder.add(name, value, max_shared, None, false);
            prev = name.clone();
        }
        let props_block = props_builder.finish().to_vec();
        let props_handle = self.write_block(&props_block, false)?;

        // The metaindex is a row block, so its names must be added in order:
        // "rocksdb.properties" sorts before "rocksdb.range_del2".
        let mut metaindex = BlockBuilder::new(INDEX_RESTART_INTERVAL);
        let mut entries: Vec<(&[u8], BlockHandle)> = vec![(b"rocksdb.properties", props_handle)];
        if let Some(h) = range_del_handle {
            entries.push((rangedel::META_NAME_V2, h));
        }
        if let Some(h) = filter_handle {
            entries.push((filter::META_NAME.as_bytes(), h));
        }
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (name, handle) in entries {
            let mut encoded_handle = Vec::new();
            put_uvarint(&mut encoded_handle, handle.offset);
            put_uvarint(&mut encoded_handle, handle.length);
            metaindex.add(name, &encoded_handle, 0, None, false);
        }
        let metaindex_block = metaindex.finish().to_vec();
        let metaindex_handle = self.write_block(&metaindex_block, false)?;

        self.write_footer(metaindex_handle, index_handle);
        Ok(self.buf)
    }

    fn write_footer(&mut self, metaindex: BlockHandle, index: BlockHandle) {
        let mut footer = vec![0u8; FOOTER_LEN];
        footer[0] = 1; // ChecksumTypeCRC32c
        let mut handles = Vec::new();
        put_uvarint(&mut handles, metaindex.offset);
        put_uvarint(&mut handles, metaindex.length);
        put_uvarint(&mut handles, index.offset);
        put_uvarint(&mut handles, index.length);
        footer[1..1 + handles.len()].copy_from_slice(&handles);
        let version_at = FOOTER_LEN - 8 - 4;
        footer[version_at..version_at + 4].copy_from_slice(&4u32.to_le_bytes());
        footer[FOOTER_LEN - 8..].copy_from_slice(PEBBLE_MAGIC);
        self.buf.extend_from_slice(&footer);
    }

    /// The properties Go Pebble reads back, sorted, as a row block needs.
    fn properties(
        &self,
        index_size: u64,
        filter_handle: Option<BlockHandle>,
    ) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut p: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut put_str = |k: &str, v: &str| {
            p.insert(k.as_bytes().to_vec(), v.as_bytes().to_vec());
        };
        put_str("rocksdb.comparator", BYTEWISE_COMPARER);
        put_str("rocksdb.merge.operator", CONCATENATE_MERGER);
        put_str("rocksdb.compression", self.opts.compression.property_name());
        if filter_handle.is_some() {
            put_str("rocksdb.filter.policy", filter::POLICY_NAME);
        }

        let mut put_num = |k: &str, v: u64| {
            let mut buf = Vec::new();
            put_uvarint(&mut buf, v);
            p.insert(k.as_bytes().to_vec(), buf);
        };
        // addTombstone increments NumEntries and NumDeletions as well as
        // NumRangeDeletions, so a table's entry count includes its tombstones.
        let tombstones = self.range_del.len() as u64;
        put_num("rocksdb.num.entries", self.num_entries + tombstones);
        put_num("rocksdb.num.data.blocks", self.num_data_blocks);
        put_num("rocksdb.deleted.keys", self.num_deletions + tombstones);
        put_num("rocksdb.num.range-deletions", tombstones);
        put_num(
            "rocksdb.raw.key.size",
            self.raw_key_size + self.range_del.raw_key_size(),
        );
        put_num(
            "rocksdb.raw.value.size",
            self.raw_value_size + self.range_del.raw_value_size(),
        );
        put_num("rocksdb.data.size", self.data_size);
        put_num("rocksdb.index.size", index_size);
        put_num("rocksdb.merge.operands", 0);
        put_num("rocksdb.filter.size", filter_handle.map_or(0, |h| h.length));

        // saveUint32, not a uvarint: four bytes little-endian. 0 is
        // single-level.
        p.insert(
            b"rocksdb.block.based.table.index.type".to_vec(),
            0u32.to_le_bytes().to_vec(),
        );
        p
    }
}

fn prev_user_key(prev: &[u8]) -> Option<&[u8]> {
    if prev.len() < TRAILER_LEN {
        None
    } else {
        Some(&prev[..prev.len() - TRAILER_LEN])
    }
}

/// A generous upper bound on what one entry adds to a block.
fn entry_cost(key: &[u8], value: &[u8]) -> usize {
    // Three uvarint32 headers, the key, a value prefix byte, and the value.
    3 * 5 + key.len() + 1 + value.len()
}

fn put_uvarint(buf: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        buf.push((x as u8) | 0x80);
        x >>= 7;
    }
    buf.push(x as u8);
}
