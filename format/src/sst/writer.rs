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
//! Pebblev4, row blocks, uncompressed, crc32c, a single-level index, values in
//! place, and the obsolete bit never set. No value blocks, no range keys, no
//! two-level index — all of which it *reads*, and none of which it needs to
//! produce.
//!
//! The index separator for a data block is that block's last internal key.
//! Pebble shortens it; a reader only needs it to be an upper bound for its own
//! block and below the next block's first key, which the last key satisfies.

use std::collections::BTreeMap;

use crate::block::{
    BlockBuilder, BlockHandle, DEFAULT_RESTART_INTERVAL, INDEX_RESTART_INTERVAL, max_shared_len,
};
use crate::checksum::ChecksumType;
use crate::error::{Error, Result};
use crate::footer::{FOOTER_LEN, PEBBLE_MAGIC, TableFormat};
use crate::key::{InternalKey, Kind, TRAILER_LEN};
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
}

impl Default for WriterOptions {
    fn default() -> Self {
        WriterOptions {
            block_size: DEFAULT_BLOCK_SIZE,
            format: TableFormat::Pebblev4,
        }
    }
}

/// Builds a Pebble sstable in memory.
///
/// Keys must be added in increasing internal-key order: increasing user key,
/// and for equal user keys decreasing sequence number. The writer checks this
/// rather than trusting it, because a table that is out of order is accepted
/// by a writer and then silently misread by every seek.
#[derive(Debug)]
pub struct TableWriter {
    opts: WriterOptions,
    out: Vec<u8>,
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
            out: Vec::new(),
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
        let trailer = (key.seq_num << 8) | u64::from(kind_byte(key.kind));
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
        let handle = self.write_block(&block)?;
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

    /// Appends a block and its trailer, returning its handle.
    fn write_block(&mut self, block: &[u8]) -> Result<BlockHandle> {
        let offset = self.out.len() as u64;
        // Uncompressed is roble's default and keeps a cached block the bytes
        // that came off the disk (DESIGN.md §8).
        let block_type = 0u8;
        let checksum = ChecksumType::Crc32c.compute(block, block_type)?;
        self.out.extend_from_slice(block);
        self.out.push(block_type);
        self.out.extend_from_slice(&checksum.to_le_bytes());
        Ok(BlockHandle {
            offset,
            length: block.len() as u64,
        })
    }

    /// Finishes the table and returns its bytes.
    pub fn finish(mut self) -> Result<Vec<u8>> {
        self.flush_data_block()?;

        let index_block = self.index.finish().to_vec();
        let index_handle = self.write_block(&index_block)?;

        let props = self.properties(index_handle.length);
        let mut props_builder = BlockBuilder::new(usize::MAX);
        let mut prev: Vec<u8> = Vec::new();
        for (name, value) in &props {
            let max_shared = prev.len();
            props_builder.add(name, value, max_shared, None, false);
            prev = name.clone();
        }
        let props_block = props_builder.finish().to_vec();
        let props_handle = self.write_block(&props_block)?;

        let mut metaindex = BlockBuilder::new(INDEX_RESTART_INTERVAL);
        let mut encoded_handle = Vec::new();
        put_uvarint(&mut encoded_handle, props_handle.offset);
        put_uvarint(&mut encoded_handle, props_handle.length);
        metaindex.add(b"rocksdb.properties", &encoded_handle, 0, None, false);
        let metaindex_block = metaindex.finish().to_vec();
        let metaindex_handle = self.write_block(&metaindex_block)?;

        self.write_footer(metaindex_handle, index_handle);
        Ok(self.out)
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
        self.out.extend_from_slice(&footer);
    }

    /// The properties Go Pebble reads back, sorted, as a row block needs.
    fn properties(&self, index_size: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut p: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut put_str = |k: &str, v: &str| {
            p.insert(k.as_bytes().to_vec(), v.as_bytes().to_vec());
        };
        put_str("rocksdb.comparator", BYTEWISE_COMPARER);
        put_str("rocksdb.merge.operator", CONCATENATE_MERGER);
        put_str("rocksdb.compression", "NoCompression");

        let mut put_num = |k: &str, v: u64| {
            let mut buf = Vec::new();
            put_uvarint(&mut buf, v);
            p.insert(k.as_bytes().to_vec(), buf);
        };
        put_num("rocksdb.num.entries", self.num_entries);
        put_num("rocksdb.num.data.blocks", self.num_data_blocks);
        put_num("rocksdb.deleted.keys", self.num_deletions);
        put_num("rocksdb.num.range-deletions", 0);
        put_num("rocksdb.raw.key.size", self.raw_key_size);
        put_num("rocksdb.raw.value.size", self.raw_value_size);
        put_num("rocksdb.data.size", self.data_size);
        put_num("rocksdb.index.size", index_size);
        put_num("rocksdb.merge.operands", 0);
        put_num("rocksdb.filter.size", 0);

        // saveUint32, not a uvarint: four bytes little-endian. 0 is
        // single-level.
        p.insert(
            b"rocksdb.block.based.table.index.type".to_vec(),
            0u32.to_le_bytes().to_vec(),
        );
        p
    }
}

fn kind_byte(kind: Kind) -> u8 {
    match kind {
        Kind::Delete => 0,
        Kind::Set => 1,
        Kind::Merge => 2,
        Kind::SingleDelete => 7,
        Kind::RangeDelete => 15,
        Kind::Separator => 17,
        Kind::SetWithDelete => 18,
        Kind::RangeKeyDelete => 19,
        Kind::RangeKeyUnset => 20,
        Kind::RangeKeySet => 21,
        Kind::IngestSst => 22,
        Kind::DeleteSized => 23,
        Kind::Excise => 24,
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
