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

//! The table reader (`sstable/reader.go`): footer, metaindex, index, blocks.
//!
//! This is also where §3's refusals live. The encoding modules describe the
//! format as Pebble writes it; what roble declines to serve is a policy, and
//! keeping it in one place is what makes the policy reviewable.

use crate::block::{Block, BlockHandle, Compression, Decoded, TRAILER_SIZE};
use crate::error::{Error, Result};
use crate::footer::{FOOTER_LEN, Footer};
use crate::key::Kind;
use crate::sst::Properties;

/// The comparer roble serves, and the only one (DESIGN.md §3).
pub const BYTEWISE_COMPARER: &str = "leveldb.BytewiseComparator";

/// Metaindex entry names (`sstable/table.go`).
const META_PROPERTIES: &[u8] = b"rocksdb.properties";
const META_RANGE_DEL_V2: &[u8] = crate::rangedel::META_NAME_V2;
const META_RANGE_DEL_V1: &[u8] = crate::rangedel::META_NAME_V1;
const META_RANGE_KEY: &[u8] = b"pebble.range_key";
const META_VALUE_INDEX: &[u8] = b"pebble.value_index";
const META_FILTER_PREFIX: &[u8] = b"filter.";
const META_FULL_FILTER_PREFIX: &[u8] = b"fullfilter.";

/// The blocks the metaindex points at.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Metaindex {
    /// The properties block, which every table has.
    pub properties: Option<BlockHandle>,
    /// The v2 range-deletion block, if the table has tombstones.
    pub range_del: Option<BlockHandle>,
    /// The filter block, if the table has one.
    pub filter: Option<BlockHandle>,
    /// The value index, present when the table has value blocks.
    pub value_index: Option<BlockHandle>,
}

/// A readable Pebble sstable over bytes already in memory.
///
/// The format crate does no I/O (DESIGN.md §7), so a table is a byte slice.
/// An engine that has fetched one block instead calls [`Table::decode_block`].
#[derive(Debug)]
pub struct Table<'a> {
    bytes: &'a [u8],
    footer: Footer,
    metaindex: Metaindex,
    properties: Properties,
    two_level_index: bool,
}

impl<'a> Table<'a> {
    /// Reads a table's footer, metaindex and properties, and applies §3.
    ///
    /// Refuses, rather than half-reads: a format above the pin, a range-key
    /// block, a comparer that is not bytewise, or a two-level index (which
    /// roble will read, but does not yet).
    pub fn open(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() < FOOTER_LEN {
            return Error::truncated("table", FOOTER_LEN, bytes.len());
        }
        let footer = Footer::decode(bytes, bytes.len() as u64)?;

        let metaindex_block = decode_block_at(bytes, footer.metaindex, footer.checksum)?;
        let metaindex = parse_metaindex(&Block::new(metaindex_block.as_slice())?)?;

        // A range-key block means a key roble does not serve, and a file that
        // should not exist in an Oxia database at all (DESIGN.md §3).
        if metaindex.range_key_present {
            return Error::unsupported(
                "table has a range-key block, which Oxia never writes and roble does not read",
            );
        }
        // Pebble refuses a v1 range-deletion block too: it is unfragmented,
        // and every format roble can open is past the point where one could
        // legitimately exist (`sstable/reader.go`).
        if metaindex.range_del_v1_present {
            return Error::unsupported(
                "table has an unfragmented v1 range-deletion block (rocksdb.range_del)",
            );
        }
        let metaindex = metaindex.handles;

        let properties = match metaindex.properties {
            Some(h) => {
                let block = decode_block_at(bytes, h, footer.checksum)?;
                Properties::decode(&Block::new(block.as_slice())?)?
            }
            None => return Error::corrupt("table has no properties block"),
        };

        if let Some(name) = properties.comparer_name()
            && name != BYTEWISE_COMPARER
        {
            return Error::unsupported(format!(
                "table was written with comparer {name:?}; roble serves only {BYTEWISE_COMPARER}"
            ));
        }
        let two_level_index = properties.is_two_level_index()?;

        Ok(Table {
            bytes,
            footer,
            metaindex,
            properties,
            two_level_index,
        })
    }

    /// The table's footer.
    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    /// The blocks the metaindex points at.
    pub fn metaindex(&self) -> &Metaindex {
        &self.metaindex
    }

    /// The table's properties.
    pub fn properties(&self) -> &Properties {
        &self.properties
    }

    /// Every range tombstone in the table, in stored order.
    ///
    /// Tombstones live only in the range-deletion block, never interleaved
    /// with point keys, so a reader that walks data blocks alone sees a table
    /// with tombstones as though it had none (DESIGN.md §6).
    pub fn range_deletions(&self) -> Result<Vec<crate::rangedel::Tombstone>> {
        let Some(handle) = self.metaindex.range_del else {
            return Ok(Vec::new());
        };
        let decoded = self.decode_block(handle)?;
        crate::rangedel::decode(&Block::new(decoded.as_slice())?)
    }

    /// The compression a block was stored with, from its trailer byte.
    ///
    /// Worth having beyond a dump: a fixture that means to exercise one
    /// algorithm and quietly got another would otherwise still pass, since
    /// every algorithm decodes to the same entries.
    pub fn block_compression(&self, handle: BlockHandle) -> Result<Compression> {
        let at = usize::try_from(handle.offset + handle.length)
            .map_err(|_| Error::Corrupt(format!("block offset {} too large", handle.offset)))?;
        match self.bytes.get(at) {
            Some(&b) => Compression::from_byte(b),
            None => Error::truncated("block trailer", at + 1, self.bytes.len()),
        }
    }

    /// Decodes one block of this table by handle: checksum, then decompression.
    pub fn decode_block(&self, handle: BlockHandle) -> Result<Decoded<'a>> {
        decode_block_at(self.bytes, handle, self.footer.checksum)
    }

    /// Whether this table's index is two-level.
    ///
    /// Pebble writes one once the index outgrows a block, which a deep level
    /// reaches easily: Oxia leaves `TargetFileSizes` at Pebble's doubling
    /// default, so an L6 file runs to ~128 MiB (DESIGN.md §3).
    pub fn has_two_level_index(&self) -> bool {
        self.two_level_index
    }

    /// The handles of every data block, in key order, from the index.
    ///
    /// With a two-level index the top-level block's values are handles to
    /// index blocks rather than to data blocks, so this descends one level.
    pub fn data_block_handles(&self) -> Result<Vec<BlockHandle>> {
        let top = self.decode_block(self.footer.index)?;
        let top_block = Block::new(top.as_slice())?;
        if !self.two_level_index {
            return collect_handles(&top_block);
        }
        let mut handles = Vec::new();
        for partition in collect_handles(&top_block)? {
            let part = self.decode_block(partition)?;
            handles.extend(collect_handles(&Block::new(part.as_slice())?)?);
        }
        Ok(handles)
    }

    /// Calls `f` with every point entry of the table, in key order.
    ///
    /// A callback rather than an iterator because a key is assembled into the
    /// block iterator's buffer and a value is borrowed from the block, so
    /// neither outlives the step that produced it — the same shape as the
    /// engine's reads, which lend key and value to a closure (DESIGN.md §6).
    pub fn for_each_entry<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(crate::InternalKey<'_>, crate::block::Value<'_>) -> Result<()>,
    {
        for handle in self.data_block_handles()? {
            let decoded = self.decode_block(handle)?;
            let block = Block::new(decoded.as_slice())?;
            let mut iter = block.iter_data(self.footer.format);
            while let Some(entry) = iter.next() {
                let entry = entry?;
                if !entry.key.kind.is_accepted() {
                    return Error::unsupported(format!(
                        "key kind {} is not served by roble",
                        entry.key.kind.name()
                    ));
                }
                if entry.key.kind == Kind::Merge {
                    return Error::unsupported("MERGE keys: Oxia configures no merger");
                }
                f(entry.key, entry.value)?;
            }
        }
        Ok(())
    }
}

/// Reads the block at `handle`, verifies its checksum and decompresses it.
fn decode_block_at(
    bytes: &[u8],
    handle: BlockHandle,
    checksum: crate::ChecksumType,
) -> Result<Decoded<'_>> {
    let start = usize::try_from(handle.offset)
        .map_err(|_| Error::Corrupt(format!("block offset {} too large", handle.offset)))?;
    let len = usize::try_from(handle.length)
        .map_err(|_| Error::Corrupt(format!("block length {} too large", handle.length)))?;
    let end = start
        .checked_add(len)
        .and_then(|e| e.checked_add(TRAILER_SIZE))
        .ok_or_else(|| Error::Corrupt("block span overflows".to_string()))?;
    if end > bytes.len() {
        return Error::truncated("block", end, bytes.len());
    }
    let data = &bytes[start..start + len];
    let trailer = &bytes[start + len..end];
    let block_type = trailer[0];
    let stored = u32::from_le_bytes(trailer[1..5].try_into().expect("4 bytes"));

    let computed = checksum.compute(data, block_type)?;
    if computed != stored {
        return Err(Error::ChecksumMismatch {
            offset: handle.offset,
            stored,
            computed,
        });
    }
    crate::block::decode_compressed(Compression::from_byte(block_type)?, data)
}

/// Reads an index block's values as block handles.
fn collect_handles(block: &Block<'_>) -> Result<Vec<BlockHandle>> {
    let mut handles = Vec::new();
    let mut iter = block.iter();
    while let Some(entry) = iter.next() {
        let entry = entry?;
        let (handle, _) = BlockHandle::decode(entry.value)?;
        handles.push(handle);
    }
    Ok(handles)
}

struct ParsedMetaindex {
    handles: Metaindex,
    range_key_present: bool,
    range_del_v1_present: bool,
}

fn parse_metaindex(block: &Block<'_>) -> Result<ParsedMetaindex> {
    let mut handles = Metaindex::default();
    let mut range_key_present = false;
    let mut range_del_v1_present = false;
    let mut iter = block.iter();
    while let Some(entry) = iter.next() {
        let entry = entry?;
        let name = entry.key;
        let (handle, _) = BlockHandle::decode(entry.value)?;
        if name == META_PROPERTIES {
            handles.properties = Some(handle);
        } else if name == META_RANGE_DEL_V2 {
            handles.range_del = Some(handle);
        } else if name == META_RANGE_DEL_V1 {
            range_del_v1_present = true;
        } else if name == META_RANGE_KEY {
            range_key_present = true;
        } else if name == META_VALUE_INDEX {
            handles.value_index = Some(handle);
        } else if name.starts_with(META_FILTER_PREFIX) || name.starts_with(META_FULL_FILTER_PREFIX)
        {
            handles.filter = Some(handle);
        }
        // Any other metaindex entry is left alone rather than rejected: an
        // unknown name is not by itself a structure roble would misread.
    }
    Ok(ParsedMetaindex {
        handles,
        range_key_present,
        range_del_v1_present,
    })
}
