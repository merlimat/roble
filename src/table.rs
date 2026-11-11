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

//! An open table: the file, and the structures worth finding once (§8).
//!
//! Until now a read meant reading a whole table file and parsing its footer,
//! metaindex, properties and index again every time. This holds the file open
//! and keeps what parsing it produced — which is what §8 means by a table
//! cache: "open readers with their index and filter blocks, bounded by count,
//! since an open reader costs file descriptors as well as memory".
//!
//! Everything here is **owned**. That is not incidental: `roble_format`'s
//! `Table` borrows the whole file, so a cache that stored one would have to
//! store the bytes beside it and borrow from itself. Keeping the index,
//! filter, tombstones and value-block handles as owned values instead means
//! an open table is a plain struct an `Rc` can hold, which is also what a
//! resumable iterator will need.
//!
//! Data blocks are fetched one at a time by handle, through
//! [`roble_format::block::decode_fetched`]. That is the seam a block cache
//! goes in front of; there is no block cache yet, so a block is read from the
//! file each time it is wanted.

use std::rc::Rc;

use roble_format::block::{Block, BlockHandle, Decoded, TRAILER_SIZE, Value, decode_fetched};
use roble_format::rangedel::Tombstone;
use roble_format::sst::{
    BYTEWISE_COMPARER, IndexEntry, Metaindex, Properties, check_kind, first_block_at_or_after,
    index_entries_of,
};
use roble_format::{FOOTER_LEN, Footer};

use crate::env::{Disk, read_exact, read_tail};
use crate::error::{Error, Result};

/// One entry a seek landed on.
pub struct Entry {
    /// The encoded internal key.
    pub key: Vec<u8>,
    /// The value, with any value-block handle already followed.
    pub value: Vec<u8>,
}

/// A table, open.
pub struct OpenTable<D: Disk> {
    /// Its file number.
    pub file_num: u64,
    disk: D,
    footer: Footer,
    index: Vec<IndexEntry>,
    filter: Option<Rc<Vec<u8>>>,
    value_blocks: Vec<BlockHandle>,
    /// Its range tombstones, decoded once.
    pub tombstones: Vec<Tombstone>,
    blocks: crate::cache::BlockCache,
}

impl<D: Disk> OpenTable<D> {
    /// Reads a table's structures, leaving the file open for its blocks.
    ///
    /// The refusals are `Table::open`'s, reached through the same
    /// [`Metaindex::parse`] rather than written again: a table refused when it
    /// is read whole and accepted when it is read block by block would be
    /// worse than either.
    pub async fn open(file_num: u64, disk: D, blocks: crate::cache::BlockCache) -> Result<Self> {
        let size = disk.len().await?;
        let tail = read_tail(&disk, FOOTER_LEN.min(size as usize), Default::default()).await?;
        let footer = Footer::decode(&tail, size)?;

        let meta_bytes = fetch(&disk, footer.metaindex, footer.checksum).await?;
        let metaindex = Metaindex::parse(&Block::new(&meta_bytes)?)?;

        let Some(props_handle) = metaindex.properties else {
            return Err(Error::Format(roble_format::Error::Corrupt(
                "table has no properties block".to_string(),
            )));
        };
        let props_bytes = fetch(&disk, props_handle, footer.checksum).await?;
        let properties = Properties::decode(&Block::new(&props_bytes)?)?;
        if let Some(name) = properties.comparer_name()
            && name != BYTEWISE_COMPARER
        {
            return Err(Error::Unsupported(format!(
                "table {file_num} was written with comparer {name:?}; roble serves only \
                 {BYTEWISE_COMPARER}"
            )));
        }

        // The index, with a two-level one flattened as it is read.
        let top_bytes = fetch(&disk, footer.index, footer.checksum).await?;
        let top = index_entries_of(&Block::new(&top_bytes)?)?;
        let index = if properties.is_two_level_index()? {
            let mut flat = Vec::new();
            for partition in top {
                let part = fetch(&disk, partition.handle, footer.checksum).await?;
                flat.extend(index_entries_of(&Block::new(&part)?)?);
            }
            flat
        } else {
            top
        };

        let filter = match metaindex.filter {
            Some(h) => Some(fetch(&disk, h, footer.checksum).await?),
            None => None,
        };
        let tombstones = match metaindex.range_del {
            Some(h) => {
                let bytes = fetch(&disk, h, footer.checksum).await?;
                roble_format::rangedel::decode(&Block::new(&bytes)?)?
            }
            None => Vec::new(),
        };
        let value_blocks = match &metaindex.value_index {
            Some(vi) => {
                let bytes = fetch(&disk, vi.handle, footer.checksum).await?;
                vi.decode_rows(&bytes)?
            }
            None => Vec::new(),
        };

        Ok(OpenTable {
            file_num,
            disk,
            footer,
            index,
            filter,
            value_blocks,
            tombstones,
            blocks,
        })
    }

    /// Whether the table's filter rules `user_key` out.
    ///
    /// `true` when there is no filter: a table without one rules nothing out.
    pub fn may_contain(&self, user_key: &[u8]) -> Result<bool> {
        let Some(bits) = &self.filter else {
            return Ok(true);
        };
        Ok(match roble_format::filter::Filter::from_block(bits)? {
            Some(filter) => filter.may_contain(user_key),
            None => true,
        })
    }

    /// One block, from the cache if it is there and from the file if not.
    ///
    /// Only the blocks a read touches go through the cache: the metaindex,
    /// properties, index, filter and tombstones are read once when the table
    /// is opened and kept in this struct, so caching them as well would spend
    /// the block budget on bytes that are already held.
    async fn block(&self, handle: BlockHandle) -> Result<Rc<Vec<u8>>> {
        if let Some(held) = self.blocks.get(self.file_num, handle.offset) {
            return Ok(held);
        }
        let bytes = fetch(&self.disk, handle, self.footer.checksum).await?;
        self.blocks
            .hold(self.file_num, handle.offset, Rc::clone(&bytes));
        Ok(bytes)
    }

    /// The first entry at or after `target`.
    ///
    /// There is deliberately no backward mirror of this. It exists for `get`'s
    /// equal comparison, which is the one comparison a bloom filter can answer
    /// and so keeps a path of its own (§7) — and a point lookup only ever
    /// looks forward, for the newest version at or after its search key. The
    /// ordered comparisons are a scan, and everything that moves backwards
    /// goes through [`TableIter`], which holds its place instead of forgetting
    /// it between calls.
    pub async fn seek_ge(&self, target: &[u8]) -> Result<Option<Entry>> {
        let start = first_block_at_or_after(&self.index, target)?;
        for entry in &self.index[start..] {
            let bytes = self.block(entry.handle).await?;
            let block = Block::new(&bytes)?;
            let mut cursor = block.data_cursor(self.footer.format);
            cursor.seek_ge(&block, target)?;
            if cursor.is_valid() {
                return Ok(Some(self.entry(&block, &cursor).await?));
            }
            // The target fell past this block's last key; the answer, if any,
            // is the next block's first entry.
        }
        Ok(None)
    }

    /// Takes the entry a cursor is on, following a value-block handle.
    async fn entry(
        &self,
        block: &Block<'_>,
        cursor: &roble_format::block::restart::DataCursor,
    ) -> Result<Entry> {
        let decoded = cursor.entry(block)?;
        check_kind(decoded.key)?;
        let key = cursor.raw_key().to_vec();
        let value = match decoded.value {
            Value::InPlace(b) => b.to_vec(),
            Value::ValueHandle(h) => self.resolve(h).await?,
            Value::BlobHandle(_) => {
                return Err(Error::Unsupported(
                    "a value stored in a blob file, which is above roble's pin".to_string(),
                ));
            }
        };
        Ok(Entry { key, value })
    }

    /// Follows a value-block handle to the value it names.
    async fn resolve(&self, handle_bytes: &[u8]) -> Result<Vec<u8>> {
        let handle = roble_format::value_block::Handle::decode(handle_bytes)?;
        let block_handle = self
            .value_blocks
            .get(handle.block_num as usize)
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "a value handle in table {} names value block {} of {}",
                    self.file_num,
                    handle.block_num,
                    self.value_blocks.len()
                ))
            })?;
        let bytes = self.block(*block_handle).await?;
        Ok(roble_format::value_block::value_in_block(&bytes, handle)?.to_vec())
    }
}

/// Reads one block and gives back its decoded bytes.
///
/// A block's span is its length plus the five-byte trailer, and the trailer is
/// what carries the compression byte and the checksum — so the fetch has to
/// cover both or there is nothing to verify against.
async fn fetch<D: Disk>(
    disk: &D,
    handle: BlockHandle,
    checksum: roble_format::ChecksumType,
) -> Result<Rc<Vec<u8>>> {
    let len = usize::try_from(handle.span_len())
        .map_err(|_| Error::Corrupt(format!("block length {} too large", handle.length)))?;
    debug_assert_eq!(len, handle.length as usize + TRAILER_SIZE);
    let raw = read_exact(disk, handle.offset, len, Default::default()).await?;
    Ok(Rc::new(
        match decode_fetched(&raw, handle.offset, checksum)? {
            Decoded::Borrowed(b) => b.to_vec(),
            Decoded::Owned(v) => v,
        },
    ))
}

/// A position in one table that survives between calls.
///
/// This is what the lifetime-free cursor is for. The iterator owns the current
/// block's bytes — an `Rc` into the block cache — and a cursor beside them,
/// which a cursor that borrowed its block could not do.
///
/// Stepping off either end of a block moves to the next or previous one, so a
/// caller sees one sequence rather than a sequence of blocks. Reverse is
/// dearer than forward for the reason §4 gives: within a block, stepping back
/// rescans from a restart point.
pub struct TableIter<D: Disk> {
    table: Rc<OpenTable<D>>,
    /// Which data block the cursor is in.
    at: usize,
    /// That block's bytes, once loaded.
    bytes: Option<Rc<Vec<u8>>>,
    cursor: roble_format::block::restart::DataCursor,
}

impl<D: Disk> OpenTable<D> {
    /// An iterator over this table, positioned nowhere.
    pub fn iter(self: &Rc<Self>) -> TableIter<D> {
        TableIter {
            cursor: roble_format::block::restart::DataCursor::new(self.footer.format),
            table: Rc::clone(self),
            at: 0,
            bytes: None,
        }
    }
}

impl<D: Disk> TableIter<D> {
    /// Whether the iterator is on an entry.
    pub fn is_valid(&self) -> bool {
        self.cursor.is_valid()
    }

    /// The encoded internal key it is on.
    ///
    /// # Panics
    /// If it is not on an entry.
    pub fn key(&self) -> &[u8] {
        self.cursor.raw_key()
    }

    /// The entry it is on, with any value-block handle followed.
    pub async fn entry(&self) -> Result<Entry> {
        let bytes = self.bytes.as_ref().expect("positioned").clone();
        let block = Block::new(&bytes)?;
        self.table.entry(&block, &self.cursor).await
    }

    /// Loads block `at`, leaving the cursor unpositioned within it.
    async fn load(&mut self, at: usize) -> Result<()> {
        let handle = self.table.index[at].handle;
        self.bytes = Some(self.table.block(handle).await?);
        self.at = at;
        Ok(())
    }

    /// The block currently loaded.
    fn block(bytes: &Rc<Vec<u8>>) -> Result<Block<'_>> {
        Ok(Block::new(bytes)?)
    }

    /// Positions on the table's first entry.
    pub async fn first(&mut self) -> Result<()> {
        if self.table.index.is_empty() {
            return Ok(());
        }
        self.load(0).await?;
        let bytes = self.bytes.clone().expect("loaded");
        self.cursor.first(&Self::block(&bytes)?)?;
        // A block with no entries would leave the cursor invalid; step on.
        if !self.cursor.is_valid() {
            self.next().await?;
        }
        Ok(())
    }

    /// Positions on the table's last entry.
    pub async fn last(&mut self) -> Result<()> {
        if self.table.index.is_empty() {
            return Ok(());
        }
        self.load(self.table.index.len() - 1).await?;
        let bytes = self.bytes.clone().expect("loaded");
        self.cursor.last(&Self::block(&bytes)?)?;
        if !self.cursor.is_valid() {
            self.prev().await?;
        }
        Ok(())
    }

    /// Steps forward, crossing into the next block at the end of this one.
    #[allow(clippy::should_implement_trait)]
    pub async fn next(&mut self) -> Result<()> {
        if self.cursor.is_valid() {
            let bytes = self.bytes.clone().expect("positioned");
            self.cursor.next(&Self::block(&bytes)?)?;
            if self.cursor.is_valid() {
                return Ok(());
            }
        }
        // Off the end of this block: the next entry is the next block's
        // first. A loop rather than one step, because a block with no entries
        // would otherwise end the iteration early.
        while self.at + 1 < self.table.index.len() {
            self.load(self.at + 1).await?;
            let bytes = self.bytes.clone().expect("loaded");
            self.cursor.first(&Self::block(&bytes)?)?;
            if self.cursor.is_valid() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Steps backward, crossing into the previous block at the start of this
    /// one.
    pub async fn prev(&mut self) -> Result<()> {
        if self.cursor.is_valid() {
            let bytes = self.bytes.clone().expect("positioned");
            self.cursor.prev(&Self::block(&bytes)?)?;
            if self.cursor.is_valid() {
                return Ok(());
            }
        }
        while self.at > 0 {
            self.load(self.at - 1).await?;
            let bytes = self.bytes.clone().expect("loaded");
            self.cursor.last(&Self::block(&bytes)?)?;
            if self.cursor.is_valid() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Positions on the first entry at or after `target`.
    pub async fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        // A table can have no data blocks at all: one holding nothing but a
        // range deletion has an empty index, and Pebble writes those. The
        // other three positioning methods guard for it; this one did not,
        // and `load` indexed an empty slice.
        if self.table.index.is_empty() {
            return Ok(());
        }
        let start = first_block_at_or_after(&self.table.index, target)?;
        if start >= self.table.index.len() {
            // Past every separator: nothing in this table reaches the target.
            self.load(self.table.index.len().saturating_sub(1)).await?;
            self.cursor = roble_format::block::restart::DataCursor::new(self.table.footer.format);
            return Ok(());
        }
        self.load(start).await?;
        let bytes = self.bytes.clone().expect("loaded");
        self.cursor.seek_ge(&Self::block(&bytes)?, target)?;
        if !self.cursor.is_valid() {
            // The target fell past this block's last key; the answer, if any,
            // is the next block's first entry.
            self.next().await?;
        }
        Ok(())
    }

    /// Positions on the last entry before `target`.
    pub async fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        if self.table.index.is_empty() {
            return Ok(());
        }
        let start = first_block_at_or_after(&self.table.index, target)?;
        let at = start.min(self.table.index.len() - 1);
        self.load(at).await?;
        let bytes = self.bytes.clone().expect("loaded");
        self.cursor.seek_lt(&Self::block(&bytes)?, target)?;
        if !self.cursor.is_valid() {
            // Every key in this block is at or past the target; the answer is
            // the previous block's last entry.
            self.prev().await?;
        }
        Ok(())
    }
}

/// Whether `kind` is a deletion, and so hides the key rather than giving it a
/// value.
pub fn is_deletion(kind: roble_format::key::Kind) -> bool {
    use roble_format::key::Kind;
    matches!(kind, Kind::Delete | Kind::SingleDelete | Kind::DeleteSized)
}

/// Whether any table's range tombstone covers `user` with a sequence number
/// above `seq`.
///
/// A tombstone deletes what came before it, so this is a sequence-number
/// comparison and not a level one. Shared by `get` and by scans, because a key
/// that one hides and the other shows would be worse than either.
pub fn covered_by_tombstone<D: Disk>(
    tables: &[Rc<OpenTable<D>>],
    user: &[u8],
    seq: u64,
) -> Result<bool> {
    for table in tables {
        for t in &table.tombstones {
            if t.seq_num > seq && t.start.as_slice() <= user && user < t.end.as_slice() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
