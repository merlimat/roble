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

use roble_format::block::restart::DataCursor;

use crate::env::{Disk, read_exact, read_tail};
use crate::error::{Error, Result};

/// The refusal both forms of reading an entry give for a blob handle.
///
/// Untested, with a reason rather than a test (§12), and the reason is that an
/// earlier refusal gets there first: blob handles arrive with Pebblev6, and
/// the footer refuses anything above v4 before a block is read. This stays as
/// the backstop for the day the pin moves, where it would become reachable in
/// the same change that raised it.
fn blob_unsupported() -> Error {
    Error::Unsupported("a value stored in a blob file, which is above roble's pin".to_string())
}

/// What a value roble cannot read is refused as.
///
/// A blob handle by name, since it is the one Pebble writes above the pin;
/// anything a later roble-format can decode that this roble does not know
/// as itself, rather than read as something it is not.
fn unreadable(value: &Value<'_>) -> Error {
    match value {
        Value::BlobHandle(_) => blob_unsupported(),
        other => Error::Unsupported(format!("a value roble does not read: {other:?}")),
    }
}

/// One entry a seek landed on.
#[derive(Debug)]
pub struct Entry {
    /// The encoded internal key.
    pub key: Vec<u8>,
    /// The value, with any value-block handle already followed.
    pub value: Vec<u8>,
}

/// What a block read does with what it fetched.
///
/// A read keeps its blocks, because the next read is likely to want them. A
/// **compaction discards** them: it reads every block of its inputs exactly
/// once and never comes back, so filling the cache with them evicts the
/// blocks the foreground depends on and replaces them with bytes nobody will
/// ask for again. Pebble draws the same line, reading a compaction's blocks
/// into a buffer pool instead of the cache.
///
/// Either way the cache is **consulted**: a block that happens to be there
/// already is free, and taking it costs the foreground nothing. What harms is
/// putting one in.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Caching {
    /// Keep what was read, for the reads that follow.
    Keep,
    /// Read it and let it go.
    Discard,
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
    /// What the store that opened it records, if anything.
    metrics: Option<Rc<crate::metrics::Metrics>>,
    /// Where to give its count back when it is dropped, if the store that
    /// opened it is counting (§5, `crate::obsolete`).
    readers: Option<Rc<crate::obsolete::Readers>>,
    /// How many entries its properties block says it holds, when it says.
    ///
    /// Read while the table is being opened, so learning it costs nothing: a
    /// store that wants an estimate of the records it holds gets this one
    /// free, for every table something has had reason to open.
    pub entries: Option<u64>,
}

/// Which table, and how much of it is in memory.
impl<D: Disk> std::fmt::Debug for OpenTable<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenTable")
            .field("file_num", &self.file_num)
            .field("index_entries", &self.index.len())
            .field("filter", &self.filter.is_some())
            .field("tombstones", &self.tombstones.len())
            .finish_non_exhaustive()
    }
}

impl<D: Disk> OpenTable<D> {
    /// Reads a table's structures, leaving the file open for its blocks.
    ///
    /// The refusals are `Table::open`'s, reached through the same
    /// [`Metaindex::parse`] rather than written again: a table refused when it
    /// is read whole and accepted when it is read block by block would be
    /// worse than either.
    pub async fn open(file_num: u64, disk: D, blocks: crate::cache::BlockCache) -> Result<Self> {
        OpenTable::open_metered(file_num, disk, blocks, None, None).await
    }

    /// The same, counting every read it makes against a store's handles.
    ///
    /// Opening a table is several reads — the footer, the metaindex, the
    /// properties, the index, the filter and the tombstones — and they are
    /// the reads a store makes most of, so they are the ones worth counting.
    pub(crate) async fn open_metered(
        file_num: u64,
        disk: D,
        blocks: crate::cache::BlockCache,
        metrics: Option<Rc<crate::metrics::Metrics>>,
        readers: Option<Rc<crate::obsolete::Readers>>,
    ) -> Result<Self> {
        let size = disk.len().await?;
        let tail = read_tail(&disk, FOOTER_LEN.min(size as usize), Default::default()).await?;
        if let Some(m) = &metrics {
            m.read.add(tail.len());
        }
        let footer = Footer::decode(&tail, size)?;

        let meta_bytes = fetch(
            &disk,
            metrics.as_deref(),
            within(footer.metaindex, size)?,
            footer.checksum,
        )
        .await?;
        let metaindex = Metaindex::parse(&Block::new(&meta_bytes)?)?;

        // Untested, with a reason (§12): every writer that exists emits a
        // properties block, so a metaindex without one is a table built by
        // hand. It is checked anyway because the comparer below is read from
        // it, and a missing block there would otherwise be a silent "no
        // comparer named" rather than a refusal.
        let Some(props_handle) = metaindex.properties else {
            return Err(Error::from(roble_format::Error::Corrupt(
                "table has no properties block".to_string(),
            )));
        };
        let props_bytes = fetch(
            &disk,
            metrics.as_deref(),
            within(props_handle, size)?,
            footer.checksum,
        )
        .await?;
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
        let top_bytes = fetch(
            &disk,
            metrics.as_deref(),
            within(footer.index, size)?,
            footer.checksum,
        )
        .await?;
        let top = index_entries_of(&Block::new(&top_bytes)?)?;
        let index = if properties.is_two_level_index()? {
            let mut flat = Vec::new();
            for partition in top {
                let part = fetch(
                    &disk,
                    metrics.as_deref(),
                    within(partition.handle, size)?,
                    footer.checksum,
                )
                .await?;
                flat.extend(index_entries_of(&Block::new(&part)?)?);
            }
            flat
        } else {
            top
        };
        for entry in &index {
            within(entry.handle, size)?;
        }

        let filter = match metaindex.filter {
            Some(h) => {
                Some(fetch(&disk, metrics.as_deref(), within(h, size)?, footer.checksum).await?)
            }
            None => None,
        };
        let tombstones = match metaindex.range_del {
            Some(h) => {
                let bytes =
                    fetch(&disk, metrics.as_deref(), within(h, size)?, footer.checksum).await?;
                roble_format::rangedel::decode(&Block::new(&bytes)?)?
            }
            None => Vec::new(),
        };
        let value_blocks = match &metaindex.value_index {
            Some(vi) => {
                let bytes = fetch(
                    &disk,
                    metrics.as_deref(),
                    within(vi.handle, size)?,
                    footer.checksum,
                )
                .await?;
                let rows = vi.decode_rows(&bytes)?;
                for &h in &rows {
                    within(h, size)?;
                }
                rows
            }
            None => Vec::new(),
        };

        let entries = properties.num_entries()?;
        // Counted here and nowhere else, so that the count exists for
        // exactly as long as the reader does: a table whose open failed
        // above was never built and has nothing to give back.
        if let Some(readers) = &readers {
            readers.opened(file_num);
        }
        Ok(OpenTable {
            file_num,
            entries,
            disk,
            footer,
            index,
            filter,
            value_blocks,
            tombstones,
            blocks,
            metrics,
            readers,
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
    ///
    /// `caching` decides what becomes of a block that had to be fetched (see
    /// [`Caching`]). The lookup happens either way; only the filling is
    /// conditional.
    /// The blocks to read in one call: those around `at` in the direction a
    /// scan is walking, while they sit together on disk and fit `ahead`.
    ///
    /// Just `at` when nothing says a scan is walking at all. A table's data
    /// blocks are written one after another, so a window is usually bounded
    /// only by the budget.
    ///
    /// A gap ends it, and not for correctness: every block is cut out of the
    /// window by its own file offset, so bytes read between them would be
    /// read and ignored rather than mistaken for anything. It ends it because
    /// the budget counts the blocks and the read covers the span, and those
    /// are the same number only while the blocks sit together.
    fn window(&self, at: usize, ahead: u64, back: bool) -> (usize, usize) {
        let (mut first, mut last) = (at, at);
        let mut span = self.index[at].handle.span_len();
        loop {
            let (edge, next) = if back {
                (first == 0, first.wrapping_sub(1))
            } else {
                (last + 1 >= self.index.len(), last + 1)
            };
            if edge {
                return (first, last);
            }
            let grown = self.index[next].handle;
            let (before, after) = if back {
                (grown, self.index[first].handle)
            } else {
                (self.index[last].handle, grown)
            };
            if before.offset + before.span_len() != after.offset || span + grown.span_len() > ahead
            {
                return (first, last);
            }
            span += grown.span_len();
            if back { first = next } else { last = next }
        }
    }

    /// The block at index position `at`, taking the blocks around it in the
    /// same read when `ahead` says a scan is walking them in order.
    ///
    /// A scan reads every block of a table exactly once, so every one of its
    /// block reads is a cache miss by construction and each was a `pread` of
    /// its own: one syscall per 16 KiB, which a profile of a forward scan put
    /// at a quarter of everything it did. Reading the neighbouring blocks in
    /// the same call is what Pebble's reader does for a sequential iterator,
    /// and this is the only part of a step that grows with a record's size —
    /// a larger value is fewer records to a block, so more syscalls for each.
    ///
    /// The blocks read ahead go into the block cache, which is where the
    /// reads that follow look anyway. So nothing downstream changes, and a
    /// window that turns out to be wasted costs the cache a few blocks it
    /// would have held soon enough.
    async fn block_at(
        &self,
        at: usize,
        ahead: usize,
        back: bool,
        caching: Caching,
    ) -> Result<Rc<Vec<u8>>> {
        let handle = self.index[at].handle;
        if let Some(held) = self.blocks.get(self.file_num, handle.offset) {
            self.looked(caching, crate::metrics::BLOCK_HIT);
            return Ok(held);
        }
        self.looked(caching, crate::metrics::BLOCK_MISS);

        // A compaction reads each block once and never comes back, so filling
        // the cache for it would spend the budget on blocks nobody asks for
        // twice (`Caching`).
        let (first, last) = match caching {
            Caching::Keep => self.window(at, ahead as u64, back),
            Caching::Discard => (at, at),
        };
        if first == last {
            let bytes = fetch(
                &self.disk,
                self.metrics.as_deref(),
                handle,
                self.footer.checksum,
            )
            .await?;
            if caching == Caching::Keep {
                self.blocks
                    .hold(self.file_num, handle.offset, Rc::clone(&bytes));
            }
            return Ok(bytes);
        }

        let from = self.index[first].handle;
        let end = self.index[last].handle;
        let span = end.offset + end.span_len() - from.offset;
        let timer = self.metrics.as_ref().and_then(|m| m.read_duration.start());
        let raw = read_exact(
            &self.disk,
            from.offset,
            usize::try_from(span)
                .map_err(|_| Error::Corrupt(format!("a read of {span} bytes is too large")))?,
            Default::default(),
        )
        .await?;
        drop(timer);
        if let Some(m) = &self.metrics {
            m.read.add(raw.len());
        }
        let mut wanted = None;
        for i in first..=last {
            let h = self.index[i].handle;
            let start = usize::try_from(h.offset - from.offset).expect("inside the window");
            let stop = start + usize::try_from(h.span_len()).expect("a block that was read");
            let block = Rc::new(
                match decode_fetched(&raw[start..stop], h.offset, self.footer.checksum)? {
                    Decoded::Borrowed(b) => b.to_vec(),
                    Decoded::Owned(v) => v,
                },
            );
            if i == at {
                wanted = Some(Rc::clone(&block));
            }
            self.blocks.hold(self.file_num, h.offset, block);
        }
        Ok(wanted.expect("the block the window was asked for"))
    }

    async fn block(&self, handle: BlockHandle, caching: Caching) -> Result<Rc<Vec<u8>>> {
        if let Some(held) = self.blocks.get(self.file_num, handle.offset) {
            self.looked(caching, crate::metrics::BLOCK_HIT);
            return Ok(held);
        }
        self.looked(caching, crate::metrics::BLOCK_MISS);
        let bytes = fetch(
            &self.disk,
            self.metrics.as_deref(),
            handle,
            self.footer.checksum,
        )
        .await?;
        if caching == Caching::Keep {
            self.blocks
                .hold(self.file_num, handle.offset, Rc::clone(&bytes));
        }
        Ok(bytes)
    }

    /// Counts a cache lookup, unless it was a compaction's.
    ///
    /// A compaction reads every block of its inputs once and never returns,
    /// so its hit rate is zero by construction and counting its lookups would
    /// bury the only hit rate anybody asks about: whether the reads that come
    /// from outside found what they needed.
    fn looked(&self, caching: Caching, which: usize) {
        if caching == Caching::Discard {
            return;
        }
        if let Some(m) = &self.metrics {
            m.cache_lookups[which].add(1);
        }
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
        let mut entry = Entry {
            key: Vec::new(),
            value: Vec::new(),
        };
        let mut cursor = None;
        Ok(self
            .seek_ge_into(target, &mut cursor, &mut entry.key, &mut entry.value)
            .await?
            .then_some(entry))
    }

    /// The same, into buffers the caller keeps, answering whether there was
    /// one. Both are cleared first, and are left empty when there is none.
    ///
    /// This is the form the point lookup wants, because it is merging several
    /// tables' answers: it needs each table's key to compare before it knows
    /// whether to keep that table's value, so what it wants is a pair of
    /// buffers it can overwrite per table and swap out of when one wins. An
    /// owned `Entry` per table was two allocations per table searched, and
    /// the losers' were thrown away.
    /// `cursor` is the caller's: a block cursor assembles the prefix-compressed
    /// key it is on into a `Vec`, so building one per table searched was an
    /// allocation per table, and the only one a warm lookup had left. It is
    /// reused untouched rather than reset, because `seek_ge` places itself
    /// from the restart array and overwrites every field — a cursor last used
    /// on another block, or another table, carries nothing into this one.
    ///
    /// The format it was built for is kept beside it. A cursor cannot be
    /// asked, and the format decides whether a `SET` value carries a prefix
    /// byte, so one built for a different format is replaced rather than
    /// reused.
    pub async fn seek_ge_into(
        &self,
        target: &[u8],
        cursor: &mut Option<(roble_format::TableFormat, DataCursor)>,
        key: &mut Vec<u8>,
        value: &mut Vec<u8>,
    ) -> Result<bool> {
        let start = first_block_at_or_after(&self.index, target)?;
        for entry in &self.index[start..] {
            let bytes = self.block(entry.handle, Caching::Keep).await?;
            let block = Block::new(&bytes)?;
            if !matches!(cursor, Some((format, _)) if *format == self.footer.format) {
                *cursor = Some((self.footer.format, block.data_cursor(self.footer.format)));
            }
            let (_, cursor) = cursor.as_mut().expect("set just above");
            cursor.seek_ge(&block, target)?;
            if cursor.is_valid() {
                self.entry_into(&block, cursor, key, value).await?;
                return Ok(true);
            }
            // The target fell past this block's last key; the answer, if any,
            // is the next block's first entry.
        }
        // No entry, so the contract's "left empty when there is none" half.
        // Clearing here rather than on the way in is what makes the clear
        // inside `entry_into` the only one on the path that finds something,
        // and so the one a test can tell is missing.
        key.clear();
        value.clear();
        Ok(false)
    }

    /// Takes the entry a cursor is on, following a value-block handle.
    ///
    /// Not written in terms of [`Self::entry_into`], though it reads as
    /// though it could be. This is a scan's per-step path — `take_newest_visible`
    /// calls it for every record it returns — and routing it through the
    /// buffer form measured 5% slower on a forward scan over fourteen tables
    /// (0.946, over six paired runs, 0.907 to 0.961). The two bodies are
    /// parallel because the ownership differs, not by oversight; what they
    /// share, the refusal below, is shared.
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
            other => return Err(unreadable(&other)),
        };
        Ok(Entry { key, value })
    }

    /// The value alone, into a buffer the caller keeps, with no key built.
    ///
    /// Two callers want an entry's value and already have its key: a snapshot
    /// lookup decoded it to get here, and a compaction is writing the encoded
    /// key it already holds. Both were building a second copy of it, on every
    /// record, to drop it unread.
    ///
    /// The scan's merge is a third such caller and is deliberately **not**
    /// converted. Removing its key copy measured worth nothing on the scan
    /// itself (1.0032 over twenty paired runs) and cost `floor` 2.3% (0.977,
    /// ahead in only 4 of 20) — `floor` positions a fresh merge per lookup,
    /// so it feels changes to that loop that a long scan amortises away. The
    /// allocation is real and wasted there; taking it out is not free, which
    /// is not what either of us would have guessed.
    async fn value_into(
        &self,
        block: &Block<'_>,
        cursor: &roble_format::block::restart::DataCursor,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let decoded = cursor.entry(block)?;
        check_kind(decoded.key)?;
        out.clear();
        match decoded.value {
            Value::InPlace(b) => {
                out.reserve(b.len());
                out.extend_from_slice(b);
            }
            Value::ValueHandle(h) => self.resolve_into(h, out).await?,
            other => return Err(unreadable(&other)),
        }
        Ok(())
    }

    /// The same, into buffers the caller keeps.
    ///
    /// Both are cleared first, so a buffer handed back from a previous entry
    /// does not leave its bytes on the front of this one.
    async fn entry_into(
        &self,
        block: &Block<'_>,
        cursor: &roble_format::block::restart::DataCursor,
        key: &mut Vec<u8>,
        value: &mut Vec<u8>,
    ) -> Result<()> {
        let decoded = cursor.entry(block)?;
        check_kind(decoded.key)?;
        let raw = cursor.raw_key();
        key.clear();
        key.reserve(raw.len());
        key.extend_from_slice(raw);
        value.clear();
        match decoded.value {
            Value::InPlace(b) => {
                value.reserve(b.len());
                value.extend_from_slice(b);
            }
            Value::ValueHandle(h) => self.resolve_into(h, value).await?,
            other => return Err(unreadable(&other)),
        }
        Ok(())
    }

    /// Follows a value-block handle to the value it names.
    ///
    /// The owned form, for [`Self::entry`]. A value block holds one value, so
    /// there is nothing here for a reused buffer to save that the allocation
    /// of the returned `Vec` does not cost anyway.
    async fn resolve(&self, handle_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.resolve_into(handle_bytes, &mut out).await?;
        Ok(out)
    }

    /// Follows a value-block handle to the value it names.
    ///
    /// Appends to `out`, which the caller has already cleared: a value block
    /// holds one value and this writes one value.
    async fn resolve_into(&self, handle_bytes: &[u8], out: &mut Vec<u8>) -> Result<()> {
        let handle = roble_format::value_block::Handle::decode(handle_bytes)?;
        let block_handle = self
            .value_blocks
            // Untested, with a reason (§12). A handle naming a block the
            // table does not have cannot come from any writer: Pebble's
            // numbers them as it writes them, and roble's own writer does the
            // same. Reaching it needs a table built by hand, and one whose
            // value-block index was edited after its checksum was computed,
            // or the block check fires first.
            .get(handle.block_num as usize)
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "a value handle in table {} names value block {} of {}",
                    self.file_num,
                    handle.block_num,
                    self.value_blocks.len()
                ))
            })?;
        let bytes = self.block(*block_handle, Caching::Keep).await?;
        out.extend_from_slice(roble_format::value_block::value_in_block(&bytes, handle)?);
        Ok(())
    }
}

/// Refuses a handle that reaches past the end of a `size`-byte table.
///
/// Every block read is sized from its handle, and the handle is read from the
/// table, so one reaching past the end would size a read of bytes the file
/// does not have — and one long enough, an allocation that aborts the process
/// rather than failing. Each handle is checked here, once, as the table is
/// opened, and so the reads that follow need not check it again. Checked in
/// full, trailer included, because not every handle went through
/// `BlockHandle::decode`'s bound: a value block's is a row of fixed-width
/// integers.
fn within(handle: BlockHandle, size: u64) -> Result<BlockHandle> {
    let end = handle
        .length
        .checked_add(TRAILER_SIZE as u64)
        .and_then(|span| handle.offset.checked_add(span));
    match end {
        Some(end) if end <= size => Ok(handle),
        _ => Err(Error::from(roble_format::Error::Corrupt(format!(
            "block handle {handle:?} reaches past the end of a {size}-byte table"
        )))),
    }
}

/// Reads one block and gives back its decoded bytes.
///
/// A block's span is its length plus the five-byte trailer, and the trailer is
/// what carries the compression byte and the checksum — so the fetch has to
/// cover both or there is nothing to verify against.
async fn fetch<D: Disk>(
    disk: &D,
    metrics: Option<&crate::metrics::Metrics>,
    handle: BlockHandle,
    checksum: roble_format::ChecksumType,
) -> Result<Rc<Vec<u8>>> {
    let len = usize::try_from(handle.span_len())
        .map_err(|_| Error::Corrupt(format!("block length {} too large", handle.length)))?;
    debug_assert_eq!(len, handle.length as usize + TRAILER_SIZE);
    let timer = metrics.and_then(|m| m.read_duration.start());
    let mut raw = read_exact(disk, handle.offset, len, Default::default()).await?;
    drop(timer);
    if let Some(m) = metrics {
        m.read.add(raw.len());
    }
    let block = match decode_fetched(&raw, handle.offset, checksum)? {
        // **The read is the block, less its trailer**, so the buffer it was
        // read into is the one kept. A stored block's bytes are the first
        // `length` of its span, and an uncompressed one is those bytes as they
        // are — which is every L0 block roble writes. This used to copy them
        // out to drop the five trailing bytes: a second block-sized
        // allocation and a 16 KiB copy on every miss, for a buffer then
        // thrown away.
        Decoded::Borrowed(block) => {
            debug_assert!(
                std::ptr::eq(block.as_ptr(), raw.as_ptr()),
                "an uncompressed block is the start of the read it came in"
            );
            let kept = block.len();
            raw.truncate(kept);
            raw
        }
        Decoded::Owned(decompressed) => decompressed,
    };
    Ok(Rc::new(block))
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
/// rescans from a restart point, once an interval.
///
/// Rebuilding the block from those bytes on every step — `self` to the `Rc`
/// to the `Vec` to the data, and twice a record, once to move the cursor and
/// once to read the value it landed on — looks like the thing to remove, and
/// is not. Dropping the clone, keeping the parsed shape, and holding the
/// block beside its bytes behind `unsafe` were each built and measured:
/// together they are some 2 ns of a 28 ns step, and each one that grows this
/// struct costs a reverse scan more than it saves. What the step is actually
/// spending, and why that makes the loop the wrong place to look, is §8.
pub struct TableIter<D: Disk> {
    table: Rc<OpenTable<D>>,
    /// What this iterator's block reads do with what they fetch.
    caching: Caching,
    /// Which data block the cursor is in.
    at: usize,
    /// That block's bytes, once loaded.
    bytes: Option<Rc<Vec<u8>>>,
    cursor: roble_format::block::restart::DataCursor,
    /// How many bytes the next crossing into a neighbouring block may read,
    /// and which way the last crossing went.
    ///
    /// Zero until this iterator has crossed twice the same way, and doubling
    /// after that: a seek reads one block and wants no more, and a scan over
    /// two blocks would pay for a window it never reads. Only a crossing that
    /// follows a crossing in the same direction says the rest of the table is
    /// wanted, and says which end of it.
    ahead: usize,
    ahead_back: bool,
}

/// What a scan's second block crossing reads ahead, and where it stops
/// doubling.
///
/// The ceiling is what one `pread` can carry without the window outliving its
/// welcome in the block cache; the floor is four of the 16 KiB blocks
/// `roble_format`'s writer aims at.
const READAHEAD_FIRST: usize = 64 * 1024;
const READAHEAD_MAX: usize = 256 * 1024;

impl<D: Disk> OpenTable<D> {
    /// An iterator over this table, positioned nowhere.
    pub fn iter(self: &Rc<Self>) -> TableIter<D> {
        self.iter_with(Caching::Keep)
    }

    /// The same, on a block cursor kept from another iterator, if it was
    /// built for this table's format: a cursor assembles the key it is on
    /// into a buffer, and reusing one is reusing that buffer.
    ///
    /// Not for a table with no data blocks, such as one holding nothing but a
    /// range deletion. Positioning an iterator over that returns before it
    /// reaches the cursor, so a kept cursor would stay on the entry of the
    /// table it came from, and the iterator would offer that entry as this
    /// table's. A level of a scan that moved onto such a table did exactly
    /// that, and the merge took an older version from a deeper level instead
    /// of the one it had passed over.
    pub(crate) fn iter_reusing(
        self: &Rc<Self>,
        cursor: Option<(
            roble_format::TableFormat,
            roble_format::block::restart::DataCursor,
        )>,
    ) -> TableIter<D> {
        let cursor = match cursor {
            Some((format, cursor)) if format == self.footer.format && !self.index.is_empty() => {
                cursor
            }
            _ => roble_format::block::restart::DataCursor::new(self.footer.format),
        };
        TableIter {
            caching: Caching::Keep,
            cursor,
            table: Rc::clone(self),
            at: 0,
            bytes: None,
            ahead: 0,
            ahead_back: false,
        }
    }

    /// The same, saying what its block reads do with what they fetch.
    pub fn iter_with(self: &Rc<Self>, caching: Caching) -> TableIter<D> {
        TableIter {
            caching,
            cursor: roble_format::block::restart::DataCursor::new(self.footer.format),
            table: Rc::clone(self),
            at: 0,
            bytes: None,
            ahead: 0,
            ahead_back: false,
        }
    }
}

/// Which table, and which block of it.
impl<D: Disk> std::fmt::Debug for TableIter<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableIter")
            .field("file_num", &self.table.file_num)
            .field("block", &self.at)
            .finish_non_exhaustive()
    }
}

impl<D: Disk> TableIter<D> {
    /// Whether the iterator is on an entry.
    pub fn is_valid(&self) -> bool {
        self.cursor.is_valid()
    }

    /// The table it reads.
    pub(crate) fn table(&self) -> &Rc<OpenTable<D>> {
        &self.table
    }

    /// Its block cursor, with the format it was built for, to build another
    /// iterator on ([`OpenTable::iter_reusing`]).
    pub(crate) fn into_cursor(
        self,
    ) -> (
        roble_format::TableFormat,
        roble_format::block::restart::DataCursor,
    ) {
        (self.table.footer.format, self.cursor)
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

    /// The value the cursor is on, into a buffer the caller keeps.
    pub async fn value_into(&self, out: &mut Vec<u8>) -> Result<()> {
        let bytes = self.bytes.as_ref().expect("positioned").clone();
        let block = Block::new(&bytes)?;
        self.table.value_into(&block, &self.cursor, out).await
    }

    /// Loads block `at`, leaving the cursor unpositioned within it.
    async fn load(&mut self, at: usize) -> Result<()> {
        // Which way this load crosses, if it is a crossing at all. A seek
        // lands anywhere and says nothing about what comes next.
        let back = match self.bytes {
            Some(_) if at == self.at + 1 => Some(false),
            Some(_) if at + 1 == self.at => Some(true),
            _ => None,
        };
        let ahead = match back {
            Some(b) if b == self.ahead_back => self.ahead,
            _ => 0,
        };
        self.bytes = Some(
            self.table
                .block_at(at, ahead, back == Some(true), self.caching)
                .await?,
        );
        match back {
            Some(b) => {
                self.ahead = (ahead.max(READAHEAD_FIRST / 2) * 2).min(READAHEAD_MAX);
                self.ahead_back = b;
            }
            None => self.ahead = 0,
        }
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
    Ok(tables
        .iter()
        .any(|table| covering_tombstone(table, user, seq).is_some()))
}

/// The range tombstone of `table` that covers `user` with a sequence number
/// above `seq`, if it has one: what [`covered_by_tombstone`] asks, with the
/// tombstone that answered, whose span a scan can then skip whole.
pub(crate) fn covering_tombstone<'t, D: Disk>(
    table: &'t OpenTable<D>,
    user: &[u8],
    seq: u64,
) -> Option<&'t Tombstone> {
    table
        .tombstones
        .iter()
        .find(|t| t.seq_num > seq && t.start.as_slice() <= user && user < t.end.as_slice())
}

/// Gives back the reason the obsolete sweep had to leave this table's file
/// alone (§5, `crate::obsolete`).
///
/// The last reader of a table the version has stopped naming is what the
/// sweep is waiting for, and this is the only thing that knows when it has
/// gone.
impl<D: Disk> Drop for OpenTable<D> {
    fn drop(&mut self) {
        if let Some(readers) = &self.readers {
            readers.closed(self.file_num);
        }
    }
}
