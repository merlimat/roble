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
use crate::key::{self, Kind};
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
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Metaindex {
    /// The properties block, which every table has.
    pub properties: Option<BlockHandle>,
    /// The v2 range-deletion block, if the table has tombstones.
    pub range_del: Option<BlockHandle>,
    /// The filter block, if the table has one.
    pub filter: Option<BlockHandle>,
    /// The value index, present when the table has value blocks.
    ///
    /// Carries the column widths as well as the block handle: the metaindex
    /// value for `pebble.value_index` is a handle *plus* three width bytes,
    /// and the index is unreadable without them.
    pub value_index: Option<crate::value_block::ValueIndex>,
}

impl Metaindex {
    /// Parses a metaindex block, applying the refusals of §3.
    ///
    /// Public for the same reason as [`index_entries_of`]: an engine with a
    /// block cache fetches the metaindex itself, and it must refuse the same
    /// tables [`Table::open`] does. Putting the refusals here rather than in
    /// the caller is what stops one path from reading a table the other turns
    /// away.
    pub fn parse(block: &Block<'_>) -> Result<Metaindex> {
        let parsed = parse_metaindex(block)?;

        // A range-key block means a key roble does not serve, and a file that
        // should not exist in an Oxia database at all.
        if parsed.range_key_present {
            return Error::unsupported(
                "table has a range-key block, which Oxia never writes and roble does not read",
            );
        }
        // Pebble refuses a v1 range-deletion block too: it is unfragmented,
        // and every format roble can open is past the point where one could
        // legitimately exist (`sstable/reader.go`).
        // Untested, with a reason rather than a test (§12), and the same
        // reason either side of the flag: no Pebble that could have written a
        // table roble opens still writes `rocksdb.range_del`. It is refused
        // because a reader that ignored the name would miss every deletion in
        // it, which is worse than refusing a table nobody has.
        if parsed.range_del_v1_present {
            return Error::unsupported(
                "table has an unfragmented v1 range-deletion block (rocksdb.range_del)",
            );
        }
        Ok(parsed.handles)
    }
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
    /// block, or a comparer that is not bytewise. A two-level index is read,
    /// not refused — [`Table::index_entries`] flattens it.
    pub fn open(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() < FOOTER_LEN {
            return Error::truncated("table", FOOTER_LEN, bytes.len());
        }
        let footer = Footer::decode(bytes, bytes.len() as u64)?;

        let metaindex_block = decode_block_at(bytes, footer.metaindex, footer.checksum)?;
        let metaindex = Metaindex::parse(&Block::new(metaindex_block.as_slice())?)?;

        let properties = match metaindex.properties {
            Some(h) => {
                let block = decode_block_at(bytes, h, footer.checksum)?;
                Properties::decode(&Block::new(block.as_slice())?)?
            }
            // Untested, with a reason (§12), as its twin in the engine is:
            // every writer that exists emits a properties block, so a
            // metaindex without one is a table built by hand. Checked anyway
            // because the comparer below is read from it, and a missing block
            // would otherwise be a silent "no comparer named".
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

    /// The value blocks' handles, read from the value index.
    ///
    /// Decoded once so that resolving many values does not re-read the index
    /// per value.
    pub fn value_blocks(&self) -> Result<Vec<BlockHandle>> {
        let Some(index) = &self.metaindex.value_index else {
            return Ok(Vec::new());
        };
        let block = self.decode_block(index.handle)?;
        index.decode_rows(block.as_slice())
    }

    /// Resolves a value-block handle into the value's bytes.
    ///
    /// `blocks` comes from [`Table::value_blocks`].
    pub fn resolve_value(&self, blocks: &[BlockHandle], handle_bytes: &[u8]) -> Result<Vec<u8>> {
        let handle = crate::value_block::Handle::decode(handle_bytes)?;
        // Untested, with a reason (§12), as its twin in the engine is: no
        // writer numbers value blocks wrongly, and reaching this needs a
        // table whose value-block index was edited after its checksum was
        // computed, or the block check fires first.
        let block_handle = blocks.get(handle.block_num as usize).ok_or_else(|| {
            Error::Corrupt(format!(
                "value handle names block {} of {}",
                handle.block_num,
                blocks.len()
            ))
        })?;
        let block = self.decode_block(*block_handle)?;
        Ok(crate::value_block::value_in_block(block.as_slice(), handle)?.to_vec())
    }

    /// The filter block's bytes, if the table has one.
    ///
    /// Pebble writes this block uncompressed, so this borrows the table rather
    /// than allocating.
    pub fn filter_block(&self) -> Result<Option<Decoded<'a>>> {
        match self.metaindex.filter {
            None => Ok(None),
            Some(h) => Ok(Some(self.decode_block(h)?)),
        }
    }

    /// The compression a block was stored with, from its trailer byte.
    ///
    /// Worth having beyond a dump: a fixture that means to exercise one
    /// algorithm and quietly got another would otherwise still pass, since
    /// every algorithm decodes to the same entries.
    pub fn block_compression(&self, handle: BlockHandle) -> Result<Compression> {
        // The `None` below is untested for the same reason as the truncation
        // check in `decode_block_at`: a handle reaching past the end is
        // refused at the footer, before anything asks what a block was
        // compressed with.
        // Checked, because the handle is the caller's: one built by hand
        // never went through `BlockHandle::decode`'s bound.
        let at = handle
            .offset
            .checked_add(handle.length)
            .and_then(|at| usize::try_from(at).ok())
            .ok_or_else(|| Error::Corrupt(format!("block handle {handle:?} is too large")))?;
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
    pub fn data_block_handles(&self) -> Result<Vec<BlockHandle>> {
        Ok(self
            .index_entries()?
            .into_iter()
            .map(|e| e.handle)
            .collect())
    }

    /// Every data block's separator and handle, in key order.
    ///
    /// With a two-level index the top-level block's values are handles to
    /// index blocks rather than to data blocks, so this descends one level and
    /// hands back the flattened sequence. A separator is an encoded internal
    /// key at or above every key in the block it bounds, which is what lets a
    /// seek skip whole blocks instead of walking entries (DESIGN.md §6).
    pub fn index_entries(&self) -> Result<Vec<IndexEntry>> {
        let top = self.decode_block(self.footer.index)?;
        let top_block = Block::new(top.as_slice())?;
        if !self.two_level_index {
            return index_entries_of(&top_block);
        }
        let mut out = Vec::new();
        for partition in index_entries_of(&top_block)? {
            let part = self.decode_block(partition.handle)?;
            out.extend(index_entries_of(&Block::new(part.as_slice())?)?);
        }
        Ok(out)
    }

    /// Calls `f` with the first entry at or after `target`, and returns
    /// whether there was one.
    ///
    /// `target` is an encoded internal key; [`key::search_key`] builds the one
    /// that finds a user key's newest version.
    ///
    /// A callback, as [`Table::for_each_entry`] is, because the key is
    /// assembled into a cursor's buffer and the value is borrowed from a block
    /// that lives only for the call. The engine's resumable iterator is built
    /// on blocks its cache owns rather than on this (§8).
    pub fn seek_ge<F>(&self, target: &[u8], mut f: F) -> Result<bool>
    where
        F: FnMut(crate::InternalKey<'_>, crate::block::Value<'_>) -> Result<()>,
    {
        let index = self.index_entries()?;
        let start = first_block_at_or_after(&index, target)?;
        // From that block forward: a target that falls past one block's last
        // key but before the next block's separator is answered by the next
        // block's first entry.
        for entry in &index[start..] {
            let decoded = self.decode_block(entry.handle)?;
            let block = Block::new(decoded.as_slice())?;
            let mut cursor = block.data_cursor(self.footer.format);
            cursor.seek_ge(&block, target)?;
            if cursor.is_valid() {
                let entry = cursor.entry(&block)?;
                check_kind(entry.key)?;
                f(entry.key, entry.value)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Calls `f` with the last entry before `target`, and returns whether
    /// there was one.
    ///
    /// This is the floor lookup of §6, the one Oxia leans hardest on.
    pub fn seek_lt<F>(&self, target: &[u8], mut f: F) -> Result<bool>
    where
        F: FnMut(crate::InternalKey<'_>, crate::block::Value<'_>) -> Result<()>,
    {
        let index = self.index_entries()?;
        if index.is_empty() {
            return Ok(false);
        }
        // The block whose separator first reaches the target may hold keys on
        // both sides of it. A target past every separator is answered by the
        // last block.
        let start = first_block_at_or_after(&index, target)?;
        let mut i = start.min(index.len() - 1);
        loop {
            let decoded = self.decode_block(index[i].handle)?;
            let block = Block::new(decoded.as_slice())?;
            let mut cursor = block.data_cursor(self.footer.format);
            cursor.seek_lt(&block, target)?;
            if cursor.is_valid() {
                let entry = cursor.entry(&block)?;
                check_kind(entry.key)?;
                f(entry.key, entry.value)?;
                return Ok(true);
            }
            // Every key in this block is at or past the target, so the answer
            // is the last entry of the block before it.
            if i == 0 {
                return Ok(false);
            }
            i -= 1;
        }
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
                check_kind(entry.key)?;
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
    // Untested, with a reason (§12): the footer refuses a handle that runs
    // past the end before any block is read, and a table cut short enough to
    // strand a data block has lost the footer with it, since the footer is
    // the last thing in the file. This is the backstop for an index that
    // names a block the file does not reach — bytes no writer produces.
    if end > bytes.len() {
        return Error::truncated("block", end, bytes.len());
    }
    // One implementation, shared with the engine's fetched-block path.
    crate::block::decode_fetched(&bytes[start..end], handle.offset, checksum)
}

/// Reads an index block's separators and block handles.
///
/// Public because an engine that fetched the index block itself has to parse
/// it the same way, and a second implementation of this is a second place for
/// a two-level index to be read wrongly.
pub fn index_entries_of(block: &Block<'_>) -> Result<Vec<IndexEntry>> {
    let mut out = Vec::new();
    // The raw iterator: an index separator is an internal key, but its value
    // is a block handle and never carries a value prefix byte.
    let mut iter = block.iter();
    while let Some(entry) = iter.next() {
        let entry = entry?;
        let (handle, _) = BlockHandle::decode(entry.value)?;
        out.push(IndexEntry {
            separator: entry.key.to_vec(),
            handle,
        });
    }
    Ok(out)
}

/// The first index entry whose separator reaches `target`, or one past the end.
///
/// Public so that an engine fetching blocks itself uses this search rather
/// than writing a second one; the unit tests below pin the equality case that
/// the golden corpus cannot reach.
///
/// "Reaches" means at or past, equality included, and the equality matters: a
/// separator is an internal key at or above every key in the block it bounds,
/// and it may *be* one of them, so a target equal to a separator is answered
/// by that block and not the next. The golden corpus cannot tell the two
/// apart, because it probes with search keys whose trailer no real separator
/// carries, so this is pinned by the unit test below instead.
pub fn first_block_at_or_after(index: &[IndexEntry], target: &[u8]) -> Result<usize> {
    let (mut lo, mut hi) = (0usize, index.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if key::compare(&index[mid].separator, target)?.is_lt() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Refuses a key roble does not serve, wherever it is met.
///
/// Public for the engine's own seeks: a kind refused when a table is scanned
/// and served when it is sought would be worse than either.
pub fn check_kind(key: crate::InternalKey<'_>) -> Result<()> {
    // MERGE first, and that order is the whole of why it is said twice.
    // `is_accepted` does not list MERGE, so the general refusal below catches
    // it too — and used to catch it first, which left this arm unreachable and
    // the reason unsaid. A MERGE key is the one unserved kind an Oxia database
    // can arrive with by configuration rather than by corruption, so it is
    // worth naming: the answer is a merger, not a different file.
    if key.kind == Kind::Merge {
        return Error::unsupported("MERGE keys: Oxia configures no merger");
    }
    if !key.kind.is_accepted() {
        return Error::unsupported(format!(
            "key kind {} is not served by roble",
            key.kind.name()
        ));
    }
    Ok(())
}

/// One entry of a table's index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    /// An encoded internal key at or above every key in the block.
    pub separator: Vec<u8>,
    /// Where the block is.
    pub handle: BlockHandle,
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
            // Set here and refused in `Metaindex::parse`, which is where the
            // reason for both is written.
            range_del_v1_present = true;
        } else if name == META_RANGE_KEY {
            range_key_present = true;
        } else if name == META_VALUE_INDEX {
            handles.value_index = Some(crate::value_block::ValueIndex::decode_metaindex_value(
                entry.value,
            )?);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockHandle;

    fn key_of(kind: Kind) -> crate::InternalKey<'static> {
        crate::InternalKey {
            user_key: b"/oxia/default/shard-0000/users/00000000",
            seq_num: 7,
            kind,
            obsolete: false,
        }
    }

    /// Every kind roble serves passes, and every kind it does not is refused
    /// with a reason that names it.
    ///
    /// Pebble puts none of the refused kinds in a data block — range keys live
    /// in a block of their own, `IngestSst` and `Excise` only in a MANIFEST —
    /// so a fixture cannot carry one and this is checked against the function.
    /// MERGE is the exception and the one that matters, and `refuse-merge.sst`
    /// carries it through the real read paths in `format/tests/golden.rs`.
    #[test]
    fn check_kind_serves_what_roble_serves_and_names_what_it_does_not() {
        for kind in [
            Kind::Set,
            Kind::Delete,
            Kind::SingleDelete,
            Kind::RangeDelete,
            Kind::Separator,
            Kind::SetWithDelete,
            Kind::DeleteSized,
        ] {
            check_kind(key_of(kind)).unwrap_or_else(|e| panic!("{kind:?} should be served: {e}"));
        }

        for kind in [
            Kind::RangeKeyDelete,
            Kind::RangeKeyUnset,
            Kind::RangeKeySet,
            Kind::IngestSst,
            Kind::Excise,
        ] {
            let Err(err) = check_kind(key_of(kind)) else {
                panic!("{kind:?} should be refused");
            };
            let text = format!("{err}");
            assert!(
                text.contains(kind.name()),
                "the refusal should name {kind:?}, got {text:?}"
            );
            assert!(
                matches!(err, Error::Unsupported(_)),
                "{kind:?}: expected Unsupported, got {err:?}"
            );
        }
    }

    /// MERGE is refused for its own reason rather than the general one.
    ///
    /// `is_accepted` does not list MERGE either, so the general arm would
    /// catch it — and did, which left the specific message unreachable. A
    /// MERGE key is the one unserved kind a database can arrive with by
    /// configuration rather than corruption, and the answer to it is a merger
    /// rather than a different file, so the reason is worth keeping.
    #[test]
    fn a_merge_key_is_refused_for_the_merger_rather_than_the_kind() {
        let Err(err) = check_kind(key_of(Kind::Merge)) else {
            panic!("MERGE should be refused");
        };
        let text = format!("{err}");
        assert!(text.contains("Oxia configures no merger"), "got {text:?}");
        assert!(
            !text.contains("is not served by roble"),
            "the general refusal should not be the one that fires, got {text:?}"
        );
    }

    fn separator(user: &[u8], seq: u64) -> IndexEntry {
        IndexEntry {
            separator: key::encode(user, seq, Kind::Set),
            handle: BlockHandle {
                offset: 0,
                length: 0,
            },
        }
    }

    #[test]
    fn the_index_search_finds_the_first_block_that_reaches_the_target() {
        let index = vec![
            separator(b"c", 9),
            separator(b"f", 7),
            separator(b"m", 5),
            separator(b"m", 2),
            separator(b"x", 1),
        ];

        // Probes before, between and past the separators, and — the case the
        // corpus cannot reach — exactly equal to each of them.
        let mut probes: Vec<Vec<u8>> = Vec::new();
        for e in &index {
            probes.push(e.separator.clone());
        }
        for user in [
            b"".as_slice(),
            b"a",
            b"c",
            b"d",
            b"f",
            b"g",
            b"m",
            b"n",
            b"x",
            b"z",
        ] {
            probes.push(key::search_key(user));
            probes.push(key::encode(user, 9, Kind::Set));
            probes.push(key::encode(user, 0, Kind::Set));
        }

        for probe in &probes {
            let want = index
                .iter()
                .position(|e| key::compare(&e.separator, probe).expect("compare").is_ge())
                .unwrap_or(index.len());
            let got = first_block_at_or_after(&index, probe).expect("search");
            assert_eq!(got, want, "searching for {probe:02x?}");
        }
    }

    #[test]
    fn a_target_equal_to_a_separator_lands_on_that_block() {
        // Stated on its own because it is the one the corpus misses: skipping
        // to the next block would lose a key equal to the separator.
        let index = vec![separator(b"c", 9), separator(b"f", 7)];
        assert_eq!(
            first_block_at_or_after(&index, &index[0].separator.clone()).expect("search"),
            0
        );
        assert_eq!(
            first_block_at_or_after(&index, &index[1].separator.clone()).expect("search"),
            1
        );
    }
}
