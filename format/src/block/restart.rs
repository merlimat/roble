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

//! Seeking inside a block, forwards and backwards (DESIGN.md §7).
//!
//! A block's entries are prefix-compressed against the entry before them, so
//! the bytes at a given offset cannot be read on their own: a key is only
//! meaningful as the tail of a chain. The restart array is what makes the
//! block searchable anyway. Every `restart_interval`-th entry stores its key
//! whole, and the array holds those entries' offsets in order, so a binary
//! search over the array lands in the right interval and a short forward scan
//! finishes the job.
//!
//! Three things in here are easy to get wrong, and each is wrong in a way that
//! returns an answer rather than an error:
//!
//! - **A restart entry's key must be stored whole.** The binary search decodes
//!   restart entries without a previous key to chain from, so a non-zero
//!   shared length there is refused rather than read as an empty prefix, which
//!   would silently truncate the key it compares against.
//! - **Stepping backwards is a rescan, not a reverse decode.** There is no way
//!   to walk entries backwards, so [`Cursor::prev`] finds the restart point
//!   before the current entry and scans forward to the entry that ends where
//!   the current one begins. Reverse iteration is therefore dearer than
//!   forward, which is the cost §4 says to measure.
//! - **The search is for the *last* restart at or before the target**, not the
//!   first one after it. Landing one interval late skips entries that belong to
//!   the answer, and a scan that starts too early is merely slower.

use std::ops::Range;

use crate::TableFormat;
use crate::block::{Block, Entry, decode_value};
use crate::error::{Error, Result};
use crate::key::{self, InternalKey};
use crate::varint;

/// One entry's layout within a block's entry region.
struct Raw {
    /// How many bytes it shares with the previous key.
    shared: usize,
    /// Where its fresh key bytes are.
    fresh: Range<usize>,
    /// Where its value is.
    value: Range<usize>,
    /// Where the next entry begins.
    end: usize,
}

/// Decodes the entry header at `pos`, without assembling its key.
fn decode_entry(entries: &[u8], pos: usize) -> Result<Raw> {
    let src = &entries[pos..];
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
    let key_at = pos + header;
    let value_at = key_at + unshared;
    Ok(Raw {
        shared,
        fresh: key_at..value_at,
        value: value_at..value_at + value_len,
        end: pos + need,
    })
}

/// The parts of a block a cursor works over.
struct Parts<'a> {
    entries: &'a [u8],
    restarts: &'a [u8],
    num_restarts: usize,
}

fn parts_of<'a>(block: &Block<'a>) -> Parts<'a> {
    let (entries, restarts, num_restarts) = block.parts();
    Parts {
        entries,
        restarts,
        num_restarts,
    }
}

impl<'a> Parts<'a> {
    /// The offset of restart point `i`, with the stolen high bit masked off.
    fn restart_offset(&self, i: usize) -> Result<usize> {
        // Untested, with a reason rather than a test (§12). Every caller
        // derives `i` from `num_restarts` — the binary searches bound
        // themselves by it and `last` takes the final one — so an index past
        // the end is a bug in this file rather than bytes off a disk. It
        // stays because the array is indexed without bounds checks below it,
        // and a wrong index there would read the entries as offsets.
        if i >= self.num_restarts {
            return Error::corrupt(format!(
                "restart {i} of a block with {} restarts",
                self.num_restarts
            ));
        }
        let at = i * 4;
        let raw = u32::from_le_bytes(self.restarts[at..at + 4].try_into().expect("4 bytes"));
        // The high bit is `setHasSameKeyPrefix`, not part of the offset.
        let offset = (raw & !super::SET_HAS_SAME_KEY_PREFIX_MASK) as usize;
        if offset > self.entries.len() {
            return Error::corrupt(format!(
                "restart {i} points to {offset}, past the {} bytes of entries",
                self.entries.len()
            ));
        }
        Ok(offset)
    }

    /// The whole key stored at restart point `i`.
    ///
    /// Refuses a restart entry that claims a shared prefix: there is no
    /// previous key to take one from, and reading it as empty would compare a
    /// truncated key during the binary search.
    fn restart_key(&self, i: usize) -> Result<&'a [u8]> {
        let pos = self.restart_offset(i)?;
        let raw = decode_entry(self.entries, pos)?;
        if raw.shared != 0 {
            return Error::corrupt(format!(
                "restart {i} shares {} bytes with a previous key, but a restart entry \
                 stores its key whole",
                raw.shared
            ));
        }
        Ok(&self.entries[raw.fresh])
    }
}

/// A position within a block, able to seek and to step either way.
///
/// **The cursor does not borrow the block.** It holds a position and the key
/// assembled at it, and every operation is given the block to work over. That
/// is what lets something else own the block's bytes — the block cache owns
/// them (§8), and an iterator that outlives a single call holds an `Rc` to
/// them and a cursor beside it. A cursor that borrowed the block could not sit
/// in the same struct as the bytes it borrows.
///
/// The caller is responsible for passing the same block each time. Passing a
/// different one resumes at an offset that means nothing in it, which the
/// entry decoder will usually refuse and may not: this is a cursor, not a
/// handle, and it is private to the iterator that owns it.
///
/// Keys are left as raw bytes; [`DataCursor`] is the wrapper that decodes
/// internal keys and the Pebblev3+ value prefix. Keeping the two apart is what
/// stops an index block's separator — which may be a `SET` key — from having a
/// byte stripped off the handle beside it.
#[derive(Clone, Debug, Default)]
pub struct Cursor {
    /// Where the current entry starts, and where it ends.
    pos: usize,
    end: usize,
    /// The assembled key of the current entry.
    key: Vec<u8>,
    value: Range<usize>,
    valid: bool,
}

impl Cursor {
    /// A cursor positioned nowhere.
    pub fn new() -> Self {
        Cursor::default()
    }

    /// Whether the cursor is on an entry.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// The current entry's key, assembled.
    ///
    /// # Panics
    /// If the cursor is not on an entry; check [`Cursor::is_valid`] first.
    pub fn key(&self) -> &[u8] {
        assert!(self.valid, "key() on a cursor that is not on an entry");
        &self.key
    }

    /// The current entry's value, as stored, borrowed from `block`.
    ///
    /// # Panics
    /// If the cursor is not on an entry.
    pub fn value<'b>(&self, block: &Block<'b>) -> &'b [u8] {
        assert!(self.valid, "value() on a cursor that is not on an entry");
        let (entries, _, _) = block.parts();
        &entries[self.value.clone()]
    }

    /// Places the cursor on the entry at `pos`, chaining from the key already
    /// held when `keep_key` is set and starting fresh otherwise.
    fn place(&mut self, parts: &Parts<'_>, pos: usize, keep_key: bool) -> Result<()> {
        let raw = decode_entry(parts.entries, pos)?;
        if !keep_key {
            // A fresh placement needs no previous key, and `truncate(0)`
            // below empties what is held — so there is nothing to clear here,
            // only something to refuse.
            if raw.shared != 0 {
                return Error::corrupt(format!(
                    "entry at {pos} shares {} bytes with no previous key",
                    raw.shared
                ));
            }
        } else if raw.shared > self.key.len() {
            return Error::corrupt(format!(
                "block entry shares {} bytes with a {}-byte previous key",
                raw.shared,
                self.key.len()
            ));
        }
        self.key.truncate(raw.shared);
        self.key.extend_from_slice(&parts.entries[raw.fresh]);
        self.pos = pos;
        self.end = raw.end;
        self.value = raw.value;
        self.valid = true;
        Ok(())
    }

    /// Positions on the first entry.
    pub fn first(&mut self, block: &Block<'_>) -> Result<()> {
        let parts = parts_of(block);
        if parts.entries.is_empty() {
            self.valid = false;
            return Ok(());
        }
        self.place(&parts, 0, false)
    }

    /// Positions on the last entry.
    pub fn last(&mut self, block: &Block<'_>) -> Result<()> {
        let parts = parts_of(block);
        if parts.entries.is_empty() {
            self.valid = false;
            return Ok(());
        }
        // From the final restart point, which is the cheapest place to start,
        // and forward to the end.
        self.place(&parts, parts.restart_offset(parts.num_restarts - 1)?, false)?;
        while self.end < parts.entries.len() {
            let pos = self.end;
            self.place(&parts, pos, true)?;
        }
        Ok(())
    }

    /// Steps to the next entry, becoming invalid at the end of the block.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self, block: &Block<'_>) -> Result<()> {
        if !self.valid {
            return Ok(());
        }
        let parts = parts_of(block);
        if self.end >= parts.entries.len() {
            self.valid = false;
            return Ok(());
        }
        let pos = self.end;
        self.place(&parts, pos, true)
    }

    /// Steps to the previous entry, becoming invalid before the first.
    ///
    /// This rescans from the restart point before the current entry, because
    /// prefix compression only runs forwards.
    pub fn prev(&mut self, block: &Block<'_>) -> Result<()> {
        if !self.valid {
            return Ok(());
        }
        let target = self.pos;
        if target == 0 {
            self.valid = false;
            return Ok(());
        }
        let parts = parts_of(block);

        // The last restart point that starts strictly before the current
        // entry. Offsets ascend, so this is a binary search.
        let mut found = None;
        let (mut lo, mut hi) = (0usize, parts.num_restarts);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if parts.restart_offset(mid)? < target {
                found = Some(mid);
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let Some(restart) = found else {
            // Nothing starts before the current entry, so it is the first.
            self.valid = false;
            return Ok(());
        };

        self.place(&parts, parts.restart_offset(restart)?, false)?;
        while self.end < target {
            let pos = self.end;
            self.place(&parts, pos, true)?;
        }
        // Untested, with a reason (§12). Overshooting means a restart
        // offset that does not fall on an entry boundary, and the scan from
        // it decodes whatever is at that offset as an entry — which the
        // checks in `place` and `decode_entry` refuse first, for any bytes
        // that could be built to reach here. It stays as the one that would
        // catch a misalignment those two happened to accept, since the
        // alternative is a cursor quietly on the wrong entry.
        if self.end != target {
            return Error::corrupt(format!(
                "scanning back from restart {restart} overshot: an entry ends at {}, not {target}",
                self.end
            ));
        }
        Ok(())
    }

    /// Positions on the first entry whose key is greater than or equal to
    /// `target`, which is an encoded internal key.
    ///
    /// Becomes invalid when every key in the block is smaller.
    pub fn seek_ge(&mut self, block: &Block<'_>, target: &[u8]) -> Result<()> {
        let parts = parts_of(block);
        if parts.entries.is_empty() {
            self.valid = false;
            return Ok(());
        }

        // The last restart whose key is at or before the target. Starting one
        // interval early only costs a few comparisons; starting one late skips
        // entries that belong to the answer.
        //
        // When even the first restart's key is past the target this settles on
        // 0, which is also the right place to start: the answer is then the
        // block's very first entry.
        //
        // `is_le` rather than `is_lt` is therefore a speed choice, not a
        // correctness one, and no test pins it: with `is_lt` a restart whose
        // key equals the target starts the scan one interval earlier and the
        // forward scan below still arrives at the same entry. The direction
        // that would be wrong is starting late, and that is pinned.
        let (mut lo, mut hi) = (0usize, parts.num_restarts - 1);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            if key::compare(parts.restart_key(mid)?, target)?.is_le() {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }

        self.place(&parts, parts.restart_offset(lo)?, false)?;
        while key::compare(&self.key, target)?.is_lt() {
            self.next(block)?;
            if !self.valid {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Positions on the last entry whose key is strictly less than `target`.
    ///
    /// Becomes invalid when every key in the block is at or past it.
    pub fn seek_lt(&mut self, block: &Block<'_>, target: &[u8]) -> Result<()> {
        self.seek_ge(block, target)?;
        if self.valid {
            self.prev(block)
        } else {
            self.last(block)
        }
    }
}

/// A [`Cursor`] over a **data** block, decoding internal keys and the
/// Pebblev3+ value prefix.
#[derive(Clone, Debug)]
pub struct DataCursor {
    inner: Cursor,
    format: TableFormat,
}

impl DataCursor {
    /// A data cursor positioned nowhere.
    ///
    /// `format` decides whether a `SET` value carries a prefix byte, so the
    /// wrong one shifts every value by a byte.
    pub fn new(format: TableFormat) -> Self {
        DataCursor {
            inner: Cursor::new(),
            format,
        }
    }

    /// Whether the cursor is on an entry.
    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    /// The current entry's encoded internal key.
    pub fn raw_key(&self) -> &[u8] {
        self.inner.key()
    }

    /// The current entry, with its key and value decoded.
    pub fn entry<'b>(&self, block: &Block<'b>) -> Result<Entry<'b, '_>> {
        let key = InternalKey::decode(self.inner.key())?;
        let value = decode_value(self.format, key, self.inner.value(block))?;
        Ok(Entry { key, value })
    }

    /// Positions on the first entry.
    pub fn first(&mut self, block: &Block<'_>) -> Result<()> {
        self.inner.first(block)
    }

    /// Positions on the last entry.
    pub fn last(&mut self, block: &Block<'_>) -> Result<()> {
        self.inner.last(block)
    }

    /// Steps forward.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self, block: &Block<'_>) -> Result<()> {
        self.inner.next(block)
    }

    /// Steps backward.
    pub fn prev(&mut self, block: &Block<'_>) -> Result<()> {
        self.inner.prev(block)
    }

    /// Positions on the first entry at or after `target`.
    pub fn seek_ge(&mut self, block: &Block<'_>, target: &[u8]) -> Result<()> {
        self.inner.seek_ge(block, target)
    }

    /// Positions on the last entry before `target`.
    pub fn seek_lt(&mut self, block: &Block<'_>, target: &[u8]) -> Result<()> {
        self.inner.seek_lt(block, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockBuilder;
    use crate::key::Kind;

    /// Builds a block from `keys`, each key given a value derived from it.
    ///
    /// `same_key_prefix` is passed through so that a block with the
    /// `setHasSameKeyPrefix` high bit set on its restart offsets can be built:
    /// reading those offsets without masking the bit gives an offset tens of
    /// millions of bytes past the block, and the only way to notice is to have
    /// such a block to read.
    fn build(keys: &[Vec<u8>], restart_interval: usize, same_key_prefix: bool) -> Vec<u8> {
        let mut b = BlockBuilder::new(restart_interval);
        for key in keys {
            let value = format!("v:{}", key.len());
            b.add(key, value.as_bytes(), usize::MAX, None, same_key_prefix);
        }
        b.finish().to_vec()
    }

    /// Internal keys for `users`, all at the same sequence number.
    fn internal(users: &[&[u8]]) -> Vec<Vec<u8>> {
        users
            .iter()
            .map(|u| crate::key::encode(u, 7, Kind::Set))
            .collect()
    }

    /// Every entry in the block, read forwards with the cursor.
    fn forwards(block: &Block<'_>) -> Vec<Vec<u8>> {
        let mut c = block.cursor();
        c.first(block).expect("first");
        let mut out = Vec::new();
        while c.is_valid() {
            out.push(c.key().to_vec());
            c.next(block).expect("next");
        }
        out
    }

    /// Every entry in the block, read backwards with the cursor.
    fn backwards(block: &Block<'_>) -> Vec<Vec<u8>> {
        let mut c = block.cursor();
        c.last(block).expect("last");
        let mut out = Vec::new();
        while c.is_valid() {
            out.push(c.key().to_vec());
            c.prev(block).expect("prev");
        }
        out
    }

    #[test]
    fn a_cursor_walks_a_block_the_same_both_ways() {
        let users: &[&[u8]] = &[
            b"a", b"ab", b"abc", b"b", b"bb", b"c", b"cc", b"ccc", b"d", b"e",
        ];
        let keys = internal(users);
        for interval in [1usize, 2, 3, 4, 16] {
            for same_prefix in [false, true] {
                let bytes = build(&keys, interval, same_prefix);
                let block = Block::new(&bytes).expect("block");
                assert_eq!(
                    forwards(&block),
                    keys,
                    "forward, interval {interval}, same_prefix {same_prefix}"
                );
                let mut reversed = keys.clone();
                reversed.reverse();
                assert_eq!(
                    backwards(&block),
                    reversed,
                    "reverse, interval {interval}, same_prefix {same_prefix}"
                );
            }
        }
    }

    #[test]
    fn seek_agrees_with_a_brute_force_search() {
        // The oracle is the key list itself: with the ordering settled in
        // key.rs, the answer to a seek is a one-line scan of a sorted slice,
        // so the cursor is checked against that rather than against itself.
        let users: &[&[u8]] = &[b"b", b"bb", b"bbb", b"d", b"dd", b"f", b"ff", b"h"];
        let keys = internal(users);

        // Probes before, between, on and after every key, plus keys sharing a
        // prefix, which is where prefix compression is most likely to confuse
        // a comparison.
        let mut probes: Vec<Vec<u8>> = Vec::new();
        for u in [
            b"".as_slice(),
            b"a",
            b"b",
            b"ba",
            b"bb",
            b"bba",
            b"bbb",
            b"bbbb",
            b"c",
            b"d",
            b"dd",
            b"e",
            b"f",
            b"ff",
            b"g",
            b"h",
            b"hh",
            b"z",
        ] {
            probes.push(crate::key::search_key(u));
            probes.push(crate::key::encode(u, 7, Kind::Set));
            probes.push(crate::key::encode(u, 0, Kind::Set));
        }

        for interval in [1usize, 2, 3, 16] {
            let bytes = build(&keys, interval, false);
            let block = Block::new(&bytes).expect("block");

            for probe in &probes {
                let want_ge = keys
                    .iter()
                    .position(|k| crate::key::compare(k, probe).expect("compare").is_ge());
                let mut c = block.cursor();
                c.seek_ge(&block, probe).expect("seek_ge");
                match want_ge {
                    Some(i) => {
                        assert!(c.is_valid(), "seek_ge should land, interval {interval}");
                        assert_eq!(c.key(), &keys[i][..], "seek_ge, interval {interval}");
                    }
                    None => assert!(
                        !c.is_valid(),
                        "seek_ge past the end should not land, interval {interval}"
                    ),
                }

                let want_lt = keys
                    .iter()
                    .rposition(|k| crate::key::compare(k, probe).expect("compare").is_lt());
                let mut c = block.cursor();
                c.seek_lt(&block, probe).expect("seek_lt");
                match want_lt {
                    Some(i) => {
                        assert!(c.is_valid(), "seek_lt should land, interval {interval}");
                        assert_eq!(c.key(), &keys[i][..], "seek_lt, interval {interval}");
                    }
                    None => assert!(
                        !c.is_valid(),
                        "seek_lt before the start should not land, interval {interval}"
                    ),
                }
            }
        }
    }

    #[test]
    fn a_seek_can_be_stepped_away_from_in_either_direction() {
        // A seek that leaves the cursor unable to walk on is the bug reverse
        // iteration actually has: the position looks right and the step after
        // it is wrong.
        let users: &[&[u8]] = &[b"a", b"c", b"e", b"g", b"i", b"k", b"m"];
        let keys = internal(users);
        for interval in [1usize, 2, 3] {
            let bytes = build(&keys, interval, false);
            let block = Block::new(&bytes).expect("block");

            for (i, key) in keys.iter().enumerate() {
                let mut c = block.cursor();
                c.seek_ge(&block, key).expect("seek_ge");
                assert_eq!(c.key(), &key[..]);

                c.next(&block).expect("next");
                if i + 1 < keys.len() {
                    assert_eq!(c.key(), &keys[i + 1][..], "forward from {i}");
                    c.prev(&block).expect("prev");
                    assert_eq!(c.key(), &key[..], "back again to {i}");
                } else {
                    assert!(!c.is_valid(), "past the last entry");
                }

                let mut c = block.cursor();
                c.seek_ge(&block, key).expect("seek_ge");
                c.prev(&block).expect("prev");
                if i > 0 {
                    assert_eq!(c.key(), &keys[i - 1][..], "backward from {i}");
                } else {
                    assert!(!c.is_valid(), "before the first entry");
                }
            }
        }
    }

    #[test]
    fn a_trailer_orders_newest_first_within_one_user_key() {
        // Several versions of one key, which is the ordinary state of an LSM
        // block. Descending trailers mean a forward scan meets the newest
        // first, and a seek to the user key lands on it.
        let mut b = BlockBuilder::new(2);
        let versions: Vec<Vec<u8>> = [9u64, 5, 1]
            .iter()
            .map(|&seq| crate::key::encode(b"k", seq, Kind::Set))
            .collect();
        for v in &versions {
            b.add(v, b"x", usize::MAX, None, true);
        }
        let bytes = b.finish().to_vec();
        let block = Block::new(&bytes).expect("block");
        assert_eq!(forwards(&block), versions, "newest first");

        let mut c = block.cursor();
        c.seek_ge(&block, &crate::key::search_key(b"k"))
            .expect("seek");
        assert_eq!(
            c.key(),
            &versions[0][..],
            "a seek to the user key lands on its newest version"
        );
    }

    #[test]
    fn the_search_trailer_is_pebbles_kind_max() {
        // SEARCH_TRAILER is written as a literal because `to_byte` is not a
        // const fn, so the number is checked here instead.
        assert_eq!(
            crate::key::SEARCH_TRAILER,
            (crate::key::SEQ_NUM_MAX << 8) | Kind::Excise.to_byte() as u64
        );
    }

    /// A block from entry bytes and restart offsets, neither of which has to
    /// make sense.
    ///
    /// `BlockBuilder` only produces blocks that do, which is what makes it
    /// useless for the checks below: every one of them is about bytes a
    /// writer would never emit, arriving off a disk that did not keep them.
    fn raw(entries: &[u8], restarts: &[u32]) -> Vec<u8> {
        let mut out = entries.to_vec();
        for r in restarts {
            out.extend_from_slice(&r.to_le_bytes());
        }
        out.extend_from_slice(&(restarts.len() as u32).to_le_bytes());
        out
    }

    /// One entry's bytes. Lengths below 128 are single-byte varints.
    fn entry(shared: u8, unshared: u8, value_len: u8, rest: &[u8]) -> Vec<u8> {
        let mut e = vec![shared, unshared, value_len];
        e.extend_from_slice(rest);
        e
    }

    #[test]
    fn a_block_with_no_entries_leaves_the_cursor_nowhere() {
        let data = raw(&[], &[0]);
        let block = Block::new(&data).expect("a restart array and nothing else");
        let mut c = Cursor::new();
        c.first(&block).expect("first");
        assert!(!c.is_valid(), "first");
        c.last(&block).expect("last");
        assert!(!c.is_valid(), "last");
    }

    #[test]
    fn stepping_an_unpositioned_cursor_does_nothing() {
        // Both are reached by a caller that steps without checking, which is
        // every caller that has just run off one end.
        let data = raw(&entry(0, 1, 0, b"a"), &[0]);
        let block = Block::new(&data).expect("a block");
        let mut c = Cursor::new();
        assert!(!c.is_valid());
        c.next(&block).expect("next");
        assert!(!c.is_valid(), "next");
        c.prev(&block).expect("prev");
        assert!(!c.is_valid(), "prev");
    }

    #[test]
    fn an_entry_sharing_with_a_key_that_is_not_there_is_refused() {
        // At a fresh placement there is no previous key, so a shared count is
        // not a short key but a lie about one.
        let data = raw(&entry(3, 1, 0, b"a"), &[0]);
        let block = Block::new(&data).expect("a block");
        let err = Cursor::new()
            .first(&block)
            .expect_err("sharing with nothing");
        assert!(
            format!("{err}").contains("shares 3 bytes with no previous key"),
            "got {err:?}"
        );
    }

    #[test]
    fn an_entry_sharing_more_than_the_previous_key_holds_is_refused() {
        // Continuing from a one-byte key, claiming five shared bytes. Taking
        // it at its word would read past the key and build one from whatever
        // followed it in memory.
        let mut entries = entry(0, 1, 0, b"a");
        entries.extend_from_slice(&entry(5, 1, 0, b"b"));
        let data = raw(&entries, &[0]);
        let block = Block::new(&data).expect("a block");
        let mut c = Cursor::new();
        c.first(&block).expect("first");
        let err = c.next(&block).expect_err("sharing too much");
        assert!(
            format!("{err}").contains("shares 5 bytes with a 1-byte previous key"),
            "got {err:?}"
        );
    }

    #[test]
    fn a_restart_point_outside_the_entries_is_refused() {
        // `Block::new` counts the restart array and checks it fits; it does
        // not read the offsets in it.
        let data = raw(&entry(0, 1, 0, b"a"), &[100]);
        let block = Block::new(&data).expect("the array fits");
        // `last` reads the final restart offset; `first` places at zero
        // without consulting one, so it would not meet this.
        let err = Cursor::new()
            .last(&block)
            .expect_err("a restart past the entries");
        assert!(
            format!("{err}").contains("points to 100, past the 4 bytes of entries"),
            "got {err:?}"
        );
    }

    #[test]
    fn a_restart_entry_that_claims_a_shared_prefix_is_refused() {
        // The binary search compares restart keys, so one that is not stored
        // whole would be compared truncated and send the search the wrong way
        // — which is a wrong answer rather than an error, unless it is caught
        // here.
        // Three restarts, so the search has a middle to land on: with one
        // or two it never asks for a restart key at all.
        let mut entries = entry(0, 1, 0, b"a");
        entries.extend_from_slice(&entry(2, 1, 0, b"b"));
        entries.extend_from_slice(&entry(0, 1, 0, b"c"));
        let data = raw(&entries, &[0, 4, 8]);
        let block = Block::new(&data).expect("a block");
        let err = Cursor::new()
            .seek_ge(&block, b"c")
            .expect_err("a restart key that is not whole");
        assert!(
            format!("{err}").contains("a restart entry stores its key whole"),
            "got {err:?}"
        );
    }

    #[test]
    fn stepping_back_from_an_entry_no_restart_precedes_ends_the_walk() {
        // The only restart starts at the second entry, so stepping back from
        // it finds nothing before it and the cursor runs off the front.
        let mut entries = entry(0, 1, 0, b"a");
        entries.extend_from_slice(&entry(0, 1, 0, b"b"));
        let data = raw(&entries, &[4]);
        let block = Block::new(&data).expect("a block");
        let mut c = Cursor::new();
        c.first(&block).expect("first");
        c.next(&block).expect("next");
        assert!(c.is_valid(), "the second entry");
        c.prev(&block).expect("prev");
        assert!(!c.is_valid(), "nothing precedes it");
    }

    #[test]
    fn an_entry_that_runs_past_the_block_is_refused() {
        // A hundred bytes of key where four bytes of entries exist. Under
        // 128, so the length is still the one-byte varint this helper writes.
        let data = raw(&entry(0, 100, 0, b"a"), &[0]);
        let block = Block::new(&data).expect("a block");
        let err = Cursor::new()
            .first(&block)
            .expect_err("an entry past the end");
        assert!(matches!(err, Error::Truncated { .. }), "got {err:?}");
    }
}
