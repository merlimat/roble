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

//! The v2 range-deletion block (`sstable/rowblk_writer.go`: `addTombstone`,
//! and `sstable/table.go`: `metaRangeDelV2Name`).
//!
//! The block is an ordinary row block in which each entry is one tombstone:
//! the key is an internal key whose user key is the tombstone's **start** and
//! whose kind is `RANGEDEL`, and the value is the tombstone's **end** user key,
//! exclusive. Pebble always writes it uncompressed.
//!
//! Three things are easy to get wrong:
//!
//! 1. **The metaindex name is `rocksdb.range_del2`**, not anything with
//!    `pebble.` in it. `rocksdb.range_del` is the unfragmented v1 block, which
//!    Pebble itself refuses, so roble refuses it too.
//! 2. **Tombstones live only here**, never interleaved with point keys in a
//!    data block. A reader that walks data blocks alone sees a table with
//!    tombstones as though it had none.
//! 3. **They are fragmented.** Within the block, tombstones are sorted by
//!    start key; two with the same start must have the same end and strictly
//!    decreasing sequence numbers; otherwise one's end must not run past the
//!    next one's start. Pebble checks this when writing, so a reader can rely
//!    on it — and roble checks it too, rather than producing a block Go Pebble
//!    would accept and then read as a different set of ranges.

use crate::block::{Block, BlockBuilder, INDEX_RESTART_INTERVAL};
use crate::error::{Error, Result};
use crate::key::{InternalKey, Kind, SEQ_NUM_MAX, TRAILER_LEN};

/// `metaRangeDelV2Name`: the fragmented range-deletion block.
pub const META_NAME_V2: &[u8] = b"rocksdb.range_del2";

/// `metaRangeDelV1Name`: the unfragmented block, which Pebble refuses.
pub const META_NAME_V1: &[u8] = b"rocksdb.range_del";

/// One range tombstone: `[start, end)` deleted as of `seq_num`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    /// Inclusive start of the deleted range.
    pub start: Vec<u8>,
    /// Exclusive end of the deleted range.
    pub end: Vec<u8>,
    /// The sequence number at which the range was deleted.
    pub seq_num: u64,
}

/// Decodes every tombstone in a v2 range-deletion block, in stored order.
///
/// Tombstones are returned owned rather than borrowed: a block's keys are
/// prefix-compressed, so a tombstone's start has to be assembled anyway, and
/// a table holds few enough tombstones that lending them through a closure
/// would buy nothing.
pub fn decode(block: &Block<'_>) -> Result<Vec<Tombstone>> {
    let mut out: Vec<Tombstone> = Vec::new();
    let mut iter = block.iter();
    while let Some(entry) = iter.next() {
        let entry = entry?;
        let key = InternalKey::decode(entry.key)?;
        if key.kind != Kind::RangeDelete {
            return Error::corrupt(format!(
                "range-deletion block holds a {} key",
                key.kind.name()
            ));
        }
        if key.seq_num == SEQ_NUM_MAX {
            // base.InternalKeyRangeDeleteSentinel is a file boundary, not a
            // tombstone; Pebble refuses to write one into the block.
            return Error::corrupt("range-deletion block holds the sentinel trailer");
        }
        let t = Tombstone {
            start: key.user_key.to_vec(),
            end: entry.value.to_vec(),
            seq_num: key.seq_num,
        };
        if t.start >= t.end {
            return Error::corrupt(format!(
                "tombstone start {:?} is not below its end {:?}",
                String::from_utf8_lossy(&t.start),
                String::from_utf8_lossy(&t.end)
            ));
        }
        if let Some(prev) = out.last() {
            check_fragmented(prev, &t)?;
        }
        out.push(t);
    }
    Ok(out)
}

/// Cuts overlapping tombstones into the fragments a v2 block demands.
///
/// A range-deletion block holds fragments: tombstones sorted by start, never
/// overlapping unless they share both bounds, and then in strictly decreasing
/// sequence order. `check_fragmented` below refuses anything else, so anything
/// that collects tombstones from more than one source has to come through
/// here first — two `delete_range` calls that overlap are ordinary use, not
/// corruption.
///
/// The bounds of every tombstone, sorted and deduplicated, cut the key space
/// into spans that no tombstone straddles. Each span is then emitted once per
/// tombstone covering it, newest first — which is exactly the shape the block
/// demands.
///
/// Input that is already fragmented comes back unchanged: its bounds cut the
/// space at exactly the edges it already has, and a span no tombstone covers
/// emits nothing.
pub fn fragment(all: &[&Tombstone]) -> Vec<Tombstone> {
    let mut cuts: Vec<&[u8]> = Vec::with_capacity(all.len() * 2);
    for t in all {
        cuts.push(&t.start);
        cuts.push(&t.end);
    }
    cuts.sort_unstable();
    cuts.dedup();

    let mut out = Vec::new();
    for pair in cuts.windows(2) {
        let (lo, hi) = (pair[0], pair[1]);
        let mut covering: Vec<u64> = all
            .iter()
            .filter(|t| t.start.as_slice() <= lo && hi <= t.end.as_slice())
            .map(|t| t.seq_num)
            .collect();
        covering.sort_unstable_by(|a, b| b.cmp(a));
        for seq_num in covering {
            out.push(Tombstone {
                start: lo.to_vec(),
                end: hi.to_vec(),
                seq_num,
            });
        }
    }
    out
}

/// The fragmentation invariant, as `addTombstone` enforces it when writing.
fn check_fragmented(prev: &Tombstone, next: &Tombstone) -> Result<()> {
    match prev.start.cmp(&next.start) {
        std::cmp::Ordering::Greater => Error::corrupt(format!(
            "tombstones out of order: {:?} after {:?}",
            String::from_utf8_lossy(&next.start),
            String::from_utf8_lossy(&prev.start)
        )),
        std::cmp::Ordering::Equal => {
            if prev.end != next.end {
                return Error::corrupt(format!(
                    "overlapping tombstones are not fragmented: {:?}..{:?} vs {:?}..{:?}",
                    String::from_utf8_lossy(&prev.start),
                    String::from_utf8_lossy(&prev.end),
                    String::from_utf8_lossy(&next.start),
                    String::from_utf8_lossy(&next.end)
                ));
            }
            if prev.seq_num <= next.seq_num {
                return Error::corrupt(format!(
                    "tombstones over {:?} are not in strictly decreasing sequence order: \
                     {} then {}",
                    String::from_utf8_lossy(&prev.start),
                    prev.seq_num,
                    next.seq_num
                ));
            }
            Ok(())
        }
        std::cmp::Ordering::Less => {
            if prev.end > next.start {
                return Error::corrupt(format!(
                    "overlapping tombstones are not fragmented: {:?}..{:?} vs {:?}..{:?}",
                    String::from_utf8_lossy(&prev.start),
                    String::from_utf8_lossy(&prev.end),
                    String::from_utf8_lossy(&next.start),
                    String::from_utf8_lossy(&next.end)
                ));
            }
            Ok(())
        }
    }
}

/// Builds a v2 range-deletion block.
#[derive(Debug, Default)]
pub struct Builder {
    tombstones: Vec<Tombstone>,
}

impl Builder {
    /// An empty builder.
    pub fn new() -> Self {
        Builder::default()
    }

    /// Whether any tombstone has been added.
    pub fn is_empty(&self) -> bool {
        self.tombstones.is_empty()
    }

    /// How many tombstones the block will hold.
    pub fn len(&self) -> usize {
        self.tombstones.len()
    }

    /// The total size of the keys, as `rocksdb.raw.key.size` counts it.
    pub fn raw_key_size(&self) -> u64 {
        self.tombstones
            .iter()
            .map(|t| (t.start.len() + TRAILER_LEN) as u64)
            .sum()
    }

    /// The total size of the values, as `rocksdb.raw.value.size` counts it.
    pub fn raw_value_size(&self) -> u64 {
        self.tombstones.iter().map(|t| t.end.len() as u64).sum()
    }

    /// Adds a tombstone, checking the fragmentation invariant as it goes.
    pub fn add(&mut self, start: &[u8], end: &[u8], seq_num: u64) -> Result<()> {
        if start >= end {
            return Error::corrupt(format!(
                "tombstone start {:?} is not below its end {:?}",
                String::from_utf8_lossy(start),
                String::from_utf8_lossy(end)
            ));
        }
        if seq_num >= SEQ_NUM_MAX {
            return Error::corrupt(format!(
                "tombstone sequence number {seq_num} is the range-delete sentinel"
            ));
        }
        let t = Tombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            seq_num,
        };
        if let Some(prev) = self.tombstones.last() {
            check_fragmented(prev, &t)?;
        }
        self.tombstones.push(t);
        Ok(())
    }

    /// Encodes the block. Pebble writes this one uncompressed, always.
    pub fn finish(&self) -> Vec<u8> {
        let mut b = BlockBuilder::new(INDEX_RESTART_INTERVAL);
        let mut prev_len = 0usize;
        for t in &self.tombstones {
            let mut key = Vec::with_capacity(t.start.len() + TRAILER_LEN);
            key.extend_from_slice(&t.start);
            let trailer = (t.seq_num << 8) | u64::from(Kind::RangeDelete.to_byte());
            key.extend_from_slice(&trailer.to_le_bytes());
            // Restart interval 1, as Pebble uses here, so nothing is shared;
            // max_shared is passed for completeness only.
            b.add(&key, &t.end, prev_len, None, false);
            prev_len = key.len().saturating_sub(TRAILER_LEN);
        }
        b.finish().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn built(tombstones: &[(&[u8], &[u8], u64)]) -> Vec<u8> {
        let mut b = Builder::new();
        for (s, e, n) in tombstones {
            b.add(s, e, *n).expect("add");
        }
        b.finish()
    }

    #[test]
    fn tombstones_read_back_as_written() {
        let bytes = built(&[
            (b"/a", b"/b", 10),
            (b"/b", b"/c", 9),
            (b"/b", b"/c", 7),
            (b"/d", b"/zz", 3),
        ]);
        let block = Block::new(&bytes).expect("parse");
        let got = decode(&block).expect("decode");
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].start, b"/a");
        assert_eq!(got[0].end, b"/b");
        assert_eq!(got[0].seq_num, 10);
        assert_eq!(got[2].seq_num, 7);
        assert_eq!(got[3].end, b"/zz");
    }

    #[test]
    fn the_fragmentation_invariant_is_enforced_both_ways() {
        // Overlapping but not identical: must be refused.
        let mut b = Builder::new();
        b.add(b"/a", b"/m", 5).expect("first");
        assert!(b.add(b"/f", b"/z", 4).is_err(), "overlap must be refused");

        // Same range, non-decreasing sequence numbers: must be refused.
        let mut b = Builder::new();
        b.add(b"/a", b"/m", 5).expect("first");
        assert!(b.add(b"/a", b"/m", 5).is_err(), "equal seqnums refused");
        assert!(b.add(b"/a", b"/m", 9).is_err(), "rising seqnums refused");

        // Descending start keys: must be refused.
        let mut b = Builder::new();
        b.add(b"/m", b"/z", 5).expect("first");
        assert!(b.add(b"/a", b"/c", 4).is_err(), "descending starts refused");

        // An empty or inverted range: must be refused.
        let mut b = Builder::new();
        assert!(b.add(b"/a", b"/a", 1).is_err(), "empty range refused");
        assert!(b.add(b"/z", b"/a", 1).is_err(), "inverted range refused");
    }

    fn stones(raw: &[(&[u8], &[u8], u64)]) -> Vec<Tombstone> {
        raw.iter()
            .map(|(s, e, n)| Tombstone {
                start: s.to_vec(),
                end: e.to_vec(),
                seq_num: *n,
            })
            .collect()
    }

    /// The property that makes fragmenting safe to apply everywhere: input a
    /// block would already accept comes back as it went in. Were that not so,
    /// fragmenting on the flush path would rewrite tables that are fine today.
    #[test]
    fn already_fragmented_tombstones_come_back_unchanged() {
        for raw in [
            // Disjoint, with a gap no tombstone covers.
            &[(&b"/a"[..], &b"/b"[..], 10u64), (b"/d", b"/e", 3)][..],
            // Adjacent, sharing a bound.
            &[(&b"/a"[..], &b"/b"[..], 10u64), (b"/b", b"/c", 9)][..],
            // Stacked on the same range, strictly decreasing.
            &[(&b"/a"[..], &b"/b"[..], 10u64), (b"/a", b"/b", 4)][..],
            // Nothing at all.
            &[][..],
        ] {
            let input = stones(raw);
            let got = fragment(&input.iter().collect::<Vec<_>>());
            assert_eq!(got, input, "fragmenting changed an accepted block");
        }
    }

    /// Two `delete_range` calls that overlap are ordinary use. Unfragmented
    /// they are refused; fragmented they describe the same deleted keys.
    #[test]
    fn overlapping_tombstones_are_cut_into_fragments() {
        // `/a`..`/d` at 10, then `/b`..`/f` at 20 — overlapping at neither
        // bound, which is what a block refuses outright.
        let input = stones(&[(b"/a", b"/d", 10), (b"/b", b"/f", 20)]);
        let mut refused = Builder::new();
        refused.add(b"/a", b"/d", 10).expect("first");
        assert!(
            refused.add(b"/b", b"/f", 20).is_err(),
            "the unfragmented form must be the one that is refused"
        );

        let got = fragment(&input.iter().collect::<Vec<_>>());

        // The cuts are /a /b /d /f, so three spans; the middle one is covered
        // by both, newest first.
        assert_eq!(
            got,
            stones(&[
                (b"/a", b"/b", 10),
                (b"/b", b"/d", 20),
                (b"/b", b"/d", 10),
                (b"/d", b"/f", 20),
            ])
        );

        // And the result is something a block will actually take.
        let mut b = Builder::new();
        for t in &got {
            b.add(&t.start, &t.end, t.seq_num).expect("fragmented add");
        }
        let bytes = b.finish();
        let block = Block::new(&bytes).expect("parse");
        assert_eq!(decode(&block).expect("decode"), got);
    }

    #[test]
    fn a_block_holding_a_point_key_is_refused() {
        // A SET key in the range-deletion block means the table is not what it
        // claims, and reading it as a tombstone would invent a deleted range.
        let mut b = BlockBuilder::new(1);
        let mut key = b"/a".to_vec();
        key.extend_from_slice(&((5u64 << 8) | 1).to_le_bytes()); // kind 1 = SET
        b.add(&key, b"/b", 0, None, false);
        let bytes = b.finish().to_vec();
        let block = Block::new(&bytes).expect("parse");
        assert!(decode(&block).is_err());
    }
}
