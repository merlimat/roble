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

//! The memtable: a skiplist over an arena, keyed by internal key (§4).
//!
//! Three things about the shape, each of which has a reason beyond taste.
//!
//! **An arena, not a map of boxes.** Keys and values are appended to one
//! buffer and nodes hold offsets into it. A memtable's whole point is that it
//! fills to a size and is then thrown away in one go (§8: arenas of 4 MiB, to
//! match Oxia), and the size it has filled is a number this can answer exactly
//! rather than estimate. A map of separately allocated entries can do neither.
//!
//! **Links are indices, not pointers.** Nodes live in a `Vec` and link to each
//! other by index, so the whole structure is safe code — `roble` denies
//! `unsafe` outside the two places §10 names, and a skiplist is the classic
//! reason to want it. Indices cost a bounds check where a pointer would not,
//! and buy a structure that cannot dangle.
//!
//! **Heights come from a seeded generator.** A skiplist's shape is random, and
//! a store whose shape depends on the thread's luck does not replay (§9). The
//! generator is part of the memtable, seeded when it is made, so the same
//! inserts in the same order build the same list every time.
//!
//! Reverse iteration searches rather than steps, because the links only run
//! forwards — the same trade as a block's restart array (§4), and the reason
//! reverse is dearer here too.

use std::cmp::Ordering;

use roble_format::key::{self, Kind, TRAILER_LEN};
use roble_format::rangedel::Tombstone;

use crate::error::{Error, Result};

/// The tallest a node can be. Four to the twelfth is sixteen million entries,
/// far past what fits in an arena of a few megabytes.
const MAX_HEIGHT: usize = 12;

/// One node in four is promoted to the next level up.
const BRANCHING: u64 = 4;

/// The index that means "no node".
const NIL: u32 = u32::MAX;

/// A node's bookkeeping. The bytes themselves are in the arena.
#[derive(Clone, Copy, Debug)]
struct Node {
    key_at: u32,
    key_len: u32,
    value_at: u32,
    value_len: u32,
    /// Where this node's forward links start in the link array.
    links_at: u32,
    height: u8,
}

/// Compares two encoded internal keys.
///
/// Both are known to be well-formed: everything that enters the list is
/// checked on the way in, and every seek target is checked before the search
/// starts. The debug assertion says so rather than leaving it understood.
fn order(a: &[u8], b: &[u8]) -> Ordering {
    debug_assert!(a.len() >= TRAILER_LEN && b.len() >= TRAILER_LEN);
    let (a_user, a_trailer) = a.split_at(a.len() - TRAILER_LEN);
    let (b_user, b_trailer) = b.split_at(b.len() - TRAILER_LEN);
    let a_trailer = u64::from_le_bytes(a_trailer.try_into().expect("8 bytes"));
    let b_trailer = u64::from_le_bytes(b_trailer.try_into().expect("8 bytes"));
    // User key ascending, then the raw trailer descending: the newest version
    // of a key is the first one a forward scan meets.
    a_user.cmp(b_user).then_with(|| b_trailer.cmp(&a_trailer))
}

/// A memtable.
pub struct Memtable {
    /// Range deletions, kept apart from the point keys and sorted by start.
    ///
    /// §4 gives these a skiplist of their own "so a point read never walks
    /// tombstones it doesn't need". A sorted `Vec` serves that reason at a
    /// memtable's scale — a 4 MiB arena holds few tombstones, and they are
    /// searched, not walked — and a second skiplist can replace it the day a
    /// workload shows it needs one.
    ranges: Vec<Tombstone>,
    arena: Vec<u8>,
    nodes: Vec<Node>,
    links: Vec<u32>,
    /// The head's forward links, one per level.
    head: [u32; MAX_HEIGHT],
    /// The tallest node so far.
    height: usize,
    rng: u64,
}

/// How many bytes a memtable holds before a write writes it out (§4, §8).
///
/// Pebble's `MemTableSize`, and the same 4 MiB, which is also what Oxia runs
/// with. Small enough that a flush is a short operation and a store that
/// crashes loses little; large enough that a table is worth writing.
pub(crate) const MEMTABLE_SIZE: u64 = 4 << 20;

impl Memtable {
    /// An empty memtable whose shape is determined by `seed`.
    pub fn new(seed: u64) -> Self {
        Memtable {
            ranges: Vec::new(),
            arena: Vec::new(),
            nodes: Vec::new(),
            links: Vec::new(),
            head: [NIL; MAX_HEIGHT],
            height: 1,
            // A zero seed would leave the generator stuck, so it is nudged.
            rng: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
        }
    }

    /// How many entries it holds.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// How many bytes the arena holds.
    ///
    /// Keys and values only: this is what the memtable's size budget counts,
    /// and it is exact rather than estimated. Replacing a value leaves the old
    /// bytes behind and they stay counted, which is the truth about the arena.
    ///
    /// Range deletions are counted too, from where they are actually held.
    pub fn bytes(&self) -> usize {
        self.arena.len()
            + self
                .ranges
                .iter()
                .map(|t| t.start.len() + t.end.len())
                .sum::<usize>()
    }

    /// The key of node `i`.
    fn key(&self, i: u32) -> &[u8] {
        let n = &self.nodes[i as usize];
        &self.arena[n.key_at as usize..(n.key_at + n.key_len) as usize]
    }

    /// The value of node `i`.
    fn value(&self, i: u32) -> &[u8] {
        let n = &self.nodes[i as usize];
        &self.arena[n.value_at as usize..(n.value_at + n.value_len) as usize]
    }

    /// Node `i`'s forward link at `level`, or the head's when `i` is `NIL`.
    fn link(&self, i: u32, level: usize) -> u32 {
        if i == NIL {
            return self.head[level];
        }
        let n = &self.nodes[i as usize];
        debug_assert!(level < n.height as usize);
        self.links[n.links_at as usize + level]
    }

    fn set_link(&mut self, i: u32, level: usize, to: u32) {
        if i == NIL {
            self.head[level] = to;
            return;
        }
        let at = self.nodes[i as usize].links_at as usize + level;
        self.links[at] = to;
    }

    /// A height for a new node: one, with a one-in-four chance of each step up.
    fn random_height(&mut self) -> usize {
        // xorshift64*, so the shape is a function of the seed alone.
        let mut h = 1;
        while h < MAX_HEIGHT {
            self.rng ^= self.rng >> 12;
            self.rng ^= self.rng << 25;
            self.rng ^= self.rng >> 27;
            let r = self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
            if !r.is_multiple_of(BRANCHING) {
                break;
            }
            h += 1;
        }
        h
    }

    /// The last node before `key` at each level, `NIL` meaning the head.
    fn predecessors(&self, key: &[u8]) -> [u32; MAX_HEIGHT] {
        let mut prev = [NIL; MAX_HEIGHT];
        let mut at = NIL;
        for level in (0..self.height).rev() {
            loop {
                let next = self.link(at, level);
                if next == NIL || order(self.key(next), key) != Ordering::Less {
                    break;
                }
                at = next;
            }
            prev[level] = at;
        }
        prev
    }

    /// Adds an entry, replacing the value if the key is already there.
    ///
    /// Replacing rather than keeping both is a choice: Pebble gives every
    /// write its own sequence number, so two entries never share an internal
    /// key, and a structure that allowed duplicates would have a state its
    /// users cannot reach and its tests cannot describe.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() < TRAILER_LEN {
            return Err(Error::Format(roble_format::Error::Truncated {
                what: "internal key",
                need: TRAILER_LEN,
                have: key.len(),
            }));
        }

        let prev = self.predecessors(key);
        // Already there? Replace its value, appending the new bytes.
        let next = self.link(prev[0], 0);
        if next != NIL && order(self.key(next), key) == Ordering::Equal {
            let value_at = self.arena.len() as u32;
            self.arena.extend_from_slice(value);
            let n = &mut self.nodes[next as usize];
            n.value_at = value_at;
            n.value_len = value.len() as u32;
            return Ok(());
        }

        let height = self.random_height();
        let key_at = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        let value_at = self.arena.len() as u32;
        self.arena.extend_from_slice(value);
        let links_at = self.links.len() as u32;
        self.links.extend(std::iter::repeat_n(NIL, height));
        let me = self.nodes.len() as u32;
        self.nodes.push(Node {
            key_at,
            key_len: key.len() as u32,
            value_at,
            value_len: value.len() as u32,
            links_at,
            height: height as u8,
        });

        // Above the old height, `predecessors` left its array untouched, and
        // it starts as `NIL` — the head. So a node taller than any so far
        // links straight from the head at those levels, and no special case
        // is needed to say so. (It had one for a while. Deleting it changed
        // no test, because it was doing nothing.)
        //
        // The two assignments are independent: `after` is read before either,
        // so their order is immaterial here. It would matter to a reader
        // running concurrently, which this store does not have — one thread
        // owns it (§4) — so nothing pins the order and nothing should.
        for (level, p) in prev.iter().enumerate().take(height) {
            let before = *p;
            let after = self.link(before, level);
            self.set_link(me, level, after);
            self.set_link(before, level, me);
        }
        if height > self.height {
            self.height = height;
        }
        Ok(())
    }

    /// Its range deletions, sorted by start key.
    pub fn tombstones(&self) -> &[Tombstone] {
        &self.ranges
    }

    /// Adds a range deletion, checking that it is one.
    ///
    /// An **empty** range is not an error. Pebble's `DeleteRange(a, a)` is
    /// legal and covers nothing, and roble applies batches that came out of
    /// Pebble's own WAL — so refusing one would mean refusing a database
    /// Pebble wrote. It is dropped rather than stored, since a tombstone
    /// covering nothing has nothing to say. A **reversed** range is
    /// malformed, and that is refused.
    pub(crate) fn add_range(&mut self, start: &[u8], end: &[u8], seq_num: u64) -> Result<()> {
        match end.cmp(start) {
            std::cmp::Ordering::Less => Err(Error::Corrupt(format!(
                "a range deletion from {start:02x?} to {end:02x?}, which ends before it starts"
            ))),
            std::cmp::Ordering::Equal => Ok(()),
            std::cmp::Ordering::Greater => {
                self.insert_range(start, end, seq_num);
                Ok(())
            }
        }
    }

    /// Adds a range deletion.
    fn insert_range(&mut self, start: &[u8], end: &[u8], seq_num: u64) {
        // The bounds are not copied into the arena: the tombstone owns them,
        // and appending them as well would store every tombstone twice for
        // the sake of a counter. `bytes` adds them up instead.
        let at = self.ranges.partition_point(|t| t.start.as_slice() < start);
        self.ranges.insert(
            at,
            Tombstone {
                start: start.to_vec(),
                end: end.to_vec(),
                seq_num,
            },
        );
    }

    /// Applies a batch representation, giving entry `i` sequence number
    /// `base + i`.
    ///
    /// That numbering is Pebble's: a batch is committed at one sequence
    /// number and its entries take the ones after it in order, which is what
    /// makes a later write in the same batch win over an earlier one for the
    /// same key.
    ///
    /// Point keys go to the skiplist and range deletions to the list beside
    /// it — never interleaved, since a reader that walks point keys must not
    /// meet a tombstone among them (§6).
    pub fn apply(&mut self, repr: &[u8], base: u64) -> Result<u64> {
        let mut reader = roble_format::batch::Batch::read(repr)?;
        let mut seq = base;
        while let Some(entry) = reader.next() {
            let entry = entry?;
            match entry.kind {
                Kind::RangeDelete => {
                    let end = entry.value.ok_or_else(|| {
                        Error::Corrupt("a range deletion with no end key".to_string())
                    })?;
                    self.add_range(entry.key, end, seq)?;
                }
                kind => {
                    if !kind.is_accepted() || kind == Kind::Merge {
                        return Err(Error::Unsupported(format!(
                            "a batch entry of kind {}, which roble does not serve",
                            kind.name()
                        )));
                    }
                    let key = key::encode(entry.key, seq, kind);
                    self.insert(&key, entry.value.unwrap_or(&[]))?;
                }
            }
            seq += 1;
        }
        Ok(seq)
    }

    /// A cursor over the memtable, positioned nowhere.
    ///
    /// The cursor does not borrow the memtable: every operation is given it
    /// again. That is what lets a scan hold one beside an `Rc` to the
    /// memtable itself — the same reason the block cursor stopped borrowing
    /// its block (§4), and the same shape.
    pub fn cursor(&self) -> MemtableCursor {
        MemtableCursor {
            at: NIL,
            valid: false,
        }
    }

    /// The first node at or after `key`, or `NIL`.
    fn first_at_or_after(&self, key: &[u8]) -> u32 {
        let prev = self.predecessors(key);
        self.link(prev[0], 0)
    }
}

/// A position in a memtable.
///
/// Holds a position and nothing else; the memtable is passed to each call.
/// The caller is responsible for passing the same one — this is a cursor, not
/// a handle.
#[derive(Clone, Copy, Debug)]
pub struct MemtableCursor {
    at: u32,
    valid: bool,
}

impl MemtableCursor {
    /// Whether it is on an entry.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// The encoded internal key it is on.
    ///
    /// # Panics
    /// If it is not on an entry.
    pub fn key<'a>(&self, table: &'a Memtable) -> &'a [u8] {
        assert!(self.valid, "key() on a cursor that is not on an entry");
        table.key(self.at)
    }

    /// Its value.
    ///
    /// # Panics
    /// If it is not on an entry.
    pub fn value<'a>(&self, table: &'a Memtable) -> &'a [u8] {
        assert!(self.valid, "value() on a cursor that is not on an entry");
        table.value(self.at)
    }

    fn place(&mut self, at: u32) {
        self.at = at;
        self.valid = at != NIL;
    }

    /// Positions on the first entry.
    pub fn first(&mut self, table: &Memtable) {
        self.place(table.link(NIL, 0));
    }

    /// Positions on the last entry.
    pub fn last(&mut self, table: &Memtable) {
        // Walk down from the top level, which is what the levels are for.
        //
        // Walking level zero alone would reach the same node, just by hopping
        // every entry instead of a few — a speed choice, not a correctness
        // one, and no test pins it. What a test would have to assert is how
        // many hops it took, which is a worse thing to write down than this
        // sentence.
        let mut at = NIL;
        for level in (0..table.height).rev() {
            loop {
                let next = table.link(at, level);
                if next == NIL {
                    break;
                }
                at = next;
            }
        }
        self.place(at);
    }

    /// Steps forward.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self, table: &Memtable) {
        if !self.valid {
            return;
        }
        self.place(table.link(self.at, 0));
    }

    /// Steps backward.
    ///
    /// A search, not a step: the links only run forwards. This is why reverse
    /// iteration over a memtable costs a descent per entry where forward costs
    /// a pointer hop.
    pub fn prev(&mut self, table: &Memtable) {
        if !self.valid {
            return;
        }
        let key = table.key(self.at).to_vec();
        let prev = table.predecessors(&key);
        self.place(prev[0]);
    }

    /// Positions on the first entry at or after `target`.
    pub fn seek_ge(&mut self, table: &Memtable, target: &[u8]) -> Result<()> {
        check(target)?;
        self.place(table.first_at_or_after(target));
        Ok(())
    }

    /// Positions on the last entry before `target`.
    pub fn seek_lt(&mut self, table: &Memtable, target: &[u8]) -> Result<()> {
        check(target)?;
        let prev = table.predecessors(target);
        self.place(prev[0]);
        Ok(())
    }
}

/// Refuses a seek target that is not an internal key, before any comparison
/// is made against it.
fn check(target: &[u8]) -> Result<()> {
    if target.len() < TRAILER_LEN {
        return Err(Error::Format(roble_format::Error::Truncated {
            what: "internal key",
            need: TRAILER_LEN,
            have: target.len(),
        }));
    }
    // Decoding it is the same check the list's own keys got on the way in.
    key::split_encoded(target)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use roble_format::key::Kind;

    fn filled(seed: u64, n: u64) -> Memtable {
        let mut t = Memtable::new(seed);
        for i in 0..n {
            let k = key::encode(format!("key{i:05}").as_bytes(), i, Kind::Set);
            t.insert(&k, b"v").expect("insert");
        }
        t
    }

    #[test]
    fn the_list_is_actually_a_skiplist() {
        // Everything else here would pass just as well against a sorted linked
        // list — slower, and with the levels never exercised. So: the levels
        // have to exist, and the tall nodes have to be rare.
        let t = filled(1, 2000);
        assert!(
            t.height >= 4,
            "a two-thousand-entry list reached height {}, so the search is nearly linear",
            t.height
        );

        let mut at_height = [0usize; MAX_HEIGHT + 1];
        for n in &t.nodes {
            at_height[n.height as usize] += 1;
        }
        assert!(
            at_height[1] > t.nodes.len() / 2,
            "most nodes should be height one, got {} of {}",
            at_height[1],
            t.nodes.len()
        );
        for (h, count) in at_height.iter().enumerate().take(5).skip(2) {
            assert!(*count > 0, "no nodes of height {h}");
        }
        // One in four, so each level should be roughly a quarter of the one
        // below. Loose bounds: this is checking the branching is not broken,
        // not that the generator is a good one.
        assert!(
            at_height[2] * 2 < at_height[1],
            "height two is {} against height one's {}",
            at_height[2],
            at_height[1]
        );
    }

    #[test]
    fn the_same_seed_builds_the_same_shape() {
        // §9: a seeded run replays. For a sorted structure the iteration order
        // is the same whatever the shape, so only the shape can show this.
        let a = filled(42, 500);
        let b = filled(42, 500);
        let heights_a: Vec<u8> = a.nodes.iter().map(|n| n.height).collect();
        let heights_b: Vec<u8> = b.nodes.iter().map(|n| n.height).collect();
        assert_eq!(heights_a, heights_b, "same seed, same shape");
        assert_eq!(a.height, b.height);

        let c = filled(43, 500);
        let heights_c: Vec<u8> = c.nodes.iter().map(|n| n.height).collect();
        assert_ne!(
            heights_a, heights_c,
            "a different seed should build a different shape"
        );
    }

    #[test]
    fn a_zero_seed_still_generates_heights() {
        // The generator is a xorshift, which is stuck at zero for ever.
        let t = filled(0, 500);
        assert!(t.height > 1, "a zero seed produced a flat list");
    }

    #[test]
    fn the_arena_counts_what_it_was_given() {
        let mut t = Memtable::new(1);
        let k = key::encode(b"key", 1, Kind::Set);
        t.insert(&k, b"value").expect("insert");
        assert_eq!(t.bytes(), k.len() + b"value".len());
    }
}
