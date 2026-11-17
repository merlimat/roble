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
//! **A node is one record in the arena, and links are offsets to records.**
//! A node's header, its forward links and its key sit together, key last, and
//! a link is the byte offset of the record it points to. So a hop through the
//! list lands on one record, whose links and key are usually in the cache
//! line the hop already fetched — the layout of Pebble's `arenaskl` and
//! RocksDB's `InlineSkipList`, for the same reason. Kept apart, as they were
//! for a while — the nodes' bookkeeping in one `Vec`, the links in a second
//! and the bytes in the arena — a hop touched four places: the node, its
//! links, the next node, and that node's key. Once a memtable is larger than
//! the CPU's caches each of the four is a trip to memory, and at the 240,000
//! entries YCSB B leaves in a 64 MiB memtable, searching it was a third of
//! every operation (`bench/`).
//!
//! Offsets rather than pointers keep the whole structure safe code — `roble`
//! denies `unsafe` outside the two places §10 names, and a skiplist is the
//! classic reason to want it. An offset costs a bounds check where a pointer
//! would not, and buys a structure that cannot dangle.
//!
//! **Heights come from a seeded generator.** A skiplist's shape is random, and
//! a store whose shape depends on the thread's luck does not replay (§9). The
//! generator is part of the memtable, seeded when it is made, so the same
//! inserts in the same order build the same list every time.
//!
//! **Level zero is linked backwards as well.** Each record also holds the
//! offset of the one before it at level zero, so a step back is one hop, as
//! a step forward is. Without it a step back was a search from the top for
//! the predecessor of a copy of the current key — a descent and an
//! allocation for every entry a reverse scan steps over, which is the
//! direction Oxia's `KeyRangeScanReverse` steps in. Pebble's `arenaskl` keeps
//! a back link at every level, for its lock-free insert, and steps back by
//! level zero's; with one writer, level zero's is the only one roble needs.

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

/// Where each field of a node's record is, from the record's start.
///
/// A record is `key_len`, `value_at` and `value_len` as little-endian `u32`s,
/// then its back link — the record before it at level zero, as a `u32` — then
/// the height as one byte, then that many forward links as `u32`s, then the
/// key, then — until the value is replaced — the value. Unaligned on
/// purpose: a record is as long as its fields and no longer, and an unaligned
/// load costs nothing on any machine this runs on.
///
/// The value is reached through `value_at` rather than found after the key
/// because a replaced value is appended at the end of the arena (see
/// [`Memtable::insert`]), and the record has to be able to point there.
const KEY_LEN: usize = 0;
const VALUE_AT: usize = 4;
const VALUE_LEN: usize = 8;
const BACK: usize = 12;
const HEIGHT: usize = 16;
const LINKS: usize = 17;

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
    /// Every node's record, in the order they were inserted, and every
    /// replacement value after them.
    arena: Vec<u8>,
    /// How many nodes there are.
    count: usize,
    /// The bytes of the keys and values it was given, which is what
    /// [`Memtable::bytes`] reports and not the arena's length: the arena also
    /// holds each record's header and links.
    given: usize,
    /// The head's forward links, one per level.
    head: [u32; MAX_HEIGHT],
    /// The tallest node so far.
    height: usize,
    rng: u64,
    /// What the store will let this memtable hold before it is written out,
    /// in keys and values, or zero for one that has no budget — a batch's
    /// index. The arena grows to what that takes in one step rather than by
    /// doubling ([`Memtable::make_room`]).
    budget: usize,
    /// Where `apply` builds each entry's internal key before inserting it,
    /// kept so that a write does not allocate one: the key is copied into the
    /// arena at once and the buffer is free again for the next.
    key: Vec<u8>,
}

/// How large an arena grows the ordinary way before it goes straight to its
/// budget ([`Memtable::make_room`]): small enough that the copy it saves
/// nothing on is cheap, large enough that an idle store's memtable — and
/// Oxia runs hundreds of stores, most of them idle — is not handed its whole
/// budget for a handful of writes.
const GROW_TO_BUDGET_PAST: usize = 1 << 20;

/// How many bytes a memtable holds before a write writes it out (§4, §8).
///
/// Pebble's `MemTableSize`, and the same 4 MiB, which is also what Oxia runs
/// with. Small enough that a flush is a short operation and a store that
/// crashes loses little; large enough that a table is worth writing.
pub(crate) const MEMTABLE_SIZE: u64 = 4 << 20;

/// What share of the budget a store keeps back for the writes that arrive
/// while a memtable is being written out (§5).
///
/// The budget is what a store holds across **every** memtable it has, so the
/// one in front and the one a flush is writing out share it. They do not
/// share it evenly: the one being written out is the one that filled, and
/// the one in front needs only the room the writes arriving during that
/// flush ask for. Dividing the budget in two would halve the tables a flush
/// writes, and halving them doubles how often L0 reaches the depth the
/// picker compacts it at — which is paying a third of a store's write
/// throughput for room nothing uses (measured, `bench/`).
///
/// So the reserve is an eighth, which at the 4 MiB default is 512 KiB and at
/// the 64 MiB one is 8 MiB. roble cannot do better than a fraction here: how
/// long a flush takes is a question about the clock, and §10 does not let a
/// store ask it. What the fraction has to cover is the writes of one flush,
/// and the two are not independent — a bigger budget means a longer flush
/// *and* a larger eighth.
///
/// A store with nobody to hand the flush to never reaches this line for any
/// purpose: the reserve is room for a flush running beside the writes, and
/// without one there is nothing to make room for.
pub(crate) const FLUSH_RESERVE: u64 = 8;

/// How much it holds, not what.
impl std::fmt::Debug for Memtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memtable")
            .field("len", &self.len())
            .field("bytes", &self.bytes())
            .field("range_deletions", &self.ranges.len())
            .finish_non_exhaustive()
    }
}

impl Memtable {
    /// An empty memtable whose shape is determined by `seed`.
    pub fn new(seed: u64) -> Self {
        Memtable {
            ranges: Vec::new(),
            arena: Vec::new(),
            count: 0,
            given: 0,
            head: [NIL; MAX_HEIGHT],
            height: 1,
            budget: 0,
            key: Vec::new(),
            // A zero seed would leave the generator stuck, so it is nudged.
            rng: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
        }
    }

    /// An empty memtable that a store will fill to `budget` bytes of keys and
    /// values.
    ///
    /// The budget is what a store's memtables are written out at, so it says
    /// how large this one's arena will get ([`Memtable::whole`]), and the
    /// arena goes there in one step once it is past [`GROW_TO_BUDGET_PAST`].
    /// Grown by doubling the way a `Vec` grows, the arena reallocated at each
    /// power of two and held the old copy and the new at once while it moved:
    /// a 64 MiB store's memtable passed 43 MiB on its way to its 56 MiB flush
    /// line, took an 86 MiB allocation, and held 129 MiB for the copy — the
    /// largest single thing a store held at its peak in YCSB A, and a 64 MiB
    /// memtable kept in an 86 MiB one afterwards (`bench/`).
    pub fn with_budget(seed: u64, budget: usize) -> Self {
        Memtable {
            budget,
            ..Self::new(seed)
        }
    }

    /// Room in the arena for `more` bytes, before they are appended.
    ///
    /// Below [`GROW_TO_BUDGET_PAST`], and for a memtable with no budget,
    /// nothing: the append grows the arena as a `Vec` grows. Past it, the
    /// arena is grown at once to all it will hold ([`Memtable::whole`]), so
    /// it moves once and small. Past that — a batch larger than the
    /// allowance, or records that change shape partway — the ordinary growth
    /// resumes: growing by exactly what each write needs would move it on
    /// every write.
    fn make_room(&mut self, more: usize) {
        let needed = self.arena.len() + more;
        if needed <= self.arena.capacity() || needed <= GROW_TO_BUDGET_PAST {
            return;
        }
        let whole = self.whole();
        if needed <= whole {
            self.arena.reserve_exact(whole - self.arena.len());
        }
    }

    /// How large the arena gets before the store writes this memtable out.
    ///
    /// Larger than the budget, because the budget counts keys and values and
    /// the arena holds each record's header and links as well — seventeen
    /// bytes and four per level, about twenty-two on average. Reserved at the
    /// budget alone, the arena of a 64 MiB store holding 36-byte keys and
    /// 128-byte values filled its 64 MiB at 58 MiB of keys and values, short
    /// of where the store wrote it out, and doubled to 128 MiB for the last
    /// few megabytes: a 64 MiB copy every memtable, and the largest thing the
    /// store held (`bench/`).
    ///
    /// So the bookkeeping is counted at the rate the records so far have
    /// paid it, which a megabyte of them measures well, on top of the budget
    /// plus an eighth ([`FLUSH_RESERVE`]): a store writes a memtable out by
    /// the time it holds its budget, and the batch that crosses that line
    /// goes in whole (§5). Pages are resident only once written, so the part
    /// of this a memtable never reaches costs address space and nothing else.
    fn whole(&self) -> usize {
        let allowed = (self.budget + self.budget / FLUSH_RESERVE as usize) as u128;
        let whole = allowed * self.arena.len() as u128 / self.given.max(1) as u128;
        whole.try_into().unwrap_or(usize::MAX)
    }

    /// How many entries it holds.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// How many bytes the arena holds.
    ///
    /// Keys and values only: this is what the memtable's size budget counts,
    /// and it is exact rather than estimated. Replacing a value leaves the old
    /// bytes behind and they stay counted, which is the truth about the arena.
    ///
    /// Range deletions are counted too, from where they are actually held.
    pub fn bytes(&self) -> usize {
        self.given
            + self
                .ranges
                .iter()
                .map(|t| t.start.len() + t.end.len())
                .sum::<usize>()
    }

    /// The `u32` at `at` in the arena.
    fn u32_at(&self, at: usize) -> u32 {
        u32::from_le_bytes(self.arena[at..at + 4].try_into().expect("4 bytes"))
    }

    fn set_u32_at(&mut self, at: usize, to: u32) {
        self.arena[at..at + 4].copy_from_slice(&to.to_le_bytes());
    }

    /// Node `i`'s height.
    fn height_of(&self, i: u32) -> usize {
        self.arena[i as usize + HEIGHT] as usize
    }

    /// The key of node `i`, which follows its links.
    fn key(&self, i: u32) -> &[u8] {
        let at = i as usize;
        let from = at + LINKS + 4 * self.height_of(i);
        &self.arena[from..from + self.u32_at(at + KEY_LEN) as usize]
    }

    /// The value of node `i`.
    fn value(&self, i: u32) -> &[u8] {
        let at = i as usize;
        let from = self.u32_at(at + VALUE_AT) as usize;
        &self.arena[from..from + self.u32_at(at + VALUE_LEN) as usize]
    }

    /// Node `i`'s forward link at `level`, or the head's when `i` is `NIL`.
    fn link(&self, i: u32, level: usize) -> u32 {
        if i == NIL {
            return self.head[level];
        }
        debug_assert!(level < self.height_of(i));
        self.u32_at(i as usize + LINKS + 4 * level)
    }

    fn set_link(&mut self, i: u32, level: usize, to: u32) {
        if i == NIL {
            self.head[level] = to;
            return;
        }
        debug_assert!(level < self.height_of(i));
        self.set_u32_at(i as usize + LINKS + 4 * level, to);
    }

    /// The node before `i` at level zero, `NIL` meaning the head.
    fn back(&self, i: u32) -> u32 {
        self.u32_at(i as usize + BACK)
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
            return Err(Error::from(roble_format::Error::Truncated {
                what: "internal key",
                need: TRAILER_LEN,
                have: key.len(),
            }));
        }

        let prev = self.predecessors(key);
        // Already there? Replace its value, appending the new bytes.
        let next = self.link(prev[0], 0);
        if next != NIL && order(self.key(next), key) == Ordering::Equal {
            self.make_room(value.len());
            let value_at = self.arena.len() as u32;
            self.arena.extend_from_slice(value);
            self.given += value.len();
            self.set_u32_at(next as usize + VALUE_AT, value_at);
            self.set_u32_at(next as usize + VALUE_LEN, value.len() as u32);
            return Ok(());
        }

        let height = self.random_height();
        self.make_room(LINKS + 4 * height + key.len() + value.len());
        let me = self.arena.len() as u32;
        let value_at = me as usize + LINKS + 4 * height + key.len();
        self.arena
            .extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.arena
            .extend_from_slice(&(value_at as u32).to_le_bytes());
        self.arena
            .extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.arena.extend_from_slice(&prev[0].to_le_bytes());
        self.arena.push(height as u8);
        for _ in 0..height {
            self.arena.extend_from_slice(&NIL.to_le_bytes());
        }
        self.arena.extend_from_slice(key);
        self.arena.extend_from_slice(value);
        debug_assert_eq!(self.arena.len(), value_at + value.len());
        self.count += 1;
        self.given += key.len() + value.len();

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
        // Its own back link went in with the record. The node after it at
        // level zero, if there is one, now comes after it instead.
        let after = self.link(me, 0);
        if after != NIL {
            self.set_u32_at(after as usize + BACK, me);
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
        let reader = roble_format::batch::Batch::read(repr)?;
        let mut seq = base;
        for entry in reader {
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
                    let mut key = std::mem::take(&mut self.key);
                    key::encode_into(entry.key, seq, kind, &mut key);
                    let inserted = self.insert(&key, entry.value.unwrap_or(&[]));
                    self.key = key;
                    inserted?;
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

    /// Steps backward, by the node's back link: one hop, as a step forward is.
    pub fn prev(&mut self, table: &Memtable) {
        if !self.valid {
            return;
        }
        self.place(table.back(self.at));
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
        return Err(Error::from(roble_format::Error::Truncated {
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

    impl Memtable {
        /// Every node's height, in key order.
        fn heights(&self) -> Vec<u8> {
            let mut out = Vec::with_capacity(self.count);
            let mut at = self.link(NIL, 0);
            while at != NIL {
                out.push(self.height_of(at) as u8);
                at = self.link(at, 0);
            }
            out
        }
    }

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
        for h in t.heights() {
            at_height[h as usize] += 1;
        }
        assert!(
            at_height[1] > t.len() / 2,
            "most nodes should be height one, got {} of {}",
            at_height[1],
            t.len()
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
        let heights_a = a.heights();
        let heights_b = b.heights();
        assert_eq!(heights_a, heights_b, "same seed, same shape");
        assert_eq!(a.height, b.height);

        let c = filled(43, 500);
        let heights_c = c.heights();
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
    fn a_budgeted_arena_moves_once_on_its_way_past_the_budget() {
        // The bench's records, a 36-byte key and a 128-byte value, whose
        // header and links add about an eighth; and small ones, an 8-byte value
        // under a 16-byte key, where they add over half. The arena reserved
        // at the budget alone passed it at 3.6 MiB of keys and values in the
        // first and 2.5 MiB in the second, and doubled.
        let budget = 4 << 20;
        let past = budget + budget / (2 * FLUSH_RESERVE as usize);
        for (key_len, value_len) in [(36, 128), (16, 8)] {
            let mut t = Memtable::with_budget(1, budget);
            let value = vec![7u8; value_len];
            // Every move of an arena already past a megabyte, which is a copy
            // of all of it with the old and the new both held meanwhile.
            let mut moves = Vec::new();
            let mut i = 0u64;
            // To the budget and then partway into the batch that may cross
            // it, which is what a store puts in a memtable (§5).
            while t.bytes() < past {
                let user = format!("{i:0key_len$}");
                let before = t.arena.capacity();
                t.insert(&key::encode(user.as_bytes(), i, Kind::Set), &value)
                    .expect("insert");
                if t.arena.capacity() != before && before >= GROW_TO_BUDGET_PAST {
                    moves.push((before, t.arena.capacity()));
                }
                i += 1;
            }
            assert_eq!(
                moves.len(),
                1,
                "{key_len}+{value_len}: the arena moved {moves:?} past a megabyte, \
                 to hold {} for {} of keys and values",
                t.arena.len(),
                t.bytes()
            );
            // And it was not simply handed far more than it holds: what it
            // reserved is the budget and an eighth at the rate these records
            // pay for their bookkeeping.
            let rate = t.arena.len() as f64 / t.bytes() as f64;
            let allowed = (budget + budget / FLUSH_RESERVE as usize) as f64;
            let reserved = t.arena.capacity();
            assert!(
                reserved as f64 <= allowed * rate * 1.01,
                "{key_len}+{value_len}: reserved {reserved} for {} of arena",
                t.arena.len()
            );
        }
    }

    #[test]
    fn the_arena_counts_what_it_was_given() {
        let mut t = Memtable::new(1);
        let k = key::encode(b"key", 1, Kind::Set);
        t.insert(&k, b"value").expect("insert");
        assert_eq!(t.bytes(), k.len() + b"value".len());
    }
}
