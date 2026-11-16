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

//! The caches of §8, and the CLOCK they evict with.
//!
//! **Caches belong to the thread, not the store.** A node runs hundreds of
//! shards; a per-store budget would either waste memory on cold shards or
//! starve hot ones. So [`Caches`] is a cheap handle that several stores on one
//! thread share, and the budget is the thread's. The sharing needs no atomics
//! because it never leaves the thread: it is `Rc` and `RefCell` throughout.
//!
//! So far it holds the **table cache** — open readers with their index and
//! filter, bounded by count, since an open reader costs file descriptors as
//! well as memory. The block cache goes beside it, keyed by file number and
//! offset with a budget in bytes; [`crate::table`] already fetches blocks one
//! at a time by handle, which is the seam it slots into.
//!
//! Eviction is CLOCK rather than LRU, as encina's leaf cache is: a second
//! chance bit per entry and a hand that sweeps, which costs one bit and no
//! reordering on a hit. An LRU would have to move an entry on every read,
//! which is a write to shared state for what is otherwise a pure read.

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::rc::Rc;

use crate::env::Fs;
use crate::table::OpenTable;

/// How many tables one thread keeps open by default: **no limit**, which is
/// RocksDB's own default for `max_open_files` and what it spells `-1`.
///
/// It was 128, and 128 is a cliff rather than a bound. A store with more
/// tables than that reopens one on nearly every lookup — and reopening a
/// table is its footer, its metaindex, its properties, its index, its filter
/// and its tombstones, not a file handle. Measured on an 846 MiB store of 243
/// tables, raising this took `floor` from 132 to 7,480 operations a second
/// and the phase's CPU from 2.29 seconds to 0.04. The degradation is not
/// gradual: it is nothing until the store outgrows the cache, then fifty-five
/// times.
///
/// **What this costs is file descriptors**, which is what the old bound was
/// for and what §8 still says about it. One per open table, and a node
/// running hundreds of shards of a few hundred tables each will pass a
/// default `ulimit -n` of 1024 without trying. A deployment that cares has to
/// set a budget — [`Caches::new`] takes one — and the right number is the one
/// CockroachDB derives from the system limit rather than any constant here.
///
/// The budget reaches the `limit` gauge as `budget as i64`, so an unlimited
/// cache reports **-1** there. That is the same -1 RocksDB uses and it is
/// meant to be read the same way.
pub const DEFAULT_OPEN_TABLES: usize = usize::MAX;

/// How many bytes of data blocks one thread keeps by default.
///
/// A size rather than a count, because blocks vary: a 16 KiB data block and a
/// much larger value block cannot be budgeted the same way.
pub const DEFAULT_BLOCK_BYTES: usize = 32 << 20;

/// The hash the caches' indexes use: a rotate, an exclusive or and a
/// multiply per word, which is all a key made of a store id, a file number
/// and an offset needs.
///
/// `HashMap`'s default is SipHash, built to withstand keys chosen to collide,
/// and these are numbers roble hands out itself. It was nearly 2% of a point
/// lookup over a store in cache, which hashes twice per table it reads: the
/// table, then the block. This is rustc's `FxHasher`, finished with its
/// rotate so that the well-mixed high bits reach the low ones the map takes
/// its bucket from.
#[derive(Default, Clone, Copy)]
struct WordHasher(u64);

impl Hasher for WordHasher {
    fn finish(&self) -> u64 {
        self.0.rotate_left(26)
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_u64(u64::from(b));
        }
    }

    fn write_u64(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0xf135_7aea_2e62_a9c5);
    }

    fn write_usize(&mut self, word: usize) {
        self.write_u64(word as u64);
    }
}

/// Builds [`WordHasher`]s for a `HashMap`.
type WordHash = BuildHasherDefault<WordHasher>;

struct Slot<K, V> {
    key: K,
    value: V,
    cost: usize,
    /// The second-chance bit: set on every hit, cleared by the sweep.
    ///
    /// **Clear when an entry comes in**, so that something read once goes
    /// the first time the sweep reaches it and only a second read buys it a
    /// reprieve. Set, as it used to be, it kept a block read once for two
    /// sweeps, as long as one read again and again — and past memory that is
    /// most of what a skewed run of lookups brings in. It cost twice: hot
    /// blocks went out to make room for it, and by the time it went itself
    /// its pages had left the page cache, which is what a miss here falls
    /// through to. RocksDB's block cache gets the same effect by putting a
    /// data block into its low-priority half until it is hit again. Clear,
    /// a lookup past memory went to the device 4% less often, and YCSB C
    /// past memory ran 4 to 10% faster (`bench/`).
    referenced: bool,
}

/// A CLOCK cache with a budget expressed in whatever unit `cost` is.
pub struct Clock<K: Eq + Hash + Clone, V: Clone> {
    slots: Vec<Option<Slot<K, V>>>,
    index: HashMap<K, usize, WordHash>,
    free: Vec<usize>,
    hand: usize,
    used: usize,
    budget: usize,
    /// What the caches export, if anything, and which of the two this is
    /// (`crate::metrics`).
    metrics: Option<std::rc::Rc<crate::metrics::CacheMetrics>>,
    which: usize,
}

impl<K: Eq + Hash + Clone, V: Clone> Clock<K, V> {
    /// A cache that holds `budget` worth of entries.
    pub fn new(budget: usize) -> Self {
        Clock {
            slots: Vec::new(),
            index: HashMap::default(),
            free: Vec::new(),
            hand: 0,
            used: 0,
            budget,
            metrics: None,
            which: 0,
        }
    }

    /// Looks `key` up, giving it a second chance if it is there.
    pub fn get(&mut self, key: &K) -> Option<V> {
        let at = *self.index.get(key)?;
        let slot = self.slots[at].as_mut()?;
        slot.referenced = true;
        Some(slot.value.clone())
    }

    /// Forgets `key`, if it is there.
    ///
    /// Not an eviction, and deliberately not counted as one: an eviction is
    /// what a cache does to make room, and their number is how an operator
    /// sees a budget that is too small. This is the opposite — the entry is
    /// gone because the thing it described is gone — and counting it would
    /// report pressure that never happened.
    pub fn remove(&mut self, key: &K) {
        let Some(at) = self.index.remove(key) else {
            return;
        };
        if let Some(slot) = self.slots[at].take() {
            self.used -= slot.cost;
        }
        self.free.push(at);
        self.usage_changed();
    }

    /// Adds `key`, evicting until it fits.
    ///
    /// An entry larger than the whole budget is **not** cached: evicting
    /// everything else to make room for one oversized thing is worse than not
    /// caching it, and the caller has the value either way.
    pub fn insert(&mut self, key: K, value: V, cost: usize) {
        if cost > self.budget {
            return;
        }
        self.usage_changed();
        if let Some(&at) = self.index.get(&key) {
            if let Some(old) = self.slots[at].take() {
                self.used -= old.cost;
            }
            self.index.remove(&key);
            self.free.push(at);
        }
        self.make_room(cost);

        let slot = Slot {
            key: key.clone(),
            value,
            cost,
            // Without a second chance: it has been read once, and that is
            // not yet a reason to keep it ([`Slot::referenced`]).
            referenced: false,
        };
        let at = match self.free.pop() {
            Some(at) => {
                self.slots[at] = Some(slot);
                at
            }
            None => {
                self.slots.push(Some(slot));
                self.slots.len() - 1
            }
        };
        self.index.insert(key, at);
        self.used += cost;
        self.usage_changed();
    }

    /// Restates what the cache holds, for whatever is recording.
    fn usage_changed(&self) {
        if let Some((usage, _, _)) = self.metrics.as_ref().map(|m| m.of(self.which)) {
            usage.set(self.used as i64);
        }
    }

    /// Records the handles this cache reports through, and its budget.
    pub(crate) fn record_into(
        &mut self,
        metrics: std::rc::Rc<crate::metrics::CacheMetrics>,
        which: usize,
    ) {
        let (_, limit, _) = metrics.of(which);
        limit.set(self.budget as i64);
        self.metrics = Some(metrics);
        self.which = which;
    }

    /// Sweeps until `need` more fits, clearing second chances as it goes.
    fn make_room(&mut self, need: usize) {
        if self.slots.is_empty() {
            return;
        }
        // One full sweep clears every second-chance bit, so a second must
        // find something to evict. The bound is there so that a cache whose
        // accounting has gone wrong overshoots its budget rather than spins.
        let limit = self.slots.len() * 2 + 2;
        let mut steps = 0;
        while self.used + need > self.budget && !self.index.is_empty() && steps < limit {
            steps += 1;
            match self.slots[self.hand].as_mut() {
                None => {}
                Some(slot) if slot.referenced => slot.referenced = false,
                Some(_) => {
                    let slot = self.slots[self.hand].take().expect("just matched");
                    self.index.remove(&slot.key);
                    self.used -= slot.cost;
                    self.free.push(self.hand);
                    if let Some((_, _, evictions)) = self.metrics.as_ref().map(|m| m.of(self.which))
                    {
                        evictions.add(1);
                    }
                }
            }
            self.hand = (self.hand + 1) % self.slots.len();
        }
    }

    /// How many entries it holds.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// How much of the budget is spent.
    pub fn used(&self) -> usize {
        self.used
    }
}

/// The table cache's shape: a CLOCK of open readers, shared by `Rc`.
type TableClock<F> = Rc<RefCell<Clock<TableKey, Rc<OpenTable<<F as Fs>::Disk>>>>>;

/// The block cache's shape. Not parameterised by the filesystem: a block is
/// bytes, and keeping it that way is what stops a cycle — the table cache
/// holds open tables, and an open table holds one of these.
type BlockClock = Rc<RefCell<Clock<BlockKey, Rc<Vec<u8>>>>>;

/// What identifies a block: the store, the table, and where in it.
///
/// The store for the same reason tables need it — file numbers are per
/// database — and the offset rather than the block's index, because that is
/// what a handle gives and what survives a table being reopened.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct BlockKey {
    store: u64,
    file_num: u64,
    offset: u64,
}

/// One store's view of the block cache.
///
/// Held by every open table, which is why it carries the store id rather than
/// taking one per call: a table belongs to exactly one store.
#[derive(Clone)]
pub struct BlockCache {
    clock: BlockClock,
    store: u64,
}

impl BlockCache {
    /// The decoded block at `offset`, if it is still held.
    ///
    /// Decoded, not as read: a compressed block is decompressed once and kept
    /// that way, so what is cached is no longer the bytes that came off the
    /// disk (§8).
    pub fn get(&self, file_num: u64, offset: u64) -> Option<Rc<Vec<u8>>> {
        self.clock.borrow_mut().get(&BlockKey {
            store: self.store,
            file_num,
            offset,
        })
    }

    /// Keeps a decoded block, evicting until it fits.
    pub fn hold(&self, file_num: u64, offset: u64, bytes: Rc<Vec<u8>>) {
        let cost = bytes.len();
        self.clock.borrow_mut().insert(
            BlockKey {
                store: self.store,
                file_num,
                offset,
            },
            bytes,
            cost,
        );
    }

    /// How many bytes of blocks are held, across every store.
    pub fn bytes_held(&self) -> usize {
        self.clock.borrow().used()
    }

    /// How many blocks are held, across every store.
    pub fn len(&self) -> usize {
        self.clock.borrow().len()
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The caches every store on one thread shares.
///
/// Cloning is cheap and shares: two stores given the same `Caches` draw on one
/// budget, which is the point (§8).
pub struct Caches<F: Fs> {
    tables: TableClock<F>,
    blocks: BlockClock,
    next_store: Rc<std::cell::Cell<u64>>,
    /// What the caches themselves export, held so a clone shares it.
    metrics: Rc<crate::metrics::CacheMetrics>,
}

/// What identifies a table inside a shared cache.
///
/// **File numbers are per database**, so the store has to be part of the key.
/// Two shards on one thread both have a table 000007, and a cache keyed by
/// file number alone would hand one shard the other's table — a wrong answer,
/// not an error. Pebble has the same problem and solves it the same way, with
/// a per-database cache id.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct TableKey {
    store: u64,
    file_num: u64,
}

// Derived `Clone` would demand `F: Clone`, which has nothing to do with it:
// what is cloned is an `Rc`.
impl<F: Fs> Clone for Caches<F> {
    fn clone(&self) -> Self {
        Caches {
            tables: Rc::clone(&self.tables),
            blocks: Rc::clone(&self.blocks),
            next_store: Rc::clone(&self.next_store),
            metrics: Rc::clone(&self.metrics),
        }
    }
}

impl<F: Fs> Caches<F> {
    /// Caches holding `open_tables` readers and `block_bytes` of blocks.
    pub fn new(open_tables: usize, block_bytes: usize) -> Self {
        Caches {
            tables: Rc::new(RefCell::new(Clock::new(open_tables))),
            blocks: Rc::new(RefCell::new(Clock::new(block_bytes))),
            next_store: Rc::new(std::cell::Cell::new(0)),
            metrics: Rc::new(crate::metrics::CacheMetrics::default()),
        }
    }

    /// Exports what the caches hold through `provider`, with `attributes`.
    ///
    /// Theirs rather than any store's: a `Caches` is shared by every store on
    /// a thread (§8), so its budget, what it holds and what it evicts are
    /// facts about the thread, and a series per store would be the same
    /// number repeated. Which store *looked* in it is the store's own
    /// `roble.cache.lookups`.
    pub fn meter_provider<P: opentelemetry::metrics::MeterProvider + ?Sized>(
        self,
        provider: &P,
        attributes: &[opentelemetry::KeyValue],
    ) -> Self {
        let metrics = Rc::new(crate::metrics::CacheMetrics::new(provider, attributes));
        self.blocks
            .borrow_mut()
            .record_into(Rc::clone(&metrics), crate::metrics::BLOCK_CACHE);
        self.tables
            .borrow_mut()
            .record_into(Rc::clone(&metrics), crate::metrics::TABLE_CACHE);
        Caches { metrics, ..self }
    }

    /// One store's view of the block cache.
    pub fn blocks(&self, store: u64) -> BlockCache {
        BlockCache {
            clock: Rc::clone(&self.blocks),
            store,
        }
    }

    /// How many bytes of blocks are held.
    pub fn block_bytes(&self) -> usize {
        self.blocks.borrow().used()
    }

    /// How many blocks are held.
    pub fn blocks_held(&self) -> usize {
        self.blocks.borrow().len()
    }

    /// An identifier for a store about to draw on these caches.
    ///
    /// Handed out once per store, so that two stores sharing a cache cannot
    /// read each other's tables through a file number they both use.
    pub fn new_store_id(&self) -> u64 {
        let id = self.next_store.get();
        self.next_store.set(id + 1);
        id
    }

    /// The open table for one store's `file_num`, if it is still held.
    pub fn table(&self, store: u64, file_num: u64) -> Option<Rc<OpenTable<F::Disk>>> {
        self.tables.borrow_mut().get(&TableKey { store, file_num })
    }

    /// Holds an open table, evicting another if the count is full.
    pub fn hold_table(&self, store: u64, file_num: u64, table: Rc<OpenTable<F::Disk>>) {
        self.tables
            .borrow_mut()
            .insert(TableKey { store, file_num }, table, 1);
    }

    /// Lets go of one store's reader for `file_num`.
    ///
    /// What the obsolete sweep calls before deleting a table: the cache
    /// holds a reader for as long as its budget allows, and a reader nobody
    /// is reading is not a reason to keep a file on disk (§5).
    pub fn forget_table(&self, store: u64, file_num: u64) {
        self.tables
            .borrow_mut()
            .remove(&TableKey { store, file_num });
    }

    /// How many tables are open.
    pub fn open_tables(&self) -> usize {
        self.tables.borrow().len()
    }
}

impl<F: Fs> Default for Caches<F> {
    fn default() -> Self {
        Caches::new(DEFAULT_OPEN_TABLES, DEFAULT_BLOCK_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_keys_spread_over_the_low_bits_the_map_buckets_by() {
        // The map takes a key's bucket from the low bits of its hash, so a
        // hash that left them alike would put every block of a table in a
        // few buckets — a map that still answers, and slowly. A thousand
        // blocks of consecutive tables, sixteen kilobytes apart, should
        // land in most of a thousand buckets.
        use std::hash::BuildHasher;
        let build = WordHash::default();
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000u64 {
            let key = BlockKey {
                store: 1,
                file_num: 7 + i / 50,
                offset: (i % 50) * 16 * 1024,
            };
            seen.insert(build.hash_one(key) & 1023);
        }
        assert!(seen.len() > 600, "only {} of 1024 buckets", seen.len());
    }

    /// The default table cache keeps everything, which is what makes it a
    /// bound nobody trips over rather than a cliff somebody falls off.
    ///
    /// A count smaller than the store is the shape that hurts: the entry
    /// evicted is the one about to be asked for, so the cost is not a lower
    /// hit rate but a reopen on nearly every lookup.
    #[test]
    fn an_unlimited_cache_evicts_nothing() {
        let mut c: Clock<u32, u32> = Clock::new(DEFAULT_OPEN_TABLES);
        // More than the old default of 128, so this would have swept.
        for k in 0..1_000 {
            c.insert(k, k, 1);
        }
        assert_eq!(c.len(), 1_000, "an unlimited cache dropped something");
        for k in 0..1_000 {
            assert_eq!(c.get(&k), Some(k), "entry {k} was evicted");
        }
    }

    /// And a budget still bounds, because a deployment that cares about file
    /// descriptors has to be able to say so (§8).
    #[test]
    fn a_budget_still_bounds_when_one_is_given() {
        let mut c: Clock<u32, u32> = Clock::new(16);
        for k in 0..1_000 {
            c.insert(k, k, 1);
        }
        assert_eq!(c.len(), 16, "a cache given a budget did not keep to it");
    }

    #[test]
    fn a_touch_buys_one_reprieve_from_the_sweep() {
        // The second chance, precisely. A newly inserted entry arrives
        // without one, so the first sweep takes the first entry it reaches.
        // After that, touching an entry is what moves the axe onto its
        // neighbour.
        let mut c: Clock<u32, u32> = Clock::new(3);
        for k in 0..3 {
            c.insert(k, k, 1);
        }

        c.insert(3, 3, 1);
        assert_eq!(c.len(), 3, "the budget is a count of three");
        assert!(c.get(&0).is_none(), "the hand took the first entry");

        // Now 1 is touched and 2 is not, so the next sweep must take 2.
        assert_eq!(c.get(&1), Some(1));
        c.insert(4, 4, 1);
        assert_eq!(c.get(&1), Some(1), "the touched entry survived");
        assert!(c.get(&2).is_none(), "the untouched one did not");
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn an_entry_read_once_goes_before_one_read_again() {
        // A was read again after it came in and B was not, and B is past
        // A as the hand goes. B arrives without a second chance, so the
        // sweep that clears A's takes B. Arriving with one, B would have its
        // bit cleared too, the sweep would come round to A first, and the
        // entry that was read again would go for the one that was not.
        let mut c: Clock<u32, u32> = Clock::new(2);
        c.insert(0, 0, 1);
        assert_eq!(c.get(&0), Some(0));
        c.insert(1, 1, 1);
        c.insert(2, 2, 1);
        assert_eq!(c.get(&0), Some(0), "the entry read again was taken");
        assert!(c.get(&1).is_none(), "the entry read once should have gone");
    }

    #[test]
    fn everything_referenced_still_makes_room() {
        // The case the sweep has to handle: every entry has its second-chance
        // bit set, so the first pass clears them and the second evicts.
        let mut c: Clock<u32, u32> = Clock::new(4);
        for k in 0..4 {
            c.insert(k, k, 1);
            assert_eq!(c.get(&k), Some(k));
        }
        c.insert(99, 99, 1);
        assert_eq!(c.len(), 4, "still within budget: {}", c.len());
        assert_eq!(c.get(&99), Some(99));
    }

    #[test]
    fn a_budget_in_bytes_is_counted_in_bytes() {
        let mut c: Clock<u32, u32> = Clock::new(100);
        c.insert(1, 1, 60);
        c.insert(2, 2, 30);
        assert_eq!(c.used(), 90);
        // 60 + 30 + 50 does not fit, so something goes.
        c.insert(3, 3, 50);
        assert!(c.used() <= 100, "used {} over budget", c.used());
        assert_eq!(c.get(&3), Some(3));
    }

    #[test]
    fn an_entry_larger_than_the_budget_is_not_cached() {
        // Better than evicting everything for one oversized thing.
        let mut c: Clock<u32, u32> = Clock::new(10);
        c.insert(1, 1, 4);
        c.insert(2, 2, 99);
        assert_eq!(c.get(&2), None, "the oversized entry is not held");
        assert_eq!(
            c.get(&1),
            Some(1),
            "and it evicted nothing to find that out"
        );
    }

    #[test]
    fn replacing_a_key_does_not_double_count_it() {
        let mut c: Clock<u32, u32> = Clock::new(100);
        c.insert(1, 1, 40);
        c.insert(1, 7, 40);
        assert_eq!(c.len(), 1);
        assert_eq!(c.used(), 40, "used {} after replacing one entry", c.used());
        assert_eq!(c.get(&1), Some(7));
    }
}
