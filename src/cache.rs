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
use std::hash::Hash;
use std::rc::Rc;

use crate::env::Fs;
use crate::table::OpenTable;

/// How many tables one thread keeps open by default.
///
/// A count rather than a size: what an open reader costs that matters first is
/// a file descriptor.
pub const DEFAULT_OPEN_TABLES: usize = 128;

struct Slot<K, V> {
    key: K,
    value: V,
    cost: usize,
    /// The second-chance bit: set on every hit, cleared by the sweep.
    referenced: bool,
}

/// A CLOCK cache with a budget expressed in whatever unit `cost` is.
pub struct Clock<K: Eq + Hash + Clone, V: Clone> {
    slots: Vec<Option<Slot<K, V>>>,
    index: HashMap<K, usize>,
    free: Vec<usize>,
    hand: usize,
    used: usize,
    budget: usize,
}

impl<K: Eq + Hash + Clone, V: Clone> Clock<K, V> {
    /// A cache that holds `budget` worth of entries.
    pub fn new(budget: usize) -> Self {
        Clock {
            slots: Vec::new(),
            index: HashMap::new(),
            free: Vec::new(),
            hand: 0,
            used: 0,
            budget,
        }
    }

    /// Looks `key` up, giving it a second chance if it is there.
    pub fn get(&mut self, key: &K) -> Option<V> {
        let at = *self.index.get(key)?;
        let slot = self.slots[at].as_mut()?;
        slot.referenced = true;
        Some(slot.value.clone())
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
            referenced: true,
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

/// The caches every store on one thread shares.
///
/// Cloning is cheap and shares: two stores given the same `Caches` draw on one
/// budget, which is the point (§8).
pub struct Caches<F: Fs> {
    tables: TableClock<F>,
    next_store: Rc<std::cell::Cell<u64>>,
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
            next_store: Rc::clone(&self.next_store),
        }
    }
}

impl<F: Fs> Caches<F> {
    /// Caches holding `open_tables` readers.
    pub fn new(open_tables: usize) -> Self {
        Caches {
            tables: Rc::new(RefCell::new(Clock::new(open_tables))),
            next_store: Rc::new(std::cell::Cell::new(0)),
        }
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

    /// How many tables are open.
    pub fn open_tables(&self) -> usize {
        self.tables.borrow().len()
    }
}

impl<F: Fs> Default for Caches<F> {
    fn default() -> Self {
        Caches::new(DEFAULT_OPEN_TABLES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_touch_buys_one_reprieve_from_the_sweep() {
        // The second chance, precisely. A newly inserted entry arrives with
        // its bit set — it has just been used — so a brand new cache has no
        // victim and the first sweep is what creates one by clearing bits.
        // After that, touching an entry is what moves the axe onto its
        // neighbour.
        let mut c: Clock<u32, u32> = Clock::new(3);
        for k in 0..3 {
            c.insert(k, k, 1);
        }

        // This sweep clears every bit and takes the first entry it comes back
        // to, leaving 1 and 2 with no second chance.
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
