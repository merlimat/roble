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

//! Scanning: one iterator per table, merged into one sequence (§4).
//!
//! Three rules, and the reverse direction is where each of them bites.
//!
//! **The least internal key is the answer, going forward.** User keys ascend
//! and trailers descend, so the smallest internal key across the iterators is
//! not merely the smallest user key — it is also that user key's *newest*
//! version. One comparison settles both questions, which is why nothing here
//! consults a level.
//!
//! **Going backward it is not.** The largest internal key gives the largest
//! user key, but the *oldest* version of it. So a backward step walks every
//! iterator back over that user key, keeping the smallest internal key it
//! passes — the newest version — and ends with every iterator positioned
//! before the key entirely.
//!
//! **A direction change re-seeks.** Going forward leaves the iterators past
//! the current key; going backward leaves them before it. Turning round means
//! they are all on the wrong side, so they are sought again rather than
//! stepped, and the [`Heap`] is rebuilt the other way round with them, since
//! its top is the least internal key or the greatest and never both. That
//! costs one seek per source plus a heapify at the turn, and nothing while
//! the direction holds, which is the trade Pebble's merging iterator makes
//! too — `switchToMinHeap` and `switchToMaxHeap` re-seek the levels and
//! reinitialise the heap in one step.
//!
//! What a scan fixes, which `get` still pays: the four ordered comparisons in
//! [`crate::read`] re-seek every table once per key they reject. Here the
//! iterators keep their place, so a run of deleted keys costs a step each
//! rather than a seek each (§6).
//!
//! Two rewrites of this loop were built, measured and thrown away: lending a
//! value rather than copying it into the caller's buffer (1.01), and
//! replacing the two passes over the sources with a single merged stream
//! (0.921). Both remove real work, and the step is not waiting on work — §8
//! has the measurement and what follows from it.
//!
//! The [`Heap`] below is not a third of those, and the distinction is §8's
//! own: the three changes that did pay were asymptotic rather than constant,
//! and this one removes the O(sources) term from a step instead of shaving
//! instructions off it. The thrown-away stream rewrite restructured the two
//! passes while keeping that term; this removes it, and folds the second pass
//! into the heap that replaced the first.

use std::rc::Rc;

use roble_format::InternalKey;
use roble_format::key::{self, Kind};

use crate::env::Disk;
use crate::error::Result;
use crate::read::successor;
use std::cell::RefCell;

use crate::memtable::{Memtable, MemtableCursor};
use crate::table::{OpenTable, TableIter, covered_by_tombstone, is_deletion};

/// One source of entries in a merge: a table, a run of them, or the memtable.
///
/// A table and the memtable differ in more than where their bytes are. A
/// table's contents are fixed, so everything in it is older than any scan;
/// the memtable is still being written to, so a scan has to ignore what
/// arrived after it began. That is what `snapshot` is for, and it is the only
/// place it is needed.
///
/// A table is its own source when nothing can be read with it: L0's, whose
/// ranges overlap, and the only table of a level. That is not merely the
/// degenerate run, it is the cheaper one — a run of one carries a bound it
/// never consults and a `Vec` to hold a single table in.
enum Source<D: Disk> {
    Table(TableIter<D>),
    Run(Run<D>),
    Mem(MemSource),
}

/// A table as a version places it: the level it is in, and the smallest user
/// key its bounds reach.
///
/// What [`Scan::new`] needs to group tables into runs, and all it needs — the
/// level says which tables may be read as one, the bound says which of them
/// to look in. Both are read out of the version while it is borrowed, since
/// that is the only place the exact answers are.
pub(crate) struct Placed<D: Disk> {
    pub level: u32,
    pub table: Rc<OpenTable<D>>,
    /// Its bound, or `None` when no run will consult it: a table of L0,
    /// which are never read as one, or the only table of its level.
    ///
    /// The bound is there to choose between the tables of a level, and a
    /// caller looking at the level knows there is nothing to choose before it
    /// has copied a key — which is the point, since copying one is what this
    /// avoids. Passing `Some` anyway is not wrong, only a run of one and the
    /// key it took to build.
    pub smallest: Option<Vec<u8>>,
}

/// A level's tables, read in order as one source.
///
/// Every level below L0 holds tables whose key ranges do not overlap, so at
/// most one of them can hold any given key and they can be read one after
/// another as if they were a single table. L0's do overlap, so each of those
/// is a run of one — which is what every table used to be.
///
/// What the grouping saves is a seek and a comparison per table. A seek,
/// because positioning a scan used to seek every table it held and throw all
/// but one answer away: `floor` over a store settled into fourteen tables
/// spent 470 ns a table doing that, six sevenths of its 2 µs. And a
/// comparison, because the merge used to walk every source on every step,
/// 7.5 ns a table a step — over half of what a forward scan cost at the same
/// shape.
///
/// The second of those two arguments is weaker now that the merge is a
/// [`Heap`]: a step sifts rather than scans, so a source costs a step
/// `log(sources)` rather than one comparison each. The grouping still pays —
/// the seek per table is untouched, and a smaller heap is a shallower one —
/// but it is no longer most of a step, and this is where that was claimed.
///
/// Correctness rests on the disjointness and nothing else. Two sources at the
/// same key are settled by sequence number (`take_newest_visible`), never by
/// which source they came from, so collapsing sources that *cannot* be at the
/// same key cannot change an answer; collapsing L0's, which can, would.
struct Run<D: Disk> {
    /// In key order, ascending, no two of them reaching the same key.
    tables: Vec<RunTable<D>>,
    /// Which one is positioned.
    ///
    /// Always a valid index: a run is only ever built around a table. Whether
    /// the run is on a key is that iterator's business, not this field's.
    at: usize,
}

/// One table in a run, with the bound that says when to read it.
struct RunTable<D: Disk> {
    iter: TableIter<D>,
    /// The smallest user key the table's bounds reach, from the version.
    ///
    /// It has to come from there. A table's own index offers no lower bound
    /// at all: an index entry's separator is an upper bound on its block,
    /// "at or above every key in the block". And an upper bound will not do
    /// instead, because it may overshoot the largest key it stands for — a
    /// run choosing its table by separators would, with
    /// `largest(i) < target <= separator(i)` and table i + 1 beginning below
    /// `target` as well, answer `seek_lt` out of table i while the key it was
    /// asked for sat in i + 1. The version's bounds are exact.
    smallest: Vec<u8>,
}

impl<D: Disk> Run<D> {
    fn is_valid(&self) -> bool {
        self.tables[self.at].iter.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.tables[self.at].iter.key()
    }

    async fn value_into(&self, out: &mut Vec<u8>) -> Result<()> {
        self.tables[self.at].iter.value_into(out).await
    }

    /// Positions on the first entry at or after `target`, an internal key.
    ///
    /// One seek, into the one table whose range can hold the target, where
    /// every table in the run used to be seeked and all but one of the
    /// answers thrown away.
    async fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        let user = InternalKey::decode(target)?.user_key;
        // The last table beginning at or below the target: the one after it
        // begins above the target, and the one before it ends below it.
        //
        // Only the start, and only one direction of it matters. Choosing a
        // table too early costs a seek and no answer, since `onward` carries
        // on from it; choosing one too late skips the keys in between, which
        // is what the bounds are here to prevent. So `<=` rather than `<` is
        // worth a seek and is not worth a test — a `<` here answers
        // everything the same way, one wasted seek later.
        self.at = self
            .tables
            .partition_point(|t| t.smallest.as_slice() <= user)
            .saturating_sub(1);
        self.tables[self.at].iter.seek_ge(target).await?;
        self.onward().await
    }

    /// Positions on the last entry before `target`, an internal key.
    async fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        let user = InternalKey::decode(target)?.user_key;
        // The last table beginning below the target, with the same slack the
        // other way: too late costs a seek, too early skips keys. None of
        // them begins below it when the whole run is at or above the target,
        // and then there is no answer to find — seeking the first table finds
        // none, which is what leaves the run invalid.
        self.at = self
            .tables
            .partition_point(|t| t.smallest.as_slice() < user)
            .saturating_sub(1);
        self.tables[self.at].iter.seek_lt(target).await?;
        self.back().await
    }

    /// Positions on the run's last entry.
    async fn last(&mut self) -> Result<()> {
        self.at = self.tables.len() - 1;
        self.tables[self.at].iter.last().await?;
        self.back().await
    }

    /// Steps forward, crossing into the next table at the end of this one.
    async fn next(&mut self) -> Result<()> {
        self.tables[self.at].iter.next().await?;
        self.onward().await
    }

    /// Steps backward, crossing into the previous table at the start of this
    /// one.
    async fn prev(&mut self) -> Result<()> {
        self.tables[self.at].iter.prev().await?;
        self.back().await
    }

    /// Moves on to the next table for as long as the one the run is at has
    /// nothing to offer, and to its first entry, which is then the answer:
    /// everything past the table a seek chose begins above what it asked for.
    ///
    /// Reached in two ways, and neither is the edge case it looks. A table
    /// can simply be spent, which is how a scan crosses from one table of a
    /// level into the next. Or a seek can choose a table that turns out to
    /// hold no point key that high — the target fell into the gap between two
    /// tables' ranges, or the table is one of the range-deletion-only ones
    /// Pebble writes, whose bounds reach keys its data blocks do not.
    async fn onward(&mut self) -> Result<()> {
        while !self.tables[self.at].iter.is_valid() && self.at + 1 < self.tables.len() {
            self.at += 1;
            self.tables[self.at].iter.first().await?;
        }
        Ok(())
    }

    /// The same backwards, to the previous table's last entry.
    async fn back(&mut self) -> Result<()> {
        while !self.tables[self.at].iter.is_valid() && self.at > 0 {
            self.at -= 1;
            self.tables[self.at].iter.last().await?;
        }
        Ok(())
    }
}

/// The memtable, as a merge source.
///
/// Holds the current key copied out rather than borrowed: the memtable lives
/// behind a `RefCell` so that a write can reach it, and a borrow of that
/// cannot be held between calls.
///
/// Note that a cursor which has gone invalid stays invalid — stepping it does
/// nothing — so a scan that began on an empty memtable never looks at it
/// again. That happens to give the same answers as the snapshot rule, since
/// anything written later is above the snapshot anyway, but it is **not** the
/// rule: with a memtable that was not empty, the cursor walks right into the
/// new entries and only the sequence number keeps them out.
struct MemSource {
    table: Rc<RefCell<Memtable>>,
    cursor: MemtableCursor,
    key: Vec<u8>,
    valid: bool,
}

impl MemSource {
    fn capture(&mut self) {
        let table = self.table.borrow();
        self.valid = self.cursor.is_valid();
        if self.valid {
            self.key.clear();
            self.key.extend_from_slice(self.cursor.key(&table));
        }
    }
}

impl<D: Disk> Source<D> {
    fn is_valid(&self) -> bool {
        match self {
            Source::Table(it) => it.is_valid(),
            Source::Run(run) => run.is_valid(),
            Source::Mem(m) => m.valid,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Source::Table(it) => it.key(),
            Source::Run(run) => run.key(),
            Source::Mem(m) => {
                // A table's iterator panics when it is asked for a key it is
                // not on, and a memtable's would hand back the last key it
                // copied — `capture` leaves the buffer alone when the cursor
                // has gone invalid, because clearing it would cost a step
                // that is about to be thrown away anyway.
                //
                // Spelled out because of the shape of the failure. [`Heap`]
                // holds only sources that are on an entry, and a bug that
                // broke that invariant would, through this arm, leave a
                // spent source at the top of the heap answering with a key
                // that no longer exists — which is not a wrong answer but a
                // loop that never ends. Pebble guards its heap's
                // `REQUIRES: every iterKV is non-nil` under
                // `invariants.Enabled` for the same reason.
                debug_assert!(m.valid, "a memtable source is not on an entry");
                &m.key
            }
        }
    }

    /// The value the source is on, into a buffer the caller keeps.
    ///
    /// What a step reads, where it used to read the whole entry: `entry`
    /// builds a `Vec` for the key as well, and a merge step has the key
    /// already — it compared it to find this source. That was two
    /// allocations a step, of which one was thrown away unread.
    async fn value_into(&self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Source::Table(it) => it.value_into(out).await,
            Source::Run(run) => run.value_into(out).await,
            Source::Mem(m) => {
                let table = m.table.borrow();
                let value = m.cursor.value(&table);
                out.clear();
                out.reserve(value.len());
                out.extend_from_slice(value);
                Ok(())
            }
        }
    }

    async fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        match self {
            Source::Table(it) => it.seek_ge(target).await,
            Source::Run(run) => run.seek_ge(target).await,
            Source::Mem(m) => {
                m.cursor.seek_ge(&m.table.borrow(), target)?;
                m.capture();
                Ok(())
            }
        }
    }

    async fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        match self {
            Source::Table(it) => it.seek_lt(target).await,
            Source::Run(run) => run.seek_lt(target).await,
            Source::Mem(m) => {
                m.cursor.seek_lt(&m.table.borrow(), target)?;
                m.capture();
                Ok(())
            }
        }
    }

    async fn last(&mut self) -> Result<()> {
        match self {
            Source::Table(it) => it.last().await,
            Source::Run(run) => run.last().await,
            Source::Mem(m) => {
                m.cursor.last(&m.table.borrow());
                m.capture();
                Ok(())
            }
        }
    }

    async fn next(&mut self) -> Result<()> {
        match self {
            Source::Table(it) => it.next().await,
            Source::Run(run) => run.next().await,
            Source::Mem(m) => {
                m.cursor.next(&m.table.borrow());
                m.capture();
                Ok(())
            }
        }
    }

    async fn prev(&mut self) -> Result<()> {
        match self {
            Source::Table(it) => it.prev().await,
            Source::Run(run) => run.prev().await,
            Source::Mem(m) => {
                m.cursor.prev(&m.table.borrow());
                m.capture();
                Ok(())
            }
        }
    }
}

impl<D: Disk> Drop for Scan<D> {
    /// A scan that has gone away is one fewer open, and a store that keeps
    /// reporting scans nobody holds is how an iterator leak hides.
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.scans_open.add(-1);
        }
    }
}

/// Which way the iterators are currently positioned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dir {
    Forward,
    Reverse,
}

/// Where the scan is.
#[derive(Default)]
struct Current {
    user: Vec<u8>,
    value: Vec<u8>,
}

/// A scan over the tables of a version.
///
/// Forward and reverse, optionally bounded. The bounds are half-open as
/// Pebble's are: `lower` is included and `upper` is not.
pub struct Scan<D: Disk> {
    iters: Vec<Source<D>>,
    /// The sources arranged so that the next key is the top of a heap rather
    /// than the winner of a look at every one of them.
    ///
    /// Empty until the scan is positioned, and rebuilt by every call that
    /// positions it. Nothing reads it before then: `next` and `prev` return
    /// at once while `current` is `None`, and every other entry point seeks
    /// first.
    heap: Heap,
    /// The buffers of the key this scan has moved off, kept to be filled
    /// again.
    ///
    /// A step has to copy the user key out of a source before it can touch
    /// that source mutably, and the copy is what `key()` then hands out — so
    /// it cannot borrow and it cannot be skipped. What it can do is reuse the
    /// `Vec` the previous key was in, which is free the moment this scan
    /// steps off it (`advance`).
    spare: Option<Current>,
    /// The tables of this scan that hold a range tombstone at all.
    ///
    /// What `visible` has to consult, and usually nothing: a table with no
    /// tombstones can never cover a key, so walking it is work that cannot
    /// change an answer. The sweep used to walk every table the scan held
    /// for every key it returned, and a profile of a forward scan over a
    /// store settled into fourteen tables put `visible` at a quarter of the
    /// whole scan — the largest single item in it, looking for tombstones
    /// that a store nobody has called `delete_range` on does not have.
    ///
    /// Decided when the scan is made, which is sound because a table's
    /// tombstones are fixed once it is written. The memtables below are
    /// still being written to, so they are consulted a key at a time.
    ///
    /// This is also every table the scan needs to name: the file itself is
    /// held open by the iterator reading it (`crate::table::TableIter` keeps
    /// an `Rc` of its table), so the obsolete sweep is kept off all of them
    /// without a list of its own (§5, `crate::obsolete`).
    tombstoned: Vec<Rc<OpenTable<D>>>,
    /// Kept for their range tombstones, which are not in any source.
    ///
    /// More than one when a scan is reading through an uncommitted batch
    /// (`crate::batch`): the store's memtable, and the batch's own index.
    memtables: Vec<Rc<RefCell<Memtable>>>,
    /// The highest sequence number this scan can see.
    ///
    /// Taken when the scan is made, so a write that happens while it is open
    /// is invisible to it: the new entry has a higher sequence number and is
    /// skipped. That is the whole of the snapshot (§4) — no copying, no
    /// reference counts, just a number and the fact that sequence numbers
    /// only go up.
    snapshot: u64,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    dir: Dir,
    current: Option<Current>,
    /// The store's handles, so a scan can count what it steps onto. `None`
    /// for a scan made without a store, which only the crate's own tests do.
    metrics: Option<Rc<crate::metrics::Metrics>>,
}

impl<D: Disk> Scan<D> {
    /// A scan over `placed`, bounded by `lower` (included) and `upper`
    /// (excluded), positioned nowhere.
    ///
    /// Crate-private: its arguments are a store's internals, and the way to
    /// one from outside is [`crate::Opened::scan`].
    ///
    /// The tables arrive as the caller found them and are sorted here, by
    /// level and then by key, because that is what makes a level a run: the
    /// tables within one are disjoint but a version keeps them in file-number
    /// order ([`crate::version::Version::level`]), which is not key order.
    /// Sorting costs a few dozen comparisons of short keys once per scan, and
    /// buys a seek per table off every `seek` and a comparison per table off
    /// every step.
    pub(crate) fn new(
        mut placed: Vec<Placed<D>>,
        memtables: Vec<Rc<RefCell<Memtable>>>,
        snapshot: u64,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        metrics: Option<Rc<crate::metrics::Metrics>>,
    ) -> Self {
        placed.sort_by(|a, b| (a.level, &a.smallest).cmp(&(b.level, &b.smallest)));
        let tombstoned: Vec<Rc<OpenTable<D>>> = placed
            .iter()
            .filter(|p| !p.table.tombstones.is_empty())
            .map(|p| Rc::clone(&p.table))
            .collect();
        let mut iters: Vec<Source<D>> = Vec::with_capacity(placed.len() + memtables.len());
        // The level the run being built is still taking tables for, and
        // `None` at L0, where the tables overlap and so each has to stay a
        // source of its own.
        //
        // Both callers already withhold a bound from L0's tables, so the test
        // for it here fires for nothing and mutating it away changes no
        // answer. It stays because this is the only place that decides what
        // may share a source, and that is where the rule belongs — stated
        // rather than tested (§12), as in `Snapshot::lookup`.
        let mut joining: Option<u32> = None;
        for p in placed {
            let Some(smallest) = p.smallest else {
                iters.push(Source::Table(p.table.iter()));
                joining = None;
                continue;
            };
            let table = RunTable {
                iter: p.table.iter(),
                smallest,
            };
            match iters.last_mut() {
                Some(Source::Run(run)) if joining == Some(p.level) => run.tables.push(table),
                _ => {
                    joining = (p.level > 0).then_some(p.level);
                    iters.push(Source::Run(Run {
                        tables: vec![table],
                        at: 0,
                    }));
                }
            }
        }
        for m in &memtables {
            let cursor = m.borrow().cursor();
            iters.push(Source::Mem(MemSource {
                table: Rc::clone(m),
                cursor,
                key: Vec::new(),
                valid: false,
            }));
        }
        Scan {
            iters,
            heap: Heap::default(),
            spare: None,
            tombstoned,
            memtables,
            snapshot,
            lower,
            upper,
            dir: Dir::Forward,
            current: None,
            metrics,
        }
    }

    /// Counts one more key the scan has landed on, and the bytes it holds.
    fn stepped_onto(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.scan_entries.add(1);
            if let Some(current) = &self.current {
                metrics
                    .read_bytes
                    .add((current.user.len() + current.value.len()) as u64);
            }
        }
    }

    /// Whether the scan is on a key.
    pub fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    /// The user key it is on.
    ///
    /// # Panics
    /// If the scan is not on a key.
    pub fn key(&self) -> &[u8] {
        &self.current.as_ref().expect("scan is not on a key").user
    }

    /// Its value.
    ///
    /// # Panics
    /// If the scan is not on a key.
    pub fn value(&self) -> &[u8] {
        &self.current.as_ref().expect("scan is not on a key").value
    }

    /// Seeks every source to the first entry at or after `target` and builds
    /// the forward heap over them.
    ///
    /// The two belong together because a heap is good for one direction only:
    /// its top is the least internal key or the greatest, never both. So
    /// every call that repositions the sources wholesale has to say which way
    /// it left them, and this is where that is said. Pebble pairs them for
    /// the same reason, in `switchToMinHeap`.
    async fn sought_ge(&mut self, target: &[u8]) -> Result<()> {
        for it in &mut self.iters {
            it.seek_ge(target).await?;
        }
        self.dir = Dir::Forward;
        self.heap = Heap::new(&self.iters, Dir::Forward)?;
        Ok(())
    }

    /// The same backwards: every source before `target`, greatest on top.
    async fn sought_lt(&mut self, target: &[u8]) -> Result<()> {
        for it in &mut self.iters {
            it.seek_lt(target).await?;
        }
        self.dir = Dir::Reverse;
        self.heap = Heap::new(&self.iters, Dir::Reverse)?;
        Ok(())
    }

    /// Positions on the first key at or after `from`, respecting the bounds.
    pub async fn seek_ge(&mut self, from: &[u8]) -> Result<()> {
        let from = match &self.lower {
            Some(lower) if lower.as_slice() > from => lower.clone(),
            _ => from.to_vec(),
        };
        let target = key::search_key(&from);
        self.sought_ge(&target).await?;
        self.advance().await
    }

    /// Positions on the last key before `bound`, respecting the bounds.
    pub async fn seek_lt(&mut self, bound: &[u8]) -> Result<()> {
        let bound = match &self.upper {
            Some(upper) if upper.as_slice() < bound => upper.clone(),
            _ => bound.to_vec(),
        };
        let target = key::search_key(&bound);
        self.sought_lt(&target).await?;
        self.retreat().await
    }

    /// Positions on the first key in range.
    pub async fn first(&mut self) -> Result<()> {
        let from = self.lower.clone().unwrap_or_default();
        self.seek_ge(&from).await
    }

    /// Positions on the last key in range.
    pub async fn last(&mut self) -> Result<()> {
        match self.upper.clone() {
            Some(upper) => self.seek_lt(&upper).await,
            None => {
                for it in &mut self.iters {
                    it.last().await?;
                }
                self.dir = Dir::Reverse;
                self.heap = Heap::new(&self.iters, Dir::Reverse)?;
                self.retreat().await
            }
        }
    }

    /// Steps to the next key.
    #[allow(clippy::should_implement_trait)]
    pub async fn next(&mut self) -> Result<()> {
        let Some(current) = &self.current else {
            return Ok(());
        };
        if self.dir == Dir::Reverse {
            // The iterators are all before the current key; going forward
            // means seeking past it.
            let target = key::search_key(&successor(&current.user));
            self.sought_ge(&target).await?;
        }
        // Going forward, `advance` has already left them past the current key.
        self.advance().await
    }

    /// Steps to the previous key.
    pub async fn prev(&mut self) -> Result<()> {
        let Some(current) = &self.current else {
            return Ok(());
        };
        if self.dir == Dir::Forward {
            let target = key::search_key(&current.user);
            self.sought_lt(&target).await?;
        }
        self.retreat().await
    }

    /// Moves to the next visible key, skipping deletions.
    async fn advance(&mut self) -> Result<()> {
        loop {
            let Some(w) = self.heap.top() else {
                self.current = None;
                return Ok(());
            };
            // Into the buffer the last key vacated, which is owned locally
            // for the rest of this turn of the loop and so conflicts with no
            // borrow of `self`.
            let mut next = self.spare.take().unwrap_or_default();
            next.user.clear();
            next.user
                .extend_from_slice(InternalKey::decode(self.iters[w].key())?.user_key);
            let chosen = self
                .take_newest_visible(&next.user, Dir::Forward, &mut next.value)
                .await?;

            if let Some(upper) = &self.upper
                && next.user >= *upper
            {
                self.current = None;
                return Ok(());
            }
            if let Some((seq, kind)) = chosen
                && self.visible(&next.user, seq, kind)?
            {
                self.spare = self.current.replace(next);
                self.stepped_onto();
                return Ok(());
            }
            // A deletion, so round again — and the buffers go back rather
            // than being dropped.
            self.spare = Some(next);
        }
    }

    /// Moves to the previous visible key, skipping deletions.
    async fn retreat(&mut self) -> Result<()> {
        loop {
            let Some(w) = self.heap.top() else {
                self.current = None;
                return Ok(());
            };
            // Into the buffer the last key vacated, which is owned locally
            // for the rest of this turn of the loop and so conflicts with no
            // borrow of `self`.
            let mut next = self.spare.take().unwrap_or_default();
            next.user.clear();
            next.user
                .extend_from_slice(InternalKey::decode(self.iters[w].key())?.user_key);
            let chosen = self
                .take_newest_visible(&next.user, Dir::Reverse, &mut next.value)
                .await?;

            if let Some(lower) = &self.lower
                && next.user < *lower
            {
                self.current = None;
                return Ok(());
            }
            if let Some((seq, kind)) = chosen
                && self.visible(&next.user, seq, kind)?
            {
                self.spare = self.current.replace(next);
                self.stepped_onto();
                return Ok(());
            }
            // A deletion, so round again — and the buffers go back rather
            // than being dropped.
            self.spare = Some(next);
        }
    }

    /// Steps every source over every version of `user`, returning the newest
    /// one the snapshot can see.
    ///
    /// The same rule both ways: among the versions the sources hold, take the
    /// highest sequence number at or below the snapshot. Going forward the
    /// versions arrive newest first, so the first one taken is the answer and
    /// every later one is refused on its sequence number; going backward they
    /// arrive oldest first, so each newer one overwrites the value. That
    /// asymmetry is why reverse is dearer, and it is the same one the block
    /// cursor has.
    ///
    /// **Driven by the heap, which is what makes it cheap.** This used to
    /// walk all the sources and step each one past `user`, so a step paid a
    /// trailer decode and a user-key comparison per source whether or not
    /// that source was anywhere near the key — a second O(sources) beside
    /// [`Scan::advance`]'s. It does not need one: every version of `user` is
    /// at the top of the heap before anything else is, because the heap is
    /// ordered by internal key and `user`'s versions sort ahead of every key
    /// the merge has left (forward) or behind them (backward). So "pop while
    /// the top is still on this user key" visits exactly the sources holding
    /// it, which is one of them in the common case, and the first top that is
    /// on another key proves the rest are already clear of this one. Pebble's
    /// `findNextEntry` and `findPrevEntry` work the same way.
    ///
    /// **The order the versions arrive in is not load-bearing**, which is
    /// worth knowing because it is on the compaction path and this looks like
    /// that one. `crate::compact`'s merge keeps the *first* version it meets
    /// of a user key and drops the rest, so it depends on newest-first
    /// arrival; this picks the highest sequence number the snapshot allows,
    /// so the arrival order decides only how many times `value_into` runs.
    /// Flipping the trailer half of [`Heap::less`], in either direction or
    /// both, answers identically — hand mutation-testing confirms it, where
    /// the same mutation of the compaction heap is caught at once. Same
    /// structure, different half of it carrying the weight.
    ///
    /// Every source ends past the key — forward, past its last version;
    /// backward, before its first — so the caller can simply go round again.
    async fn take_newest_visible(
        &mut self,
        user: &[u8],
        dir: Dir,
        value: &mut Vec<u8>,
    ) -> Result<Option<(u64, Kind)>> {
        let mut chosen: Option<(u64, Kind)> = None;
        while let Some(i) = self.heap.top() {
            // The trailer, read out of the borrowed key rather than out of a
            // copy of it. Copying was how this ended the borrow before the
            // `value_into().await` below, which needs the source mutably —
            // and it cost one allocation per source per step. Scoping the
            // borrow to the three scalars this actually needs costs none.
            let (seq_num, kind, matches) = {
                let k = InternalKey::decode(self.iters[i].key())?;
                (k.seq_num, k.kind, k.user_key == user)
            };
            if !matches {
                break;
            }
            // A version written after this scan began is not its business.
            if visible_at(seq_num, self.snapshot) && chosen.is_none_or(|(seq, _)| seq_num > seq) {
                // Into the buffer the scan already holds, which a later
                // version with a higher sequence number simply overwrites:
                // `value_into` clears before it writes, so what is left at
                // the end is the newest visible version's and nothing else's.
                self.iters[i].value_into(value).await?;
                chosen = Some((seq_num, kind));
            }
            match dir {
                Dir::Forward => self.iters[i].next().await?,
                Dir::Reverse => self.iters[i].prev().await?,
            }
            self.heap.stepped(&self.iters)?;
        }
        Ok(chosen)
    }

    /// Whether the newest version of `user` is a live value.
    fn visible(&self, user: &[u8], seq: u64, kind: Kind) -> Result<bool> {
        if is_deletion(kind) {
            return Ok(false);
        }
        if covered_by_tombstone(&self.tombstoned, user, seq)? {
            return Ok(false);
        }
        // A memtable's tombstones are not in any source — they are kept
        // apart from the point keys (§4) — so they are consulted here.
        for m in &self.memtables {
            for t in m.borrow().tombstones() {
                if t.seq_num > seq
                    && visible_at(t.seq_num, self.snapshot)
                    && t.start.as_slice() <= user
                    && user < t.end.as_slice()
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

/// Whether an entry numbered `seq` is one a scan at `snapshot` may see.
///
/// Two rules, and the second is the whole of reading through an uncommitted
/// batch. An ordinary entry is visible when it was written at or before the
/// scan began. A batch's own entries carry [`key::SEQ_NUM_BATCH`] in place of
/// numbers they have not been given yet, and are always visible, because they
/// are what the scan was made to read through.
///
/// No second snapshot is needed for them, as Pebble's `base.Visible` has: a
/// batch index is only ever a source of a scan made from that same batch, and
/// nothing else in the database can carry the bit — committing replaces those
/// stand-ins with real sequence numbers.
fn visible_at(seq: u64, snapshot: u64) -> bool {
    seq <= snapshot || seq & key::SEQ_NUM_BATCH != 0
}

/// The sources of a scan, arranged so that the next key costs a sift rather
/// than a look at every one of them.
///
/// A step used to ask every source for its key twice over — once to find the
/// extreme one and once to walk the chosen user key's versions — so a scan
/// over a store settled into a memtable and a handful of levels paid a
/// comparison and a trailer decode per source per record. That is the term
/// §14 named as the read path's remaining structural cost, and it is the term
/// a heap removes: Pebble's merging iterator is a heap for this reason
/// (`mergingIterHeap` in `merging_iter_heap.go`), and this is that structure,
/// `winnerChild` included.
///
/// Every source in the heap is on an entry: one that runs out leaves, so
/// [`Heap::top`] needs no validity check and no comparison is ever made
/// against a source positioned nowhere.
///
/// Why a second copy of this rather than the one `crate::compact` already
/// has. The two merge different things — a compaction merges
/// `crate::table::TableIter`s while a scan merges [`Source`]s, which is
/// already an enum over a table, a run of them and a memtable — so sharing
/// would mean a trait for "the key at index i" over both. That part is free,
/// monomorphised. What is not is `reverse`: a scan needs a heap that flips
/// and a compaction merges forward only, so the shared `less` would carry a
/// direction a compaction never changes, in the one function a compaction's
/// merge calls most. That path is measured and this one is not yet, so the
/// duplication is the cheaper risk; worth revisiting if a third merge appears
/// or once both are measured over the same shapes.
#[derive(Default)]
struct Heap {
    /// A binary heap in an array, the answer at `items[0]`: the children of
    /// `i` are at `2i + 1` and `2i + 2`.
    items: Vec<Item>,
    /// Whether the order is flipped, so that the top is the *greatest*
    /// internal key — Pebble's `mergingIterHeap.reverse`.
    ///
    /// One heap that turns round rather than two structures, which is
    /// Pebble's choice and worth copying for a reason beyond the code saved:
    /// a scan's two directions have to agree about ties and about what "the
    /// answer" is, and one comparator with a flag cannot drift from itself
    /// the way a `least` and a `greatest` written out separately can.
    reverse: bool,
}

/// One position in the heap.
struct Item {
    /// The source whose key this position holds.
    source: usize,
    /// Which of **this position's** two children beat the other, when that
    /// is known — Pebble's `winnerChild`, and worth having for the reason its
    /// comment gives: a sift-down that knows this already compares the winner
    /// against the parent and stops, so a replacement that still wins costs
    /// one comparison instead of two.
    ///
    /// It belongs to the position and not to the source sitting there, which
    /// is why [`Heap::swap`] exchanges `source` alone. A swap into `i`
    /// changes what one of `i`'s children holds, so `i` forgets; the position
    /// swapped *down* to keeps what it knew, because its own children are
    /// untouched, and that is the whole saving.
    winner: Winner,
}

/// Which child of a heap position won, if it has been worked out.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Winner {
    Unknown,
    Left,
    Right,
}

impl Heap {
    /// Heapifies the sources that are on an entry, ordered for `dir`.
    fn new<D: Disk>(iters: &[Source<D>], dir: Dir) -> Result<Self> {
        let items = iters
            .iter()
            .enumerate()
            .filter(|(_, it)| it.is_valid())
            .map(|(source, _)| Item {
                source,
                winner: Winner::Unknown,
            })
            .collect();
        let mut heap = Heap {
            items,
            reverse: dir == Dir::Reverse,
        };
        for i in (0..heap.items.len() / 2).rev() {
            heap.down(i, heap.items.len(), iters)?;
        }
        Ok(heap)
    }

    /// The source on the extreme internal key — least going forward, greatest
    /// going backward — if any is still positioned.
    fn top(&self) -> Option<usize> {
        self.items.first().map(|item| item.source)
    }

    /// Puts the heap right after the source on top has been stepped.
    ///
    /// Only the top has moved, and it has moved the way the order runs, so a
    /// sift-down from the root is all that is owed — Pebble's `fixTop`. A
    /// source that has run out leaves instead, which is what keeps every key
    /// in the heap a key that exists.
    fn stepped<D: Disk>(&mut self, iters: &[Source<D>]) -> Result<()> {
        let n = self.items.len();
        if iters[self.items[0].source].is_valid() {
            return self.down(0, n, iters);
        }
        // The last position takes the root's place and the heap shrinks, as
        // Pebble's `pop` does. The root's own `winner` survives: its children
        // are positions 1 and 2, and the position that moved is neither of
        // those unless the heap is small enough that the right child is
        // already gone, in which case nothing reads it. The stale `winner` at
        // the *removed* position's parent is never read either, since that
        // parent now has at most one child and the heap only ever shrinks.
        self.swap(0, n - 1);
        self.items.truncate(n - 1);
        self.down(0, n - 1, iters)
    }

    /// Whether the key at position `a` comes first in the heap's order.
    ///
    /// Going forward that is internal key order itself — user keys ascending
    /// and, within a user key, trailers descending, so the newest version of
    /// a key arrives before its older ones. Going backward it is exactly that
    /// order reversed, which is what Pebble's `reverse` branch spells out as
    /// "user key greater, then trailer smaller".
    ///
    /// The source index breaks a tie beyond the key, in both directions,
    /// because that is what the two linear scans this replaces did: each only
    /// ever took a *strictly* more extreme key, and so left the **first**
    /// source holding the extreme one as the answer. A tie means two sources
    /// hold the same user key, sequence number and kind, which cannot happen
    /// while sequence numbers are handed out once — a batch's stand-ins
    /// included, since those number each entry in turn (`key::SEQ_NUM_BATCH`)
    /// — but a heap is not stable, and the index settles it anyway. That
    /// makes the order total, so the extreme key is unique and the heap
    /// cannot choose differently from the scans whatever shape it is in: by
    /// construction rather than by argument. Pebble leaves this to the
    /// sequence numbers.
    fn less<D: Disk>(&self, a: usize, b: usize, iters: &[Source<D>]) -> Result<bool> {
        let (a, b) = (self.items[a].source, self.items[b].source);
        let ordered = key::compare(iters[a].key(), iters[b].key())?;
        let ordered = if self.reverse {
            ordered.reverse()
        } else {
            ordered
        };
        Ok(match ordered {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => a < b,
        })
    }

    /// Exchanges the sources at two positions, leaving `winner` where it is.
    fn swap(&mut self, a: usize, b: usize) {
        let (x, y) = (self.items[a].source, self.items[b].source);
        self.items[a].source = y;
        self.items[b].source = x;
    }

    /// Sifts the source at `i` down through a heap of `n` positions until the
    /// heap property holds again.
    fn down<D: Disk>(&mut self, mut i: usize, n: usize, iters: &[Source<D>]) -> Result<()> {
        loop {
            let left = 2 * i + 1;
            if left >= n {
                break;
            }
            let mut child = left;
            let right = left + 1;
            if right < n {
                if self.items[i].winner == Winner::Unknown {
                    self.items[i].winner = if self.less(right, left, iters)? {
                        Winner::Right
                    } else {
                        Winner::Left
                    };
                }
                if self.items[i].winner == Winner::Right {
                    child = right;
                }
            }
            if !self.less(child, i, iters)? {
                break;
            }
            self.swap(i, child);
            self.items[i].winner = Winner::Unknown;
            i = child;
        }
        Ok(())
    }
}

impl<F: crate::env::Fs> crate::Opened<F> {
    /// A scan over every key in the database.
    pub async fn scan(&self) -> Result<Scan<F::Disk>> {
        self.scan_range(None, None).await
    }

    /// A scan bounded below by `lower` (included) and above by `upper`
    /// (excluded), as Pebble's bounds are.
    pub async fn scan_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<Scan<F::Disk>> {
        self.scan_with(Vec::new(), lower, upper).await
    }

    /// The same, with `extra` memtables merged in ahead of the store's.
    ///
    /// What an uncommitted batch reads through (`crate::batch`): its index is
    /// a memtable like any other, and its entries win because of the numbers
    /// they carry rather than because of where they are in this list. The
    /// store's are both of them, so a scan made in the middle of a flush
    /// reads the memtable being written out as well as the one in front.
    pub(crate) async fn scan_with(
        &self,
        extra: Vec<Rc<RefCell<Memtable>>>,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<Scan<F::Disk>> {
        self.metrics.scans.add(1);
        self.metrics.scans_open.add(1);
        // Only the tables this scan could reach. A table whose keys are all
        // outside the bounds has nothing to offer: its iterator would go
        // invalid on its first seek, and holding it open would pin a file
        // the obsolete sweep is then not allowed to collect (§5,
        // `crate::obsolete`). A scan over three keys would otherwise retain
        // every table in the database for as long as it was open.
        //
        // The file numbers first, so the borrow is released before any
        // waiting: opening a table is I/O, and a `RefCell` held across it
        // would collide with the flush that is allowed to run meanwhile.
        // Each with where the version puts it, which is what groups them
        // into runs ([`Scan::new`]) and is only knowable while it is
        // borrowed.
        let wanted: Vec<(u32, u64, Option<Vec<u8>>)> = {
            let versions = self.versions();
            let mut wanted = Vec::new();
            for (level, metas) in versions.version.levels() {
                // A bound is only there to choose between the tables of a
                // level. L0's are never read as one, and a level holding a
                // single table has nothing to choose between, so neither
                // copies a key no run would consult.
                let several = level > 0 && metas.len() > 1;
                // Below L0 the tables a bounded scan reaches are one stretch
                // of the level, because its tables are disjoint and in key
                // order: the ones before the stretch end below `lower`, the
                // ones after it start at or past `upper`. So the stretch is
                // found rather than every table being tested — a lookup that
                // bounds its scan to a table a level (`crate::read`) would
                // otherwise still pay a test for every table in the store.
                // L0's overlap, so each of those is tested.
                let (from, to) = if level == 0 {
                    (0, metas.len())
                } else {
                    let from = match lower {
                        Some(lower) => crate::version::ending_below(metas, lower)?,
                        None => 0,
                    };
                    let to = match upper {
                        Some(upper) => crate::version::starting_below(metas, upper)?,
                        None => metas.len(),
                    };
                    (from, to.max(from))
                };
                for meta in &metas[from..to] {
                    if meta.overlaps_range(lower, upper)? {
                        let smallest = several
                            .then(|| meta.smallest_user_key().map(<[u8]>::to_vec))
                            .transpose()?;
                        wanted.push((level, meta.file_num, smallest));
                    }
                }
            }
            wanted
        };
        let mut tables = Vec::with_capacity(wanted.len());
        for (level, file_num, smallest) in wanted {
            tables.push(crate::scan::Placed {
                level,
                table: self.open_table(file_num).await?,
                smallest,
            });
        }
        let mut memtables = extra;
        memtables.extend(self.memtables());
        Ok(Scan::new(
            tables,
            memtables,
            self.last_seq_num(),
            lower.map(|b| b.to_vec()),
            upper.map(|b| b.to_vec()),
            Some(Rc::clone(&self.metrics)),
        ))
    }
}
