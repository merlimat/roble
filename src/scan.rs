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

//! Scanning: one iterator per level, merged into one sequence (§4).
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

use std::borrow::Cow;
use std::future::Future;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::rc::Rc;

use roble_format::InternalKey;
use roble_format::key::{self, Kind};

use crate::env::{Disk, Fs};
use crate::error::Result;
use crate::read::successor_into;
use std::cell::RefCell;

use crate::memtable::{Memtable, MemtableCursor};
use crate::table::{OpenTable, TableIter, covered_by_tombstone, is_deletion};
use crate::version::{Table, Version};
use roble_format::TableFormat;
use roble_format::block::restart::DataCursor;

/// One source of entries in a merge: a level's tables, or a memtable.
///
/// A table and the memtable differ in more than where their bytes are. A
/// table's contents are fixed, so everything in it is older than any scan;
/// the memtable is still being written to, so a scan has to ignore what
/// arrived after it began. That is what `snapshot` is for, and it is the only
/// place it is needed.
///
/// A level is some 260 bytes and a memtable source 48, which is the size
/// difference clippy's `large_enum_variant` objects to. Boxing the level would
/// put a pointer between the merge and the source it steps on every key, to
/// save a few hundred bytes in a vector of one source per level.
#[allow(clippy::large_enum_variant)]
enum Source<D: Disk> {
    Level(Level<D>),
    Mem(MemSource),
}

/// Where a scan gets the tables it reads, as it reaches them.
///
/// A scan outlives the call that made it and opens a table only when a seek
/// or a step lands in one ([`Level`]), so it has to be able to open tables on
/// its own. It is generic over the disk and not over the filesystem that
/// opens one, which is why this is a trait object: the store's table cache
/// and filesystem behind one pointer, shared by every scan of the store.
pub(crate) trait Opener<D: Disk> {
    /// The table, if it is open already, without waiting for anything.
    fn held(&self, file_num: u64) -> Option<Rc<OpenTable<D>>>;

    /// Opens it.
    fn open(&self, file_num: u64) -> Opening<'_, D>;
}

/// An [`Opener`] opening a table.
///
/// Boxed, which is the price of a trait object with an `async` method, and
/// paid only when [`Opener::held`] has nothing: a table that has to be opened
/// costs reads of its footer, index and filter, against which one allocation
/// does not register.
pub(crate) type Opening<'a, D> = Pin<Box<dyn Future<Output = Result<Rc<OpenTable<D>>>> + 'a>>;

/// The table `file_num` names, open, from `tables`.
pub(crate) async fn open_table<D: Disk>(
    tables: &dyn Opener<D>,
    file_num: u64,
) -> Result<Rc<OpenTable<D>>> {
    match tables.held(file_num) {
        Some(table) => Ok(table),
        None => tables.open(file_num).await,
    }
}

/// The tables of one level a scan's bounds reach, read in order as one source
/// and opened one at a time as the scan reaches them.
///
/// Pebble's `levelIter` and RocksDB's `LevelIterator`. Every level below L0
/// holds tables whose key ranges do not overlap, so at most one of them can
/// hold any given key and they can be read one after another as if they were
/// a single table. A seek lands in the one table that can hold its target —
/// found by its bounds in the version, which are exact — and a step off the
/// end of a table moves to the next. So making a scan opens nothing, and
/// positioning it opens at most one table a level.
///
/// That is the whole of what this is for. A scan used to open every table its
/// bounds reached and build an iterator for each, because holding a table
/// open was how it kept the file from the obsolete sweep. A `floor` bounded
/// only above reaches every table that starts below its probe — half the
/// store — and a profile on one pinned core put 52.6% of a `floor` in
/// building that scan, against 32.7% in the seek that answered it. What keeps
/// the files now is the version this holds (`crate::obsolete::Pinned`), which
/// names the tables this has not opened yet as well as the one it has.
///
/// **A table the level lands in is opened only when the merge needs it.**
/// When the table lies wholly past where the level is going — a seek whose
/// target falls before the table starts, going forward, or after it ends,
/// going backward, and every step from one table into the next — the
/// level's next entry is that table's first or last, and the version already
/// knows a key no entry of the table passes: its smallest bound, or its
/// largest. So the level waits there, `deferred`, and stands in the merge
/// with that bound for a key ([`Level::key`]). If the bound never reaches the
/// top of the heap, the table is never opened; when it does, the scan opens
/// it ([`Level::realize`]) and the heap is put right. This is what a `floor`
/// needs from a level holding a few tables scattered over the keyspace — an
/// L1 of nineteen at six million records — whose landing table usually ends
/// far below the probe: seeking it would read and decode its last block for a
/// key another level beats. A window `floor` used to try first existed to
/// skip exactly that seek, and this skips it for every scan, either way round.
///
/// **Tombstones come from the table the level last opened**, as Pebble's and
/// RocksDB's come from each level's current file ([`Scan::visible`]). A range
/// deletion lies within its own table's bounds (§5), so of a level's tables
/// only the one whose bounds take in the key being decided can cover it.
/// Pebble keeps that table current by interleaving its tombstones' bounds
/// among the point keys, so its level iterator cannot leave the table while
/// the merge still needs them. The deferral above does the same here: the
/// next table is not opened until its bound reaches the top of the heap, and
/// any key a tombstone of that table can cover sorts after its bound, so by
/// the time such a key is decided its table is the one the level has open.
///
/// L0's tables overlap, so each of those is a level of its own here, one
/// table long. That is not merely the degenerate case: it is what lets L0's
/// tables be opened as lazily as any other, by the seek that lands in one.
///
/// Correctness rests on the disjointness and nothing else. Two sources at the
/// same key are settled by sequence number (`take_newest_visible`), never by
/// which source they came from, so reading tables that *cannot* be at the
/// same key as one source cannot change an answer; reading L0's as one,
/// which can, would.
struct Level<D: Disk> {
    /// The version the scan was made on, which says what this level holds.
    ///
    /// Holding it is what keeps the sweep off every table it names, the ones
    /// this level has not opened yet included (§5, `crate::obsolete`).
    version: Rc<Version>,
    level: u32,
    /// The tables the scan's bounds reach, as a range of the level's.
    from: usize,
    to: usize,
    tables: Rc<dyn Opener<D>>,
    /// Which table of the stretch the level is on or waiting at, counted
    /// from `from`.
    at: usize,
    /// The iterator of the table it last opened. `None` until a seek has
    /// opened one, and after a seek that found no table to land in.
    ///
    /// While the level is `deferred` this is still the table it left, which
    /// is what keeps that table's tombstones in reach of the keys the merge
    /// decides before it gets to the next one.
    iter: Option<TableIter<D>>,
    /// Waiting at table `at` without having opened it, and which way the
    /// scan is going: forward the level's next entry is the table's first,
    /// backward its last.
    deferred: Option<Dir>,
    /// A block cursor to build the next table's iterator on, kept from the
    /// scan this one was rebuilt out of ([`Scan::rebuild`]). A cursor
    /// assembles the key it is on into a buffer, and a fresh one per table
    /// opened is an allocation per table.
    cursor: Option<(TableFormat, DataCursor)>,
}

impl<D: Disk> Level<D> {
    fn new(
        version: &Rc<Version>,
        tables: &Rc<dyn Opener<D>>,
        level: u32,
        from: usize,
        to: usize,
        cursor: Option<(TableFormat, DataCursor)>,
    ) -> Self {
        Level {
            version: Rc::clone(version),
            level,
            from,
            to,
            tables: Rc::clone(tables),
            at: 0,
            iter: None,
            deferred: None,
            cursor,
        }
    }

    /// The block cursors it holds, for the next scan to build on.
    fn into_cursors(self) -> impl Iterator<Item = (TableFormat, DataCursor)> {
        self.iter
            .map(TableIter::into_cursor)
            .into_iter()
            .chain(self.cursor)
    }

    /// The tables this level may read, in key order.
    fn metas(&self) -> &[Table] {
        &self.version.level(self.level)[self.from..self.to]
    }

    /// The iterator of the table it is on.
    ///
    /// # Panics
    /// If it is on none, which the callers rule out: a step is only taken by
    /// a source that is on an entry, and a seek loads a table first.
    fn iter(&mut self) -> &mut TableIter<D> {
        self.iter.as_mut().expect("a level is on a table")
    }

    /// Whether the table it last opened is on an entry.
    fn on_entry(&self) -> bool {
        self.iter.as_ref().is_some_and(TableIter::is_valid)
    }

    /// Whether it has an entry to offer, opened or not.
    fn is_valid(&self) -> bool {
        self.deferred.is_some() || self.on_entry()
    }

    /// Whether it is waiting at a table it has not opened.
    fn is_deferred(&self) -> bool {
        self.deferred.is_some()
    }

    /// The table it last opened, whose tombstones are the level's.
    fn table(&self) -> Option<&Rc<OpenTable<D>>> {
        self.iter.as_ref().map(TableIter::table)
    }

    /// The key it is on, or while it is deferred the bound of the table it
    /// is waiting at, which no entry of that table passes in the direction
    /// the scan is going.
    fn key(&self) -> &[u8] {
        match self.deferred {
            Some(Dir::Forward) => &self.metas()[self.at].smallest,
            Some(Dir::Reverse) => &self.metas()[self.at].largest,
            None => self.iter.as_ref().expect("a level is on a table").key(),
        }
    }

    async fn value_into(&self, out: &mut Vec<u8>) -> Result<()> {
        debug_assert!(self.deferred.is_none(), "a deferred level has no value");
        let iter = self.iter.as_ref().expect("a level is on a table");
        iter.value_into(out).await
    }

    /// Opens the `i`th table of its stretch and puts the level on it, unless
    /// it is the one the level last opened.
    async fn load(&mut self, i: usize) -> Result<()> {
        let file_num = self.metas()[i].file_num;
        self.at = i;
        if self.table().is_some_and(|t| t.file_num == file_num) {
            return Ok(());
        }
        let table = open_table(&*self.tables, file_num).await?;
        let cursor = self
            .iter
            .take()
            .map(TableIter::into_cursor)
            .or(self.cursor.take());
        self.iter = Some(table.iter_reusing(cursor));
        Ok(())
    }

    /// Opens the table a deferred level is waiting at and puts it on that
    /// table's first entry, or its last going backward — carrying on past
    /// the table if it holds none, which may leave the level deferred again,
    /// at the next.
    async fn realize(&mut self) -> Result<()> {
        let Some(dir) = self.deferred.take() else {
            return Ok(());
        };
        self.load(self.at).await?;
        match dir {
            Dir::Forward => {
                self.iter().first().await?;
                self.onward();
            }
            Dir::Reverse => {
                self.iter().last().await?;
                self.back();
            }
        }
        Ok(())
    }

    /// Positions on the first entry at or after `target`, an internal key.
    ///
    /// In the first table whose largest bound reaches the target — Pebble's
    /// `findFileGE` — since every table before it ends below the target. A
    /// table that starts past the target is waited at rather than opened. No
    /// table reaching the target means nothing at or after it in the
    /// stretch.
    async fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        self.deferred = None;
        let user = InternalKey::decode(target)?.user_key;
        let i = crate::version::ending_below(self.metas(), user)?;
        if i == self.metas().len() {
            self.iter = None;
            return Ok(());
        }
        // `>` and not `>=` is not pinned by a test, and need not be (§12): a
        // table starting at the target has its first entry for an answer
        // either way, found by the seek below or by the wait.
        if self.metas()[i].smallest_user_key()? > user {
            self.at = i;
            self.deferred = Some(Dir::Forward);
            return Ok(());
        }
        self.load(i).await?;
        self.iter().seek_ge(target).await?;
        self.onward();
        Ok(())
    }

    /// Positions on the last entry before `target`, an internal key.
    ///
    /// In the last table starting below the target — Pebble's `findFileLT` —
    /// since every table after it starts at or above the target. A table
    /// that ends before the target is waited at rather than opened, and none
    /// starting below it means nothing to find.
    async fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        self.deferred = None;
        let user = InternalKey::decode(target)?.user_key;
        let i = crate::version::starting_below(self.metas(), user)?;
        if i == 0 {
            self.iter = None;
            return Ok(());
        }
        if self.metas()[i - 1].largest_user_key()? < user {
            self.at = i - 1;
            self.deferred = Some(Dir::Reverse);
            return Ok(());
        }
        self.load(i - 1).await?;
        self.iter().seek_lt(target).await?;
        self.back();
        Ok(())
    }

    /// Positions on the level's last entry.
    async fn last(&mut self) -> Result<()> {
        self.deferred = None;
        self.load(self.metas().len() - 1).await?;
        self.iter().last().await?;
        self.back();
        Ok(())
    }

    /// Steps forward, onto the next table at the end of this one.
    async fn next(&mut self) -> Result<()> {
        debug_assert!(self.deferred.is_none(), "a deferred level is not stepped");
        self.iter().next().await?;
        self.onward();
        Ok(())
    }

    /// Steps backward, onto the previous table at the start of this one.
    async fn prev(&mut self) -> Result<()> {
        debug_assert!(self.deferred.is_none(), "a deferred level is not stepped");
        self.iter().prev().await?;
        self.back();
        Ok(())
    }

    /// Moves on to the next table when the one the level is on has nothing
    /// more to offer, and waits there.
    ///
    /// Reached in two ways, and neither is the edge case it looks. A table
    /// can simply be spent, which is how a scan crosses from one table of a
    /// level into the next. Or a seek can land in a table that turns out to
    /// hold no point key that high — the table is one whose bounds reach
    /// further than its point keys, because a range deletion does, or one of
    /// the range-deletion-only ones Pebble writes.
    fn onward(&mut self) {
        if !self.on_entry() && self.at + 1 < self.metas().len() {
            self.at += 1;
            self.deferred = Some(Dir::Forward);
        }
    }

    /// The same backwards, to the previous table.
    fn back(&mut self) {
        if !self.on_entry() && self.at > 0 {
            self.at -= 1;
            self.deferred = Some(Dir::Reverse);
        }
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
            Source::Level(level) => level.is_valid(),
            Source::Mem(m) => m.valid,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Source::Level(level) => level.key(),
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
            Source::Level(level) => level.value_into(out).await,
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
            Source::Level(level) => level.seek_ge(target).await,
            Source::Mem(m) => {
                m.cursor.seek_ge(&m.table.borrow(), target)?;
                m.capture();
                Ok(())
            }
        }
    }

    async fn seek_lt(&mut self, target: &[u8]) -> Result<()> {
        match self {
            Source::Level(level) => level.seek_lt(target).await,
            Source::Mem(m) => {
                m.cursor.seek_lt(&m.table.borrow(), target)?;
                m.capture();
                Ok(())
            }
        }
    }

    async fn last(&mut self) -> Result<()> {
        match self {
            Source::Level(level) => level.last().await,
            Source::Mem(m) => {
                m.cursor.last(&m.table.borrow());
                m.capture();
                Ok(())
            }
        }
    }

    async fn next(&mut self) -> Result<()> {
        match self {
            Source::Level(level) => level.next().await,
            Source::Mem(m) => {
                m.cursor.next(&m.table.borrow());
                m.capture();
                Ok(())
            }
        }
    }

    async fn prev(&mut self) -> Result<()> {
        match self {
            Source::Level(level) => level.prev().await,
            Source::Mem(m) => {
                m.cursor.prev(&m.table.borrow());
                m.capture();
                Ok(())
            }
        }
    }
}

impl<F: Fs> Drop for Scan<F> {
    /// A scan that has gone away is one fewer open, and a store that keeps
    /// reporting scans nobody holds is how an iterator leak hides.
    fn drop(&mut self) {
        self.park();
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

/// A bound of a scan, in a buffer the scan keeps across rebuilds.
#[derive(Default)]
struct Bound {
    key: Vec<u8>,
    set: bool,
}

impl Bound {
    fn get(&self) -> Option<&[u8]> {
        self.set.then_some(self.key.as_slice())
    }

    fn set(&mut self, to: Option<&[u8]>) {
        self.key.clear();
        self.set = to.is_some();
        if let Some(to) = to {
            self.key.extend_from_slice(to);
        }
    }
}

/// A scan over the tables of a version.
///
/// Forward and reverse, optionally bounded. The bounds are half-open as
/// Pebble's are: `lower` is included and `upper` is not.
pub struct Scan<F: Fs> {
    iters: Vec<Source<F::Disk>>,
    /// The sources arranged so that the next key is the top of a heap rather
    /// than the winner of a look at every one of them.
    ///
    /// Empty until the scan is positioned, and rebuilt by every call that
    /// positions it. Nothing reads it before then: `next` and `prev` return
    /// at once while `current` is `None`, and every other entry point seeks
    /// first.
    heap: Heap,
    /// The buffers of the keys this scan has moved off, kept to be filled
    /// again.
    ///
    /// A step has to copy the user key out of a source before it can touch
    /// that source mutably, and the copy is what `key()` then hands out — so
    /// it cannot borrow and it cannot be skipped. What it can do is reuse the
    /// `Vec` the previous key was in, which is free the moment this scan
    /// steps off it (`advance`).
    spare: Vec<Current>,
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
    lower: Bound,
    upper: Bound,
    /// The internal key a seek targets, and the key it is derived from when
    /// that is not the caller's: two buffers kept rather than two allocated
    /// a seek.
    target: Vec<u8>,
    scratch: Vec<u8>,
    dir: Dir,
    current: Option<Current>,
    /// What a [`Scan::rebuild`] builds the next sources out of: the block
    /// cursors of the levels it took apart, and the key buffers of its
    /// memtable sources.
    cursors: Vec<(TableFormat, DataCursor)>,
    mem_keys: Vec<Vec<u8>>,
    /// Whether this scan is one the store counts as open: from a rebuild to
    /// the [`Scan::park`] or the drop after it.
    open: bool,
    /// The store's handles, so a scan can count what it steps onto. `None`
    /// for a scan made without a store, which only the crate's own tests do.
    metrics: Option<Rc<crate::metrics::Metrics>>,
}

impl<F: Fs> Scan<F> {
    /// A scan over nothing, for [`Scan::rebuild`] to make one of.
    pub(crate) fn empty(metrics: Option<Rc<crate::metrics::Metrics>>) -> Self {
        Scan {
            iters: Vec::new(),
            heap: Heap::default(),
            spare: Vec::new(),
            memtables: Vec::new(),
            snapshot: 0,
            lower: Bound::default(),
            upper: Bound::default(),
            target: Vec::new(),
            scratch: Vec::new(),
            dir: Dir::Forward,
            current: None,
            cursors: Vec::new(),
            mem_keys: Vec::new(),
            open: false,
            metrics,
        }
    }

    /// Makes this a scan over `version`'s tables and `memtables`, bounded by
    /// `lower` (included) and `upper` (excluded), positioned nowhere — in the
    /// allocations of whatever scan it was before.
    ///
    /// Crate-private: its arguments are a store's internals, and the way to
    /// one from outside is [`crate::Db::range`].
    ///
    /// Opens nothing. Each level below L0 becomes one [`Level`] over the
    /// stretch of its tables the bounds reach, found by two binary searches
    /// of its bounds, and each table of L0 the bounds reach becomes a level
    /// of its own; the tables are opened by the seeks that land in them. So
    /// this costs the same over fourteen tables as over two hundred and
    /// forty, where it used to open and build an iterator for every table
    /// its bounds reached.
    ///
    /// And it allocates nothing once the scan has been built before over as
    /// many sources: the sources, the heap, the bounds and the key buffers
    /// are all the old scan's, and each level builds its iterators on a block
    /// cursor the old scan's levels left. That is what lets an ordered
    /// comparison through a kept [`crate::GetBuf`] allocate nothing.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rebuild(
        &mut self,
        version: &Rc<Version>,
        tables: &Rc<dyn Opener<F::Disk>>,
        memtables: impl Iterator<Item = Rc<RefCell<Memtable>>>,
        snapshot: u64,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<()> {
        self.park();
        for (level, metas) in version.levels() {
            if level == 0 {
                // Tested one by one, because L0's overlap and so have no
                // order to search. A table outside the bounds would only go
                // invalid on its first seek, and is left out so that no seek
                // opens it to find that out.
                for (i, meta) in metas.iter().enumerate() {
                    if meta.overlaps_range(lower, upper)? {
                        let cursor = self.cursors.pop();
                        let level = Level::new(version, tables, 0, i, i + 1, cursor);
                        self.iters.push(Source::Level(level));
                    }
                }
                continue;
            }
            // Below L0 the tables a bounded scan reaches are one stretch of
            // the level, because its tables are disjoint and in key order:
            // the ones before the stretch end below `lower`, the ones after it
            // start at or past `upper`. Leaving them out is what stops a step
            // off the end of the stretch opening a table only to find it
            // past the bound.
            let from = match lower {
                Some(lower) => crate::version::ending_below(metas, lower)?,
                None => 0,
            };
            let to = match upper {
                Some(upper) => crate::version::starting_below(metas, upper)?,
                None => metas.len(),
            };
            if from < to {
                let cursor = self.cursors.pop();
                let level = Level::new(version, tables, level, from, to, cursor);
                self.iters.push(Source::Level(level));
            }
        }
        for m in memtables {
            let cursor = m.borrow().cursor();
            self.iters.push(Source::Mem(MemSource {
                table: Rc::clone(&m),
                cursor,
                key: self.mem_keys.pop().unwrap_or_default(),
                valid: false,
            }));
            self.memtables.push(m);
        }
        self.snapshot = snapshot;
        self.lower.set(lower);
        self.upper.set(upper);
        self.dir = Dir::Forward;
        if let Some(metrics) = &self.metrics {
            metrics.scans.add(1);
            metrics.scans_open.add(1);
        }
        self.open = true;
        Ok(())
    }

    /// Lets go of everything this scan holds of the store — the version, the
    /// tables, the memtables, the blocks — and keeps only its own buffers,
    /// for the next [`Scan::rebuild`].
    ///
    /// A scan kept between reads has to hold nothing between them: a kept
    /// version would keep the sweep off every table it names for as long as
    /// the caller kept the buffer, and a kept memtable would keep its whole
    /// arena.
    pub(crate) fn park(&mut self) {
        let mut iters = std::mem::take(&mut self.iters);
        for source in iters.drain(..) {
            match source {
                Source::Level(level) => self.cursors.extend(level.into_cursors()),
                Source::Mem(m) => self.mem_keys.push(m.key),
            }
        }
        self.iters = iters;
        self.memtables.clear();
        self.heap.items.clear();
        if let Some(current) = self.current.take() {
            self.spare.push(current);
        }
        if std::mem::take(&mut self.open)
            && let Some(metrics) = &self.metrics
        {
            metrics.scans_open.add(-1);
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

    /// Seeks every source to the first entry at or after the internal key in
    /// `self.target` and builds the forward heap over them.
    ///
    /// The two belong together because a heap is good for one direction only:
    /// its top is the least internal key or the greatest, never both. So
    /// every call that repositions the sources wholesale has to say which way
    /// it left them, and this is where that is said. Pebble pairs them for
    /// the same reason, in `switchToMinHeap`.
    async fn sought_ge(&mut self) -> Result<()> {
        for it in &mut self.iters {
            it.seek_ge(&self.target).await?;
        }
        self.dir = Dir::Forward;
        self.heap.rebuild(&self.iters, Dir::Forward)
    }

    /// The same backwards: every source before the target, greatest on top.
    async fn sought_lt(&mut self) -> Result<()> {
        for it in &mut self.iters {
            it.seek_lt(&self.target).await?;
        }
        self.dir = Dir::Reverse;
        self.heap.rebuild(&self.iters, Dir::Reverse)
    }

    /// Positions on the first key at or after `from`, respecting the bounds.
    pub async fn seek_ge(&mut self, from: &[u8]) -> Result<()> {
        let from = match self.lower.get() {
            Some(lower) if lower > from => lower,
            _ => from,
        };
        key::search_key_into(from, &mut self.target);
        self.sought_ge().await?;
        self.advance().await
    }

    /// Positions on the last key before `bound`, respecting the bounds.
    pub async fn seek_lt(&mut self, bound: &[u8]) -> Result<()> {
        let bound = match self.upper.get() {
            Some(upper) if upper < bound => upper,
            _ => bound,
        };
        key::search_key_into(bound, &mut self.target);
        self.sought_lt().await?;
        self.retreat().await
    }

    /// Positions on the first key in range.
    pub async fn first(&mut self) -> Result<()> {
        // The lower bound itself, which the seek puts in place of anything
        // below it.
        self.seek_ge(&[]).await
    }

    /// Positions on the last key in range.
    pub async fn last(&mut self) -> Result<()> {
        if let Some(upper) = self.upper.get() {
            key::search_key_into(upper, &mut self.target);
            self.sought_lt().await?;
            return self.retreat().await;
        }
        for it in &mut self.iters {
            it.last().await?;
        }
        self.dir = Dir::Reverse;
        self.heap.rebuild(&self.iters, Dir::Reverse)?;
        self.retreat().await
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
            successor_into(&current.user, &mut self.scratch);
            key::search_key_into(&self.scratch, &mut self.target);
            self.sought_ge().await?;
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
            key::search_key_into(&current.user, &mut self.target);
            self.sought_lt().await?;
        }
        self.retreat().await
    }

    /// The source on top of the heap, opening the table of any level that
    /// reaches the top while waiting at one ([`Level::realize`]).
    ///
    /// The top is the source whose key the scan takes next, so a level there
    /// has to say what it really holds rather than the bound it stood in
    /// with. Opening it moves its key on the way the heap runs — the bound is
    /// one no entry of the table passes — so putting the heap right is the
    /// same sift a step is.
    ///
    /// Untested, with a reason (§12): taking the top unopened changes no
    /// answer, because `take_newest_visible` opens a waiting level whose
    /// bound is at the key being taken, and a key no source turns out to
    /// hold is one the loop goes round again for. What this saves is that
    /// turn, and a reader having to know about it.
    async fn top(&mut self) -> Result<Option<usize>> {
        while let Some(i) = self.heap.top() {
            match &mut self.iters[i] {
                Source::Level(level) if level.is_deferred() => {
                    level.realize().await?;
                    self.heap.stepped(&self.iters)?;
                }
                _ => return Ok(Some(i)),
            }
        }
        Ok(None)
    }

    /// Moves to the next visible key, skipping deletions.
    async fn advance(&mut self) -> Result<()> {
        loop {
            let Some(w) = self.top().await? else {
                self.current = None;
                return Ok(());
            };
            // Into the buffer the last key vacated, which is owned locally
            // for the rest of this turn of the loop and so conflicts with no
            // borrow of `self`.
            let mut next = self.spare.pop().unwrap_or_default();
            next.user.clear();
            next.user
                .extend_from_slice(InternalKey::decode(self.iters[w].key())?.user_key);
            let chosen = self
                .take_newest_visible(&next.user, Dir::Forward, &mut next.value)
                .await?;

            if let Some(upper) = self.upper.get()
                && next.user.as_slice() >= upper
            {
                self.spare.push(next);
                self.current = None;
                return Ok(());
            }
            if let Some((seq, kind)) = chosen
                && self.visible(&next.user, seq, kind)?
            {
                if let Some(old) = self.current.replace(next) {
                    self.spare.push(old);
                }
                self.stepped_onto();
                return Ok(());
            }
            // A deletion, so round again — and the buffers go back rather
            // than being dropped.
            self.spare.push(next);
        }
    }

    /// Moves to the previous visible key, skipping deletions.
    async fn retreat(&mut self) -> Result<()> {
        loop {
            let Some(w) = self.top().await? else {
                self.current = None;
                return Ok(());
            };
            // Into the buffer the last key vacated, which is owned locally
            // for the rest of this turn of the loop and so conflicts with no
            // borrow of `self`.
            let mut next = self.spare.pop().unwrap_or_default();
            next.user.clear();
            next.user
                .extend_from_slice(InternalKey::decode(self.iters[w].key())?.user_key);
            let chosen = self
                .take_newest_visible(&next.user, Dir::Reverse, &mut next.value)
                .await?;

            if let Some(lower) = self.lower.get()
                && next.user.as_slice() < lower
            {
                self.spare.push(next);
                self.current = None;
                return Ok(());
            }
            if let Some((seq, kind)) = chosen
                && self.visible(&next.user, seq, kind)?
            {
                if let Some(old) = self.current.replace(next) {
                    self.spare.push(old);
                }
                self.stepped_onto();
                return Ok(());
            }
            // A deletion, so round again — and the buffers go back rather
            // than being dropped.
            self.spare.push(next);
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
            // A level waiting at a table it has not opened stands in with
            // that table's bound, which can be at `user` without the table
            // holding it. Opened, it says what it does hold, and the heap is
            // looked at again. One whose bound is past `user` is left
            // waiting: the test above has already told it apart, which is how
            // a level never needed is never opened.
            if let Source::Level(level) = &mut self.iters[i]
                && level.is_deferred()
            {
                level.realize().await?;
                self.heap.stepped(&self.iters)?;
                continue;
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
        // Each level's, from the table it last opened, which is the one
        // table of the level that can hold a tombstone over `user` (see
        // [`Level`]).
        for source in &self.iters {
            if let Source::Level(level) = source
                && let Some(table) = level.table()
                && covered_by_tombstone(std::slice::from_ref(table), user, seq)?
            {
                return Ok(false);
            }
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
/// Every source in the heap is on an entry, or is a level waiting at a table
/// with that table's bound for a key ([`Level`]): one that runs out leaves, so
/// [`Heap::top`] needs no validity check and no comparison is ever made
/// against a source positioned nowhere. A waiting level's bound is one no
/// entry of its table passes in the heap's order, so the level reaches the
/// top no later than its next entry would, and is opened there
/// ([`Scan::top`]).
///
/// Why a second copy of this rather than the one `crate::compact` already
/// has. The two merge different things — a compaction merges
/// `crate::table::TableIter`s while a scan merges [`Source`]s, which is
/// already an enum over a level and a memtable — so sharing
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
    /// Heapifies the sources that are on an entry, ordered for `dir`, in
    /// the heap's own allocation.
    fn rebuild<D: Disk>(&mut self, iters: &[Source<D>], dir: Dir) -> Result<()> {
        self.items.clear();
        self.items.extend(
            iters
                .iter()
                .enumerate()
                .filter(|(_, it)| it.is_valid())
                .map(|(source, _)| Item {
                    source,
                    winner: Winner::Unknown,
                }),
        );
        self.reverse = dir == Dir::Reverse;
        for i in (0..self.items.len() / 2).rev() {
            self.down(i, self.items.len(), iters)?;
        }
        Ok(())
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

/// One end of a scan's range, borrowed from the caller's key unless it had to
/// be moved to that key's successor.
pub(crate) type Edge<'k> = Option<Cow<'k, [u8]>>;

/// `range` as the bounds a scan is built with: Pebble's, an included lower
/// and an excluded upper.
///
/// An excluded lower or an included upper becomes the key's successor, the
/// least key after it, which is the only case that allocates.
pub(crate) fn bounds<'k>(range: &impl RangeBounds<&'k [u8]>) -> (Edge<'k>, Edge<'k>) {
    let lower = match range.start_bound() {
        std::ops::Bound::Included(key) => Some(Cow::Borrowed(*key)),
        std::ops::Bound::Excluded(key) => Some(Cow::Owned(crate::read::successor(key))),
        std::ops::Bound::Unbounded => None,
    };
    let upper = match range.end_bound() {
        std::ops::Bound::Included(key) => Some(Cow::Owned(crate::read::successor(key))),
        std::ops::Bound::Excluded(key) => Some(Cow::Borrowed(*key)),
        std::ops::Bound::Unbounded => None,
    };
    (lower, upper)
}

impl<F: crate::env::Fs> crate::Db<F> {
    /// A scan over the keys in `range`, in either direction.
    ///
    /// `db.range(..)` covers every key, and `db.range(&b"/a/"[..]..&b"/b/"[..])`
    /// the keys from `/a/` up to but not including `/b/`. Making one does no
    /// I/O: it opens nothing until a seek lands in a table. Position it with
    /// [`Scan::first`] or [`Scan::seek_ge`] and step with [`Scan::next`] to
    /// read forwards, or with [`Scan::last`] or [`Scan::seek_lt`] and
    /// [`Scan::prev`] to read backwards.
    pub fn range<'k>(&self, range: impl RangeBounds<&'k [u8]>) -> Result<Scan<F>> {
        let (lower, upper) = bounds(&range);
        self.scan_with(Vec::new(), lower.as_deref(), upper.as_deref())
    }

    /// The same, with `extra` memtables merged in ahead of the store's.
    ///
    /// What an uncommitted batch reads through (`crate::batch`): its index is
    /// a memtable like any other, and its entries win because of the numbers
    /// they carry rather than because of where they are in this list. The
    /// store's are both of them, so a scan made in the middle of a flush
    /// reads the memtable being written out as well as the one in front.
    pub(crate) fn scan_with(
        &self,
        extra: Vec<Rc<RefCell<Memtable>>>,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<Scan<F>> {
        let mut scan = Scan::empty(Some(Rc::clone(&self.metrics)));
        self.rebuild_scan(&mut scan, extra.into_iter(), lower, upper)?;
        Ok(scan)
    }

    /// Makes `scan` a scan of the store as it stands, with `extra` memtables
    /// merged in ahead of the store's, in the allocations it already has.
    ///
    /// The version as it stands is held for as long as the scan is: the
    /// tables it names are the ones the scan will open as it reaches them,
    /// and holding it is what keeps the sweep off every one of them (§5,
    /// `crate::obsolete::Pinned`).
    pub(crate) fn rebuild_scan(
        &self,
        scan: &mut Scan<F>,
        extra: impl Iterator<Item = Rc<RefCell<Memtable>>>,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<()> {
        let version = Rc::clone(&self.versions().version);
        let tables: Rc<dyn Opener<F::Disk>> = self.tables();
        scan.rebuild(
            &version,
            &tables,
            extra.chain(self.memtables_each()),
            self.last_seq_num(),
            lower,
            upper,
        )
    }
}
