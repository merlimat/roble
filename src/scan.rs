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
//! stepped. That costs one seek per table at the turn and nothing while the
//! direction holds, which is the trade Pebble's merging iterator makes too.
//!
//! What a scan fixes, which `get` still pays: the four ordered comparisons in
//! [`crate::read`] re-seek every table once per key they reject. Here the
//! iterators keep their place, so a run of deleted keys costs a step each
//! rather than a seek each (§6).

use std::rc::Rc;

use roble_format::InternalKey;
use roble_format::key::{self, Kind};

use crate::env::Disk;
use crate::error::Result;
use crate::read::successor;
use std::cell::RefCell;

use crate::memtable::{Memtable, MemtableCursor};
use crate::table::{OpenTable, TableIter, covered_by_tombstone, is_deletion};

/// One source of entries in a merge: a table, or the memtable.
///
/// The two differ in more than where their bytes are. A table's contents are
/// fixed, so everything in it is older than any scan; the memtable is still
/// being written to, so a scan has to ignore what arrived after it began.
/// That is what `snapshot` is for, and it is the only place it is needed.
enum Source<D: Disk> {
    Table(TableIter<D>),
    Mem(MemSource),
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
            Source::Mem(m) => m.valid,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Source::Table(it) => it.key(),
            Source::Mem(m) => &m.key,
        }
    }

    async fn entry(&self) -> Result<crate::table::Entry> {
        match self {
            Source::Table(it) => it.entry().await,
            Source::Mem(m) => {
                let table = m.table.borrow();
                Ok(crate::table::Entry {
                    key: m.key.clone(),
                    value: m.cursor.value(&table).to_vec(),
                })
            }
        }
    }

    async fn seek_ge(&mut self, target: &[u8]) -> Result<()> {
        match self {
            Source::Table(it) => it.seek_ge(target).await,
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
    /// The buffers of the key this scan has moved off, kept to be filled
    /// again.
    ///
    /// A step has to copy the user key out of a source before it can touch
    /// that source mutably, and the copy is what `key()` then hands out — so
    /// it cannot borrow and it cannot be skipped. What it can do is reuse the
    /// `Vec` the previous key was in, which is free the moment this scan
    /// steps off it (`advance`).
    spare: Option<Current>,
    tables: Vec<Rc<OpenTable<D>>>,
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
    /// A scan over `tables`, bounded by `lower` (included) and `upper`
    /// (excluded), positioned nowhere.
    ///
    /// Crate-private: its arguments are a store's internals, and the way to
    /// one from outside is [`crate::Opened::scan`].
    pub(crate) fn new(
        tables: Vec<Rc<OpenTable<D>>>,
        memtables: Vec<Rc<RefCell<Memtable>>>,
        snapshot: u64,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        metrics: Option<Rc<crate::metrics::Metrics>>,
    ) -> Self {
        let mut iters: Vec<Source<D>> = tables.iter().map(|t| Source::Table(t.iter())).collect();
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
            spare: None,
            tables,
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

    /// Positions on the first key at or after `from`, respecting the bounds.
    pub async fn seek_ge(&mut self, from: &[u8]) -> Result<()> {
        let from = match &self.lower {
            Some(lower) if lower.as_slice() > from => lower.clone(),
            _ => from.to_vec(),
        };
        let target = key::search_key(&from);
        for it in &mut self.iters {
            it.seek_ge(&target).await?;
        }
        self.dir = Dir::Forward;
        self.advance().await
    }

    /// Positions on the last key before `bound`, respecting the bounds.
    pub async fn seek_lt(&mut self, bound: &[u8]) -> Result<()> {
        let bound = match &self.upper {
            Some(upper) if upper.as_slice() < bound => upper.clone(),
            _ => bound.to_vec(),
        };
        let target = key::search_key(&bound);
        for it in &mut self.iters {
            it.seek_lt(&target).await?;
        }
        self.dir = Dir::Reverse;
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
            for it in &mut self.iters {
                it.seek_ge(&target).await?;
            }
            self.dir = Dir::Forward;
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
            for it in &mut self.iters {
                it.seek_lt(&target).await?;
            }
            self.dir = Dir::Reverse;
        }
        self.retreat().await
    }

    /// Moves to the next visible key, skipping deletions.
    async fn advance(&mut self) -> Result<()> {
        loop {
            let Some(w) = self.least()? else {
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
            let chosen = self.take_newest_visible(&next.user, Dir::Forward).await?;

            if let Some(upper) = &self.upper
                && next.user >= *upper
            {
                self.current = None;
                return Ok(());
            }
            if let Some((seq, kind, value)) = chosen
                && self.visible(&next.user, seq, kind)?
            {
                next.value = value;
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
            let Some(w) = self.greatest()? else {
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
            let chosen = self.take_newest_visible(&next.user, Dir::Reverse).await?;

            if let Some(lower) = &self.lower
                && next.user < *lower
            {
                self.current = None;
                return Ok(());
            }
            if let Some((seq, kind, value)) = chosen
                && self.visible(&next.user, seq, kind)?
            {
                next.value = value;
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
    /// The same rule both ways: among the versions a source holds, take the
    /// highest sequence number at or below the snapshot. Going forward the
    /// first version met is the newest, so this usually stops at once; going
    /// backward the first met is the oldest, so it reads them all. That
    /// asymmetry is why reverse is dearer, and it is the same one the block
    /// cursor has.
    ///
    /// Every source ends past the key — forward, past its last version;
    /// backward, before its first — so the caller can simply go round again.
    async fn take_newest_visible(
        &mut self,
        user: &[u8],
        dir: Dir,
    ) -> Result<Option<(u64, Kind, Vec<u8>)>> {
        let mut chosen: Option<(u64, Kind, Vec<u8>)> = None;
        for i in 0..self.iters.len() {
            loop {
                if !self.iters[i].is_valid() {
                    break;
                }
                // The trailer, read out of the borrowed key rather than out of
                // a copy of it. Copying was how this ended the borrow before
                // the `entry().await` below, which needs the source mutably —
                // and it cost one allocation per source per step, so a scan
                // over a store settled into fourteen tables made fourteen of
                // them for every record it returned. Scoping the borrow to
                // the three scalars this actually needs costs none.
                let (seq_num, kind, matches) = {
                    let k = InternalKey::decode(self.iters[i].key())?;
                    (k.seq_num, k.kind, k.user_key == user)
                };
                if !matches {
                    break;
                }
                // A version written after this scan began is not its business.
                if visible_at(seq_num, self.snapshot)
                    && chosen.as_ref().is_none_or(|(seq, _, _)| seq_num > *seq)
                {
                    let entry = self.iters[i].entry().await?;
                    chosen = Some((seq_num, kind, entry.value));
                }
                match dir {
                    Dir::Forward => self.iters[i].next().await?,
                    Dir::Reverse => self.iters[i].prev().await?,
                }
            }
        }
        Ok(chosen)
    }

    /// The iterator on the least internal key, if any is still positioned.
    fn least(&self) -> Result<Option<usize>> {
        let mut best: Option<usize> = None;
        for i in 0..self.iters.len() {
            if !self.iters[i].is_valid() {
                continue;
            }
            match best {
                None => best = Some(i),
                Some(b) => {
                    if key::compare(self.iters[i].key(), self.iters[b].key())?.is_lt() {
                        best = Some(i);
                    }
                }
            }
        }
        Ok(best)
    }

    /// The iterator on the greatest internal key, if any is still positioned.
    fn greatest(&self) -> Result<Option<usize>> {
        let mut best: Option<usize> = None;
        for i in 0..self.iters.len() {
            if !self.iters[i].is_valid() {
                continue;
            }
            match best {
                None => best = Some(i),
                Some(b) => {
                    if key::compare(self.iters[i].key(), self.iters[b].key())?.is_gt() {
                        best = Some(i);
                    }
                }
            }
        }
        Ok(best)
    }

    /// Whether the newest version of `user` is a live value.
    fn visible(&self, user: &[u8], seq: u64, kind: Kind) -> Result<bool> {
        if is_deletion(kind) {
            return Ok(false);
        }
        if covered_by_tombstone(&self.tables, user, seq)? {
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
        let file_nums: Vec<u64> = {
            let versions = self.versions();
            let mut wanted = Vec::new();
            for (_, metas) in versions.version.levels() {
                for meta in metas {
                    if meta.overlaps_range(lower, upper)? {
                        wanted.push(meta.file_num);
                    }
                }
            }
            wanted
        };
        let mut tables = Vec::new();
        for file_num in file_nums {
            tables.push(self.open_table(file_num).await?);
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
