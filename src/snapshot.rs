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

//! A view of the store at one instant, read by key as well as scanned
//! (§6, §11).
//!
//! **What Oxia's `KeyPrefixIterator` is.** Its contract
//! (`oxiad/dataserver/database/kvstore/kv.go`) is an iterator over the keys
//! that start with a prefix "which also reads the other keys of the store as
//! they were when it was created", with a `Lookup(key)` that returns a key's
//! value "wherever it sorts, at the point in time of the iterator". So it is
//! two things over one instant: a bounded cursor, and a point lookup over the
//! whole key space.
//!
//! roble had the first and not the second. A [`Scan`] already fixes an
//! instant — a sequence number taken when it is made, above which nothing is
//! visible (§4) — but it opens only the tables its bounds reach (§5), so it
//! cannot answer about a key outside them. That filtering is worth keeping:
//! it is what stops a scan over three records pinning a shard's files
//! against the obsolete sweep. A caller that wants to read the whole store at
//! one instant should say so and pay for it, rather than every scan paying in
//! case one does.
//!
//! **A snapshot is that saying-so.** It opens every table of the version when
//! it is made and holds them, which is both what lets a lookup reach any key
//! and what keeps the sweep off those files (`crate::obsolete`). Its scans
//! then need no I/O at all to create: the tables are open already, and bounds
//! only choose among them.
//!
//! **The prefix is not here.** Oxia's prefix goes through its own key
//! encoder, which can map one prefix to several ranges; a plain byte prefix is
//! the range `[p, p+1)`. Either way the bounds are the caller's to compute
//! from its own encoding, so roble offers the view and the range and leaves
//! the word "prefix" where the encoding is.

use std::cell::RefCell;
use std::rc::Rc;

use roble_format::InternalKey;
use roble_format::key::{self, Kind};

use crate::env::{Disk, Fs};
use crate::error::Result;
use crate::memtable::Memtable;
use crate::metrics::Metrics;
use crate::open::Opened;
use crate::scan::Scan;
use crate::table::{OpenTable, covered_by_tombstone, is_deletion};
use crate::version::Table;

/// A read-only view of a store as it was when the snapshot was made.
///
/// Made by [`Opened::snapshot`]. Scans over it see what it saw, whatever the
/// store does meanwhile, and [`Snapshot::lookup`] answers about any key at
/// all rather than only the ones a scan's bounds take in.
pub struct Snapshot<D: Disk> {
    /// Every table of the version, open, each beside its metadata and the
    /// level it was in.
    ///
    /// The open reader is what keeps the sweep off the file; the metadata is
    /// what a bounded scan and a lookup choose among them by, and it is not
    /// derivable from the reader — a table's largest bound can be the
    /// exclusive sentinel a range deletion leaves (§5), which only the
    /// version records. Nor is the level, and a scan needs it to know which
    /// tables it may read as one ([`crate::scan::Placed`]), so a snapshot
    /// has to carry it from the version it was made from.
    tables: Vec<(u32, Table, Rc<OpenTable<D>>)>,
    /// The memtables as they were: the one writes go to, and the one a flush
    /// is writing out if there is one. Both keep being written to, and the
    /// sequence number below is what keeps the later writes out.
    memtables: Vec<Rc<RefCell<Memtable>>>,
    /// The highest sequence number this view can see.
    seq: u64,
    metrics: Rc<Metrics>,
}

impl<D: Disk> Drop for Snapshot<D> {
    /// A snapshot that has gone away releases the whole version it was
    /// holding, which is the point of saying how many are open.
    fn drop(&mut self) {
        self.metrics.snapshots_open.add(-1);
    }
}

impl<D: Disk> Snapshot<D> {
    /// A scan over every key the snapshot holds.
    pub fn scan(&self) -> Result<Scan<D>> {
        self.scan_range(None, None)
    }

    /// A scan bounded below by `lower` (included) and above by `upper`
    /// (excluded), as Pebble's bounds are.
    ///
    /// Bounded only in what it reads: the snapshot goes on holding every
    /// table whatever its scans ask for, so this chooses which iterators to
    /// build and nothing more. It needs no I/O for that, which is what makes
    /// a snapshot the right handle for a caller that scans a range and then
    /// looks up keys outside it.
    pub fn scan_range(&self, lower: Option<&[u8]>, upper: Option<&[u8]>) -> Result<Scan<D>> {
        // Only the tables the bounds can reach. Removing this check changes
        // no answer — an iterator over a table outside the bounds goes
        // invalid on its first seek, and a tombstone covering anything inside
        // them belongs to a table whose bounds reach inside them too (§5) —
        // so no test pins it and none should, as with the bloom filter in
        // `crate::read`. It is here for the seek per table per step it saves.
        let mut tables = Vec::new();
        // By level, because that is what decides whether a bound is needed at
        // all, and `self.tables` is in the version's own order — every table
        // of a level together, L0 first — so a chunk is a level.
        for of_level in self.tables.chunk_by(|a, b| a.0 == b.0) {
            // `Opened::scan_with`'s rule: L0's tables are never read as one,
            // and a level holding a single table has nothing to choose
            // between, so neither copies a key no run would consult.
            let several = of_level[0].0 > 0 && of_level.len() > 1;
            for (level, meta, table) in of_level {
                if meta.overlaps_range(lower, upper)? {
                    tables.push(crate::scan::Placed {
                        level: *level,
                        table: Rc::clone(table),
                        smallest: several
                            .then(|| meta.smallest_user_key().map(<[u8]>::to_vec))
                            .transpose()?,
                    });
                }
            }
        }
        self.metrics.scans.add(1);
        self.metrics.scans_open.add(1);
        Ok(Scan::new(
            tables,
            self.memtables.clone(),
            self.seq,
            lower.map(|b| b.to_vec()),
            upper.map(|b| b.to_vec()),
            Some(Rc::clone(&self.metrics)),
        ))
    }

    /// The value of `key` as the snapshot saw it, wherever the key sorts, or
    /// `None` if the snapshot held no live value for it.
    ///
    /// Oxia's `Lookup`, which returns `ErrKeyNotFound` where this returns
    /// `None`. It is a point lookup rather than a scan positioned once, so
    /// that it costs what a point lookup costs: the bloom filters rule out
    /// the tables that cannot hold the key, and a scan consults no filter
    /// because a scan asks about a range (§6). Oxia reaches the same place
    /// from the other side, seeking a cloned iterator with `SeekPrefixGE` so
    /// that the filters are consulted after all.
    pub async fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _timer = self.metrics.get.start();
        let result = self.lookup_inner(key).await;
        let value = self.metrics.read_errors.saw(result)?;
        if let Some(value) = &value {
            self.metrics
                .read_bytes
                .add((key.len() + value.len()) as u64);
        }
        Ok(value)
    }

    async fn lookup_inner(&self, user: &[u8]) -> Result<Option<Vec<u8>>> {
        let target = key::search_key(user);
        // The newest version of `user` the snapshot can see: its sequence
        // number, its kind, and its value.
        let mut best: Option<(u64, Kind, Vec<u8>)> = None;

        // Every version has to be stepped over rather than only the newest
        // one read, which is where this parts company with `crate::read`'s
        // point lookup: that one has no instant to respect, so the first
        // entry a seek finds for the key is its answer. Here the newest
        // version may have been written after the snapshot, and then the one
        // below it is the answer.
        for held in &self.memtables {
            let memtable = held.borrow();
            let mut cursor = memtable.cursor();
            cursor.seek_ge(&memtable, &target)?;
            while cursor.is_valid() {
                let here = cursor.key(&memtable).to_vec();
                let k = InternalKey::decode(&here)?;
                if k.user_key != user {
                    break;
                }
                // `<=` and not `crate::scan`'s `visible_at`: that one also
                // admits the stand-in numbers of an uncommitted batch, and a
                // snapshot's memtables are the store's own, never a batch
                // index.
                if k.seq_num <= self.seq && best.as_ref().is_none_or(|(seq, _, _)| k.seq_num > *seq)
                {
                    best = Some((k.seq_num, k.kind, cursor.value(&memtable).to_vec()));
                }
                cursor.next(&memtable);
            }
        }

        // The same rule over the tables, where it is the rule and not a
        // filter: no table a snapshot holds can carry a version above it,
        // because every entry in one was written before the snapshot was made
        // and a table written afterwards is not in this list. So the
        // comparison below is always true here, and a mutation that removes
        // it survives — stated rather than tested (§12), because the
        // alternative is a reader having to prove that invariant before
        // believing the loop.
        for (_, _, table) in &self.tables {
            if !table.may_contain(user)? {
                continue;
            }
            let mut it = table.iter();
            it.seek_ge(&target).await?;
            while it.is_valid() {
                // The trailer read out of the borrowed key rather than out of
                // a copy of it, and the value without a second copy of that
                // key beside it. The copy was here to end the borrow before
                // the call below; scoping it to the three scalars this needs
                // ends the borrow just as well, and `value_into` never builds
                // the key it was going to throw away. Two allocations a
                // version stepped over, both of them of the same key.
                let (seq_num, kind, matches) = {
                    let k = InternalKey::decode(it.key())?;
                    (k.seq_num, k.kind, k.user_key == user)
                };
                if !matches {
                    break;
                }
                if seq_num <= self.seq && best.as_ref().is_none_or(|(seq, _, _)| seq_num > *seq) {
                    let mut value = Vec::new();
                    it.value_into(&mut value).await?;
                    best = Some((seq_num, kind, value));
                }
                it.next().await?;
            }
        }

        let Some((seq, kind, value)) = best else {
            return Ok(None);
        };
        if is_deletion(kind) {
            return Ok(None);
        }

        // A filter answers about point keys, and a range deletion is not one:
        // it lives in a block of its own and no filter knows about it. So a
        // table whose filter rejected the key above is consulted here all the
        // same, and only its bounds may rule it out — safely, because a
        // table's bounds take in its range deletions as well as its point
        // keys (`crate::flush`). `crate::read` splits the two the same way.
        let mut bracketing = Vec::new();
        for (_, meta, table) in &self.tables {
            if meta.smallest_user_key()? > user || meta.largest_user_key()? < user {
                continue;
            }
            bracketing.push(Rc::clone(table));
        }
        if covered_by_tombstone(&bracketing, user, seq)? {
            return Ok(None);
        }
        // And the memtables', which are kept apart from their point keys and
        // so are in no cursor's way (§4). A tombstone written after the
        // snapshot is not its business either.
        for held in &self.memtables {
            for t in held.borrow().tombstones() {
                if t.seq_num > seq
                    && t.seq_num <= self.seq
                    && t.start.as_slice() <= user
                    && user < t.end.as_slice()
                {
                    return Ok(None);
                }
            }
        }
        Ok(Some(value))
    }
}

impl<F: Fs> Opened<F> {
    /// A read-only view of the store as it is now.
    ///
    /// Opens every table of the current version, which is what lets
    /// [`Snapshot::lookup`] reach a key wherever it sorts, and what stops the
    /// obsolete sweep collecting a file the snapshot still needs
    /// (`crate::obsolete`: a table a reader is open for is left alone). A
    /// scan made from the store opens only what its bounds reach (§5); this
    /// is the deliberate opposite, and it costs the whole version's footers
    /// and indexes up front rather than one table's at a time.
    ///
    /// So a snapshot is worth holding for as long as a caller is reading at
    /// one instant and no longer: while it lives, nothing the store writes
    /// over can be collected.
    pub async fn snapshot(&self) -> Result<Snapshot<F::Disk>> {
        // The metadata first, so the version's borrow is released before any
        // opening: a table is I/O, and a flush is allowed to run meanwhile.
        let metas: Vec<(u32, Table)> = {
            let versions = self.versions();
            versions
                .version
                .levels()
                .flat_map(|(level, tables)| tables.iter().cloned().map(move |t| (level, t)))
                .collect()
        };
        let mut tables = Vec::with_capacity(metas.len());
        for (level, meta) in metas {
            let open = self.open_table(meta.file_num).await?;
            tables.push((level, meta, open));
        }
        self.metrics.snapshots_open.add(1);
        Ok(Snapshot {
            tables,
            memtables: self.memtables(),
            seq: self.last_seq_num(),
            metrics: Rc::clone(&self.metrics),
        })
    }
}
