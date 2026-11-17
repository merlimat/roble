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
//! visible (§4) — and holds the version it was made on, which names every
//! table the store had then (§5). But it is a cursor, bounded, and a lookup
//! is not something a cursor does.
//!
//! **A snapshot is the instant without the cursor.** It holds the version,
//! the memtables and the sequence number, and nothing else: making one opens
//! no table, and holding it keeps the sweep off every table that version
//! names (`crate::obsolete::Pinned`), as a scan does. A lookup opens the
//! tables whose bounds contain its key and a scan the tables its seeks land
//! in, each when it is asked, so that a snapshot costs what is read through
//! it rather than what the store holds.
//!
//! **The prefix is not here.** Oxia's prefix goes through its own key
//! encoder, which can map one prefix to several ranges; a plain byte prefix is
//! the range `[p, p+1)`. Either way the bounds are the caller's to compute
//! from its own encoding, so roble offers the view and the range and leaves
//! the word "prefix" where the encoding is.

use std::cell::RefCell;
use std::ops::RangeBounds;
use std::rc::Rc;

use roble_format::InternalKey;
use roble_format::key::{self, Kind};

use crate::env::Fs;
use crate::error::Result;
use crate::memtable::Memtable;
use crate::metrics::Metrics;
use crate::open::Db;
use crate::scan::{Opener, Scan, open_table};
use crate::table::{covered_by_tombstone, is_deletion};
use crate::version::Version;

/// A read-only view of a store as it was when the snapshot was made.
///
/// Made by [`Db::snapshot`]. Scans over it see what it saw, whatever the
/// store does meanwhile, and [`Snapshot::lookup`] answers about any key at
/// all rather than only the ones a scan's bounds take in.
pub struct Snapshot<F: Fs> {
    /// The version as it was, which says which tables there were and where.
    ///
    /// Holding it is what keeps the sweep off those tables, however many
    /// compactions later a lookup comes to open one (§5,
    /// `crate::obsolete::Pinned`).
    version: Rc<Version>,
    /// How the store opens a table, which a lookup and a scan both do as
    /// they reach one.
    tables: Rc<dyn Opener<F::Disk>>,
    /// The memtables as they were: the one writes go to, and the one a flush
    /// is writing out if there is one. Both keep being written to, and the
    /// sequence number below is what keeps the later writes out.
    memtables: Vec<Rc<RefCell<Memtable>>>,
    /// The highest sequence number this view can see.
    seq: u64,
    metrics: Rc<Metrics>,
}

impl<F: Fs> Drop for Snapshot<F> {
    /// A snapshot that has gone away releases the whole version it was
    /// holding, which is the point of saying how many are open.
    fn drop(&mut self) {
        self.metrics.snapshots_open.add(-1);
    }
}

impl<F: Fs> Snapshot<F> {
    /// A scan over the keys in `range` as the snapshot saw them, as
    /// [`Db::range`](crate::Db::range) makes one over the store.
    ///
    /// Made over the version the snapshot holds: it opens nothing until a
    /// seek lands in a table, and needs no I/O to create. The snapshot goes
    /// on holding the whole version whatever its scans' bounds, so a caller
    /// can scan a range and then look up keys outside it.
    pub fn range<'k>(&self, range: impl RangeBounds<&'k [u8]>) -> Result<Scan<F>> {
        let (lower, upper) = crate::scan::bounds(&range);
        let (lower, upper) = (lower.as_deref(), upper.as_deref());
        let mut scan = Scan::empty(Some(Rc::clone(&self.metrics)));
        scan.rebuild(
            &self.version,
            &self.tables,
            self.memtables.iter().cloned(),
            self.seq,
            lower,
            upper,
        )?;
        Ok(scan)
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

        // The tables whose bounds contain the key, which are the only ones
        // that can hold a version of it or a tombstone over it: a table's
        // bounds take in its range deletions as well as its point keys
        // (`crate::flush`). Opened here, as the lookup reaches them.
        let mut containing = Vec::new();
        self.version.tables_containing(user, &mut containing)?;
        let mut bracketing = Vec::with_capacity(containing.len());
        for (file_num, _) in containing {
            bracketing.push(open_table(&*self.tables, file_num).await?);
        }

        // The same rule over the tables, where it is the rule and not a
        // filter: no table a snapshot holds can carry a version above it,
        // because every entry in one was written before the snapshot was made
        // and a table written afterwards is not in its version. So the
        // comparison below is always true here, and a mutation that removes
        // it survives — stated rather than tested (§12), because the
        // alternative is a reader having to prove that invariant before
        // believing the loop.
        for table in &bracketing {
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
        // same, and only its bounds may rule it out. `crate::read` splits the
        // two the same way.
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

impl<F: Fs> Db<F> {
    /// A read-only view of the store as it is now.
    ///
    /// Holds the current version, which is what lets [`Snapshot::lookup`]
    /// reach a key wherever it sorts, and what stops the obsolete sweep
    /// collecting a file the snapshot still needs (`crate::obsolete::Pinned`).
    /// Opens nothing: the snapshot's lookups and scans open the tables they
    /// reach, as a scan made from the store does.
    ///
    /// So a snapshot is worth holding for as long as a caller is reading at
    /// one instant and no longer: while it lives, nothing the store writes
    /// over can be collected.
    pub fn snapshot(&self) -> Result<Snapshot<F>> {
        let version = Rc::clone(&self.versions().version);
        self.metrics.snapshots_open.add(1);
        Ok(Snapshot {
            version,
            tables: self.tables(),
            memtables: self.memtables(),
            seq: self.last_seq_num(),
            metrics: Rc::clone(&self.metrics),
        })
    }
}
