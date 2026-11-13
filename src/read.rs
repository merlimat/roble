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

//! Reading a key out of a version: the five comparisons of §6, across every
//! level, with point deletes and range tombstones applied.
//!
//! This is the first code that answers a question about a *key* rather than
//! about a file, and the first that has to decide which of several answers
//! wins. Two rules carry all of it:
//!
//! - **The highest sequence number wins.** Sequence numbers are global, so a
//!   level's position in the tree is not consulted: the newest entry for a
//!   user key is the answer whatever level it came from. L0's tables overlap
//!   and the levels below it do not, and neither fact needs special handling
//!   once the comparison is by sequence number.
//! - **A deletion is an answer too, and then the search continues.** A floor
//!   lookup that lands on a deleted key has not finished; it has to keep going
//!   down. This is why the four ordered comparisons are loops rather than
//!   single seeks, and it is the cost §2 says the format chooses for us: a
//!   range tombstone keeps being consulted on every read in its range until a
//!   compaction drops it.
//!
//! What is deliberately not here: a resumable iterator. A cursor that outlives
//! one call has to hold the blocks it is reading, and the thing that owns
//! blocks is the cache (§8). So `get` does its whole merge within one call,
//! and `seek_ge`/`next`/`prev` over a snapshot arrive with the table cache
//! that can hold an open reader.

use roble_format::InternalKey;
use roble_format::key::{self, Kind};

use crate::env::Fs;
use crate::error::Result;
use crate::open::Opened;

/// Which key a read is asking for, relative to the one it is given.
///
/// The same five encina offers. `Equal` is a point lookup; the other four are
/// seeks over every level, since the answer may be anywhere (§6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    /// Exactly this key.
    Equal,
    /// The greatest key less than or equal to it.
    Floor,
    /// The least key greater than or equal to it.
    Ceiling,
    /// The greatest key strictly less than it.
    Lower,
    /// The least key strictly greater than it.
    Higher,
}

/// What a read found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    /// The user key, which for anything but [`Comparison::Equal`] is not the
    /// key that was asked for.
    pub key: Vec<u8>,
    /// Its value.
    pub value: Vec<u8>,
}

/// A lookup's working space, kept and handed back by the caller.
///
/// Not [`crate::ReadBuf`], which is a block of bytes for one file read. This
/// is everything one `get` needs somewhere to put.
///
/// A point lookup needs more scratch than its answer: the internal key it
/// seeks to, the file numbers of the tables whose range holds the key, and a
/// pair of buffers to take each table's candidate into before it knows which
/// one wins. All of it used to be allocated per call and dropped, which made
/// a lookup's allocation count grow with the number of tables in the store.
///
/// It lives here rather than on the store because a lookup `.await`s in the
/// middle: scratch behind a `RefCell` on a shared handle could not be held
/// across those awaits once two reads were ever in flight at once, and a
/// buffer the caller owns needs no pool and no sharing — a caller with two
/// reads in flight has two buffers.
///
/// Hand the same one back on every call and a read in a steady state stops
/// allocating. [`Opened::get`] makes one per call, which is what a caller
/// that wants its answer owned is paying for.
#[derive(Debug, Default)]
pub struct Lookup {
    key: Vec<u8>,
    value: Vec<u8>,
    /// The encoded internal key a seek targets.
    target: Vec<u8>,
    /// The tables whose key range holds the key being read.
    file_nums: Vec<u64>,
    /// One table's candidate, before it is known to be the newest.
    entry_key: Vec<u8>,
    entry_value: Vec<u8>,
    /// A block cursor, with the table format it was built for.
    ///
    /// A cursor assembles the prefix-compressed key it is on into a `Vec`, so
    /// one built per table searched was an allocation per table — the last
    /// one a warm lookup made ([`crate::table::OpenTable::seek_ge_into`]).
    cursor: Option<(
        roble_format::TableFormat,
        roble_format::block::restart::DataCursor,
    )>,
}

impl Lookup {
    /// Working space that has not been used yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The key the last read answered with, or empty if it found nothing.
    ///
    /// For [`Comparison::Equal`] this is the key that was asked for; for the
    /// other four it is the key the seek landed on.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Its value, or empty if the last read found nothing.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Takes the answer out as a [`Found`], leaving the buffer empty.
    ///
    /// This moves the two buffers rather than copying them, so it gives up
    /// the capacity they had reached. A caller reading in a loop wants
    /// [`Self::key`] and [`Self::value`] instead.
    pub fn take(&mut self) -> Found {
        Found {
            key: std::mem::take(&mut self.key),
            value: std::mem::take(&mut self.value),
        }
    }
}

/// The least user key strictly greater than `user`.
///
/// Appending a zero byte is exactly that: any key greater than `user` either
/// extends it, and so is at or after `user || 0`, or differs from it at some
/// byte, and compares the same way against both. It is how a search steps past
/// a user key it has rejected without having to enumerate its versions.
pub(crate) fn successor(user: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(user.len() + 1);
    successor_into(user, &mut out);
    out
}

/// The same, into a buffer the caller keeps.
pub(crate) fn successor_into(user: &[u8], out: &mut Vec<u8>) {
    out.clear();
    // Sized once, as `successor`'s `with_capacity` does: extending to the
    // key's length and then pushing the zero byte would grow the buffer a
    // second time.
    out.reserve(user.len() + 1);
    out.extend_from_slice(user);
    out.push(0);
}

/// One table's answer for a user key: which entry won, not what it holds.
///
/// The value lives in the caller's buffer. Keeping it here meant one
/// allocation per table that offered a candidate, including every one a later
/// table's trailer then beat.
struct Candidate {
    trailer: u64,
    kind: Kind,
}

impl<F: Fs> Opened<F> {
    /// Reads the key `how` says, or `None` if there is none.
    ///
    /// Allocates the answer and the scratch the read needed to find it.
    /// [`Self::get_into`] is the same read into a [`Lookup`] the caller
    /// keeps, which allocates neither once it is warm.
    pub async fn get(&self, key: &[u8], how: Comparison) -> Result<Option<Found>> {
        let mut buf = Lookup::new();
        Ok(self.get_into(key, how, &mut buf).await?.then(|| buf.take()))
    }

    /// Reads the key `how` says into `buf`, answering whether there was one.
    ///
    /// On `true`, [`Lookup::key`] and [`Lookup::value`] hold the answer. On
    /// `false` both are empty, so the same buffer can be handed back call
    /// after call without a miss reading as the previous hit. What it holds
    /// after an `Err` is not defined.
    ///
    /// This is the allocation-free read: hand back the same `buf` and, once
    /// its buffers have reached the sizes the data needs, a lookup in a steady
    /// state allocates nothing at all.
    pub async fn get_into(&self, key: &[u8], how: Comparison, buf: &mut Lookup) -> Result<bool> {
        let _timer = self.metrics.get.start();
        buf.key.clear();
        buf.value.clear();
        let result = match how {
            // Equal keeps its own path: it is the one comparison a bloom
            // filter can answer, and a scan does not consult filters because
            // it is asking about a range (§6).
            Comparison::Equal => self.equal(key, buf).await,
            _ => self.positioned(key, how, buf).await,
        };
        let found = self.metrics.read_errors.saw(result)?;
        if found {
            self.metrics
                .read_bytes
                .add((buf.key.len() + buf.value.len()) as u64);
        } else {
            // One place for the "empty on false" half of the contract above,
            // rather than every path that can decide there is no answer.
            buf.key.clear();
            buf.value.clear();
        }
        Ok(found)
    }

    /// A point lookup.
    async fn equal(&self, user: &[u8], buf: &mut Lookup) -> Result<bool> {
        // The range check both halves need, done once. They narrow it
        // differently from here — `newest_for` consults each table's filter
        // and `visible` must not, since a filter is built from point keys and
        // a range tombstone is not one — but which tables *contain* the key
        // is one question with one answer.
        self.tables_containing(user, &mut buf.file_nums)?;
        let Some(found) = self.newest_for(user, buf).await? else {
            return Ok(false);
        };
        if !self.visible(user, &found, &buf.file_nums).await? {
            return Ok(false);
        }
        // The key an `Equal` found is the key it was asked for.
        buf.key.extend_from_slice(user);
        Ok(true)
    }

    /// The first visible key a scan positioned `how` lands on.
    ///
    /// The four ordered comparisons are a scan that is positioned once and
    /// read, which is what they always were in §6's description: "the other
    /// four are a seek ... over the full iterator tree". They used to be a
    /// merge of their own, re-seeking every candidate table once per key they
    /// rejected; a scan steps instead, and skipping deleted keys is its job
    /// rather than this one's.
    async fn positioned(&self, key: &[u8], how: Comparison, buf: &mut Lookup) -> Result<bool> {
        // **Bounded on the side the comparison already decides.** A `Floor`
        // cannot answer above `key`, a `Ceiling` cannot answer below it, and
        // `scan_with` opens only the tables its bounds reach — so the half of
        // the store on the wrong side of the probe is never opened at all.
        // Unbounded, this built a merge over every table in the version for
        // every one of these lookups, which at six million records is two
        // hundred and forty-three of them.
        //
        // The other side stays open because nothing here knows how far the
        // answer is: a `Floor` whose neighbourhood is entirely deleted is
        // answered by a key arbitrarily far below.
        let (lower, upper) = match how {
            Comparison::Ceiling => (Some(key), None),
            Comparison::Higher => {
                successor_into(key, &mut buf.target);
                (Some(buf.target.as_slice()), None)
            }
            // The greatest key at or below `key` is the greatest key strictly
            // below the one after it, and `upper` is exclusive.
            Comparison::Floor => {
                successor_into(key, &mut buf.target);
                (None, Some(buf.target.as_slice()))
            }
            Comparison::Lower => (None, Some(key)),
            Comparison::Equal => unreachable!("Equal is a point lookup"),
        };
        let mut scan = self.scan_range(lower, upper).await?;
        match how {
            Comparison::Ceiling | Comparison::Higher => {
                let at = lower
                    .expect("a lower bound for a forward comparison")
                    .to_vec();
                scan.seek_ge(&at).await?
            }
            Comparison::Floor | Comparison::Lower => {
                let at = upper
                    .expect("an upper bound for a backward comparison")
                    .to_vec();
                scan.seek_lt(&at).await?
            }
            Comparison::Equal => unreachable!("Equal is a point lookup"),
        }
        if !scan.is_valid() {
            return Ok(false);
        }
        buf.key.extend_from_slice(scan.key());
        buf.value.extend_from_slice(scan.value());
        Ok(true)
    }

    /// The newest entry any table holds for `user`.
    ///
    /// Newest by sequence number, across every level: an entry's level says
    /// nothing a trailer does not already say, and trailers are what the
    /// ordering is defined on.
    async fn newest_for(&self, user: &[u8], buf: &mut Lookup) -> Result<Option<Candidate>> {
        // Split so the value being filled, the scratch being overwritten and
        // the list being walked can be held at once; they are disjoint parts
        // of the caller's one buffer.
        let Lookup {
            value,
            target,
            file_nums,
            entry_key,
            entry_value,
            cursor,
            ..
        } = buf;
        key::search_key_into(user, target);
        let mut best: Option<Candidate> = None;

        // The memtables first — the one writes go to, and the one a flush is
        // writing out if there is one. Anything either holds was written
        // after every table was, so one of them usually wins outright, but
        // the comparison below is by trailer either way: this is an ordering
        // of convenience, not of correctness.
        for held in self.memtables_each() {
            let memtable = held.borrow();
            let mut cursor = memtable.cursor();
            cursor.seek_ge(&memtable, target)?;
            if cursor.is_valid() {
                let k = InternalKey::decode(cursor.key(&memtable))?;
                if k.user_key == user {
                    let candidate = Candidate {
                        trailer: (k.seq_num << 8) | k.kind.to_byte() as u64,
                        kind: k.kind,
                    };
                    // Copied only if it wins. A memtable's value is borrowed
                    // from the memtable, so there is nothing to swap.
                    if best.as_ref().is_none_or(|b| candidate.trailer > b.trailer) {
                        value.clear();
                        value.extend_from_slice(cursor.value(&memtable));
                        best = Some(candidate);
                    }
                }
            }
        }

        for &file_num in file_nums.iter() {
            let table = self.open_table(file_num).await?;
            // The filter is consulted only here: it answers about one key, and
            // the ordered comparisons are asking about a range (§6).
            //
            // Deleting this check changes no answer, and a test that failed
            // when it was removed would be testing the wrong thing: a filter
            // is an optimisation. What *is* pinned is that its answer is not
            // inverted, which would skip the table holding the key.
            if !table.may_contain(user)? {
                continue;
            }

            if !table
                .seek_ge_into(target, cursor, entry_key, entry_value)
                .await?
            {
                continue;
            }
            let k = InternalKey::decode(entry_key)?;
            if k.user_key != user {
                // The table has no version of this key; the seek landed past
                // it.
                continue;
            }
            // Trailers sort descending, so the first entry a seek finds for a
            // user key is that table's newest version of it.
            let candidate = Candidate {
                trailer: (k.seq_num << 8) | k.kind.to_byte() as u64,
                kind: k.kind,
            };
            if best.as_ref().is_none_or(|b| candidate.trailer > b.trailer) {
                // Swapped, not copied: the winner's bytes are already in a
                // buffer of the right size, and the one they replace goes
                // back to being scratch with its capacity intact.
                std::mem::swap(value, entry_value);
                best = Some(candidate);
            }
        }
        Ok(best)
    }

    /// Whether `found` is a live value for `user`.
    async fn visible(&self, user: &[u8], found: &Candidate, containing: &[u64]) -> Result<bool> {
        if crate::table::is_deletion(found.kind) {
            return Ok(false);
        }
        // A range tombstone newer than the entry covers it. Older ones do not,
        // which is why this is a sequence-number comparison and not a level
        // one: a tombstone in a lower level can still be newer than a key
        // above it only if its sequence number says so.
        //
        // `>` rather than `>=` is not pinned by any test, and cannot be:
        // every entry in a batch gets its own sequence number, so a point key
        // and a tombstone never share one and the two readings cannot be told
        // apart. `>` is what Pebble means — a tombstone deletes what came
        // before it — and it is written that way for that reason, not because
        // a test demanded it.
        let seq = found.trailer >> 8;
        // One table at a time, rather than opening all of them and then
        // asking: any covering tombstone answers the whole question, so the
        // tables after it need not be opened at all.
        for &file_num in containing {
            let table = self.open_table(file_num).await?;
            if crate::table::covered_by_tombstone(std::slice::from_ref(&table), user, seq)? {
                return Ok(false);
            }
        }
        // And the memtables', which are kept apart from their point keys and
        // so are in no cursor's way (§4).
        for held in self.memtables_each() {
            for t in held.borrow().tombstones() {
                if t.seq_num > seq && t.start.as_slice() <= user && user < t.end.as_slice() {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

/// The one table of a level below L0 whose range contains `user`, if any.
///
/// Those levels hold disjoint ranges in key order (`crate::version`), so at
/// most one table can contain the key and a binary search finds it. Walking
/// the level instead is what a lookup used to do, and at six million records
/// — two hundred tables in a level — it cost `floor` 23 milliseconds against
/// RocksDB's 4 microseconds, because `floor` is the one comparison a bloom
/// filter cannot answer and so had nothing to rule a table out with.
///
/// The search is for the first table whose largest bound reaches `user`;
/// that table holds the key if its smallest bound does not start above it.
/// Written out rather than `partition_point`, because reading a bound can
/// fail and a closure has nowhere to put the error.
fn table_containing<'a>(
    tables: &'a [crate::version::Table],
    user: &[u8],
) -> Result<Option<&'a crate::version::Table>> {
    let (mut lo, mut hi) = (0usize, tables.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if tables[mid].largest_user_key()? < user {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    match tables.get(lo) {
        Some(meta) if meta.smallest_user_key()? <= user => Ok(Some(meta)),
        _ => Ok(None),
    }
}

impl<F: Fs> Opened<F> {
    /// The file number of every table whose key range contains `user`.
    ///
    /// The comparison happens **here**, while the version is still borrowed,
    /// and only file numbers leave. What it replaced carried a clone of every
    /// table's metadata out instead, and a table's two bounds are `Vec<u8>`
    /// fields: a point lookup allocated twice per table in the database, for
    /// tables whose range cannot hold the key as much as for the one that
    /// does, and twice over because both halves of the lookup asked. A store
    /// settling into more tables made every lookup allocate more — 28 per
    /// `get` at four tables and 122 at twenty-six, measured, growing by about
    /// four per table added.
    ///
    /// The borrow is why the clone was there: the callers `.await` on opening
    /// a table, and a `Ref` cannot be held across that. A file number can.
    ///
    /// Order does not matter: every comparison in the callers is by key or by
    /// trailer, never by position in the tree.
    fn tables_containing(&self, user: &[u8], out: &mut Vec<u64>) -> Result<()> {
        let versions = self.versions();
        out.clear();
        for (level, tables) in versions.version.levels() {
            if level == 0 {
                // L0's tables overlap, so any number of them can hold the
                // key and there is no order to search.
                for meta in tables {
                    if meta.smallest_user_key()? <= user && meta.largest_user_key()? >= user {
                        out.push(meta.file_num);
                    }
                }
            } else if let Some(meta) = table_containing(tables, user)? {
                out.push(meta.file_num);
            }
        }
        Ok(())
    }
}
