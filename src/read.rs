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
use crate::version::Table as TableMeta;

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

/// The least user key strictly greater than `user`.
///
/// Appending a zero byte is exactly that: any key greater than `user` either
/// extends it, and so is at or after `user || 0`, or differs from it at some
/// byte, and compares the same way against both. It is how a search steps past
/// a user key it has rejected without having to enumerate its versions.
pub(crate) fn successor(user: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(user.len() + 1);
    out.extend_from_slice(user);
    out.push(0);
    out
}

/// One table's answer: the newest entry it holds for a user key.
struct Candidate {
    trailer: u64,
    kind: Kind,
    value: Vec<u8>,
}

impl<F: Fs> Opened<'_, F> {
    /// Reads the key `how` says, or `None` if there is none.
    pub async fn get(&self, key: &[u8], how: Comparison) -> Result<Option<Found>> {
        let _timer = self.metrics.get.start();
        let result = match how {
            // Equal keeps its own path: it is the one comparison a bloom
            // filter can answer, and a scan does not consult filters because
            // it is asking about a range (§6).
            Comparison::Equal => self.equal(key).await,
            _ => self.positioned(key, how).await,
        };
        let found = self.metrics.read_errors.saw(result)?;
        if let Some(found) = &found {
            self.metrics
                .read_bytes
                .add((found.key.len() + found.value.len()) as u64);
        }
        Ok(found)
    }

    /// A point lookup.
    async fn equal(&self, user: &[u8]) -> Result<Option<Found>> {
        let Some(found) = self.newest_for(user).await? else {
            return Ok(None);
        };
        if !self.visible(user, &found).await? {
            return Ok(None);
        }
        Ok(Some(Found {
            key: user.to_vec(),
            value: found.value,
        }))
    }

    /// The first visible key a scan positioned `how` lands on.
    ///
    /// The four ordered comparisons are a scan that is positioned once and
    /// read, which is what they always were in §6's description: "the other
    /// four are a seek ... over the full iterator tree". They used to be a
    /// merge of their own, re-seeking every candidate table once per key they
    /// rejected; a scan steps instead, and skipping deleted keys is its job
    /// rather than this one's.
    async fn positioned(&self, key: &[u8], how: Comparison) -> Result<Option<Found>> {
        let mut scan = self.scan().await?;
        match how {
            Comparison::Ceiling => scan.seek_ge(key).await?,
            Comparison::Higher => scan.seek_ge(&successor(key)).await?,
            // The greatest key at or below `key` is the greatest key strictly
            // below the one after it.
            Comparison::Floor => scan.seek_lt(&successor(key)).await?,
            Comparison::Lower => scan.seek_lt(key).await?,
            Comparison::Equal => unreachable!("Equal is a point lookup"),
        }
        Ok(scan.is_valid().then(|| Found {
            key: scan.key().to_vec(),
            value: scan.value().to_vec(),
        }))
    }

    /// The newest entry any table holds for `user`.
    ///
    /// Newest by sequence number, across every level: an entry's level says
    /// nothing a trailer does not already say, and trailers are what the
    /// ordering is defined on.
    async fn newest_for(&self, user: &[u8]) -> Result<Option<Candidate>> {
        let target = key::search_key(user);
        let mut best: Option<Candidate> = None;

        // The memtable first. Anything it holds was written after every table
        // was, so it usually wins outright — but the comparison below is by
        // trailer either way, so this is an ordering of convenience, not of
        // correctness.
        {
            let held = self.memtable();
            let memtable = held.borrow();
            let mut cursor = memtable.cursor();
            cursor.seek_ge(&memtable, &target)?;
            if cursor.is_valid() {
                let k = InternalKey::decode(cursor.key(&memtable))?;
                if k.user_key == user {
                    best = Some(Candidate {
                        trailer: (k.seq_num << 8) | k.kind.to_byte() as u64,
                        kind: k.kind,
                        value: cursor.value(&memtable).to_vec(),
                    });
                }
            }
        }

        for meta in self.live_tables() {
            if meta.smallest_user_key()? > user || meta.largest_user_key()? < user {
                continue;
            }
            let table = self.open_table(meta.file_num).await?;
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

            let Some(entry) = table.seek_ge(&target).await? else {
                continue;
            };
            let k = InternalKey::decode(&entry.key)?;
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
                value: entry.value,
            };
            if best.as_ref().is_none_or(|b| candidate.trailer > b.trailer) {
                best = Some(candidate);
            }
        }
        Ok(best)
    }

    /// Whether `found` is a live value for `user`.
    async fn visible(&self, user: &[u8], found: &Candidate) -> Result<bool> {
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
        let mut tables = Vec::new();
        for meta in self.live_tables() {
            if meta.smallest_user_key()? > user || meta.largest_user_key()? < user {
                continue;
            }
            tables.push(self.open_table(meta.file_num).await?);
        }
        if crate::table::covered_by_tombstone(&tables, user, seq)? {
            return Ok(false);
        }
        // And the memtable's, which are kept apart from its point keys and so
        // are in no cursor's way (§4).
        for t in self.memtable().borrow().tombstones() {
            if t.seq_num > seq && t.start.as_slice() <= user && user < t.end.as_slice() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Every table of the version, in no particular order.
    ///
    /// Order does not matter: every comparison below is by key or by trailer,
    /// never by position in the tree.
    fn live_tables(&self) -> Vec<TableMeta> {
        self.versions()
            .version
            .levels()
            .flat_map(|(_, tables)| tables.iter().cloned())
            .collect()
    }
}
