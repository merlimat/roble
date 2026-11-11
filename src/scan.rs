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
use crate::table::{OpenTable, TableIter, covered_by_tombstone, is_deletion};

/// Which way the iterators are currently positioned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dir {
    Forward,
    Reverse,
}

/// Where the scan is.
struct Current {
    user: Vec<u8>,
    value: Vec<u8>,
}

/// A scan over the tables of a version.
///
/// Forward and reverse, optionally bounded. The bounds are half-open as
/// Pebble's are: `lower` is included and `upper` is not.
pub struct Scan<D: Disk> {
    iters: Vec<TableIter<D>>,
    tables: Vec<Rc<OpenTable<D>>>,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    dir: Dir,
    current: Option<Current>,
}

impl<D: Disk> Scan<D> {
    /// A scan over `tables`, bounded by `lower` (included) and `upper`
    /// (excluded), positioned nowhere.
    pub fn new(
        tables: Vec<Rc<OpenTable<D>>>,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    ) -> Self {
        let iters = tables.iter().map(|t| t.iter()).collect();
        Scan {
            iters,
            tables,
            lower,
            upper,
            dir: Dir::Forward,
            current: None,
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

            // The least internal key is the newest version of the least user
            // key, so this entry is the candidate without further searching.
            let entry = self.iters[w].entry().await?;
            let decoded = InternalKey::decode(&entry.key)?;
            let user = decoded.user_key.to_vec();
            let (seq, kind) = (decoded.seq_num, decoded.kind);

            self.skip_forward_over(&user).await?;

            if let Some(upper) = &self.upper
                && user >= *upper
            {
                self.current = None;
                return Ok(());
            }
            if self.visible(&user, seq, kind)? {
                self.current = Some(Current {
                    user,
                    value: entry.value,
                });
                return Ok(());
            }
        }
    }

    /// Moves to the previous visible key, skipping deletions.
    async fn retreat(&mut self) -> Result<()> {
        loop {
            let Some(w) = self.greatest()? else {
                self.current = None;
                return Ok(());
            };
            let user = InternalKey::decode(self.iters[w].key())?.user_key.to_vec();

            // Walk every iterator back over this user key, keeping the
            // smallest internal key seen — which is its newest version. The
            // largest, which `greatest` found, is the oldest.
            let mut newest: Option<(Vec<u8>, Vec<u8>)> = None;
            for i in 0..self.iters.len() {
                loop {
                    if !self.iters[i].is_valid() {
                        break;
                    }
                    let here = self.iters[i].key().to_vec();
                    if InternalKey::decode(&here)?.user_key != user.as_slice() {
                        break;
                    }
                    let better = match &newest {
                        None => true,
                        Some((best, _)) => key::compare(&here, best)?.is_lt(),
                    };
                    if better {
                        let e = self.iters[i].entry().await?;
                        newest = Some((e.key, e.value));
                    }
                    self.iters[i].prev().await?;
                }
            }
            let (key_bytes, value) = newest.expect("the winning iterator held this user key");
            let decoded = InternalKey::decode(&key_bytes)?;
            let (seq, kind) = (decoded.seq_num, decoded.kind);

            if let Some(lower) = &self.lower
                && user < *lower
            {
                self.current = None;
                return Ok(());
            }
            if self.visible(&user, seq, kind)? {
                self.current = Some(Current { user, value });
                return Ok(());
            }
        }
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

    /// Steps every iterator forward past every version of `user`.
    async fn skip_forward_over(&mut self, user: &[u8]) -> Result<()> {
        for i in 0..self.iters.len() {
            loop {
                if !self.iters[i].is_valid() {
                    break;
                }
                let here = self.iters[i].key().to_vec();
                if InternalKey::decode(&here)?.user_key != user {
                    break;
                }
                self.iters[i].next().await?;
            }
        }
        Ok(())
    }

    /// Whether the newest version of `user` is a live value.
    fn visible(&self, user: &[u8], seq: u64, kind: Kind) -> Result<bool> {
        if is_deletion(kind) {
            return Ok(false);
        }
        Ok(!covered_by_tombstone(&self.tables, user, seq)?)
    }
}

impl<F: crate::env::Fs> crate::Opened<'_, F> {
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
        // Every table of the version. A table whose key range misses the
        // bounds could be left out, but that is the version's business to
        // decide and it costs nothing here: an iterator over a table outside
        // the bounds goes invalid on its first seek.
        let mut tables = Vec::new();
        for (_, metas) in self.versions.version.levels() {
            for meta in metas {
                tables.push(self.open_table(meta.file_num).await?);
            }
        }
        Ok(Scan::new(
            tables,
            lower.map(|b| b.to_vec()),
            upper.map(|b| b.to_vec()),
        ))
    }
}
