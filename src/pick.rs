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

//! Deciding which tables to compact, and whether to compact at all (§9).
//!
//! The merge in `crate::compact` does what it is told. This is what tells
//! it: every level is scored against what it is allowed to hold, and the
//! worst offender is compacted into the level below. A level at or under its
//! allowance scores below one and is left alone.
//!
//! Compatibility constrains the files a compaction produces, not when it runs
//! or what it picks, so this is roble's own and is deliberately simpler than
//! Pebble's — which sizes levels dynamically against the lowest non-empty one
//! and spreads its work across a level with a key cursor it carries between
//! compactions.
//!
//! **One thing it does not do**, and it is §9 asking rather than an
//! oversight: it reads no clock and draws no random number. A picker that did
//! either would make a seeded run stop replaying, and replay is how the
//! failure-path tests work at all (§10, §12). So what it decides is a
//! function of the version alone, and two stores given the same version pick
//! the same compaction.
//!
//! **What runs it.** A flush, through [`Db::maybe_compact`]: a flush is
//! what fills L0, so it is where the store notices it has too much in it.
//! That waited on the cooperative execution of `crate::work`, because a
//! compaction that ran to completion while holding the thread was exactly
//! the latency spike units of work exist to avoid; now it merges a slice at
//! a time and the thread comes back between them, and the permit comes back
//! with it (§9): a merge holds a claim on its inputs and nothing else, so
//! the flush that scheduled it does not wait for it to finish. An *idle*
//! store drains its backlog in its own merge task, which asks the picker
//! again whenever the version changes (`Db::compaction_wanted`).
//!
//! **The score is a fraction, not a float.** Two levels are compared by
//! cross-multiplying, which is exact; a float would be deterministic in
//! practice and unprovable in principle, and this is the one component whose
//! whole job is to make the same decision twice.

use roble_format::manifest::NUM_LEVELS;

use crate::env::Fs;
use crate::error::Result;
use crate::open::Db;
use crate::version::Version;

/// How deep L0 may grow, in layers, before a write waits for it to drain.
///
/// Pebble's `L0StopWritesThreshold`, and the same twelve, counted the way
/// Pebble counts it: in sublevels (`l0_depth`), not in tables. Twelve
/// tables a read crosses in one seek are one layer and do not stop a write.
pub const L0_STOP_WRITES_THRESHOLD: u64 = 12;

/// How deep L0 may grow, in layers, before it is worth compacting.
///
/// Pebble's `L0CompactionThreshold`, counted in sublevels (`l0_depth`) like
/// the stop-writes line. L0 is counted rather than measured because its
/// tables overlap: what makes L0 expensive is that a read consults every
/// layer, whatever the tables in it weigh.
pub const L0_COMPACTION_THRESHOLD: u64 = 4;

/// How many bytes L1 may hold.
///
/// Pebble's `LBaseMaxBytes`, which it applies to the lowest non-empty level
/// rather than to L1 specifically.
pub const LBASE_MAX_BYTES: u64 = 64 << 20;

/// How much more each level may hold than the one above it.
pub const LEVEL_MULTIPLIER: u64 = 10;

/// What a level holds, in the units its allowance is given in.
fn held(version: &Version, level: u32) -> u64 {
    let tables = version.level(level);
    if level == 0 {
        return l0_depth(version);
    }
    tables.iter().map(|t| t.size).sum()
}

/// How deep L0 is: the layers its tables form, which is what a read of it
/// costs (§9, [`Version::l0_sublevels`]).
///
/// Not the number of tables. Ten tables flushed from disjoint key ranges are
/// one layer and one seek, and compacting them is not urgent; ten flushed
/// from the same range are ten seeks, and it is. The thresholds are Pebble's
/// and Pebble counts sublevels.
///
/// A bound that does not decode falls back to the number of tables, which is
/// the upper bound on the layers and the answer this gave before. It cannot
/// happen to a database that opened — the ordering check decodes every bound
/// — and a picker is not where one would want to find out.
pub(crate) fn l0_depth(version: &Version) -> u64 {
    version
        .l0_sublevels()
        .unwrap_or_else(|_| version.level(0).len()) as u64
}

/// A compaction the picker would run.
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub struct Pick {
    /// The level the tables come from.
    pub level: u32,
    /// The tables to compact, by file number.
    pub tables: Vec<u64>,
    /// What the level holds, in the units of its allowance.
    pub held: u64,
    /// What it is allowed to hold.
    pub allowance: u64,
}

impl Pick {
    /// How far past its allowance the level is, for a report to print.
    ///
    /// Never for a decision: those are made by [`Pick::worse_than`], which
    /// compares exactly.
    pub fn score(&self) -> f64 {
        self.held as f64 / self.allowance as f64
    }

    /// Whether this level is further past its allowance than `other`.
    ///
    /// Cross-multiplied rather than divided, so the comparison is exact and
    /// the same two levels compare the same way every time.
    fn worse_than(&self, other: &Pick) -> bool {
        (self.held as u128) * (other.allowance as u128)
            > (other.held as u128) * (self.allowance as u128)
    }
}

impl<F: Fs> Db<F> {
    /// The compaction worth running, if any level is past its allowance.
    ///
    /// Ties go to the level nearest L0, which is where a table being there
    /// costs the most: every read consults all of L0, and only the levels
    /// above the answer below it.
    #[doc(hidden)]
    pub fn pick(&self) -> Option<Pick> {
        let versions = self.version_set();
        let allowance = |level: u32| {
            if level == 0 {
                self.l0_compaction_threshold()
            } else {
                self.lbase_max_bytes()
                    .saturating_mul(LEVEL_MULTIPLIER.saturating_pow(level - 1))
            }
        };

        // L0 at the stop-writes line is not a score to weigh against the
        // others. A write is about to wait for L0 to drain, and no other
        // level this could compact instead would let that write through, so
        // L0 wins outright however far past its allowance anything else is
        // (§9, `crate::open::Db::make_room`).
        //
        // Pebble arrives at the same place from the other direction: a deep
        // L0 buys it *extra concurrent* compactions
        // (`compaction_picker.go`'s `l0ReadAmpCompactions`), so the L0 merge
        // runs alongside whatever was already going rather than instead of
        // it. A store that runs one compaction at a time has no second one
        // to add, so what it can do is make the one it has the L0 one.
        let in_l0 = held(&versions.version, 0);
        if in_l0 >= self.l0_stop_writes_threshold() {
            let tables = inputs_at(&versions.version, 0);
            if !tables.is_empty() {
                return Some(Pick {
                    level: 0,
                    tables,
                    held: in_l0,
                    allowance: allowance(0),
                });
            }
        }

        let mut best: Option<Pick> = None;
        // The last level is not scored: there is nowhere below it to compact
        // into, so saying it is over its allowance would be saying something
        // nothing can act on.
        for level in 0..NUM_LEVELS - 1 {
            let held = held(&versions.version, level);
            let allowance = allowance(level);
            if held < allowance {
                continue;
            }
            let candidate = Pick {
                level,
                tables: inputs_at(&versions.version, level),
                held,
                allowance,
            };
            if candidate.tables.is_empty() {
                continue;
            }
            if best.as_ref().is_none_or(|b| candidate.worse_than(b)) {
                best = Some(candidate);
            }
        }
        best
    }

    /// How many bytes the levels hold above what their allowances let them.
    ///
    /// Pebble's compaction debt: the number that says a
    /// store is falling behind before anything stalls. L0 is counted in
    /// tables rather than bytes, so its excess is charged at what its tables
    /// weigh.
    pub fn compaction_debt(&self) -> u64 {
        let versions = self.version_set();
        let mut debt = 0;
        for level in 0..NUM_LEVELS - 1 {
            let tables = versions.version.level(level);
            if level == 0 {
                // In layers, like the allowance it is weighed against (§9),
                // and charged at what a layer weighs. Counting tables here
                // would report a store as behind for holding ten that a read
                // crosses in one seek.
                let depth = l0_depth(&versions.version);
                let over = depth.saturating_sub(self.l0_compaction_threshold());
                let each = tables
                    .iter()
                    .map(|t| t.size)
                    .sum::<u64>()
                    .checked_div(depth)
                    .unwrap_or(0);
                debt += over * each;
            } else {
                let held: u64 = tables.iter().map(|t| t.size).sum();
                let allowance = self
                    .lbase_max_bytes()
                    .saturating_mul(LEVEL_MULTIPLIER.saturating_pow(level - 1));
                debt += held.saturating_sub(allowance);
            }
        }
        debt
    }

    /// Restates the debt, for whatever is recording.
    pub(crate) fn saw_debt(&self) {
        self.store.metrics.saw_debt(self.compaction_debt());
    }

    /// Runs the compaction the picker chooses, if there is one.
    ///
    /// Returns the output tables' file numbers, empty if nothing was worth
    /// compacting or if the merge dropped everything it read.
    #[doc(hidden)]
    pub async fn compact_once(&self) -> Result<Vec<u64>> {
        self.writable()?;
        let Some(plan) = self.plan_picked().await? else {
            return Ok(Vec::new());
        };
        self.run(plan).await
    }

    /// Plans the compaction [`Db::pick`] chose, if there is one and
    /// nothing else has claimed its tables.
    ///
    /// The picker is not told what is claimed, and so can choose tables a
    /// compaction already has. Standing down rather than waiting is the
    /// point of the narrowing: the caller asking is often a flush, and a
    /// flush that waited for a merge is what §9 set out to stop. The next
    /// flush asks again, against a version that has moved on.
    async fn plan_picked(&self) -> Result<Option<crate::compact::Plan<'_>>> {
        let pick = {
            let _change = self.changes().begin().await;
            self.pick()
        };
        let Some(pick) = pick else {
            return Ok(None);
        };
        self.plan(pick.level, &pick.tables).await
    }

    /// Runs one compaction on the store's own account, if there is one worth
    /// running and the caller did not ask to be left in charge.
    ///
    /// **One compaction, not every one the picker could find.** A flush adds
    /// a single table to L0 and a compaction of L0 takes all of it, so one
    /// per flush more than keeps up with the level a flush fills; below L0
    /// the levels trade off, because the picker's next choice is whichever
    /// is furthest past its allowance. What one per flush deliberately does
    /// *not* do is drain a store that has already fallen behind: a flush
    /// that compacted until nothing was pickable would make its caller wait
    /// out a backlog it did not create. Pebble answers that with its
    /// stop-writes thresholds — a write waits only once the backlog is
    /// dangerous — and roble still owes those (§9).
    ///
    /// **The failure is swallowed**, for the reason the obsolete sweep's is:
    /// the flush that called this is already durable, and calling it failed
    /// because the maintenance behind it did not run would be the larger
    /// lie. Nor is a compaction that keeps failing silent — the debt it is
    /// not paying off is `roble.compaction.debt`, which is the number that
    /// says a store is falling behind.
    pub(crate) async fn maybe_compact(&self) {
        if self.automatic_compactions_disabled() {
            return;
        }
        let _ = self.compact_once().await;
    }
}

/// Which tables of `level` a compaction would take.
fn inputs_at(version: &Version, level: u32) -> Vec<u64> {
    let tables = version.level(level);
    if level == 0 {
        // All of L0. Its tables overlap each other, so compacting one means
        // pulling in every one that meets it, and in L0 that spreads until
        // it has usually taken the lot. Pebble expands from a seed file
        // instead and sometimes stops short; taking all of L0 is the same
        // answer without the bookkeeping, and L0 is bounded by the threshold
        // that got us here.
        return tables.iter().map(|t| t.file_num).collect();
    }
    // One table, the largest. It moves the most bytes down for one merge,
    // and it is a choice that makes progress: the table it picks is gone
    // afterwards, so the next pick is a different one. Pebble rotates
    // through a level by key instead, to spread compactions evenly over the
    // key space; that needs a cursor carried between compactions, and this
    // does not.
    //
    // Ties by file number, so two tables of the same size do not leave the
    // decision to whatever order the version happens to hold them in.
    tables
        .iter()
        .max_by_key(|t| (t.size, std::cmp::Reverse(t.file_num)))
        .map(|t| vec![t.file_num])
        .unwrap_or_default()
}
