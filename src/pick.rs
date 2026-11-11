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
//! **Two things it does not do**, and both are §9 asking rather than an
//! oversight. It reads no clock and draws no random number: a picker that did
//! either would make a seeded run stop replaying, and replay is how the
//! failure-path tests work at all (§10, §12). And it does not *schedule*
//! itself — nothing here runs a compaction behind the caller's back. Wiring
//! this into a flush waits for the cooperative execution §9 describes,
//! because a compaction that runs to completion inside a flush is exactly the
//! latency spike units of work exist to avoid.
//!
//! **The score is a fraction, not a float.** Two levels are compared by
//! cross-multiplying, which is exact; a float would be deterministic in
//! practice and unprovable in principle, and this is the one component whose
//! whole job is to make the same decision twice.

use roble_format::manifest::NUM_LEVELS;

use crate::env::Fs;
use crate::error::Result;
use crate::open::Opened;
use crate::version::Version;

/// How many tables L0 may hold before it is worth compacting.
///
/// Pebble's `L0CompactionThreshold`. L0 is counted rather than measured
/// because its tables overlap: what makes L0 expensive is that a read
/// consults every one of them, whatever they weigh.
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
        return tables.len() as u64;
    }
    tables.iter().map(|t| t.size).sum()
}

/// A compaction the picker would run.
#[derive(Clone, Debug, PartialEq, Eq)]
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

impl<F: Fs> Opened<'_, F> {
    /// The compaction worth running, if any level is past its allowance.
    ///
    /// Ties go to the level nearest L0, which is where a table being there
    /// costs the most: every read consults all of L0, and only the levels
    /// above the answer below it.
    pub fn pick(&self) -> Option<Pick> {
        let versions = self.versions();
        let allowance = |level: u32| {
            if level == 0 {
                self.l0_compaction_threshold()
            } else {
                self.lbase_max_bytes()
                    .saturating_mul(LEVEL_MULTIPLIER.saturating_pow(level - 1))
            }
        };
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

    /// Runs the compaction [`Opened::pick`] chose, if there is one.
    ///
    /// Returns the output table's file number, `None` if nothing was worth
    /// compacting or if the merge dropped everything it read.
    pub async fn compact_once(&self) -> Result<Option<u64>> {
        let Some(pick) = self.pick() else {
            return Ok(None);
        };
        self.compact(pick.level, &pick.tables).await
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
