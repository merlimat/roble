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

//! What a MANIFEST means once it has been replayed: which table sits at which
//! level, and the counters a store resumes from (DESIGN.md §6).
//!
//! A MANIFEST is a log of edits, not a picture of the database. Nothing in it
//! says "table 6 is in L6"; that is the result of applying every edit in order,
//! where a compaction appears as a table deleted from one level and added to
//! another. Replaying it wrongly does not fail loudly — it produces a
//! plausible database that is missing data or serving stale data — so the
//! replay checks what it can as it goes, and [`VersionSet::replay`] refuses
//! rather than guesses.

use std::collections::BTreeSet;

use roble_format::key::InternalKey;
use roble_format::manifest::{NUM_LEVELS, NewTable, VersionEdit};

use crate::error::{Error, Result};

/// The comparer roble supports, and the only one Oxia leaves behind (§3).
pub const BYTEWISE_COMPARER: &str = "leveldb.BytewiseComparator";

/// One table in a version, as the MANIFEST describes it.
///
/// The bounds are encoded internal keys, kept as Pebble wrote them rather than
/// decoded: they are compared far more often than they are read, and a
/// round-trip through a decoded form is a chance to lose the trailer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    /// Its file number, which is also its name: `NNNNNN.sst`.
    pub file_num: u64,
    /// Its size in bytes.
    pub size: u64,
    /// Its smallest internal key.
    pub smallest: Vec<u8>,
    /// Its largest internal key.
    pub largest: Vec<u8>,
    /// The smallest sequence number it holds.
    pub smallest_seq_num: u64,
    /// The largest sequence number it holds.
    pub largest_seq_num: u64,
    /// Whether a compaction was asked to pick this table up.
    pub marked_for_compaction: bool,
    /// Unix seconds when the table was created, or zero if unrecorded.
    ///
    /// Nothing in roble reads it. It is carried so that rotating the
    /// MANIFEST writes back what Pebble wrote: the snapshot edit a rotation
    /// starts the new file with is built from this, and a field dropped here
    /// is a field Pebble loses the next time roble rotates.
    pub creation_time: u64,
}

impl Table {
    fn from_new(new: &NewTable) -> Self {
        Table {
            file_num: new.file_num,
            size: new.size,
            smallest: new.smallest.clone(),
            largest: new.largest.clone(),
            smallest_seq_num: new.smallest_seq_num,
            largest_seq_num: new.largest_seq_num,
            marked_for_compaction: new.marked_for_compaction,
            creation_time: new.creation_time,
        }
    }

    /// The user key part of its smallest bound.
    pub fn smallest_user_key(&self) -> Result<&[u8]> {
        Ok(InternalKey::decode(&self.smallest)?.user_key)
    }

    /// The user key part of its largest bound.
    pub fn largest_user_key(&self) -> Result<&[u8]> {
        Ok(InternalKey::decode(&self.largest)?.user_key)
    }
}

/// Which tables are in the database, by level.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Version {
    levels: Vec<Vec<Table>>,
}

impl Version {
    /// An empty version: seven levels, no tables.
    pub fn new() -> Self {
        Version {
            levels: vec![Vec::new(); NUM_LEVELS as usize],
        }
    }

    /// The tables at `level`, ordered by file number.
    pub fn level(&self, level: u32) -> &[Table] {
        &self.levels[level as usize]
    }

    /// Every level, L0 first.
    pub fn levels(&self) -> impl Iterator<Item = (u32, &[Table])> {
        self.levels
            .iter()
            .enumerate()
            .map(|(i, tables)| (i as u32, tables.as_slice()))
    }

    /// How many tables the version holds.
    pub fn table_count(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    /// The file numbers of every table, which is what says whether a file on
    /// disk is live.
    pub fn table_file_nums(&self) -> BTreeSet<u64> {
        self.levels
            .iter()
            .flat_map(|tables| tables.iter().map(|t| t.file_num))
            .collect()
    }
}

/// Everything a MANIFEST establishes: the version, and the counters a store
/// resumes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionSet {
    /// The comparer the database was written with, which roble checks before
    /// trusting a single key.
    pub comparer_name: String,
    /// The oldest log whose contents no table holds yet. Logs below it are
    /// obsolete; this one and anything above it has to be replayed.
    pub min_unflushed_log_num: u64,
    /// The next file number to hand out.
    pub next_file_num: u64,
    /// The highest sequence number the database has committed.
    pub last_seq_num: u64,
    /// Which table is where.
    pub version: Version,
}

impl VersionSet {
    /// Replays a MANIFEST's edits, in order, into a version.
    ///
    /// Fails rather than papering over an edit that does not apply: an edit
    /// deleting a table the version does not hold, or adding one it already
    /// does, means either the MANIFEST or this replay is wrong, and both are
    /// reasons to refuse the database rather than open a version that is
    /// missing a table.
    pub fn replay(edits: &[VersionEdit]) -> Result<Self> {
        let mut version = Version::new();
        let mut comparer_name: Option<String> = None;
        let mut min_unflushed_log_num = 0u64;
        let mut next_file_num = 0u64;
        let mut last_seq_num = 0u64;

        for (i, edit) in edits.iter().enumerate() {
            apply_one(
                edit,
                i,
                &mut version,
                &mut comparer_name,
                &mut min_unflushed_log_num,
                &mut next_file_num,
                &mut last_seq_num,
            )?;
        }

        let Some(comparer_name) = comparer_name else {
            return Err(Error::Corrupt(
                "MANIFEST names no comparer, so there is nothing to check the key order against"
                    .to_string(),
            ));
        };

        let set = VersionSet {
            comparer_name,
            min_unflushed_log_num,
            next_file_num,
            last_seq_num,
            version,
        };
        set.check()?;
        Ok(set)
    }

    /// Applies one more edit, as a flush or a compaction produces.
    ///
    /// The same code the replay uses, so an edit cannot mean one thing when
    /// it is read back and another when it is written.
    pub fn apply(&mut self, edit: &VersionEdit) -> Result<()> {
        let mut comparer = Some(self.comparer_name.clone());
        apply_one(
            edit,
            0,
            &mut self.version,
            &mut comparer,
            &mut self.min_unflushed_log_num,
            &mut self.next_file_num,
            &mut self.last_seq_num,
        )?;
        self.comparer_name = comparer.expect("the set already had one");
        self.check()
    }
}

/// Applies one edit to the pieces a version set is made of.
#[allow(clippy::too_many_arguments)]
fn apply_one(
    edit: &VersionEdit,
    i: usize,
    version: &mut Version,
    comparer_name: &mut Option<String>,
    min_unflushed_log_num: &mut u64,
    next_file_num: &mut u64,
    last_seq_num: &mut u64,
) -> Result<()> {
    let at = |m: String| Error::Corrupt(format!("version edit {i}: {m}"));

    if let Some(name) = &edit.comparer_name {
        // Pebble writes the comparer once, in the first edit of a
        // MANIFEST. A second, different one would mean the database
        // changed key encoding underneath itself.
        if let Some(held) = &comparer_name
            && held != name
        {
            return Err(at(format!(
                "comparer changes from {held:?} to {name:?} mid-MANIFEST"
            )));
        }
        *comparer_name = Some(name.clone());
    }
    if let Some(n) = edit.min_unflushed_log_num {
        *min_unflushed_log_num = n;
    }
    if let Some(n) = edit.next_file_num {
        *next_file_num = n;
    }
    if let Some(n) = edit.last_seq_num {
        *last_seq_num = n;
    }

    // Deletions first, which is the order Pebble's own
    // `BulkVersionEdit` accumulates them in.
    //
    // It makes no difference to any edit Pebble writes today: a
    // compaction moves a table by deleting it from one level and
    // adding it to another, and the duplicate check below is
    // per-level, so either order gives the same version. Swapping the
    // two loops breaks no test here, and saying otherwise would be a
    // claim nothing checks. The order is kept because it is the one
    // that stays correct if an edit ever does replace a table within a
    // level.
    for &(level, file_num) in &edit.deleted_tables {
        let tables = &mut version.levels[level as usize];
        let Some(pos) = tables.iter().position(|t| t.file_num == file_num) else {
            return Err(at(format!(
                "deletes table {file_num} from L{level}, which does not hold it"
            )));
        };
        tables.remove(pos);
    }
    for new in &edit.new_tables {
        let tables = &mut version.levels[new.level as usize];
        if tables.iter().any(|t| t.file_num == new.file_num) {
            return Err(at(format!(
                "adds table {} to L{}, which already holds it",
                new.file_num, new.level
            )));
        }
        // By file number, so a version has one order however the
        // edits arrived.
        let table = Table::from_new(new);
        let pos = tables
            .iter()
            .position(|t| t.file_num > table.file_num)
            .unwrap_or(tables.len());
        tables.insert(pos, table);
    }
    Ok(())
}

impl VersionSet {
    /// Checks the invariants a replayed version has to satisfy.
    ///
    /// These are Pebble's guarantees, not roble's preferences, which makes
    /// them a test of the replay: if deletions and additions were applied
    /// wrongly, the most likely result is two tables overlapping in a level
    /// below L0, where Pebble never leaves any.
    fn check(&self) -> Result<()> {
        if self.comparer_name != BYTEWISE_COMPARER {
            return Err(Error::Unsupported(format!(
                "database was written with comparer {:?}; roble reads only {BYTEWISE_COMPARER}",
                self.comparer_name
            )));
        }

        // L0 may overlap, by design: it holds whole flushed memtables.
        for level in 1..NUM_LEVELS {
            // Decoded up front rather than inside a comparison, so a
            // malformed bound is one error here instead of an ordering that
            // quietly depends on which pairs got compared.
            let mut bounds = Vec::new();
            for t in self.version.level(level) {
                bounds.push((
                    t.smallest_user_key()?.to_vec(),
                    t.largest_user_key()?.to_vec(),
                    t.file_num,
                ));
            }
            bounds.sort();
            for pair in bounds.windows(2) {
                let ((_, before_largest, before), (after_smallest, _, after)) =
                    (&pair[0], &pair[1]);
                if before_largest >= after_smallest {
                    return Err(Error::Corrupt(format!(
                        "L{level} tables {before} and {after} overlap, which Pebble never \
                         leaves below L0"
                    )));
                }
            }
        }

        // A table's own bounds have to agree with the sequence numbers beside
        // them, or one of the two was decoded wrongly.
        for (level, tables) in self.version.levels() {
            for t in tables {
                if t.smallest_seq_num > t.largest_seq_num {
                    return Err(Error::Corrupt(format!(
                        "L{level} table {}: sequence numbers {}..{} run backwards",
                        t.file_num, t.smallest_seq_num, t.largest_seq_num
                    )));
                }
                if t.largest_seq_num > self.last_seq_num {
                    return Err(Error::Corrupt(format!(
                        "L{level} table {}: holds sequence number {}, above the MANIFEST's last {}",
                        t.file_num, t.largest_seq_num, self.last_seq_num
                    )));
                }
            }
        }
        Ok(())
    }
}
