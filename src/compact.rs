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

//! Compacting tables of one level into the level below (§9).
//!
//! A flush only adds: every memtable becomes another table in L0, and reads
//! get slower as they pile up. Compaction is what takes away — it merges
//! tables together, keeps the newest version of each key and drops the rest,
//! and leaves one non-overlapping run in the level below.
//!
//! This is the mechanism, driven by its caller. **Which** tables to compact
//! and **when** is the picker's business (§9) and is not here: compatibility
//! constrains the files a compaction produces, not when it runs, so the two
//! are worth separating and the mechanism is the half with the risk in it.
//!
//! **What may be dropped**, and why each is safe:
//!
//! - **Every version of a key but the newest.** Nothing can ask for an older
//!   one. A read starts from the version, and a scan that was already open
//!   holds `Rc`s to the tables it was made with — it goes on reading those,
//!   whatever this compaction writes or the sweep afterwards deletes. That is
//!   why roble needs no list of registered snapshots to compact against: a
//!   scan already *is* the version it started on.
//! - **A key a newer range tombstone covers**, because every input's
//!   tombstones are carried into the output, so the thing that hid the key
//!   is still there to hide whatever is below.
//! - **A deletion with nothing below it.** A tombstone exists to hide an
//!   older version; once no level below the output holds the key, there is
//!   nothing left for it to hide and it is only cost. This is the one rule
//!   that depends on the output level, and getting it wrong resurrects
//!   deleted keys, so it asks the version rather than guessing from the
//!   level number.
//!
//! What is deliberately **not** dropped yet: a range tombstone itself, which
//! could go once nothing below its range survives, and which stays instead.
//! Carrying one that has nothing left to delete costs a reader a comparison;
//! dropping one that still has something to delete brings a key back from the
//! dead. The second mistake is the one worth not making first.
//!
//! The output is a single table. Splitting it at a target size belongs with
//! the picker, which is the part that has opinions about how large a file
//! should be.

use std::collections::BTreeSet;
use std::rc::Rc;

use roble_format::manifest::{NUM_LEVELS, VersionEdit};
use roble_format::rangedel::Tombstone;
use roble_format::sst::{TableWriter, WriterOptions};
use roble_format::{InternalKey, key};

use crate::buf::WriteBuf;
use crate::env::{Disk, Fs};
use crate::error::{Error, Result};
use crate::flush::Bounds;
use crate::open::Opened;
use crate::table::{OpenTable, is_deletion};
use crate::version::Table as TableMeta;

impl<F: Fs> Opened<'_, F> {
    /// Compacts the named tables of `level` into `level + 1`.
    ///
    /// Every table of `level + 1` whose key range meets theirs joins them, or
    /// the result would overlap a table already there and Pebble leaves no
    /// overlap below L0.
    ///
    /// Returns the output table's file number, or `None` when the merge
    /// dropped everything it read, which is what a level of nothing but
    /// superseded keys compacts to.
    pub async fn compact(&self, level: u32, file_nums: &[u64]) -> Result<Option<u64>> {
        let output_level = level + 1;
        if output_level >= NUM_LEVELS {
            return Err(Error::Unsupported(format!(
                "L{level} is the last level, so there is nowhere to compact it into"
            )));
        }
        if file_nums.is_empty() {
            return Err(Error::Unsupported(
                "a compaction of no tables has nothing to do".to_string(),
            ));
        }

        let (inputs, below) = self.inputs_for(level, output_level, file_nums)?;
        let mut tables = Vec::new();
        for (_, meta) in &inputs {
            tables.push(self.open_table(meta.file_num).await?);
        }

        let file_num = self.versions().next_file_num;
        let merged = merge(&tables, &below).await?;

        let edit = match merged {
            Some((bytes, bounds)) => {
                let mut new_table = bounds.into_new_table(output_level, file_num)?;
                new_table.size = bytes.len() as u64;
                self.write_table(file_num, bytes).await?;
                VersionEdit {
                    comparer_name: None,
                    min_unflushed_log_num: None,
                    obsolete_prev_log_num: None,
                    next_file_num: Some(file_num + 1),
                    last_seq_num: Some(self.snapshot()),
                    deleted_tables: inputs.iter().map(|(l, m)| (*l, m.file_num)).collect(),
                    new_tables: vec![new_table],
                }
            }
            // Nothing survived. The inputs still go: an edit that only
            // deletes is as much a compaction as one that writes.
            None => VersionEdit {
                comparer_name: None,
                min_unflushed_log_num: None,
                obsolete_prev_log_num: None,
                next_file_num: None,
                last_seq_num: Some(self.snapshot()),
                deleted_tables: inputs.iter().map(|(l, m)| (*l, m.file_num)).collect(),
                new_tables: Vec::new(),
            },
        };

        // The one-way door, in the same place a flush puts it: before this
        // the output is a file nothing refers to and the inputs are still the
        // database's.
        let manifest_bytes = self.append_version_edit(&edit).await?;
        self.versions_mut().apply(&edit)?;
        if manifest_bytes >= self.max_manifest_bytes() {
            let _ = self.rotate_manifest().await;
        }
        let _ = self.remove_obsolete_files().await;

        Ok(edit.new_tables.first().map(|t| t.file_num))
    }

    /// The tables a compaction reads, and the key ranges below it.
    ///
    /// Returned together because both are read off the same borrow of the
    /// version, and both have to describe the same one: a compaction that
    /// took its inputs from one version and its "is anything below?" answer
    /// from another could drop a tombstone that something below still needs.
    #[allow(clippy::type_complexity)]
    fn inputs_for(
        &self,
        level: u32,
        output_level: u32,
        file_nums: &[u64],
    ) -> Result<(Vec<(u32, TableMeta)>, Vec<(Vec<u8>, Vec<u8>)>)> {
        let versions = self.versions();
        let wanted: BTreeSet<u64> = file_nums.iter().copied().collect();

        let mut inputs: Vec<(u32, TableMeta)> = Vec::new();
        for t in versions.version.level(level) {
            if wanted.contains(&t.file_num) {
                inputs.push((level, t.clone()));
            }
        }
        if inputs.len() != wanted.len() {
            let found: BTreeSet<u64> = inputs.iter().map(|(_, t)| t.file_num).collect();
            let missing: Vec<u64> = wanted.difference(&found).copied().collect();
            return Err(Error::Unsupported(format!(
                "L{level} does not hold {missing:?}, so they cannot be compacted out of it"
            )));
        }

        // The range the inputs cover, which is what decides who joins them.
        let mut lowest = inputs[0].1.smallest_user_key()?.to_vec();
        let mut highest = inputs[0].1.largest_user_key()?.to_vec();
        for (_, t) in &inputs {
            lowest = lowest.min(t.smallest_user_key()?.to_vec());
            highest = highest.max(t.largest_user_key()?.to_vec());
        }
        for t in versions.version.level(output_level) {
            if t.smallest_user_key()? <= highest.as_slice()
                && t.largest_user_key()? >= lowest.as_slice()
            {
                inputs.push((output_level, t.clone()));
            }
        }

        // What is below the output, for the deletion rule. The inputs at the
        // output level are not below it — they are being rewritten — and
        // every level above holds newer data, which a tombstone down here
        // was never hiding.
        let mut below = Vec::new();
        for (l, tables) in versions.version.levels() {
            if l <= output_level {
                continue;
            }
            for t in tables {
                below.push((
                    t.smallest_user_key()?.to_vec(),
                    t.largest_user_key()?.to_vec(),
                ));
            }
        }
        Ok((inputs, below))
    }

    /// Writes the output table and makes its name durable.
    async fn write_table(&self, file_num: u64, bytes: Vec<u8>) -> Result<()> {
        let path = self.dir.join(roble_format::files::table_filename(file_num));
        let disk = self.fs().create(&path).await?;
        let (result, _) = disk
            .write(WriteBuf::new(bytes), 0, Default::default())
            .await;
        result?;
        disk.sync(Default::default()).await?;
        disk.close().await?;
        self.fs().sync_dir(&self.dir).await?;
        Ok(())
    }
}

/// Merges every input into one table's bytes, dropping what may go.
///
/// Returns `None` when nothing survived, which is not an error: a level of
/// superseded keys and spent tombstones compacts to nothing at all.
async fn merge<D: Disk>(
    tables: &[Rc<OpenTable<D>>],
    below: &[(Vec<u8>, Vec<u8>)],
) -> Result<Option<(Vec<u8>, Bounds)>> {
    let mut writer = TableWriter::new(WriterOptions::default())?;
    let mut bounds = Bounds::default();
    let mut wrote = false;

    let mut iters = Vec::new();
    for t in tables {
        let mut it = t.iter();
        it.first().await?;
        iters.push(it);
    }

    // Every input's tombstones, carried whole. They are what makes dropping
    // a covered key safe, so they are gathered before any key is decided
    // about — not because the writer needs them first, but because the rule
    // below is only true if they are going.
    //
    // Fragmented, because a range-deletion block holds fragments: tombstones
    // sorted by start, never overlapping unless they share both bounds. Two
    // inputs' tombstones routinely overlap at neither, so concatenating them
    // would build a block the format refuses — and rightly, since a reader
    // walking it would have to decide what an overlap meant.
    let tombstones = fragment(tables);

    let mut seen: Option<Vec<u8>> = None;
    while let Some(w) = least(&iters)? {
        let encoded = iters[w].key().to_vec();
        let k = InternalKey::decode(&encoded)?;
        let user = k.user_key.to_vec();

        // The first version met for a user key is its newest, since trailers
        // descend. Every one after it is superseded and goes.
        let newest = seen.as_deref() != Some(user.as_slice());
        seen = Some(user.clone());

        if newest && keep(&user, k.seq_num, k.kind, &tombstones, below) {
            let entry = iters[w].entry().await?;
            writer.add(k, &entry.value)?;
            bounds.saw_key(&encoded)?;
            bounds.saw_seq(k.seq_num);
            wrote = true;
        }
        iters[w].next().await?;
    }

    for t in &tombstones {
        writer.add_range_deletion(&t.start, &t.end, t.seq_num)?;
        bounds.saw_seq(t.seq_num);
        bounds.saw_key(&key::encode(&t.start, t.seq_num, key::Kind::RangeDelete))?;
        bounds.saw_key(&key::encode(
            &t.end,
            key::SEQ_NUM_MAX,
            key::Kind::RangeDelete,
        ))?;
        wrote = true;
    }

    if !wrote {
        return Ok(None);
    }
    Ok(Some((writer.finish()?, bounds)))
}

/// Whether the newest version of a key survives the compaction.
fn keep(
    user: &[u8],
    seq: u64,
    kind: key::Kind,
    tombstones: &[Tombstone],
    below: &[(Vec<u8>, Vec<u8>)],
) -> bool {
    // A newer range tombstone hides it, and that tombstone is going into the
    // output, so it will go on hiding whatever is below.
    if tombstones
        .iter()
        .any(|t| t.seq_num > seq && t.start.as_slice() <= user && user < t.end.as_slice())
    {
        return false;
    }
    // A deletion with nothing below it has nothing left to hide. The test is
    // on the version rather than on the level number: a table two levels down
    // whose range covers this key is a reason to keep the tombstone, and the
    // output level being the last one with tables is not.
    if is_deletion(kind)
        && !below
            .iter()
            .any(|(lo, hi)| lo.as_slice() <= user && user <= hi.as_slice())
    {
        return false;
    }
    true
}

/// Splits every input's tombstones into fragments a range-deletion block can
/// hold.
///
/// The bounds of every tombstone, sorted and deduplicated, cut the key space
/// into spans that no tombstone straddles. Each span is then emitted once per
/// tombstone covering it, newest first — which is exactly the shape the block
/// demands: equal bounds, strictly decreasing sequence numbers.
fn fragment<D: Disk>(tables: &[Rc<OpenTable<D>>]) -> Vec<Tombstone> {
    let all: Vec<&Tombstone> = tables.iter().flat_map(|t| t.tombstones.iter()).collect();
    let mut cuts: Vec<&[u8]> = Vec::with_capacity(all.len() * 2);
    for t in &all {
        cuts.push(&t.start);
        cuts.push(&t.end);
    }
    cuts.sort_unstable();
    cuts.dedup();

    let mut out = Vec::new();
    for pair in cuts.windows(2) {
        let (lo, hi) = (pair[0], pair[1]);
        let mut covering: Vec<u64> = all
            .iter()
            .filter(|t| t.start.as_slice() <= lo && hi <= t.end.as_slice())
            .map(|t| t.seq_num)
            .collect();
        covering.sort_unstable_by(|a, b| b.cmp(a));
        for seq_num in covering {
            out.push(Tombstone {
                start: lo.to_vec(),
                end: hi.to_vec(),
                seq_num,
            });
        }
    }
    out
}

/// The iterator on the least internal key, if any is still positioned.
fn least<D: Disk>(iters: &[crate::table::TableIter<D>]) -> Result<Option<usize>> {
    let mut best: Option<usize> = None;
    for (i, it) in iters.iter().enumerate() {
        if !it.is_valid() {
            continue;
        }
        match best {
            None => best = Some(i),
            Some(b) => {
                if key::compare(it.key(), iters[b].key())?.is_lt() {
                    best = Some(i);
                }
            }
        }
    }
    Ok(best)
}
