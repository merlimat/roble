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
use roble_format::rangedel::{self, Tombstone};
use roble_format::sst::{TableWriter, WriterOptions};
use roble_format::{InternalKey, key};

use crate::buf::WriteBuf;
use crate::change::Claim;
use crate::env::{Disk, Fs};
use crate::error::{Error, Result};
use crate::flush::Bounds;
use crate::open::Opened;
use crate::table::{OpenTable, is_deletion};
use crate::version::Table as TableMeta;

/// How large a table a compaction into L0 aims to write.
///
/// Pebble's `TargetFileSizes[0]`, with each level below it twice the one
/// above.
pub const TARGET_FILE_SIZE: u64 = 2 << 20;

/// What a compaction has decided to read, with those tables claimed.
///
/// Made under the permit and carried out without it, which is what lets a
/// merge run while a flush gets on with its own work (§9). Dropping it
/// releases the claim, so a compaction that was never run, or that failed
/// half way, leaves the picker free to choose those tables again.
pub(crate) struct Plan<'a> {
    output_level: u32,
    inputs: Vec<(u32, TableMeta)>,
    below: Vec<(Vec<u8>, Vec<u8>)>,
    _claim: Claim<'a>,
}

impl<F: Fs> Opened<F> {
    /// Compacts the named tables of `level` into `level + 1`.
    ///
    /// Every table of `level + 1` whose key range meets theirs joins them, or
    /// the result would overlap a table already there and Pebble leaves no
    /// overlap below L0.
    ///
    /// Returns the output tables' file numbers, in key order. Empty when the
    /// merge dropped everything it read, which is what a level of nothing
    /// but superseded keys compacts to.
    pub async fn compact(&self, level: u32, file_nums: &[u64]) -> Result<Vec<u64>> {
        let Some(plan) = self.plan(level, file_nums).await? else {
            return Err(Error::Unsupported(format!(
                "{file_nums:?} are already being compacted out of L{level}"
            )));
        };
        self.run(plan).await
    }

    /// Decides what a compaction of `file_nums` out of L`level` will read,
    /// claims it, and lets the permit go — `None` if something else has
    /// claimed one of them already.
    ///
    /// The permit is held for this and not for the merge that follows, which
    /// is the whole of §9's narrowing: deciding reads the version and so has
    /// to be alone, and merging reads tables nothing else may touch, which a
    /// claim says just as well and without stopping a flush
    /// (`crate::change::Compacting`).
    pub(crate) async fn plan(&self, level: u32, file_nums: &[u64]) -> Result<Option<Plan<'_>>> {
        let _change = self.changes().begin().await;

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
        let wanted: Vec<u64> = inputs.iter().map(|(_, m)| m.file_num).collect();
        let Some(claim) = self.compacting().claim(&wanted) else {
            return Ok(None);
        };
        Ok(Some(Plan {
            output_level,
            inputs,
            below,
            _claim: claim,
        }))
    }

    /// Carries out `plan`, and takes account of it having failed.
    ///
    /// A compaction often has nobody to hand an error to: a flush swallows
    /// the one it scheduled, and a task running one in the background was
    /// told to by a flush that has already returned (§9). So a failure is
    /// said twice over — in a counter an operator graphs and a line an
    /// operator reads — and is then left alone until the version changes,
    /// because the picker would give the same answer against the same
    /// version and asking again at once is a spin rather than a retry.
    pub(crate) async fn run(&self, plan: Plan<'_>) -> Result<Vec<u64>> {
        let result = self
            .metrics
            .compaction_errors
            .saw(self.run_inner(plan).await);
        if let Err(e) = &result {
            tracing::warn!(
                store = %self.dir.display(),
                error = %e,
                "compaction failed"
            );
            self.saw_compaction_failure();
        }
        result
    }

    /// The compaction proper: the merge with no permit held, then the
    /// outputs and the edit that claims them with it.
    async fn run_inner(&self, plan: Plan<'_>) -> Result<Vec<u64>> {
        let _timer = self.metrics.compaction.start();
        let Plan {
            output_level,
            inputs,
            below,
            ..
        } = &plan;
        let output_level = *output_level;

        self.metrics.compaction_tables[0].add(inputs.len() as u64);
        self.metrics.compaction_io[0].add(inputs.iter().map(|(_, m)| m.size).sum());
        let mut tables = Vec::new();
        for (_, meta) in inputs {
            tables.push(self.open_table(meta.file_num).await?);
        }

        // The merge, with no permit held: it is the long part of a compaction
        // and the part a flush must not wait behind (§9). Three things stand
        // in for the permit here. The claim `plan` took keeps another
        // compaction off these tables; they are still the version's, so the
        // sweep does not count them obsolete; and a reader is open for each,
        // so it could not collect them even if it did (§5,
        // `crate::obsolete`).
        //
        // `below` was read before the merge and is used for the deletion
        // rule, which is sound because it can only be read too wide: a
        // compaction that runs meanwhile moves keys down, and the keys any
        // level below holds are the same ones or fewer. Too wide keeps a
        // tombstone that could have gone, which costs a little space; too
        // narrow would drop one that was still hiding something.
        let merged = merge(
            &tables,
            below,
            self.target_file_size(output_level),
            self.compaction_unit(),
        )
        .await?;
        // Read, and done with. The readers are dropped here rather than at
        // the end of the function because the sweep below cannot collect a
        // table something still has open, and these are its inputs (§5,
        // `crate::obsolete`).
        drop(tables);

        // The permit again, for all of what is left, because all of what is
        // left changes what the store is made of. The numbers the outputs
        // take are read from the version and spent by the edit, so anything
        // in between that read the same version would read the same numbers
        // — which is why this is one hold and not two.
        let change = self.changes().begin().await;

        // Every output written and made durable before the edit that claims
        // any of them: a crash in the middle leaves files nothing refers to,
        // which the sweep collects, and the database exactly as it was.
        let first_file_num = self.versions().next_file_num;
        let mut new_tables = Vec::with_capacity(merged.len());
        let mut written: Vec<(u64, u64)> = Vec::new();
        for (i, (bytes, bounds, entries)) in merged.into_iter().enumerate() {
            let file_num = first_file_num + i as u64;
            written.push((file_num, entries));
            let mut new_table = bounds.into_new_table(output_level, file_num)?;
            new_table.size = bytes.len() as u64;
            self.metrics.compaction_tables[1].add(1);
            self.metrics.compaction_io[1].add(new_table.size);
            self.write_table(file_num, bytes).await?;
            new_tables.push(new_table);
        }

        // Nothing survived? The inputs still go: an edit that only deletes is
        // as much a compaction as one that writes.
        let edit = VersionEdit {
            comparer_name: None,
            min_unflushed_log_num: None,
            obsolete_prev_log_num: None,
            next_file_num: (!new_tables.is_empty())
                .then(|| first_file_num + new_tables.len() as u64),
            last_seq_num: Some(self.last_seq_num()),
            deleted_tables: inputs.iter().map(|(l, m)| (*l, m.file_num)).collect(),
            new_tables,
        };

        // The one-way door, in the same place a flush puts it: before this
        // the output is a file nothing refers to and the inputs are still the
        // database's.
        let manifest_bytes = self.append_version_edit(&edit, &change).await?;
        self.versions_mut().apply(&edit)?;
        // More may be worth compacting now, and whoever asked to be told is
        // told here rather than waiting for the next flush (§9).
        let _ = self.version_changed();
        if manifest_bytes >= self.max_manifest_bytes() {
            let _ = self.rotate_inner(&change).await;
        }
        let _ = self.sweep(&change).await;
        self.metrics.saw_version(&self.versions().version);
        self.measure_disk().await;
        self.metrics.compactions.add(1);
        self.saw_debt();
        for (file_num, entries) in written {
            self.saw_table_entries(file_num, Some(entries));
        }
        self.recount_records();

        Ok(edit.new_tables.iter().map(|t| t.file_num).collect())
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
        let written = bytes.len();
        let (result, _) = disk
            .write(WriteBuf::new(bytes), 0, Default::default())
            .await;
        result?;
        self.metrics.written.add(written);
        {
            let _timer = self.metrics.sync_duration.start();
            disk.sync(Default::default()).await?;
        }
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
    target_bytes: u64,
    unit: u64,
) -> Result<Vec<(Vec<u8>, Bounds, u64)>> {
    // The thread goes back to whatever else is ready every `unit` entries,
    // so a merge of a million keys is a million keys' worth of work rather
    // than a million keys' worth of waiting for everyone else (§9).
    let mut pacer = crate::work::Pacer::new(unit);
    // Discarding, not keeping: a compaction reads every block of its inputs
    // once and never comes back, so filling the block cache with them would
    // evict what the foreground is reading and replace it with bytes nobody
    // will ask for again (`crate::table::Caching`).
    let mut iters = Vec::new();
    for t in tables {
        let mut it = t.iter_with(crate::table::Caching::Discard);
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

    let mut outputs = Vec::new();
    let mut open = Output::new(None)?;
    // Reused for every entry that survives the merge. A compaction reads
    // millions of them and already holds the encoded key it is writing, so
    // taking an owned entry per survivor meant allocating that key again,
    // and the value again, for each one.
    let mut value = Vec::new();
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
            // The break comes *before* the key rather than after, so the
            // span that closes ends at a key the next table holds and the
            // two tables meet rather than overlap. Only a user key the
            // output has not started yet may be a boundary, and after the
            // drops above every surviving user key appears exactly once, so
            // any of them will do.
            if open.estimate >= target_bytes && !open.is_empty() {
                let finished = std::mem::replace(&mut open, Output::new(Some(user.clone()))?);
                outputs.push(finished.finish(&tombstones, Some(&user))?);
            }
            iters[w].value_into(&mut value).await?;
            open.add(k, &encoded, &value)?;
        }
        iters[w].next().await?;
        pacer.did(1).await;
    }

    outputs.push(open.finish(&tombstones, None)?);
    Ok(outputs.into_iter().flatten().collect())
}

/// One table a compaction is building, and the span of keys it owns.
///
/// The span is what the tombstones are clipped to, and it is half-open: from
/// the key this table started at, up to but not including the key the next
/// one starts at. The first table's span has no lower bound and the last
/// one's no upper.
struct Output {
    writer: TableWriter,
    bounds: Bounds,
    start: Option<Vec<u8>>,
    /// Roughly how many bytes of keys and values have gone in, which is what
    /// the target is compared against. The table itself is smaller once its
    /// blocks are built, and larger by its index and filter; an estimate is
    /// what Pebble splits on too.
    estimate: u64,
    points: usize,
}

impl Output {
    fn new(start: Option<Vec<u8>>) -> Result<Self> {
        Ok(Output {
            writer: TableWriter::new(WriterOptions::default())?,
            bounds: Bounds::default(),
            start,
            estimate: 0,
            points: 0,
        })
    }

    fn is_empty(&self) -> bool {
        self.points == 0
    }

    fn add(&mut self, key: InternalKey<'_>, encoded: &[u8], value: &[u8]) -> Result<()> {
        self.estimate += (encoded.len() + value.len()) as u64;
        self.points += 1;
        self.bounds.saw_key(encoded)?;
        self.bounds.saw_seq(key.seq_num);
        self.writer.add(key, value)?;
        Ok(())
    }

    /// Writes the tombstones this table owns and finishes it.
    ///
    /// A tombstone that straddles the boundary is clipped to it, which is
    /// what makes the two tables meet at one user key rather than overlap
    /// across it: this table's largest key becomes the exclusive sentinel at
    /// `end`, and the next table's smallest is `end` itself.
    fn finish(
        mut self,
        tombstones: &[Tombstone],
        end: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, Bounds, u64)>> {
        let mut wrote = self.points > 0;
        for t in tombstones {
            let Some(clipped) = clip(t, self.start.as_deref(), end) else {
                continue;
            };
            self.writer
                .add_range_deletion(&clipped.start, &clipped.end, clipped.seq_num)?;
            self.bounds.saw_seq(clipped.seq_num);
            self.bounds.saw_key(&key::encode(
                &clipped.start,
                clipped.seq_num,
                key::Kind::RangeDelete,
            ))?;
            self.bounds.saw_key(&key::encode(
                &clipped.end,
                key::SEQ_NUM_MAX,
                key::Kind::RangeDelete,
            ))?;
            wrote = true;
        }
        if !wrote {
            return Ok(None);
        }
        let entries = self.points as u64;
        Ok(Some((self.writer.finish()?, self.bounds, entries)))
    }
}

/// A tombstone cut down to `[lo, hi)`, or `None` if nothing of it is left.
fn clip(t: &Tombstone, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Option<Tombstone> {
    let start = match lo {
        Some(lo) if lo > t.start.as_slice() => lo,
        _ => &t.start,
    };
    let end = match hi {
        Some(hi) if hi < t.end.as_slice() => hi,
        _ => &t.end,
    };
    if start >= end {
        return None;
    }
    Some(Tombstone {
        start: start.to_vec(),
        end: end.to_vec(),
        seq_num: t.seq_num,
    })
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

/// Every input table's tombstones, fragmented into the shape a
/// range-deletion block demands.
///
/// Two inputs' tombstones routinely overlap, so concatenating them would
/// build a block the format refuses — and rightly, since a reader walking it
/// would have to decide what an overlap meant. The fragmenting itself is the
/// format crate's, which is also what refuses the unfragmented form.
fn fragment<D: Disk>(tables: &[Rc<OpenTable<D>>]) -> Vec<Tombstone> {
    let all: Vec<&Tombstone> = tables.iter().flat_map(|t| t.tombstones.iter()).collect();
    rangedel::fragment(&all)
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
