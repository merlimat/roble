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
//! **Where the output goes.** A table's bytes are written as the writer
//! finishes each block, and the table the moment the merge has finished it
//! rather than when the merge has finished: nothing accumulates, so
//! what a compaction holds is one output table and not all of them. That
//! matters because the one job this has no bound on is L0's — the picker
//! takes all of L0 and every L0 table whose range meets it below, so the
//! output is L0's depth times a memtable (§9, `crate::pick`) — and because
//! roble's first consumer runs hundreds of stores on one node, where a
//! transient per-store peak is multiplied by hundreds. Pebble goes a layer
//! further and writes each *block* as it fills, so what it holds is one
//! block and one index; roble's `TableWriter` builds a whole table's bytes,
//! which is what bounds this at the target file size rather than at a block.
//!
//! The split points are the target size's, and the sizes themselves are the
//! picker's business (§9): this is the part that cannot write a table
//! straddling a boundary, not the part with opinions about how large a file
//! should be.

use std::collections::BTreeSet;
use std::rc::Rc;

use roble_format::block::Codec;
use roble_format::manifest::{NUM_LEVELS, VersionEdit};
use roble_format::rangedel::{self, Tombstone};
use roble_format::sst::{TableWriter, WriterOptions};
use roble_format::{InternalKey, key};

use crate::buf::WriteBuf;
use crate::change::Claim;
use crate::env::{Disk, Fs};
use crate::error::{Error, Result};
use crate::flush::Bounds;
use crate::open::Db;
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

impl<F: Fs> Db<F> {
    /// Compacts the named tables of `level` into `level + 1`.
    ///
    /// Every table of `level + 1` whose key range meets theirs joins them, or
    /// the result would overlap a table already there and Pebble leaves no
    /// overlap below L0.
    ///
    /// Returns the output tables' file numbers, in key order. Empty when the
    /// merge dropped everything it read, which is what a level of nothing
    /// but superseded keys compacts to.
    ///
    /// Takes the tables of `level` it is given and no others, so a pick of
    /// some of L0 can leave behind an older table that overlaps them and put
    /// newer keys below it. The store's own compactions never do — they take
    /// L0 whole — and nor does Pebble, whose reads, like a scan's skip past
    /// a range tombstone here (`crate::scan`), take a deeper level's keys to
    /// be older.
    #[doc(hidden)]
    pub async fn compact(&self, level: u32, file_nums: &[u64]) -> Result<Vec<u64>> {
        self.writable()?;
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
            .store
            .metrics
            .compaction_errors
            .saw(self.run_inner(plan).await);
        if let Err(e) = &result {
            tracing::warn!(
                store = %self.store.dir.display(),
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
        // A store whose MANIFEST failed records no more (`crate::flush`), so
        // a merge it could not record is not started — and refused here
        // rather than in `compact_once`, so that `run` counts it as failed
        // and the merge task waits for the version to move instead of
        // asking again at once.
        self.recording()?;
        let _timer = self.store.metrics.compaction.start();
        let Plan {
            output_level,
            inputs,
            below,
            ..
        } = &plan;
        let output_level = *output_level;

        self.store.metrics.compaction_tables[0].add(inputs.len() as u64);
        self.store.metrics.compaction_io[0].add(inputs.iter().map(|(_, m)| m.size).sum());
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
        //
        // Every output is written and made durable here, as the merge
        // finishes it, and still before the edit that claims any of them: a
        // crash in the middle leaves files nothing refers to, which the
        // sweep collects, and the database exactly as it was. Streaming
        // them does not weaken that — the edit is after the merge, so it is
        // after every write — but it does mean the files are on disk while
        // another change can be underway, which is what `writing` is for.
        let mut new_tables = Vec::new();
        let mut written: Vec<(u64, u64)> = Vec::new();
        let mut writing = self.pending().writing();
        // The output the merge is part way through: its number, its file, and
        // how much of it is on disk. `None` until the first chunk of one
        // arrives, so a merge that drops everything it read spends no number
        // and leaves no file. Only ever one at a time — the merge finishes an
        // output before it starts the next, so the `Last` that closes this
        // file comes before any `More` for the one after it.
        let mut open: Option<(u64, F::Disk, u64)> = None;
        merge(
            &tables,
            below,
            self.target_file_size(output_level),
            self.compaction_unit(),
            self.compression(),
            async |chunk| {
                let (bytes, last) = match chunk {
                    Chunk::More(bytes) => (bytes, None),
                    Chunk::Last(bytes, bounds, entries) => (bytes, Some((bounds, entries))),
                };
                if open.is_none() {
                    let file_num = self.take_file_num(&mut writing).await?;
                    open = Some((file_num, self.create_table(file_num).await?, 0));
                }
                let spent = {
                    let (_, disk, at) = open.as_mut().expect("just opened above");
                    self.append_table(disk, at, bytes).await?
                };
                if let Some((bounds, entries)) = last {
                    let (file_num, disk, size) = open.take().expect("open, and the last of it");
                    written.push((file_num, entries));
                    let mut new_table = bounds.into_new_table(output_level, file_num)?;
                    new_table.size = size;
                    self.store.metrics.compaction_tables[1].add(1);
                    self.store.metrics.compaction_io[1].add(new_table.size);
                    self.finish_table(disk).await?;
                    new_tables.push(new_table);
                }
                Ok(spent)
            },
        )
        .await?;
        // Read, and done with. The readers are dropped here rather than at
        // the end of the function because the sweep below cannot collect a
        // table something still has open, and these are its inputs (§5,
        // `crate::obsolete`).
        drop(tables);

        // The permit again, for the edit and everything after it, because
        // all of what is left changes what the store is made of.
        let change = self.changes().begin().await;

        // Nothing survived? The inputs still go: an edit that only deletes is
        // as much a compaction as one that writes.
        //
        // The counter rather than the last number spent: a flush that
        // interleaved with the merge has taken numbers of its own, and an
        // edit saying the number after *this* compaction's last would hand
        // one of them out again (`take_file_num`). Pebble writes the live
        // counter into every edit for the same reason.
        let next_file_num = self.version_set().next_file_num;
        let edit = VersionEdit {
            comparer_name: None,
            min_unflushed_log_num: None,
            obsolete_prev_log_num: None,
            next_file_num: (!new_tables.is_empty()).then_some(next_file_num),
            last_seq_num: Some(self.last_seq_num()),
            deleted_tables: inputs.iter().map(|(l, m)| (*l, m.file_num)).collect(),
            new_tables,
        };

        // The one-way door, in the same place a flush puts it: before this
        // the output is a file nothing refers to and the inputs are still the
        // database's.
        let manifest_bytes = self.append_version_edit(&edit, &change).await?;
        self.apply_edit(&edit)?;
        // More may be worth compacting now, and whoever asked to be told is
        // told here rather than waiting for the next flush (§9).
        let _ = self.version_changed();
        if manifest_bytes >= self.max_manifest_bytes() {
            let _ = self.rotate_inner(&change).await;
        }
        let _ = self.sweep(&change).await;
        self.store.metrics.saw_version(&self.version_set().version);
        self.measure_disk().await;
        self.store.metrics.compactions.add(1);
        self.saw_debt();
        for (file_num, entries) in written {
            self.saw_table_entries(file_num, entries);
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
        let versions = self.version_set();
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

    /// Takes the number for one output table, and spends it.
    ///
    /// Taken *and* spent, which is not how the rest of roble numbers a file:
    /// a flush reads the counter and lets its edit spend it, and a rotation
    /// spends past the marker, both safe because the permit is held from the
    /// one to the other. A merge holds no permit (§9), so a number it took
    /// and left would be a number the flush beside it takes as well — and
    /// then one of the two tables is written over the other. So the permit is
    /// taken for this one step, and the counter moves inside it: there is no
    /// await between reading it and bumping it, so nothing else can be
    /// between either. Pebble's `getNextDiskFileNum` is this, an atomic
    /// increment it says needs no lock at all.
    ///
    /// What a number taken by a compaction that then fails costs is that it
    /// is never used: the in-memory counter is past it and the MANIFEST's is
    /// not. The next open hands it out again only if the file it named never
    /// reached the directory; one that did, the open moves past
    /// (`crate::open`), and the sweep collects. Pebble skips numbers the same
    /// way.
    ///
    /// Refused once a change to the MANIFEST has failed, which a flush beside
    /// the merge can do while it runs (`crate::flush`). The flush read the
    /// counter and left moving it to its edit, so after that edit fails the
    /// counter offers the very number it gave its table: a merge that took
    /// it would write over a table the failed record names, and a reopen
    /// that replayed the record would read the merge's keys in its place.
    /// Under the permit, so no edit can fail between the check and the take.
    async fn take_file_num(&self, writing: &mut crate::obsolete::Writing<'_>) -> Result<u64> {
        let _change = self.changes().begin().await;
        self.recording()?;
        let mut versions = self.versions_mut();
        let file_num = versions.next_file_num;
        versions.next_file_num = file_num + 1;
        drop(versions);
        // Under the same permit, so no sweep can run between the number
        // being spent and the sweep being told to leave it alone.
        writing.add(file_num);
        Ok(file_num)
    }

    /// Creates the file an output table is written into.
    ///
    /// The number was claimed before this was called, which is the ordering
    /// `obsolete::Writing::add` insists on: a sweep listing the directory
    /// between the file appearing and the number being recorded would find a
    /// table nothing says to keep. It matters more now than it did when a
    /// table was one write — the file is on disk, incomplete, for as long as
    /// the merge takes to fill it (§5).
    async fn create_table(&self, file_num: u64) -> Result<F::Disk> {
        let path = self
            .store
            .dir
            .join(roble_format::files::table_filename(file_num));
        self.fs().create(&path).await
    }

    /// Appends a chunk of an output table at `at`, moves `at` past it, and
    /// gives the buffer back for the next chunk.
    async fn append_table(&self, disk: &F::Disk, at: &mut u64, bytes: Vec<u8>) -> Result<Vec<u8>> {
        let written = bytes.len();
        let (result, buf) = disk
            .write(WriteBuf::new(bytes), *at, Default::default())
            .await;
        result?;
        *at += written as u64;
        self.store.metrics.written.add(written);
        Ok(buf.into_vec())
    }

    /// Makes a finished output table durable, and its name with it.
    async fn finish_table(&self, disk: F::Disk) -> Result<()> {
        {
            let _timer = self.store.metrics.sync_duration.start();
            disk.sync(Default::default()).await?;
        }
        disk.close().await?;
        self.fs().sync_dir(&self.store.dir).await?;
        Ok(())
    }
}

/// What a merge hands its caller: some of the output table it is building,
/// and whether that output is finished.
///
/// One sink taking this rather than two closures — "open me a file" and
/// "here is a finished table" — because both need the same borrows of the
/// caller's state, the numbers it has claimed and the tables it is
/// collecting, and two `AsyncFnMut`s cannot share them.
///
/// The caller gets no file number and opens no file until the first chunk of
/// an output arrives, which is what keeps a merge that drops everything it
/// read from leaving a file behind.
pub(crate) enum Chunk {
    /// More of the output being built, to append to it.
    More(Vec<u8>),
    /// The last of an output, with what the MANIFEST needs for it.
    Last(Vec<u8>, Bounds, u64),
}

/// Merges every input into tables of up to `target_bytes`, dropping what may
/// go, and hands each one to `write` as it is finished.
///
/// `write` rather than a list of tables, so that the bytes of a finished
/// table are somebody else's the moment they are finished — and it is handed
/// [`Chunk`]s rather than whole tables, so that what is held is not even the
/// one being built: a block of it, and the index and filter the format makes
/// the writer keep to the end (`roble_format::sst::TableWriter`). The buffer
/// comes back from `write` and is handed on to the next output, so a merge of
/// a hundred tables holds one chunk's worth of it. It may be called no times at all,
/// which is not an error: a level of superseded keys and spent tombstones
/// compacts to nothing at all. Pebble splits the same two halves apart the
/// other way round, with `compact.Runner` as a pump its caller draws one
/// output table at a time from.
async fn merge<D: Disk>(
    tables: &[Rc<OpenTable<D>>],
    below: &[(Vec<u8>, Vec<u8>)],
    target_bytes: u64,
    unit: u64,
    compression: Codec,
    mut write: impl AsyncFnMut(Chunk) -> Result<Vec<u8>>,
) -> Result<()> {
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

    let mut open = Output::new(None, compression)?;
    // Reused for every entry that survives the merge. A compaction reads
    // millions of them and already holds the encoded key it is writing, so
    // taking an owned entry per survivor meant allocating that key again,
    // and the value again, for each one.
    let mut value = Vec::new();
    // The user key of the entry last taken, and whether there is one. Reused
    // for the same reason `value` is, and it is the same loop: a fresh
    // allocation to answer "was the last key this one?" is one per entry
    // read, not one per survivor, so a compaction that drops nearly
    // everything pays it most.
    let mut seen = Vec::new();
    let mut seen_any = false;
    // The surviving entry of the user key being merged, held back until the
    // versions it supersedes have gone past: what they are decides what it
    // is written as — a Set over a deletion as a SETWITHDEL, a SingleDelete
    // over anything as a Delete (`crate::table::written_over`) — and the
    // merge cannot look ahead. Its value is in `value`, which the next
    // survivor's would overwrite, so it is added to the output before that
    // one is read. `held` is reused as `value` is.
    let mut held = Vec::new();
    let mut holding = false;
    // Its kind, kept beside it so that a superseded entry is weighed against
    // it without decoding the key again.
    let mut held_kind = key::Kind::Set;

    let mut heap = Heap::new(&iters)?;
    while let Some(w) = heap.top() {
        // Borrowed from the iterator rather than copied out of it. What
        // `key` hands back is the cursor's own assembled buffer
        // (`block::restart::Cursor::key`), so it stays put until something
        // advances the iterator — which nothing here does until the entry
        // has been written, `value_into` reading it through a shared borrow
        // as `key` does. What is left is a survivor's copy into `held`,
        // reused and only of the key, and `Output::add`'s, which the table
        // being built has to have.
        let encoded = iters[w].key();
        let k = InternalKey::decode(encoded)?;
        let user = k.user_key;

        // The first version met for a user key is its newest, since trailers
        // descend. Every one after it is superseded and goes.
        let newest = !seen_any || seen != user;
        if newest {
            seen.clear();
            seen.extend_from_slice(user);
            seen_any = true;
            // The last key's versions are all behind it, so what it is
            // written as is settled, and it goes before anything of this key
            // — or before the table it belongs to is finished below.
            if holding {
                open.add(InternalKey::decode(&held)?, &held, &value)?;
                holding = false;
            }
        } else if holding {
            let kind = crate::table::written_over(held_kind, k.kind);
            if kind != held_kind {
                // The trailer's low byte is the kind, with the obsolete bit
                // beside it, which stays as it was.
                let at = held.len() - key::TRAILER_LEN;
                held[at] = kind.to_byte() | (held[at] & key::OBSOLETE_BIT);
                held_kind = kind;
            }
        }

        if newest && keep(user, k.seq_num, k.kind, &tombstones, below) {
            // The break comes *before* the key rather than after, so the
            // span that closes ends at a key the next table holds and the
            // two tables meet rather than overlap. Only a user key the
            // output has not started yet may be a boundary, and after the
            // drops above every surviving user key appears exactly once, so
            // any of them will do.
            if open.writer.estimated_size() >= target_bytes && !open.is_empty() {
                let finished =
                    std::mem::replace(&mut open, Output::new(Some(user.to_vec()), compression)?);
                if let Some((bytes, bounds, entries)) = finished.finish(&tombstones, Some(user))? {
                    // To the table that has just been started: the buffer
                    // the last one's bytes went to the disk in is the one
                    // this one's want (`TableWriter::reuse`).
                    let spent = write(Chunk::Last(bytes, bounds, entries)).await?;
                    open.writer.reuse(spent);
                }
            }
            iters[w].value_into(&mut value).await?;
            held.clear();
            held.extend_from_slice(encoded);
            held_kind = k.kind;
            holding = true;
        }
        iters[w].next().await?;
        heap.advanced(&iters)?;
        // Out of the writer and onto the disk, so that what the next entries
        // encode is all this merge holds. After the iterator has moved on, so
        // nothing borrowed from it is alive across the write.
        if open.writer.ready_len() >= crate::flush::WRITE_CHUNK {
            let spent = write(Chunk::More(open.writer.take_ready())).await?;
            open.writer.reuse(spent);
        }
        pacer.did(1).await;
    }

    if holding {
        open.add(InternalKey::decode(&held)?, &held, &value)?;
    }
    if let Some((bytes, bounds, entries)) = open.finish(&tombstones, None)? {
        // Nothing left to hand the buffer on to: this was the last output.
        let _ = write(Chunk::Last(bytes, bounds, entries)).await?;
    }
    Ok(())
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
    points: usize,
}

impl Output {
    fn new(start: Option<Vec<u8>>, compression: Codec) -> Result<Self> {
        Ok(Output {
            writer: TableWriter::new(WriterOptions::default().compression(compression))?,
            bounds: Bounds::default(),
            start,
            points: 0,
        })
    }

    fn is_empty(&self) -> bool {
        self.points == 0
    }

    fn add(&mut self, key: InternalKey<'_>, encoded: &[u8], value: &[u8]) -> Result<()> {
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
        // The whole table in one buffer, which is what `finish` gives a
        // caller that took nothing as it went. A flush streams instead
        // (`crate::flush`), and this could: the writer holds no finished
        // block either way. What is missing here is somewhere to stream
        // *to* — a merge has no file open, because it does not know which
        // table it is writing until it decides to end one, and giving it one
        // is the per-table sink a compaction needs first.
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

/// The inputs of a merge, arranged so that finding the least key again costs
/// a sift rather than a look at every input.
///
/// A merge asks for the least key once per entry it pulls, and a scan over
/// the inputs answers in O(N) every time. N is one table and its overlaps
/// below L0, but all of L0 plus the whole overlapping run of the level below
/// for an L0 job (§9, `crate::pick`), and those wide jobs are the ones that
/// merge nearly all the entries — so the scan is the term that grows as a
/// store does. Pebble's merging iterator is a heap for the same reason
/// (`mergingIterHeap`), and `compact.Iter` draws from it on the compaction
/// path.
///
/// Every input in the heap is on an entry: one that runs out leaves it, so
/// `top` needs no validity check and a comparison is never made against an
/// iterator that is positioned nowhere.
struct Heap {
    /// A binary heap in an array, least at `items[0]`: the children of `i`
    /// are at `2i + 1` and `2i + 2`.
    items: Vec<Item>,
}

/// One position in the heap.
struct Item {
    /// The input whose key this position holds.
    input: usize,
    /// Which of **this position's** two children beat the other, when that
    /// is known — Pebble's `winnerChild`, and worth having for the reason
    /// its comment gives: a sift-down that knows this already compares the
    /// winner against the parent and stops, so a replacement that still
    /// wins costs one comparison instead of two.
    ///
    /// It belongs to the position and not to the input sitting there, which
    /// is why [`Heap::swap`] exchanges `input` alone. A swap into `i`
    /// changes what one of `i`'s children holds, so `i` forgets; the
    /// position swapped *down* to keeps what it knew, because its own
    /// children are untouched, and that is the whole saving.
    winner: Winner,
}

/// Which child of a heap position won, if it has been worked out.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Winner {
    Unknown,
    Left,
    Right,
}

impl Heap {
    /// Heapifies the inputs that are on an entry.
    fn new<D: Disk>(iters: &[crate::table::TableIter<D>]) -> Result<Self> {
        let items = iters
            .iter()
            .enumerate()
            .filter(|(_, it)| it.is_valid())
            .map(|(input, _)| Item {
                input,
                winner: Winner::Unknown,
            })
            .collect();
        let mut heap = Heap { items };
        for i in (0..heap.items.len() / 2).rev() {
            heap.down(i, heap.items.len(), iters)?;
        }
        Ok(heap)
    }

    /// The input on the least internal key, if any is still positioned.
    fn top(&self) -> Option<usize> {
        self.items.first().map(|item| item.input)
    }

    /// Puts the heap right after the iterator on top has been advanced.
    ///
    /// The top's key has grown, and nothing else has changed, so a sift-down
    /// from the root is all that is owed — Pebble's `fixTop`. An input that
    /// has run out leaves instead, which is what keeps every key in the heap
    /// a key that exists.
    fn advanced<D: Disk>(&mut self, iters: &[crate::table::TableIter<D>]) -> Result<()> {
        let n = self.items.len();
        if iters[self.items[0].input].is_valid() {
            return self.down(0, n, iters);
        }
        // The last position takes the root's place and the heap shrinks, as
        // Pebble's `pop` does. The root's own `winner` survives: its
        // children are positions 1 and 2, and the position that moved is
        // neither of those unless the heap is small enough that the right
        // child is already gone, in which case nothing reads it.
        self.swap(0, n - 1);
        self.items.truncate(n - 1);
        self.down(0, n - 1, iters)
    }

    /// Whether the key at position `a` sorts before the one at `b`.
    ///
    /// An internal key carries a descending trailer, so this is Pebble's
    /// `mergingIterHeap.less` exactly: user keys ascending, and the newest
    /// version of a user key first. The index breaks a tie beyond that,
    /// which the scan this replaces did by only ever taking a strictly
    /// smaller key and so leaving the *first* input holding the least one on
    /// top. A tie means one compaction read the same user key, sequence
    /// number and kind from two of its inputs, which cannot happen while
    /// sequence numbers are handed out once — but a heap is not stable, and
    /// an unstable choice there would change which value the output kept.
    /// Settling it on the index makes the order total, so the least key is
    /// unique and the heap cannot pick differently from the scan whatever
    /// shape it is in. Pebble leaves this to the sequence numbers.
    fn less<D: Disk>(
        &self,
        a: usize,
        b: usize,
        iters: &[crate::table::TableIter<D>],
    ) -> Result<bool> {
        let (a, b) = (self.items[a].input, self.items[b].input);
        Ok(match key::compare(iters[a].key(), iters[b].key())? {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => a < b,
        })
    }

    /// Exchanges the inputs at two positions, leaving `winner` where it is.
    fn swap(&mut self, a: usize, b: usize) {
        let (x, y) = (self.items[a].input, self.items[b].input);
        self.items[a].input = y;
        self.items[b].input = x;
    }

    /// Sifts the input at `i` down through a heap of `n` positions until the
    /// heap property holds again.
    fn down<D: Disk>(
        &mut self,
        mut i: usize,
        n: usize,
        iters: &[crate::table::TableIter<D>],
    ) -> Result<()> {
        loop {
            let left = 2 * i + 1;
            if left >= n {
                break;
            }
            let mut child = left;
            let right = left + 1;
            if right < n {
                if self.items[i].winner == Winner::Unknown {
                    self.items[i].winner = if self.less(right, left, iters)? {
                        Winner::Right
                    } else {
                        Winner::Left
                    };
                }
                if self.items[i].winner == Winner::Right {
                    child = right;
                }
            }
            if !self.less(child, i, iters)? {
                break;
            }
            self.swap(i, child);
            self.items[i].winner = Winner::Unknown;
            i = child;
        }
        Ok(())
    }
}
