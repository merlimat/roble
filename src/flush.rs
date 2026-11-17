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

//! Flush: the memtable written out as a table, and recorded (§6).
//!
//! The order is what makes a crash during a flush harmless, and it is the
//! order Pebble uses:
//!
//! 1. Create the log the store will write to next and sync the directory,
//!    then freeze the memtable and point the store at both in one step. From
//!    here a write goes to a memtable this flush is not writing out and a
//!    log its edit does not retire, which is what makes a write arriving in
//!    the middle of a flush safe (§5, `crate::open::Opened::freeze_memtable`).
//!    With the WAL off there is no log, so only the memtable moves and the
//!    edit's minimum unflushed log number falls away.
//! 2. Write the table and sync it, then sync the directory. A table whose
//!    name never reached the disk is a table the MANIFEST will point at and
//!    nobody can open. The bytes go out in chunks as the walk of the memtable
//!    finishes them rather than all at the end, which bounds what a flush
//!    holds to a block and changes nothing about this order: the sync is
//!    still after the last chunk, and the edit still after the sync.
//! 3. Append the version edit to the MANIFEST and sync **that**. Until this
//!    moment the new table is a file nothing refers to, and a crash leaves
//!    the database exactly as it was, with both logs still holding every
//!    write: the one the frozen memtable came from, and the one the writes
//!    since have gone to. An open replays every log the version leaves
//!    unflushed, which is both of them. After the edit, the table is the
//!    database's and the first of the two is no longer needed.
//! 4. Only then let go of the frozen memtable, replace the MANIFEST if it
//!    has outgrown its limit (`crate::rotate`), and collect what the edit
//!    retired — the log the freeze replaced is now a file the MANIFEST has
//!    moved past (`crate::obsolete`).
//!
//! The one-way door is step 3. Everything before it is reversible by doing
//! nothing; everything after it is bookkeeping that a replay would redo.
//!
//! Step 1 is the one thing a failure does not undo: the store goes on
//! writing to the fresh memtable and the fresh log whatever becomes of the
//! rest, so a flush that fails leaves a memtable frozen and the next flush
//! writes *that* one out rather than freezing again.

use roble_format::key::{Kind, SEQ_NUM_MAX};
use roble_format::manifest::{NewTable, VersionEdit};
use roble_format::rangedel;
use roble_format::sst::{TableWriter, WriterOptions};
use roble_format::{InternalKey, key};

use crate::buf::WriteBuf;
use crate::change::Change;
use crate::env::{Access, Disk, Fs};
use crate::error::{Error, Result};
use crate::memtable::Memtable;
use crate::open::Opened;

/// How many of a table's bytes a flush, or a compaction, gathers before
/// handing them to the disk.
///
/// The writer finishes a block at a time and holds nothing it has handed over
/// (`roble_format::sst::TableWriter::take_ready`), so the choice here is only
/// how large a write is: one per block would be four thousand of them for a
/// 64 MiB memtable, and one for the table is the buffer this is here to get
/// rid of. A megabyte is large enough that the per-write cost is noise
/// against the encoding either side of it, and small enough that what a flush
/// holds is a rounding error beside the memtable it is writing out.
///
/// Every table smaller than this — which is every table the tests write — is
/// still one write, made by `finish`.
///
/// Shared with `crate::compact`, which writes its outputs the same way and
/// has the stronger reason to: a target file size doubles per level, so an
/// L6 output is 128 MiB of it where a flush is bounded by the memtable.
pub(crate) const WRITE_CHUNK: usize = 1 << 20;

impl<F: Fs> Opened<F> {
    /// Writes the memtable out as a table and records it in the MANIFEST.
    ///
    /// Returns the new table's file number, or `None` when there was nothing
    /// to write. With the WAL off this is the only durability point (§7);
    /// with it on, it is what lets a log be retired.
    ///
    /// A flush that fails loses nothing: the memtable it froze is still read
    /// from and still written out, by the next flush. What it does not do is
    /// put that memtable back in front — the writes that arrived meanwhile
    /// are in the one that replaced it, and carry higher sequence numbers.
    ///
    /// A flush is also where the store notices it has too many tables, so a
    /// flush that wrote one runs the compaction the picker chose before
    /// returning, unless [`crate::Options::disable_automatic_compactions`]
    /// says otherwise (§9, `crate::pick`). The number here names the table
    /// *this flush wrote*, which that compaction may already have merged
    /// away — a caller holding it has a file number and not a promise.
    ///
    /// The two are timed apart, which is why the timing is inside
    /// `flush_inner` rather than here: `roble.flush.duration` covers the
    /// flush and stops where it ends, and a compaction's time is its own, in
    /// `roble.compaction.duration`. A flush that reported the compaction it
    /// scheduled as part of itself would make its own latency graph
    /// unreadable — the number an operator watches would jump by whatever a
    /// merge happened to cost.
    ///
    /// Untested, with a reason (§12). Telling a timer that stops here from
    /// one that spans the compaction is a claim about elapsed time, and §10
    /// keeps the clock out of the tests; the only test would be a duration
    /// threshold, which would pin the machine it ran on rather than this.
    /// `tests/metrics.rs` does not collect histograms at all, for the
    /// separate reason written at the top of it.
    pub async fn flush(&self) -> Result<Option<u64>> {
        self.writable()?;
        // Taken for the flush and let go of before the compaction, which
        // takes its own and holds it only to decide and to record (§9): a
        // flush that kept the permit across a merge is a flush every write
        // waits behind, and the narrowing is what that was in the way of.
        let flushed = {
            let change = self.changes().begin().await;
            let flushed = self.flush_inner(&change).await;
            // Remembered so that a task waiting to be told there is a
            // memtable to write out is not told the same thing again at
            // once: the next flush would write out the same memtable to the
            // same disk, and a task that asked for it again on the spot
            // would be spinning rather than retrying. A write is what makes
            // it a retry (§5, `crate::open::Opened::flush_wanted`).
            if flushed.is_err() {
                self.saw_flush_failure();
            }
            flushed?
        };
        // Nothing written is nothing changed, and so nothing new for the
        // picker to see: its answer is the one it already gave, which the
        // flush that wrote the last table already ran.
        if flushed.is_some() {
            // Handed to whoever is waiting to be told there is work, and run
            // here only if nobody is (§9,
            // `crate::open::Opened::compaction_wanted`). A store whose
            // caller runs a task for this stops paying for merges in its
            // writes; a store without one is exactly as it was.
            if !self.version_changed() {
                self.maybe_compact().await;
            }
        }
        Ok(flushed)
    }

    /// The flush proper: the table, the log, the edit, the bookkeeping.
    async fn flush_inner(&self, change: &Change<'_>) -> Result<Option<u64>> {
        let _timer = self.metrics.flush.start();

        // 1. The memtable this flush writes out, frozen so that a write
        //    arriving from here is none of this flush's business.
        let memtable = match self.frozen() {
            // An earlier flush froze one and then failed. It is written out
            // as it stands rather than frozen over: a second freeze would
            // leave two memtables owed a table, and a log already rotated
            // past both — so the edit below would retire a log holding the
            // records of the one left behind.
            Some(frozen) => frozen,
            None => {
                let current = self.memtable();
                if current.borrow().is_empty() && current.borrow().tombstones().is_empty() {
                    return Ok(None);
                }

                // The log the store writes to next, created before anything
                // points at it. With the WAL off there is none, and so
                // nothing to create and nothing for the edit to retire.
                let log_num = if self.wal_disabled() {
                    None
                } else {
                    let log_num = self.versions().next_file_num;
                    let log_path = self.dir.join(roble_format::files::log_filename(log_num));
                    let log_disk = self.fs().create(&log_path).await?;
                    log_disk.sync(Default::default()).await?;
                    log_disk.close().await?;
                    self.fs().sync_dir(&self.dir).await?;
                    Some(log_num)
                };
                self.freeze_memtable(log_num).await?;
                current
            }
        };

        // The table's number is taken and not yet spent: nothing refers to
        // it until the edit lands, and a flush that fails leaves it free for
        // the next one. The log's was spent by the freeze above, which is
        // why this reads the counter again rather than adding to it.
        let table_num = self.versions().next_file_num;

        // 2. The table, then the directory, so its name is durable too. The
        //    file is created before the walk because the walk writes into it:
        //    a flush that built the whole table first held a memtable's worth
        //    of bytes to do it, and that buffer is what this is not.
        //
        //    The create awaits, which the walk of a frozen memtable does not
        //    mind, and the reason is the freeze rather than any absence of
        //    awaits: `freeze_memtable` waits for every write in flight and
        //    only then swaps, so from the moment it returned this memtable is
        //    nobody's to write to and every write applies to the one that
        //    replaced it (§5). The walk awaits too, and must — a flush
        //    running beside the writes rather than inside one has to be able
        //    to put the thread down (§9, `crate::work`).
        //
        //    Nothing can sweep in this window either, which is why a
        //    half-written table needs no telling the collector about: the
        //    flush holds the one-change-at-a-time permit from the top of this
        //    function to the bottom, and `crate::obsolete::sweep` will not run
        //    without one (§5, `crate::change`). So the only reader of a table
        //    the MANIFEST does not yet name is this flush.
        let path = self
            .dir
            .join(roble_format::files::table_filename(table_num));
        let disk = self.fs().create(&path).await?;
        let entries = memtable.borrow().len() as u64;
        let (written, new_table) =
            build_table(&memtable, &disk, table_num, self.compaction_unit()).await?;
        self.metrics.flush_size.record(written);
        self.metrics.flush_bytes.add(written);
        self.metrics.written.add(written as usize);
        {
            let _timer = self.metrics.sync_duration.start();
            disk.sync(Default::default()).await?;
        }
        disk.close().await?;
        self.fs().sync_dir(&self.dir).await?;

        // 3. The edit. Before this the table is a file nothing refers to.
        // With the WAL off the edit says nothing about log numbers, which
        // leaves the MANIFEST's where it was. Pebble writes a zero there,
        // because with the WAL off every one of its memtables carries log
        // number zero — but a database written *with* a WAL and reopened
        // without one has a real minimum, and writing zero over it would
        // make logs that are already in a table live again, and then
        // replayed.
        let edit = VersionEdit {
            comparer_name: None,
            min_unflushed_log_num: self.current_log(),
            obsolete_prev_log_num: None,
            next_file_num: Some(table_num + 1),
            last_seq_num: Some(self.last_seq_num()),
            deleted_tables: Vec::new(),
            new_tables: vec![new_table],
        };
        let manifest_bytes = self.append_version_edit(&edit, change).await?;

        // 4. Bookkeeping a replay would redo anyway. The memtable and the
        //    log were swapped in step 1; all that is left is to stop holding
        //    the one a table now holds.
        self.apply_edit(&edit)?;
        self.thaw();

        // 5. A MANIFEST that has outgrown its limit is replaced by one
        //    holding a snapshot of the version, so a reopen replays one
        //    record and not a million (`crate::rotate`). Swallowed for the
        //    same reason as the sweep below: the flush is durable, and a
        //    rotation that did not happen is one the next flush attempts
        //    again.
        if manifest_bytes >= self.max_manifest_bytes() {
            let _ = self.rotate_inner(change).await;
        }

        // 6. What the edit retired: the log this flush replaced, and any
        //    table an earlier flush wrote whose edit never landed. The
        //    failure is swallowed on purpose — the flush is durable and a
        //    file that would not delete is one nothing refers to, which the
        //    next flush will find again (§6). Reporting a flush that worked
        //    as one that did not would be the larger lie.
        let _ = self.sweep(change).await;
        self.metrics.saw_version(&self.versions().version);
        self.metrics
            .memtable_size
            .set(self.unflushed_bytes() as i64);
        self.measure_disk().await;
        self.metrics.flushes.add(1);
        self.saw_debt();
        self.saw_table_entries(table_num, Some(entries));
        self.recount_records();
        Ok(Some(table_num))
    }

    /// Appends one edit to the MANIFEST and syncs it, and says how large
    /// the file has grown.
    ///
    /// **A failure here ends this store's writing life.** The bytes of a
    /// failed append stay where they were put — nothing truncates a
    /// MANIFEST, and a crash would not undo them — so the next append lands
    /// after them, and the sync of *that* record makes both durable. The
    /// first of the two was written against a version the store then carried
    /// on from unchanged, so the second says the same thing again: delete
    /// the tables the first deleted. Replaying both is how a database stops
    /// opening — "deletes table 7 from L0, which does not hold it" — and the
    /// store that wrote it had been handed an error for each one.
    ///
    /// So the first failure is the last edit: every later append is refused.
    /// Pebble reaches the same conclusion and acts on it harder, taking the
    /// process down with `Fatalf` on a MANIFEST write error; refusing the
    /// one store leaves a caller holding a hundred shards the other
    /// ninety-nine.
    ///
    /// Either way the answer is to reopen, and that is sound whichever side
    /// of the failure the file ended on: a record written whole replays, a
    /// record torn across a block boundary is dropped with the tail, and the
    /// tables of an edit that did not land are named by nothing and swept.
    ///
    /// A **write** is not refused, with a log or without one. It appends to
    /// the log and the memtable and says nothing to the MANIFEST, so it can
    /// still be exactly as durable as it promises — and refusing it would
    /// throw away records the log would otherwise have kept.
    pub(crate) async fn append_version_edit(
        &self,
        edit: &VersionEdit,
        _change: &Change<'_>,
    ) -> Result<u64> {
        if self.manifest_failed() {
            return Err(Error::Corrupt(
                "an earlier version edit failed to append, so this store records no more; \
                 reopen the database"
                    .to_string(),
            ));
        }
        let appended = self.append_edit(edit).await;
        if appended.is_err() {
            self.saw_manifest_failure();
        }
        appended
    }

    /// The append itself, whose every failure the caller above makes final.
    async fn append_edit(&self, edit: &VersionEdit) -> Result<u64> {
        let path = self.dir.join(self.manifest_filename());
        let disk = self.fs().open(&path, Access::ReadWrite).await?;
        let at = disk.len().await? as usize;

        // Framed against the file: a MANIFEST is blocked like a log.
        let mut writer = roble_format::record::RecordWriter::legacy_resuming(at);
        writer.write_record(&edit.encode())?;
        let framed = writer.as_bytes().to_vec();

        let written = framed.len();
        let (result, _) = disk
            .write(WriteBuf::new(framed), at as u64, Default::default())
            .await;
        result?;
        self.metrics.written.add(written);
        {
            let _timer = self.metrics.sync_duration.start();
            disk.sync(Default::default()).await?;
        }
        disk.close().await?;
        Ok(writer.position() as u64)
    }
}

/// Writes the memtable out as a table into `disk`, and says how many bytes
/// that took and what the MANIFEST will hold for it.
///
/// **Paced**, every `unit` entries, as a merge is and for the reason a merge
/// is (§9, `crate::work`): this is the one part of a flush that is work
/// rather than waiting, and a 64 MiB memtable is a few hundred thousand
/// entries to walk and encode. Left unpaced it is tens of milliseconds in
/// which nothing else on the core runs — which is invisible while a flush
/// happens *inside* the write that asked for it, and is the whole of the
/// remaining latency once it does not.
///
/// **Bounded**, because the bytes are handed to the disk as the writer
/// finishes them. The whole table used to come back from here as one `Vec`,
/// which made a flush cost the memtable bound — 64 MiB as Oxia configures
/// it, and more than that while the `Vec` doubled — on a node running
/// hundreds of stores, which is the thing §8 bounds. What is held now is
/// what Pebble's writer holds: the open
/// data block, the index, the filter's hashes, and at most [`WRITE_CHUNK`]
/// of finished bytes waiting for a write.
///
/// The memtable is borrowed a slice at a time rather than for the walk,
/// because a borrow held across an await is a panic waiting for whoever
/// borrows next. Nothing writes to this one — it is frozen, and a freeze
/// waits for every write in flight (§5) — so the cursor is as good after a
/// yield as before it.
async fn build_table<D: Disk>(
    memtable: &std::cell::RefCell<Memtable>,
    disk: &D,
    file_num: u64,
    unit: u64,
) -> Result<(u64, NewTable)> {
    let mut writer = TableWriter::new(WriterOptions::default())?;
    // Where the next chunk goes, and in the end how large the table is.
    let mut at = 0u64;

    // Every key the table will hold, point and tombstone alike, so the
    // bounds the MANIFEST records are the table's real ones.
    let mut bounds = Bounds::default();

    // The user key last written, so that every older version of it can be
    // left out: the first version the walk meets of a key is its newest,
    // because trailers sort descending, and nothing can ask a table for any
    // other. A read of the store as it stands wants the newest, and a scan
    // or snapshot made before this flush holds the memtable itself, not the
    // table written from it — so a table written now is only ever read at a
    // sequence number past all of its entries. That is the reasoning
    // `crate::compact` drops versions on, and it holds here for the same
    // reason. Under a skewed workload it is most of what a memtable holds:
    // YCSB's Zipf draws put two of every three updates on a key updated
    // earlier in the same memtable, and writing those cost the flush the
    // encoding, the checksums and the bytes, and the compaction after it the
    // reading of them back only to drop them.
    let mut written = Vec::new();
    let mut written_any = false;

    let mut cursor = {
        let held = memtable.borrow();
        let mut cursor = held.cursor();
        cursor.first(&held);
        cursor
    };
    loop {
        let done = {
            let held = memtable.borrow();
            let mut did = 0;
            while cursor.is_valid() {
                let encoded = cursor.key(&held);
                let key = InternalKey::decode(encoded)?;
                if !written_any || key.user_key != written.as_slice() {
                    writer.add(key, cursor.value(&held))?;
                    bounds.saw_key(encoded)?;
                    bounds.saw_seq(key.seq_num);
                    written.clear();
                    written.extend_from_slice(key.user_key);
                    written_any = true;
                }
                cursor.next(&held);
                did += 1;
                if did == unit {
                    break;
                }
            }
            !cursor.is_valid()
        };
        // Out of the writer and onto the disk, so that what the next slice
        // encodes is all this holds. Outside the borrow above, because this
        // awaits.
        if writer.ready_len() >= WRITE_CHUNK {
            let spent = write_chunk(disk, &mut at, writer.take_ready()).await?;
            // Back to the writer rather than dropped, so a flush holds one
            // chunk's worth of buffer and not a fresh one per chunk: the
            // allocator keeps large free regions resident, so freeing and
            // allocating a megabyte sixty-four times costs more memory than
            // passing one megabyte round does (§8).
            writer.reuse(spent);
        }
        if done {
            break;
        }
        // A unit of zero is a caller asking to be left alone — a bulk load,
        // a repair tool. The slice above never breaks on a count of zero, so
        // it walks to the end in one pass and this line is never reached.
        crate::work::yield_now().await;
    }

    // Fragmented, because a range-deletion block holds fragments and the
    // memtable holds whatever was asked for. Two `delete_range` calls that
    // overlap are ordinary use — `/a`..`/d` then `/b`..`/d` — and without
    // this every flush of that memtable would be refused by the writer, so
    // the store could never shed it.
    //
    // Taken out of the borrow rather than held under it, because what
    // follows writes to the disk and a `RefCell` held across an await is a
    // panic waiting for whoever borrows next.
    let tombstones = {
        let held = memtable.borrow();
        rangedel::fragment(&held.tombstones().iter().collect::<Vec<_>>())
    };

    for t in &tombstones {
        writer.add_range_deletion(&t.start, &t.end, t.seq_num)?;
        // A tombstone's bounds count toward the table's, and its upper one
        // is the sentinel Pebble uses: the end key at the largest sequence
        // number there is. Without it the table would claim to end before a
        // tombstone it holds, and a reader would skip the table for keys the
        // tombstone covers.
        bounds.saw_seq(t.seq_num);
        bounds.saw_key(&key::encode(&t.start, t.seq_num, Kind::RangeDelete))?;
        // The sentinel bounds the keys and not the sequence numbers.
        bounds.saw_key(&key::encode(&t.end, SEQ_NUM_MAX, Kind::RangeDelete))?;
    }

    // Everything the walk did not take: the blocks still buffered, then the
    // index, the range-deletion and filter blocks, the properties, the
    // metaindex and the footer. For a table smaller than one chunk — which
    // is every table in the tests — this is the only write.
    let _ = write_chunk(disk, &mut at, writer.finish()?).await?;
    let mut new_table = bounds.into_new_table(0, file_num)?;
    new_table.size = at;
    Ok((at, new_table))
}

/// Appends one chunk of a table at `at`, moves `at` past it, and gives the
/// buffer back for the next chunk.
async fn write_chunk<D: Disk>(disk: &D, at: &mut u64, chunk: Vec<u8>) -> Result<Vec<u8>> {
    let len = chunk.len() as u64;
    let (result, buf) = disk
        .write(WriteBuf::new(chunk), *at, Default::default())
        .await;
    result?;
    *at += len;
    Ok(buf.into_vec())
}

/// The least and greatest keys a table will hold, and their sequence numbers.
///
/// Shared with `crate::compact`, which has to describe its output the same
/// way: a bound computed one way by a flush and another by a compaction is a
/// MANIFEST that lies about one of them.
pub(crate) struct Bounds {
    smallest: Option<Vec<u8>>,
    largest: Option<Vec<u8>>,
    smallest_seq: u64,
    largest_seq: u64,
}

impl Default for Bounds {
    fn default() -> Self {
        Bounds {
            smallest: None,
            largest: None,
            smallest_seq: u64::MAX,
            largest_seq: 0,
        }
    }
}

impl Bounds {
    /// Takes account of one more **key bound**.
    ///
    /// Key bounds and sequence-number bounds are tracked apart, and that is
    /// not tidiness. A range deletion's upper bound is the sentinel key — its
    /// end at the largest sequence number there is — and that number is a
    /// marker, not a sequence number the table holds. Folding it into the
    /// sequence range gives a table claiming to hold `2^56 - 1`, which is
    /// above anything the MANIFEST has ever committed. Pebble's own tables
    /// show the split: `seq=15..17` beside a largest key of
    /// `#72057594037927935 rangedel`.
    ///
    /// The comparison can fail, and when it does this says so rather than
    /// guessing: a bound chosen from a comparison that did not work is a
    /// table that lies about what it holds, and every reader believes it.
    pub(crate) fn saw_key(&mut self, encoded: &[u8]) -> Result<()> {
        match &self.smallest {
            Some(s) if key::compare(encoded, s)?.is_ge() => {}
            _ => self.smallest = Some(encoded.to_vec()),
        }
        match &self.largest {
            Some(l) if key::compare(encoded, l)?.is_le() => {}
            _ => self.largest = Some(encoded.to_vec()),
        }
        Ok(())
    }

    /// Takes account of one more entry's sequence number.
    pub(crate) fn saw_seq(&mut self, seq: u64) {
        self.smallest_seq = self.smallest_seq.min(seq);
        self.largest_seq = self.largest_seq.max(seq);
    }

    /// The MANIFEST entry for a table with these bounds.
    ///
    /// `size` is left at zero for the caller to fill once the bytes exist.
    pub(crate) fn into_new_table(self, level: u32, file_num: u64) -> Result<NewTable> {
        let (Some(smallest), Some(largest)) = (self.smallest, self.largest) else {
            return Err(Error::Corrupt(
                "a table with entries produced no key bounds".to_string(),
            ));
        };
        Ok(NewTable {
            level,
            file_num,
            size: 0,
            smallest,
            largest,
            smallest_seq_num: self.smallest_seq,
            largest_seq_num: self.largest_seq,
            marked_for_compaction: false,
            creation_time: 0,
        })
    }
}
