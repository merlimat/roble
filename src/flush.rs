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
//! 1. Write the table and sync it, then sync the directory. A table whose
//!    name never reached the disk is a table the MANIFEST will point at and
//!    nobody can open.
//! 2. Create the log the store will write to next, and sync the directory
//!    again. With the WAL off there is no log, so this step and the edit's
//!    minimum unflushed log number both fall away.
//! 3. Append the version edit to the MANIFEST and sync **that**. Until this
//!    moment the new table is a file nothing refers to, and a crash leaves
//!    the database exactly as it was, with the old log still holding every
//!    write. After it, the table is the database's and the old log is no
//!    longer needed.
//! 4. Only then swap the memtable and the log in memory, replace the
//!    MANIFEST if it has outgrown its limit (`crate::rotate`), and collect
//!    what the edit retired — the log it replaced is now a file the MANIFEST
//!    has moved past (`crate::obsolete`).
//!
//! The one-way door is step 3. Everything before it is reversible by doing
//! nothing; everything after it is bookkeeping that a replay would redo.

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

impl<F: Fs> Opened<'_, F> {
    /// Writes the memtable out as a table and records it in the MANIFEST.
    ///
    /// Returns the new table's file number, or `None` when there was nothing
    /// to write. With the WAL off this is the only durability point (§7);
    /// with it on, it is what lets a log be retired.
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
        // Taken for the flush *and* the compaction it may run: one change at
        // a time means one, and a compaction scheduled by a flush is part of
        // the same one (§5, `crate::change`).
        let change = self.changes().begin().await;
        let flushed = self.flush_inner(&change).await?;
        // Nothing written is nothing changed, and so nothing new for the
        // picker to see: its answer is the one it already gave, which the
        // flush that wrote the last table already ran.
        if flushed.is_some() {
            self.maybe_compact(&change).await;
        }
        Ok(flushed)
    }

    /// The flush proper: the table, the log, the edit, the bookkeeping.
    async fn flush_inner(&self, change: &Change<'_>) -> Result<Option<u64>> {
        let _timer = self.metrics.flush.start();
        let memtable = self.memtable();
        if memtable.borrow().is_empty() && memtable.borrow().tombstones().is_empty() {
            return Ok(None);
        }

        // File numbers for the table and, when there is a WAL, for the log
        // that follows it.
        let (table_num, log_num, next_file_num) = {
            let versions = self.versions();
            let table_num = versions.next_file_num;
            if self.wal_disabled() {
                (table_num, None, table_num + 1)
            } else {
                (table_num, Some(table_num + 1), table_num + 2)
            }
        };

        let entries = memtable.borrow().len() as u64;
        let (bytes, new_table) = build_table(&memtable.borrow(), table_num)?;
        self.metrics.flush_size.record(bytes.len() as u64);
        self.metrics.flush_bytes.add(bytes.len() as u64);

        // 1. The table, then the directory, so its name is durable too.
        let path = self
            .dir
            .join(roble_format::files::table_filename(table_num));
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

        // 2. The log the store writes to next. It has to exist before the
        //    MANIFEST points at it. With the WAL off there is none, and so
        //    nothing to create and nothing for the edit below to retire.
        if let Some(log_num) = log_num {
            let log_path = self.dir.join(roble_format::files::log_filename(log_num));
            let log_disk = self.fs().create(&log_path).await?;
            log_disk.sync(Default::default()).await?;
            log_disk.close().await?;
            self.fs().sync_dir(&self.dir).await?;
        }

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
            min_unflushed_log_num: log_num,
            obsolete_prev_log_num: None,
            next_file_num: Some(next_file_num),
            last_seq_num: Some(self.snapshot()),
            deleted_tables: Vec::new(),
            new_tables: vec![new_table],
        };
        let manifest_bytes = self.append_version_edit(&edit, change).await?;

        // 4. Bookkeeping a replay would redo anyway.
        self.versions_mut().apply(&edit)?;
        self.install_fresh_memtable(Memtable::new(self.snapshot().wrapping_add(1)));
        if let Some(log_num) = log_num {
            self.install_log(log_num).await?;
        }

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

/// Builds the table bytes, and the entry the MANIFEST will hold for it.
fn build_table(memtable: &Memtable, file_num: u64) -> Result<(Vec<u8>, NewTable)> {
    let mut writer = TableWriter::new(WriterOptions::default())?;

    // Every key the table will hold, point and tombstone alike, so the
    // bounds the MANIFEST records are the table's real ones.
    let mut bounds = Bounds::default();

    let mut cursor = memtable.cursor();
    cursor.first(memtable);
    while cursor.is_valid() {
        let encoded = cursor.key(memtable);
        let key = InternalKey::decode(encoded)?;
        writer.add(key, cursor.value(memtable))?;
        bounds.saw_key(encoded)?;
        bounds.saw_seq(key.seq_num);
        cursor.next(memtable);
    }

    // Fragmented, because a range-deletion block holds fragments and the
    // memtable holds whatever was asked for. Two `delete_range` calls that
    // overlap are ordinary use — `/a`..`/d` then `/b`..`/d` — and without
    // this every flush of that memtable would be refused by the writer, so
    // the store could never shed it.
    let tombstones = rangedel::fragment(&memtable.tombstones().iter().collect::<Vec<_>>());

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

    let bytes = writer.finish()?;
    let size = bytes.len() as u64;
    let mut new_table = bounds.into_new_table(0, file_num)?;
    new_table.size = size;
    Ok((bytes, new_table))
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
