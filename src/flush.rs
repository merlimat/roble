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
//!    again.
//! 3. Append the version edit to the MANIFEST and sync **that**. Until this
//!    moment the new table is a file nothing refers to, and a crash leaves
//!    the database exactly as it was, with the old log still holding every
//!    write. After it, the table is the database's and the old log is no
//!    longer needed.
//! 4. Only then swap the memtable and the log in memory, and collect what
//!    the edit retired — the log it replaced is now a file the MANIFEST has
//!    moved past (`crate::obsolete`).
//!
//! The one-way door is step 3. Everything before it is reversible by doing
//! nothing; everything after it is bookkeeping that a replay would redo.

use roble_format::key::{Kind, SEQ_NUM_MAX};
use roble_format::manifest::{NewTable, VersionEdit};
use roble_format::sst::{TableWriter, WriterOptions};
use roble_format::{InternalKey, key};

use crate::buf::WriteBuf;
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
    pub async fn flush(&self) -> Result<Option<u64>> {
        let memtable = self.memtable();
        if memtable.borrow().is_empty() && memtable.borrow().tombstones().is_empty() {
            return Ok(None);
        }

        // File numbers for the table and for the log that follows it.
        let (table_num, log_num, next_file_num) = {
            let versions = self.versions();
            (
                versions.next_file_num,
                versions.next_file_num + 1,
                versions.next_file_num + 2,
            )
        };

        let (bytes, new_table) = build_table(&memtable.borrow(), table_num)?;

        // 1. The table, then the directory, so its name is durable too.
        let path = self
            .dir
            .join(roble_format::files::table_filename(table_num));
        let disk = self.fs().create(&path).await?;
        let (result, _) = disk
            .write(WriteBuf::new(bytes), 0, Default::default())
            .await;
        result?;
        disk.sync(Default::default()).await?;
        disk.close().await?;
        self.fs().sync_dir(&self.dir).await?;

        // 2. The log the store writes to next. It has to exist before the
        //    MANIFEST points at it.
        let log_path = self.dir.join(roble_format::files::log_filename(log_num));
        let log_disk = self.fs().create(&log_path).await?;
        log_disk.sync(Default::default()).await?;
        log_disk.close().await?;
        self.fs().sync_dir(&self.dir).await?;

        // 3. The edit. Before this the table is a file nothing refers to.
        let edit = VersionEdit {
            comparer_name: None,
            min_unflushed_log_num: Some(log_num),
            obsolete_prev_log_num: None,
            next_file_num: Some(next_file_num),
            last_seq_num: Some(self.snapshot()),
            deleted_tables: Vec::new(),
            new_tables: vec![new_table],
        };
        self.append_version_edit(&edit).await?;

        // 4. Bookkeeping a replay would redo anyway.
        self.versions_mut().apply(&edit)?;
        self.install_fresh_memtable(Memtable::new(self.snapshot().wrapping_add(1)));
        self.install_log(log_num).await?;

        // 5. What the edit retired: the log this flush replaced, and any
        //    table an earlier flush wrote whose edit never landed. The
        //    failure is swallowed on purpose — the flush is durable and a
        //    file that would not delete is one nothing refers to, which the
        //    next flush will find again (§6). Reporting a flush that worked
        //    as one that did not would be the larger lie.
        let _ = self.remove_obsolete_files().await;
        Ok(Some(table_num))
    }

    /// Appends one edit to the MANIFEST and syncs it.
    async fn append_version_edit(&self, edit: &VersionEdit) -> Result<()> {
        let path = self.dir.join(&self.manifest_filename);
        let disk = self.fs().open(&path, Access::ReadWrite).await?;
        let at = disk.len().await? as usize;

        // Framed against the file: a MANIFEST is blocked like a log.
        let mut writer = roble_format::record::RecordWriter::legacy_resuming(at);
        writer.write_record(&edit.encode())?;
        let framed = writer.as_bytes().to_vec();

        let (result, _) = disk
            .write(WriteBuf::new(framed), at as u64, Default::default())
            .await;
        result?;
        disk.sync(Default::default()).await?;
        disk.close().await?;
        Ok(())
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

    for t in memtable.tombstones() {
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
    let (Some(smallest), Some(largest)) = (bounds.smallest, bounds.largest) else {
        return Err(Error::Corrupt(
            "a memtable with entries produced no key bounds".to_string(),
        ));
    };
    let size = bytes.len() as u64;
    Ok((
        bytes,
        NewTable {
            level: 0,
            file_num,
            size,
            smallest,
            largest,
            smallest_seq_num: bounds.smallest_seq,
            largest_seq_num: bounds.largest_seq,
            marked_for_compaction: false,
            creation_time: 0,
        },
    ))
}

/// The least and greatest keys a table will hold, and their sequence numbers.
struct Bounds {
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
    fn saw_key(&mut self, encoded: &[u8]) -> Result<()> {
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
    fn saw_seq(&mut self, seq: u64) {
        self.smallest_seq = self.smallest_seq.min(seq);
        self.largest_seq = self.largest_seq.max(seq);
    }
}
