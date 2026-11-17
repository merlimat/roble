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

//! Rotating the MANIFEST, and moving the marker that names it (§6).
//!
//! A MANIFEST only grows: every flush appends one record and nothing ever
//! rewrites it, so a long-lived store would hand a reopen a file of millions
//! of edits to replay in order. Rotation cuts that off. A fresh MANIFEST
//! opens with a single **snapshot** edit describing the whole version — every
//! table at every level, the comparer, the counters — after which a reopen
//! replays one record plus whatever has happened since.
//!
//! The one-way door is the **marker**. Until it moves, the new MANIFEST is a
//! file nothing refers to and a crash leaves the database exactly as it was,
//! still appending to the old one; afterwards the new file is the database's
//! and the old one is obsolete like any other file (`crate::obsolete`). So
//! the order is: write the new MANIFEST, sync it, sync the directory, move
//! the marker — and only then change anything in memory, because after the
//! marker has moved an append that still went to the old file would be an
//! append a reopen never sees.
//!
//! **Moving a marker** is two files for a moment. Pebble creates the new
//! marker, removes the old one and syncs the directory once at the end; roble
//! syncs the directory *between* the two, so the new marker's name is durable
//! before the old one's removal can be. The cost is one extra directory sync
//! per rotation, which happens once per 128 MiB of MANIFEST. What it buys is
//! that no crash can land in a state with no marker at all, which is the one
//! state that is not recoverable — §12 asks that a half-rotated marker leave
//! the database openable, and with both markers present the higher iteration
//! simply wins. Removing the old marker is left to the obsolete sweep, which
//! already knows how to tell a current marker from a leftover.
//!
//! Two things roble does differently from Pebble's `createManifest`, both
//! written down rather than inherited:
//!
//! - The snapshot carries **`last_seq_num`**. Pebble omits it because its
//!   rotation is always immediately followed by the edit that triggered it,
//!   which carries one. roble's rotation stands on its own, and a MANIFEST
//!   whose records never mention a sequence number replays to zero — which
//!   would hand the next write a number the tables already hold.
//! - roble **does not rotate at open**. Pebble does, to bound replay; rotating
//!   on size bounds it just as well, and leaves opening a database something
//!   that does not write to it.

use roble_format::files::{self, MARKER_MANIFEST};
use roble_format::manifest::{NewTable, VersionEdit};

use crate::buf::WriteBuf;
use crate::change::Change;
use crate::env::{Disk, Fs};
use crate::error::Result;
use crate::open::Opened;
use crate::version::VersionSet;

/// The default size a MANIFEST may reach before it is replaced.
///
/// Pebble's `MaxManifestFileSize`. Pebble also weighs how many edits have
/// accumulated against how many tables a snapshot would hold, so that a
/// database with a huge version does not spend half its time writing
/// snapshots; roble has one trigger because it has one writer of edits, and
/// the heuristic can arrive with the compactions that make it matter.
pub const MAX_MANIFEST_BYTES: u64 = 128 << 20;

impl<F: Fs> Opened<F> {
    /// Starts a fresh MANIFEST holding a snapshot of the current version, and
    /// points the manifest marker at it.
    ///
    /// Returns the new MANIFEST's name. A flush does this for itself once the
    /// file it is appending to has grown past [`Opened::max_manifest_bytes`].
    pub async fn rotate_manifest(&self) -> Result<String> {
        self.writable()?;
        let change = self.changes().begin().await;
        self.rotate_inner(&change).await
    }

    /// The rotation proper, for a caller that already holds the permit.
    pub(crate) async fn rotate_inner(&self, _change: &Change<'_>) -> Result<String> {
        // The number is taken but not spent: nothing in memory moves until
        // the marker has. A crash before that leaves this MANIFEST an orphan
        // and the number free again, and the sweep collects the file.
        let (file_num, snapshot) = {
            let versions = self.versions();
            let file_num = versions.next_file_num;
            (file_num, snapshot_edit(&versions, file_num + 1))
        };

        let filename = files::manifest_filename(file_num);
        let mut writer = roble_format::record::RecordWriter::legacy();
        writer.write_record(&snapshot.encode())?;
        let bytes = writer.as_bytes().to_vec();

        let disk = self.fs().create(&self.dir.join(&filename)).await?;
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

        // The one-way door.
        let iter = self.manifest_marker_iter() + 1;
        let marker = move_marker(self.fs(), &self.dir, MARKER_MANIFEST, iter, &filename).await?;

        // Past the door, and nothing here can fail.
        self.install_manifest(filename.clone(), iter, marker);
        self.versions_mut().next_file_num = file_num + 1;
        Ok(filename)
    }
}

/// Builds the one edit a fresh MANIFEST opens with.
///
/// It has to replay, on its own, to the version set it was built from: that
/// is the whole claim rotation makes, and `tests/rotate.rs` checks it by
/// replaying the file before trusting it.
pub(crate) fn snapshot_edit(versions: &VersionSet, next_file_num: u64) -> VersionEdit {
    let mut new_tables = Vec::new();
    for (level, tables) in versions.version.levels() {
        for t in tables {
            new_tables.push(NewTable {
                level,
                file_num: t.file_num,
                size: t.size,
                smallest: t.smallest.clone(),
                largest: t.largest.clone(),
                smallest_seq_num: t.smallest_seq_num,
                largest_seq_num: t.largest_seq_num,
                marked_for_compaction: t.marked_for_compaction,
                creation_time: t.creation_time,
            });
        }
    }
    VersionEdit {
        comparer_name: Some(versions.comparer_name.clone()),
        min_unflushed_log_num: Some(versions.min_unflushed_log_num),
        obsolete_prev_log_num: None,
        next_file_num: Some(next_file_num),
        last_seq_num: Some(versions.last_seq_num),
        deleted_tables: Vec::new(),
        new_tables,
    }
}

/// Creates the marker file naming `value`, and makes its name durable.
///
/// The marker's whole content is its name, so there is nothing to write into
/// the file and nothing to sync but the directory. The earlier iteration is
/// left where it is: with both present the higher one is current, and the
/// obsolete sweep collects the other.
pub(crate) async fn move_marker<F: Fs>(
    fs: &F,
    dir: &std::path::Path,
    name: &str,
    iter: u64,
    value: &str,
) -> Result<String> {
    let filename = files::marker_filename(name, iter, value);
    let disk = fs.create(&dir.join(&filename)).await?;
    disk.close().await?;
    fs.sync_dir(dir).await?;
    Ok(filename)
}
