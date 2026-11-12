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

//! A directory Go Pebble opens, holding what this store holds (§6, §11).
//!
//! How Oxia moves a shard: it checkpoints one, streams the directory to
//! another node, and that node opens it. So this is the one operation whose
//! output is read by somebody else's code, and §12 calls it the single most
//! important interop test for that reason.
//!
//! Four things go in, and the list is short because a database is short:
//! hard links to the live tables, a MANIFEST holding one edit that describes
//! the version, and the two markers that say which MANIFEST to replay and
//! which format version it is in.
//!
//! **Links rather than copies.** A table is immutable once written, so the
//! checkpoint and the store can share the bytes until one of them deletes
//! its name (§10, [`crate::env::Fs::link`], which falls back to a copy where
//! a link cannot be made).
//!
//! **No `OPTIONS-*`.** roble writes none and reads nothing from one
//! (`crate::obsolete`), and Pebble does not need one: it writes its own when
//! it opens a directory, and checks a previous one only if it finds one.
//!
//! **No logs.** The flush below leaves nothing unflushed, so there is
//! nothing for a log to carry. The minimum unflushed log number the edit
//! records names a log the checkpoint does not hold, which is the same thing
//! as a log holding nothing — to roble's replay, which looks for the names
//! the directory has, and to Pebble's, which does the same.

use std::path::Path;

use roble_format::files::{self, MARKER_FORMAT_VERSION, MARKER_MANIFEST};
use roble_format::record::RecordWriter;

use crate::buf::WriteBuf;
use crate::env::{Disk, Fs};
use crate::error::Result;
use crate::open::Opened;

impl<F: Fs> Opened<F> {
    /// Writes a checkpoint of this store into `dir`, which must not already
    /// hold a database.
    ///
    /// **It flushes first.** A checkpoint of a store with writes still in
    /// its memtable would be a checkpoint missing them, and with the WAL off
    /// — which is Oxia's configuration (§4) — there would be no log in the
    /// directory to carry them either. So the memtable becomes a table
    /// first, and what the checkpoint holds is everything written before it
    /// was asked for.
    ///
    /// A write that arrives while this is running is not in it, and that is
    /// what a checkpoint is: a directory as of a moment, and the moment is
    /// the flush.
    pub async fn checkpoint(&self, dir: &Path) -> Result<()> {
        // Before the permit, not under it: a flush takes it for itself, and
        // what this needs the permit for is that the version it is about to
        // describe stays the version while its tables are being linked.
        self.flush().await?;
        let _change = self.changes().begin().await;

        self.fs().mkdir_all(dir).await?;

        let (tables, edit, manifest_num) = {
            let versions = self.versions();
            // The MANIFEST takes the next number the store had to give, and
            // the edit says the number after it is next. Nothing in the
            // checkpoint has to agree with the store it came from — it is a
            // database of its own from here — only with itself.
            let manifest_num = versions.next_file_num;
            let tables: Vec<u64> = versions.version.table_file_nums().into_iter().collect();
            let edit = crate::rotate::snapshot_edit(&versions, manifest_num + 1);
            (tables, edit, manifest_num)
        };

        for file_num in tables {
            let name = files::table_filename(file_num);
            self.fs()
                .link(&self.dir.join(&name), &dir.join(&name))
                .await?;
        }

        // One record, because a checkpoint has no history to carry: only a
        // version to describe. The same edit a MANIFEST rotation writes
        // (`crate::rotate`), which is the same claim — this version, from
        // nothing.
        let filename = files::manifest_filename(manifest_num);
        let mut writer = RecordWriter::legacy();
        writer.write_record(&edit.encode())?;
        let bytes = writer.as_bytes().to_vec();
        let disk = self.fs().create(&dir.join(&filename)).await?;
        let (result, _) = disk
            .write(WriteBuf::new(bytes), 0, Default::default())
            .await;
        result?;
        disk.sync(Default::default()).await?;
        disk.close().await?;

        // Iteration one for both markers: the directory is new, so there is
        // nothing for them to supersede.
        crate::rotate::move_marker(self.fs(), dir, MARKER_MANIFEST, 1, &filename).await?;
        crate::rotate::move_marker(
            self.fs(),
            dir,
            MARKER_FORMAT_VERSION,
            1,
            &self.format_version.to_string(),
        )
        .await?;
        self.fs().sync_dir(dir).await?;
        Ok(())
    }
}
