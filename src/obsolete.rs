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

//! Deleting what the database no longer refers to (§6).
//!
//! Everything else roble does to a directory adds to it: a table, a log, a
//! record at the end of the MANIFEST. This is the one thing that takes
//! away, and a wrong answer here is not a wrong answer — it is a database
//! that no longer exists. So the rule is written as a list of what may go,
//! never as a list of what must stay, and the fall-through is to keep:
//!
//! - A **table** goes when the current version names it at no level. A table
//!   the version does not name is unreachable: every read starts from the
//!   version, so no code path can ask for it.
//! - A **log** goes when its number is below `min_unflushed_log_num`. That
//!   counter is the MANIFEST's statement that every record in the logs below
//!   it is in a table, and it is durable before any file is touched.
//! - A **MANIFEST** goes when the manifest marker names a different one —
//!   which catches both the file a rotation replaced and the file a rotation
//!   wrote and never got to point at (`crate::rotate`). Pebble keeps one
//!   previous MANIFEST for people to look at when something has gone wrong;
//!   roble keeps none, because nothing reads it.
//! - A **marker** goes when a higher iteration of the same name is there, and
//!   only for the markers roble itself moves. A leftover iteration of
//!   somebody else's marker is somebody else's.
//! - Everything else stays, including every name roble cannot parse. A
//!   database directory may hold files that are none of roble's business, and
//!   a name it cannot reason about is one it cannot decide about.
//!
//! Two leaks make this worth having. A flush retires the log it replaced, and
//! without deletion every flush leaves one behind for the lifetime of the
//! database. And a crash between writing a table and recording it in the
//! MANIFEST leaves a table nothing refers to — the flush was right to write
//! it first (§6), and this is what collects it afterwards. Rotation adds two
//! more of the same shape: the MANIFEST it leaves behind, and the marker
//! iteration it moved past.
//!
//! **When** matters as much as what. Deletion runs only after the edit that
//! retired the file is durable: at the end of a flush, and at an open, where
//! the MANIFEST that was just replayed is as durable as it gets. A file
//! deleted before its edit is a file a crash will go looking for.
//!
//! A removal that fails is not worth failing a flush over. The flush already
//! succeeded, the edit is durable, and the file it could not delete is one
//! nothing refers to: the next flush lists the directory again and finds it
//! again. Reporting a flush that worked as one that did not would be the
//! larger lie. For the same reason the directory is not synced afterwards —
//! Pebble does not either — because an unlink that a crash undoes merely
//! brings back a file that is still obsolete.

use std::collections::BTreeSet;
use std::path::Path;

use roble_format::files::{self, FileType, MARKER_MANIFEST};

use crate::env::Fs;
use crate::error::Result;
use crate::open::Opened;
use crate::version::VersionSet;

/// What the database still refers to.
pub(crate) struct Live {
    /// Every table the current version names, at any level.
    pub tables: BTreeSet<u64>,
    /// The first log whose records no table holds.
    pub min_unflushed_log_num: u64,
    /// The file number of the MANIFEST the manifest marker names.
    pub manifest_num: u64,
    /// Iterations of `marker.manifest.*` a higher one has replaced.
    pub obsolete_markers: BTreeSet<String>,
}

impl Live {
    /// Reads what the database refers to out of a version set and a listing.
    ///
    /// The listing is where the markers come from, because which iteration is
    /// current is a fact about the directory rather than about the version —
    /// and [`files::current_marker`] is already the one place that decides
    /// it, refusals included.
    pub fn of(versions: &VersionSet, manifest_num: u64, names: &[String]) -> Result<Self> {
        let obsolete_markers =
            match files::current_marker(names.iter().map(String::as_str), MARKER_MANIFEST)? {
                Some(current) => current.obsolete.into_iter().collect(),
                None => BTreeSet::new(),
            };
        Ok(Live {
            tables: versions.version.table_file_nums(),
            min_unflushed_log_num: versions.min_unflushed_log_num,
            manifest_num,
            obsolete_markers,
        })
    }
}

/// Whether `name` is a file the database no longer refers to.
fn is_obsolete(name: &str, live: &Live) -> bool {
    // Markers first: a `marker.` name is none of `parse_filename`'s business,
    // and which iteration is current cannot be read off one name anyway.
    if name.starts_with("marker.") {
        return live.obsolete_markers.contains(name);
    }
    let Some(parsed) = files::parse_filename(name) else {
        return false;
    };
    match parsed.file_type {
        FileType::Table => !live.tables.contains(&parsed.file_num),
        FileType::Log => parsed.file_num < live.min_unflushed_log_num,
        // Any MANIFEST but the one the marker names: the file a rotation
        // replaced, or the one a rotation wrote and never pointed at.
        FileType::Manifest => parsed.file_num != live.manifest_num,
        // roble writes no OPTIONS file and reads nothing from one, so it
        // cannot say which of several is current — and the one that is
        // current is how Pebble reports the settings a database was written
        // with.
        FileType::Options => false,
        // The lock is this process's claim on the directory.
        FileType::Lock => false,
        // A half-written file Pebble was about to rename into place. Pebble's
        // own obsolete-file scan does not collect these either — they fall
        // into its "don't delete files we don't know about" arm — and being
        // no more aggressive than Pebble is the whole posture here.
        FileType::Temp => false,
        // Neither can be here: `open` refuses a directory holding one
        // outright (§3), so nothing is open to delete it from.
        FileType::Blob | FileType::LogFailover => false,
    }
}

/// Deletes every obsolete file among `names`, and says which it deleted.
pub(crate) async fn remove_obsolete<F: Fs>(
    fs: &F,
    dir: &Path,
    names: &[String],
    live: &Live,
) -> Result<Vec<String>> {
    let mut removed = Vec::new();
    for name in names {
        if !is_obsolete(name, live) {
            continue;
        }
        fs.remove(&dir.join(name)).await?;
        removed.push(name.clone());
    }
    Ok(removed)
}

impl<F: Fs> Opened<'_, F> {
    /// Deletes every file the database no longer refers to, and says which.
    ///
    /// A flush calls this for itself, so a caller writing through the store
    /// never has to. It is public for the caller who flushes rarely and
    /// wants the space back, and for a test that wants to watch.
    ///
    /// The table cache is not consulted, and does not need to be: a reader is
    /// only ever opened for a table the version names, so no entry can be
    /// holding one of the files this deletes.
    pub async fn remove_obsolete_files(&self) -> Result<Vec<String>> {
        let names: Vec<String> = self
            .fs()
            .list(&self.dir)
            .await?
            .iter()
            .map(|e| e.name.to_string_lossy().into_owned())
            .collect();
        let manifest_num = files::parse_filename(&self.manifest_filename())
            .map(|p| p.file_num)
            .expect("the MANIFEST is named by a marker that parsed");
        // The borrow is released before the await: a `RefCell` held across
        // one is a panic waiting for the next reader.
        let live = {
            let versions = self.versions();
            Live::of(&versions, manifest_num, &names)?
        };
        let removed = remove_obsolete(self.fs(), &self.dir, &names, &live).await?;
        self.forget_markers(&removed);
        Ok(removed)
    }
}
