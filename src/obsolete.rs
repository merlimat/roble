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
//! - A **table** goes when the current version names it at no level **and
//!   no reader is open for it**. The version is where every read starts, so
//!   a table it does not name is one nothing can newly ask for — but a scan
//!   that began before the version changed holds open readers of its own,
//!   and those are exactly the reads that would find the file gone. See
//!   [`Readers`].
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

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use roble_format::files::{self, FileType, MARKER_MANIFEST};

use crate::change::Change;
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

/// How many readers are open for each table (§5).
///
/// This is the reference counting §5 asks for, counted per table rather than
/// per version. A scan owns an `Rc<OpenTable>` for every table of the
/// version it began on, so keeping those tables is keeping that version, and
/// a table counted here is one some reader can still ask for a block of.
/// Pebble arrives at the same guarantee from the other side — it reference
/// counts the version itself and collects a file once no live version names
/// it — but roble already holds the references where they matter, so
/// counting them needs no second structure to keep in step with the first.
///
/// Every count is one `OpenTable`'s whole life: taken when one is built and
/// given back when it is dropped, both in `crate::table`, so nothing here
/// depends on a caller remembering.
#[derive(Default)]
pub(crate) struct Readers {
    open: RefCell<BTreeMap<u64, usize>>,
}

impl Readers {
    /// Takes account of a reader that has just been opened.
    pub(crate) fn opened(&self, file_num: u64) {
        *self.open.borrow_mut().entry(file_num).or_insert(0) += 1;
    }

    /// Takes account of one that has just been dropped.
    pub(crate) fn closed(&self, file_num: u64) {
        let mut open = self.open.borrow_mut();
        let Some(n) = open.get_mut(&file_num) else {
            // Untested, with a reason (§12): a count is taken when an
            // `OpenTable` is built and given back when that same one is
            // dropped, so a number arriving here is a number this map
            // holds. It returns rather than panicking because this runs in
            // a `Drop` — and if the pairing ever did break, a count left
            // behind only delays a file to the next sweep, where a panic
            // while unwinding would take the process.
            return;
        };
        *n -= 1;
        if *n == 0 {
            open.remove(&file_num);
        }
    }

    /// The tables at least one reader is open for.
    pub(crate) fn open(&self) -> BTreeSet<u64> {
        self.open.borrow().keys().copied().collect()
    }
}

/// Deletes every obsolete file among `names`, and says which it deleted.
///
/// A table that `in_use` names is left where it is, however obsolete: the
/// file is unreachable to anything starting from the version, and still open
/// to a reader that started before it changed. Nothing is lost by waiting —
/// the next sweep finds it again, as it does every file this leaves behind.
pub(crate) async fn remove_obsolete<F: Fs>(
    fs: &F,
    dir: &Path,
    names: &[String],
    live: &Live,
    in_use: &BTreeSet<u64>,
) -> Result<Vec<String>> {
    let mut removed = Vec::new();
    for name in names {
        if !is_obsolete(name, live) {
            continue;
        }
        // A table a reader still has open is left where it is. Only a
        // table has readers, and a file number names one file, so nothing
        // else can match: the numbers come from the one counter the
        // MANIFEST carries.
        if files::parse_filename(name).is_some_and(|p| in_use.contains(&p.file_num)) {
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
    /// A table a reader is still open for is left for a later sweep, and the
    /// table cache's own reader is let go of first (§5).
    pub async fn remove_obsolete_files(&self) -> Result<Vec<String>> {
        let change = self.changes().begin().await;
        self.sweep(&change).await
    }

    /// The sweep proper, for a caller that already holds the permit.
    ///
    /// The permit is what makes the answer it computes still true by the time
    /// it acts on it: without one, a flush installing a table between the
    /// `Live` below and the deletions above it would have installed a table
    /// this does not know about, and this would delete it.
    pub(crate) async fn sweep(&self, _change: &Change<'_>) -> Result<Vec<String>> {
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
        let in_use = self.release_dead_readers(&live);
        let removed = remove_obsolete(self.fs(), &self.dir, &names, &live, &in_use).await?;
        self.forget_markers(&removed);
        Ok(removed)
    }

    /// Lets go of the table cache's readers for tables the version no longer
    /// names, and says which tables a reader is open for after that.
    ///
    /// The two halves are in this order because the cache is not a reader in
    /// the sense the sweep cares about. It holds one for as long as its
    /// budget allows and nobody may be reading it, so leaving it there would
    /// make a file wait for cache pressure rather than for whoever is
    /// actually reading it. Pebble evicts from its own table cache at
    /// exactly this point, and for this reason.
    ///
    /// So the second `open()` is not the first: letting go of a reader
    /// nothing else holds drops it, and dropping it is what takes its count
    /// back out.
    fn release_dead_readers(&self, live: &Live) -> BTreeSet<u64> {
        for file_num in self.readers().open() {
            if !live.tables.contains(&file_num) {
                self.forget_reader(file_num);
            }
        }
        self.readers().open()
    }
}
