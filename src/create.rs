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

//! A database where there was none: the other half of `crate::destroy`
//! (§6, §11).
//!
//! Oxia places a shard on a node and opens its store, and the same call does
//! it whether the shard has been there all along or arrived a moment ago —
//! `newKVPebble` builds a path and hands it to `pebble.Open`, which creates
//! what it does not find. So the decision belongs where Pebble puts it,
//! inside the open, behind [`crate::Options::create_if_missing`]: a caller
//! that cannot tell the two apart should not have to.
//!
//! **What a database is, at its smallest.** An
//! [`open`](crate::open::open_with) refuses a directory with no
//! `marker.manifest.*`, and the whole of this module is writing the least
//! that marker can truthfully name:
//!
//! 1. `MANIFEST-000001`, holding one version edit that describes a version
//!    with no tables in it. The same edit a rotation writes
//!    (`crate::rotate::snapshot_edit`), of a version set with nothing in it,
//!    so a created MANIFEST and a rotated one are the same claim made about
//!    different contents.
//! 2. `000002.log`, empty. See below — this is the one file whose presence
//!    takes some arguing.
//! 3. `marker.manifest.000001.MANIFEST-000001`, and
//!    `marker.format-version.000001.016`. Iteration one for both, as
//!    `crate::checkpoint`'s are, because a directory with no markers has
//!    nothing for them to supersede.
//!
//! Pebble's own creation reaches almost the same place from further away: it
//! creates the database at `FormatMinSupported` and then upgrades it to the
//! version asked for, which is why every fixture in `tests/golden` carries
//! `marker.format-version.000003.016` — iteration three, after two upgrade
//! steps. roble writes the pin at iteration one instead. Pebble reads either,
//! because a marker's iteration says only which of several is current, and
//! `compat/wal-replay --checkpoint` has been making that claim about a
//! roble-written iteration one since M4.
//!
//! **Why an empty log.** With the WAL on, a store whose log is `None`
//! refuses writes outright (`crate::open`): a write it could not recover is
//! a write it will not acknowledge. Nothing else in roble creates the first
//! log — a flush's new log is opened by the flush that needs it — so a
//! database created without one would be a database that cannot be written
//! to until it is flushed, which is to say never.
//!
//! Pebble solves this on the other side, creating a log in every open that
//! is not read-only. Doing it here instead keeps the open's log handling to
//! the one thing it does today, which is resuming the log a replay found,
//! and it means the created directory is complete the moment this returns
//! rather than complete once something has opened it.
//!
//! So the edit says the minimum unflushed log is 2, and log 2 is there and
//! holds nothing. Those agree: a log that holds nothing has nothing to
//! replay, which is what `crate::checkpoint` says from the other direction
//! about a log it does not hold at all. The next file number is 3, so the
//! first flush numbers its table past the log rather than beside it.
//!
//! **The directory entry itself is not made durable.** `mkdir_all` and the
//! `sync_dir` below make the database's *files* durable within the
//! directory, but nothing syncs the parent that names the directory, so a
//! crash just after a creation can take the whole thing. `crate::checkpoint`
//! has the same gap and Pebble does not (`mkdirAllAndSyncParents`). It is
//! left as it is because the consequence is the one Oxia already handles —
//! the shard comes back absent and is created again from the replication log
//! — and closing it properly means syncing a directory roble was not handed.

use std::path::Path;

use roble_format::files::{self, FORMAT_VERSION_PIN, MARKER_FORMAT_VERSION, MARKER_MANIFEST};
use roble_format::record::RecordWriter;

use crate::buf::WriteBuf;
use crate::env::{Disk, Fs};
use crate::error::{Error, Result};
use crate::version::{BYTEWISE_COMPARER, Version, VersionSet};

/// The MANIFEST a creation writes, and the log it names as unflushed.
///
/// Pebble's numbering, which every fixture in `tests/golden` shows: the
/// MANIFEST takes file number one, the first log takes two.
const MANIFEST_NUM: u64 = 1;
const LOG_NUM: u64 = 2;

/// Writes an empty database into `dir`, whose listing is `names`.
///
/// Refuses a directory holding any `marker.` file. Two of them are what this
/// is about to write, so one already there means either a database — which
/// `open` would have found, and this would be overwriting — or the remains
/// of a creation that did not finish, which the two markers cannot be told
/// apart from because an iteration written twice with different values is
/// ambiguous rather than wrong. Pebble refuses the same case, through
/// `ErrorIfNotPristine`.
pub(crate) async fn create<F: Fs>(fs: &F, dir: &Path, names: &[String]) -> Result<()> {
    if let Some(marker) = names.iter().find(|n| n.starts_with("marker.")) {
        return Err(Error::Unsupported(format!(
            "will not create a database in {}: it already holds {marker}",
            dir.display()
        )));
    }

    // A version set with nothing in it, described by the edit a rotation
    // would write about it. Building it this way rather than filling a
    // `VersionEdit` in by hand is the point: whatever a rotation has to say
    // about a version, a creation says about the empty one.
    let versions = VersionSet {
        comparer_name: BYTEWISE_COMPARER.to_string(),
        // Pebble's own creation writes 0 here, because it writes the MANIFEST
        // before the WAL manager has made a log to name; both values behave
        // identically, since a replay takes every log at or above this one
        // and the only log there is is 2. So a mutation either way survives —
        // stated rather than tested (§12). This one is the true statement:
        // log 2 is there, and it is the oldest whose records no table holds.
        min_unflushed_log_num: LOG_NUM,
        next_file_num: LOG_NUM + 1,
        // Nothing has been committed, so nothing has been numbered. Writing 9
        // instead would reach the same place — an open adds one and would
        // land on Pebble's floor without clamping — and that mutation
        // survives too, for the reason the clamp exists: where the first key
        // is numbered is a rule about opening a database, not a fact about
        // this one, so it belongs in `crate::open` and not in these bytes.
        last_seq_num: 0,
        version: Version::new(),
    };
    let edit = crate::rotate::snapshot_edit(&versions, versions.next_file_num);

    let manifest = files::manifest_filename(MANIFEST_NUM);
    let mut writer = RecordWriter::legacy();
    writer.write_record(&edit.encode())?;
    write_file(fs, &dir.join(&manifest), writer.as_bytes()).await?;

    // Empty, and created rather than left for the open to find missing: see
    // the module's documentation.
    write_file(fs, &dir.join(files::log_filename(LOG_NUM)), &[]).await?;

    // The manifest marker last of the two, so that the moment the directory
    // reads as a database is the moment every file it names is already in
    // it: `move_marker` syncs the directory as it goes, and an `open` that
    // raced this one would find no marker at all rather than one naming a
    // MANIFEST whose own bytes had not landed.
    crate::rotate::move_marker(
        fs,
        dir,
        MARKER_FORMAT_VERSION,
        1,
        &files::format_version_value(FORMAT_VERSION_PIN),
    )
    .await?;
    crate::rotate::move_marker(fs, dir, MARKER_MANIFEST, 1, &manifest).await?;
    Ok(())
}

/// Writes `bytes` to a new file and makes both the bytes and the name
/// durable.
async fn write_file<F: Fs>(fs: &F, path: &Path, bytes: &[u8]) -> Result<()> {
    let disk = fs.create(path).await?;
    if !bytes.is_empty() {
        let (result, _) = disk
            .write(WriteBuf::new(bytes.to_vec()), 0, Default::default())
            .await;
        result?;
    }
    disk.sync(Default::default()).await?;
    disk.close().await?;
    Ok(())
}
