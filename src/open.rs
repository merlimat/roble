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

//! Opening a database directory: the first code that reads a whole Pebble
//! database rather than one file at a time (DESIGN.md §6).
//!
//! The order matters, and it is Pebble's:
//!
//! 1. Take the directory lock, before reading anything. Two owners of one
//!    database corrupt it, and the window to lose that race is exactly the
//!    time spent reading.
//! 2. List the directory once. Both markers and every refusal are decided from
//!    that one listing, so the open sees a single state of the directory
//!    rather than a different one per question.
//! 3. Read `marker.format-version.*`, and refuse a version above the pin
//!    **before** trusting any structure inside the files.
//! 4. Read `marker.manifest.*`, which names the MANIFEST to replay. No marker
//!    means no database.
//! 5. Replay that MANIFEST into a version, then check the comparer.
//! 6. Refuse a log that holds records, because replaying them is not
//!    implemented yet and opening without them would drop acknowledged writes.
//!
//! Step 3 before step 5 is the part worth keeping: a database written above
//! the pin must be refused for being above the pin, not for whatever its
//! MANIFEST happens to trip over first.

use std::path::{Path, PathBuf};

use roble_format::files::{
    self, FORMAT_VERSION_MIN, FORMAT_VERSION_PIN, FileType, MARKER_FORMAT_VERSION, MARKER_MANIFEST,
};
use roble_format::manifest::VersionEdit;
use roble_format::record::RecordReader;

use crate::env::{Access, Env, Fs, read_all};
use crate::error::{Error, Result};
use crate::version::VersionSet;

/// A database directory, open: its lock held and its MANIFEST replayed.
///
/// This is what `open` has established, not yet a store you can read keys
/// from: the memtables, caches and iterator tree are built on top of it.
/// Dropping it releases the directory lock.
pub struct Opened<'a, F: Fs> {
    // Held, not used: dropping it is what releases the directory.
    _lock: F::Lock,
    /// The filesystem the database's files are opened in.
    fs: &'a F,
    /// Table files, read whole and kept.
    ///
    /// **This is the seam the caches replace** (§8). A real read fetches the
    /// blocks it needs, keyed by file number and offset, inside a budget;
    /// this reads a whole table the first time anything touches it and never
    /// lets go. That is wrong for a 64 MiB table and right for getting the
    /// read path correct first, and it is why there is no public scan
    /// iterator yet: a cursor that outlives a call has to hold the blocks it
    /// is reading, and nothing here is fit to hold them.
    table_cache: std::cell::RefCell<std::collections::BTreeMap<u64, std::rc::Rc<Vec<u8>>>>,
    /// Each table's range tombstones, decoded once.
    pub(crate) tombstone_cache: std::cell::RefCell<
        std::collections::BTreeMap<u64, std::rc::Rc<Vec<roble_format::rangedel::Tombstone>>>,
    >,
    /// The directory the database lives in.
    pub dir: PathBuf,
    /// The format major version it was written in.
    pub format_version: u64,
    /// The MANIFEST the manifest marker named.
    pub manifest_filename: String,
    /// Every `marker.` file found, sorted, current and obsolete alike.
    pub markers: Vec<String>,
    /// What replaying the MANIFEST established.
    pub versions: VersionSet,
}

/// Opens the database in `dir`.
pub async fn open<'a, E: Env>(env: &'a E, dir: &Path) -> Result<Opened<'a, E::Fs>> {
    let fs = env.fs();

    // 1. The lock comes first, so nothing below races a second owner.
    let lock = fs.lock(&dir.join("LOCK")).await?;

    // 2. One listing, and every question answered from it.
    let entries = fs.list(dir).await?;
    let names: Vec<String> = entries
        .iter()
        .map(|e| e.name.to_string_lossy().into_owned())
        .collect();

    refuse_unreadable_files(&names)?;

    // 3. The format version, before anything inside a file is trusted.
    let format_version = read_format_version(&names)?;

    // 4. Which MANIFEST.
    let manifest_marker = files::current_marker(names.iter().map(String::as_str), MARKER_MANIFEST)?
        .ok_or_else(|| {
            Error::NotFound(format!(
                "no database in {}: it holds no marker.{MARKER_MANIFEST}.* file",
                dir.display()
            ))
        })?;
    let manifest_filename = manifest_marker.value;
    let Some(parsed) = files::parse_filename(&manifest_filename) else {
        return Err(Error::Corrupt(format!(
            "the manifest marker names {manifest_filename:?}, which is not a file name"
        )));
    };
    if parsed.file_type != FileType::Manifest {
        return Err(Error::Corrupt(format!(
            "the manifest marker names {manifest_filename:?}, which is a {} rather than a MANIFEST",
            parsed.file_type.name()
        )));
    }

    // 5. Replay it.
    let edits = read_version_edits(fs, &dir.join(&manifest_filename), parsed.file_num).await?;
    let versions = VersionSet::replay(&edits)?;

    // Every table the version names has to be in the directory. A checkpoint
    // that was streamed short, or a file deleted by hand, shows up here rather
    // than as a missing key much later.
    let present: std::collections::BTreeSet<u64> = names
        .iter()
        .filter_map(|n| files::parse_filename(n))
        .filter(|p| p.file_type == FileType::Table)
        .map(|p| p.file_num)
        .collect();
    for (level, tables) in versions.version.levels() {
        for t in tables {
            if !present.contains(&t.file_num) {
                return Err(Error::Corrupt(format!(
                    "L{level} table {} is in the MANIFEST but not in {}",
                    t.file_num,
                    dir.display()
                )));
            }
        }
    }

    // 6. A log with records in it cannot be honoured yet.
    refuse_unreplayed_logs(fs, dir, &names, versions.min_unflushed_log_num).await?;

    let mut markers: Vec<String> = names
        .iter()
        .filter(|n| n.starts_with("marker."))
        .cloned()
        .collect();
    markers.sort();

    Ok(Opened {
        _lock: lock,
        fs,
        table_cache: Default::default(),
        tombstone_cache: Default::default(),
        dir: dir.to_path_buf(),
        format_version,
        manifest_filename,
        markers,
        versions,
    })
}

/// Refuses the files whose mere presence puts the database above roble's pin.
///
/// Both are decided by name alone, which is the point: a blob file or a
/// failover log says the database used a feature roble does not read, and
/// saying so from the listing is better than discovering it halfway through a
/// MANIFEST.
fn refuse_unreadable_files(names: &[String]) -> Result<()> {
    for name in names {
        let Some(parsed) = files::parse_filename(name) else {
            continue;
        };
        match parsed.file_type {
            FileType::Blob => {
                return Err(Error::Unsupported(format!(
                    "{name} is a blob file, which arrives above roble's pin"
                )));
            }
            FileType::LogFailover => {
                return Err(Error::Unsupported(format!(
                    "{name} is a log written by WAL failover, which roble does not read"
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Reads the format-version marker and checks it against the pin.
fn read_format_version(names: &[String]) -> Result<u64> {
    let marker = files::current_marker(names.iter().map(String::as_str), MARKER_FORMAT_VERSION)?
        .ok_or_else(|| {
            // Pebble reads a missing marker as FormatDefault, which only
            // happens for a directory it is about to create. A database with a
            // MANIFEST and no format marker predates markers entirely.
            Error::Unsupported(
                "the database has no marker.format-version.* file, so it was written before \
                 Pebble recorded the format version"
                    .to_string(),
            )
        })?;

    let version: u64 = marker.value.parse().map_err(|_| {
        Error::Corrupt(format!(
            "the format-version marker holds {:?}, which is not a version number",
            marker.value
        ))
    })?;

    if version == 0 {
        return Err(Error::Corrupt(
            "the format-version marker holds 0, which Pebble never persists".to_string(),
        ));
    }
    if version < FORMAT_VERSION_MIN {
        return Err(Error::Unsupported(format!(
            "the database is in format major version {version}; Pebble itself supports no \
             version below {FORMAT_VERSION_MIN}"
        )));
    }
    if version > FORMAT_VERSION_PIN {
        return Err(Error::Unsupported(format!(
            "the database is in format major version {version}, above roble's pin of \
             {FORMAT_VERSION_PIN}; reading it would mean guessing at structures roble does \
             not know"
        )));
    }
    Ok(version)
}

/// Reads every version edit in a MANIFEST.
async fn read_version_edits<F: Fs>(fs: &F, path: &Path, file_num: u64) -> Result<Vec<VersionEdit>> {
    let disk = fs.open(path, Access::ReadOnly).await?;
    let bytes = read_all(&disk, Default::default()).await?;

    // The file number is passed even though a MANIFEST's chunks are legacy and
    // carry none, so that one which turns out to be recyclable is checked
    // rather than waved through.
    let mut reader = RecordReader::new(&bytes, file_num as u32);
    let mut edits = Vec::new();
    while let Some(record) = reader.next() {
        let record = record.map_err(|e| {
            Error::Corrupt(format!("{}: record {}: {e}", path.display(), edits.len()))
        })?;
        edits.push(VersionEdit::decode(&record)?);
    }
    if edits.is_empty() {
        return Err(Error::Corrupt(format!(
            "{} holds no version edits",
            path.display()
        )));
    }
    Ok(edits)
}

/// Refuses a database whose logs still hold writes.
///
/// `disable_wal` leaves an empty log file behind rather than no log file, which
/// is Oxia's shape (§1) and has to open. So the question is not whether a log
/// exists but whether it holds a record: one record means a write was
/// acknowledged and no table has it yet, and opening without it would discard
/// that write silently. Replaying it is M3's work; until then this refuses,
/// because that is the one answer that cannot lose data (§6).
async fn refuse_unreplayed_logs<F: Fs>(
    fs: &F,
    dir: &Path,
    names: &[String],
    min_unflushed_log_num: u64,
) -> Result<()> {
    let mut logs: Vec<(u64, &String)> = names
        .iter()
        .filter_map(|n| files::parse_filename(n).map(|p| (p, n)))
        .filter(|(p, _)| p.file_type == FileType::Log && p.file_num >= min_unflushed_log_num)
        .map(|(p, n)| (p.file_num, n))
        .collect();
    logs.sort();

    for (log_num, name) in logs {
        let disk = fs.open(&dir.join(name), Access::ReadOnly).await?;
        let bytes = read_all(&disk, Default::default()).await?;
        let mut reader = RecordReader::new(&bytes, log_num as u32);
        match reader.next() {
            None => {}
            Some(Ok(_)) => {
                return Err(Error::Unsupported(format!(
                    "{name} holds writes that no table holds yet, and replaying a log is not \
                     implemented; opening without it would discard writes that were already \
                     acknowledged"
                )));
            }
            Some(Err(e)) => {
                return Err(Error::Corrupt(format!(
                    "{name}: {e}, so what it holds cannot be told from what it lost"
                )));
            }
        }
    }
    Ok(())
}

// Written out rather than derived: the lock has no business in a debug line,
// and a `Fs::Lock` is not required to be `Debug` at all.
impl<F: Fs> std::fmt::Debug for Opened<'_, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opened")
            .field("dir", &self.dir)
            .field("format_version", &self.format_version)
            .field("manifest_filename", &self.manifest_filename)
            .field("markers", &self.markers)
            .field("versions", &self.versions)
            .finish()
    }
}

impl<F: Fs> Opened<'_, F> {
    /// The bytes of one table, read whole the first time and kept.
    ///
    /// See [`Opened`]'s `table_cache` for why this is whole files.
    pub(crate) async fn table_bytes(&self, file_num: u64) -> Result<std::rc::Rc<Vec<u8>>> {
        // The borrow is dropped before the read, so no borrow is held across
        // an await.
        if let Some(held) = self.table_cache.borrow().get(&file_num) {
            return Ok(std::rc::Rc::clone(held));
        }
        let path = self.dir.join(roble_format::files::table_filename(file_num));
        let disk = self.fs.open(&path, Access::ReadOnly).await?;
        let bytes = std::rc::Rc::new(read_all(&disk, Default::default()).await?);
        self.table_cache
            .borrow_mut()
            .insert(file_num, std::rc::Rc::clone(&bytes));
        Ok(bytes)
    }

    /// Renders what the directory says, in the text the Go harness's
    /// `db-fixture` prints from Pebble's own reader.
    ///
    /// The two are compared character for character (§12), which is why this
    /// prints only what both can vouch for: the markers, the format version,
    /// and the tables at each level. The MANIFEST's counters are roble's to
    /// report and Pebble exposes no public accessor for them, so they are
    /// asserted separately rather than smuggled into a comparison that would
    /// then only be checking roble against itself.
    pub fn dump(&self) -> Result<String> {
        use std::fmt::Write as _;

        let mut out = String::new();
        for m in &self.markers {
            let _ = writeln!(out, "marker {m}");
        }
        let _ = writeln!(out, "format-version {}", self.format_version);
        for (level, tables) in self.versions.version.levels() {
            if tables.is_empty() {
                continue;
            }
            let _ = writeln!(out, "level {level}");
            for t in tables {
                let _ = writeln!(
                    out,
                    "  table {} size={} seq={}..{}",
                    files::file_num(t.file_num),
                    t.size,
                    t.smallest_seq_num,
                    t.largest_seq_num
                );
                let _ = writeln!(out, "    smallest {}", internal_key(&t.smallest)?);
                let _ = writeln!(out, "    largest  {}", internal_key(&t.largest)?);
            }
        }
        Ok(out)
    }
}

/// Shows an encoded internal key as the Go harness spells it: the user key in
/// hex, because a bound is bytes and a dump is compared rather than read.
fn internal_key(encoded: &[u8]) -> Result<String> {
    let k = roble_format::InternalKey::decode(encoded)?;
    let hex: String = k.user_key.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("{hex} #{} {}", k.seq_num, k.kind.name()))
}
