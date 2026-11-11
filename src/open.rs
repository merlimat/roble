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
//! 6. Replay the logs the version leaves unflushed, so every acknowledged
//!    write is back in the memtable before anyone can read.
//! 7. Collect what the MANIFEST no longer refers to, which is where a flush
//!    interrupted between its table and its edit is cleaned up.
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

use crate::env::{Access, Disk, Env, Fs, read_all};
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
    /// The caches this store draws on, shared with every other store on the
    /// thread that was given the same handle (§8).
    pub(crate) caches: crate::cache::Caches<F>,
    /// This store's identity within those caches: file numbers are per
    /// database, so the caches cannot be keyed by file number alone.
    store_id: u64,
    /// Writes that no table holds yet.
    ///
    /// Two cells deep on purpose. A flush replaces the **handle**, so a scan
    /// that cloned the old one keeps reading the memtable it started with —
    /// swapping the contents instead would empty a snapshot out from under
    /// it. The inner cell is what a write needs.
    memtable: std::cell::RefCell<std::rc::Rc<std::cell::RefCell<crate::memtable::Memtable>>>,
    /// The log this store appends to, if it has one.
    log: std::cell::RefCell<Option<Log<F::Disk>>>,
    /// Whether this store was opened with the WAL off, so that `flush` is
    /// the only durability point (§4).
    disable_wal: bool,
    /// The next sequence number to hand out.
    ///
    /// Starts past the MANIFEST's last, so nothing written here can collide
    /// with what the tables already hold.
    next_seq: std::cell::Cell<u64>,
    /// The directory the database lives in.
    pub dir: PathBuf,
    /// The format major version it was written in.
    pub format_version: u64,
    /// The MANIFEST being appended to, and the iteration of the marker that
    /// names it.
    ///
    /// One cell for both, because moving one without the other is exactly
    /// the failure rotation is shaped to avoid (`crate::rotate`).
    manifest: std::cell::RefCell<Manifest>,
    /// Every `marker.` file the directory holds, sorted, current and obsolete
    /// alike. Kept in step with the directory by the two things that change
    /// it: a rotation adds one, and the obsolete sweep removes what it
    /// deleted.
    markers: std::cell::RefCell<Vec<String>>,
    /// How large the MANIFEST may grow before a flush replaces it.
    max_manifest_bytes: u64,
    /// What replaying the MANIFEST established, and what a flush adds to.
    versions: std::cell::RefCell<VersionSet>,
}

/// Opens the database in `dir` with the defaults.
///
/// A store that shares a thread with others, or that wants Oxia's
/// configuration, is opened with [`open_with`] instead (§8, §11).
pub async fn open<'a, E: Env>(env: &'a E, dir: &Path) -> Result<Opened<'a, E::Fs>> {
    open_with(env, dir, crate::options::Options::default()).await
}

/// Opens the database in `dir` as `options` says.
pub async fn open_with<'a, E: Env>(
    env: &'a E,
    dir: &Path,
    options: crate::options::Options<E::Fs>,
) -> Result<Opened<'a, E::Fs>> {
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
    let manifest_marker_iter = manifest_marker.iter;
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

    // 6. Replay the logs the version edits leave unflushed.
    let mut memtable = crate::memtable::Memtable::new(versions.last_seq_num.wrapping_add(1));
    let replayed = replay_logs(
        fs,
        dir,
        &names,
        versions.min_unflushed_log_num,
        &mut memtable,
    )
    .await?;

    // 7. What the replayed MANIFEST no longer refers to: the logs it has
    //    moved past, and any table a flush wrote whose edit never landed.
    //    This is where an interrupted flush is collected — the table was
    //    written and synced before the edit, so a crash in between leaves a
    //    file in no version (`crate::obsolete`). It runs after the replay,
    //    so no log it deletes is one the replay still needed, and its
    //    failure is swallowed: a leftover file is no reason to refuse a
    //    database that reads.
    let live = crate::obsolete::Live::of(&versions, parsed.file_num, &names)?;
    let swept = crate::obsolete::remove_obsolete(fs, dir, &names, &live)
        .await
        .unwrap_or_default();

    // The next sequence number never goes backwards: a write replay brought
    // back must not be overwritten by one numbered the same (§6).
    let next_seq = versions
        .last_seq_num
        .max(replayed.highest_seq)
        .saturating_add(1);

    // The log the store carries on writing to, opened where the replay
    // stopped. Overwriting a torn tail is the point: those bytes were never
    // acknowledged.
    //
    // With the WAL off there is none, which is not the same as having
    // skipped the replay above: the records already in a log were
    // acknowledged when they were written, whatever this store has been
    // asked to do from here (§6).
    let log = match replayed.tail.filter(|_| !options.disable_wal) {
        Some((log_num, at)) => {
            let path = dir.join(roble_format::files::log_filename(log_num));
            let disk = fs.open(&path, Access::ReadWrite).await?;
            Some(Log {
                disk: std::rc::Rc::new(disk),
                log_num,
                at,
            })
        }
        None => None,
    };

    let mut markers: Vec<String> = names
        .iter()
        .filter(|n| n.starts_with("marker.") && !swept.contains(n))
        .cloned()
        .collect();
    markers.sort();

    Ok(Opened {
        _lock: lock,
        fs,
        store_id: options.caches.new_store_id(),
        caches: options.caches,
        disable_wal: options.disable_wal,
        memtable: std::cell::RefCell::new(std::rc::Rc::new(std::cell::RefCell::new(memtable))),
        next_seq: std::cell::Cell::new(next_seq),
        log: std::cell::RefCell::new(log),
        dir: dir.to_path_buf(),
        format_version,
        manifest: std::cell::RefCell::new(Manifest {
            filename: manifest_filename,
            marker_iter: manifest_marker_iter,
        }),
        markers: std::cell::RefCell::new(markers),
        max_manifest_bytes: options.max_manifest_bytes,
        versions: std::cell::RefCell::new(versions),
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

/// The MANIFEST a store appends to, and the marker that names it.
struct Manifest {
    /// Its file name.
    filename: String,
    /// The iteration of `marker.manifest.*` that points at it.
    marker_iter: u64,
}

/// The log a store is writing to.
struct Log<D: Disk> {
    disk: std::rc::Rc<D>,
    log_num: u64,
    /// Where the next record goes.
    at: usize,
}

/// What replaying the logs established.
struct Replayed {
    /// The log that is still open for writing, and how far into it the valid
    /// records reach.
    tail: Option<(u64, usize)>,
    /// The highest sequence number any replayed batch used.
    highest_seq: u64,
}

/// Replays every log the version edits leave unflushed, in order.
///
/// A torn record at the tail of the **last** log is the ordinary shape of a
/// crash: the replay stops there and the store carries on writing from that
/// point, overwriting what was half-written. A torn record in an earlier log
/// is corruption, because a later log exists only if that one was complete.
///
/// The rule as written tolerates a torn record anywhere in the last log, not
/// only at its tail. Telling a torn tail from damage in the middle of the
/// same file would mean deciding what the bytes after it were meant to be,
/// and the answer that loses no data either way is to stop at the last record
/// that reads (§6).
async fn replay_logs<F: Fs>(
    fs: &F,
    dir: &Path,
    names: &[String],
    min_unflushed_log_num: u64,
    memtable: &mut crate::memtable::Memtable,
) -> Result<Replayed> {
    let mut logs: Vec<(u64, &String)> = names
        .iter()
        .filter_map(|n| files::parse_filename(n).map(|p| (p, n)))
        .filter(|(p, _)| p.file_type == FileType::Log && p.file_num >= min_unflushed_log_num)
        .map(|(p, n)| (p.file_num, n))
        .collect();
    logs.sort();

    let mut out = Replayed {
        tail: None,
        highest_seq: 0,
    };
    let last = logs.len().saturating_sub(1);
    for (i, (log_num, name)) in logs.iter().enumerate() {
        let disk = fs.open(&dir.join(name), Access::ReadOnly).await?;
        let bytes = read_all(&disk, Default::default()).await?;
        let mut reader = RecordReader::new(&bytes, *log_num as u32);

        let mut good = 0usize;
        loop {
            match reader.next() {
                None => {
                    good = reader.offset();
                    break;
                }
                Some(Ok(record)) => {
                    // A batch's own header says what sequence number it was
                    // committed at; replay must use it rather than inventing
                    // one, or a key would come back with a different age than
                    // it was written with.
                    let base = roble_format::batch::seq_num_of(&record)?;
                    let next = memtable.apply(&record, base)?;
                    out.highest_seq = out.highest_seq.max(next.saturating_sub(1));
                    good = reader.offset();
                }
                Some(Err(e)) => {
                    if i == last {
                        // The tail of the last log: a crash, not corruption.
                        //
                        // Untested here, with a reason rather than a test
                        // (§12). Reaching it needs a log whose final record
                        // was cut mid-write, which is a crash scenario rather
                        // than a malformed directory — the simulated
                        // filesystem can stop a write anywhere, and that is
                        // where this belongs, beside "reopening yields the
                        // state as of the last synced write". Pinning it from
                        // a hand-built log here would check the branch
                        // without checking the claim.
                        break;
                    }
                    return Err(Error::Corrupt(format!(
                        "{name}: {e}. A later log exists, so this one was complete when it \
                         was written, and what it holds cannot be told from what it lost"
                    )));
                }
            }
        }
        if i == last {
            out.tail = Some((*log_num, good));
        }
    }
    Ok(out)
}

// Written out rather than derived: the lock has no business in a debug line,
// and a `Fs::Lock` is not required to be `Debug` at all.
impl<F: Fs> std::fmt::Debug for Opened<'_, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opened")
            .field("dir", &self.dir)
            .field("format_version", &self.format_version)
            .field("manifest_filename", &self.manifest.borrow().filename)
            .field("markers", &self.markers.borrow())
            .field("versions", &self.versions)
            .finish()
    }
}

impl<F: Fs> Opened<'_, F> {
    /// Appends a batch to the log, syncs it if asked, and then applies it to
    /// the memtable.
    ///
    /// That order is the whole point: **nothing is readable that is not
    /// recoverable** (§6). `sync` decides whether the write is durable when
    /// this returns or merely written — without it the batch survives the
    /// store going away but not the machine.
    ///
    /// With the WAL off there is no log step and `sync` is refused rather
    /// than ignored: nothing but `flush` can make a write durable, and
    /// saying otherwise would be the silent half of a data loss.
    pub async fn write(&self, batch: &mut roble_format::batch::Batch, sync: bool) -> Result<()> {
        // Refused before anything is stamped, so a write that cannot keep
        // its promise changes nothing. Pebble refuses this too, and the
        // reason is the same: with no log there is nowhere for a sync to
        // make the batch durable, and returning `Ok` would be a promise of
        // durability that only `flush` can keep (§4).
        if sync && self.disable_wal {
            return Err(Error::Unsupported(format!(
                "{} was opened with the WAL off, so a write cannot be made durable on its \
                 own; flush instead",
                self.dir.display()
            )));
        }

        let base = self.next_seq.get();
        batch.set_seq_num(base);

        if !self.disable_wal {
            // The log, with the borrow released before any waiting: a
            // `RefCell` held across an await is a panic waiting for the
            // first reader that arrives in the middle of a write.
            let Some((disk, log_num, at)) = self
                .log
                .borrow()
                .as_ref()
                .map(|l| (std::rc::Rc::clone(&l.disk), l.log_num, l.at))
            else {
                return Err(Error::Unsupported(format!(
                    "{} holds no log file, so a write could not be recovered",
                    self.dir.display()
                )));
            };

            let mut writer = roble_format::record::RecordWriter::resuming(log_num as u32, at);
            writer.write_record(batch.as_bytes())?;
            let framed = writer.as_bytes().to_vec();
            let end = writer.position();

            let (result, _) = disk
                .write(
                    crate::buf::WriteBuf::new(framed),
                    at as u64,
                    Default::default(),
                )
                .await;
            result?;
            if sync {
                disk.sync(Default::default()).await?;
            }
            if let Some(log) = self.log.borrow_mut().as_mut() {
                log.at = end;
            }
        }

        let next = self.memtable().borrow_mut().apply(batch.as_bytes(), base)?;
        self.next_seq.set(next);
        Ok(())
    }

    /// The highest sequence number any read may see.
    pub(crate) fn snapshot(&self) -> u64 {
        self.next_seq.get().saturating_sub(1)
    }

    /// The memtable a read should use, which a flush may replace afterwards.
    pub(crate) fn memtable(&self) -> std::rc::Rc<std::cell::RefCell<crate::memtable::Memtable>> {
        std::rc::Rc::clone(&self.memtable.borrow())
    }

    /// The log this store is appending to, if it has one.
    ///
    /// Not the same as the MANIFEST's `min_unflushed_log_num`, which can name
    /// a log that was never created — a database Pebble wrote with the WAL
    /// disabled has it at zero. The store writes to the newest log it
    /// replayed.
    pub fn current_log(&self) -> Option<u64> {
        self.log.borrow().as_ref().map(|l| l.log_num)
    }

    /// How many bytes of writes no table holds yet.
    ///
    /// What a caller watches to decide when to flush: §8 sizes a memtable at
    /// 4 MiB, to match Oxia.
    pub fn unflushed_bytes(&self) -> usize {
        self.memtable().borrow().bytes()
    }

    /// The MANIFEST this store is appending to.
    pub fn manifest_filename(&self) -> String {
        self.manifest.borrow().filename.clone()
    }

    /// Every `marker.` file the directory holds, sorted, current and obsolete
    /// alike.
    pub fn markers(&self) -> Vec<String> {
        self.markers.borrow().clone()
    }

    /// Which iteration of `marker.manifest.*` names the current MANIFEST.
    pub(crate) fn manifest_marker_iter(&self) -> u64 {
        self.manifest.borrow().marker_iter
    }

    /// How large the MANIFEST may grow before a flush replaces it
    /// (`crate::rotate`), as [`crate::Options`] was asked for.
    pub fn max_manifest_bytes(&self) -> u64 {
        self.max_manifest_bytes
    }

    /// Points the store at a MANIFEST the marker now names.
    ///
    /// Called only after the marker has moved, and deliberately infallible:
    /// past that point an append that still went to the old file would be an
    /// append a reopen never sees.
    pub(crate) fn install_manifest(&self, filename: String, marker_iter: u64, marker: String) {
        *self.manifest.borrow_mut() = Manifest {
            filename,
            marker_iter,
        };
        let mut markers = self.markers.borrow_mut();
        markers.push(marker);
        markers.sort();
    }

    /// Drops names the obsolete sweep removed from the marker list.
    pub(crate) fn forget_markers(&self, removed: &[String]) {
        self.markers.borrow_mut().retain(|m| !removed.contains(m));
    }

    /// The filesystem this store's files live in.
    pub(crate) fn fs(&self) -> &F {
        self.fs
    }

    /// The version set, for a flush to add to.
    pub(crate) fn versions_mut(&self) -> std::cell::RefMut<'_, VersionSet> {
        self.versions.borrow_mut()
    }

    /// Puts a fresh memtable in front, leaving any open scan on the old one.
    pub(crate) fn install_fresh_memtable(&self, fresh: crate::memtable::Memtable) {
        *self.memtable.borrow_mut() = std::rc::Rc::new(std::cell::RefCell::new(fresh));
    }

    /// Whether this store was opened with the WAL off.
    pub fn wal_disabled(&self) -> bool {
        self.disable_wal
    }

    /// Points the store at a different log, which must already exist.
    pub(crate) async fn install_log(&self, log_num: u64) -> Result<()> {
        let path = self.dir.join(roble_format::files::log_filename(log_num));
        let disk = self.fs.open(&path, Access::ReadWrite).await?;
        *self.log.borrow_mut() = Some(Log {
            disk: std::rc::Rc::new(disk),
            log_num,
            at: 0,
        });
        Ok(())
    }

    /// What the MANIFEST says, as it stands.
    pub fn versions(&self) -> std::cell::Ref<'_, VersionSet> {
        self.versions.borrow()
    }

    /// The caches this store draws on, for tests that check what they hold.
    pub fn caches_for_test(&self) -> &crate::cache::Caches<F> {
        &self.caches
    }

    /// The open reader for one table, from the cache or freshly opened.
    ///
    /// Opening one reads its footer, metaindex, properties, index, filter and
    /// tombstones — never the whole file — and the reader keeps the file open
    /// for the data blocks that follow.
    pub(crate) async fn open_table(
        &self,
        file_num: u64,
    ) -> Result<std::rc::Rc<crate::table::OpenTable<F::Disk>>> {
        if let Some(held) = self.caches.table(self.store_id, file_num) {
            return Ok(held);
        }
        let path = self.dir.join(roble_format::files::table_filename(file_num));
        let disk = self.fs.open(&path, Access::ReadOnly).await?;
        let table = std::rc::Rc::new(
            crate::table::OpenTable::open(file_num, disk, self.caches.blocks(self.store_id))
                .await?,
        );
        self.caches
            .hold_table(self.store_id, file_num, std::rc::Rc::clone(&table));
        Ok(table)
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
        for m in self.markers.borrow().iter() {
            let _ = writeln!(out, "marker {m}");
        }
        let _ = writeln!(out, "format-version {}", self.format_version);
        for (level, tables) in self.versions.borrow().version.levels() {
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
