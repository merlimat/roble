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
//! 3. Write a database, if there is none and the caller asked for one
//!    (`crate::create`). Under the lock, so two opens racing to create one
//!    directory cannot both write it, and before the format version is read,
//!    because a directory with no database has no format version either. The
//!    listing is taken again afterwards: the creation changed what the
//!    directory holds, and every step below still answers from one listing.
//! 4. Read `marker.format-version.*`, and refuse a version above the pin
//!    **before** trusting any structure inside the files.
//! 5. Read `marker.manifest.*`, which names the MANIFEST to replay. No marker
//!    means no database.
//! 6. Replay that MANIFEST into a version, then check the comparer.
//! 7. Replay the logs the version leaves unflushed, so every acknowledged
//!    write is back in the memtable before anyone can read.
//! 8. Collect what the MANIFEST no longer refers to, which is where a flush
//!    interrupted between its table and its edit is cleaned up.
//!
//! Step 4 before step 6 is the part worth keeping: a database written above
//! the pin must be refused for being above the pin, not for whatever its
//! MANIFEST happens to trip over first.
//!
//! [`open_read_only`] walks the same steps without 1, 3 and 8: it takes no
//! lock, creates nothing and deletes nothing, so it can read a directory
//! another process is writing to. What that costs is that the writer may
//! retire a file the reader still needs, which the reader reports as
//! [`Error::Stale`] rather than as damage (DESIGN.md §6, "Reading beside a
//! writer").

use std::io;
use std::path::{Path, PathBuf};

use roble_format::files::{
    self, FORMAT_VERSION_MIN, FORMAT_VERSION_PIN, FileType, MARKER_FORMAT_VERSION, MARKER_MANIFEST,
};
use roble_format::manifest::VersionEdit;
use roble_format::record::RecordReader;

use crate::env::{Access, Disk, Env, Fs, read_all};
use crate::error::{Error, Result};
use crate::version::VersionSet;

/// The first sequence number a key may be given, Pebble's `base.SeqNumStart`.
///
/// Zero means "no key below this one", which a compaction may stamp onto a
/// key it has proven to be the last of its kind, and 1 to 9 Pebble reserves
/// for uses it has not found yet. So the first key written to a database
/// Pebble made is numbered 10, and Pebble clamps up to it when it loads a
/// MANIFEST that says less — `version_set.go`:
///
/// ```text
/// if ve.LastSeqNum+1 < base.SeqNumStart {
///     vs.logSeqNum.Store(base.SeqNumStart)
/// ```
///
/// roble clamps in the same place and for the same reason. Without it a
/// database with nothing committed yet — one `crate::create` just wrote, or
/// one Pebble created and never wrote to — would number its first key 1, and
/// roble would be writing keys into the range Pebble has set aside while
/// Pebble, handed the same directory, would not. The number is not
/// importable: `base` is one of Pebble's internal packages, so what pins it
/// is `harness/db-fixture`, whose `db-created` fixture is a database Pebble
/// made and wrote one key to.
const SEQ_NUM_START: u64 = 10;

/// An open database: its directory locked, its MANIFEST and logs replayed.
///
/// Made by [`open`], [`open_with`] or [`open_read_only`]. Close it with
/// [`Db::close`], which waits for the store's background work to stop and
/// then releases the directory; or just drop it, which stops that work and
/// releases the directory at once if none is in flight, or as soon as the
/// flush or merge in flight has finished.
///
/// Generic over the filesystem it was opened in, which is [`crate::FileFs`]
/// unless an [`Env`] says otherwise, and so is written plain `Db` there; the
/// [`Scan`](crate::Scan)s, [`Snapshot`](crate::Snapshot)s and batches it makes
/// carry the same parameter with the same default.
///
/// It is `!Send` and `!Sync`, and not `Clone`: a store belongs to one thread
/// (§10). To share one between tasks, put it in an [`Rc`](std::rc::Rc).
///
/// **It flushes and compacts in the background.** A store opened to write
/// spawns two tasks through [`Env::spawn`]: one writes a memtable out when it
/// is nearly full, so a write rarely waits for a flush, and one runs the
/// merges the picker chooses, so a write never pays for one. They hold the
/// store's state, not this handle, and stop when it is closed or dropped.
pub struct Db<F: Fs = crate::FileFs> {
    /// The state, shared with the background tasks.
    pub(crate) store: std::rc::Rc<Store<F>>,
    /// Whether this is the caller's handle, whose drop shuts the store down,
    /// or one of the handles a background task holds.
    owner: bool,
}

/// What a [`Db`] is a handle on: everything an open store holds.
///
/// Behind an `Rc` so that the background tasks can hold it beyond any borrow
/// of the caller's handle (`Env::spawn` takes a `'static` future).
pub(crate) struct Store<F: Fs> {
    // Held, not used: dropping it is what releases the directory. None for a
    // store opened read-only, which takes no lock, and taken early by a
    // handle dropped while nothing is in flight ([`Db`]'s `Drop`).
    _lock: std::cell::RefCell<Option<F::Lock>>,
    /// How many background tasks are still running, and who is waiting for
    /// that to reach zero ([`Db::close`]).
    tasks: std::cell::Cell<usize>,
    tasks_done: crate::change::Waiters,
    /// How many background tasks are inside a flush or a merge right now:
    /// what decides whether dropping the caller's handle may release the
    /// directory at once.
    working: std::cell::Cell<usize>,
    /// Whether this store was opened by [`open_read_only`], and so refuses
    /// everything that would change the directory.
    read_only: bool,
    /// The filesystem the database's files are opened in.
    ///
    /// Owned rather than borrowed, so that a store is a `'static` value and
    /// can be put in an `Rc` and handed to a task of its own (§10). An `Fs`
    /// is a handle — `Clone` is part of the trait — so this costs what
    /// cloning that handle costs, which for the two here is nothing.
    fs: F,
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
    /// The memtable a flush is writing out, if there is one.
    ///
    /// Filled at the start of a flush and emptied once the edit naming the
    /// table has landed. What it buys is that a write arriving in the middle
    /// of a flush goes to a different memtable and a different log, and so
    /// is neither left out of the table being built nor retired with the log
    /// that edit replaces (§5, `crate::flush`).
    ///
    /// A flush that fails leaves it filled, and the next flush writes *it*
    /// out rather than freezing again. Everything in it is older than
    /// everything in the memtable in front, so a table built from it holds a
    /// prefix of what this store has been told — which is what makes the log
    /// the edit retires the right one.
    frozen: std::cell::RefCell<Option<std::rc::Rc<std::cell::RefCell<crate::memtable::Memtable>>>>,
    /// The log this store appends to, if it has one.
    log: std::cell::RefCell<Option<Log<F::Disk>>>,
    /// Whether this store was opened with the WAL off, so that `flush` is
    /// the only durability point (§4).
    disable_wal: bool,
    /// Whether every write syncs the log before it returns
    /// ([`crate::Wal::Synced`]).
    sync_writes: bool,
    /// Whether a flush leaves the picker's compaction for the caller (§9).
    disable_automatic_compactions: bool,
    /// The next sequence number to hand out.
    ///
    /// Starts past the MANIFEST's last, so nothing written here can collide
    /// with what the tables already hold.
    next_seq: std::cell::Cell<u64>,
    /// The directory the database lives in.
    pub(crate) dir: PathBuf,
    /// The format major version it was written in.
    format_version: u64,
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
    /// How many bytes a memtable holds before a write writes it out (§4).
    memtable_size: u64,
    /// How large the MANIFEST may grow before a flush replaces it.
    max_manifest_bytes: u64,
    /// How many readers are open for each of this store's tables, which is
    /// what keeps the sweep from deleting one out from under a scan (§5,
    /// `crate::obsolete`).
    readers: std::rc::Rc<crate::obsolete::Readers>,
    /// The output tables a compaction has written and no version names yet,
    /// which is what keeps the sweep off a file the edit is about to name
    /// (§5, `crate::obsolete`).
    pending: crate::obsolete::Pending,
    /// The versions a scan or a snapshot still holds after an edit replaced
    /// them, which is what keeps the sweep off the tables they will open
    /// (§5, `crate::obsolete`).
    pinned: crate::obsolete::Pinned,
    /// Whether an append to the MANIFEST has failed, after which this store
    /// records nothing more (`crate::flush`).
    manifest_failed: std::cell::Cell<bool>,
    /// The one change at a time this store makes to what it is made of
    /// (§5, `crate::change`).
    changes: crate::change::Changes,
    /// The writes that have chosen a memtable and not yet applied to it,
    /// which a freeze has to wait for (§5, `crate::change`).
    writers: crate::change::Writers,
    /// The tables the compaction underway is reading, which is what keeps
    /// two compactions off the same ones now that neither holds the permit
    /// across its merge (§9, `crate::change`).
    compacting: crate::change::Compacting,
    /// Who is waiting to be told there is a compaction worth running (§9).
    compaction_waiters: crate::change::Waiters,
    /// Who is waiting to be told there is a memtable worth writing out (§5).
    ///
    /// Apart from the compaction waiters, because the two have to be able to
    /// run at once: a merge is the longest thing a store does, and a flush
    /// that queued behind one would be a write queueing behind it a memtable
    /// later.
    flush_waiters: crate::change::Waiters,
    /// Whether the last flush failed, in which case there is nothing new for
    /// a task to write out until the store has taken another write (§5).
    flush_failed: std::cell::Cell<bool>,
    /// Whether the caller has said it will run nothing more beside this
    /// store, so the tasks waiting above can end and let go of it (§9).
    stopped: std::cell::Cell<bool>,
    /// Whether the last compaction failed, in which case the picker's answer
    /// is not worth asking for again until the version changes (§9).
    compaction_failed: std::cell::Cell<bool>,
    /// What this store records, if anything (`crate::metrics`).
    ///
    /// Shared rather than owned outright because a scan outlives the call
    /// that made it and counts the keys it steps onto as it goes.
    pub(crate) metrics: std::rc::Rc<crate::metrics::Metrics>,
    /// How the store opens its tables, shared with every scan and snapshot
    /// it makes, which open theirs as they reach them (`crate::scan`).
    tables: std::rc::Rc<Tables<F>>,
    /// The total of the entries the tables the version names are known to
    /// hold ([`Tables::entries`]).
    records_in_tables: std::cell::Cell<u64>,
    /// What the picker allows L0 and L1 to hold (`crate::pick`).
    l0_compaction_threshold: u64,
    /// How many tables L0 may hold before a write waits for it to drain.
    l0_stop_writes_threshold: u64,
    lbase_max_bytes: u64,
    target_file_size: u64,
    /// What a compaction's tables are compressed with.
    compression: roble_format::block::Codec,
    compaction_unit: u64,
    /// What replaying the MANIFEST established, and what a flush adds to.
    versions: std::cell::RefCell<VersionSet>,
}

/// Opens the database in `dir` with the defaults.
///
/// A store that shares a thread with others, or that wants Oxia's
/// configuration, is opened with [`open_with`] instead (§8, §11).
pub async fn open<E: Env>(env: &E, dir: impl AsRef<Path>) -> Result<Db<E::Fs>> {
    open_with(env, dir, crate::options::Options::default()).await
}

/// Opens the database in `dir` as `options` says.
pub async fn open_with<E: Env>(
    env: &E,
    dir: impl AsRef<Path>,
    options: crate::options::Options<E::Fs>,
) -> Result<Db<E::Fs>> {
    open_dir(env, dir.as_ref(), options, false).await
}

/// Opens the database in `dir` to read, beside whatever process may be
/// writing to it.
///
/// It takes no lock, and it creates, writes and deletes nothing in the
/// directory: `create_if_missing` is not acted on, the logs are replayed into
/// a memtable of its own, and nothing obsolete is swept. Everything that
/// would change the directory — a write, a flush, a compaction, a rotation,
/// a checkpoint, a sweep, a destroy — fails with [`Error::ReadOnly`].
///
/// It reads the database as of the open, and does not follow the writer
/// after it. The writer does not know it is there, so it may delete a table
/// or a log this store still needs; the open or the read that finds it gone
/// fails with [`Error::Stale`], and the answer is to open again (DESIGN.md
/// §6, "Reading beside a writer").
pub async fn open_read_only<E: Env>(
    env: &E,
    dir: impl AsRef<Path>,
    options: crate::options::Options<E::Fs>,
) -> Result<Db<E::Fs>> {
    open_dir(env, dir.as_ref(), options, true).await
}

async fn open_dir<E: Env>(
    env: &E,
    dir: &Path,
    options: crate::options::Options<E::Fs>,
    read_only: bool,
) -> Result<Db<E::Fs>> {
    // Cloned, not borrowed: the store outlives this call and is meant to be
    // ownable by a task of its own (§10).
    let fs = env.fs().clone();
    let create_if_missing = options.create_if_missing && !read_only;

    // The directory itself, when the caller has said it may not be there.
    // Ahead of the lock because the lock is a file inside it; an open that
    // was not asked to create anything is left to fail on the lock, which is
    // what a mistyped path should do.
    if create_if_missing {
        fs.mkdir_all(dir).await?;
    }

    // 1. The lock comes first, so nothing below races a second owner. A
    //    reader takes none: the owner is exactly who it means to read beside.
    let lock = if read_only {
        None
    } else {
        Some(fs.lock(&dir.join("LOCK")).await?)
    };

    // 2. One listing, and every question answered from it.
    let entries = fs.list(dir).await?;
    let mut names: Vec<String> = entries
        .iter()
        .map(|e| e.name.to_string_lossy().into_owned())
        .collect();

    refuse_unreadable_files(&names)?;

    // 3. A database, if the caller asked for one and there is none to find.
    //    The marker is what says which: it is the same question step 4 asks,
    //    and asking it here rather than acting on step 4's refusal is what
    //    keeps "there is no database" apart from the several other reasons a
    //    directory can be refused.
    if create_if_missing
        && files::current_marker(names.iter().map(String::as_str), MARKER_MANIFEST)?.is_none()
    {
        crate::create::create(&fs, dir, &names).await?;
        names = fs
            .list(dir)
            .await?
            .iter()
            .map(|e| e.name.to_string_lossy().into_owned())
            .collect();
    }

    // 4. The format version, before anything inside a file is trusted.
    let format_version = read_format_version(&names)?;

    // 5. Which MANIFEST.
    let manifest_marker = files::current_marker(names.iter().map(String::as_str), MARKER_MANIFEST)?
        .ok_or_else(|| {
            Error::NoDatabase(format!(
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

    // 6. Replay it.
    let edits = read_version_edits(
        &fs,
        &dir.join(&manifest_filename),
        parsed.file_num,
        read_only,
    )
    .await?;
    let versions = VersionSet::replay(&edits)?;

    // Every table the version names has to be in the directory. A checkpoint
    // that was streamed short, or a file deleted by hand, shows up here rather
    // than as a missing key much later.
    //
    // Not for a reader: the listing was taken before the MANIFEST was read,
    // so a flush in between names a table the listing never saw. A table
    // that really has gone shows up when a read opens it, as stale.
    let present: std::collections::BTreeSet<u64> = names
        .iter()
        .filter_map(|n| files::parse_filename(n))
        .filter(|p| p.file_type == FileType::Table)
        .map(|p| p.file_num)
        .collect();
    for (level, tables) in versions.version.levels() {
        for t in tables {
            if !read_only && !present.contains(&t.file_num) {
                return Err(Error::Corrupt(format!(
                    "L{level} table {} is in the MANIFEST but not in {}",
                    t.file_num,
                    dir.display()
                )));
            }
        }
    }

    // 7. Replay the logs the version edits leave unflushed.
    let mut memtable = crate::memtable::Memtable::with_budget(
        versions.last_seq_num.wrapping_add(1),
        options.memtable_size as usize,
    );
    let replayed = replay_logs(
        &fs,
        dir,
        &names,
        versions.min_unflushed_log_num,
        &mut memtable,
        read_only,
    )
    .await?;

    // 8. What the replayed MANIFEST no longer refers to: the logs it has
    //    moved past, and any table a flush wrote whose edit never landed.
    //    This is where an interrupted flush is collected — the table was
    //    written and synced before the edit, so a crash in between leaves a
    //    file in no version (`crate::obsolete`). It runs after the replay,
    //    so no log it deletes is one the replay still needed, and its
    //    failure is swallowed: a leftover file is no reason to refuse a
    //    database that reads.
    //    A reader skips it: what is obsolete is the writer's to delete.
    let swept = if read_only {
        Vec::new()
    } else {
        let live = crate::obsolete::Live::of(&versions, parsed.file_num, &names)?;
        // No reader can be open yet: the store being opened has made none,
        // and another process holding the directory is what the lock
        // refuses.
        crate::obsolete::remove_obsolete(
            &fs,
            dir,
            &names,
            &live,
            &std::collections::BTreeSet::new(),
        )
        .await
        .unwrap_or_default()
    };

    // The next sequence number never goes backwards: a write replay brought
    // back must not be overwritten by one numbered the same (§6). And never
    // below Pebble's floor, which is the arithmetic below reaching into a
    // range Pebble has set aside.
    let next_seq = versions
        .last_seq_num
        .max(replayed.highest_seq)
        .saturating_add(1)
        .max(SEQ_NUM_START);

    // The log the store carries on writing to, opened where the replay
    // stopped. Overwriting a torn tail is the point: those bytes were never
    // acknowledged.
    //
    // With the WAL off there is none, which is not the same as having
    // skipped the replay above: the records already in a log were
    // acknowledged when they were written, whatever this store has been
    // asked to do from here (§6).
    //
    // A reader opens it only to read, so that it says which log is current;
    // nothing it does appends to it.
    let log = match replayed.tail.filter(|_| options.wal != crate::Wal::Off) {
        Some((log_num, at)) => {
            let path = dir.join(roble_format::files::log_filename(log_num));
            let access = if read_only {
                Access::ReadOnly
            } else {
                Access::ReadWrite
            };
            let disk = fs
                .open(&path, access)
                .await
                .map_err(retired(read_only, &path))?;
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

    let metrics = std::rc::Rc::new(crate::metrics::Metrics::new(&crate::metrics::Meter::new(
        &options, dir,
    )));
    metrics.saw_version(&versions.version);
    metrics.memtable_size.set(memtable.bytes() as i64);
    if metrics.recording()
        && let Ok(usage) = fs.disk_usage(dir).await
    {
        metrics.disk_size.set(usage.used_bytes as i64);
    }

    let store_id = options.caches.new_store_id();
    let readers = std::rc::Rc::new(crate::obsolete::Readers::default());
    let tables = std::rc::Rc::new(Tables {
        fs: fs.clone(),
        dir: dir.to_path_buf(),
        caches: options.caches.clone(),
        store_id,
        metrics: std::rc::Rc::clone(&metrics),
        readers: std::rc::Rc::clone(&readers),
        entries: Default::default(),
        read_only,
    });
    let db = Db {
        store: std::rc::Rc::new(Store {
            _lock: std::cell::RefCell::new(lock),
            tasks: std::cell::Cell::new(0),
            tasks_done: Default::default(),
            working: std::cell::Cell::new(0),
            read_only,
            metrics,
            fs,
            store_id,
            caches: options.caches,
            disable_wal: options.wal == crate::Wal::Off,
            sync_writes: options.wal == crate::Wal::Synced,
            disable_automatic_compactions: options.disable_automatic_compactions,
            memtable: std::cell::RefCell::new(std::rc::Rc::new(std::cell::RefCell::new(memtable))),
            frozen: std::cell::RefCell::new(None),
            writers: Default::default(),
            compacting: Default::default(),
            compaction_waiters: Default::default(),
            compaction_failed: std::cell::Cell::new(false),
            flush_waiters: Default::default(),
            flush_failed: std::cell::Cell::new(false),
            stopped: std::cell::Cell::new(false),
            next_seq: std::cell::Cell::new(next_seq),
            log: std::cell::RefCell::new(log),
            dir: dir.to_path_buf(),
            format_version,
            manifest: std::cell::RefCell::new(Manifest {
                filename: manifest_filename,
                marker_iter: manifest_marker_iter,
            }),
            tables,
            records_in_tables: std::cell::Cell::new(0),
            markers: std::cell::RefCell::new(markers),
            memtable_size: options.memtable_size,
            max_manifest_bytes: options.max_manifest_bytes,
            readers,
            pending: Default::default(),
            pinned: Default::default(),
            manifest_failed: std::cell::Cell::new(false),
            changes: crate::change::Changes::default(),
            l0_compaction_threshold: options.l0_compaction_threshold,
            l0_stop_writes_threshold: options.l0_stop_writes_threshold,
            lbase_max_bytes: options.lbase_max_bytes,
            compaction_unit: options.compaction_unit,
            target_file_size: options.target_file_size,
            compression: options.compression,
            versions: std::cell::RefCell::new(versions),
        }),
        owner: true,
    };
    if !read_only && options.background_tasks {
        db.spawn_background(env);
    }
    Ok(db)
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

/// Turns a file that has gone into [`Error::Stale`], for a reader.
///
/// A store with the lock deletes only what no version it can still read
/// names, so a file it needs and cannot find is damage, and stays an I/O
/// error. A reader holds no lock, and the file it cannot find is one the
/// writer retired after the reader read the version naming it.
pub(crate) fn retired(read_only: bool, path: &Path) -> impl FnOnce(Error) -> Error + '_ {
    move |e| match e {
        Error::Io(e) if read_only && e.kind() == io::ErrorKind::NotFound => Error::Stale(format!(
            "{} is gone: the process writing the database retired it",
            path.display()
        )),
        e => e,
    }
}

/// Reads every version edit in a MANIFEST.
///
/// A reader may find the writer halfway through appending to it, so for a
/// reader a record that does not read is stale rather than corrupt: opening
/// again reads it whole, or, if it is damage, keeps failing on it.
async fn read_version_edits<F: Fs>(
    fs: &F,
    path: &Path,
    file_num: u64,
    read_only: bool,
) -> Result<Vec<VersionEdit>> {
    let disk = fs
        .open(path, Access::ReadOnly)
        .await
        .map_err(retired(read_only, path))?;
    let bytes = read_all(&disk, Default::default()).await?;

    // The file number is passed even though a MANIFEST's chunks are legacy and
    // carry none, so that one which turns out to be recyclable is checked
    // rather than waved through.
    let reader = RecordReader::new(&bytes, file_num as u32);
    let mut edits = Vec::new();
    for record in reader {
        let record = record.map_err(|e| {
            let m = format!("{}: record {}: {e}", path.display(), edits.len());
            if read_only {
                Error::Stale(m)
            } else {
                Error::Corrupt(m)
            }
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
///
/// A reader is beside a writer that may still be appending to a log that is
/// no longer the last: a flush creates the next log before the writes in
/// flight on the old one have finished. So for a reader a torn record in an
/// earlier log is stale rather than corrupt, and so is an earlier log that
/// grew while the later ones were read — what it gained is older than what
/// they hold, and replaying one without the other would be a hole.
async fn replay_logs<F: Fs>(
    fs: &F,
    dir: &Path,
    names: &[String],
    min_unflushed_log_num: u64,
    memtable: &mut crate::memtable::Memtable,
    read_only: bool,
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
    // The earlier logs a reader replayed, and how long each was when it did.
    let mut earlier = Vec::new();
    for (i, (log_num, name)) in logs.iter().enumerate() {
        let path = dir.join(name);
        let disk = fs
            .open(&path, Access::ReadOnly)
            .await
            .map_err(retired(read_only, &path))?;
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
                    if read_only {
                        return Err(Error::Stale(format!(
                            "{name}: {e}, and a later log exists: the process writing the \
                             database may still have been appending to it"
                        )));
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
        } else if read_only {
            earlier.push((name, disk, bytes.len() as u64));
        }
    }
    for (name, disk, read) in earlier {
        if disk.len().await? != read {
            return Err(Error::Stale(format!(
                "{name} grew while the logs after it were read: the process writing the \
                 database was still appending to it"
            )));
        }
    }
    Ok(out)
}

// Written out rather than derived: the lock has no business in a debug line,
// and a `Fs::Lock` is not required to be `Debug` at all.
impl<F: Fs> std::fmt::Debug for Db<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("dir", &self.store.dir)
            .field("format_version", &self.store.format_version)
            .field("manifest_filename", &self.store.manifest.borrow().filename)
            .field("markers", &self.store.markers.borrow())
            .field("versions", &self.store.versions)
            .finish()
    }
}

impl<F: Fs> Db<F> {
    /// Sets `key` to `value`.
    ///
    /// A batch of that one write, appended to the log and then applied to
    /// the memtable, as durable when this returns as the store's
    /// [`Wal`](crate::Wal) setting makes every write.
    ///
    /// Several writes that have to land together, or that are read back
    /// before they do, go in an [`Db::indexed_batch`] instead. This one
    /// builds no index, which is about 90 ns a write cheaper.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = roble_format::batch::Batch::new();
        batch.put(key, value)?;
        self.write(&mut batch).await
    }

    /// Deletes `key`, as [`Db::put`] writes.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let mut batch = roble_format::batch::Batch::new();
        batch.delete(key)?;
        self.write(&mut batch).await
    }

    /// Deletes every key in `start..end`, as [`Db::put`] writes.
    pub async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<()> {
        let mut batch = roble_format::batch::Batch::new();
        batch.delete_range(start, end)?;
        self.write(&mut batch).await
    }

    /// Writes a batch as the store's [`Wal`](crate::Wal) setting says every
    /// write is made: synced for [`Wal::Synced`](crate::Wal::Synced), and
    /// not otherwise.
    pub(crate) async fn write(&self, batch: &mut roble_format::batch::Batch) -> Result<()> {
        self.write_raw(batch, self.store.sync_writes).await
    }

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
    ///
    /// What [`IndexedBatch::commit`](crate::IndexedBatch::commit) and
    /// [`Db::put`] go through, with `sync` from the store's
    /// [`Wal`](crate::Wal) setting. It takes the format's own batch, which
    /// roble's tests build directly, and a `sync` of its own, which they
    /// choose per write; a caller builds an [`Db::indexed_batch`].
    #[doc(hidden)]
    pub async fn write_raw(
        &self,
        batch: &mut roble_format::batch::Batch,
        sync: bool,
    ) -> Result<()> {
        let _timer = self.store.metrics.write.start();
        let result = self.write_inner(batch, sync).await;
        self.store.metrics.write_errors.saw(result)
    }

    /// The body of [`Db::write_raw`], so one place counts what it returned.
    async fn write_inner(&self, batch: &mut roble_format::batch::Batch, sync: bool) -> Result<()> {
        self.writable()?;
        // Refused before anything is stamped, so a write that cannot keep
        // its promise changes nothing. Pebble refuses this too, and the
        // reason is the same: with no log there is nowhere for a sync to
        // make the batch durable, and returning `Ok` would be a promise of
        // durability that only `flush` can keep (§4).
        if sync && self.store.disable_wal {
            return Err(Error::Unsupported(format!(
                "{} was opened with the WAL off, so a write cannot be made durable on its \
                 own; flush instead",
                self.store.dir.display()
            )));
        }

        // Room for this batch before it is accepted, which is the only thing
        // that bounds a store's memory: nothing else writes a memtable out
        // unless a caller asks (§4).
        //
        // Before the batch is applied rather than after, and that is the
        // whole of the ordering. A flush that fails here fails a write that
        // has not happened, so a caller handed the error has nothing of its
        // batch in the store and can simply try again. The other way round,
        // the write would be in the log and in the memtable and reported as
        // failed, and a caller doing the obvious thing would write bytes the
        // store already had.
        //
        // And before the count below, which is not a matter of taste: the
        // flush inside here freezes the memtable, and a freeze waits for
        // every write in flight to finish (§5, `crate::change`). A write
        // that had counted itself first would be waiting for itself.
        self.make_room().await?;
        self.drain_l0().await?;

        // Room made and L0 shallow enough, this write gives the thread up
        // once to whatever is flushing or merging beside it — which is how
        // the flush this write did *not* have to do gets to happen at all
        // (§9, [`Db::share_the_thread`]).
        self.share_the_thread().await;

        // Held from here to the end of the function, which is the span no
        // other write may share and no freeze may happen in: the sequence
        // number stamped below and the offset the record goes to are both
        // read here and both committed to after the disk has been waited on
        // (§5, `crate::change`).
        let _writing = self.store.writers.begin().await;

        let base = self.store.next_seq.get();
        // Every number the batch takes has to fit a key's trailer. Only a
        // database whose files claim some 2^56 writes gets anywhere near
        // this, so it is refused as theirs rather than numbered past the end.
        if base.saturating_add(u64::from(batch.count())) > roble_format::key::SEQ_NUM_MAX + 1 {
            return Err(Error::Corrupt(format!(
                "a batch of {} at sequence number {base} would run past the largest one a key holds",
                batch.count()
            )));
        }
        batch.set_seq_num(base);

        if !self.store.disable_wal {
            let _timer = self.store.metrics.wal.start();
            // The log, with the borrow released before any waiting: a
            // `RefCell` held across an await is a panic waiting for the
            // first reader that arrives in the middle of a write.
            let Some((disk, log_num, at)) = self
                .store
                .log
                .borrow()
                .as_ref()
                .map(|l| (std::rc::Rc::clone(&l.disk), l.log_num, l.at))
            else {
                return Err(Error::Unsupported(format!(
                    "{} holds no log file, so a write could not be recovered",
                    self.store.dir.display()
                )));
            };

            let mut writer = roble_format::record::RecordWriter::resuming(log_num as u32, at);
            writer.write_record(batch.as_bytes())?;
            let framed = writer.as_bytes().to_vec();
            let end = writer.position();

            let written = framed.len();
            let (result, _) = disk
                .write(
                    crate::buf::WriteBuf::new(framed),
                    at as u64,
                    Default::default(),
                )
                .await;
            result?;
            self.store.metrics.written.add(written);
            if sync {
                let _timer = self.store.metrics.sync_duration.start();
                disk.sync(Default::default()).await?;
            }
            if let Some(log) = self.store.log.borrow_mut().as_mut() {
                log.at = end;
            }
        }

        let next = self.memtable().borrow_mut().apply(batch.as_bytes(), base)?;
        self.store.next_seq.set(next);
        // The store holds something a flush did not, so a flush that failed
        // is worth asking for again (§5, [`Db::flush_wanted`]). This is
        // the only thing that changes that answer, which is why the retry
        // hangs off a write and not off a clock (§10).
        self.store.flush_failed.set(false);

        // Only when something is recording. Each of these would do nothing
        // with what it was handed otherwise, but the handing was not free: the
        // batch read a second time into a list, and every memtable's bytes
        // summed, on every write of a store nobody was metering.
        if self.store.metrics.recording() {
            for entry in iter_kinds(batch.as_bytes())? {
                self.store.metrics.write_operations[entry].add(1);
            }
            self.store
                .metrics
                .write_size
                .record(batch.as_bytes().len() as u64);
            self.store
                .metrics
                .write_bytes
                .add(batch.as_bytes().len() as u64);
            self.store
                .metrics
                .write_entries
                .record(batch.count() as u64);
            self.store
                .metrics
                .memtable_size
                .set(self.unflushed_bytes() as i64);
            self.saw_records();
        }
        Ok(())
    }

    /// Writes a memtable out if the store has reached its budget, so that
    /// the batch about to be applied has somewhere to go
    /// ([`crate::Options::memtable_size`]).
    ///
    /// **Every** memtable counts, the one a failed flush left frozen
    /// included (§5): the budget is what the store holds, not what one of
    /// its memtables holds. So the first write after a failed flush pays for
    /// the retry, which is the tighter bound and the sooner recovery both —
    /// and a store that cannot shed what it holds refuses writes rather than
    /// growing past the budget, which is where the write stall belongs (§9).
    ///
    /// One flush writes one memtable, and that is enough: what a frozen one
    /// holds arrived while the store was under the budget, so writing it out
    /// puts the store back under it.
    ///
    /// A loop, and not because one flush might not be enough. It is Pebble's
    /// `maybeInduceWriteStall`: a change already underway may be the very
    /// flush this write needs, so the thing to do is wait for it and then
    /// ask again rather than queue up to repeat it. The second look is the
    /// whole point — without it a write would take the permit the moment it
    /// was free and flush a memtable somebody else had just emptied.
    ///
    /// This is a `flush` and not half of one, so it also runs the compaction
    /// the picker chose (§9). A store filled only through `write` would
    /// otherwise pile L0 up for ever, which is the thing a flush was taught
    /// to prevent; a caller who would rather decide when to merge has
    /// [`crate::Options::disable_automatic_compactions`] for it.
    ///
    /// **Reaching the budget is the backstop and not the plan.** A store
    /// with a task waiting on [`Db::flush_wanted`] is told there is a
    /// memtable to write out an eighth of the budget before it fills
    /// ([`Db::share_the_thread`], [`Db::flush_line`]), and that task
    /// freezes it and writes it out while the writes carry on into the fresh
    /// one behind it. Only a store whose flushes have stopped keeping up —
    /// or one with nobody to hand them to — comes down here, and then it is
    /// this loop again.
    async fn make_room(&self) -> Result<()> {
        loop {
            if (self.unflushed_bytes() as u64) < self.store.memtable_size {
                return Ok(());
            }

            // Somebody else is already changing what the store is made of,
            // and it may be the very flush this write needs (§5). Waiting
            // for them and then looking again is Pebble's loop in
            // `maybeInduceWriteStall`, and it is the difference between a
            // write that waited and a write that waited and then did work
            // nobody needed: without the second look, this would queue for
            // the permit and flush a memtable the other task had just
            // emptied.
            //
            // **This** is the stall §9 names, and the flush below is not: a
            // write doing the work is a write getting on with it, and a
            // write waiting for somebody else's is a write that has stopped.
            if self.changes().underway() {
                self.stalled(crate::metrics::STALL_MEMTABLE).await;
                continue;
            }

            // Nothing to write is nothing this can do about the budget, and
            // saying so rather than trying again is what keeps a store whose
            // budget is smaller than an empty memtable from spinning here.
            if self.flush().await?.is_none() {
                return Ok(());
            }
        }
    }

    /// Tells whoever is waiting that there is a memtable to write out, and
    /// hands the thread back once if there is work on it that this write is
    /// otherwise about to run away from (§9, §10).
    ///
    /// After the room and the depth have been seen to, and not before: a
    /// write that gave the thread away first would come back to a store
    /// somebody else had just made room in, which is the right answer for
    /// the write and the wrong place to decide it — the two waits above
    /// are what §9 names as stalls, and they are what a store reports when
    /// it is behind.
    ///
    /// The second half is the part that is easy to leave out, and without it
    /// the first half does nothing. roble's background work is a task on the
    /// runtime that owns the store, and a task only runs when the thread is
    /// handed back to the executor. A write with the WAL off awaits nothing
    /// that is ever pending — no log append, no disk — so a writer in a loop
    /// polls to completion every time and the flush it asked for never gets
    /// a turn, nor does the merge after it. It would be found out at the
    /// budget, where the write waits for the flush it starved, and at L0's
    /// depth, where it waits for the merge.
    ///
    /// So a write that has work going on beside it gives the thread up once
    /// before taking the write permit — the same bargain a compaction makes
    /// every [`crate::Options::compaction_unit`] entries, from the other
    /// side. It costs a trip through the executor, which is why it is asked
    /// only while something is actually running: a store with nothing going
    /// on adds up what its memtables hold, reads two cells, and carries on.
    async fn share_the_thread(&self) {
        // Waking is what crossing the line *is*: the wait below it is
        // level-triggered, so whoever is parked only looks again when told,
        // and this is the write that has something new to tell them.
        let told = (self.unflushed_bytes() as u64) >= self.flush_line()
            && !self.store.flush_failed.get()
            && self.store.flush_waiters.wake();
        // A change underway is a flush — a compaction holds the permit only
        // to decide and to record (§9) — and a claim is the merge itself.
        if told || self.changes().underway() || self.compacting().underway() {
            crate::work::yield_now().await;
        }
    }

    /// How much a store holds before it starts wanting a memtable written
    /// out, which is an eighth below the budget it stops writing at (§5).
    ///
    /// The eighth is the room the writes that arrive during a flush go into,
    /// and [`crate::memtable::FLUSH_RESERVE`] says why it is an eighth and
    /// not a half. Crossing it does nothing at all unless somebody is
    /// waiting on [`Db::flush_wanted`] to be told: a store on its own
    /// goes on filling the whole budget for the one memtable, and writes the
    /// tables it did before there was anywhere else for a flush to run.
    #[doc(hidden)]
    pub fn flush_line(&self) -> u64 {
        self.store.memtable_size - self.store.memtable_size / crate::memtable::FLUSH_RESERVE
    }

    /// Waits until L0 is shallow enough to accept another table, draining it
    /// if nobody else is ([`crate::Options::l0_stop_writes_threshold`]).
    ///
    /// The other thing a write can be short of room for, and the reason it
    /// is worth waiting over: every read consults the whole of L0, so a
    /// store that let it grow without limit would be trading write latency
    /// it does not have for read latency it cannot bound (§9).
    ///
    /// Nothing happens here for a store told to leave compaction to its
    /// caller. It owns L0's depth as well, and a write that waited for a
    /// merge nobody was going to run would wait for ever — which is what
    /// Pebble does in that configuration, and is a worse answer than letting
    /// the caller have what it asked for.
    async fn drain_l0(&self) -> Result<()> {
        if self.automatic_compactions_disabled() {
            return Ok(());
        }
        loop {
            // A level cannot have more layers than tables, so one with fewer
            // tables than the line is not past it, and counting the layers —
            // a list of L0's tables built, sorted and compared pairwise — is
            // only for the store that might be. This runs on every write.
            if (self.version_set().version.level(0).len() as u64)
                < self.store.l0_stop_writes_threshold
            {
                return Ok(());
            }
            // In layers, which is what the line is counted in: a store
            // holding twelve tables a read crosses in one seek is not deep
            // (§9, `crate::pick::l0_depth`).
            let in_l0 = crate::pick::l0_depth(&self.version_set().version);
            if in_l0 < self.store.l0_stop_writes_threshold {
                return Ok(());
            }
            // Both of the things the merge this needs can be behind: the
            // permit, which a compaction holds to decide and to record, and
            // the claim it holds for the merge itself with no permit at all
            // (§9, `crate::change::Compacting`).
            if self.changes().underway() || self.compacting().underway() {
                self.stalled(crate::metrics::STALL_L0).await;
                continue;
            }
            // The picker prefers L0 at this depth, so the compaction it
            // chooses is the L0 one (`crate::pick`). Nothing to pick is
            // nothing this can do about the depth, and the alternative to
            // saying so is spinning on a version that will not change.
            if self.pick().is_none() {
                return Ok(());
            }
            // One merge and no more: an L0 compaction takes the whole of L0,
            // because its tables overlap each other, so there is nothing
            // left to look at again. The loop above is for the other branch,
            // where somebody else's merge is what this is waiting on.
            return self.compact_once().await.map(|_| ());
        }
    }

    /// Waits for the change already underway, and records that a write
    /// stopped for it (§9).
    ///
    /// Queued for the permit and then let go of again, because what a write
    /// wants here is not the permit but the state whoever holds it leaves
    /// behind. By the time it is handed over, the change that was underway
    /// has finished and the question is worth asking again.
    async fn stalled(&self, why: usize) {
        self.store.metrics.write_stalls[why].add(1);
        let _timer = self.store.metrics.write_stall[why].start();
        if self.changes().underway() {
            drop(self.changes().begin().await);
        } else {
            // A merge, which holds no permit: there is nothing to queue for,
            // only the claim to wait out (§9).
            self.compacting().settled().await;
        }
    }

    /// The highest sequence number any read may see.
    pub(crate) fn last_seq_num(&self) -> u64 {
        self.store.next_seq.get().saturating_sub(1)
    }

    /// The memtable a write goes to, which a flush may replace afterwards.
    pub(crate) fn memtable(&self) -> std::rc::Rc<std::cell::RefCell<crate::memtable::Memtable>> {
        std::rc::Rc::clone(&self.store.memtable.borrow())
    }

    /// The memtable a flush is writing out, if there is one.
    pub(crate) fn frozen(
        &self,
    ) -> Option<std::rc::Rc<std::cell::RefCell<crate::memtable::Memtable>>> {
        self.store.frozen.borrow().clone()
    }

    /// Every memtable a read has to look in: the one writes go to, and the
    /// one a flush is writing out if there is one.
    ///
    /// The order is for a reader's convenience and not for correctness.
    /// Entries win on the sequence numbers they carry, wherever they are
    /// found (§4) — which is also why a key held by both is not a problem to
    /// resolve here.
    pub(crate) fn memtables(
        &self,
    ) -> Vec<std::rc::Rc<std::cell::RefCell<crate::memtable::Memtable>>> {
        self.memtables_each().collect()
    }

    /// The same, without a vector to hold them.
    ///
    /// A read looks in these twice per lookup and wants neither copy, so the
    /// set is an iterator and `memtables` is the one caller that still needs
    /// it collected.
    pub(crate) fn memtables_each(
        &self,
    ) -> impl Iterator<Item = std::rc::Rc<std::cell::RefCell<crate::memtable::Memtable>>> {
        std::iter::once(self.memtable()).chain(self.frozen())
    }

    /// The directory the database lives in.
    pub fn dir(&self) -> &Path {
        &self.store.dir
    }

    /// The format major version the database was written in.
    pub fn format_version(&self) -> u64 {
        self.store.format_version
    }

    /// The log this store is appending to, if it has one.
    ///
    /// Not the same as the MANIFEST's `min_unflushed_log_num`, which can name
    /// a log that was never created — a database Pebble wrote with the WAL
    /// disabled has it at zero. The store writes to the newest log it
    /// replayed.
    #[doc(hidden)]
    pub fn current_log(&self) -> Option<u64> {
        self.store.log.borrow().as_ref().map(|l| l.log_num)
    }

    /// How many bytes of writes no table holds yet.
    ///
    /// What a caller watches to decide when to flush: §8 sizes a memtable at
    /// 4 MiB, to match Oxia.
    pub fn unflushed_bytes(&self) -> usize {
        self.memtables_each().map(|m| m.borrow().bytes()).sum()
    }

    /// The MANIFEST this store is appending to.
    #[doc(hidden)]
    pub fn manifest_filename(&self) -> String {
        self.store.manifest.borrow().filename.clone()
    }

    /// Every `marker.` file the directory holds, sorted, current and obsolete
    /// alike.
    #[doc(hidden)]
    pub fn markers(&self) -> Vec<String> {
        self.store.markers.borrow().clone()
    }

    /// Which iteration of `marker.manifest.*` names the current MANIFEST.
    pub(crate) fn manifest_marker_iter(&self) -> u64 {
        self.store.manifest.borrow().marker_iter
    }

    /// How many readers this store has open for each of its tables
    /// (`crate::obsolete`).
    pub(crate) fn readers(&self) -> &std::rc::Rc<crate::obsolete::Readers> {
        &self.store.readers
    }

    /// The output tables a compaction is in the middle of writing
    /// (`crate::obsolete`).
    pub(crate) fn pending(&self) -> &crate::obsolete::Pending {
        &self.store.pending
    }

    /// The versions a reader still holds after an edit replaced them
    /// (`crate::obsolete`).
    pub(crate) fn pinned(&self) -> &crate::obsolete::Pinned {
        &self.store.pinned
    }

    /// How this store opens its tables, for a scan or a snapshot to open
    /// theirs with as they reach them.
    pub(crate) fn tables(&self) -> std::rc::Rc<Tables<F>> {
        std::rc::Rc::clone(&self.store.tables)
    }

    /// Lets go of the table cache's reader for one of this store's tables.
    pub(crate) fn forget_reader(&self, file_num: u64) {
        self.store
            .caches
            .forget_table(self.store.store_id, file_num);
    }

    /// The tables the compaction underway is reading (§9).
    pub(crate) fn compacting(&self) -> &crate::change::Compacting {
        &self.store.compacting
    }

    /// Waits until no compaction is underway.
    ///
    /// For a caller waiting for the store's own merge task (§9) to drain
    /// what the picker has chosen: it parks while a merge runs and returns
    /// once none does, whether or not the picker has more, so the caller
    /// asks [`Db::pick`] again rather than polling it. Waiting for the
    /// picker to have work would not do, because once the last merge
    /// finishes there is none and may never be again.
    #[doc(hidden)]
    pub async fn compaction_settled(&self) {
        self.compacting().settled().await
    }

    /// Waits until there is a memtable worth writing out.
    ///
    /// What the store's flush task waits on ([`Db::spawn_background`]), and
    /// the half of its background work that takes the flush off the write
    /// that found the memtable full (§5). A write tells whoever is parked
    /// here an eighth of the budget before it fills; the task freezes that
    /// memtable and writes it out while the writes carry on into the fresh
    /// one, which is what the eighth is kept back for.
    ///
    /// Not part of the API: public only so that a test can stand in for the
    /// task, with the store's own turned off
    /// ([`crate::Options::background_tasks`]).
    ///
    /// Its own wait and not [`Db::compaction_wanted`]'s, because the two
    /// have to be able to run at once. A merge is the longest thing a store
    /// does and it holds no permit while it runs (§9), so a flush can go
    /// through the middle of one — but only if something is there to run it,
    /// and one task cannot be in two places. A caller that runs only the
    /// compaction task gets its flushes back inside its writes a memtable
    /// later.
    ///
    /// Level-triggered and asked again on every poll, as
    /// [`Db::compaction_wanted`] is, so a task that starts after the
    /// line was crossed is told at once. The two exceptions are both
    /// "later with no clock to say when" (§10): a change already underway
    /// may be the very flush this would run, so this waits for it and looks
    /// again rather than queueing to repeat it — Pebble's loop in
    /// `maybeInduceWriteStall`, which `Db::make_room` runs for the same
    /// reason; and a flush that **failed** is not wanted again until the
    /// store has taken another write, because until then it would write out
    /// the same memtable to the same disk and fail the same way.
    #[doc(hidden)]
    pub async fn flush_wanted(&self) {
        std::future::poll_fn(|cx| {
            if self.store.stopped.get() {
                return std::task::Poll::Ready(());
            }
            if self.changes().underway() {
                self.changes().park(cx);
                return std::task::Poll::Pending;
            }
            let held = self.unflushed_bytes() as u64;
            // A reader never flushes, however much its replay brought back.
            if self.store.read_only
                || self.store.flush_failed.get()
                || held == 0
                || held < self.flush_line()
            {
                self.store.flush_waiters.park(cx);
                return std::task::Poll::Pending;
            }
            std::task::Poll::Ready(())
        })
        .await
    }

    /// Tells the tasks waiting beside this store that nothing more is
    /// coming, so they can end and let go of it (§9).
    ///
    /// What closing and dropping the store start with: a parked task has to
    /// be woken by whatever it is parked on. From here both waits return at
    /// once, [`Db::running`] is false, and the last task to let go is what
    /// lets the store's state be freed.
    ///
    /// Nothing else changes, but for one thing: a store that has been
    /// stopped still reads, still writes, and still flushes inside its
    /// writes, but a flush no longer goes on to the merge nobody is waiting
    /// for, since closing waits for the flush in flight and should not wait
    /// for a merge it started.
    #[doc(hidden)]
    pub fn stop(&self) {
        self.store.stopped.set(true);
        self.store.flush_waiters.wake();
        self.store.compaction_waiters.wake();
    }

    /// Waits for the store's background work to stop, then releases the
    /// directory.
    ///
    /// The flush or merge in flight runs to its end first; nothing new is
    /// started. Once this returns, the directory can be opened again at once,
    /// which dropping the store promises only when nothing was in flight.
    ///
    /// It does not flush. With the WAL off ([`crate::Wal::Off`]),
    /// what the memtable holds is gone with the store unless
    /// [`Db::flush`] wrote it out first.
    pub async fn close(self) -> Result<()> {
        self.wind_down().await;
        Ok(())
    }

    /// Stops the background tasks and waits until every one has ended.
    pub(crate) async fn wind_down(&self) {
        self.stop();
        std::future::poll_fn(|cx| {
            if self.store.tasks.get() == 0 {
                return std::task::Poll::Ready(());
            }
            self.store.tasks_done.park(cx);
            std::task::Poll::Pending
        })
        .await
    }

    /// Starts the tasks that flush and compact in the background (§5, §9).
    ///
    /// Each holds a handle of its own on the store's state, and each counts
    /// itself in while it runs, so [`Db::close`] knows when they are gone.
    /// The merge task is left out for a store told to leave compaction to
    /// its caller: it would run exactly the merges that option is there to
    /// hold back.
    pub(crate) fn spawn_background<E: Env<Fs = F>>(&self, env: &E) {
        let flusher = self.task_handle();
        env.spawn(async move {
            while flusher.running() {
                flusher.flush_wanted().await;
                if !flusher.running() {
                    break;
                }
                let _working = Working::of(&flusher.store);
                if let Err(e) = flusher.flush().await {
                    // Nobody to hand it to. The store has taken note
                    // (`saw_flush_failure`) and asks again after the next
                    // write; a failure that lasts reaches a write at the
                    // budget, which flushes inline and gets it itself.
                    tracing::warn!(dir = %flusher.store.dir.display(), error = %e, "background flush failed");
                }
            }
        });
        if self.store.disable_automatic_compactions {
            return;
        }
        let merger = self.task_handle();
        env.spawn(async move {
            while merger.running() {
                merger.compaction_wanted().await;
                if !merger.running() {
                    break;
                }
                let _working = Working::of(&merger.store);
                if let Err(e) = merger.compact_once().await {
                    // Not asked for again until the version changes
                    // (`saw_compaction_failure`), so this is not a spin.
                    tracing::warn!(dir = %merger.store.dir.display(), error = %e, "background compaction failed");
                }
            }
        });
    }

    /// A handle for a background task: the same state, counted in
    /// [`Store::tasks`] until it is dropped, and with no say over the
    /// directory.
    fn task_handle(&self) -> Db<F> {
        self.store.tasks.set(self.store.tasks.get() + 1);
        Db {
            store: std::rc::Rc::clone(&self.store),
            owner: false,
        }
    }

    /// Whether the tasks beside this store should keep going.
    ///
    /// False once [`Db::stop`] has been called: the condition the store's
    /// own loops are written around ([`Db::spawn_background`]).
    #[doc(hidden)]
    pub fn running(&self) -> bool {
        !self.store.stopped.get()
    }

    /// Refuses a change to the directory if this store was opened read-only
    /// ([`open_read_only`]).
    ///
    /// Called first by everything public that writes, flushes, compacts,
    /// rotates, sweeps or destroys, before it has taken anything.
    pub(crate) fn writable(&self) -> Result<()> {
        if self.store.read_only {
            return Err(Error::ReadOnly);
        }
        Ok(())
    }

    /// Takes account of a flush that failed (§5).
    pub(crate) fn saw_flush_failure(&self) {
        self.store.flush_failed.set(true);
    }

    /// Waits until there is a compaction worth running.
    ///
    /// What the store's merge task waits on ([`Db::spawn_background`]). Not
    /// part of the API: public only so that a test can stand in for the
    /// task, with the store's own turned off
    /// ([`crate::Options::background_tasks`]).
    ///
    /// A flush that finds somebody waiting here leaves the merge to them and
    /// returns; a flush that finds nobody runs it where it stands (§9). So a
    /// store with a task like that one stops paying for merges inside its
    /// writes, and a store without one behaves exactly as it did.
    ///
    /// A task is not told while another compaction is underway, because the
    /// picker would name the tables that one has claimed and `compact_once`
    /// would stand down — and then this would say yes again at once, which
    /// is a spin and not a wait.
    ///
    /// Level-triggered, and asked again on every poll, so a task that starts
    /// after the work appeared is told at once rather than waiting for a
    /// flush that may not come. The one exception is a compaction that
    /// **failed**: that is not wanted again until the version changes,
    /// because the picker would give the same answer against the same
    /// version and asking for it again at once is a spin rather than a
    /// retry. The next flush is what makes it a retry.
    #[doc(hidden)]
    pub async fn compaction_wanted(&self) {
        std::future::poll_fn(|cx| {
            // Nothing more is coming, so the task around this is meant to
            // end rather than wait ([`Db::stop`]).
            if self.store.stopped.get() {
                return std::task::Poll::Ready(());
            }
            // Somebody is already on it. Waiting for them is the only answer
            // that does not spin: the picker would name the tables they have
            // claimed, `compact_once` would stand down, and this would say
            // yes again at once (§9, `crate::change::Compacting`).
            if self.store.compacting.underway() {
                self.store.compacting.park(cx);
                return std::task::Poll::Pending;
            }
            // A reader never compacts, whatever the picker would choose.
            if self.store.read_only || self.store.compaction_failed.get() || self.pick().is_none() {
                self.store.compaction_waiters.park(cx);
                return std::task::Poll::Pending;
            }
            std::task::Poll::Ready(())
        })
        .await
    }

    /// Takes account of the version having changed: a compaction that failed
    /// is worth trying again, and anybody waiting to be told there is work is
    /// woken.
    ///
    /// Says whether it woke anybody, which is how a flush knows whether the
    /// merge is somebody else's now (§9).
    pub(crate) fn version_changed(&self) -> bool {
        self.store.compaction_failed.set(false);
        self.store.compaction_waiters.wake()
    }

    /// Takes account of a compaction that failed.
    pub(crate) fn saw_compaction_failure(&self) {
        self.store.compaction_failed.set(true);
    }

    /// The permit every operation that changes what the database is made of
    /// takes first (§5, `crate::change`).
    pub(crate) fn changes(&self) -> &crate::change::Changes {
        &self.store.changes
    }

    /// Whether an append to the MANIFEST has failed, which is the end of
    /// this store's writing life (`crate::flush`).
    pub(crate) fn manifest_failed(&self) -> bool {
        self.store.manifest_failed.get()
    }

    /// Takes account of an append to the MANIFEST that failed.
    pub(crate) fn saw_manifest_failure(&self) {
        self.store.manifest_failed.set(true);
    }

    /// How many bytes a store holds before a write writes a memtable out
    /// itself (§4), as [`crate::Options`] was asked for.
    ///
    /// The backstop, not the line a flush is usually started at: that is
    /// [`Db::flush_line`], an eighth below this (§5).
    #[doc(hidden)]
    pub fn memtable_size(&self) -> u64 {
        self.store.memtable_size
    }

    /// How large the MANIFEST may grow before a flush replaces it
    /// (`crate::rotate`), as [`crate::Options`] was asked for.
    pub(crate) fn max_manifest_bytes(&self) -> u64 {
        self.store.max_manifest_bytes
    }

    /// How many tables L0 may hold before the picker wants it compacted.
    #[doc(hidden)]
    pub fn l0_compaction_threshold(&self) -> u64 {
        self.store.l0_compaction_threshold
    }

    /// How many tables L0 may hold before a write waits for it to drain
    /// (§9).
    #[doc(hidden)]
    pub fn l0_stop_writes_threshold(&self) -> u64 {
        self.store.l0_stop_writes_threshold
    }

    /// How many bytes L1 may hold, each level below it ten times more.
    pub(crate) fn lbase_max_bytes(&self) -> u64 {
        self.store.lbase_max_bytes
    }

    /// How many entries a compaction merges before it hands the thread back.
    pub(crate) fn compaction_unit(&self) -> u64 {
        self.store.compaction_unit
    }

    /// What a compaction's tables are compressed with.
    pub(crate) fn compression(&self) -> roble_format::block::Codec {
        self.store.compression
    }

    /// How large a table a compaction into `level` aims to write.
    ///
    /// Pebble's `TargetFileSizes`: L0's, then twice that for each level
    /// below. Deeper levels hold more and are rewritten less often, so
    /// larger files there cost fewer of them.
    #[doc(hidden)]
    pub fn target_file_size(&self, level: u32) -> u64 {
        self.store
            .target_file_size
            .saturating_mul(1u64.checked_shl(level).unwrap_or(u64::MAX))
    }

    /// Points the store at a MANIFEST the marker now names.
    ///
    /// Called only after the marker has moved, and deliberately infallible:
    /// past that point an append that still went to the old file would be an
    /// append a reopen never sees.
    pub(crate) fn install_manifest(&self, filename: String, marker_iter: u64, marker: String) {
        *self.store.manifest.borrow_mut() = Manifest {
            filename,
            marker_iter,
        };
        let mut markers = self.store.markers.borrow_mut();
        markers.push(marker);
        markers.sort();
    }

    /// Drops names the obsolete sweep removed from the marker list.
    pub(crate) fn forget_markers(&self, removed: &[String]) {
        self.store
            .markers
            .borrow_mut()
            .retain(|m| !removed.contains(m));
    }

    /// Takes note of how many entries a table holds.
    pub(crate) fn saw_table_entries(&self, file_num: u64, entries: Option<u64>) {
        if !self.store.metrics.recording() {
            return;
        }
        let Some(entries) = entries else {
            return;
        };
        if self
            .store
            .tables
            .entries
            .borrow_mut()
            .insert(file_num, entries)
            .is_none()
        {
            self.recount_records();
        }
    }

    /// Re-adds the tables' known counts, dropping those the version has let
    /// go, and restates the estimate.
    ///
    /// Only where the version changes or a table's count is first learned:
    /// the sum walks every table, and a write must not.
    pub(crate) fn recount_records(&self) {
        if !self.store.metrics.recording() {
            return;
        }
        let live = self.version_set().version.table_file_nums();
        let mut known = self.store.tables.entries.borrow_mut();
        known.retain(|file_num, _| live.contains(file_num));
        self.store
            .records_in_tables
            .set(known.values().copied().sum::<u64>());
        drop(known);
        self.saw_records();
    }

    /// Restates the estimate from what the tables and the memtable hold.
    pub(crate) fn saw_records(&self) {
        if !self.store.metrics.recording() {
            return;
        }
        let in_memory: usize = self.memtables().iter().map(|m| m.borrow().len()).sum();
        self.store
            .metrics
            .records
            .set((self.store.records_in_tables.get() + in_memory as u64) as i64);
    }

    /// Restates how much of the disk the directory takes.
    ///
    /// Called where files appear or disappear — a flush, a compaction — and
    /// only when something is recording, since it costs a walk of the
    /// directory. A failure is dropped rather than reported: a metric that
    /// could not be measured is not a reason to fail the work it describes.
    pub(crate) async fn measure_disk(&self) {
        if !self.store.metrics.recording() {
            return;
        }
        let _ = self.disk_usage().await;
    }

    /// Bytes of every file in this store's directory, which is Oxia's
    /// `DiskSpaceUsage`.
    ///
    /// Measured from the directory rather than summed from the version, so it
    /// takes in the MANIFEST, the logs, the markers and anything obsolete the
    /// sweep has not collected yet — the number `roble.disk.size` reports, and
    /// this is what refreshes it. Pebble answers the same question from the
    /// other side, adding up the sizes it tracks (`Metrics.DiskSpaceUsage`),
    /// which is a sum of what it believes rather than of what is there.
    ///
    /// It walks the directory, so it is a call to make when something asks
    /// and not one to make on a timer. Oxia memoizes Pebble's for five
    /// seconds for that reason ([METRICS.md](../METRICS.md)).
    pub async fn disk_usage(&self) -> Result<u64> {
        let usage = self.store.fs.disk_usage(&self.store.dir).await?;
        self.store.metrics.disk_size.set(usage.used_bytes as i64);
        Ok(usage.used_bytes)
    }

    /// The filesystem this store's files live in.
    pub(crate) fn fs(&self) -> &F {
        &self.store.fs
    }

    /// The version set, for a flush to add to.
    pub(crate) fn versions_mut(&self) -> std::cell::RefMut<'_, VersionSet> {
        self.store.versions.borrow_mut()
    }

    /// Applies an edit a flush or a compaction has made durable.
    ///
    /// The one place a version stops being current, and so the one place a
    /// reader can be left holding one the store no longer does: noting it
    /// here is what keeps the sweep off the tables that reader has still to
    /// open (`crate::obsolete::Pinned`).
    pub(crate) fn apply_edit(&self, edit: &VersionEdit) -> Result<()> {
        let mut versions = self.store.versions.borrow_mut();
        self.store.pinned.retiring(&versions.version);
        versions.apply(edit)
    }

    /// Freezes the memtable a flush is about to write out, puts a fresh one
    /// in front of it, and — with a WAL — points the store at the log
    /// `log_num` names, which must already exist.
    ///
    /// The memtable and the log move **together**, in one step with nothing
    /// awaited between them. A write that landed in between would be in the
    /// new memtable and the old log, or the old memtable and the new log,
    /// and either way the edit this flush is about to append would retire a
    /// log holding a record no table has.
    ///
    /// Any open scan keeps reading the memtable it started with: a flush
    /// replaces the handle and never the contents (§4).
    pub(crate) async fn freeze_memtable(&self, log_num: Option<u64>) -> Result<()> {
        // Opened before anything moves, because this is the only part that
        // can fail. A store left pointing at a log it could not open is a
        // store that cannot write, and this is a flush, not a write.
        let log = match log_num {
            Some(log_num) => Some(self.open_log(log_num).await?),
            None => None,
        };
        // The last await before the swap, and that is the whole point of
        // where it is: a write that started after this one returned would be
        // in flight on the memtable the next lines take away (§5).
        self.store.writers.settled().await;
        let fresh = std::rc::Rc::new(std::cell::RefCell::new(
            crate::memtable::Memtable::with_budget(
                self.last_seq_num().wrapping_add(1),
                self.store.memtable_size as usize,
            ),
        ));
        let old = std::mem::replace(&mut *self.store.memtable.borrow_mut(), fresh);
        *self.store.frozen.borrow_mut() = Some(old);
        if let Some(log) = log {
            // The log's number is spent here rather than taken, because from
            // here the store is writing to it and nothing can put that back.
            // `crate::rotate` draws the same line in the other place a file
            // number outlives the operation that asked for it.
            self.versions_mut().next_file_num = log.log_num + 1;
            *self.store.log.borrow_mut() = Some(log);
        }
        Ok(())
    }

    /// Forgets the frozen memtable, now that a table holds what it held.
    pub(crate) fn thaw(&self) {
        *self.store.frozen.borrow_mut() = None;
    }

    /// Whether this store was opened with the WAL off.
    #[doc(hidden)]
    pub fn wal_disabled(&self) -> bool {
        self.store.disable_wal
    }

    /// Whether this store leaves compaction to its caller (§9).
    pub(crate) fn automatic_compactions_disabled(&self) -> bool {
        self.store.disable_automatic_compactions
    }

    /// Opens an existing log for appending, without pointing the store at
    /// it: that is a step of its own, taken with the memtable.
    async fn open_log(&self, log_num: u64) -> Result<Log<F::Disk>> {
        let path = self
            .store
            .dir
            .join(roble_format::files::log_filename(log_num));
        let disk = self.store.fs.open(&path, Access::ReadWrite).await?;
        Ok(Log {
            disk: std::rc::Rc::new(disk),
            log_num,
            at: 0,
        })
    }

    /// What the MANIFEST says, as it stands: a copy, for roble's own tests
    /// and tools, which read counters and tables [`Db::levels`] does not show.
    ///
    /// Owned rather than a borrow of the store's own, so that a caller can
    /// hold it across an `.await` while a flush or a compaction applies an
    /// edit. Cheap: the version inside is shared, not copied.
    #[doc(hidden)]
    pub fn versions(&self) -> VersionSet {
        self.store.versions.borrow().clone()
    }

    /// The store's own version set, borrowed.
    ///
    /// Never held across an `.await`: an edit applied meanwhile would find
    /// it borrowed.
    pub(crate) fn version_set(&self) -> std::cell::Ref<'_, VersionSet> {
        self.store.versions.borrow()
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
        if let Some(held) = self.store.tables.cached(file_num) {
            return Ok(held);
        }
        let (table, learned) = self.store.tables.opened(file_num).await?;
        if learned {
            self.recount_records();
        }
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
    #[doc(hidden)]
    pub fn dump(&self) -> Result<String> {
        use std::fmt::Write as _;

        let mut out = String::new();
        for m in self.store.markers.borrow().iter() {
            let _ = writeln!(out, "marker {m}");
        }
        let _ = writeln!(out, "format-version {}", self.store.format_version);
        for (level, tables) in self.store.versions.borrow().version.levels() {
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

/// How a store opens its tables: through the table cache, and from its
/// directory when the cache does not hold one.
///
/// Apart from [`Db`] and shared by `Rc`, because a scan outlives the
/// call that made it and opens tables as it reaches them (`crate::scan`), so
/// it needs everything opening one takes without a borrow of the store. Each
/// field is a handle the store holds as well.
pub(crate) struct Tables<F: Fs> {
    fs: F,
    dir: PathBuf,
    caches: crate::cache::Caches<F>,
    store_id: u64,
    metrics: std::rc::Rc<crate::metrics::Metrics>,
    readers: std::rc::Rc<crate::obsolete::Readers>,
    /// How many entries each table is known to hold, learned when it is
    /// opened.
    ///
    /// The records a store holds is not a number any MANIFEST records: it is
    /// in each table's properties block, which is read when the table is
    /// opened. So rather than opening every table at once — which would make
    /// an open pay for a metric — a store learns a table's count the first
    /// time something has reason to read it, and the estimate climbs to the
    /// truth as the store is used. A flush and a compaction know their own
    /// outputs outright, so a store that writes is exact from the start.
    ///
    /// Here rather than on the store so that a scan's opening teaches it too.
    /// A count a scan learns joins the estimate at the store's next recount,
    /// since a scan cannot reach the version to make one.
    entries: std::cell::RefCell<std::collections::BTreeMap<u64, u64>>,
    /// Whether the store is a reader beside a writer, for which a table that
    /// has gone is stale rather than lost ([`retired`]).
    read_only: bool,
}

impl<F: Fs> Tables<F> {
    /// The table, if the cache holds it.
    fn cached(&self, file_num: u64) -> Option<std::rc::Rc<crate::table::OpenTable<F::Disk>>> {
        let held = self.caches.table(self.store_id, file_num)?;
        self.metrics.cache_lookups[crate::metrics::TABLE_HIT].add(1);
        Some(held)
    }

    /// Opens a table the cache does not hold and holds it there, saying
    /// whether its count of entries is one the store did not know.
    async fn opened(
        &self,
        file_num: u64,
    ) -> Result<(std::rc::Rc<crate::table::OpenTable<F::Disk>>, bool)> {
        self.metrics.cache_lookups[crate::metrics::TABLE_MISS].add(1);
        let path = self.dir.join(roble_format::files::table_filename(file_num));
        let disk = self
            .fs
            .open(&path, Access::ReadOnly)
            .await
            .map_err(retired(self.read_only, &path))?;
        let table = std::rc::Rc::new(
            crate::table::OpenTable::open_metered(
                file_num,
                disk,
                self.caches.blocks(self.store_id),
                Some(std::rc::Rc::clone(&self.metrics)),
                Some(std::rc::Rc::clone(&self.readers)),
            )
            .await?,
        );
        let learned = match table.entries {
            Some(entries) if self.metrics.recording() => self
                .entries
                .borrow_mut()
                .insert(file_num, entries)
                .is_none(),
            _ => false,
        };
        self.caches
            .hold_table(self.store_id, file_num, std::rc::Rc::clone(&table));
        Ok((table, learned))
    }
}

impl<F: Fs> crate::scan::Opener<F::Disk> for Tables<F> {
    fn held(&self, file_num: u64) -> Option<std::rc::Rc<crate::table::OpenTable<F::Disk>>> {
        self.cached(file_num)
    }

    fn open(&self, file_num: u64) -> crate::scan::Opening<'_, F::Disk> {
        Box::pin(async move { Ok(self.opened(file_num).await?.0) })
    }
}

/// Shows an encoded internal key as the Go harness spells it: the user key in
/// hex, because a bound is bytes and a dump is compared rather than read.
fn internal_key(encoded: &[u8]) -> Result<String> {
    let k = roble_format::InternalKey::decode(encoded)?;
    let hex: String = k.user_key.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("{hex} #{} {}", k.seq_num, k.kind.name()))
}

/// Which of `write_operations`' three counters each entry of a batch belongs
/// to: put, delete, or range delete.
///
/// Read off the representation rather than counted as the caller builds it,
/// because a batch reaches `write` from several places — an indexed batch, a
/// log replay, a caller's own — and only the bytes are common to all of them.
fn iter_kinds(repr: &[u8]) -> Result<Vec<usize>> {
    use roble_format::key::Kind;
    let reader = roble_format::batch::Batch::read(repr)?;
    let mut out = Vec::new();
    for entry in reader {
        out.push(match entry?.kind {
            Kind::RangeDelete => 2,
            Kind::Delete | Kind::SingleDelete | Kind::DeleteSized => 1,
            _ => 0,
        });
    }
    Ok(out)
}

impl<F: Fs> Drop for Db<F> {
    /// Stops the background tasks; and releases the directory at once if
    /// none of them is in the middle of a flush or a merge.
    ///
    /// A task that is only waiting does nothing more once it is woken: it
    /// sees the store stopped and ends, and what it lets go of is state no
    /// file is open for writing in. A task inside a flush or a merge keeps
    /// the lock until that work has finished and the task has ended, so
    /// nobody else opens the directory while files are still being written
    /// in it. [`Db::close`] is how a caller waits for that.
    fn drop(&mut self) {
        if !self.owner {
            let left = self.store.tasks.get() - 1;
            self.store.tasks.set(left);
            if left == 0 {
                self.store.tasks_done.wake();
            }
            return;
        }
        self.stop();
        if self.store.working.get() == 0 {
            drop(self.store._lock.borrow_mut().take());
        }
    }
}

/// Counts a background task in as working until it is dropped.
struct Working<'a>(&'a std::cell::Cell<usize>);

impl<'a> Working<'a> {
    fn of<F: Fs>(store: &'a Store<F>) -> Self {
        store.working.set(store.working.get() + 1);
        Working(&store.working)
    }
}

impl Drop for Working<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}
