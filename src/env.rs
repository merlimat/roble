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

//! Where a store runs: the env that runs its tasks, the filesystem it opens its
//! files in, and the disk that holds one of them (DESIGN.md §10).
//!
//! This is encina's shape with two changes, both of which the format forces.
//!
//! **A filesystem, not one file.** encina has a single file, created once. An
//! LSM creates, renames, links and deletes files constantly, and its
//! correctness depends on directory syncs — so the env grows an [`Fs`], which
//! is Pebble's `vfs.FS` in Rust terms. It is a trait of its own rather than
//! methods on [`Env`] because that is the seam Oxia already injects: it passes
//! a `vfs.FS` today, including a crashable in-memory one in its tests, and
//! keeps Pebble's scheduling and clock.
//!
//! **A read is a byte range.** encina reads whole pages, because its file is
//! pages. A table has no page size: a block is wherever the index says, at
//! whatever length. Alignment for direct I/O is therefore the caller's
//! business — the block cache reads the enclosing aligned span and keeps the
//! block, not the span (§8) — and not something [`Disk`] can do on its own.
//!
//! What carries over is the part that makes a run reproducible: buffers are
//! owned and handed back, so a disk that keeps one cannot make roble touch
//! memory it still lends; and nothing reads the clock unless
//! [`Env::real_time`] says it may.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};

use crate::buf::{ReadBuf, WriteBuf};
use crate::error::Result;

/// What a file is opened for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// To read and write it.
    ReadWrite,
    /// To read it only, as a snapshot reader or a follower seeded from a
    /// checkpoint does.
    ReadOnly,
}

/// Where a store runs: what opens its files, runs its tasks, and lets it read
/// the clock, or not.
///
/// [`DefaultEnv`](crate::DefaultEnv) keeps databases in the file system, on the
/// current compio runtime. A caller that runs its own I/O — a scheduler
/// ordering the requests of several shards, or a simulation replaying a run
/// from its seed — implements this trait and [`Fs`] instead.
pub trait Env {
    /// The filesystem this env opens files in.
    type Fs: Fs;

    /// The filesystem.
    fn fs(&self) -> &Self::Fs;

    /// Runs `task` beside the store until it ends.
    fn spawn(&self, task: impl Future<Output = ()> + 'static);

    /// Whether the store may read the real clock: false by default.
    ///
    /// Compaction is paced in units of work rather than by the clock so that a
    /// seeded run replays (§9). An env that returns true lets the store pace
    /// by time instead, which is faster and not reproducible.
    fn real_time(&self) -> bool {
        false
    }
}

/// What a directory listing says about one entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    /// The entry's name within the directory it was listed from.
    pub name: PathBuf,
    /// Its size in bytes, or zero for a directory.
    pub size: u64,
    /// Whether it is a directory.
    pub is_dir: bool,
}

/// How much space a directory's files take.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskUsage {
    /// Bytes used by the files in the directory, recursively.
    pub used_bytes: u64,
}

/// The filesystem a database lives in: Pebble's `vfs.FS` in Rust terms.
///
/// Every path a store touches goes through here, which is what lets Oxia
/// inject its own — and what lets roble's tests fail or crash any single
/// operation (§12).
pub trait Fs {
    /// The disk that [`create`](Self::create) and [`open`](Self::open) return.
    type Disk: Disk;

    /// Held for as long as this process owns the database directory.
    ///
    /// Dropping it releases the lock. Pebble's `LOCK` file is this.
    type Lock;

    /// Creates `path`, truncating it if it exists, and opens it for writing.
    fn create(&self, path: &Path) -> impl Future<Output = Result<Self::Disk>>;

    /// Opens `path` as it is.
    fn open(&self, path: &Path, access: Access) -> impl Future<Output = Result<Self::Disk>>;

    /// Removes `path`.
    fn remove(&self, path: &Path) -> impl Future<Output = Result<()>>;

    /// Renames `from` to `to`, replacing `to` if it exists.
    ///
    /// Marker rotation depends on this being atomic
    /// (`vfs/atomicfs`): a reader must see either name, never neither.
    fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = Result<()>>;

    /// Hard-links `from` to `to`, which `checkpoint` wants so that a snapshot
    /// costs no copy; falls back to a copy where links are unavailable.
    fn link(&self, from: &Path, to: &Path) -> impl Future<Output = Result<()>>;

    /// Lists `dir`.
    fn list(&self, dir: &Path) -> impl Future<Output = Result<Vec<DirEntry>>>;

    /// Creates `dir` and its parents.
    fn mkdir_all(&self, dir: &Path) -> impl Future<Output = Result<()>>;

    /// Makes the directory's entries durable, which marker rotation needs.
    ///
    /// Without this a crash can leave a file whose name never reached the
    /// disk, which is how an LSM loses a table it believes it wrote.
    fn sync_dir(&self, dir: &Path) -> impl Future<Output = Result<()>>;

    /// How much space the directory's files take, for Oxia's `DiskSpaceUsage`.
    fn disk_usage(&self, dir: &Path) -> impl Future<Output = Result<DiskUsage>>;

    /// Takes the database directory's lock, keeping out a second owner.
    ///
    /// Two owners of one database corrupt it, so this is not advisory.
    fn lock(&self, path: &Path) -> impl Future<Output = Result<Self::Lock>>;
}

/// One file of a database: a table, a log, a MANIFEST, a marker.
///
/// Requests move owned buffers, handed back with the outcome, so that a disk
/// which keeps a buffer cannot make roble touch memory it still lends.
///
/// Unlike encina's, a read here is a **byte range**: a table has no page size.
/// With direct I/O the caller must pass an aligned offset and length — the
/// block cache reads the enclosing aligned span and keeps the block (§8) —
/// and a disk is free to reject anything else.
// `len` here is a file's size, not a collection's count, so the companion
// `is_empty` clippy asks for would be noise. encina's Disk allows the same.
#[allow(clippy::len_without_is_empty)]
pub trait Disk: Sized + 'static {
    /// What a request is for, so that a disk which schedules can order them.
    /// Of two classes the greater is the more urgent. A disk that does not
    /// schedule takes `()`.
    ///
    /// An LSM has more use for this than encina does: a compaction's reads
    /// should not delay a lookup's.
    type Class: Copy + Ord + Default + 'static;

    /// The file's length in bytes.
    fn len(&self) -> impl Future<Output = Result<u64>>;

    /// Fills `buf` from `offset`.
    ///
    /// A read past the end of the file fails with
    /// [`io::ErrorKind::UnexpectedEof`] rather than returning a short buffer:
    /// a partly filled block is indistinguishable from a corrupt one, and the
    /// difference matters when deciding whether to refuse a database.
    fn read(
        &self,
        buf: ReadBuf,
        offset: u64,
        class: Self::Class,
    ) -> impl Future<Output = (Result<()>, ReadBuf)>;

    /// Writes all of `buf` at `offset`, growing the file if needed.
    fn write(
        &self,
        buf: WriteBuf,
        offset: u64,
        class: Self::Class,
    ) -> impl Future<Output = (Result<()>, WriteBuf)>;

    /// Makes every finished write durable, and the file's length.
    fn sync(&self, class: Self::Class) -> impl Future<Output = Result<()>>;

    /// Closes the file.
    fn close(self) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }
}

/// Reads exactly `len` bytes at `offset` into a fresh buffer.
///
/// A convenience for the places that read something small and whole — a
/// footer, a marker, a metaindex — where the caller has no frame to lend and
/// alignment is handled by reading the enclosing span.
pub async fn read_exact<D: Disk>(
    disk: &D,
    offset: u64,
    len: usize,
    class: D::Class,
) -> Result<Vec<u8>> {
    let (result, buf) = disk.read(ReadBuf::with_len(len), offset, class).await;
    result?;
    Ok(buf.into_vec())
}

/// Reads the last `len` bytes of the file, which is how a footer is found.
pub async fn read_tail<D: Disk>(disk: &D, len: usize, class: D::Class) -> Result<Vec<u8>> {
    let size = disk.len().await?;
    let len64 = len as u64;
    if size < len64 {
        return Err(crate::Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("file is {size} bytes, less than the {len} wanted from its end"),
        )));
    }
    read_exact(disk, size - len64, len, class).await
}
