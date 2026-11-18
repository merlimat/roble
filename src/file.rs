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

//! [`DefaultEnv`]: databases in the file system, on compio.
//!
//! The `flock` lock and the directory sync are encina's, adapted: a lock that
//! belongs to the open rather than the process, so that a second thread of this
//! process is kept out too (DESIGN.md §10).
//!
//! **No direct I/O here, deliberately.** encina opens with `O_DIRECT` because
//! its file is pages: every request it makes is already aligned in offset,
//! length and buffer address. A Pebble file is not pages. Its footer is 53
//! bytes at the end, its metaindex a few dozen, its MANIFEST a chain of partial
//! chunks — and `O_DIRECT` answers such a request with `EINVAL` rather than
//! serving it. Direct I/O therefore belongs under the block cache, which
//! already reads the enclosing aligned span and keeps the block (§8), as a
//! [`Disk`] that aligns what it is handed. Until that exists these files go
//! through the page cache, which is also what Pebble does.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};

use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};

use crate::buf::{ReadBuf, WriteBuf};
use crate::env::{Access, DirEntry, Disk, DiskUsage, Env, Fs};
use crate::error::{Error, Result};

/// Whether a hard link that failed this way should be retried as a copy.
///
/// Across file systems, and on those without links, a copy is the same thing
/// more slowly: the point of the link is that a checkpoint costs no bytes, not
/// that it is a link (§4).
///
/// Both kinds have to be named. `Unsupported` is a file system with no links
/// at all — `ENOSYS` or `EOPNOTSUPP` — and `CrossesDevices` is `EXDEV`, the two
/// paths being on different file systems, which is what a checkpoint written
/// to another volume hits. Only the first was matched here until the coverage
/// report asked why neither arm ever ran: `EXDEV` has had an `io::ErrorKind` of
/// its own since Rust 1.85 rather than falling into `Unsupported`, so the
/// cross-file-system case this was written for was the one it did not catch.
///
/// Anything else is a real failure — a missing source, a refusal, a name
/// already taken — and is returned rather than papered over with a copy.
fn copy_instead(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::Unsupported | io::ErrorKind::CrossesDevices
    )
}

/// What a directory entry's metadata came back as, or `None` if the entry
/// was deleted between reading the directory and asking.
///
/// A reader beside a writer meets that whenever the writer retires a file
/// while the reader lists the directory (DESIGN.md §6, "Reading beside a
/// writer"). The file is not in the directory any more, so the answer is to
/// leave it out of the listing, not to fail it. Any other error is one.
fn unless_gone<T>(meta: io::Result<T>) -> io::Result<Option<T>> {
    match meta {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        meta => meta.map(Some),
    }
}

/// Databases in the file system, on the current compio runtime.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultEnv;

impl Env for DefaultEnv {
    type Fs = FileFs;

    fn fs(&self) -> &FileFs {
        &FileFs
    }

    fn spawn(&self, task: impl Future<Output = ()> + 'static) {
        compio::runtime::spawn(task).detach();
    }
}

/// The file system, as [`DefaultEnv`] sees it.
#[derive(Clone, Copy, Debug, Default)]
pub struct FileFs;

impl Fs for FileFs {
    type Disk = FileDisk;
    type Lock = FileLock;

    async fn create(&self, path: &Path) -> Result<FileDisk> {
        let file = open_file(path, true, true).await?;
        Ok(FileDisk { file })
    }

    async fn open(&self, path: &Path, access: Access) -> Result<FileDisk> {
        let file = open_file(path, access == Access::ReadWrite, false).await?;
        Ok(FileDisk { file })
    }

    async fn remove(&self, path: &Path) -> Result<()> {
        Ok(compio::fs::remove_file(path).await?)
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        Ok(compio::fs::rename(from, to).await?)
    }

    async fn link(&self, from: &Path, to: &Path) -> Result<()> {
        match compio::fs::hard_link(from, to).await {
            Ok(()) => Ok(()),
            // The copy itself is untested, with a reason rather than a test
            // (§12). Reaching it needs a second file system — one mounted
            // elsewhere, or one without links — which a test cannot conjure on
            // the machine it happens to run on. What *was* wrong here is which
            // errors arrive at this arm, and that is decided by `copy_instead`
            // and tested there, against the errnos themselves.
            Err(e) if copy_instead(e.kind()) => {
                let bytes = std::fs::read(from)?;
                std::fs::write(to, bytes)?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, dir: &Path) -> Result<Vec<DirEntry>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            out.extend(unless_gone(entry.metadata())?.map(|meta| DirEntry {
                name: PathBuf::from(entry.file_name()),
                size: if meta.is_dir() { 0 } else { meta.len() },
                is_dir: meta.is_dir(),
            }));
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn mkdir_all(&self, dir: &Path) -> Result<()> {
        Ok(std::fs::create_dir_all(dir)?)
    }

    async fn sync_dir(&self, dir: &Path) -> Result<()> {
        sync_dir(dir).await
    }

    async fn disk_usage(&self, dir: &Path) -> Result<DiskUsage> {
        let mut used_bytes = 0u64;
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for entry in std::fs::read_dir(&d)? {
                let entry = entry?;
                // One deleted since the listing takes no space any more.
                let meta = unless_gone(entry.metadata())?;
                if meta.as_ref().is_some_and(|m| m.is_dir()) {
                    stack.push(entry.path());
                } else {
                    used_bytes += meta.map_or(0, |m| m.len());
                }
            }
        }
        Ok(DiskUsage { used_bytes })
    }

    async fn lock(&self, path: &Path) -> Result<FileLock> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;
        take_lock(&file)?;
        Ok(FileLock { _file: file })
    }
}

/// The database directory's lock, released by dropping it.
#[derive(Debug)]
pub struct FileLock {
    // Closing the file releases the `flock`, so holding it is the lock.
    _file: File,
}

/// One file of a database, read and written through the OS page cache.
///
/// Not direct I/O: the note at the top of this module says why a Pebble file
/// cannot be read that way without an aligning layer under it.
#[derive(Debug)]
pub struct FileDisk {
    file: File,
}

impl Disk for FileDisk {
    // Request classes are in the trait because an LSM needs them (§10), but
    // this disk does not schedule: it issues each request as it arrives.
    type Class = ();

    async fn len(&self) -> Result<u64> {
        Ok(self.file.metadata().await?.len())
    }

    async fn read(&self, buf: ReadBuf, offset: u64, _class: ()) -> (Result<()>, ReadBuf) {
        let want = buf.len();
        let result = self.file.read_exact_at(buf.into_vec(), offset).await;
        match result {
            compio::BufResult(Ok(()), bytes) => {
                debug_assert_eq!(bytes.len(), want);
                (Ok(()), ReadBuf::from_vec(bytes))
            }
            compio::BufResult(Err(e), bytes) => (Err(e.into()), ReadBuf::from_vec(bytes)),
        }
    }

    async fn write(&self, buf: WriteBuf, offset: u64, _class: ()) -> (Result<()>, WriteBuf) {
        // compio implements AsyncWriteAt for &File, so a &mut &File satisfies
        // the &mut self the extension trait wants without the disk needing
        // interior mutability.
        let mut file = &self.file;
        match file.write_all_at(buf.into_vec(), offset).await {
            compio::BufResult(Ok(()), bytes) => (Ok(()), WriteBuf::new(bytes)),
            compio::BufResult(Err(e), bytes) => (Err(e.into()), WriteBuf::new(bytes)),
        }
    }

    async fn sync(&self, _class: ()) -> Result<()> {
        Ok(self.file.sync_all().await?)
    }

    async fn close(self) -> Result<()> {
        Ok(self.file.close().await?)
    }
}

/// Opens a file, creating it when the open is for writing.
///
/// Through the page cache: the note at the top of this module says why an
/// uncached open cannot serve a 53-byte footer.
async fn open_file(path: &Path, writable: bool, truncate: bool) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(writable)
        .create(writable)
        .truncate(truncate)
        .open(path)
        .await?;
    Ok(file)
}

/// Takes an exclusive lock on `file`, which closing it releases.
///
/// A `flock` lock belongs to this open of the file rather than to the process,
/// so it also keeps out a second open by another thread of this process —
/// which matters because a node runs hundreds of shards on a handful of
/// threads (§1).
#[cfg(unix)]
fn take_lock(file: &File) -> Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: the descriptor is open for the duration of the call, and flock
    // takes an int.
    #[allow(unsafe_code)]
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == -1 {
        let e = io::Error::last_os_error();
        return Err(match e.kind() {
            io::ErrorKind::WouldBlock => Error::Locked,
            // Untested, with a reason rather than a test (§12). The arm above
            // is `EWOULDBLOCK`, another owner holding the lock, which a test
            // takes by opening the database twice. What is left is
            // `EBADF`, `EINVAL` and `EINTR`: the first two cannot happen on a
            // descriptor this function was just handed and a constant flag,
            // and reaching the third would mean delivering a signal mid-call
            // from inside the test. Covering it would need a seam through
            // `flock` itself, and a seam exists to be used by the code it
            // guards — this one would exist only to be exercised.
            _ => e.into(),
        });
    }
    Ok(())
}

/// Elsewhere roble does not lock, as encina does not.
#[cfg(not(unix))]
fn take_lock(_file: &File) -> Result<()> {
    Ok(())
}

/// Syncs a directory, which makes the names in it durable.
#[cfg(unix)]
async fn sync_dir(dir: &Path) -> Result<()> {
    Ok(File::open(dir).await?.sync_all().await?)
}

/// Elsewhere roble does not sync directories, as encina does not.
#[cfg(not(unix))]
async fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The branch a listing takes when a writer deletes a file between the
    /// directory read and the stat, which no test can time on a real file
    /// system, so it is checked against the error kinds themselves.
    #[test]
    fn an_entry_deleted_mid_listing_is_left_out_and_nothing_else_is() {
        let gone = io::Error::from(io::ErrorKind::NotFound);
        assert!(matches!(unless_gone::<u64>(Err(gone)), Ok(None)));
        let refused = io::Error::from(io::ErrorKind::PermissionDenied);
        assert!(unless_gone::<u64>(Err(refused)).is_err());
        assert!(matches!(unless_gone(Ok(7u64)), Ok(Some(7))));
    }

    #[test]
    fn a_failed_link_copies_only_where_no_link_was_possible() {
        assert!(copy_instead(io::ErrorKind::Unsupported));
        assert!(copy_instead(io::ErrorKind::CrossesDevices));
        // A copy would paper over each of these, and each of them is a real
        // failure a checkpoint should stop on.
        for kind in [
            io::ErrorKind::NotFound,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AlreadyExists,
            io::ErrorKind::StorageFull,
        ] {
            assert!(!copy_instead(kind), "{kind:?} should not become a copy");
        }
    }

    /// The premise the fallback rests on, pinned rather than assumed.
    ///
    /// This is the mapping that was wrong: a std that moved `EXDEV` back into
    /// `Unsupported`, or anywhere else, would put the cross-file-system case
    /// into the arm that returns an error, and nothing else here would notice
    /// — a checkpoint onto another volume only fails on a machine that has
    /// one.
    #[cfg(unix)]
    #[test]
    fn the_errnos_a_checkpoint_meets_map_to_the_kinds_matched() {
        let exdev = io::Error::from_raw_os_error(libc::EXDEV);
        assert_eq!(exdev.kind(), io::ErrorKind::CrossesDevices);
        assert!(copy_instead(exdev.kind()), "a checkpoint across volumes");

        let unsupported = io::Error::from_raw_os_error(libc::EOPNOTSUPP);
        assert_eq!(unsupported.kind(), io::ErrorKind::Unsupported);
        assert!(
            copy_instead(unsupported.kind()),
            "a file system with no links"
        );
    }
}
