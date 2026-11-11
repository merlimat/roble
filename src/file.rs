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

    fn real_time(&self) -> bool {
        true
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
            // Across file systems, and on those without links, a copy is the
            // same thing more slowly: the point of the link is that a
            // checkpoint costs no bytes, not that it is a link.
            Err(e) if matches!(e.kind(), io::ErrorKind::Unsupported) => {
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
            let meta = entry.metadata()?;
            out.push(DirEntry {
                name: PathBuf::from(entry.file_name()),
                size: if meta.is_dir() { 0 } else { meta.len() },
                is_dir: meta.is_dir(),
            });
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
                let meta = entry.metadata()?;
                if meta.is_dir() {
                    stack.push(entry.path());
                } else {
                    used_bytes += meta.len();
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

/// One file of a database, read and written without the OS page cache.
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
