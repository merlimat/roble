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

//! An in-memory filesystem that can be made to fail, and to crash.
//!
//! §12 tests the failure paths by running a scenario once to count its
//! operations, then once per operation with an error injected there, once with
//! a crash, and once with buffers withheld. All of that needs a filesystem
//! that counts, fails and forgets on command — this is it. Oxia relies on the
//! same seam, passing Pebble a `vfs.NewCrashableMem` in its own tests.
//!
//! **Crashing is the interesting part.** [`SimFs::crash`] drops everything
//! that was written but not synced, and forgets directory entries that no
//! [`Fs::sync_dir`] made durable. A store that survives that has actually
//! earned the claim that reopening yields the state as of the last flush
//! (§4); one tested only against clean closes has not.
//!
//! It also keeps the trait honest: a second implementation is what turns the
//! trait from a description of `FileFs` into an interface.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::buf::{ReadBuf, WriteBuf};
use crate::env::{Access, DirEntry, Disk, DiskUsage, Env, Fs};
use crate::error::{Error, Result};

/// What a simulated operation is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// Creating a file.
    Create,
    /// Opening a file.
    Open,
    /// Removing a file.
    Remove,
    /// Renaming a file.
    Rename,
    /// Linking a file.
    Link,
    /// Listing a directory.
    List,
    /// Creating a directory.
    Mkdir,
    /// Syncing a directory.
    SyncDir,
    /// Measuring a directory.
    DiskUsage,
    /// Taking the directory lock.
    Lock,
    /// Reading from a file.
    Read,
    /// Writing to a file.
    Write,
    /// Syncing a file.
    Sync,
}

/// One file's bytes: what is durable, and what has only been written.
#[derive(Clone, Debug, Default)]
struct SimFile {
    /// Bytes a sync has made durable: what survives a crash.
    durable: Vec<u8>,
    /// Bytes written since, durable or not: what a reader sees.
    live: Vec<u8>,
    /// Whether the name itself has been made durable by a directory sync.
    name_durable: bool,
}

#[derive(Debug, Default)]
struct State {
    files: BTreeMap<PathBuf, SimFile>,
    dirs: BTreeMap<PathBuf, bool>,
    locked: BTreeMap<PathBuf, bool>,
    /// Operations counted so far, which a scenario uses to decide where to
    /// inject a failure next.
    //
    // `bytes_read` sits beside it below.
    ops: u64,
    /// The operation number that should fail, if any.
    fail_at: Option<u64>,
    /// Bytes handed back by reads, so a test can tell a store that fetches
    /// the blocks it needs from one that reads whole files.
    bytes_read: u64,
}

impl State {
    /// Counts an operation and says whether it should fail.
    fn step(&mut self, op: Op) -> Result<()> {
        self.ops += 1;
        if self.fail_at == Some(self.ops) {
            return Err(Error::Io(io::Error::other(format!(
                "simulated failure at operation {} ({op:?})",
                self.ops
            ))));
        }
        Ok(())
    }
}

/// An in-memory filesystem shared by the disks it opens.
#[derive(Clone, Debug, Default)]
pub struct SimFs {
    state: Rc<RefCell<State>>,
}

impl SimFs {
    /// An empty filesystem.
    pub fn new() -> Self {
        SimFs::default()
    }

    /// How many operations have been performed.
    pub fn bytes_read(&self) -> u64 {
        self.state.borrow().bytes_read
    }

    /// How many operations have run.
    pub fn ops(&self) -> u64 {
        self.state.borrow().ops
    }

    /// Makes the `n`th operation from now fail, counting from one.
    pub fn fail_at(&self, n: u64) {
        let mut state = self.state.borrow_mut();
        state.fail_at = Some(state.ops + n);
    }

    /// Stops failing.
    pub fn stop_failing(&self) {
        self.state.borrow_mut().fail_at = None;
    }

    /// Drops everything not made durable, as a machine crash would.
    ///
    /// Writes that no [`Disk::sync`] covered are lost, and names that no
    /// [`Fs::sync_dir`] covered disappear — including, deliberately, files
    /// whose contents were synced but whose name was not.
    pub fn crash(&self) {
        let mut state = self.state.borrow_mut();
        state.files.retain(|_, f| f.name_durable);
        for f in state.files.values_mut() {
            f.live = f.durable.clone();
        }
        state.locked.clear();
    }

    /// The durable bytes of a file, for a test to inspect.
    pub fn durable(&self, path: &Path) -> Option<Vec<u8>> {
        self.state
            .borrow()
            .files
            .get(path)
            .map(|f| f.durable.clone())
    }

    fn parent_of(path: &Path) -> PathBuf {
        path.parent().unwrap_or(Path::new("")).to_path_buf()
    }
}

impl Fs for SimFs {
    type Disk = SimDisk;
    type Lock = SimLock;

    async fn create(&self, path: &Path) -> Result<SimDisk> {
        let mut state = self.state.borrow_mut();
        state.step(Op::Create)?;
        state.files.insert(path.to_path_buf(), SimFile::default());
        drop(state);
        Ok(SimDisk {
            fs: self.clone(),
            path: path.to_path_buf(),
            writable: true,
        })
    }

    async fn open(&self, path: &Path, access: Access) -> Result<SimDisk> {
        let mut state = self.state.borrow_mut();
        state.step(Op::Open)?;
        if !state.files.contains_key(path) {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{}", path.display()),
            )));
        }
        drop(state);
        Ok(SimDisk {
            fs: self.clone(),
            path: path.to_path_buf(),
            writable: access == Access::ReadWrite,
        })
    }

    async fn remove(&self, path: &Path) -> Result<()> {
        let mut state = self.state.borrow_mut();
        state.step(Op::Remove)?;
        if state.files.remove(path).is_none() {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{}", path.display()),
            )));
        }
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let mut state = self.state.borrow_mut();
        state.step(Op::Rename)?;
        let Some(file) = state.files.remove(from) else {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{}", from.display()),
            )));
        };
        // A rename is atomic, and the new name is no more durable than any
        // other new name: a marker rotation still needs its directory synced.
        state.files.insert(
            to.to_path_buf(),
            SimFile {
                name_durable: false,
                ..file
            },
        );
        Ok(())
    }

    async fn link(&self, from: &Path, to: &Path) -> Result<()> {
        let mut state = self.state.borrow_mut();
        state.step(Op::Link)?;
        let Some(file) = state.files.get(from).cloned() else {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{}", from.display()),
            )));
        };
        state.files.insert(
            to.to_path_buf(),
            SimFile {
                name_durable: false,
                ..file
            },
        );
        Ok(())
    }

    async fn list(&self, dir: &Path) -> Result<Vec<DirEntry>> {
        let mut state = self.state.borrow_mut();
        state.step(Op::List)?;
        let mut out = Vec::new();
        for (path, file) in &state.files {
            if SimFs::parent_of(path) == dir {
                out.push(DirEntry {
                    name: PathBuf::from(path.file_name().unwrap_or_default()),
                    size: file.live.len() as u64,
                    is_dir: false,
                });
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn mkdir_all(&self, dir: &Path) -> Result<()> {
        let mut state = self.state.borrow_mut();
        state.step(Op::Mkdir)?;
        state.dirs.insert(dir.to_path_buf(), true);
        Ok(())
    }

    async fn sync_dir(&self, dir: &Path) -> Result<()> {
        let mut state = self.state.borrow_mut();
        state.step(Op::SyncDir)?;
        let dir = dir.to_path_buf();
        for (path, file) in state.files.iter_mut() {
            if SimFs::parent_of(path) == dir {
                file.name_durable = true;
            }
        }
        Ok(())
    }

    async fn disk_usage(&self, dir: &Path) -> Result<DiskUsage> {
        let mut state = self.state.borrow_mut();
        state.step(Op::DiskUsage)?;
        let used_bytes = state
            .files
            .iter()
            .filter(|(p, _)| p.starts_with(dir))
            .map(|(_, f)| f.live.len() as u64)
            .sum();
        Ok(DiskUsage { used_bytes })
    }

    async fn lock(&self, path: &Path) -> Result<SimLock> {
        let mut state = self.state.borrow_mut();
        state.step(Op::Lock)?;
        if state.locked.get(path).copied().unwrap_or(false) {
            return Err(Error::Locked);
        }
        state.locked.insert(path.to_path_buf(), true);
        state.files.entry(path.to_path_buf()).or_default();
        drop(state);
        Ok(SimLock {
            fs: self.clone(),
            path: path.to_path_buf(),
        })
    }
}

/// The simulated directory lock, released by dropping it.
#[derive(Debug)]
pub struct SimLock {
    fs: SimFs,
    path: PathBuf,
}

impl Drop for SimLock {
    fn drop(&mut self) {
        self.fs
            .state
            .borrow_mut()
            .locked
            .insert(self.path.clone(), false);
    }
}

/// One simulated file.
#[derive(Debug)]
pub struct SimDisk {
    fs: SimFs,
    path: PathBuf,
    writable: bool,
}

impl Disk for SimDisk {
    type Class = ();

    async fn len(&self) -> Result<u64> {
        let state = self.fs.state.borrow();
        match state.files.get(&self.path) {
            Some(f) => Ok(f.live.len() as u64),
            None => Err(Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{}", self.path.display()),
            ))),
        }
    }

    async fn read(&self, mut buf: ReadBuf, offset: u64, _class: ()) -> (Result<()>, ReadBuf) {
        let mut state = self.fs.state.borrow_mut();
        if let Err(e) = state.step(Op::Read) {
            return (Err(e), buf);
        }
        state.bytes_read += buf.len() as u64;
        let Some(file) = state.files.get(&self.path) else {
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{}", self.path.display()),
                ))),
                buf,
            );
        };
        let start = offset as usize;
        let end = start + buf.len();
        if end > file.live.len() {
            // Short of the whole range is an error, not a short read: a
            // partly filled block cannot be told from a corrupt one.
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "{}: {} bytes at {offset} reaches past the file's {} bytes",
                        self.path.display(),
                        buf.len(),
                        file.live.len()
                    ),
                ))),
                buf,
            );
        }
        buf.as_mut_slice().copy_from_slice(&file.live[start..end]);
        (Ok(()), buf)
    }

    async fn write(&self, buf: WriteBuf, offset: u64, _class: ()) -> (Result<()>, WriteBuf) {
        let mut state = self.fs.state.borrow_mut();
        if let Err(e) = state.step(Op::Write) {
            return (Err(e), buf);
        }
        if !self.writable {
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("{} is open read-only", self.path.display()),
                ))),
                buf,
            );
        }
        let path = self.path.clone();
        let Some(file) = state.files.get_mut(&path) else {
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{}", path.display()),
                ))),
                buf,
            );
        };
        let start = offset as usize;
        let end = start + buf.len();
        if file.live.len() < end {
            file.live.resize(end, 0);
        }
        file.live[start..end].copy_from_slice(buf.as_slice());
        (Ok(()), buf)
    }

    async fn sync(&self, _class: ()) -> Result<()> {
        let mut state = self.fs.state.borrow_mut();
        state.step(Op::Sync)?;
        let path = self.path.clone();
        match state.files.get_mut(&path) {
            Some(file) => {
                file.durable = file.live.clone();
                Ok(())
            }
            None => Err(Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{}", path.display()),
            ))),
        }
    }
}

/// An env over a [`SimFs`], which runs tasks on the current compio runtime and
/// never reads the clock.
#[derive(Clone, Debug, Default)]
pub struct SimEnv {
    fs: SimFs,
}

impl SimEnv {
    /// An env over a fresh filesystem.
    pub fn new() -> Self {
        SimEnv::default()
    }

    /// The filesystem, for a test to inspect or to make fail.
    pub fn sim(&self) -> &SimFs {
        &self.fs
    }
}

impl Env for SimEnv {
    type Fs = SimFs;

    fn fs(&self) -> &SimFs {
        &self.fs
    }

    fn spawn(&self, task: impl Future<Output = ()> + 'static) {
        compio::runtime::spawn(task).detach();
    }

    // Deliberately not overridden: a simulated run must not read the clock, so
    // that the same seed replays the same way (§9).
}
