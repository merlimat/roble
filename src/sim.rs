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
//! that counts, fails and forgets on command — this is it, as Pebble's
//! `vfs.NewCrashableMem` is for Go, and an application's own crash tests can
//! use it the same way.
//!
//! **Crashing is the interesting part.** [`SimFs::crash`] drops everything
//! that was written but not synced, and forgets directory entries that no
//! [`Fs::sync_dir`] made durable. A store that survives that has actually
//! earned the claim that reopening yields the state as of the last flush
//! (§4); one tested only against clean closes has not.
//!
//! It also keeps the trait honest: a second implementation is what turns the
//! trait from a description of `FileFs` into an interface.
//!
//! Behind the `sim` feature, off by default: an application that wants it
//! for its own crash tests enables it in its `[dev-dependencies]`.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::buf::{ReadBuf, WriteBuf};
use crate::env::{Access, DirEntry, Disk, DiskUsage, Env, Fs};
use crate::error::{Error, Result};

/// What a withheld buffer is filled with instead: not zero, so that an
/// engine reading it after the error gets something that cannot be mistaken
/// for data it wrote.
const POISON: u8 = 0xa5;

/// What a simulated operation is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
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
    /// Cutting or growing a file.
    SetLen,
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
    /// The operation number that should suspend once before it runs, if any.
    ///
    /// Nothing the simulated filesystem does blocks: every operation
    /// finishes in the call that starts it. That is what makes a replay a
    /// replay, and it is also why a test cannot otherwise catch two of a
    /// store's operations overlapping — polled once, a flush runs from its
    /// first byte to its last without ever handing control back, so the
    /// interleaving a real disk produces at every await never happens. This
    /// is one await, put where a test asks for it.
    pause_at: Option<u64>,
    /// The operation number that should withhold its buffer, if any.
    withhold_at: Option<u64>,
    /// The buffers withheld so far, kept so that the memory behind them is
    /// not reused while a test is still running — which is the whole point of
    /// withholding one.
    withheld: Vec<Vec<u8>>,
    /// Bytes handed back by reads, so a test can tell a store that fetches
    /// the blocks it needs from one that reads whole files.
    bytes_read: u64,
}

impl State {
    /// Counts an operation and says whether it should fail.
    /// Whether the operation just counted should keep its buffer.
    fn withholds(&self) -> bool {
        self.withhold_at == Some(self.ops)
    }

    /// Keeps `bytes` and hands back a buffer of the same length that is not
    /// it, poisoned so that reading it is visibly wrong rather than quietly
    /// stale.
    fn withhold(&mut self, bytes: Vec<u8>) -> Vec<u8> {
        let len = bytes.len();
        self.withheld.push(bytes);
        vec![POISON; len]
    }

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

    /// Makes the operation `n` from now suspend once before it runs.
    ///
    /// One yield, not a block: whatever else is ready runs, and then this
    /// operation carries on. It is the suspension a real disk would have
    /// given, at a place a test chose — which is how a test gets two of a
    /// store's own operations to overlap (§5, §12).
    pub fn pause_at(&self, n: u64) {
        let mut state = self.state.borrow_mut();
        state.pause_at = Some(state.ops + n);
    }

    /// Suspends once if the operation about to be counted is the one a test
    /// asked to pause at.
    ///
    /// Taken before the operation runs rather than after, so that it needs
    /// no borrow held across the await: the number it compares against is
    /// the one `step` is about to hand out.
    ///
    /// One yield and no more, without anything here having to remember: the
    /// poll that resumes comes back *inside* the yield rather than at the top
    /// of this, and the counter only goes up, so an operation that has run is
    /// an operation whose number can never come round again. Two tasks both
    /// reaching for the same number both yield once, which is more
    /// interleaving to choose from and not less.
    async fn pause_if_next(&self) {
        let due = {
            let state = self.state.borrow();
            state.pause_at == Some(state.ops + 1)
        };
        if due {
            crate::work::yield_now().await;
        }
    }

    /// Stops failing.
    pub fn stop_failing(&self) {
        self.state.borrow_mut().fail_at = None;
    }

    /// Makes the `n`th operation from now hand back a buffer that is not the
    /// one it was given, counting from one (DESIGN.md §12).
    ///
    /// A disk is allowed to do this: `Disk::read` and `Disk::write` take a
    /// buffer by value and return one, and nothing in the trait says it is
    /// the same buffer. A kernel that still owns the memory it was handed
    /// returns another, and an engine that assumed otherwise would be reading
    /// bytes it no longer owns.
    ///
    /// The buffer handed back is poisoned rather than zeroed, so that an
    /// engine which uses it after the error reads something visibly wrong
    /// instead of something plausible. The buffer taken is kept until the
    /// filesystem is dropped, which is what keeps its memory out of use.
    pub fn withhold_at(&self, n: u64) {
        let mut state = self.state.borrow_mut();
        state.withhold_at = Some(state.ops + n);
    }

    /// Stops withholding.
    pub fn stop_withholding(&self) {
        self.state.borrow_mut().withhold_at = None;
    }

    /// How many buffers the disk is still holding.
    pub fn withheld(&self) -> usize {
        self.state.borrow().withheld.len()
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
        self.pause_if_next().await;
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
        self.pause_if_next().await;
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
        self.pause_if_next().await;
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
        self.pause_if_next().await;
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
        self.pause_if_next().await;
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
        self.pause_if_next().await;
        let mut state = self.state.borrow_mut();
        state.step(Op::List)?;
        let mut out = Vec::new();
        for (path, file) in &state.files {
            if SimFs::parent_of(path) == dir {
                out.push(DirEntry::new(
                    path.file_name().unwrap_or_default(),
                    file.live.len() as u64,
                    false,
                ));
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn mkdir_all(&self, dir: &Path) -> Result<()> {
        self.pause_if_next().await;
        let mut state = self.state.borrow_mut();
        state.step(Op::Mkdir)?;
        state.dirs.insert(dir.to_path_buf(), true);
        Ok(())
    }

    async fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.pause_if_next().await;
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
        self.pause_if_next().await;
        let mut state = self.state.borrow_mut();
        state.step(Op::DiskUsage)?;
        let used_bytes = state
            .files
            .iter()
            .filter(|(p, _)| p.starts_with(dir))
            .map(|(_, f)| f.live.len() as u64)
            .sum();
        Ok(DiskUsage::new(used_bytes))
    }

    async fn lock(&self, path: &Path) -> Result<SimLock> {
        self.pause_if_next().await;
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
        self.fs.pause_if_next().await;
        let mut state = self.fs.state.borrow_mut();
        if let Err(e) = state.step(Op::Read) {
            return (Err(e), buf);
        }
        if state.withholds() {
            let kept = state.withhold(buf.into_vec());
            return (
                Err(Error::Io(io::Error::other(
                    "simulated disk kept the read's buffer",
                ))),
                ReadBuf::from_vec(kept),
            );
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
        self.fs.pause_if_next().await;
        let mut state = self.fs.state.borrow_mut();
        if let Err(e) = state.step(Op::Write) {
            return (Err(e), buf);
        }
        if state.withholds() {
            let kept = state.withhold(buf.into_vec());
            return (
                Err(Error::Io(io::Error::other(
                    "simulated disk kept the write's buffer",
                ))),
                WriteBuf::new(kept),
            );
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
        self.fs.pause_if_next().await;
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

    async fn set_len(&self, len: u64) -> Result<()> {
        self.fs.pause_if_next().await;
        let mut state = self.fs.state.borrow_mut();
        state.step(Op::SetLen)?;
        if !self.writable {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{} is open read-only", self.path.display()),
            )));
        }
        let path = self.path.clone();
        match state.files.get_mut(&path) {
            // Only what a reader sees: until a sync, a crash leaves the
            // durable bytes as they were, as it leaves an unsynced write.
            Some(file) => {
                file.live.resize(len as usize, 0);
                Ok(())
            }
            None => Err(Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{}", path.display()),
            ))),
        }
    }
}

/// An env over a [`SimFs`], which runs tasks on the current compio runtime.
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
}
