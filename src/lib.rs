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

//! roble: an embedded, ordered key-value store whose files are Pebble's.
//!
//! See `DESIGN.md`. This crate is the engine; [`roble_format`] encodes and
//! decodes the files, with no I/O of its own.
//!
//! So far it holds the seam the engine sits on — the [`Env`] a store runs in,
//! the [`Fs`] it opens files in, and the [`Disk`] that holds one of them
//! (§10) — and, on top of it, [`open`], which reads a database directory:
//! its markers, and its MANIFEST replayed into a [`Version`] (§6).
//!
//! [`DefaultEnv`] is the file system on compio; [`sim`] is an in-memory
//! filesystem that can be made to fail or crash at any single operation, which
//! is how §12 tests the failure paths.

mod batch;
mod buf;
mod cache;
mod change;
mod checkpoint;
mod compact;
mod create;
mod destroy;
mod env;
mod flush;
mod memtable;
mod metrics;
mod obsolete;
mod open;
mod options;
mod pick;
mod read;
mod rotate;
mod scan;
pub mod sim;
mod snapshot;
mod table;
mod version;
mod work;

mod error;
mod file;

pub use batch::IndexedBatch;
pub use buf::{ReadBuf, WriteBuf};
pub use cache::{BlockCache, Caches, DEFAULT_BLOCK_BYTES, DEFAULT_OPEN_TABLES};
pub use env::{Access, DirEntry, Disk, DiskUsage, Env, Fs};
pub use error::{Error, FormatError, Result};
pub use file::{DefaultEnv, FileDisk, FileFs, FileLock};
pub use open::{Opened, open, open_with};
pub use options::Options;
pub use pick::Pick;
pub use read::{Comparison, Found, Lookup};
pub use scan::Scan;
pub use snapshot::Snapshot;
pub use version::{Table, Version, VersionSet};

/// What roble's own tests and benchmark reach inside the engine.
///
/// Not part of the API: hidden from the documentation, and free to change in
/// any release.
#[doc(hidden)]
pub mod __private {
    pub use crate::cache::Clock;
    pub use crate::compact::TARGET_FILE_SIZE;
    pub use crate::env::read_all;
    pub use crate::memtable::Memtable;
    pub use crate::pick::{L0_COMPACTION_THRESHOLD, L0_STOP_WRITES_THRESHOLD, LBASE_MAX_BYTES};
    pub use crate::table::OpenTable;
}
