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

pub mod batch;
pub mod buf;
pub mod cache;
pub mod compact;
pub mod env;
pub mod flush;
pub mod memtable;
mod metrics;
pub mod obsolete;
pub mod open;
pub mod options;
pub mod pick;
pub mod read;
pub mod rotate;
pub mod scan;
pub mod sim;
pub mod table;
pub mod version;
pub mod work;

mod error;
mod file;

pub use batch::IndexedBatch;
pub use buf::{ReadBuf, WriteBuf};
pub use cache::{BlockCache, Caches};
pub use env::{Access, DirEntry, Disk, DiskUsage, Env, Fs};
pub use error::{Error, Result};
pub use file::{DefaultEnv, FileDisk, FileFs, FileLock};
pub use open::{Opened, open, open_with};
pub use options::Options;
pub use pick::Pick;
pub use read::{Comparison, Found};
pub use scan::Scan;
pub use version::{Table, Version, VersionSet};
