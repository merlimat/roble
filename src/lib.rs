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

//! An embedded, ordered key-value store whose files are
//! [Pebble](https://github.com/cockroachdb/pebble)'s: a database roble writes
//! is one Go Pebble v2.1.7 opens, and the other way round.
//!
//! This crate is the engine. [`roble_format`] encodes and decodes the files,
//! with no I/O of its own.
//!
//! # Where to start
//!
//! - [`open_with`] opens a database directory, creating one if
//!   [`Options::create_if_missing`] says so, and returns a [`Db`].
//! - [`Db::put`], [`Db::delete`] and [`Db::delete_range`] write one change;
//!   [`Db::indexed_batch`] builds an [`IndexedBatch`] of several, readable
//!   through before it commits. Each write says its [`Durability`].
//! - [`Db::get`] finds a key, or the nearest one on either side of it
//!   ([`Comparison`]); [`Db::range`] makes a [`Scan`] that steps forwards or
//!   backwards; [`Db::snapshot`] holds one instant for reads that must agree.
//! - [`Db::flush`] writes the memtable out, which is what makes a write
//!   durable with the WAL off ([`Options::disable_wal`]);
//!   [`Db::checkpoint`] copies the database to a directory Go Pebble opens;
//!   [`Db::close`] stops the store and lets go of the directory.
//!
//! ```no_run
//! use roble::{Comparison, DefaultEnv, Durability, Options};
//!
//! # async fn example() -> roble::Result<()> {
//! let options = Options::new().create_if_missing(true);
//! let db = roble::open_with(&DefaultEnv, "data/shard-0", options).await?;
//!
//! db.put(b"/users/alice", b"1", Durability::Sync).await?;
//!
//! let mut batch = db.indexed_batch();
//! batch.put(b"/users/bob", b"2")?;
//! batch.delete(b"/users/carol")?;
//! batch.commit(Durability::NoSync).await?;
//!
//! // The greatest key at or below `/users/b`, which is alice.
//! let floor = db.get(b"/users/b", Comparison::Floor).await?;
//! assert_eq!(floor.map(|f| f.key), Some(b"/users/alice".to_vec()));
//!
//! let mut scan = db.range(&b"/users/"[..]..&b"/users0"[..])?;
//! scan.first().await?;
//! while scan.is_valid() {
//!     println!("{:?} = {:?}", scan.key(), scan.value());
//!     scan.next().await?;
//! }
//!
//! db.close().await
//! # }
//! ```
//!
//! # One thread per store
//!
//! A [`Db`] and everything it hands out — [`Scan`], [`Snapshot`],
//! [`IndexedBatch`], [`Caches`] — are `!Send` and `!Sync`: a store belongs to
//! the thread that opened it, as in a thread-per-core server, and needs no
//! locks for it. It flushes and compacts in two tasks it spawns on that
//! thread through [`Env::spawn`], which hand the thread back every so often
//! while they work. [`DefaultEnv`] runs on the current
//! [compio](https://docs.rs/compio) runtime and keeps databases in the file
//! system. Many stores can share one thread and, through [`Caches`], one
//! budget of open tables and cached blocks.
//!
//! # Testing against failures
//!
//! All I/O goes through [`Fs`] and [`Disk`]. Behind the `sim` feature,
//! `roble::sim` is an in-memory filesystem that can be made to fail or crash
//! at any single operation, for an application's own crash tests.
//!
//! # Design
//!
//! Section numbers (§3, §6, …) throughout refer to
//! [DESIGN.md](https://github.com/merlimat/roble/blob/main/DESIGN.md) in the
//! repository, which is not part of this crate.

#![cfg_attr(docsrs, feature(doc_cfg))]

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
mod levels;
mod memtable;
mod metrics;
mod obsolete;
mod open;
mod options;
mod pick;
mod read;
mod rotate;
mod scan;
#[cfg(feature = "sim")]
#[cfg_attr(docsrs, doc(cfg(feature = "sim")))]
pub mod sim;
mod snapshot;
mod table;
mod version;
mod work;

mod error;
mod file;

pub use batch::IndexedBatch;
pub use buf::{ReadBuf, WriteBuf};
pub use cache::{Caches, DEFAULT_BLOCK_BYTES, DEFAULT_OPEN_TABLES};
pub use env::{Access, DirEntry, Disk, DiskUsage, Env, Fs};
pub use error::{Error, FormatError, Result};
pub use file::{DefaultEnv, FileDisk, FileFs, FileLock};
pub use levels::{LevelInfo, TableInfo};
pub use open::{Db, open, open_read_only, open_with};
pub use options::{Options, Wal};
pub use pick::Pick;
pub use read::{Comparison, Found, GetBuf};
/// What [`Options::compression`] takes.
pub use roble_format::block::Codec;
pub use scan::Scan;
pub use snapshot::Snapshot;
pub use version::{Table, Version, VersionSet};

/// The format crate roble reads and writes Pebble's files with, re-exported
/// so that the `roble_format::Error` a [`FormatError`] hands out through
/// [`source`](std::error::Error::source) is always named at the version roble
/// was built with: a downcast to another version's type would find nothing.
pub use roble_format;

/// What roble's own tests and benchmark reach inside the engine.
///
/// Not part of the API: hidden from the documentation, and free to change in
/// any release. The same goes for the `#[doc(hidden)]` methods on [`Db`], for
/// [`Pick`], and for [`VersionSet`], [`Version`] and [`Table`]: the
/// maintenance and inspection calls the tests, the CLI, bench, compat and the
/// fuzz targets drive by hand, and what they read the store's state through.
#[doc(hidden)]
pub mod __private {
    pub use crate::cache::{BlockCache, Clock};
    pub use crate::compact::TARGET_FILE_SIZE;
    pub use crate::env::read_all;
    pub use crate::memtable::Memtable;
    pub use crate::pick::{L0_COMPACTION_THRESHOLD, L0_STOP_WRITES_THRESHOLD, LBASE_MAX_BYTES};
    pub use crate::table::OpenTable;

    /// One store's view of `caches`' block cache, for a test that opens an
    /// [`OpenTable`] on its own.
    pub fn block_cache<F: crate::Fs>(caches: &crate::Caches<F>, store: u64) -> BlockCache {
        caches.blocks(store)
    }
}
