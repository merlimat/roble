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

//! Encodes and decodes [Pebble](https://github.com/cockroachdb/pebble)'s
//! on-disk formats: no I/O, no async, no runtime, no engine.
//!
//! Pebble's Go source at v2.1.7 is the specification (DESIGN.md §3). Each
//! module names the Go file and symbol it implements, so that a reader can
//! check it and a Pebble upgrade has a list of things to re-read.
//!
//! # Scope
//!
//! The files of a Pebble database directory, as bytes in and bytes out:
//!
//! - sstables ([`sst`]), read at every table format from RocksDBv2 to
//!   Pebblev4 and written at Pebblev4, with their blocks ([`block`]), bloom
//!   filters ([`filter`]) and range deletions ([`rangedel`]);
//! - the record framing of the write-ahead log and the MANIFEST
//!   ([`record`]), the batches a log holds ([`batch`]) and the version edits
//!   a MANIFEST holds ([`manifest`]);
//! - internal keys ([`key`]), and the names and markers a directory holds
//!   ([`files`]).
//!
//! Anything Pebble writes above that pin, or for a feature roble does not
//! read — range keys, blob files, MinLZ, a table format past Pebblev4 — is
//! refused with [`Error::Unsupported`] rather than half-read. Reading files,
//! caching blocks and merging them is the engine's: this crate was written
//! for [roble](https://github.com/merlimat/roble), and has no I/O of its own.
//!
//! # Stability
//!
//! Semantic versioning, from 0.1. Items marked `#[doc(hidden)]` are not part
//! of the API: they are the seams roble's engine reads tables through, and
//! may change in any release. The enums and structs a newer Pebble format
//! could extend are `#[non_exhaustive]`; those that mirror a format at the
//! pin, such as [`manifest::VersionEdit`], are not, since extending them
//! means reading a new format, which is a breaking change anyway.
//!
//! Section numbers (§3, §6, …) throughout refer to
//! [DESIGN.md](https://github.com/merlimat/roble/blob/main/DESIGN.md) in the
//! repository, which is not part of this crate.

mod checksum;
mod dump;
mod error;
mod footer;
mod varint;

pub mod batch;
pub mod block;
pub mod files;
pub mod filter;
pub mod key;
pub mod manifest;
pub mod rangedel;
pub mod record;
pub mod sst;
#[doc(hidden)]
pub mod value_block;

pub use block::BlockHandle;
pub use checksum::ChecksumType;
pub use dump::dump_table;
pub use error::{Error, Result};
pub use footer::{FOOTER_LEN, Footer, TableFormat};
pub use key::{InternalKey, Kind};
