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

mod checksum;
mod dump;
mod error;
mod footer;
mod varint;

pub mod batch;
pub mod block;
pub mod filter;
pub mod key;
pub mod rangedel;
pub mod record;
pub mod sst;

pub use block::{BlockBuilder, BlockHandle};
pub use checksum::ChecksumType;
pub use dump::{dump_table, first_difference};
pub use error::{Error, Result};
pub use footer::{Footer, TableFormat};
pub use key::{InternalKey, Kind};
