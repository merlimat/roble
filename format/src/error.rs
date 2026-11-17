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

//! Errors. Every variant says what was being read and why it was refused, since
//! a format error on a follower shows up far from its cause (DESIGN.md §2).

use std::fmt;

/// The result of decoding something in Pebble's format.
pub type Result<T> = std::result::Result<T, Error>;

/// What went wrong while decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The input ended before the structure did.
    Truncated {
        /// What was being decoded.
        what: &'static str,
        /// Bytes needed.
        need: usize,
        /// Bytes available.
        have: usize,
    },
    /// A checksum did not match the bytes it covers.
    ChecksumMismatch {
        /// Offset of the block whose checksum failed.
        offset: u64,
        /// The checksum stored in the trailer.
        stored: u32,
        /// The checksum computed over the block.
        computed: u32,
    },
    /// A structure was well-formed but held a value this version rejects.
    Unsupported(String),
    /// A structure was malformed.
    Corrupt(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Truncated { what, need, have } => {
                write!(f, "{what} truncated: need {need} bytes, have {have}")
            }
            Error::ChecksumMismatch {
                offset,
                stored,
                computed,
            } => write!(
                f,
                "block at {offset}: checksum mismatch, stored {stored:#010x} != computed {computed:#010x}"
            ),
            Error::Unsupported(m) => write!(f, "unsupported: {m}"),
            Error::Corrupt(m) => write!(f, "corrupt: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    pub(crate) fn truncated<T>(what: &'static str, need: usize, have: usize) -> Result<T> {
        Err(Error::Truncated { what, need, have })
    }

    pub(crate) fn corrupt<T>(msg: impl Into<String>) -> Result<T> {
        Err(Error::Corrupt(msg.into()))
    }

    pub(crate) fn unsupported<T>(msg: impl Into<String>) -> Result<T> {
        Err(Error::Unsupported(msg.into()))
    }
}
