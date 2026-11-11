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

//! What a store can fail with.

use std::fmt;
use std::io;

/// The result of an operation on a store.
pub type Result<T> = std::result::Result<T, Error>;

/// What went wrong.
#[derive(Debug)]
pub enum Error {
    /// The filesystem or the device failed.
    Io(io::Error),
    /// Another process already owns the database directory.
    ///
    /// Two owners of one database corrupt it, so this is a refusal rather than
    /// a warning (DESIGN.md §10).
    Locked,
    /// A file was not in the format roble reads, or holds a structure it
    /// refuses to read (DESIGN.md §3).
    Format(roble_format::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{e}"),
            Error::Locked => write!(f, "the database directory is already open elsewhere"),
            Error::Format(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Locked => None,
            Error::Format(e) => Some(e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<roble_format::Error> for Error {
    fn from(e: roble_format::Error) -> Self {
        Error::Format(e)
    }
}
