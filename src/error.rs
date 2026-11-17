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
    /// The database is valid Pebble, but uses something roble does not read:
    /// a format major version above the pin, a comparer other than bytewise,
    /// WAL failover (DESIGN.md §3).
    ///
    /// Separate from [`Error::Format`] because the subject is the database
    /// rather than one file, and because the answer is different: a refusal
    /// here means roble is the wrong reader, not that the bytes are bad.
    Unsupported(String),
    /// The database's own metadata does not hang together — a MANIFEST edit
    /// that does not apply, a table the version names and the directory does
    /// not hold.
    Corrupt(String),
    /// There is no database in the directory.
    NotFound(String),
    /// The store was opened read-only, and this would change the directory
    /// ([`crate::open_read_only`]).
    ReadOnly,
    /// A store opened read-only needed a file the process writing the
    /// database has retired or not finished writing; opening again reads the
    /// database as it is now (DESIGN.md §6, "Reading beside a writer").
    Stale(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{e}"),
            Error::Locked => write!(f, "the database directory is already open elsewhere"),
            Error::Format(e) => write!(f, "{e}"),
            Error::Unsupported(m) => write!(f, "unsupported: {m}"),
            Error::Corrupt(m) => write!(f, "corrupt: {m}"),
            Error::NotFound(m) => write!(f, "{m}"),
            Error::ReadOnly => write!(f, "the database was opened read-only"),
            Error::Stale(m) => write!(f, "stale: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Locked => None,
            Error::Format(e) => Some(e),
            Error::Unsupported(_)
            | Error::Corrupt(_)
            | Error::NotFound(_)
            | Error::ReadOnly
            | Error::Stale(_) => None,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn io() -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, "a reason the OS gave")
    }

    /// The prefix is the operator-facing distinction, so it is what is pinned.
    ///
    /// Every test in this crate matches on the variant — `Err(Error::Corrupt(m))`
    /// and then `m.contains(..)` — which never formats anything. So the strings
    /// someone reads when a database will not open had never been produced.
    /// "corrupt:" means the file is damaged and "unsupported:" means roble is
    /// the wrong reader for it, and those call for different actions.
    #[test]
    fn a_refusal_says_which_kind_of_refusal_it_is() {
        assert_eq!(
            Error::Corrupt("the MANIFEST ends early".to_string()).to_string(),
            "corrupt: the MANIFEST ends early"
        );
        assert_eq!(
            Error::Unsupported("range keys".to_string()).to_string(),
            "unsupported: range keys"
        );
    }

    /// The three that carry their own wording, unprefixed.
    ///
    /// `NotFound` is a sentence already — "no database in /tmp/x: it holds no
    /// marker..." — and prefixing it would read as a category where the text
    /// is the explanation. `Io` and `Format` defer to an error that has
    /// already said it better than a wrapper could.
    #[test]
    fn the_errors_that_speak_for_themselves_are_not_dressed_up() {
        assert_eq!(Error::Io(io()).to_string(), io().to_string());

        let inner = roble_format::Error::Corrupt("a bad block".to_string());
        assert_eq!(Error::Format(inner).to_string(), "corrupt: a bad block");

        assert_eq!(
            Error::NotFound("no database in /tmp/x".to_string()).to_string(),
            "no database in /tmp/x"
        );
        assert_eq!(
            Error::Locked.to_string(),
            "the database directory is already open elsewhere"
        );
    }

    /// What an error chain walker finds underneath.
    ///
    /// `anyhow` and `tracing` print the chain rather than the top line, so a
    /// source that came back `None` would drop the operating system's own
    /// wording from a log at the moment it was the only useful part.
    #[test]
    fn the_errors_that_wrap_another_hand_it_over() {
        use std::error::Error as _;

        let wrapped = Error::Io(io());
        let source = wrapped.source().expect("an io error has a source");
        assert_eq!(source.to_string(), io().to_string());

        let inner = roble_format::Error::Corrupt("a bad block".to_string());
        let wrapped = Error::Format(inner);
        let source = wrapped.source().expect("a format error has a source");
        assert!(source.to_string().contains("a bad block"), "{source}");

        // And the ones that are the whole story have nothing underneath.
        for e in [
            Error::Locked,
            Error::Unsupported("x".to_string()),
            Error::Corrupt("x".to_string()),
            Error::NotFound("x".to_string()),
        ] {
            assert!(e.source().is_none(), "{e} should have no source");
        }
    }

    #[test]
    fn an_io_error_becomes_one_of_ours_without_losing_its_kind() {
        // The `?` in every filesystem call goes through this conversion, so
        // the kind has to survive it: `main` tells a broken pipe from a real
        // failure by asking for it.
        let converted: Error = io().into();
        match converted {
            Error::Io(e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
            other => panic!("expected Io, got {other:?}"),
        }
    }
}
