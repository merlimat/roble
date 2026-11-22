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
///
/// `#[non_exhaustive]`, so that a kind of failure added later is not a
/// breaking change: a `match` on it needs a `_` arm.
#[derive(Debug)]
#[non_exhaustive]
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
    Format(FormatError),
    /// The database is valid Pebble, but uses something roble does not read:
    /// a format major version above the pin, a comparer other than bytewise,
    /// WAL failover (DESIGN.md §3), or logs not yet flushed whose records run
    /// past the 4 GiB one memtable's offsets reach, which roble replays into
    /// one memtable (DESIGN.md §6).
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
    NoDatabase(String),
    /// The store was opened read-only, and this would change the directory
    /// ([`crate::open_read_only`]).
    ReadOnly,
    /// A store opened read-only needed a file the process writing the
    /// database has retired, cut or not finished writing; opening again reads
    /// the database as it is now (DESIGN.md §6, "Reading beside a writer").
    Stale(String),
    /// The caller asked for something no database could do: a
    /// [`delete_range`](crate::Db::delete_range) whose start is not below its
    /// end, a batch past 2³² − 1 entries, a batch whose records could take
    /// more of a memtable than one holds, or a step of a scan an error left on
    /// no key ([`Scan`](crate::Scan)).
    ///
    /// Nothing was read and nothing on disk is wrong, which is what sets it
    /// apart from [`Error::Format`] and [`Error::Corrupt`].
    InvalidArgument(String),
}

/// What [`Error::Format`] carries: a file roble could not read, and why.
///
/// Opaque, so that roble's error type does not change whenever the format
/// crate's does. Its message says which structure was refused and why, and
/// [`source`](std::error::Error::source) hands over `roble_format::Error`
/// itself, for a caller that depends on that crate and wants the detail.
#[derive(Debug)]
pub struct FormatError(roble_format::Error);

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for FormatError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{e}"),
            Error::Locked => write!(f, "the database directory is already open elsewhere"),
            Error::Format(e) => write!(f, "{e}"),
            Error::Unsupported(m) => write!(f, "unsupported: {m}"),
            Error::Corrupt(m) => write!(f, "corrupt: {m}"),
            Error::NoDatabase(m) => write!(f, "{m}"),
            Error::ReadOnly => write!(f, "the database was opened read-only"),
            Error::Stale(m) => write!(f, "stale: {m}"),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Locked => None,
            // The format crate's error, not the wrapper: the wrapper says
            // nothing the error it wraps does not, and a chain walker would
            // print the same line twice.
            Error::Format(e) => Some(&e.0),
            Error::Unsupported(_)
            | Error::Corrupt(_)
            | Error::NoDatabase(_)
            | Error::ReadOnly
            | Error::Stale(_)
            | Error::InvalidArgument(_) => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

/// What lets `?` carry a format error out of the engine.
///
/// Hidden, because it names the format crate's type: it is for roble's own
/// code, and not a way in that callers should come to rely on.
#[doc(hidden)]
impl From<roble_format::Error> for Error {
    fn from(e: roble_format::Error) -> Self {
        match e {
            // The caller's mistake, found while the format crate encoded what
            // it was given: not a file roble could not read, which is what
            // `Format` says.
            roble_format::Error::InvalidArgument(m) => Error::InvalidArgument(m),
            e => Error::Format(FormatError(e)),
        }
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
        // "stale:" means try again, which a reader beside a writer should,
        // and is not damage.
        assert_eq!(
            Error::Stale("000004.sst is gone".to_string()).to_string(),
            "stale: 000004.sst is gone"
        );
        assert_eq!(
            Error::ReadOnly.to_string(),
            "the database was opened read-only"
        );
        // "invalid argument:" means the caller's mistake, with nothing on
        // disk to look at.
        assert_eq!(
            Error::InvalidArgument("an empty range".to_string()).to_string(),
            "invalid argument: an empty range"
        );
    }

    /// The format crate's `InvalidArgument` is the caller's mistake, found
    /// while encoding what it was given, so it comes out as roble's own and
    /// not as `Format`, which says a file could not be read.
    #[test]
    fn a_format_invalid_argument_is_not_a_format_error() {
        let e = Error::from(roble_format::Error::InvalidArgument("x".to_string()));
        assert!(
            matches!(e, Error::InvalidArgument(ref m) if m == "x"),
            "{e:?}"
        );
        assert!(
            std::error::Error::source(&e).is_none(),
            "nothing underneath the caller's mistake"
        );
    }

    /// The three that carry their own wording, unprefixed.
    ///
    /// `NoDatabase` is a sentence already — "no database in /tmp/x: it holds no
    /// marker..." — and prefixing it would read as a category where the text
    /// is the explanation. `Io` and `Format` defer to an error that has
    /// already said it better than a wrapper could.
    #[test]
    fn the_errors_that_speak_for_themselves_are_not_dressed_up() {
        assert_eq!(Error::Io(io()).to_string(), io().to_string());

        let inner = roble_format::Error::Corrupt("a bad block".to_string());
        assert_eq!(Error::from(inner).to_string(), "corrupt: a bad block");

        assert_eq!(
            Error::NoDatabase("no database in /tmp/x".to_string()).to_string(),
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
        let wrapped = Error::from(inner.clone());
        let source = wrapped.source().expect("a format error has a source");
        assert_eq!(
            source.downcast_ref::<roble_format::Error>(),
            Some(&inner),
            "the source is the format crate's own error"
        );
        assert!(
            source.source().is_none(),
            "and the chain ends there, with the line said once"
        );

        // The wrapper on its own hands over the same error.
        let Error::Format(format) = wrapped else {
            unreachable!()
        };
        let source = format.source().expect("the wrapper has a source");
        assert_eq!(source.downcast_ref::<roble_format::Error>(), Some(&inner));

        // And the ones that are the whole story have nothing underneath.
        for e in [
            Error::Locked,
            Error::Unsupported("x".to_string()),
            Error::Corrupt("x".to_string()),
            Error::NoDatabase("x".to_string()),
            Error::InvalidArgument("x".to_string()),
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
