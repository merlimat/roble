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

//! The names of the files in a database directory, and the markers that say
//! which of them is current (DESIGN.md §7).
//!
//! A database's shape is not written down in any one file. It is spread across
//! a directory, and the only thing that says where to start is a **file name**:
//! `marker.manifest.000001.MANIFEST-000001` names the MANIFEST to replay, and
//! `marker.format-version.000003.016` names the format the database was written
//! in. So parsing names is part of reading the format, and it lives here,
//! beside the structures, with no I/O of its own.
//!
//! Two rules in here are easy to get subtly wrong, so both are stated and
//! tested rather than left to the reader:
//!
//! - **A marker's value may contain dots.** The value is everything after the
//!   iteration's delimiter, not the next dot-separated field, which is what
//!   lets `MANIFEST-000001` be a value at all.
//! - **Every `marker.` file must parse.** Pebble treats an unparseable one as
//!   an error rather than ignoring it, because a marker it cannot read might be
//!   the current one. roble does the same: see [`current_marker`].

use std::fmt;

use crate::error::{Error, Result};

/// The format major version roble is pinned at: Pebble's
/// `FormatVirtualSSTables` (DESIGN.md §3).
///
/// This is 16, which is not a number to read off Pebble's source: the
/// constants around it are an `iota` chain with unexported entries in it. The
/// Go harness asserts the symbol equals this number, so a Pebble upgrade that
/// renumbers it fails there rather than somewhere subtler.
pub const FORMAT_VERSION_PIN: u64 = 16;

/// The oldest format major version Pebble itself will open, its
/// `FormatMinSupported`.
///
/// Below this there is nothing to be compatible with, since Pebble refuses
/// such a database too.
pub const FORMAT_VERSION_MIN: u64 = 13;

/// The marker naming the MANIFEST to replay.
pub const MARKER_MANIFEST: &str = "manifest";

/// The marker naming the format major version the database was written in.
pub const MARKER_FORMAT_VERSION: &str = "format-version";

/// What a file in a database directory is.
///
/// Not `#[non_exhaustive]`, and on purpose: roble's obsolete-file sweep
/// matches it exhaustively, so a new kind of file is a compile error there
/// that asks whether it can be deleted, rather than a `_` arm that decides
/// without asking. A new kind of file is a new Pebble format anyway.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileType {
    /// An sstable, `NNNNNN.sst`.
    Table,
    /// A write-ahead log, `NNNNNN.log`.
    Log,
    /// A log written to a secondary directory by WAL failover,
    /// `NNNNNN-NNN.log`, which is above roble's pin (§3) and refused.
    LogFailover,
    /// A MANIFEST, `MANIFEST-NNNNNN`.
    Manifest,
    /// An options record, `OPTIONS-NNNNNN`, which Pebble writes and roble
    /// reads nothing from.
    Options,
    /// The directory lock, `LOCK`.
    Lock,
    /// A half-written file Pebble was about to rename, `temporary.*.dbtmp` or
    /// `CURRENT.*.dbtmp`.
    Temp,
    /// A blob file, `NNNNNN.blob`, which arrives above roble's pin (§3).
    Blob,
}

impl FileType {
    /// The name this type goes by in an error or a dump.
    pub fn name(self) -> &'static str {
        match self {
            FileType::Table => "table",
            FileType::Log => "log",
            FileType::LogFailover => "failover log",
            FileType::Manifest => "MANIFEST",
            FileType::Options => "OPTIONS",
            FileType::Lock => "LOCK",
            FileType::Temp => "temporary file",
            FileType::Blob => "blob file",
        }
    }
}

/// A database file name, parsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParsedName {
    /// What kind of file it is.
    pub file_type: FileType,
    /// Its file number. Zero for `LOCK`, which has none.
    pub file_num: u64,
}

/// Parses a file name, or returns `None` for a name a database does not own.
///
/// `None` is not an error: a database directory may hold files that are none of
/// roble's business, and Pebble ignores them too. The one exception is a
/// `marker.` name, which [`current_marker`] insists on parsing.
pub fn parse_filename(name: &str) -> Option<ParsedName> {
    let of = |file_type, file_num| {
        Some(ParsedName {
            file_type,
            file_num,
        })
    };

    if name == "LOCK" {
        return of(FileType::Lock, 0);
    }
    if let Some(rest) = name.strip_prefix("MANIFEST-") {
        return of(FileType::Manifest, parse_file_num(rest)?);
    }
    if let Some(rest) = name.strip_prefix("OPTIONS-") {
        return of(FileType::Options, parse_file_num(rest)?);
    }
    // Pebble renames these into place, so one left behind is a crash's
    // leftover rather than part of the database.
    for prefix in ["temporary.", "CURRENT."] {
        if let Some(rest) = name.strip_prefix(prefix) {
            let num = rest.strip_suffix(".dbtmp")?;
            return of(FileType::Temp, parse_file_num(num)?);
        }
    }

    let (stem, ext) = name.rsplit_once('.')?;
    match ext {
        "sst" => of(FileType::Table, parse_file_num(stem)?),
        "blob" => of(FileType::Blob, parse_file_num(stem)?),
        // A log is either `NNNNNN.log` or, once WAL failover has written to a
        // secondary directory, `NNNNNN-NNN.log`. The second form is the only
        // sign in the directory that failover happened, so it gets its own
        // type rather than being read as an ordinary log.
        "log" => match stem.split_once('-') {
            Some((num, index)) => {
                parse_file_num(index)?;
                of(FileType::LogFailover, parse_file_num(num)?)
            }
            None => of(FileType::Log, parse_file_num(stem)?),
        },
        _ => None,
    }
}

/// Parses a file number the way Pebble's `ParseDiskFileNum` does: base ten,
/// and leading zeros are fine because that is how they are written.
fn parse_file_num(s: &str) -> Option<u64> {
    // Rust's parser accepts a leading `+`, which Pebble's does not, and which
    // would make `+00001.sst` a table.
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Spells a file number the way Pebble does, zero-padded to six digits.
///
/// Six is a minimum, not a maximum: a database that has outlived a million
/// files writes `1000000.sst`, and Pebble's parser reads it back.
pub fn file_num(num: u64) -> String {
    format!("{num:06}")
}

/// `NNNNNN.sst`.
pub fn table_filename(num: u64) -> String {
    format!("{}.sst", file_num(num))
}

/// `NNNNNN.log`.
pub fn log_filename(num: u64) -> String {
    format!("{}.log", file_num(num))
}

/// `MANIFEST-NNNNNN`.
pub fn manifest_filename(num: u64) -> String {
    format!("MANIFEST-{}", file_num(num))
}

/// A marker file name, parsed: `marker.<name>.<iter>.<value>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarkerName<'a> {
    /// Which marker this is, such as `manifest`.
    pub name: &'a str,
    /// Its iteration. Higher is newer, and a real marker's is positive.
    pub iter: u64,
    /// What it points at, such as `MANIFEST-000001`. May contain dots.
    pub value: &'a str,
}

/// Parses a marker file name.
///
/// Every name beginning `marker.` has to parse, so this returns an error
/// rather than `None`: an unreadable marker may be the current one, and
/// guessing which would mean opening the wrong MANIFEST.
pub fn parse_marker(filename: &str) -> Result<MarkerName<'_>> {
    let bad = || Error::Corrupt(format!("marker file name {filename:?}"));

    let rest = filename.strip_prefix("marker.").ok_or_else(bad)?;
    let (name, rest) = rest.split_once('.').ok_or_else(bad)?;
    // The value is everything after this delimiter, dots included, which is
    // what lets `MANIFEST-000001` be a value.
    let (iter, value) = rest.split_once('.').ok_or_else(bad)?;
    let iter = parse_file_num(iter).ok_or_else(bad)?;
    Ok(MarkerName { name, iter, value })
}

/// Spells a marker file name: `marker.<name>.<%06d iter>.<value>`.
pub fn marker_filename(name: &str, iter: u64, value: &str) -> String {
    format!("marker.{name}.{}.{value}", file_num(iter))
}

/// The marker value for a format major version, zero-padded to three digits as
/// Pebble's `FormatMajorVersion.String` writes it.
pub fn format_version_value(version: u64) -> String {
    format!("{version:03}")
}

/// Which marker file is current, and which are leftovers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentMarker {
    /// The current marker's whole file name.
    pub filename: String,
    /// Its iteration.
    pub iter: u64,
    /// What it points at.
    pub value: String,
    /// Earlier markers for the same name, which a crash during rotation can
    /// leave behind. Removing them is tidying, not recovery.
    pub obsolete: Vec<String>,
}

/// Finds the current marker of a given name in a directory listing.
///
/// The highest iteration wins; the rest are obsolete. `None` means the marker
/// was never placed, which for `manifest` is how an empty directory is told
/// from a database.
///
/// Any name beginning `marker.` must parse, even one belonging to a different
/// marker, because a name this cannot read might be the current one.
pub fn current_marker<'a>(
    listing: impl IntoIterator<Item = &'a str>,
    name: &str,
) -> Result<Option<CurrentMarker>> {
    let mut current: Option<CurrentMarker> = None;
    for filename in listing {
        if !filename.starts_with("marker.") {
            continue;
        }
        let parsed = parse_marker(filename)?;
        if parsed.name != name {
            continue;
        }
        match &mut current {
            // A newer one: the one held so far becomes a leftover.
            Some(held) if held.iter < parsed.iter => {
                let superseded = std::mem::replace(&mut held.filename, filename.to_string());
                held.obsolete.push(superseded);
                held.iter = parsed.iter;
                held.value = parsed.value.to_string();
            }
            Some(held) => held.obsolete.push(filename.to_string()),
            None => {
                current = Some(CurrentMarker {
                    filename: filename.to_string(),
                    iter: parsed.iter,
                    value: parsed.value.to_string(),
                    obsolete: Vec::new(),
                })
            }
        }
    }
    Ok(current)
}

impl fmt::Display for MarkerName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", marker_filename(self.name, self.iter, self.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_parse_as_pebble_spells_them() {
        let cases: &[(&str, Option<(FileType, u64)>)] = &[
            ("LOCK", Some((FileType::Lock, 0))),
            ("MANIFEST-000001", Some((FileType::Manifest, 1))),
            ("OPTIONS-000003", Some((FileType::Options, 3))),
            ("000007.sst", Some((FileType::Table, 7))),
            ("000002.log", Some((FileType::Log, 2))),
            ("000004.blob", Some((FileType::Blob, 4))),
            ("temporary.000009.dbtmp", Some((FileType::Temp, 9))),
            ("CURRENT.000009.dbtmp", Some((FileType::Temp, 9))),
            // Six digits is a minimum, not a maximum: a long-lived database
            // writes more.
            ("1000000.sst", Some((FileType::Table, 1_000_000))),
            ("7.sst", Some((FileType::Table, 7))),
            // Not a database's business, and not an error either.
            ("CURRENT", None),
            ("README", None),
            ("000007.sst.bak", None),
            ("oxia-key-encoding-format", None),
            ("MANIFEST-", None),
            ("MANIFEST-00x1", None),
            (".sst", None),
            // Rust's integer parser accepts a leading sign and Pebble's does
            // not, so this would otherwise be table 1.
            ("+00001.sst", None),
            ("-00001.sst", None),
        ];
        for (name, want) in cases {
            let got = parse_filename(name).map(|p| (p.file_type, p.file_num));
            assert_eq!(got, *want, "parsing {name:?}");
        }
    }

    #[test]
    fn a_failover_log_is_told_from_an_ordinary_one() {
        // The index after the dash is the only sign in a directory that WAL
        // failover happened, so it gets its own type rather than reading as
        // log 4.
        let plain = parse_filename("000004.log").expect("a log");
        assert_eq!(plain.file_type, FileType::Log);
        assert_eq!(plain.file_num, 4);

        let failover = parse_filename("000004-001.log").expect("a failover log");
        assert_eq!(failover.file_type, FileType::LogFailover);
        assert_eq!(failover.file_num, 4, "still log 4, written elsewhere");

        assert!(parse_filename("000004-.log").is_none());
        assert!(parse_filename("000004-x.log").is_none());
    }

    #[test]
    fn a_marker_value_may_contain_dots() {
        // This is the rule that makes markers work at all: the value is
        // everything after the iteration's delimiter. Splitting on the next
        // dot would truncate a MANIFEST name, and any value with a dot in it
        // would be read as a shorter one.
        let m = parse_marker("marker.manifest.000001.MANIFEST-000001").expect("parse");
        assert_eq!(m.name, "manifest");
        assert_eq!(m.iter, 1);
        assert_eq!(m.value, "MANIFEST-000001");

        let m = parse_marker("marker.format-version.000003.016").expect("parse");
        assert_eq!(m.name, "format-version", "the name may contain a dash");
        assert_eq!(m.iter, 3);
        assert_eq!(m.value, "016");

        let m = parse_marker("marker.odd.000002.a.b.c").expect("parse");
        assert_eq!(m.value, "a.b.c", "dots belong to the value");
    }

    #[test]
    fn a_marker_name_that_does_not_parse_is_an_error() {
        // Not `None`: an unreadable marker may be the current one, so guessing
        // means opening the wrong MANIFEST.
        for bad in [
            "marker.manifest",
            "marker.manifest.",
            "marker.manifest.x.MANIFEST-000001",
            "marker.manifest.-1.MANIFEST-000001",
            "marker.",
            "marker",
            "markers.manifest.000001.x",
        ] {
            assert!(parse_marker(bad).is_err(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn names_round_trip_through_their_spellings() {
        assert_eq!(table_filename(7), "000007.sst");
        assert_eq!(log_filename(2), "000002.log");
        assert_eq!(manifest_filename(1), "MANIFEST-000001");
        assert_eq!(
            marker_filename(MARKER_MANIFEST, 1, &manifest_filename(1)),
            "marker.manifest.000001.MANIFEST-000001"
        );
        assert_eq!(
            marker_filename(MARKER_FORMAT_VERSION, 3, &format_version_value(16)),
            "marker.format-version.000003.016"
        );
        // The value Pebble writes is zero-padded to three digits, and parses
        // back to the number whichever way it was written.
        assert_eq!(format_version_value(FORMAT_VERSION_PIN), "016");
        assert_eq!("016".parse::<u64>().unwrap(), 16);
    }

    #[test]
    fn the_highest_iteration_is_current_and_the_rest_are_leftovers() {
        // A crash during rotation leaves the old file behind, in either order
        // in the listing, and the newer one still has to win.
        for listing in [
            vec![
                "marker.manifest.000001.MANIFEST-000001",
                "marker.manifest.000007.MANIFEST-000009",
                "marker.manifest.000003.MANIFEST-000004",
            ],
            vec![
                "marker.manifest.000007.MANIFEST-000009",
                "marker.manifest.000003.MANIFEST-000004",
                "marker.manifest.000001.MANIFEST-000001",
            ],
        ] {
            let current = current_marker(listing.iter().copied(), MARKER_MANIFEST)
                .expect("parse")
                .expect("a marker");
            assert_eq!(current.iter, 7);
            assert_eq!(current.value, "MANIFEST-000009");
            assert_eq!(current.obsolete.len(), 2, "{:?}", current.obsolete);
            assert!(!current.obsolete.contains(&current.filename));
        }
    }

    #[test]
    fn markers_of_other_names_are_left_alone_but_still_have_to_parse() {
        let listing = [
            "marker.format-version.000003.016",
            "marker.manifest.000001.MANIFEST-000001",
            "000007.sst",
        ];
        let m = current_marker(listing.iter().copied(), MARKER_MANIFEST)
            .expect("parse")
            .expect("a marker");
        assert_eq!(m.value, "MANIFEST-000001");
        assert!(m.obsolete.is_empty(), "the format marker is not a leftover");

        // A different marker's name still has to parse, because a name this
        // cannot read might be the one being looked for.
        let broken = ["marker.somethingelse.bad.value"];
        assert!(current_marker(broken.iter().copied(), MARKER_MANIFEST).is_err());
    }

    #[test]
    fn an_absent_marker_is_not_an_error() {
        // For the manifest marker this is how an empty directory is told from
        // a database, so it must not be an error.
        let listing = ["000007.sst", "LOCK"];
        assert_eq!(
            current_marker(listing.iter().copied(), MARKER_MANIFEST).expect("parse"),
            None
        );
    }
}
