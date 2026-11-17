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

//! The table footer (`sstable/table.go`: `rocksDBFooterLen`, `pebbleDBMagic`,
//! `parseFooter`) and the table formats (`sstable/format.go`: `parseTableFormat`).
//!
//! The footer is the last 53 bytes of a Pebblev1..v4 table:
//!
//! ```text
//!   [0]        checksum type
//!   [1..41)    metaindex handle, then index handle, as uvarints, zero-padded
//!   [41..45)   format version, u32 little-endian
//!   [45..53)   magic, "\xf0\x9f\xaa\xb3" twice
//! ```
//!
//! Pebblev5 and above are longer and are refused rather than half-read
//! (DESIGN.md §3).

use crate::block::BlockHandle;
use crate::checksum::ChecksumType;
use crate::error::{Error, Result};

/// `pebbleDBMagic`: two copies of U+1FAB3, the cockroach.
pub(crate) const PEBBLE_MAGIC: &[u8; 8] = b"\xf0\x9f\xaa\xb3\xf0\x9f\xaa\xb3";

/// `rocksDBMagic`, which shares the Pebble footer layout at version 2.
pub(crate) const ROCKSDB_MAGIC: &[u8; 8] = b"\xf7\xcf\xf4\x85\xb7\x41\xe2\x88";

/// `levelDBMagic`, whose 48-byte footer roble does not read.
pub(crate) const LEVELDB_MAGIC: &[u8; 8] = b"\x57\xfb\x80\x8b\x24\x75\x47\xdb";

/// `rocksDBFooterLen`: 1 + 2 * 20 + 4 + 8.
pub const FOOTER_LEN: usize = 53;

const MAGIC_LEN: usize = 8;
const VERSION_LEN: usize = 4;

/// The table format, which decides how a block's entries are laid out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum TableFormat {
    /// `TableFormatRocksDBv2`, the oldest a Pebble database can hold.
    RocksDBv2,
    /// `TableFormatPebblev1`: block properties.
    Pebblev1,
    /// `TableFormatPebblev2`: range keys.
    Pebblev2,
    /// `TableFormatPebblev3`: value blocks, and a prefix byte on every `SET` value.
    Pebblev3,
    /// `TableFormatPebblev4`: `DELSIZED` and the obsolete bit. roble's write format.
    Pebblev4,
}

impl TableFormat {
    /// Whether a `SET` value in a data block carries a one-byte value prefix.
    ///
    /// This is the detail most likely to be silently wrong: from Pebblev3 on,
    /// *every* `SET` value is stored as one prefix byte followed by the value,
    /// whether or not the value itself went to a value block (DESIGN.md §3).
    pub fn has_value_prefix(self) -> bool {
        self >= TableFormat::Pebblev3
    }

    /// Whether a key's trailer may carry the obsolete bit.
    pub fn has_obsolete_bit(self) -> bool {
        self >= TableFormat::Pebblev4
    }

    /// The name used in dumps.
    pub fn name(self) -> &'static str {
        match self {
            TableFormat::RocksDBv2 => "rocksdbv2",
            TableFormat::Pebblev1 => "pebblev1",
            TableFormat::Pebblev2 => "pebblev2",
            TableFormat::Pebblev3 => "pebblev3",
            TableFormat::Pebblev4 => "pebblev4",
        }
    }

    fn parse(magic: &[u8], version: u32) -> Result<Self> {
        if magic == ROCKSDB_MAGIC {
            return if version == 2 {
                Ok(TableFormat::RocksDBv2)
            } else {
                Error::unsupported(format!("rocksdb table format version {version}"))
            };
        }
        debug_assert_eq!(magic, PEBBLE_MAGIC);
        match version {
            1 => Ok(TableFormat::Pebblev1),
            2 => Ok(TableFormat::Pebblev2),
            3 => Ok(TableFormat::Pebblev3),
            4 => Ok(TableFormat::Pebblev4),
            // Pebblev5 is columnar, v6 adds a footer checksum and blob handles,
            // v7 footer attributes. All are above the pin, so the table is
            // refused whole rather than partly read (DESIGN.md §3).
            5..=7 => Error::unsupported(format!(
                "table format pebblev{version} is above roble's pin of pebblev4"
            )),
            other => Error::unsupported(format!("pebble table format version {other}")),
        }
    }
}

/// A decoded table footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    /// The table format.
    pub format: TableFormat,
    /// The checksum every block in this table uses.
    pub checksum: ChecksumType,
    /// Where the metaindex block is.
    pub metaindex: BlockHandle,
    /// Where the (top-level) index block is.
    pub index: BlockHandle,
}

impl Footer {
    /// Reads the footer from the end of a table of `size` bytes.
    ///
    /// `tail` must be the last [`FOOTER_LEN`] bytes of the table; `size` is the
    /// whole table's length, used to reject a handle that points past the end.
    pub fn decode(tail: &[u8], size: u64) -> Result<Self> {
        if tail.len() < FOOTER_LEN {
            return Error::truncated("footer", FOOTER_LEN, tail.len());
        }
        let footer = &tail[tail.len() - FOOTER_LEN..];
        let magic = &footer[FOOTER_LEN - MAGIC_LEN..];
        if magic == LEVELDB_MAGIC {
            return Error::unsupported(
                "leveldb table format: its 48-byte footer predates everything roble reads",
            );
        }
        if magic != PEBBLE_MAGIC && magic != ROCKSDB_MAGIC {
            return Error::corrupt(format!(
                "bad table magic {:02x?}, not a Pebble sstable",
                magic
            ));
        }
        let version_at = FOOTER_LEN - MAGIC_LEN - VERSION_LEN;
        let version = u32::from_le_bytes(
            footer[version_at..version_at + VERSION_LEN]
                .try_into()
                .expect("4 bytes"),
        );
        let format = TableFormat::parse(magic, version)?;
        let checksum = ChecksumType::from_byte(footer[0])?;

        let (metaindex, n) = BlockHandle::decode(&footer[1..])?;
        let (index, _) = BlockHandle::decode(&footer[1 + n..])?;
        for (name, h) in [("metaindex", metaindex), ("index", index)] {
            if h.offset + h.length > size {
                return Error::corrupt(format!(
                    "{name} handle {h:?} runs past the end of a {size}-byte table"
                ));
            }
        }
        Ok(Footer {
            format,
            checksum,
            metaindex,
            index,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A footer naming `magic` and `version`, with both handles at zero.
    ///
    /// Built rather than patched out of a fixture, because what is under test
    /// is the footer alone: the magic, the version and the refusals either
    /// side of them. Both block handles encode as a zero varint pair, which is
    /// in range for a table of any size.
    fn footer_of(magic: &[u8; MAGIC_LEN], version: u32) -> Vec<u8> {
        let mut f = vec![0u8; FOOTER_LEN];
        f[0] = 1; // crc32c
        let version_at = FOOTER_LEN - MAGIC_LEN - VERSION_LEN;
        f[version_at..version_at + VERSION_LEN].copy_from_slice(&version.to_le_bytes());
        f[FOOTER_LEN - MAGIC_LEN..].copy_from_slice(magic);
        f
    }

    /// The footer is the one structure in a table no checksum covers, so a
    /// flipped bit in a handle reaches this check as it is. An offset and a
    /// length whose sum is past `u64::MAX` must be refused, not added.
    #[test]
    fn a_handle_past_the_end_of_u64_is_refused() {
        let mut f = footer_of(PEBBLE_MAGIC, 4);
        // The metaindex handle: offset u64::MAX - 2 in ten varint bytes,
        // then length 10 in one. The index handle after it is zero.
        let mut handle = vec![0xff; 9];
        handle.extend_from_slice(&[0x01, 0x0a]);
        handle[0] = 0xfd;
        f[1..1 + handle.len()].copy_from_slice(&handle);
        let err = Footer::decode(&f, 1 << 20).expect_err("an overflowing handle");
        assert!(matches!(err, Error::Corrupt(_)), "got {err:?}");
    }

    /// Every version roble reads, decoded from the footer that names it.
    ///
    /// The corpus carries tables at v1 through v4, so these are pinned there
    /// too; what it cannot carry is RocksDBv2, which the Pebble the harness is
    /// built against will no longer write — `TableFormatMinSupported` is
    /// Pebblev1 — and which roble still has to read, because a database old
    /// enough to hold one is exactly the database nobody has looked at.
    #[test]
    fn each_format_is_decoded_from_its_magic_and_version() {
        let cases: &[(&[u8; MAGIC_LEN], u32, TableFormat, &str)] = &[
            (ROCKSDB_MAGIC, 2, TableFormat::RocksDBv2, "rocksdbv2"),
            (PEBBLE_MAGIC, 1, TableFormat::Pebblev1, "pebblev1"),
            (PEBBLE_MAGIC, 2, TableFormat::Pebblev2, "pebblev2"),
            (PEBBLE_MAGIC, 3, TableFormat::Pebblev3, "pebblev3"),
            (PEBBLE_MAGIC, 4, TableFormat::Pebblev4, "pebblev4"),
        ];
        for &(magic, version, want, name) in cases {
            let footer = Footer::decode(&footer_of(magic, version), 1 << 20)
                .unwrap_or_else(|e| panic!("{name} should decode: {e}"));
            assert_eq!(footer.format, want, "{name}");
            assert_eq!(want.name(), name);
        }
    }

    /// The value prefix arrives at v3 and the obsolete bit at v4, which is the
    /// whole reason the version is read rather than assumed.
    #[test]
    fn the_version_decides_what_a_value_and_a_trailer_carry() {
        use TableFormat::*;
        for f in [RocksDBv2, Pebblev1, Pebblev2] {
            assert!(!f.has_value_prefix(), "{f:?}");
            assert!(!f.has_obsolete_bit(), "{f:?}");
        }
        assert!(Pebblev3.has_value_prefix());
        assert!(!Pebblev3.has_obsolete_bit());
        assert!(Pebblev4.has_value_prefix());
        assert!(Pebblev4.has_obsolete_bit());
    }

    #[test]
    fn a_version_roble_does_not_read_is_refused_by_name() {
        // Above the pin: columnar blocks, a footer checksum, footer
        // attributes. Each is refused whole rather than partly read (§3).
        for version in 5..=7u32 {
            let err = Footer::decode(&footer_of(PEBBLE_MAGIC, version), 1 << 20)
                .expect_err("above the pin");
            let text = format!("{err}");
            assert!(
                text.contains(&format!("pebblev{version}")) && text.contains("pin"),
                "got {text:?}"
            );
            assert!(matches!(err, Error::Unsupported(_)), "got {err:?}");
        }

        // Past anything Pebble has defined, and the zero it never writes.
        for version in [0u32, 8, 99] {
            let err = Footer::decode(&footer_of(PEBBLE_MAGIC, version), 1 << 20)
                .expect_err("not a version");
            assert!(
                format!("{err}").contains(&format!("version {version}")),
                "got {err:?}"
            );
        }

        // RocksDB's magic carries its own version space, and only v2 of it.
        let err = Footer::decode(&footer_of(ROCKSDB_MAGIC, 3), 1 << 20).expect_err("rocksdb v3");
        assert!(
            format!("{err}").contains("rocksdb table format version 3"),
            "got {err:?}"
        );
    }

    #[test]
    fn a_footer_that_is_not_a_pebble_footer_says_which_kind_of_wrong() {
        // LevelDB is named rather than called corrupt: the bytes are a real
        // footer, just one whose 48-byte shape predates everything roble
        // reads.
        let err = Footer::decode(&footer_of(LEVELDB_MAGIC, 0), 1 << 20).expect_err("leveldb");
        assert!(format!("{err}").contains("leveldb"), "got {err:?}");
        assert!(matches!(err, Error::Unsupported(_)), "got {err:?}");

        // Anything else is not a table at all.
        let err = Footer::decode(&footer_of(b"notatabl", 4), 1 << 20).expect_err("bad magic");
        assert!(format!("{err}").contains("bad table magic"), "got {err:?}");
        assert!(matches!(err, Error::Corrupt(_)), "got {err:?}");

        // And a tail too short to hold a footer is truncated, not corrupt:
        // the difference is whether there is reason to think bytes are
        // missing.
        let short = footer_of(PEBBLE_MAGIC, 4)[..FOOTER_LEN - 1].to_vec();
        let err = Footer::decode(&short, 1 << 20).expect_err("short tail");
        assert!(matches!(err, Error::Truncated { .. }), "got {err:?}");
    }

    /// A handle that points outside the table is caught at the footer rather
    /// than at the read.
    ///
    /// The footer is the first thing read and the last thing written, so a
    /// handle past the end means the table was cut short or the footer was
    /// not the one that belongs to it. Catching it here is what lets every
    /// later read assume its handles are in range.
    #[test]
    fn a_handle_that_runs_past_the_end_is_refused_at_the_footer() {
        let mut f = footer_of(PEBBLE_MAGIC, 4);
        // The metaindex handle, as two single-byte varints: offset 10,
        // length 10, in a table of 5 bytes.
        f[1] = 10;
        f[2] = 10;
        let err = Footer::decode(&f, 5).expect_err("a handle past the end");
        let text = format!("{err}");
        assert!(
            text.contains("metaindex handle") && text.contains("runs past the end"),
            "the refusal should name the handle, got {text:?}"
        );
        assert!(matches!(err, Error::Corrupt(_)), "got {err:?}");
    }
}
