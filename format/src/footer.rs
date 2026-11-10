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
