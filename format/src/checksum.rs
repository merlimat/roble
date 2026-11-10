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

//! Block checksums (`sstable/block`: `ChecksumType`, `Checksummer`, and
//! `internal/crc`).
//!
//! Pebble's CRC is CRC-32 Castagnoli over the block bytes *followed by the
//! one-byte block type*, then "cooked" by a rotation and an offset so that
//! arbitrary key data is less likely to look like a checksum. Forgetting
//! either the trailing type byte or the cooking produces a checksum that is
//! wrong for every block, which is at least a loud failure.

use crate::error::{Error, Result};

/// The checksum over a block, from the first byte of the footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
    /// `ChecksumTypeCRC32c`, Pebble's default and the only one roble writes.
    Crc32c,
    /// `ChecksumTypeXXHash64`, read because a table may have been written with it.
    XxHash64,
}

impl ChecksumType {
    /// Decodes the checksum type byte at the start of the footer.
    pub fn from_byte(b: u8) -> Result<Self> {
        match b {
            1 => Ok(ChecksumType::Crc32c),
            3 => Ok(ChecksumType::XxHash64),
            // 0 (none) and 2 (the 32-bit xxhash) are not written by any Pebble
            // that could have produced a table roble opens.
            other => Error::unsupported(format!("checksum type {other}")),
        }
    }

    /// The name the `pebble` tool prints, which the golden dumps use.
    pub fn name(self) -> &'static str {
        match self {
            ChecksumType::Crc32c => "crc32c",
            ChecksumType::XxHash64 => "xxhash64",
        }
    }

    /// Computes the checksum over `data` and the block type byte.
    pub fn compute(self, data: &[u8], block_type: u8) -> Result<u32> {
        match self {
            ChecksumType::Crc32c => {
                let c = crc32c::crc32c_append(0, data);
                let c = crc32c::crc32c_append(c, &[block_type]);
                // internal/crc: Value() = (c>>15 | c<<17) + 0xa282ead8, which
                // is a rotation, since 15 + 17 == 32.
                Ok(c.rotate_right(15).wrapping_add(0xa282_ead8))
            }
            ChecksumType::XxHash64 => {
                Error::unsupported("xxhash64 block checksums are not implemented yet")
            }
        }
    }
}
