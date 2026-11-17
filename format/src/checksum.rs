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

use crc_fast::CrcAlgorithm::Crc32Iscsi;

use crate::error::{Error, Result};

/// The checksum over a block, from the first byte of the footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChecksumType {
    /// `ChecksumTypeCRC32c`, Pebble's default and the only one roble writes.
    Crc32c,
    /// `ChecksumTypeXXHash64`.
    ///
    /// Not reachable in an Oxia table today — Pebble defaults to crc32c
    /// (`sstable/options.go`: `ensureDefaults`) and Oxia never sets `Checksum`
    /// — but unlike MinLZ, which the table format forbids outright, this is
    /// one option away. The cost of reading it is a few lines; the cost of
    /// being wrong is a shard that will not open.
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
    pub fn compute(self, data: &[u8], block_type: u8) -> u32 {
        match self {
            ChecksumType::Crc32c => {
                let mut d = crc_fast::Digest::new(Crc32Iscsi);
                d.update(data);
                d.update(&[block_type]);
                cook(d.finalize() as u32)
            }
            ChecksumType::XxHash64 => {
                // block.go: uint32(xxhash.Sum64(block || blockType)) — the low
                // 32 bits of the 64-bit hash, with no cooking.
                let mut buf = Vec::with_capacity(data.len() + 1);
                buf.extend_from_slice(data);
                buf.push(block_type);
                twox_hash::XxHash64::oneshot(0, &buf) as u32
            }
        }
    }
}

/// The cooked crc32c over exactly `data`.
///
/// What record framing checksums: a byte range with no trailing block type,
/// and always crc32c, whatever a table's footer says. A function rather than
/// a method on [`ChecksumType`], because it never was a choice between the
/// two — as a method it took `self` and ignored it, so asking an xxhash64
/// for it handed back a crc32c.
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    cook(crc_fast::checksum(Crc32Iscsi, data) as u32)
}

/// internal/crc: Value() = (c>>15 | c<<17) + 0xa282ead8, which is a
/// rotation, since 15 + 17 == 32.
fn cook(c: u32) -> u32 {
    c.rotate_right(15).wrapping_add(0xa282_ead8)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each type computes its own checksum. `compute_raw`, which this
    /// replaced, took the type and ignored it, so an xxhash64 table asked
    /// for one got a crc32c.
    #[test]
    fn each_checksum_type_computes_its_own() {
        let data = b"a block's bytes";
        let crc = ChecksumType::Crc32c.compute(data, 0);
        let xx = ChecksumType::XxHash64.compute(data, 0);
        assert_ne!(crc, xx);
        assert_eq!(
            xx,
            twox_hash::XxHash64::oneshot(0, b"a block's bytes\0") as u32
        );
        // And the record checksum is the crc32c of the bytes alone, with no
        // block type after them.
        assert_ne!(crc32c(data), crc);
        assert_eq!(crc32c(b""), cook(0));
    }
}
