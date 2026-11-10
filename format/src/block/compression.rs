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

//! Per-block compression (`sstable/block/compression.go`: `CompressionIndicator`,
//! and `internal/compression`).
//!
//! The algorithm is recorded in the first byte of each block's trailer, so one
//! table mixes algorithms freely: Pebble stores a block uncompressed when
//! compression did not shrink it by `MinReductionPercent`, and an adaptive
//! profile falls back per block. A reader therefore branches per block and
//! never per table.
//!
//! **The indicator values are RocksDB's, not Pebble's algorithm enum.** They
//! are not consecutive and they are not the order the algorithms were added:
//! zstd is 7 and MinLZ is 8, while 2 to 6 are zlib, bzip2, lz4, lz4hc and
//! xpress, which Pebble never writes. Reading 2 as zstd — the obvious guess
//! from `internal/compression`'s own `Algorithm` enum, where `Zstd` *is* 2 —
//! gets every Zstd block wrong.

use crate::error::{Error, Result};
use crate::varint;

/// The algorithm named by a block's trailer byte
/// (`block.CompressionIndicator`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// 0, stored as-is. roble's own blocks are these.
    None,
    /// 1, Snappy: what Oxia configures for L1 and below, and what Pebble
    /// falls back to below Pebblev6 (see [`Compression::MinLz`]).
    Snappy,
    /// 2, zlib. In the format, never written by Pebble.
    Zlib,
    /// 3, bzip2. In the format, never written by Pebble.
    Bzip2,
    /// 4, lz4. In the format, never written by Pebble.
    Lz4,
    /// 5, lz4hc. In the format, never written by Pebble.
    Lz4hc,
    /// 6, xpress. In the format, never written by Pebble.
    Xpress,
    /// 7, Zstd. Written by Oxia's conversion profile for data and value
    /// blocks, so roble has to read it (DESIGN.md §1).
    Zstd,
    /// 8, MinLZ. Unreachable at roble's pin: Pebble replaces a whole
    /// compression profile with Snappy when the profile uses MinLZ and the
    /// table format is below Pebblev6
    /// (`sstable/options.go`: `tableFormatSupportsCompressionProfile`).
    MinLz,
}

impl Compression {
    /// Decodes the block type byte.
    pub fn from_byte(b: u8) -> Result<Self> {
        Ok(match b {
            0 => Compression::None,
            1 => Compression::Snappy,
            2 => Compression::Zlib,
            3 => Compression::Bzip2,
            4 => Compression::Lz4,
            5 => Compression::Lz4hc,
            6 => Compression::Xpress,
            7 => Compression::Zstd,
            8 => Compression::MinLz,
            other => return Error::corrupt(format!("block compression indicator {other}")),
        })
    }

    /// The byte that names this algorithm in a block trailer.
    pub fn to_byte(self) -> u8 {
        match self {
            Compression::None => 0,
            Compression::Snappy => 1,
            Compression::Zlib => 2,
            Compression::Bzip2 => 3,
            Compression::Lz4 => 4,
            Compression::Lz4hc => 5,
            Compression::Xpress => 6,
            Compression::Zstd => 7,
            Compression::MinLz => 8,
        }
    }

    /// The name used in dumps and errors.
    pub fn name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Snappy => "snappy",
            Compression::Zlib => "zlib",
            Compression::Bzip2 => "bzip2",
            Compression::Lz4 => "lz4",
            Compression::Lz4hc => "lz4hc",
            Compression::Xpress => "xpress",
            Compression::Zstd => "zstd",
            Compression::MinLz => "minlz",
        }
    }
}

/// A block's bytes, borrowed when they were stored uncompressed.
///
/// The borrowed case is the one that matters: an uncompressed cached block is
/// the bytes that came off the disk, with no allocation on the read path
/// (DESIGN.md §8).
#[derive(Debug)]
pub enum Decoded<'a> {
    /// Stored uncompressed: a subslice of the table.
    Borrowed(&'a [u8]),
    /// Decompressed into a fresh buffer.
    Owned(Vec<u8>),
}

impl Decoded<'_> {
    /// The block's bytes.
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Decoded::Borrowed(b) => b,
            Decoded::Owned(v) => v,
        }
    }
}

/// Decompresses a block according to its trailer byte.
pub(crate) fn decode(compression: Compression, data: &[u8]) -> Result<Decoded<'_>> {
    match compression {
        Compression::None => Ok(Decoded::Borrowed(data)),
        Compression::Snappy => {
            // Pebble uses the Snappy *block* format, whose uncompressed length
            // is a varint at the head of the payload.
            let mut dec = snap::raw::Decoder::new();
            match dec.decompress_vec(data) {
                Ok(v) => Ok(Decoded::Owned(v)),
                Err(e) => Error::corrupt(format!("snappy block: {e}")),
            }
        }
        Compression::Zstd => decode_zstd(data),
        Compression::MinLz => Error::unsupported(
            "a MinLZ block cannot occur at or below Pebblev4, because Pebble replaces a \
             MinLZ profile with Snappy there; this table was written by something else",
        ),
        other => Error::unsupported(format!(
            "{} blocks: no Pebble that could have written this table produces them",
            other.name()
        )),
    }
}

/// Decodes a Zstd block: `uvarint(uncompressed_len)` then a Zstd frame.
///
/// The length prefix is RocksDB's convention, which Pebble keeps
/// (`internal/compression/zstd_nocgo.go`: `Compress` writes it, `DecompressedLen`
/// reads it). A bare frame would decode with a stock Zstd reader and so this is
/// easy to get wrong in a way that only fails on real files.
fn decode_zstd(data: &[u8]) -> Result<Decoded<'static>> {
    let (len, prefix) = varint::uvarint(data)?;
    let len = usize::try_from(len)
        .map_err(|_| Error::Corrupt(format!("zstd block claims {len} bytes uncompressed")))?;
    // Pebble sizes the destination from the prefix and requires the frame to
    // fill it exactly, which is also the cheapest way to reject a lying prefix.
    let mut out = vec![0u8; len];
    let mut decoder = ruzstd::decoding::FrameDecoder::new();
    let written = decoder
        .decode_all(&data[prefix..], &mut out)
        .map_err(|e| Error::Corrupt(format!("zstd block: {e}")))?;
    if written != len {
        return Error::corrupt(format!(
            "zstd block claims {len} bytes uncompressed but produced {written}"
        ));
    }
    Ok(Decoded::Owned(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indicator_values_are_rocksdbs() {
        // The one that matters: zstd is 7, not the 2 that Pebble's own
        // Algorithm enum uses internally.
        assert_eq!(Compression::from_byte(7).unwrap(), Compression::Zstd);
        assert_eq!(Compression::from_byte(2).unwrap(), Compression::Zlib);
        assert_eq!(Compression::from_byte(8).unwrap(), Compression::MinLz);
        for b in 0u8..=8 {
            let c = Compression::from_byte(b).expect("every indicator decodes");
            assert_eq!(c.to_byte(), b, "round trip for {b}");
        }
        assert!(Compression::from_byte(9).is_err());
    }

    #[test]
    fn a_zstd_block_with_a_lying_length_prefix_is_refused() {
        // A prefix saying 1 MiB with no frame behind it must not allocate and
        // then claim success.
        let mut data = Vec::new();
        data.extend_from_slice(&[0x80, 0x80, 0x40]); // uvarint, ~1 MiB
        assert!(decode(Compression::Zstd, &data).is_err());
    }
}
