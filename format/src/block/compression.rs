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
#[non_exhaustive]
pub enum Compression {
    /// 0, stored as-is: every block of L0, and any block compression did
    /// not shrink enough to keep.
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

/// What a table writer compresses its data and index blocks with.
///
/// Only what Go Pebble reads at roble's pin: none, Snappy and Zstd. Pebble's
/// own profiles for these are `NoCompression`, `SnappyCompression` and
/// `ZstdCompression` (`sstable/block/compression.go`), the last at level 3;
/// any level is accepted here, since the level is not recorded anywhere a
/// reader looks.
///
/// `#[non_exhaustive]`: MinLZ joins it when the pin reaches Pebblev6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Codec {
    /// Every block stored as built, which keeps a cached block the bytes
    /// that came off the disk (DESIGN.md §8).
    #[default]
    None,
    /// Snappy, Pebble's default and what Oxia configures for L1 and below.
    Snappy,
    /// Zstd at the given level.
    Zstd(i32),
}

impl Codec {
    /// The level Pebble's `ZstdCompression` profile uses.
    pub const ZSTD_DEFAULT_LEVEL: i32 = 3;

    /// The profile name Pebble records as `rocksdb.compression`.
    pub fn property_name(self) -> &'static str {
        match self {
            Codec::None => "NoCompression",
            Codec::Snappy => "Snappy",
            Codec::Zstd(_) => "ZSTD",
        }
    }
}

/// A block is stored uncompressed unless compressing it saved at least this
/// percentage: `simpleCompressionProfile`'s `MinReductionPercent`, which is
/// what each of the three profiles above uses.
const MIN_REDUCTION_PERCENT: usize = 12;

/// Compresses a writer's blocks, holding the buffers and the Zstd context
/// from one block to the next.
pub(crate) struct Compressor {
    codec: Codec,
    snappy: snap::raw::Encoder,
    zstd: Option<zstd::bulk::Compressor<'static>>,
    out: Vec<u8>,
}

impl std::fmt::Debug for Compressor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compressor")
            .field("codec", &self.codec)
            .finish_non_exhaustive()
    }
}

impl Compressor {
    pub(crate) fn new(codec: Codec) -> Result<Self> {
        let zstd = match codec {
            Codec::Zstd(level) => Some(
                zstd::bulk::Compressor::new(level)
                    .map_err(|e| Error::InvalidArgument(format!("zstd level {level}: {e}")))?,
            ),
            _ => None,
        };
        Ok(Compressor {
            codec,
            snappy: snap::raw::Encoder::new(),
            zstd,
            out: Vec::new(),
        })
    }

    /// The trailer's algorithm and the bytes to store for `block`: the
    /// block itself when the codec is none or did not save
    /// [`MIN_REDUCTION_PERCENT`] (`block.Compressor.Compress`).
    pub(crate) fn compress<'a>(&'a mut self, block: &'a [u8]) -> Result<(Compression, &'a [u8])> {
        let algorithm = match self.codec {
            Codec::None => return Ok((Compression::None, block)),
            Codec::Snappy => {
                self.out.clear();
                self.out.resize(snap::raw::max_compress_len(block.len()), 0);
                let n = self
                    .snappy
                    .compress(block, &mut self.out)
                    .map_err(|e| Error::InvalidArgument(format!("snappy compression: {e}")))?;
                self.out.truncate(n);
                Compression::Snappy
            }
            Codec::Zstd(_) => {
                // RocksDB's convention, which `decode_zstd` reads back:
                // the uncompressed length, then the frame.
                self.out.clear();
                put_uvarint(&mut self.out, block.len() as u64);
                let prefix = self.out.len();
                self.out
                    .resize(prefix + zstd::zstd_safe::compress_bound(block.len()), 0);
                let n = self
                    .zstd
                    .as_mut()
                    .expect("a Zstd codec has a context")
                    .compress_to_buffer(block, &mut self.out[prefix..])
                    .map_err(|e| Error::InvalidArgument(format!("zstd compression: {e}")))?;
                self.out.truncate(prefix + n);
                Compression::Zstd
            }
        };
        if self.out.len() * 100 > block.len() * (100 - MIN_REDUCTION_PERCENT) {
            return Ok((Compression::None, block));
        }
        Ok((algorithm, &self.out))
    }
}

fn put_uvarint(buf: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        buf.push((x as u8) | 0x80);
        x >>= 7;
    }
    buf.push(x as u8);
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
            // is a varint at the head of the payload. `decompress_vec` sizes
            // its buffer from that varint, up to 4 GiB, so it is held to what
            // the block could produce first, as a Zstd block's is.
            let len = snap::raw::decompress_len(data)
                .map_err(|e| Error::Corrupt(format!("snappy block: {e}")))?;
            if len > data.len().saturating_mul(SNAPPY_MAX_EXPANSION) {
                return Error::corrupt(format!(
                    "snappy block claims {len} bytes uncompressed from {} bytes",
                    data.len()
                ));
            }
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

/// The most a byte of Snappy block can decode to, rounded up.
///
/// The densest element is a copy with a two-byte offset: three bytes that
/// repeat up to 64 (the Snappy format description, "Copy with 2-byte
/// offset"). Literals decode to fewer bytes than they take, and the other
/// copies to fewer per byte.
const SNAPPY_MAX_EXPANSION: usize = 22;

/// The most a byte of Zstd frame can decode to.
///
/// The densest a frame gets is a run of RLE blocks: a three-byte block header
/// and one byte of content, repeated up to the 128 KiB a block may hold
/// (RFC 8878 §3.1.1.2). That is 32 KiB of output for each byte of input, and
/// every other block type, and the frame header, decode to less per byte.
const ZSTD_MAX_EXPANSION: usize = 128 * 1024 / 4;

/// Decodes a Zstd block: `uvarint(uncompressed_len)` then a Zstd frame.
///
/// The length prefix is RocksDB's convention, which Pebble keeps
/// (`internal/compression/zstd_nocgo.go`: `Compress` writes it, `DecompressedLen`
/// reads it). A bare frame would decode with a stock Zstd reader and so this is
/// easy to get wrong in a way that only fails on real files.
fn decode_zstd(data: &[u8]) -> Result<Decoded<'static>> {
    let (len, prefix) = varint::uvarint(data)?;
    let frame = &data[prefix..];
    // The length sizes the allocation below, and it is read from the table, so
    // it is held to what the frame could produce before anything is
    // allocated: one past what memory holds aborts the process.
    let len = usize::try_from(len)
        .ok()
        .filter(|&len| len <= frame.len().saturating_mul(ZSTD_MAX_EXPANSION))
        .ok_or_else(|| {
            Error::Corrupt(format!(
                "zstd block claims {len} bytes uncompressed from a {}-byte frame",
                frame.len()
            ))
        })?;
    // Pebble sizes the destination from the prefix and requires the frame to
    // fill it exactly, which is also the cheapest way to reject a lying prefix.
    let mut out = vec![0u8; len];
    // One decompression context per thread, kept: libzstd allocates one per
    // call otherwise, which is a second allocation on every miss.
    thread_local! {
        static DECOMPRESSOR: std::cell::RefCell<Option<zstd::bulk::Decompressor<'static>>> =
            const { std::cell::RefCell::new(None) };
    }
    let written = DECOMPRESSOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(
                zstd::bulk::Decompressor::new()
                    .map_err(|e| Error::Corrupt(format!("zstd context: {e}")))?,
            );
        }
        let decompressor = slot.as_mut().expect("just made");
        decompressor
            .decompress_to_buffer(frame, &mut out[..])
            .map_err(|e| Error::Corrupt(format!("zstd block: {e}")))
    })?;
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

    /// A Snappy block's header says how long it is uncompressed, and can
    /// claim up to 4 GiB. A buffer that size is not something a few bytes of
    /// block may ask for, so the claim is refused before it is allocated.
    #[test]
    fn a_snappy_length_no_block_could_produce_is_refused() {
        // 0x80 0x80 0x80 0x80 0x08 is 2 GiB, then four bytes of literal.
        let data = [0x80, 0x80, 0x80, 0x80, 0x08, 0x0c, b'a', b'b', b'c', b'd'];
        let err = decode(Compression::Snappy, &data).expect_err("an impossible length");
        assert!(
            format!("{err}").contains("snappy block claims"),
            "got {err:?}"
        );
    }

    /// A Zstd block says how long it is uncompressed, and that length is read
    /// from the table. A lie that large must be refused before it sizes an
    /// allocation: one past what memory can hold aborts the process, which no
    /// caller can recover from.
    #[test]
    fn a_zstd_length_no_frame_could_produce_is_refused() {
        for claim in [1u64 << 62, 1 << 40, 1 << 30] {
            let mut data = Vec::new();
            let mut v = claim;
            while v >= 0x80 {
                data.push(v as u8 | 0x80);
                v >>= 7;
            }
            data.push(v as u8);
            // Thirty-two bytes of frame cannot expand to a gigabyte.
            data.extend_from_slice(&[0u8; 32]);
            let err = decode(Compression::Zstd, &data).expect_err("an impossible length");
            assert!(
                format!("{err}").contains("zstd block claims"),
                "{claim}: got {err:?}"
            );
        }
    }

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

    /// Every algorithm's name, which nothing had asked for.
    ///
    /// `indicator_values_are_rocksdbs` above already round-trips every byte,
    /// so the bytes are not repeated here. What it never does is call `name`,
    /// and a name is what the refusals below put in front of whoever has to
    /// act on them.
    #[test]
    fn every_algorithm_says_what_it_is() {
        let names: &[(Compression, &str)] = &[
            (Compression::None, "none"),
            (Compression::Snappy, "snappy"),
            (Compression::Zlib, "zlib"),
            (Compression::Bzip2, "bzip2"),
            (Compression::Lz4, "lz4"),
            (Compression::Lz4hc, "lz4hc"),
            (Compression::Xpress, "xpress"),
            (Compression::Zstd, "zstd"),
            (Compression::MinLz, "minlz"),
        ];
        for &(c, name) in names {
            assert_eq!(c.name(), name, "{c:?}");
        }
        assert_eq!(names.len(), 9, "an algorithm was added without a row here");
    }

    /// The algorithms roble declines, each by name.
    ///
    /// MinLZ gets its own wording because it is the one that could plausibly
    /// be reached by mistake: Pebble's "good" compression profile is MinLZ on
    /// amd64, and the thing worth saying is that Pebble *replaces* it with
    /// Snappy at or below Pebblev4 — so a MinLZ block here was not written by
    /// a Pebble at all.
    #[test]
    fn an_algorithm_roble_does_not_decode_is_refused_by_name() {
        let err = decode(Compression::MinLz, b"whatever").expect_err("minlz");
        let text = format!("{err}");
        assert!(text.contains("MinLZ") && text.contains("written by something else"));

        for c in [
            Compression::Zlib,
            Compression::Bzip2,
            Compression::Lz4,
            Compression::Lz4hc,
            Compression::Xpress,
        ] {
            let err = decode(c, b"whatever").expect_err("not decoded");
            let text = format!("{err}");
            assert!(
                text.contains(c.name()) && text.contains("no Pebble"),
                "{c:?}: got {text:?}"
            );
        }
    }

    /// What each codec writes, `decode` reads back as it was.
    #[test]
    fn every_codec_round_trips() {
        let block: Vec<u8> = (0..4000u32)
            .flat_map(|i| format!("/oxia/default/users/{:08} ", i % 97).into_bytes())
            .collect();
        for (codec, algorithm) in [
            (Codec::Snappy, Compression::Snappy),
            (Codec::Zstd(Codec::ZSTD_DEFAULT_LEVEL), Compression::Zstd),
            (Codec::Zstd(1), Compression::Zstd),
            (Codec::Zstd(19), Compression::Zstd),
        ] {
            let mut c = Compressor::new(codec).expect("compressor");
            let (got, stored) = c.compress(&block).expect("compress");
            assert_eq!(got, algorithm, "{codec:?}");
            assert!(
                stored.len() < block.len() / 2,
                "{codec:?}: {}",
                stored.len()
            );
            let stored = stored.to_vec();
            let decoded = decode(got, &stored).expect("decode");
            assert_eq!(decoded.as_slice(), &block[..], "{codec:?}");
            // And again on the same compressor, whose buffer is reused.
            let (_, again) = c.compress(&block[..1000]).expect("compress");
            let again = again.to_vec();
            assert_eq!(
                decode(got, &again).expect("decode").as_slice(),
                &block[..1000]
            );
        }
    }

    /// A block compression does not shrink by 12% is stored as built, with
    /// the trailer saying so, which is what Pebble's `Compressor.Compress`
    /// does — and an uncompressed block is the one a reader need not copy.
    #[test]
    fn a_block_that_does_not_compress_is_stored_as_built() {
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let noise: Vec<u8> = (0..4096)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        for codec in [Codec::None, Codec::Snappy, Codec::Zstd(3)] {
            let mut c = Compressor::new(codec).expect("compressor");
            let (got, stored) = c.compress(&noise).expect("compress");
            assert_eq!(got, Compression::None, "{codec:?}");
            assert_eq!(stored, &noise[..], "{codec:?}");
        }
    }

    #[test]
    fn a_snappy_block_that_is_not_snappy_is_refused() {
        // The decoder's own complaint is carried through rather than replaced:
        // whoever is looking at a corrupt block wants to know what the
        // decompressor made of it.
        let err = decode(Compression::Snappy, b"not a snappy block").expect_err("not snappy");
        assert!(format!("{err}").contains("snappy block:"), "got {err:?}");
    }
}
