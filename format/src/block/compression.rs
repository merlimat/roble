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

//! Per-block compression (`internal/compression`: `Algorithm`, and
//! `sstable/block/compression.go`).
//!
//! The algorithm is recorded in the first byte of each block's trailer, so one
//! table mixes algorithms freely: Pebble stores a block uncompressed when
//! compression did not shrink it by `MinReductionPercent`, and an adaptive
//! profile falls back per block. A reader therefore has to branch per block
//! and never per table.

use crate::error::{Error, Result};

/// The algorithm named by a block's trailer byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Stored as-is. roble's blocks are these.
    None,
    /// Snappy, which is what Oxia configures for L1 and below.
    Snappy,
    /// Zstd, written by Oxia's conversion profile for data and value blocks.
    Zstd,
    /// MinLZ, written by that same profile for index and filter blocks on
    /// every architecture but arm64 (DESIGN.md §1).
    MinLz,
}

impl Compression {
    /// Decodes the block type byte.
    pub fn from_byte(b: u8) -> Result<Self> {
        Ok(match b {
            0 => Compression::None,
            1 => Compression::Snappy,
            2 => Compression::Zstd,
            3 => Compression::MinLz,
            other => return Error::corrupt(format!("block compression type {other}")),
        })
    }

    /// The name used in dumps.
    pub fn name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Snappy => "snappy",
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
pub(crate) fn decode<'a>(compression: Compression, data: &'a [u8]) -> Result<Decoded<'a>> {
    match compression {
        Compression::None => Ok(Decoded::Borrowed(data)),
        Compression::Snappy => {
            // Pebble uses the Snappy *block* format, so the uncompressed
            // length is a varint at the head of the block.
            let mut dec = snap::raw::Decoder::new();
            match dec.decompress_vec(data) {
                Ok(v) => Ok(Decoded::Owned(v)),
                Err(e) => Error::corrupt(format!("snappy block: {e}")),
            }
        }
        Compression::Zstd | Compression::MinLz => Error::unsupported(format!(
            "{} block decompression is not implemented yet",
            compression.name()
        )),
    }
}
