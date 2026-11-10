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

//! The LEB128 varints Pebble's block handles and block entries use
//! (`encoding/binary`: `Uvarint`, `PutUvarint`).

use crate::error::{Error, Result};

/// Reads a uvarint, returning it and the bytes consumed.
pub(crate) fn uvarint(src: &[u8]) -> Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in src.iter().enumerate() {
        if shift >= 64 {
            return Error::corrupt("uvarint wider than 64 bits");
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            // Go's Uvarint rejects an overlong encoding of a value that fits.
            if i > 0 && byte == 0 {
                return Error::corrupt("uvarint has a redundant continuation byte");
            }
            return Ok((value, i + 1));
        }
        shift += 7;
    }
    Error::truncated("uvarint", src.len() + 1, src.len())
}

/// Reads a uvarint that must fit in a `u32`, as a block entry's three lengths do.
pub(crate) fn uvarint32(src: &[u8]) -> Result<(u32, usize)> {
    let (value, n) = uvarint(src)?;
    match u32::try_from(value) {
        Ok(v) => Ok((v, n)),
        Err(_) => Error::corrupt(format!("uvarint {value} does not fit in u32")),
    }
}
