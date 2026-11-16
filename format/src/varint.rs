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
///
/// One and two bytes are read before the loop, inline. A block entry's three
/// lengths are one or two bytes each for keys and values the size Oxia
/// writes, and they are decoded for every entry a seek or a scan passes: the
/// call alone was 5% of a point lookup over a store in cache.
#[inline]
pub(crate) fn uvarint(src: &[u8]) -> Result<(u64, usize)> {
    match *src {
        [b0, ..] if b0 < 0x80 => Ok((u64::from(b0), 1)),
        // A zero second byte is an overlong encoding, which the loop refuses
        // as Go does, so it is left to the loop to say so.
        [b0, b1, ..] if b1 < 0x80 && b1 != 0 => {
            Ok((u64::from(b0 & 0x7f) | (u64::from(b1) << 7), 2))
        }
        _ => uvarint_long(src),
    }
}

/// [`uvarint`] for three bytes and more, and for every refusal.
fn uvarint_long(src: &[u8]) -> Result<(u64, usize)> {
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
#[inline]
pub(crate) fn uvarint32(src: &[u8]) -> Result<(u32, usize)> {
    let (value, n) = uvarint(src)?;
    match u32::try_from(value) {
        Ok(v) => Ok((v, n)),
        Err(_) => Error::corrupt(format!("uvarint {value} does not fit in u32")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's `PutUvarint`, for the tests to encode with.
    fn put(mut x: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while x >= 0x80 {
            out.push((x as u8) | 0x80);
            x >>= 7;
        }
        out.push(x as u8);
        out
    }

    #[test]
    fn every_width_reads_back_what_was_written() {
        // Either side of each width, the one- and two-byte fast paths and
        // the loop past them alike, with a byte after it that is not read.
        for x in [
            0,
            1,
            0x7f,
            0x80,
            129,
            // An even second byte, which is the one that shows the first
            // byte's continuation bit was not masked off.
            0x100,
            0x3fff,
            0x4000,
            0x1f_ffff,
            0x20_0000,
            u64::from(u32::MAX),
            u64::MAX,
        ] {
            let mut bytes = put(x);
            let n = bytes.len();
            bytes.push(0xff);
            assert_eq!(uvarint(&bytes).expect("decodes"), (x, n), "{x}");
        }
    }

    #[test]
    fn an_overlong_encoding_is_refused_at_every_width() {
        // A zero last byte encodes nothing the bytes before it did not, so Go
        // refuses it; the two-byte fast path must not let one through.
        for bytes in [&[0x80, 0x00][..], &[0x81, 0x00], &[0x80, 0x80, 0x00]] {
            assert!(uvarint(bytes).is_err(), "{bytes:?}");
        }
    }

    #[test]
    fn a_truncated_or_empty_uvarint_is_refused() {
        for bytes in [&[][..], &[0x80], &[0xff, 0xff]] {
            assert!(uvarint(bytes).is_err(), "{bytes:?}");
        }
    }

    #[test]
    fn a_u32_length_past_u32_is_refused() {
        assert_eq!(uvarint32(&put(129)).expect("fits"), (129, 2));
        assert!(uvarint32(&put(u64::from(u32::MAX) + 1)).is_err());
    }
}
