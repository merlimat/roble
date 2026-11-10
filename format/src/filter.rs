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

//! The `rocksdb.BuiltinBloomFilter` block (`bloom/bloom.go`), which Oxia
//! configures at 10 bits per key.
//!
//! RocksDB's full-file filter: cache-line-sized blocks of bits, then a
//! five-byte tail of one probe count and a four-byte little-endian line count.
//!
//! ```text
//!   [0 .. nLines*64)      the bits, one 64-byte line per block
//!   [nLines*64]           nProbes
//!   [nLines*64+1 .. +5)   nLines, u32 little-endian
//! ```
//!
//! Two details decide whether roble's filter agrees with Go's, and both fail
//! silently as a "not found" for a key that is really there:
//!
//! 1. **The hash sign-extends its trailing bytes.** The last one to three
//!    bytes are folded in as `uint32(int8(b))`, not `uint32(b)`, to match
//!    RocksDB. Every key whose tail byte is `0x80` or above hashes differently
//!    if you use the unsigned value — and Oxia's keys are arbitrary bytes.
//! 2. **The bytes hashed are `key[:Comparer.Split(key)]`**, not the whole
//!    internal key and not necessarily the whole user key
//!    (`rowblk_writer.go`: `maybeAddToFilter`). Under the bytewise comparer
//!    `Split` is the full user key, which is one more reason roble refuses any
//!    other comparer (DESIGN.md §3): `oxia-slash-spans` sets `Split` to return
//!    0, and a filter built over empty prefixes excludes nothing while a
//!    filter built over whole keys would exclude almost everything.

use crate::error::{Error, Result};

/// `FilterPolicy.Name()`, written into `rocksdb.filter.policy`.
pub const POLICY_NAME: &str = "rocksdb.BuiltinBloomFilter";

/// The metaindex entry the filter block is stored under.
pub const META_NAME: &str = "fullfilter.rocksdb.BuiltinBloomFilter";

/// Oxia's `bloom.FilterPolicy(10)`, and so roble's default (DESIGN.md §1).
pub const DEFAULT_BITS_PER_KEY: usize = 10;

const CACHE_LINE_SIZE: usize = 64;
const CACHE_LINE_BITS: u32 = (CACHE_LINE_SIZE * 8) as u32;

/// The LevelDB/RocksDB murmur-like hash the filter is built on.
///
/// The tail is sign-extended, which is the whole reason this is written out
/// rather than taken from a crate.
pub fn hash(b: &[u8]) -> u32 {
    const SEED: u32 = 0xbc9f_1d34;
    const M: u32 = 0xc6a4_a793;

    let mut h = SEED ^ (b.len() as u32).wrapping_mul(M);
    let mut rest = b;
    while rest.len() >= 4 {
        let (word, tail) = rest.split_at(4);
        h = h.wrapping_add(u32::from_le_bytes(word.try_into().expect("4 bytes")));
        h = h.wrapping_mul(M);
        h ^= h >> 16;
        rest = tail;
    }

    // `as i8 as u32` sign-extends, which `as u32` alone would not.
    if rest.len() == 3 {
        h = h.wrapping_add((rest[2] as i8 as u32) << 16);
    }
    if rest.len() >= 2 {
        h = h.wrapping_add((rest[1] as i8 as u32) << 8);
    }
    if !rest.is_empty() {
        h = h.wrapping_add(rest[0] as i8 as u32);
        h = h.wrapping_mul(M);
        h ^= h >> 24;
    }
    h
}

/// `calculateProbes`: `bitsPerKey * ln(2)`, rounded down, clamped to 1..=30.
pub fn probes_for(bits_per_key: usize) -> u8 {
    let n = (bits_per_key as f64 * 0.69) as u32;
    n.clamp(1, 30) as u8
}

/// A filter block, validated so that probing it cannot go out of bounds.
#[derive(Debug, Clone, Copy)]
pub struct Filter<'a> {
    bits: &'a [u8],
    probes: u8,
    lines: u32,
    line_bits: u32,
}

impl<'a> Filter<'a> {
    /// Validates a filter block.
    ///
    /// Everything probing needs is checked once here, so [`Filter::may_contain`]
    /// has no failure case and cannot index out of range: with
    /// `line_bits = 8 * (n / lines)` truncating, `lines * line_bits <= 8 * n`,
    /// so the highest bit a probe can reach is the last bit of the block.
    pub fn new(block: &'a [u8]) -> Result<Self> {
        if block.len() <= 5 {
            // Pebble's MayContain answers "no" to everything for a filter this
            // short, which is correct only for a filter over no keys at all.
            // Treating it as a filter would silently exclude every key in the
            // table, so roble refuses it instead.
            return Error::corrupt(format!(
                "filter block of {} bytes holds no lines",
                block.len()
            ));
        }
        let n = block.len() - 5;
        let probes = block[n];
        let lines = u32::from_le_bytes(block[n + 1..n + 5].try_into().expect("4 bytes"));
        if lines == 0 {
            return Error::corrupt("filter block claims zero lines but has bits");
        }
        let line_bits = 8 * (n as u32 / lines);
        if line_bits == 0 {
            return Error::corrupt(format!(
                "filter block of {n} bytes over {lines} lines leaves no bits per line"
            ));
        }
        if probes == 0 {
            return Error::corrupt("filter block claims zero probes");
        }
        Ok(Filter {
            bits: &block[..n],
            probes,
            lines,
            line_bits,
        })
    }

    /// Whether the key may be present. False is definitive; true may be wrong.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let mut h = hash(key);
        // Go writes `h>>17 | h<<15` and calls it a rotate right 17, which it is.
        let delta = h.rotate_right(17);
        let base = (h % self.lines) * self.line_bits;
        for _ in 0..self.probes {
            let bit = base + (h % self.line_bits);
            let byte = self.bits[(bit / 8) as usize];
            if byte & (1 << (bit % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    /// The number of probes this filter was built with.
    pub fn probes(&self) -> u8 {
        self.probes
    }

    /// The number of cache lines of bits.
    pub fn lines(&self) -> u32 {
        self.lines
    }
}

/// Builds a filter block.
#[derive(Debug)]
pub struct FilterBuilder {
    bits_per_key: usize,
    hashes: Vec<u32>,
    last: Option<u32>,
}

impl FilterBuilder {
    /// A builder at the given bits per key.
    pub fn new(bits_per_key: usize) -> Self {
        FilterBuilder {
            bits_per_key: bits_per_key.max(1),
            hashes: Vec::new(),
            last: None,
        }
    }

    /// Whether any key has been added.
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Adds a key's prefix, which under the bytewise comparer is the whole key.
    ///
    /// Consecutive duplicates are dropped, as `AddKey` drops them: a repeated
    /// user key at several sequence numbers contributes one hash, so rebuilding
    /// a filter by walking a table's entries in order reproduces Go's bits
    /// exactly.
    pub fn add_key(&mut self, key: &[u8]) {
        let h = hash(key);
        if self.last == Some(h) {
            return;
        }
        self.hashes.push(h);
        self.last = Some(h);
    }

    /// Encodes the filter block.
    pub fn finish(&self) -> Vec<u8> {
        let lines = if self.hashes.is_empty() {
            0usize
        } else {
            let mut l = (self.hashes.len() * self.bits_per_key).div_ceil(CACHE_LINE_BITS as usize);
            // Pebble forces an odd line count so that more of the hash decides
            // which line a key lands in.
            if l.is_multiple_of(2) {
                l += 1;
            }
            l
        };
        let n_bytes = lines * CACHE_LINE_SIZE;
        let mut buf = vec![0u8; n_bytes + 5];
        if lines == 0 {
            return buf;
        }
        let probes = probes_for(self.bits_per_key);
        let lines_u32 = lines as u32;
        for &h0 in &self.hashes {
            let mut h = h0;
            let delta = h.rotate_right(17);
            let base = (h % lines_u32) * CACHE_LINE_BITS;
            for _ in 0..probes {
                let bit = base + (h % CACHE_LINE_BITS);
                buf[(bit / 8) as usize] |= 1 << (bit % 8);
                h = h.wrapping_add(delta);
            }
        }
        buf[n_bytes] = probes;
        buf[n_bytes + 1..n_bytes + 5].copy_from_slice(&lines_u32.to_le_bytes());
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_counts_match_pebbles() {
        assert_eq!(probes_for(10), 6, "Oxia's 10 bits per key");
        assert_eq!(probes_for(1), 1);
        assert_eq!(probes_for(100), 30, "clamped");
    }

    #[test]
    fn a_filter_answers_yes_to_every_key_it_was_built_from() {
        let keys: Vec<Vec<u8>> = (0..500u32)
            .map(|i| format!("/oxia/default/shard-0001/users/{i:08}").into_bytes())
            .collect();
        let mut b = FilterBuilder::new(DEFAULT_BITS_PER_KEY);
        for k in &keys {
            b.add_key(k);
        }
        let block = b.finish();
        let f = Filter::new(&block).expect("valid filter");
        assert_eq!(f.probes(), 6);
        for k in &keys {
            assert!(
                f.may_contain(k),
                "{:?} must be present",
                String::from_utf8_lossy(k)
            );
        }

        // A filter is allowed false positives but not at any rate: 10 bits per
        // key is about 1%, so 5% over 10000 absent keys means something is
        // wrong with the hash rather than with luck.
        let mut false_positives = 0;
        let absent = 10_000u32;
        for i in 0..absent {
            let k = format!("/absent/{i:08}").into_bytes();
            if f.may_contain(&k) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives * 20 < absent,
            "{false_positives} false positives in {absent} is too many"
        );
    }

    #[test]
    fn high_bytes_in_the_tail_are_sign_extended() {
        // If the tail were folded in unsigned, these would hash the same as
        // their low-bit counterparts for some inputs. The point of the test is
        // that the hash is sensitive to the high bit at every tail length.
        assert_ne!(hash(&[0x7f]), hash(&[0xff]));
        assert_ne!(hash(&[0x01, 0x7f]), hash(&[0x01, 0xff]));
        assert_ne!(hash(&[0x01, 0x02, 0x7f]), hash(&[0x01, 0x02, 0xff]));
        // A key whose every tail byte is high must still round-trip.
        let mut b = FilterBuilder::new(DEFAULT_BITS_PER_KEY);
        let key = vec![0xff, 0xfe, 0xfd, 0xfc, 0x80];
        b.add_key(&key);
        let block = b.finish();
        assert!(Filter::new(&block).expect("valid").may_contain(&key));
    }

    #[test]
    fn consecutive_duplicates_contribute_one_hash() {
        let mut b = FilterBuilder::new(DEFAULT_BITS_PER_KEY);
        for _ in 0..10 {
            b.add_key(b"/same");
        }
        let one = b.finish();
        let mut b2 = FilterBuilder::new(DEFAULT_BITS_PER_KEY);
        b2.add_key(b"/same");
        assert_eq!(one, b2.finish());
    }

    #[test]
    fn a_degenerate_filter_block_is_refused_rather_than_excluding_everything() {
        assert!(Filter::new(&[]).is_err());
        assert!(Filter::new(&[0, 0, 0, 0, 0]).is_err());
        // Bits present but a zero line count.
        let mut block = vec![0u8; 64 + 5];
        block[64] = 6;
        assert!(Filter::new(&block).is_err());
    }
}
