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

//! The row-block builder (`sstable/rowblk/rowblk_writer.go`: `Writer`,
//! `storeWithOptionalValuePrefix`, `Finish`).
//!
//! Three rules here are not free choices, because a Go reader depends on them:
//!
//! 1. **A shared prefix never reaches into the previous key's trailer.** Pebble
//!    caps it at the previous key's *user key* length (`maxSharedKeyLen`), so
//!    that any key sharing a prefix can be reassembled from the one before it
//!    without walking back to a restart point.
//! 2. **The high bit of a restart offset is `setHasSameKeyPrefixSinceLastRestart`**,
//!    not part of the offset.
//! 3. **A `SET` value in a Pebblev3+ data block carries a prefix byte.**
//!
//! Everything else — when to start a new block, how long a separator is — is
//! roble's choice, because it changes no reader's answer.

use crate::key::TRAILER_LEN;

/// `base.DefaultBlockRestartInterval`: a full key every 16 entries.
pub const DEFAULT_RESTART_INTERVAL: usize = 16;

/// `indexBlockRestartInterval`: index blocks restart at every entry.
pub const INDEX_RESTART_INTERVAL: usize = 1;

/// `setHasSameKeyPrefixRestartMask`.
const SET_HAS_SAME_KEY_PREFIX_MASK: u32 = 1 << 31;

/// Builds one row block.
#[derive(Debug)]
pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    restart_interval: usize,
    n_entries: usize,
    next_restart: usize,
    prev_key: Vec<u8>,
    same_prefix_since_last_restart: bool,
}

impl BlockBuilder {
    /// A builder that writes a full key every `restart_interval` entries.
    pub fn new(restart_interval: usize) -> Self {
        BlockBuilder {
            buf: Vec::new(),
            restarts: Vec::new(),
            restart_interval: restart_interval.max(1),
            n_entries: 0,
            next_restart: 0,
            prev_key: Vec::new(),
            same_prefix_since_last_restart: false,
        }
    }

    /// Whether anything has been added since the last [`BlockBuilder::finish`].
    pub fn is_empty(&self) -> bool {
        self.n_entries == 0
    }

    /// How many entries are in the block so far.
    pub fn len(&self) -> usize {
        self.n_entries
    }

    /// The size the block would have if finished now.
    pub fn estimated_size(&self) -> usize {
        self.buf.len() + self.restarts.len() * 4 + 4
    }

    /// Adds an entry whose key is stored as given.
    ///
    /// `max_shared` caps the shared prefix; `value_prefix` is the Pebblev3+
    /// `SET` prefix byte when one applies; `same_key_prefix` says whether this
    /// key's prefix equals the previous key's, which drives both that byte and
    /// the restart array's high bit.
    pub fn add(
        &mut self,
        key: &[u8],
        value: &[u8],
        max_shared: usize,
        value_prefix: Option<u8>,
        same_key_prefix: bool,
    ) {
        if !same_key_prefix {
            self.same_prefix_since_last_restart = false;
        }
        let shared = if self.n_entries == self.next_restart {
            self.next_restart = self.n_entries + self.restart_interval;
            let mut restart = self.buf.len() as u32;
            if self.same_prefix_since_last_restart {
                restart |= SET_HAS_SAME_KEY_PREFIX_MASK;
            }
            self.same_prefix_since_last_restart = true;
            self.restarts.push(restart);
            0
        } else {
            let cap = max_shared.min(self.prev_key.len()).min(key.len());
            let mut shared = 0;
            while shared < cap && key[shared] == self.prev_key[shared] {
                shared += 1;
            }
            shared
        };

        let value_len = value.len() + usize::from(value_prefix.is_some());
        put_uvarint32(&mut self.buf, shared as u32);
        put_uvarint32(&mut self.buf, (key.len() - shared) as u32);
        put_uvarint32(&mut self.buf, value_len as u32);
        self.buf.extend_from_slice(&key[shared..]);
        if let Some(p) = value_prefix {
            self.buf.push(p);
        }
        self.buf.extend_from_slice(value);

        self.prev_key.clear();
        self.prev_key.extend_from_slice(key);
        self.n_entries += 1;
    }

    /// Appends the restart array and the restart count, and returns the block.
    ///
    /// Every block has at least one restart point, even an empty one, which is
    /// what `rowblk.Finish` guarantees and what the reader relies on.
    pub fn finish(&mut self) -> &[u8] {
        if self.n_entries == 0 {
            self.restarts.clear();
            self.restarts.push(0);
        }
        for r in &self.restarts {
            self.buf.extend_from_slice(&r.to_le_bytes());
        }
        self.buf
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        &self.buf
    }

    /// Empties the builder for the next block, keeping its allocations.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.restarts.clear();
        self.n_entries = 0;
        self.next_restart = 0;
        self.prev_key.clear();
        self.same_prefix_since_last_restart = false;
    }
}

/// The cap Pebble puts on a data-block entry's shared prefix.
///
/// From Pebblev3 on it is the *previous* key's user-key length, so the shared
/// prefix stops at the previous key's trailer. Before that it is the current
/// key's user-key length. Either way a trailer is never shared.
pub fn max_shared_len(
    format: crate::TableFormat,
    prev_internal_key: &[u8],
    cur_internal_key: &[u8],
) -> usize {
    if format.has_value_prefix() {
        prev_internal_key.len().saturating_sub(TRAILER_LEN)
    } else {
        cur_internal_key.len().saturating_sub(TRAILER_LEN)
    }
}

fn put_uvarint32(buf: &mut Vec<u8>, mut x: u32) {
    while x >= 0x80 {
        buf.push((x as u8) | 0x80);
        x >>= 7;
    }
    buf.push(x as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;

    #[test]
    fn a_built_block_reads_back_entry_for_entry() {
        let mut b = BlockBuilder::new(2);
        let keys: &[&[u8]] = &[b"alpha", b"alphabet", b"alphabetical", b"beta", b"delta"];
        let mut prev: Vec<u8> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            let max_shared = prev.len();
            b.add(k, format!("v{i}").as_bytes(), max_shared, None, false);
            prev = k.to_vec();
        }
        let bytes = b.finish().to_vec();
        let block = Block::new(&bytes).expect("parse the block just built");
        let mut got = Vec::new();
        let mut it = block.iter();
        while let Some(e) = it.next() {
            let e = e.expect("entry");
            got.push((e.key.to_vec(), e.value.to_vec()));
        }
        assert_eq!(got.len(), keys.len());
        for (i, (k, v)) in got.iter().enumerate() {
            assert_eq!(k.as_slice(), keys[i], "key {i}");
            assert_eq!(v.as_slice(), format!("v{i}").as_bytes(), "value {i}");
        }
        // Restart interval 2 over 5 entries: entries 0, 2 and 4.
        assert_eq!(block.num_restarts(), 3);
    }

    #[test]
    fn the_same_prefix_bit_stays_out_of_the_restart_offset() {
        let mut b = BlockBuilder::new(1);
        // Two entries with the same user key, which is what sets the bit.
        let mut k1 = b"dup".to_vec();
        k1.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]);
        let mut k2 = b"dup".to_vec();
        k2.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]);
        b.add(&k1, b"a", 0, Some(0x20), true);
        b.add(&k2, b"b", 3, Some(0x20), true);
        let bytes = b.finish().to_vec();
        let block = Block::new(&bytes).expect("parse");
        // Both offsets must be small, not 2^31-ish, and must stay in range.
        for i in 0..block.num_restarts() {
            let off = block.restart_offset(i).expect("restart offset in range");
            assert!(off < bytes.len(), "restart {i} offset {off} out of range");
        }
    }
}
