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

//! The properties block (`sstable/properties.go`).
//!
//! A row block of name/value entries. Numbers are uvarints (`saveUvarint`)
//! except `rocksdb.block.based.table.index.type`, which is four bytes
//! little-endian (`saveUint32`) — a difference worth keeping in one place,
//! since reading one as the other silently yields a plausible number.
//!
//! roble reads the handful of properties it acts on and keeps the rest as raw
//! bytes, so an unknown collector's property is carried, not rejected
//! (DESIGN.md §3).

use std::collections::BTreeMap;

use crate::block::Block;
use crate::error::{Error, Result};
use crate::varint;

/// `rocksdb.block.based.table.index.type` when the index is two-level.
const TWO_LEVEL_INDEX: u32 = 2;

/// The decoded properties block.
#[derive(Debug, Default, Clone)]
pub struct Properties {
    raw: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Properties {
    /// Decodes a properties block.
    #[doc(hidden)]
    pub fn decode(block: &Block<'_>) -> Result<Self> {
        let mut raw = BTreeMap::new();
        let mut iter = block.iter();
        while let Some(entry) = iter.next() {
            let entry = entry?;
            raw.insert(entry.key.to_vec(), entry.value.to_vec());
        }
        Ok(Properties { raw })
    }

    /// A property's raw bytes.
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.raw.get(name.as_bytes()).map(Vec::as_slice)
    }

    /// A property stored as a uvarint.
    pub fn uvarint(&self, name: &str) -> Result<Option<u64>> {
        match self.get(name) {
            None => Ok(None),
            Some(bytes) => {
                let (value, n) = varint::uvarint(bytes)?;
                if n != bytes.len() {
                    return Error::corrupt(format!(
                        "property {name}: {} bytes for a uvarint of {n}",
                        bytes.len()
                    ));
                }
                Ok(Some(value))
            }
        }
    }

    /// A property stored as a string.
    pub fn string(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|b| std::str::from_utf8(b).ok())
    }

    /// `rocksdb.num.data.blocks`, which a reader can check its own walk against.
    pub fn num_data_blocks(&self) -> Result<Option<u64>> {
        self.uvarint("rocksdb.num.data.blocks")
    }

    /// `rocksdb.num.entries`.
    pub fn num_entries(&self) -> Result<Option<u64>> {
        self.uvarint("rocksdb.num.entries")
    }

    /// `rocksdb.comparator`, which must be the bytewise one (DESIGN.md §3).
    pub fn comparer_name(&self) -> Option<&str> {
        self.string("rocksdb.comparator")
    }

    /// `rocksdb.merge.operator`.
    pub fn merger_name(&self) -> Option<&str> {
        self.string("rocksdb.merge.operator")
    }

    /// Whether the index is two-level.
    pub fn is_two_level_index(&self) -> Result<bool> {
        match self.get("rocksdb.block.based.table.index.type") {
            None => Ok(false),
            Some(bytes) => {
                let bytes: [u8; 4] = bytes.try_into().map_err(|_| {
                    Error::Corrupt(format!(
                        "property rocksdb.block.based.table.index.type: {} bytes, want 4",
                        bytes.len()
                    ))
                })?;
                Ok(u32::from_le_bytes(bytes) == TWO_LEVEL_INDEX)
            }
        }
    }

    /// Every property, for a dump.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.raw.iter().map(|(k, v)| (k.as_slice(), v.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::{Block, BlockBuilder};

    /// A properties block, framed the way `TableWriter` frames one.
    ///
    /// Names must ascend: it is a row block, and the reader walks it in order.
    fn props(pairs: &[(&str, &[u8])]) -> Properties {
        let mut b = BlockBuilder::new(usize::MAX);
        let mut prev: Vec<u8> = Vec::new();
        for (name, value) in pairs {
            b.add(name.as_bytes(), value, prev.len(), None, false);
            prev = name.as_bytes().to_vec();
        }
        let bytes = b.finish().to_vec();
        Properties::decode(&Block::new(&bytes).expect("a block")).expect("properties")
    }

    /// A property that is not there is absent, not zero.
    ///
    /// Every one of these is optional, and the difference matters: a table
    /// with no `rocksdb.num.entries` has not told anyone how many entries it
    /// holds, which is not the same as holding none. Reading the first as the
    /// second is how a reader decides a table is empty and skips it.
    #[test]
    fn a_property_that_is_not_there_is_absent_rather_than_zero() {
        let p = props(&[("rocksdb.comparator", b"leveldb.BytewiseComparator")]);
        assert_eq!(p.uvarint("rocksdb.num.entries").expect("absent"), None);
        assert_eq!(p.num_entries().expect("absent"), None);
        assert_eq!(p.num_data_blocks().expect("absent"), None);
        assert_eq!(p.merger_name(), None);
        // And the index type, where absent means single-level rather than
        // unknown: RocksDB wrote the property only once two-level existed.
        assert!(!p.is_two_level_index().expect("absent"));
    }

    #[test]
    fn a_property_that_is_there_is_read() {
        let p = props(&[
            ("rocksdb.merge.operator", b"pebble.concatenate".as_slice()),
            ("rocksdb.num.entries", &[42]),
        ]);
        assert_eq!(p.num_entries().expect("present"), Some(42));
        assert_eq!(p.merger_name(), Some("pebble.concatenate"));
    }

    /// A uvarint property with anything after the number is refused.
    ///
    /// The trailing bytes are the tell: a property this reader has
    /// misunderstood looks exactly like one it has read correctly, right up
    /// to the bytes it did not consume.
    #[test]
    fn a_uvarint_property_with_trailing_bytes_is_refused() {
        let p = props(&[("rocksdb.num.entries", &[42, 0, 0])]);
        let err = p
            .uvarint("rocksdb.num.entries")
            .expect_err("trailing bytes");
        assert!(
            format!("{err}").contains("3 bytes for a uvarint of 1"),
            "got {err:?}"
        );
    }

    /// The index-type property is a four-byte number, and anything else is
    /// refused rather than guessed at.
    ///
    /// Guessing would decide whether the index has one level or two, which is
    /// the difference between reading a table's index and reading the first
    /// block of its second level as if it were the whole thing.
    #[test]
    fn an_index_type_that_is_not_four_bytes_is_refused() {
        let p = props(&[("rocksdb.block.based.table.index.type", &[0, 0])]);
        let err = p.is_two_level_index().expect_err("two bytes");
        assert!(format!("{err}").contains("2 bytes, want 4"), "got {err:?}");
    }
}
