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

//! Pebble's batch representation (`batchrepr`).
//!
//! ```text
//!   [0 .. 8)     sequence number, u64 little-endian
//!   [8 .. 12)    count of entries, u32 little-endian
//!   entry*       kind byte, then the key, then the value for kinds that
//!                carry one — each length-prefixed with a uvarint
//! ```
//!
//! **These bytes are a replication contract, not an internal encoding.** Oxia
//! checksums the whole repr, header included, and compares that checksum
//! across replicas (`oxiad/dataserver/database/db.go`). A Go batch and a roble
//! batch of the same operations that differ by one byte produce different
//! checksums, and the disagreement surfaces as a replica mismatch rather than
//! as a format error — so roble uses Pebble's encoding rather than one of its
//! own (DESIGN.md §6).
//!
//! The checksum is taken *before* commit, so what it covers is an uncommitted
//! repr whose sequence-number field is still zero. That is measured, not
//! assumed: Go Pebble's repr for an empty batch is twelve zero bytes
//! (`format/tests/golden/batches.txt`).
//!
//! Which kinds carry a value is not guessable from the kind's meaning:
//! `SETWITHDEL` does **not**, while `DELSIZED` does (`batchrepr.Reader.Next`).

use crate::error::{Error, Result};
use crate::key::Kind;
use crate::varint;

/// `batchrepr.HeaderLen`.
pub const HEADER_LEN: usize = 12;

const COUNT_OFFSET: usize = 8;

/// Whether a kind's entry is followed by a length-prefixed value.
///
/// Mirrors the switch in `batchrepr.Reader.Next`. `SETWITHDEL` is absent from
/// it, which is easy to miss because the kind plainly involves a value.
pub fn kind_has_value(kind: Kind) -> bool {
    matches!(
        kind,
        Kind::Set
            | Kind::Merge
            | Kind::RangeDelete
            | Kind::RangeKeySet
            | Kind::RangeKeyUnset
            | Kind::RangeKeyDelete
            | Kind::DeleteSized
            | Kind::Excise
    )
}

/// Builds a batch in Pebble's representation.
#[derive(Debug, Clone)]
pub struct Batch {
    repr: Vec<u8>,
}

impl Default for Batch {
    fn default() -> Self {
        Batch::new()
    }
}

impl Batch {
    /// An empty batch: a zeroed header and nothing else.
    pub fn new() -> Self {
        Batch {
            repr: vec![0u8; HEADER_LEN],
        }
    }

    /// Empties the batch without releasing its buffer, as `clear()` does in
    /// Oxia's write path (DESIGN.md §6).
    pub fn clear(&mut self) {
        self.repr.truncate(HEADER_LEN);
        self.repr[..HEADER_LEN].fill(0);
    }

    /// The whole representation, header included: the bytes Oxia checksums.
    pub fn as_bytes(&self) -> &[u8] {
        &self.repr
    }

    /// The number of entries, from the header.
    pub fn count(&self) -> u32 {
        u32::from_le_bytes(
            self.repr[COUNT_OFFSET..HEADER_LEN]
                .try_into()
                .expect("4 bytes"),
        )
    }

    /// The sequence number in the header, zero until commit assigns one.
    pub fn seq_num(&self) -> u64 {
        u64::from_le_bytes(self.repr[..COUNT_OFFSET].try_into().expect("8 bytes"))
    }

    /// Stamps the sequence number, as `batchrepr.SetSeqNum` does at commit.
    pub fn set_seq_num(&mut self, seq_num: u64) {
        self.repr[..COUNT_OFFSET].copy_from_slice(&seq_num.to_le_bytes());
    }

    /// The size of the representation, which is never below the header.
    pub fn len(&self) -> usize {
        self.repr.len()
    }

    /// Whether the batch holds no entries.
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// `Put`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.entry(Kind::Set, key, Some(value))
    }

    /// `Delete`.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.entry(Kind::Delete, key, None)
    }

    /// `SingleDelete`.
    pub fn single_delete(&mut self, key: &[u8]) -> Result<()> {
        self.entry(Kind::SingleDelete, key, None)
    }

    /// `DeleteRange`: the value is the exclusive end key.
    pub fn delete_range(&mut self, start: &[u8], end: &[u8]) -> Result<()> {
        if start >= end {
            return Error::corrupt(format!(
                "delete_range start {:?} is not below its end {:?}",
                String::from_utf8_lossy(start),
                String::from_utf8_lossy(end)
            ));
        }
        self.entry(Kind::RangeDelete, start, Some(end))
    }

    /// `DeleteSized`: the value is the deleted value's size as a uvarint.
    pub fn delete_sized(&mut self, key: &[u8], deleted_value_size: u32) -> Result<()> {
        let mut v = Vec::new();
        put_uvarint(&mut v, u64::from(deleted_value_size));
        self.entry(Kind::DeleteSized, key, Some(&v))
    }

    /// Reserves room for a value and lets the caller write it in place.
    ///
    /// This is what Oxia's `PutMarshalable` needs: marshal the protobuf once,
    /// into the batch, instead of into a temporary that is then copied
    /// (DESIGN.md §11).
    pub fn put_with<F>(&mut self, key: &[u8], value_len: usize, fill: F) -> Result<()>
    where
        F: FnOnce(&mut [u8]),
    {
        self.bump_count()?;
        self.repr.push(Kind::Set.to_byte());
        put_uvarint(&mut self.repr, key.len() as u64);
        self.repr.extend_from_slice(key);
        put_uvarint(&mut self.repr, value_len as u64);
        let at = self.repr.len();
        self.repr.resize(at + value_len, 0);
        fill(&mut self.repr[at..]);
        Ok(())
    }

    fn entry(&mut self, kind: Kind, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        debug_assert_eq!(
            value.is_some(),
            kind_has_value(kind),
            "{} must {} a value",
            kind.name(),
            if kind_has_value(kind) {
                "carry"
            } else {
                "omit"
            }
        );
        self.bump_count()?;
        self.repr.push(kind.to_byte());
        put_uvarint(&mut self.repr, key.len() as u64);
        self.repr.extend_from_slice(key);
        if let Some(v) = value {
            put_uvarint(&mut self.repr, v.len() as u64);
            self.repr.extend_from_slice(v);
        }
        Ok(())
    }

    fn bump_count(&mut self) -> Result<()> {
        let next = match self.count().checked_add(1) {
            Some(n) => n,
            None => return Error::corrupt("batch would exceed 2^32 - 1 entries"),
        };
        self.repr[COUNT_OFFSET..HEADER_LEN].copy_from_slice(&next.to_le_bytes());
        Ok(())
    }

    /// Walks the entries of a representation.
    pub fn read(repr: &[u8]) -> Result<Reader<'_>> {
        if repr.len() < HEADER_LEN {
            return Error::truncated("batch header", HEADER_LEN, repr.len());
        }
        Ok(Reader {
            rest: &repr[HEADER_LEN..],
            remaining: u32::from_le_bytes(
                repr[COUNT_OFFSET..HEADER_LEN].try_into().expect("4 bytes"),
            ),
        })
    }
}

/// One entry of a batch representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry<'a> {
    /// What the entry does.
    pub kind: Kind,
    /// The user key.
    pub key: &'a [u8],
    /// The value, for kinds that carry one.
    pub value: Option<&'a [u8]>,
}

/// The sequence number a representation was committed at.
///
/// For a batch read back out of a log: replay has to use the number the batch
/// was written with, not a fresh one, or a key would come back with a
/// different age than it had.
pub fn seq_num_of(repr: &[u8]) -> Result<u64> {
    if repr.len() < HEADER_LEN {
        return Error::truncated("batch header", HEADER_LEN, repr.len());
    }
    Ok(u64::from_le_bytes(
        repr[..COUNT_OFFSET].try_into().expect("8 bytes"),
    ))
}

/// Walks a batch representation's entries.
#[derive(Debug)]
pub struct Reader<'a> {
    rest: &'a [u8],
    remaining: u32,
}

impl<'a> Reader<'a> {
    /// How many entries the header said there were, less those already read.
    pub fn remaining(&self) -> u32 {
        self.remaining
    }

    fn next_inner(&mut self) -> Result<Entry<'a>> {
        let kind = Kind::from_byte(self.rest[0])?;
        let mut at = 1;
        let (key, n) = read_str(&self.rest[at..])?;
        at += n;
        let value = if kind_has_value(kind) {
            let (v, n) = read_str(&self.rest[at..])?;
            at += n;
            Some(v)
        } else {
            None
        };
        self.rest = &self.rest[at..];
        self.remaining = self.remaining.saturating_sub(1);
        Ok(Entry { kind, key, value })
    }
}

/// The entries, in the order they were added.
///
/// Fused: after an error it yields nothing more, as at the end. An entry
/// that failed to decode has no length to step over, so whatever follows it
/// cannot be found.
impl<'a> Iterator for Reader<'a> {
    type Item = Result<Entry<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            if self.remaining != 0 {
                let missing = self.remaining;
                self.remaining = 0;
                return Some(Error::corrupt(format!(
                    "batch header promised {missing} more entries than it holds"
                )));
            }
            return None;
        }
        let entry = self.next_inner();
        if entry.is_err() {
            self.rest = &[];
            self.remaining = 0;
        }
        Some(entry)
    }
}

impl std::iter::FusedIterator for Reader<'_> {}

fn read_str(src: &[u8]) -> Result<(&[u8], usize)> {
    let (len, n) = varint::uvarint(src)?;
    let len =
        usize::try_from(len).map_err(|_| Error::Corrupt(format!("batch string of {len} bytes")))?;
    let end = n
        .checked_add(len)
        .ok_or_else(|| Error::Corrupt("batch string length overflows".to_string()))?;
    if end > src.len() {
        return Error::truncated("batch string", end, src.len());
    }
    Ok((&src[n..end], end))
}

fn put_uvarint(buf: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        buf.push((x as u8) | 0x80);
        x >>= 7;
    }
    buf.push(x as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_batch_is_twelve_zero_bytes() {
        // Measured from Go Pebble, not assumed: an uncommitted batch's
        // sequence number is zero, and Oxia checksums it.
        assert_eq!(Batch::new().as_bytes(), &[0u8; 12]);
        assert_eq!(Batch::new().count(), 0);
        assert_eq!(Batch::new().seq_num(), 0);
    }

    #[test]
    fn entries_round_trip_through_the_reader() {
        let mut b = Batch::new();
        b.put(b"/a", b"1").expect("put");
        b.delete(b"/b").expect("delete");
        b.single_delete(b"/c").expect("single delete");
        b.delete_range(b"/d", b"/e").expect("delete range");
        b.delete_sized(b"/f", 300).expect("delete sized");
        assert_eq!(b.count(), 5);

        let r = Batch::read(b.as_bytes()).expect("read");
        let mut got = Vec::new();
        for e in r {
            let e = e.expect("entry");
            got.push((e.kind, e.key.to_vec(), e.value.map(<[u8]>::to_vec)));
        }
        assert_eq!(
            got,
            vec![
                (Kind::Set, b"/a".to_vec(), Some(b"1".to_vec())),
                (Kind::Delete, b"/b".to_vec(), None),
                (Kind::SingleDelete, b"/c".to_vec(), None),
                (Kind::RangeDelete, b"/d".to_vec(), Some(b"/e".to_vec())),
                (Kind::DeleteSized, b"/f".to_vec(), Some(vec![0xac, 0x02])),
            ]
        );
    }

    #[test]
    fn put_with_writes_the_value_in_place() {
        let mut direct = Batch::new();
        direct.put(b"/k", b"hello").expect("put");

        let mut deferred = Batch::new();
        deferred
            .put_with(b"/k", 5, |buf| buf.copy_from_slice(b"hello"))
            .expect("put_with");

        assert_eq!(direct.as_bytes(), deferred.as_bytes());
    }

    #[test]
    fn clear_keeps_the_header_zeroed() {
        let mut b = Batch::new();
        b.put(b"/a", b"1").expect("put");
        b.set_seq_num(42);
        b.clear();
        assert_eq!(b.as_bytes(), &[0u8; 12]);
        assert_eq!(b.count(), 0);
        assert_eq!(b.seq_num(), 0);
    }

    #[test]
    fn a_header_that_promises_more_entries_than_it_holds_is_refused() {
        let mut b = Batch::new();
        b.put(b"/a", b"1").expect("put");
        let mut repr = b.as_bytes().to_vec();
        repr[COUNT_OFFSET..HEADER_LEN].copy_from_slice(&9u32.to_le_bytes());
        let r = Batch::read(&repr).expect("read");
        let mut err = None;
        for e in r {
            if let Err(e) = e {
                err = Some(e);
            }
        }
        assert!(err.is_some(), "a short batch must not read as complete");
    }

    /// An entry that fails to decode has no length to step over, so the
    /// reader cannot go on past it. It used to hand back the same error on
    /// every call, which a loop that logged and carried on would never leave.
    #[test]
    fn a_reader_stops_after_an_entry_it_cannot_decode() {
        let mut b = Batch::new();
        b.put(b"/a", b"1").expect("put");
        b.put(b"/b", b"2").expect("put");
        let mut repr = b.as_bytes().to_vec();
        repr[HEADER_LEN] = 0xff; // no kind has this byte
        let mut r = Batch::read(&repr).expect("read");
        r.next()
            .expect("an entry")
            .expect_err("a kind no batch writes");
        assert!(r.next().is_none(), "and nothing follows the error");
        assert_eq!(r.remaining(), 0, "nor is anything said to be left");
    }

    #[test]
    fn a_truncated_entry_errors_rather_than_panicking() {
        let mut b = Batch::new();
        b.put(b"/some/key", b"some value").expect("put");
        let full = b.as_bytes().to_vec();
        for cut in HEADER_LEN..full.len() {
            let r = Batch::read(&full[..cut]).expect("header survives");
            for e in r {
                if e.is_err() {
                    break;
                }
            }
        }
    }
}
