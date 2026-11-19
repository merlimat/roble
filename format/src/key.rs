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

//! Internal keys (`internal/base/internal.go`: `InternalKeyKind*`,
//! `InternalTrailerLen = 8`).
//!
//! An internal key is the user key followed by an 8-byte little-endian
//! trailer: the kind in the low byte, the sequence number in the upper seven.
//! This module describes the format and nothing more — which kinds roble
//! accepts is a policy of the table reader (DESIGN.md §3), not of the encoding.

use crate::error::{Error, Result};

/// The number of trailer bytes after the user key.
pub const TRAILER_LEN: usize = 8;

/// The largest sequence number, used as a boundary sentinel.
pub const SEQ_NUM_MAX: u64 = (1 << 56) - 1;

/// The bit an **uncommitted batch** sets in place of a sequence number it has
/// not been given yet (Pebble's `SeqNumBatchBit`).
///
/// A batch that can be read back through itself has to order its own entries
/// against the store's, and it has no sequence numbers to do it with: those
/// are handed out when it commits. So while it is being built, entry `i`
/// stands in at `SEQ_NUM_BATCH | i` — above every number a committed entry
/// can hold, so the batch always wins, and in the order it was written, so a
/// later entry wins over an earlier one for the same key.
///
/// These numbers never leave the batch's own index. The repr carries no
/// sequence numbers at all until commit stamps the header, so nothing with
/// this bit set is ever written to a log, a table or a MANIFEST.
#[doc(hidden)]
pub const SEQ_NUM_BATCH: u64 = 1 << 55;

/// `InternalKeyKindSSTableInternalObsoleteBit`: set in a Pebblev4 block to mark
/// a key no reader needs. roble honours it and never sets it (DESIGN.md §3).
pub const OBSOLETE_BIT: u8 = 64;

/// `InternalKeyKindSSTableInternalObsoleteMask`, which clears the obsolete bit.
pub(crate) const OBSOLETE_MASK: u8 = 191;

/// What an internal key does to the key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Kind {
    /// `DELETE`, 0.
    Delete,
    /// `SET`, 1.
    Set,
    /// `MERGE`, 2. Refused by the table reader: roble has no merge operator.
    Merge,
    /// `SINGLEDEL`, 7. Read, never created; a compaction carries one it read,
    /// or writes a `DEL` for it over an older version of its key it drops.
    SingleDelete,
    /// `RANGEDEL`, 15.
    RangeDelete,
    /// `SEPARATOR`, 17, which appears only as an index separator.
    Separator,
    /// `SETWITHDEL`, 18. Written for a Set that a flush or compaction lays
    /// over a deletion of its key it drops, as Pebble's `setNext` does.
    SetWithDelete,
    /// `RANGEKEYDEL`, 19. Refused: roble does not serve range keys.
    RangeKeyDelete,
    /// `RANGEKEYUNSET`, 20. Refused.
    RangeKeyUnset,
    /// `RANGEKEYSET`, 21. Refused.
    RangeKeySet,
    /// `INGESTSST`, 22. Refused: roble does not ingest tables.
    IngestSst,
    /// `DELSIZED`, 23. Read, never created; a compaction carries one it read.
    DeleteSized,
    /// `EXCISE`, 24. Refused: roble does not excise.
    Excise,
}

impl Kind {
    /// Decodes a kind byte that has already had the obsolete bit cleared.
    pub fn from_byte(b: u8) -> Result<Self> {
        Ok(match b {
            0 => Kind::Delete,
            1 => Kind::Set,
            2 => Kind::Merge,
            7 => Kind::SingleDelete,
            15 => Kind::RangeDelete,
            17 => Kind::Separator,
            18 => Kind::SetWithDelete,
            19 => Kind::RangeKeyDelete,
            20 => Kind::RangeKeyUnset,
            21 => Kind::RangeKeySet,
            22 => Kind::IngestSst,
            23 => Kind::DeleteSized,
            24 => Kind::Excise,
            other => return Error::corrupt(format!("internal key kind {other}")),
        })
    }

    /// The byte that encodes this kind in an internal key's trailer.
    pub fn to_byte(self) -> u8 {
        match self {
            Kind::Delete => 0,
            Kind::Set => 1,
            Kind::Merge => 2,
            Kind::SingleDelete => 7,
            Kind::RangeDelete => 15,
            Kind::Separator => 17,
            Kind::SetWithDelete => 18,
            Kind::RangeKeyDelete => 19,
            Kind::RangeKeyUnset => 20,
            Kind::RangeKeySet => 21,
            Kind::IngestSst => 22,
            Kind::DeleteSized => 23,
            Kind::Excise => 24,
        }
    }

    /// The lowercase name used in dumps and errors.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Delete => "del",
            Kind::Set => "set",
            Kind::Merge => "merge",
            Kind::SingleDelete => "singledel",
            Kind::RangeDelete => "rangedel",
            Kind::Separator => "separator",
            Kind::SetWithDelete => "setwithdel",
            Kind::RangeKeyDelete => "rangekeydel",
            Kind::RangeKeyUnset => "rangekeyunset",
            Kind::RangeKeySet => "rangekeyset",
            Kind::IngestSst => "ingestsst",
            Kind::DeleteSized => "delsized",
            Kind::Excise => "excise",
        }
    }

    /// Whether roble serves this kind, as opposed to refusing the table
    /// (DESIGN.md §3). The reader applies this; the encoding does not care.
    #[doc(hidden)]
    pub fn is_accepted(self) -> bool {
        matches!(
            self,
            Kind::Delete
                | Kind::Set
                | Kind::SingleDelete
                | Kind::RangeDelete
                | Kind::Separator
                | Kind::SetWithDelete
                | Kind::DeleteSized
        )
    }
}

/// The trailer a seek targets, from Pebble's `MakeSearchKey`: the largest
/// sequence number with the largest kind byte.
///
/// Trailers sort **descending**, so this is the trailer that sorts before every
/// real key sharing its user key — which is what makes a seek to a user key
/// land on that user key's newest entry rather than past it. The kind is
/// Pebble's `InternalKeyKindMax`, asserted against [`Kind::Excise`] in this
/// module's tests because `to_byte` cannot be called in a constant.
pub(crate) const SEARCH_TRAILER: u64 = (SEQ_NUM_MAX << 8) | 24;

/// Splits an encoded internal key into its user key and its **raw** trailer.
///
/// Raw, rather than the decoded sequence number and kind, because that is what
/// ordering compares: the Pebblev4 obsolete bit lives in the kind byte, and
/// reassembling a trailer from [`InternalKey`]'s fields would drop it and with
/// it the ordering between two keys that differ only there.
pub fn split_encoded(encoded: &[u8]) -> Result<(&[u8], u64)> {
    if encoded.len() < TRAILER_LEN {
        return Error::truncated("internal key", TRAILER_LEN, encoded.len());
    }
    let (user_key, trailer) = encoded.split_at(encoded.len() - TRAILER_LEN);
    Ok((
        user_key,
        u64::from_le_bytes(trailer.try_into().expect("8 bytes")),
    ))
}

/// Compares two encoded internal keys as Pebble's `InternalCompare` does: user
/// key ascending bytewise, then the raw trailer **descending**.
///
/// The descending trailer is why the newest version of a key is the first one a
/// forward scan meets, and getting it backwards does not produce an error — it
/// produces a reader that returns stale values.
pub fn compare(a: &[u8], b: &[u8]) -> Result<std::cmp::Ordering> {
    let (a_user, a_trailer) = split_encoded(a)?;
    let (b_user, b_trailer) = split_encoded(b)?;
    Ok(a_user.cmp(b_user).then_with(|| b_trailer.cmp(&a_trailer)))
}

/// Encodes an internal key: the user key, then the little-endian trailer.
///
/// `seq_num` must be at most [`SEQ_NUM_MAX`]: the trailer has 56 bits for it,
/// and the bits above them would be shifted out, leaving a different number
/// with nothing to say so. Checked in debug builds only: a number read from
/// disk is held to it where it is read ([`crate::batch::seq_num_of`],
/// [`crate::manifest::VersionEdit::decode`]), and roble refuses a write that
/// would number past it.
pub fn encode(user_key: &[u8], seq_num: u64, kind: Kind) -> Vec<u8> {
    debug_assert!(
        seq_num <= SEQ_NUM_MAX,
        "sequence number {seq_num} past SEQ_NUM_MAX"
    );
    encode_with_trailer(user_key, (seq_num << 8) | kind.to_byte() as u64)
}

/// [`encode`], into a buffer the caller keeps rather than a fresh one.
///
/// A memtable builds one of these for every entry a write applies and copies
/// it into its arena straight away, so a fresh `Vec` per entry was an
/// allocation per write with nothing to keep. `seq_num` is held to
/// [`SEQ_NUM_MAX`] as [`encode`]'s is.
pub fn encode_into(user_key: &[u8], seq_num: u64, kind: Kind, out: &mut Vec<u8>) {
    debug_assert!(
        seq_num <= SEQ_NUM_MAX,
        "sequence number {seq_num} past SEQ_NUM_MAX"
    );
    out.clear();
    out.reserve(user_key.len() + TRAILER_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&((seq_num << 8) | kind.to_byte() as u64).to_le_bytes());
}

/// Encodes an internal key with a trailer given whole.
pub fn encode_with_trailer(user_key: &[u8], trailer: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + TRAILER_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&trailer.to_le_bytes());
    out
}

/// The internal key a seek to `user_key` targets.
pub fn search_key(user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + TRAILER_LEN);
    search_key_into(user_key, &mut out);
    out
}

/// The same, into a buffer the caller keeps rather than a fresh one.
///
/// A point lookup builds one of these per call and throws it away, so the
/// caller of a read in a loop can hand the same buffer back each time. The
/// encoding stays here rather than being spelled out at the call site: a
/// search key that disagreed with [`search_key`] would seek to the wrong
/// place, and the two would have to be kept in step by eye.
pub fn search_key_into(user_key: &[u8], out: &mut Vec<u8>) {
    out.clear();
    // Sized once. Two `extend_from_slice` calls onto an empty buffer would
    // allocate for the key and then grow again for the trailer, which a
    // counting allocator sees as two — the whole point here is that a warm
    // buffer costs neither.
    out.reserve(user_key.len() + TRAILER_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&SEARCH_TRAILER.to_le_bytes());
}

/// A borrowed internal key: the user key, its sequence number and its kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternalKey<'a> {
    /// The user key, without the trailer.
    pub user_key: &'a [u8],
    /// The sequence number, `trailer >> 8`.
    pub seq_num: u64,
    /// The kind, with the obsolete bit cleared.
    pub kind: Kind,
    /// Whether the Pebblev4 obsolete bit was set on this key.
    pub obsolete: bool,
}

impl<'a> InternalKey<'a> {
    /// Splits an encoded internal key into its parts.
    pub fn decode(encoded: &'a [u8]) -> Result<Self> {
        if encoded.len() < TRAILER_LEN {
            return Error::truncated("internal key", TRAILER_LEN, encoded.len());
        }
        let split = encoded.len() - TRAILER_LEN;
        let (user_key, trailer) = encoded.split_at(split);
        let trailer = u64::from_le_bytes(trailer.try_into().expect("8 bytes"));
        let kind_byte = (trailer & 0xff) as u8;
        Ok(InternalKey {
            user_key,
            seq_num: trailer >> 8,
            kind: Kind::from_byte(kind_byte & OBSOLETE_MASK)?,
            obsolete: kind_byte & OBSOLETE_BIT != 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_into_writes_what_encode_returns_over_what_was_there() {
        let mut out = b"left over from the last key, and longer".to_vec();
        for (user, seq, kind) in [
            (&b"k"[..], 7, Kind::Set),
            (b"", 0, Kind::Delete),
            (b"/a/b", 1 << 40, Kind::RangeDelete),
        ] {
            encode_into(user, seq, kind, &mut out);
            assert_eq!(out, encode(user, seq, kind), "{user:?} {seq} {kind:?}");
        }
    }

    /// Every kind, its byte and its name, pinned one row at a time.
    ///
    /// A round trip alone would not do: two constants swapped in both
    /// directions survive `from_byte(to_byte(k)) == k` and still write a
    /// trailer Pebble reads as the wrong kind. The bytes are Pebble's, so they
    /// are asserted against literals rather than against each other.
    ///
    /// Most of these never appear in a table roble reads — it refuses them
    /// (§3) — which is exactly why nothing else reaches them: a kind roble
    /// declines still has to be decoded correctly to be declined for the right
    /// reason.
    #[test]
    fn every_kind_keeps_pebbles_byte_and_its_name() {
        let table: &[(Kind, u8, &str)] = &[
            (Kind::Delete, 0, "del"),
            (Kind::Set, 1, "set"),
            (Kind::Merge, 2, "merge"),
            (Kind::SingleDelete, 7, "singledel"),
            (Kind::RangeDelete, 15, "rangedel"),
            (Kind::Separator, 17, "separator"),
            (Kind::SetWithDelete, 18, "setwithdel"),
            (Kind::RangeKeyDelete, 19, "rangekeydel"),
            (Kind::RangeKeyUnset, 20, "rangekeyunset"),
            (Kind::RangeKeySet, 21, "rangekeyset"),
            (Kind::IngestSst, 22, "ingestsst"),
            (Kind::DeleteSized, 23, "delsized"),
            (Kind::Excise, 24, "excise"),
        ];
        for &(kind, byte, name) in table {
            assert_eq!(kind.to_byte(), byte, "{kind:?} encodes to {byte}");
            assert_eq!(
                Kind::from_byte(byte).expect("a kind roble knows"),
                kind,
                "byte {byte} decodes to {kind:?}"
            );
            assert_eq!(kind.name(), name, "{kind:?} is named {name:?}");
        }
        assert_eq!(
            table.len(),
            13,
            "a kind was added to the enum without a row here"
        );
    }

    #[test]
    fn a_byte_that_is_no_kind_is_corrupt_rather_than_guessed() {
        // 3 is LogData, which Pebble writes only into a WAL and never into a
        // table; the rest are gaps between the kinds and past the end.
        for b in [3u8, 4, 8, 16, 25, 99, 255] {
            let Err(err) = Kind::from_byte(b) else {
                panic!("byte {b} is not a kind roble serves");
            };
            let text = format!("{err}");
            assert!(
                text.contains(&b.to_string()),
                "the error should name the byte, got {text:?}"
            );
        }
    }
}
