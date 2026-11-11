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

/// `InternalKeyKindSSTableInternalObsoleteBit`: set in a Pebblev4 block to mark
/// a key no reader needs. roble honours it and never sets it (DESIGN.md §3).
pub const OBSOLETE_BIT: u8 = 64;

/// `InternalKeyKindSSTableInternalObsoleteMask`, which clears the obsolete bit.
pub const OBSOLETE_MASK: u8 = 191;

/// What an internal key does to the key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `DELETE`, 0.
    Delete,
    /// `SET`, 1.
    Set,
    /// `MERGE`, 2. Refused by the table reader: Oxia configures no merger.
    Merge,
    /// `SINGLEDEL`, 7. Read, never written.
    SingleDelete,
    /// `RANGEDEL`, 15.
    RangeDelete,
    /// `SEPARATOR`, 17, which appears only as an index separator.
    Separator,
    /// `SETWITHDEL`, 18.
    SetWithDelete,
    /// `RANGEKEYDEL`, 19. Refused: Oxia creates no range keys.
    RangeKeyDelete,
    /// `RANGEKEYUNSET`, 20. Refused.
    RangeKeyUnset,
    /// `RANGEKEYSET`, 21. Refused.
    RangeKeySet,
    /// `INGESTSST`, 22. Refused: Oxia does not ingest.
    IngestSst,
    /// `DELSIZED`, 23. Read, never written.
    DeleteSized,
    /// `EXCISE`, 24. Refused: Oxia does not excise.
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
pub const SEARCH_TRAILER: u64 = (SEQ_NUM_MAX << 8) | 24;

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
pub fn encode(user_key: &[u8], seq_num: u64, kind: Kind) -> Vec<u8> {
    encode_with_trailer(user_key, (seq_num << 8) | kind.to_byte() as u64)
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
    encode_with_trailer(user_key, SEARCH_TRAILER)
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
