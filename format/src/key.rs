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
