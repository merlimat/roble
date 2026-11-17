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

//! Version edits (`internal/manifest/version_edit.go`), the records a MANIFEST
//! is made of.
//!
//! An edit is a flat sequence of `(tag, payload)` pairs, tag first as a
//! uvarint, read until the record runs out. There is no count and no length:
//! the record's own framing (§record) delimits it.
//!
//! The parts roble reads are the ones a version is rebuilt from — the comparer
//! name, the minimum unflushed log number, the next file number, the last
//! sequence number, and files added and deleted per level. Two of those carry
//! the recovery rules §6 depends on, which is why they are not optional
//! niceties: `MinUnflushedLogNum` says which logs a reopen must replay, and
//! `LastSeqNum` is where sequence numbers resume when there is nothing to
//! replay.
//!
//! **Which new-file tag Pebble writes is a signal, not a detail.** It picks
//! `tagNewFile5` only for a table with range keys, `tagNewFile4` when a custom
//! field is present, and `tagNewFile2` otherwise. So a range-key table
//! announces itself in the MANIFEST before any sstable is opened, and roble
//! refuses it there (DESIGN.md §3) rather than discovering it a block at a
//! time.
//!
//! Likewise the backing-table tags and the `virtual` custom field exist only
//! for virtual sstables, which roble refuses; and Pebble's own rule for
//! unknown custom fields is that any tag with the `1 << 6` bit set must not be
//! ignored, so roble refuses those too instead of skipping them.

use crate::error::{Error, Result};
use crate::varint;

/// Levels in a Pebble database: L0 through L6.
pub const NUM_LEVELS: u32 = 7;

// Tags, from version_edit.go. Tag 8 is unused.
const TAG_COMPARATOR: u64 = 1;
const TAG_LOG_NUMBER: u64 = 2;
const TAG_NEXT_FILE_NUMBER: u64 = 3;
const TAG_LAST_SEQUENCE: u64 = 4;
const TAG_COMPACT_POINTER: u64 = 5;
const TAG_DELETED_FILE: u64 = 6;
const TAG_NEW_FILE: u64 = 7;
const TAG_PREV_LOG_NUMBER: u64 = 9;
const TAG_NEW_FILE2: u64 = 100;
const TAG_NEW_FILE3: u64 = 102;
const TAG_NEW_FILE4: u64 = 103;
const TAG_NEW_FILE5: u64 = 104;
const TAG_CREATED_BACKING_TABLE: u64 = 105;
const TAG_REMOVED_BACKING_TABLE: u64 = 106;
const TAG_NEW_BLOB_FILE: u64 = 107;
const TAG_DELETED_BLOB_FILE: u64 = 108;
const TAG_COLUMN_FAMILY: u64 = 200;
const TAG_COLUMN_FAMILY_ADD: u64 = 201;
const TAG_COLUMN_FAMILY_DROP: u64 = 202;
const TAG_MAX_COLUMN_FAMILY: u64 = 203;

// Custom fields inside tagNewFile4 and above.
const CUSTOM_TAG_TERMINATE: u64 = 1;
const CUSTOM_TAG_NEEDS_COMPACTION: u64 = 2;
const CUSTOM_TAG_CREATION_TIME: u64 = 6;
const CUSTOM_TAG_PATH_ID: u64 = 65;
const CUSTOM_TAG_NON_SAFE_IGNORE_MASK: u64 = 1 << 6;
const CUSTOM_TAG_VIRTUAL: u64 = 66;
const CUSTOM_TAG_SYNTHETIC_PREFIX: u64 = 67;
const CUSTOM_TAG_SYNTHETIC_SUFFIX: u64 = 68;
const CUSTOM_TAG_BLOB_REFERENCES: u64 = 69;

/// A table added to a level by an edit.
///
/// Not `#[non_exhaustive]`, and on purpose: its fields are the MANIFEST tags
/// Pebble writes at the pinned format, so one more is a new format to read,
/// which is a breaking change whatever this struct says. Left open, it can
/// still be built as a literal with `..Default::default()`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NewTable {
    /// The level the table enters.
    pub level: u32,
    /// The table's file number.
    pub file_num: u64,
    /// The table's size in bytes.
    pub size: u64,
    /// The smallest internal key in the table, encoded.
    pub smallest: Vec<u8>,
    /// The largest internal key in the table, encoded.
    pub largest: Vec<u8>,
    /// The smallest sequence number in the table.
    pub smallest_seq_num: u64,
    /// The largest sequence number in the table.
    pub largest_seq_num: u64,
    /// Whether a compaction was requested for this table.
    pub marked_for_compaction: bool,
    /// Unix seconds when the table was created, or zero if unrecorded.
    pub creation_time: u64,
}

/// One MANIFEST record: a change to the set of files.
///
/// Not `#[non_exhaustive]`, for the reason [`NewTable`] is not.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VersionEdit {
    /// The comparer the database was created with.
    pub comparer_name: Option<String>,
    /// The lowest log number a reopen must replay (`MinUnflushedLogNum`).
    pub min_unflushed_log_num: Option<u64>,
    /// A log number kept for RocksDB compatibility.
    pub obsolete_prev_log_num: Option<u64>,
    /// The next file number to hand out.
    pub next_file_num: Option<u64>,
    /// The highest sequence number committed when this edit was written.
    pub last_seq_num: Option<u64>,
    /// `(level, file number)` of each table removed.
    pub deleted_tables: Vec<(u32, u64)>,
    /// Each table added.
    pub new_tables: Vec<NewTable>,
}

impl VersionEdit {
    /// Decodes one version edit.
    pub fn decode(mut src: &[u8]) -> Result<Self> {
        let mut edit = VersionEdit::default();
        while !src.is_empty() {
            let tag = take_uvarint(&mut src)?;
            match tag {
                TAG_COMPARATOR => {
                    let s = take_bytes(&mut src)?;
                    edit.comparer_name = Some(
                        std::str::from_utf8(s)
                            .map_err(|_| Error::Corrupt("comparer name is not UTF-8".to_string()))?
                            .to_string(),
                    );
                }
                TAG_LOG_NUMBER => edit.min_unflushed_log_num = Some(take_uvarint(&mut src)?),
                TAG_PREV_LOG_NUMBER => edit.obsolete_prev_log_num = Some(take_uvarint(&mut src)?),
                TAG_NEXT_FILE_NUMBER => edit.next_file_num = Some(take_uvarint(&mut src)?),
                TAG_LAST_SEQUENCE => edit.last_seq_num = Some(take_uvarint(&mut src)?),
                TAG_COMPACT_POINTER => {
                    // RocksDB stopped using these; Pebble reads and drops them.
                    let _level = take_level(&mut src)?;
                    let _key = take_bytes(&mut src)?;
                }
                TAG_DELETED_FILE => {
                    let level = take_level(&mut src)?;
                    let file_num = take_uvarint(&mut src)?;
                    edit.deleted_tables.push((level, file_num));
                }
                TAG_NEW_FILE | TAG_NEW_FILE2 | TAG_NEW_FILE3 | TAG_NEW_FILE4 => {
                    edit.new_tables.push(decode_new_table(&mut src, tag)?);
                }
                TAG_NEW_FILE5 => {
                    // Pebble picks this tag only for a table with range keys,
                    // so the MANIFEST announces them before any sstable is
                    // opened.
                    return Error::unsupported(
                        "version edit adds a table with range keys, which Oxia never writes \
                         and roble does not read",
                    );
                }
                TAG_CREATED_BACKING_TABLE | TAG_REMOVED_BACKING_TABLE => {
                    return Error::unsupported(
                        "version edit refers to a virtual sstable's backing table, which only \
                         Excise and IngestAndExcise create",
                    );
                }
                TAG_NEW_BLOB_FILE | TAG_DELETED_BLOB_FILE => {
                    return Error::unsupported(
                        "version edit refers to a blob file, which arrives above roble's pin",
                    );
                }
                TAG_COLUMN_FAMILY
                | TAG_COLUMN_FAMILY_ADD
                | TAG_COLUMN_FAMILY_DROP
                | TAG_MAX_COLUMN_FAMILY => {
                    return Error::unsupported(
                        "version edit uses RocksDB column families, which Pebble never writes",
                    );
                }
                other => {
                    return Error::corrupt(format!("version edit tag {other}"));
                }
            }
        }
        Ok(edit)
    }

    /// Encodes the edit, in the field order Pebble writes.
    ///
    /// The order is part of matching Pebble byte for byte: comparer, log
    /// number, previous log number, next file number, last sequence, deleted
    /// files, new files.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        if let Some(name) = &self.comparer_name {
            put_uvarint(&mut out, TAG_COMPARATOR);
            put_bytes(&mut out, name.as_bytes());
        }
        if let Some(n) = self.min_unflushed_log_num {
            put_uvarint(&mut out, TAG_LOG_NUMBER);
            put_uvarint(&mut out, n);
        }
        if let Some(n) = self.obsolete_prev_log_num {
            put_uvarint(&mut out, TAG_PREV_LOG_NUMBER);
            put_uvarint(&mut out, n);
        }
        if let Some(n) = self.next_file_num {
            put_uvarint(&mut out, TAG_NEXT_FILE_NUMBER);
            put_uvarint(&mut out, n);
        }
        if let Some(n) = self.last_seq_num {
            put_uvarint(&mut out, TAG_LAST_SEQUENCE);
            put_uvarint(&mut out, n);
        }
        for (level, file_num) in &self.deleted_tables {
            put_uvarint(&mut out, TAG_DELETED_FILE);
            put_uvarint(&mut out, u64::from(*level));
            put_uvarint(&mut out, *file_num);
        }
        for t in &self.new_tables {
            // Pebble uses tagNewFile4 when any custom field is present and
            // tagNewFile2 otherwise; roble writes no table with range keys, so
            // tagNewFile5 never arises.
            let custom = t.marked_for_compaction || t.creation_time != 0;
            put_uvarint(&mut out, if custom { TAG_NEW_FILE4 } else { TAG_NEW_FILE2 });
            put_uvarint(&mut out, u64::from(t.level));
            put_uvarint(&mut out, t.file_num);
            put_uvarint(&mut out, t.size);
            put_bytes(&mut out, &t.smallest);
            put_bytes(&mut out, &t.largest);
            put_uvarint(&mut out, t.smallest_seq_num);
            put_uvarint(&mut out, t.largest_seq_num);
            if custom {
                // Creation time before needs-compaction. This order is read
                // from Pebble's Encode and is *not* verified against Go
                // output: no table in the golden MANIFEST carries both
                // fields, because MarkedForCompaction is set only inside
                // Pebble and no public API produces one. The test below pins
                // it against this reading so it cannot drift unnoticed.
                if t.creation_time != 0 {
                    put_uvarint(&mut out, CUSTOM_TAG_CREATION_TIME);
                    let mut field = Vec::new();
                    put_uvarint(&mut field, t.creation_time);
                    put_bytes(&mut out, &field);
                }
                if t.marked_for_compaction {
                    put_uvarint(&mut out, CUSTOM_TAG_NEEDS_COMPACTION);
                    put_bytes(&mut out, &[1]);
                }
                put_uvarint(&mut out, CUSTOM_TAG_TERMINATE);
            }
        }
        out
    }
}

fn decode_new_table(src: &mut &[u8], tag: u64) -> Result<NewTable> {
    let mut t = NewTable {
        level: take_level(src)?,
        file_num: take_uvarint(src)?,
        ..Default::default()
    };
    if tag == TAG_NEW_FILE3 {
        // RocksDB's path ID, which Pebble reads and drops.
        let _path_id = take_uvarint(src)?;
    }
    t.size = take_uvarint(src)?;
    t.smallest = take_bytes(src)?.to_vec();
    t.largest = take_bytes(src)?.to_vec();
    if tag != TAG_NEW_FILE {
        t.smallest_seq_num = take_uvarint(src)?;
        t.largest_seq_num = take_uvarint(src)?;
    }
    if tag == TAG_NEW_FILE4 {
        loop {
            let custom = take_uvarint(src)?;
            match custom {
                CUSTOM_TAG_TERMINATE => break,
                CUSTOM_TAG_CREATION_TIME => {
                    let field = take_bytes(src)?;
                    let (v, n) = varint::uvarint(field)?;
                    if n != field.len() {
                        return Error::corrupt("version edit creation time is not a lone uvarint");
                    }
                    t.creation_time = v;
                }
                CUSTOM_TAG_NEEDS_COMPACTION => {
                    let field = take_bytes(src)?;
                    if field.len() != 1 {
                        return Error::corrupt(format!(
                            "version edit needs-compaction field is {} bytes, want 1",
                            field.len()
                        ));
                    }
                    t.marked_for_compaction = field[0] == 1;
                }
                CUSTOM_TAG_VIRTUAL => {
                    return Error::unsupported(
                        "version edit adds a virtual sstable, which only Excise and \
                         IngestAndExcise create",
                    );
                }
                CUSTOM_TAG_SYNTHETIC_PREFIX | CUSTOM_TAG_SYNTHETIC_SUFFIX => {
                    return Error::unsupported(
                        "version edit adds a table with a synthetic prefix or suffix, which \
                         comes with FormatSyntheticPrefixSuffix, above roble's pin",
                    );
                }
                CUSTOM_TAG_BLOB_REFERENCES => {
                    return Error::unsupported(
                        "version edit adds a table with blob references, which arrive above \
                         roble's pin",
                    );
                }
                CUSTOM_TAG_PATH_ID => {
                    // Pebble refuses this one outright rather than skipping it.
                    return Error::unsupported("version edit uses a path-id custom field");
                }
                other => {
                    // Pebble's own rule: a tag with this bit set changes how
                    // the table must be read, so skipping it would be reading
                    // the table wrong rather than reading less of it.
                    if other & CUSTOM_TAG_NON_SAFE_IGNORE_MASK != 0 {
                        return Error::unsupported(format!(
                            "version edit custom field {other} may not be ignored"
                        ));
                    }
                    let _ignored = take_bytes(src)?;
                }
            }
        }
    }
    Ok(t)
}

fn take_uvarint(src: &mut &[u8]) -> Result<u64> {
    let (v, n) = varint::uvarint(src)?;
    *src = &src[n..];
    Ok(v)
}

fn take_level(src: &mut &[u8]) -> Result<u32> {
    let level = take_uvarint(src)?;
    let level = u32::try_from(level)
        .ok()
        .filter(|&l| l < NUM_LEVELS)
        .ok_or_else(|| Error::Corrupt(format!("version edit names level {level}")))?;
    Ok(level)
}

fn take_bytes<'a>(src: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = take_uvarint(src)?;
    let len = usize::try_from(len)
        .map_err(|_| Error::Corrupt(format!("version edit string of {len} bytes")))?;
    if len > src.len() {
        return Error::truncated("version edit string", len, src.len());
    }
    let (head, tail) = src.split_at(len);
    *src = tail;
    Ok(head)
}

fn put_uvarint(buf: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        buf.push((x as u8) | 0x80);
        x >>= 7;
    }
    buf.push(x as u8);
}

fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(buf, b.len() as u64);
    buf.extend_from_slice(b);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> VersionEdit {
        VersionEdit {
            comparer_name: Some("leveldb.BytewiseComparator".to_string()),
            min_unflushed_log_num: Some(12),
            obsolete_prev_log_num: None,
            next_file_num: Some(34),
            last_seq_num: Some(5678),
            deleted_tables: vec![(0, 7), (6, 8)],
            new_tables: vec![
                NewTable {
                    level: 0,
                    file_num: 9,
                    size: 4096,
                    smallest: b"/a\x01\x00\x00\x00\x00\x00\x00\x00".to_vec(),
                    largest: b"/z\x01\x00\x00\x00\x00\x00\x00\x00".to_vec(),
                    smallest_seq_num: 1,
                    largest_seq_num: 2,
                    marked_for_compaction: false,
                    creation_time: 0,
                },
                NewTable {
                    level: 6,
                    file_num: 10,
                    size: 1 << 20,
                    smallest: b"/b\x01\x00\x00\x00\x00\x00\x00\x00".to_vec(),
                    largest: b"/y\x01\x00\x00\x00\x00\x00\x00\x00".to_vec(),
                    smallest_seq_num: 3,
                    largest_seq_num: 4,
                    marked_for_compaction: true,
                    creation_time: 1_762_000_000,
                },
            ],
        }
    }

    #[test]
    fn an_edit_round_trips() {
        let edit = sample();
        let bytes = edit.encode();
        assert_eq!(VersionEdit::decode(&bytes).expect("decode"), edit);
    }

    #[test]
    fn an_empty_edit_encodes_to_nothing() {
        let edit = VersionEdit::default();
        assert!(edit.encode().is_empty());
        assert_eq!(VersionEdit::decode(&[]).expect("decode"), edit);
    }

    #[test]
    fn the_new_file_tag_follows_pebbles_rule() {
        // No custom fields: tagNewFile2. With one: tagNewFile4. The tag is the
        // first byte of the entry, and both are multi-byte uvarints.
        let mut plain = VersionEdit::default();
        plain.new_tables.push(NewTable {
            smallest: b"k\x01\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            largest: b"k\x01\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            ..Default::default()
        });
        let (tag, _) = varint::uvarint(&plain.encode()).expect("tag");
        assert_eq!(tag, TAG_NEW_FILE2);

        let mut dated = plain.clone();
        dated.new_tables[0].creation_time = 1;
        let (tag, _) = varint::uvarint(&dated.encode()).expect("tag");
        assert_eq!(tag, TAG_NEW_FILE4);
    }

    /// Pins the order of the two custom fields a table can carry together.
    ///
    /// Unlike every other claim in this module, this one is **not** checked
    /// against Go Pebble's own bytes: the golden MANIFEST has no table with
    /// both a creation time and a needs-compaction mark, and no public API
    /// produces one. So this test records what Pebble's Encode does, read from
    /// the source, and fails if roble drifts from it.
    #[test]
    fn custom_fields_are_written_in_pebbles_order() {
        let mut edit = VersionEdit::default();
        edit.new_tables.push(NewTable {
            level: 0,
            file_num: 1,
            size: 100,
            smallest: b"a\x01\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            largest: b"z\x01\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            smallest_seq_num: 1,
            largest_seq_num: 2,
            marked_for_compaction: true,
            creation_time: 1_762_000_000,
        });
        let bytes = edit.encode();

        // Searching for the tag's byte value would be ambiguous — 2 and 6 also
        // occur as lengths and sequence numbers — so the whole custom-field
        // region is built and compared. The field encodings themselves are
        // pinned against Go by the golden MANIFEST; what this adds is their
        // relative order.
        let mut expected = Vec::new();
        put_uvarint(&mut expected, CUSTOM_TAG_CREATION_TIME);
        let mut field = Vec::new();
        put_uvarint(&mut field, 1_762_000_000);
        put_bytes(&mut expected, &field);
        put_uvarint(&mut expected, CUSTOM_TAG_NEEDS_COMPACTION);
        put_bytes(&mut expected, &[1]);
        put_uvarint(&mut expected, CUSTOM_TAG_TERMINATE);
        assert!(
            bytes.ends_with(&expected),
            "custom fields are not in Pebble's order; tail is {:02x?}, expected {:02x?}",
            &bytes[bytes.len().saturating_sub(expected.len())..],
            expected
        );
        // And it still round-trips.
        assert_eq!(VersionEdit::decode(&bytes).expect("decode"), edit);
    }

    #[test]
    fn the_structures_roble_refuses_are_refused_by_name() {
        // Range keys announce themselves through the new-file tag.
        let mut bytes = Vec::new();
        put_uvarint(&mut bytes, TAG_NEW_FILE5);
        let err = VersionEdit::decode(&bytes).expect_err("range keys must be refused");
        assert!(format!("{err}").contains("range keys"), "{err}");

        for tag in [TAG_CREATED_BACKING_TABLE, TAG_REMOVED_BACKING_TABLE] {
            let mut bytes = Vec::new();
            put_uvarint(&mut bytes, tag);
            let err = VersionEdit::decode(&bytes).expect_err("backing tables must be refused");
            assert!(format!("{err}").contains("virtual"), "{err}");
        }

        for tag in [TAG_COLUMN_FAMILY, TAG_MAX_COLUMN_FAMILY] {
            let mut bytes = Vec::new();
            put_uvarint(&mut bytes, tag);
            assert!(VersionEdit::decode(&bytes).is_err());
        }
    }

    #[test]
    fn a_virtual_table_inside_a_new_file_entry_is_refused() {
        // The custom field, rather than the tag, is how an excised table shows
        // up — and skipping it would read the table as though it owned its
        // whole backing file.
        let mut bytes = Vec::new();
        put_uvarint(&mut bytes, TAG_NEW_FILE4);
        put_uvarint(&mut bytes, 0); // level
        put_uvarint(&mut bytes, 1); // file number
        put_uvarint(&mut bytes, 100); // size
        put_bytes(&mut bytes, b"a\x01\x00\x00\x00\x00\x00\x00\x00");
        put_bytes(&mut bytes, b"z\x01\x00\x00\x00\x00\x00\x00\x00");
        put_uvarint(&mut bytes, 1); // smallest seqnum
        put_uvarint(&mut bytes, 2); // largest seqnum
        put_uvarint(&mut bytes, CUSTOM_TAG_VIRTUAL);
        put_uvarint(&mut bytes, 42); // backing file number
        put_uvarint(&mut bytes, CUSTOM_TAG_TERMINATE);
        let err = VersionEdit::decode(&bytes).expect_err("a virtual table must be refused");
        assert!(format!("{err}").contains("virtual sstable"), "{err}");
    }

    #[test]
    fn an_unknown_custom_field_is_skipped_only_when_pebble_says_it_is_safe() {
        let mut base = Vec::new();
        put_uvarint(&mut base, TAG_NEW_FILE4);
        put_uvarint(&mut base, 0);
        put_uvarint(&mut base, 1);
        put_uvarint(&mut base, 100);
        put_bytes(&mut base, b"a\x01\x00\x00\x00\x00\x00\x00\x00");
        put_bytes(&mut base, b"z\x01\x00\x00\x00\x00\x00\x00\x00");
        put_uvarint(&mut base, 1);
        put_uvarint(&mut base, 2);

        // Below the mask: length-prefixed and ignorable.
        let mut safe = base.clone();
        put_uvarint(&mut safe, 3);
        put_bytes(&mut safe, b"whatever");
        put_uvarint(&mut safe, CUSTOM_TAG_TERMINATE);
        let edit = VersionEdit::decode(&safe).expect("a safe field is skipped");
        assert_eq!(edit.new_tables.len(), 1);

        // At or above the mask: refused.
        let mut unsafe_field = base;
        put_uvarint(&mut unsafe_field, CUSTOM_TAG_NON_SAFE_IGNORE_MASK | 3);
        put_bytes(&mut unsafe_field, b"whatever");
        put_uvarint(&mut unsafe_field, CUSTOM_TAG_TERMINATE);
        assert!(VersionEdit::decode(&unsafe_field).is_err());
    }

    #[test]
    fn a_level_outside_the_tree_is_refused() {
        let mut bytes = Vec::new();
        put_uvarint(&mut bytes, TAG_DELETED_FILE);
        put_uvarint(&mut bytes, u64::from(NUM_LEVELS));
        put_uvarint(&mut bytes, 1);
        assert!(VersionEdit::decode(&bytes).is_err());
    }

    #[test]
    fn a_truncated_edit_errors_rather_than_panicking() {
        let full = sample().encode();
        for cut in 0..full.len() {
            let _ = VersionEdit::decode(&full[..cut]);
        }
    }

    /// Builds a version edit's bytes a tag at a time.
    ///
    /// `VersionEdit::encode` only writes the tags roble writes, which is why
    /// it cannot produce any of the edits below: every one of them is a tag
    /// Pebble or RocksDB writes and roble has to read — or refuse — without
    /// ever writing one itself.
    #[derive(Default)]
    struct Edit(Vec<u8>);

    impl Edit {
        fn tag(mut self, tag: u64) -> Self {
            put_uvarint(&mut self.0, tag);
            self
        }
        fn num(mut self, v: u64) -> Self {
            put_uvarint(&mut self.0, v);
            self
        }
        fn bytes(mut self, b: &[u8]) -> Self {
            put_uvarint(&mut self.0, b.len() as u64);
            self.0.extend_from_slice(b);
            self
        }
        fn decode(&self) -> Result<VersionEdit> {
            VersionEdit::decode(&self.0)
        }
    }

    /// An internal key, as a new-table bound has to be.
    fn bound(user: &[u8], seq: u64) -> Vec<u8> {
        crate::key::encode_with_trailer(user, (seq << 8) | crate::key::Kind::Set.to_byte() as u64)
    }

    #[test]
    fn a_compact_pointer_is_read_and_dropped() {
        // RocksDB stopped writing these; Pebble reads past them. A MANIFEST
        // old enough to hold one still has to open, and the fields have to be
        // consumed or every tag after it would be read at the wrong offset.
        let edit = Edit::default()
            .tag(TAG_COMPACT_POINTER)
            .num(3)
            .bytes(b"a-key")
            .tag(TAG_NEXT_FILE_NUMBER)
            .num(42)
            .decode()
            .expect("a compact pointer is dropped, not refused");
        assert_eq!(
            edit.next_file_num,
            Some(42),
            "the tag after it must still be read at the right offset"
        );
    }

    #[test]
    fn a_custom_field_that_may_be_ignored_is_ignored() {
        // Pebble's rule: a tag without the non-safe-ignore bit describes
        // something a reader can do without.
        let edit = Edit::default()
            .tag(TAG_NEW_FILE4)
            .num(0)
            .num(7)
            .num(100)
            .bytes(&bound(b"a", 1))
            .bytes(&bound(b"z", 2))
            .num(1)
            .num(2)
            .num(3)
            .bytes(b"whatever")
            .num(CUSTOM_TAG_TERMINATE)
            .decode()
            .expect("an ignorable field is ignored");
        assert_eq!(edit.new_tables.len(), 1);
        assert_eq!(edit.new_tables[0].file_num, 7);
    }

    #[test]
    fn a_path_id_in_the_tag_is_read_and_dropped() {
        // TAG_NEW_FILE3 carries RocksDB's path ID between the file number and
        // the size. Skipping it would read the size as the path and every
        // field after it one place late.
        let edit = Edit::default()
            .tag(TAG_NEW_FILE3)
            .num(2)
            .num(9)
            .num(0)
            .num(500)
            .bytes(&bound(b"a", 1))
            .bytes(&bound(b"z", 2))
            .num(3)
            .num(4)
            .decode()
            .expect("a path id is dropped, not refused");
        let t = &edit.new_tables[0];
        assert_eq!((t.level, t.file_num, t.size), (2, 9, 500));
        assert_eq!((t.smallest_seq_num, t.largest_seq_num), (3, 4));
    }

    #[test]
    fn a_creation_time_that_is_not_one_number_is_refused() {
        let err = Edit::default()
            .tag(TAG_NEW_FILE4)
            .num(0)
            .num(7)
            .num(100)
            .bytes(&bound(b"a", 1))
            .bytes(&bound(b"z", 2))
            .num(1)
            .num(2)
            .num(CUSTOM_TAG_CREATION_TIME)
            .bytes(&[1, 2, 3, 4])
            .num(CUSTOM_TAG_TERMINATE)
            .decode()
            .expect_err("trailing bytes after the time");
        assert!(
            format!("{err}").contains("creation time is not a lone uvarint"),
            "got {err:?}"
        );
    }

    #[test]
    fn a_needs_compaction_field_that_is_not_one_byte_is_refused() {
        // Pebble writes a single byte here and reads it as a bool. A field of
        // another length is not a longer bool — it is a field this reader has
        // misunderstood, and guessing at `field[0]` would mark a table for
        // compaction on the strength of whatever happened to be first.
        let err = Edit::default()
            .tag(TAG_NEW_FILE4)
            .num(0)
            .num(7)
            .num(100)
            .bytes(&bound(b"a", 1))
            .bytes(&bound(b"z", 2))
            .num(1)
            .num(2)
            .num(CUSTOM_TAG_NEEDS_COMPACTION)
            .bytes(&[1, 1])
            .num(CUSTOM_TAG_TERMINATE)
            .decode()
            .expect_err("two bytes where one was expected");
        assert!(
            format!("{err}").contains("needs-compaction field is 2 bytes, want 1"),
            "got {err:?}"
        );
    }

    /// The tags and custom fields that say the table must be read a way roble
    /// cannot read it, each refused by name.
    ///
    /// These are §3's pin expressed in the MANIFEST: every one of them is
    /// announced before any sstable is opened, so refusing here is refusing
    /// before a single wrong byte has been read.
    #[test]
    fn a_table_roble_cannot_read_is_refused_from_the_manifest() {
        // Range keys and backing tables already have a case above, in
        // `the_structures_roble_refuses_are_refused_by_name`. The blob-file
        // tags did not: they arrive with Pebblev6, so a MANIFEST naming one
        // is a database written past the pin.
        for tag in [TAG_NEW_BLOB_FILE, TAG_DELETED_BLOB_FILE] {
            let err = Edit::default()
                .tag(tag)
                .decode()
                .expect_err("a blob file must be refused");
            assert!(format!("{err}").contains("blob file"), "got {err:?}");
        }

        // And the custom fields, which need a whole new-table entry in front
        // of them before the field is reached.
        let with_custom = |custom: u64, payload: &[u8]| {
            let mut e = Edit::default()
                .tag(TAG_NEW_FILE4)
                .num(0)
                .num(7)
                .num(100)
                .bytes(&bound(b"a", 1))
                .bytes(&bound(b"z", 2))
                .num(1)
                .num(2)
                .num(custom);
            if !payload.is_empty() {
                e = e.bytes(payload);
            }
            e.decode().expect_err("a field roble cannot ignore")
        };

        let err = with_custom(CUSTOM_TAG_SYNTHETIC_PREFIX, b"p");
        assert!(format!("{err}").contains("synthetic prefix"), "got {err:?}");

        let err = with_custom(CUSTOM_TAG_BLOB_REFERENCES, b"b");
        assert!(format!("{err}").contains("blob references"), "got {err:?}");

        let err = with_custom(CUSTOM_TAG_PATH_ID, b"");
        assert!(format!("{err}").contains("path-id"), "got {err:?}");

        // An unknown tag with the non-safe-ignore bit set. Pebble's own rule
        // is that such a tag changes how the table must be read, so skipping
        // it would be reading the table wrong rather than reading less of it.
        let err = with_custom(CUSTOM_TAG_NON_SAFE_IGNORE_MASK | 6, b"x");
        assert!(
            format!("{err}").contains("may not be ignored"),
            "got {err:?}"
        );
    }
}
