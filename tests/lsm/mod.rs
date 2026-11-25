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

//! An LSM written out by hand, as Pebble's test data writes one: memtables
//! and tables at levels, each entry at the sequence number its case
//! chooses. The memtables become a log of one batch per entry, which the
//! open replays; each table is written with the format crate's writer and
//! named in a MANIFEST at its level.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, WriteBuf};
use roble_format::InternalKey;
use roble_format::batch::Batch;
use roble_format::files::{MARKER_FORMAT_VERSION, MARKER_MANIFEST, marker_filename};
use roble_format::key::{self, Kind, SEQ_NUM_MAX};
use roble_format::manifest::{NewTable, VersionEdit};
use roble_format::rangedel::{self, Tombstone};
use roble_format::record::RecordWriter;
use roble_format::sst::{TableWriter, WriterOptions};

const BYTEWISE: &str = "leveldb.BytewiseComparator";
/// The log the memtables are replayed from.
const LOG: u64 = 2;

/// An entry of a `define`, `user.KIND.seq:value`. A range deletion's value
/// is its end key.
#[derive(Clone)]
pub struct Entry {
    pub user: Vec<u8>,
    pub kind: Kind,
    pub seq: u64,
    pub value: Vec<u8>,
}

/// Where a `define` puts a group of entries: a memtable, or one table at a
/// level.
#[derive(Clone)]
pub enum Place {
    Mem,
    Level(u32),
}

/// A `define`: groups of entries, each a memtable or a table, in order. `None`
/// for one roble cannot hold.
pub fn define(input: &[String]) -> Option<Vec<(Place, Vec<Entry>)>> {
    let mut out: Vec<(Place, Vec<Entry>)> = Vec::new();
    for line in input {
        let line = line.trim();
        if line == "mem" {
            out.push((Place::Mem, Vec::new()));
        } else if let Some(level) = line.strip_prefix('L') {
            out.push((Place::Level(level.parse().expect("a level")), Vec::new()));
        } else {
            let (k, value) = line.split_once(':').expect("key:value");
            let mut parts = k.rsplitn(3, '.');
            let seq = parts
                .next()
                .expect("seq")
                .parse()
                .expect("a sequence number");
            let kind = match parts.next().expect("kind") {
                "SET" => Kind::Set,
                "DEL" => Kind::Delete,
                "SINGLEDEL" => Kind::SingleDelete,
                "RANGEDEL" => Kind::RangeDelete,
                "MERGE" => return None,
                other => panic!("a kind this runner does not know: {other}"),
            };
            let user = parts.next().expect("user key").as_bytes().to_vec();
            out.last_mut()
                .expect("a mem or level heading")
                .1
                .push(Entry {
                    user,
                    kind,
                    seq,
                    value: value.as_bytes().to_vec(),
                });
        }
    }
    Some(out)
}

async fn write(fs: &SimFs, path: &Path, bytes: &[u8]) {
    let disk = fs.create(path).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(bytes.to_vec()), 0, ()).await;
    result.expect("write");
    disk.close().await.expect("close");
}

/// A table holding `entries`, written with `writer`, and the MANIFEST's
/// account of it.
fn table(
    level: u32,
    file_num: u64,
    entries: &[Entry],
    writer: WriterOptions,
) -> (Vec<u8>, NewTable) {
    let mut points: Vec<&Entry> = entries
        .iter()
        .filter(|e| e.kind != Kind::RangeDelete)
        .collect();
    // Internal key order: user key ascending, then newest first.
    points.sort_by(|a, b| a.user.cmp(&b.user).then(b.seq.cmp(&a.seq)));
    let tombstones: Vec<Tombstone> = entries
        .iter()
        .filter(|e| e.kind == Kind::RangeDelete)
        .map(|e| Tombstone {
            start: e.user.clone(),
            end: e.value.clone(),
            seq_num: e.seq,
        })
        .collect();
    let fragments = rangedel::fragment(&tombstones.iter().collect::<Vec<_>>());

    let mut writer = TableWriter::new(writer).expect("a writer");
    // Every key the table holds, encoded, so its bounds are its own.
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for e in &points {
        writer
            .add(
                InternalKey {
                    user_key: &e.user,
                    seq_num: e.seq,
                    kind: e.kind,
                    obsolete: false,
                },
                &e.value,
            )
            .expect("add");
        keys.push(key::encode(&e.user, e.seq, e.kind));
    }
    for t in &fragments {
        writer
            .add_range_deletion(&t.start, &t.end, t.seq_num)
            .expect("add a range deletion");
        keys.push(key::encode(&t.start, t.seq_num, Kind::RangeDelete));
        keys.push(key::encode(&t.end, SEQ_NUM_MAX, Kind::RangeDelete));
    }
    let bytes = writer.finish().expect("finish");
    let order = |a: &Vec<u8>, b: &Vec<u8>| {
        let (a, b) = (
            InternalKey::decode(a).expect("a key"),
            InternalKey::decode(b).expect("a key"),
        );
        a.user_key
            .cmp(b.user_key)
            .then((b.seq_num, b.kind.to_byte()).cmp(&(a.seq_num, a.kind.to_byte())))
    };
    keys.sort_by(order);
    let seqs = entries.iter().map(|e| e.seq);
    let meta = NewTable {
        level,
        file_num,
        size: bytes.len() as u64,
        smallest: keys.first().expect("a key").clone(),
        largest: keys.last().expect("a key").clone(),
        smallest_seq_num: seqs.clone().min().expect("a seq"),
        largest_seq_num: seqs.max().expect("a seq"),
        marked_for_compaction: false,
        creation_time: 0,
    };
    (bytes, meta)
}

/// The LSM `groups` describes, less every entry numbered `below` or above,
/// as a store directory, its tables written with `writer`.
pub async fn build(
    groups: &[(Place, Vec<Entry>)],
    below: u64,
    writer: WriterOptions,
) -> (SimEnv, PathBuf) {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let fs: &SimFs = env.sim();
    fs.mkdir_all(&dir).await.expect("mkdir");

    let mut log = RecordWriter::recyclable(LOG as u32);
    let mut logged: Vec<&Entry> = Vec::new();
    let mut tables = Vec::new();
    let mut next = LOG + 1;
    let mut last_seq = 0;
    for (place, entries) in groups {
        let kept: Vec<Entry> = entries.iter().filter(|e| e.seq < below).cloned().collect();
        last_seq = kept.iter().map(|e| e.seq).fold(last_seq, u64::max);
        match place {
            Place::Mem => logged.extend(entries.iter().filter(|e| e.seq < below)),
            Place::Level(level) if !kept.is_empty() => {
                let (bytes, meta) = table(*level, next, &kept, writer);
                write(fs, &dir.join(format!("{next:06}.sst")), &bytes).await;
                tables.push(meta);
                next += 1;
            }
            Place::Level(_) => {}
        }
    }
    // One batch per entry at its own sequence number, in the order they were
    // numbered, as a store would have logged them.
    logged.sort_by_key(|e| e.seq);
    for e in logged {
        let mut batch = Batch::new();
        match e.kind {
            Kind::Set => batch.put(&e.user, &e.value),
            Kind::Delete => batch.delete(&e.user),
            Kind::SingleDelete => batch.single_delete(&e.user),
            Kind::RangeDelete => batch.delete_range(&e.user, &e.value),
            other => panic!("{other:?} in a memtable"),
        }
        .expect("batch");
        batch.set_seq_num(e.seq);
        log.write_record(batch.as_bytes()).expect("frame");
    }
    write(fs, &dir.join(format!("{LOG:06}.log")), &log.finish()).await;

    let edit = VersionEdit {
        comparer_name: Some(BYTEWISE.to_string()),
        min_unflushed_log_num: Some(LOG),
        next_file_num: Some(next),
        last_seq_num: Some(last_seq),
        new_tables: tables,
        ..Default::default()
    };
    let mut manifest = RecordWriter::legacy();
    manifest
        .write_record(&edit.encode())
        .expect("frame the edit");
    write(fs, &dir.join("MANIFEST-000001"), &manifest.finish()).await;
    write(
        fs,
        &dir.join(marker_filename(MARKER_FORMAT_VERSION, 1, "016")),
        b"",
    )
    .await;
    write(
        fs,
        &dir.join(marker_filename(MARKER_MANIFEST, 1, "MANIFEST-000001")),
        b"",
    )
    .await;
    fs.sync_dir(&dir).await.expect("sync dir");
    (env, dir)
}
