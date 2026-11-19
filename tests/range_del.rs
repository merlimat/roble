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

//! Range deletions over a hand-built LSM, judged against Pebble's own cases.
//!
//! `tests/pebble/range_del` is Pebble's `testdata/range_del` at the pin,
//! unchanged (`tests/pebble/README.md`). Each `define` lays out an LSM entry
//! by entry: memtables, and tables at named levels, holding sets and range
//! deletions at sequence numbers the case chooses, so that a tombstone sits
//! exactly where a reader has to step over or around it — across memtables,
//! across levels, newer and older than the keys it covers. Then `get` and
//! `iter` read it, and the case gives Pebble's answers.
//!
//! The same LSM is built for roble: the memtables as a log of one batch per
//! entry, each at its own sequence number, which the open replays; and each
//! level's tables written with the format crate's writer and named in a
//! MANIFEST at that level.
//!
//! **Every read in the file is at a sequence number**, `get seq=N`, which sees
//! only what is numbered below N. roble has no read at an arbitrary past
//! number, and never needs one: its snapshots hold the version they were made
//! on, so no table a snapshot reads holds anything newer than it. What a
//! read at N sees, though, is by definition what a read of the newest state
//! sees when everything numbered N or above was never written. So each read
//! builds the LSM without those entries — the same tables at the same levels,
//! less their newer entries — and reads that at the newest sequence number,
//! which is the read roble does. Answers are compared line for line.
//!
//! Skipped: a `define` holding a MERGE, which roble refuses (§3); and what
//! follows a `compact`, which is Pebble's compaction rewriting the LSM.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
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

/// One command of a datadriven file: the command line, its input lines, and
/// the output written under it.
struct Case {
    line: usize,
    command: String,
    input: Vec<String>,
    expected: String,
}

/// Splits a file in the format of cockroachdb/datadriven, the one Pebble's
/// test data is written in. An output runs to the first blank line, or,
/// when it opens with a second `----`, to a closing `----` pair.
fn cases(text: &str) -> Vec<Case> {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i].trim_end();
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }
        let at = i + 1;
        let command = line.to_string();
        i += 1;
        let mut input = Vec::new();
        while i < lines.len() && lines[i] != "----" {
            input.push(lines[i].to_string());
            i += 1;
        }
        i += 1;
        let mut expected = Vec::new();
        if lines.get(i) == Some(&"----") {
            i += 1;
            while i + 1 < lines.len() && !(lines[i] == "----" && lines[i + 1] == "----") {
                expected.push(lines[i]);
                i += 1;
            }
            i += 2;
        } else {
            while i < lines.len() && !lines[i].trim().is_empty() {
                expected.push(lines[i]);
                i += 1;
            }
        }
        out.push(Case {
            line: at,
            command,
            input,
            expected: expected.join("\n"),
        });
    }
    out
}

/// An entry of a `define`, `user.KIND.seq:value`. A range deletion's value
/// is its end key.
#[derive(Clone)]
struct Entry {
    user: Vec<u8>,
    kind: Kind,
    seq: u64,
    value: Vec<u8>,
}

/// Where a `define` puts a group of entries: a memtable, or one table at a
/// level.
#[derive(Clone)]
enum Place {
    Mem,
    Level(u32),
}

/// A `define`: groups of entries, each a memtable or a table, in order. `None`
/// for one roble cannot hold.
fn define(input: &[String]) -> Option<Vec<(Place, Vec<Entry>)>> {
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

/// Whether every layer of the LSM holds only sequence numbers above those of
/// every layer older than it: later memtables over earlier ones, memtables
/// over tables, and each level over the ones below.
///
/// Pebble's merging iterator relies on this, letting a range deletion delete
/// every older layer's keys without comparing sequence numbers. A `define`
/// can break it, putting a tombstone numbered 13 in a newer memtable than a
/// key numbered 14, and Pebble's answer then follows the layers and not the
/// numbers. No store writes that: a newer memtable or level holds what was
/// written later. roble replays the memtables as one, ordered by sequence
/// number alone, so such a `define` is not one it can be built as.
fn layered(groups: &[(Place, Vec<Entry>)]) -> bool {
    // Newer first: the last memtable, then the others, then L0, L1, ...
    let rank = |i: usize, p: &Place| match p {
        Place::Mem => (0, usize::MAX - i),
        Place::Level(l) => (1 + *l as usize, 0),
    };
    let span = |es: &[Entry]| {
        let seqs = es.iter().map(|e| e.seq);
        (seqs.clone().min(), seqs.max())
    };
    for (i, (p, a)) in groups.iter().enumerate() {
        for (j, (q, b)) in groups.iter().enumerate() {
            if rank(i, p) < rank(j, q)
                && let ((Some(newer), _), (_, Some(older))) = (span(a), span(b))
                && newer <= older
            {
                return false;
            }
        }
    }
    true
}

async fn write(fs: &SimFs, path: &Path, bytes: &[u8]) {
    let disk = fs.create(path).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(bytes.to_vec()), 0, ()).await;
    result.expect("write");
    disk.close().await.expect("close");
}

/// A table holding `entries`, and the MANIFEST's account of it.
fn table(level: u32, file_num: u64, entries: &[Entry]) -> (Vec<u8>, NewTable) {
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

    let mut writer = TableWriter::new(WriterOptions::default()).expect("a writer");
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
/// as a store directory.
async fn build(groups: &[(Place, Vec<Entry>)], below: u64) -> (SimEnv, PathBuf) {
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
                let (bytes, meta) = table(*level, next, &kept);
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

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// What `get` answers, spelled as Pebble's test spells it.
async fn get(db: &roble::Db<SimFs>, keys: &[String]) -> String {
    let mut out = Vec::new();
    for k in keys {
        let k = k.trim();
        out.push(
            match db.get(k.as_bytes(), Comparison::Equal).await.expect("get") {
                Some(f) => format!("{k}:{}", text(&f.value)),
                None => format!("{k}: pebble: not found"),
            },
        );
    }
    out.join("\n")
}

/// What an iterator's steps answer, spelled as Pebble's `runIterCmd` spells
/// them: `key: (value, .)` on a key, the `.` being the range key roble does
/// not have, and `.` off the end.
async fn iter(db: &roble::Db<SimFs>, steps: &[String]) -> String {
    let mut scan = db.range(..).expect("scan");
    let mut out = Vec::new();
    for step in steps {
        let mut words = step.split_whitespace();
        let result = match (words.next(), words.next()) {
            (Some("first"), None) => scan.first().await,
            (Some("last"), None) => scan.last().await,
            (Some("next"), None) => scan.next().await,
            (Some("prev"), None) => scan.prev().await,
            (Some("seek-ge"), Some(k)) => scan.seek_ge(k.as_bytes()).await,
            (Some("seek-lt"), Some(k)) => scan.seek_lt(k.as_bytes()).await,
            other => panic!("a step this runner does not know: {other:?}"),
        };
        result.expect("step");
        out.push(if scan.is_valid() {
            format!("{}: ({}, .)", text(scan.key()), text(scan.value()))
        } else {
            ".".to_string()
        });
    }
    out.join("\n")
}

#[compio::test]
async fn roble_reads_pebbles_range_deletion_cases_as_pebble_does() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pebble/range_del");
    let text = std::fs::read_to_string(&path).expect("Pebble's range_del test data");

    let mut lsm: Option<Vec<(Place, Vec<Entry>)>> = None;
    let mut judged = 0;
    let mut unlayered = Vec::new();
    for case in cases(&text) {
        let mut words = case.command.split_whitespace();
        let command = words.next().expect("a command");
        let seq: Option<u64> = words
            .find_map(|w| w.strip_prefix("seq="))
            .map(|s| s.parse().expect("a sequence number"));
        match command {
            "define" => {
                lsm = define(&case.input);
                if lsm.as_ref().is_some_and(|g| !layered(g)) {
                    unlayered.push(case.line);
                    lsm = None;
                }
                continue;
            }
            // Pebble rewrites the LSM, and the reads after it are of that.
            "compact" => {
                lsm = None;
                continue;
            }
            "get" | "iter" => {}
            _ => continue,
        }
        let Some(groups) = &lsm else { continue };
        let below = seq.expect("every read in this file is at a sequence number");

        let (env, dir) = build(groups, below).await;
        let db = roble::open_with(
            &env,
            &dir,
            Options::new()
                .disable_automatic_compactions(true)
                .background_tasks(false),
        )
        .await
        .unwrap_or_else(|e| panic!("line {}: the store did not open: {e}", case.line));
        let got = if command == "get" {
            get(&db, &case.input).await
        } else {
            iter(&db, &case.input).await
        };
        assert_eq!(
            got,
            case.expected,
            "line {}: {}\n{}",
            case.line,
            case.command,
            case.input.join("\n")
        );
        judged += 1;
    }
    // Every read outside the skipped defines, so a runner that lost some fails
    // here rather than passing on fewer.
    // Of the file's 67 reads: 26 in defines holding a MERGE, 6 after a
    // compact, and 11 in the two defines whose layers are out of order —
    // the second of which Pebble's own comment calls an LSM "not generated
    // naturally". The rest are judged, so a runner that lost some fails here.
    assert_eq!(
        unlayered,
        [459, 973],
        "the defines skipped as out of layer order"
    );
    assert_eq!(judged, 24, "the reads judged in {}", path.display());
}

/// A reverse seek that steps back out of a table holding only a range
/// deletion below the target keeps that deletion in play.
///
/// The layout `compat/differential` met in a database Pebble wrote (seed
/// 509). Pebble split a compaction's output inside a range deletion at
/// `k0403`, so in L5 the first table ends at the exclusive sentinel there and
/// the second starts there with nothing but the deletion's fragment: its
/// first point key is `k0411`. Beneath, L6 still holds `k0403` from before
/// the deletion.
///
/// A `Floor` of `k0403` seeks below `k0403\0`. In L5 that lands in the
/// second table, finds no point key below the target and waits at the first
/// table's bound — still holding the second, whose fragment covers `k0403`.
/// L6's `k0403` was taken first, and then that bound, at the same user key,
/// came up, and the merge opened the first table to see what it held: which
/// put the level on it and out of reach of the fragment, and the deleted
/// `k0403` came back. A table whose bound is the exclusive sentinel at a key
/// holds nothing there, so it waits until the key is decided.
#[compio::test]
async fn a_reverse_seek_keeps_the_deletion_of_the_table_it_stepped_back_out_of() {
    let entry = |user: &str, kind, seq, value: &str| Entry {
        user: user.as_bytes().to_vec(),
        kind,
        seq,
        value: value.as_bytes().to_vec(),
    };
    let groups = vec![
        (
            Place::Level(5),
            vec![
                entry("k0390", Kind::Set, 300, "new"),
                entry("k0392", Kind::RangeDelete, 339, "k0403"),
            ],
        ),
        (
            Place::Level(5),
            vec![
                entry("k0403", Kind::RangeDelete, 339, "k0416"),
                entry("k0411", Kind::Set, 340, "after"),
            ],
        ),
        (
            Place::Level(6),
            vec![
                entry("k0380", Kind::Set, 85, "deep"),
                entry("k0403", Kind::Set, 85, "deleted"),
                entry("k0420", Kind::Set, 85, "deep"),
            ],
        ),
    ];
    let (env, dir) = build(&groups, u64::MAX).await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .background_tasks(false),
    )
    .await
    .expect("open");
    let l5 = db.versions().version.level(5).to_vec();
    assert!(
        l5.len() == 2 && l5[0].largest_is_exclusive().expect("bound"),
        "the layout should be two L5 tables meeting at an exclusive bound"
    );

    let answer = |f: Option<roble::Found>| f.map(|f| text(&f.key));
    for (probe, how) in [
        (&b"k0403"[..], Comparison::Floor),
        (b"k0403\0", Comparison::Floor),
        (b"k0404", Comparison::Lower),
        (b"k0410", Comparison::Floor),
    ] {
        assert_eq!(
            answer(db.get(probe, how).await.expect("get")),
            Some("k0390".to_string()),
            "{how:?} of {:?} should pass the deleted k0403",
            text(probe)
        );
    }
    assert_eq!(
        answer(db.get(b"k0403", Comparison::Equal).await.expect("get")),
        None
    );

    let mut scan = db.range(..).expect("scan");
    scan.seek_lt(b"k0403\0").await.expect("seek");
    assert_eq!(text(scan.key()), "k0390", "a scan's reverse seek, too");
    scan.prev().await.expect("prev");
    assert_eq!(text(scan.key()), "k0380");
    scan.seek_lt(b"k0412").await.expect("seek");
    assert_eq!(text(scan.key()), "k0411");
    scan.prev().await.expect("prev");
    assert_eq!(text(scan.key()), "k0390", "and a step back across it");
}
