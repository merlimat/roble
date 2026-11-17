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

//! Opening a database.
//!
//! Two kinds of case, and they check different things.
//!
//! **Against Go.** `tests/golden/db-*` are whole databases written by Go
//! Pebble, committed as the harness left them. The dump beside each one is
//! Pebble's own reader describing what it finds, so comparing roble's dump to
//! it character for character is a check against Pebble rather than against
//! roble's idea of Pebble.
//!
//! **Against a directory built on purpose.** A refusal has no Go counterpart —
//! Pebble will not write a database above roble's pin, and cannot be asked to
//! write one with a comparer it does not have — so those directories are
//! synthesised in the simulated filesystem, where any byte can be arranged.
//! What they check is that roble refuses for the stated reason, not merely
//! that it fails.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Error, Fs, WriteBuf};
use roble_format::files::{MARKER_FORMAT_VERSION, MARKER_MANIFEST, marker_filename};
use roble_format::key::Kind;
use roble_format::manifest::{NewTable, VersionEdit};
use roble_format::record::RecordWriter;

const BYTEWISE: &str = "leveldb.BytewiseComparator";

// ---------------------------------------------------------------- against Go

/// Copies a committed fixture out of the repository, so a test never locks or
/// writes to the corpus itself and two tests cannot contend for one directory.
fn fixture(name: &str) -> (tempfile::TempDir, PathBuf) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    let tmp = tempfile::tempdir().expect("temp dir");
    let dst = tmp.path().join(name);
    std::fs::create_dir(&dst).expect("create");
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), dst.join(entry.file_name())).expect("copy");
    }
    (tmp, dst)
}

fn expected_dump(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(format!("{name}.dump"));
    std::fs::read_to_string(path).expect("read dump")
}

#[compio::test]
async fn roble_reads_the_database_go_pebble_wrote() {
    let (_tmp, dir) = fixture("db-simple");
    let db = roble::open(&roble::DefaultEnv, &dir)
        .await
        .expect("open the database Go wrote");

    // Pebble's own reader describes this directory; roble has to agree with it
    // exactly, which covers the marker choice, the format version, and which
    // table the replay put at which level.
    assert_eq!(db.dump().expect("dump"), expected_dump("db-simple"));
}

#[compio::test]
async fn the_replay_recovers_the_counters_a_store_resumes_from() {
    let (_tmp, dir) = fixture("db-simple");
    let db = roble::open(&roble::DefaultEnv, &dir).await.expect("open");

    // Pebble exposes no public accessor for these, so they are asserted
    // against what the database must be rather than against a Go dump.
    let v = db.versions();
    assert_eq!(v.comparer_name, BYTEWISE);
    assert_eq!(db.dir(), dir, "the directory it was opened in");
    assert_eq!(db.format_version(), 16, "the fixture is written at the pin");
    assert_eq!(db.manifest_filename(), "MANIFEST-000001");

    // Three flushes and a compaction happened, so the next file number has to
    // be past every file the directory holds, and the last sequence number at
    // or above every table's.
    let highest_file = [2u64, 6, 7].into_iter().max().unwrap();
    assert!(
        v.next_file_num > highest_file,
        "next file number {} should be past the {highest_file} already used",
        v.next_file_num
    );
    let highest_seq = v
        .version
        .levels()
        .flat_map(|(_, tables)| tables.iter().map(|t| t.largest_seq_num))
        .max()
        .expect("the fixture has tables");
    assert!(
        v.last_seq_num >= highest_seq,
        "last sequence number {} should be at or above every table's {highest_seq}",
        v.last_seq_num
    );

    // The WAL was disabled, so the log that exists is empty and nothing is
    // left unflushed above it.
    assert_eq!(v.version.table_count(), 2);
}

#[compio::test]
async fn a_log_holding_unflushed_writes_is_replayed() {
    // This fixture has a write that was synced to the log and never flushed.
    // Until replay existed roble refused the database rather than open it
    // short, and the test asserted the refusal; §6 said then that it would be
    // "a refusal today and a replay later, which is the same test either
    // way". This is the later.
    let (_tmp, dir) = fixture("db-unflushed-wal");
    let db = roble::open(&roble::DefaultEnv, &dir)
        .await
        .expect("a database with unreplayed writes should now open");

    assert_eq!(
        db.get(b"flushed", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"in a table".to_vec()),
        "the flushed key comes from the table"
    );
    assert_eq!(
        db.get(b"logged", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"only in the log".to_vec()),
        "and the logged one from the log, or it was lost"
    );
}

#[compio::test]
async fn a_log_holding_no_records_does_not_stop_an_open() {
    // Pebble leaves a log file behind even with DisableWAL, which is Oxia's
    // configuration. Refusing on a log's existence rather than its contents
    // would reject the one shape that matters most.
    //
    // And the file is not empty: it holds Pebble's eleven-byte EOF trailer,
    // the recyclable header with a zero checksum that marks a closed log. So
    // this is also the first time roble reads that trailer off a log Pebble
    // wrote rather than one roble framed itself.
    let (_tmp, dir) = fixture("db-simple");
    let log = std::fs::read(dir.join("000002.log")).expect("read the log");
    assert_eq!(log.len(), 11, "a closed, empty log is just the EOF trailer");
    assert!(
        roble_format::record::RecordReader::new(&log, 2)
            .next()
            .is_none(),
        "and the trailer reads as a clean end of stream, not as a record"
    );

    roble::open(&roble::DefaultEnv, &dir)
        .await
        .expect("a log with no records must not stop an open");
}

// ------------------------------------------- against a directory built on purpose

/// Builds a database directory in the simulated filesystem.
///
/// Everything is explicit, because what these tests vary is exactly the thing
/// Pebble would never write.
struct Synth {
    /// Whether to write a format-version marker at all. A database with a
    /// MANIFEST and no marker predates markers entirely.
    format_marker: bool,
    format_version: String,
    format_marker_iter: u64,
    manifest_marker: Option<(u64, String)>,
    manifest_name: String,
    edits: Vec<VersionEdit>,
    /// Table file numbers to actually place in the directory.
    tables: Vec<u64>,
    /// Extra files, as (name, contents).
    extra: Vec<(String, Vec<u8>)>,
}

impl Default for Synth {
    fn default() -> Self {
        Synth {
            format_marker: true,
            format_version: "016".to_string(),
            format_marker_iter: 1,
            manifest_marker: Some((1, "MANIFEST-000001".to_string())),
            manifest_name: "MANIFEST-000001".to_string(),
            edits: vec![VersionEdit {
                comparer_name: Some(BYTEWISE.to_string()),
                next_file_num: Some(10),
                last_seq_num: Some(20),
                ..Default::default()
            }],
            tables: Vec::new(),
            extra: Vec::new(),
        }
    }
}

impl Synth {
    async fn build(self) -> (SimEnv, PathBuf) {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        let fs: &SimFs = env.sim();
        fs.mkdir_all(&dir).await.expect("mkdir");

        if self.format_marker {
            write(
                fs,
                &dir.join(marker_filename(
                    MARKER_FORMAT_VERSION,
                    self.format_marker_iter,
                    &self.format_version,
                )),
                b"",
            )
            .await;
        }

        if let Some((iter, value)) = &self.manifest_marker {
            write(
                fs,
                &dir.join(marker_filename(MARKER_MANIFEST, *iter, value)),
                b"",
            )
            .await;
        }

        let mut writer = RecordWriter::legacy();
        for edit in &self.edits {
            writer.write_record(&edit.encode()).expect("frame the edit");
        }
        write(fs, &dir.join(&self.manifest_name), &writer.finish()).await;

        for num in &self.tables {
            write(fs, &dir.join(format!("{num:06}.sst")), b"not read here").await;
        }
        for (name, bytes) in &self.extra {
            write(fs, &dir.join(name), bytes).await;
        }
        (env, dir)
    }
}

async fn write(fs: &SimFs, path: &Path, bytes: &[u8]) {
    let disk = fs.create(path).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(bytes.to_vec()), 0, ()).await;
    result.expect("write");
    disk.close().await.expect("close");
}

/// An encoded internal key: the user key, then an eight-byte little-endian
/// trailer of `(sequence << 8) | kind`.
fn internal_key(user_key: &[u8], seq: u64, kind: Kind) -> Vec<u8> {
    let mut out = user_key.to_vec();
    out.extend_from_slice(&((seq << 8) | kind.to_byte() as u64).to_le_bytes());
    out
}

fn table(level: u32, file_num: u64, smallest: &[u8], largest: &[u8], seq: u64) -> NewTable {
    NewTable {
        level,
        file_num,
        size: 100,
        smallest: internal_key(smallest, seq, Kind::Set),
        largest: internal_key(largest, seq, Kind::Set),
        smallest_seq_num: seq,
        largest_seq_num: seq,
        marked_for_compaction: false,
        creation_time: 0,
    }
}

#[compio::test]
async fn a_format_version_above_the_pin_is_refused_before_anything_is_read() {
    let (env, dir) = Synth {
        format_version: "017".to_string(),
        // Deliberately unreadable: if the refusal came from here rather than
        // from the version, the message would say so and this test would fail.
        manifest_name: "MANIFEST-000001".to_string(),
        edits: Vec::new(),
        ..Default::default()
    }
    .build()
    .await;

    let err = roble::open(&env, &dir)
        .await
        .expect_err("17 is above the pin");
    match err {
        Error::Unsupported(m) => assert!(
            m.contains("format major version 17") && m.contains("pin of 16"),
            "the refusal should name the version and the pin, got: {m}"
        ),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[compio::test]
async fn a_format_version_below_what_pebble_supports_is_refused() {
    let (env, dir) = Synth {
        format_version: "012".to_string(),
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Unsupported(m)) => assert!(m.contains("no version below 13"), "got: {m}"),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[compio::test]
async fn the_zero_format_version_pebble_never_persists_is_refused() {
    let (env, dir) = Synth {
        format_version: "000".to_string(),
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(m.contains("never persists"), "got: {m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn the_highest_marker_iteration_wins() {
    // A crash during rotation leaves the old marker behind. The newer one is
    // the truth, and the older is a leftover, not a tie.
    let (env, dir) = Synth {
        format_marker_iter: 7,
        extra: vec![(marker_filename(MARKER_FORMAT_VERSION, 3, "013"), Vec::new())],
        ..Default::default()
    }
    .build()
    .await;

    let db = roble::open(&env, &dir).await.expect("open");
    assert_eq!(
        db.format_version(),
        16,
        "iteration 7 should win over iteration 3"
    );
    assert_eq!(
        db.markers().len(),
        3,
        "the leftover marker is still reported: {:?}",
        db.markers()
    );
}

#[compio::test]
async fn a_marker_file_that_does_not_parse_is_refused() {
    // Pebble treats an unparseable `marker.` file as an error rather than
    // ignoring it, because the file it cannot read might be the current one.
    let (env, dir) = Synth {
        extra: vec![(
            "marker.manifest.not-a-number.MANIFEST-000009".to_string(),
            Vec::new(),
        )],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Format(e)) => {
            let m = e.to_string();
            assert!(m.contains("marker file name"), "got: {m}");
        }
        other => panic!("expected a format error, got {other:?}"),
    }
}

#[compio::test]
async fn a_directory_with_no_manifest_marker_is_not_a_database() {
    let (env, dir) = Synth {
        manifest_marker: None,
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::NoDatabase(m)) => assert!(m.contains("no database in"), "got: {m}"),
        other => panic!("expected NoDatabase, got {other:?}"),
    }
}

#[compio::test]
async fn a_comparer_other_than_bytewise_is_refused() {
    // Oxia's own `oxia-slash-spans` comparer is the real case: a database that
    // was never converted. roble has no custom comparers at all (§3), so the
    // name is checked before a single key is trusted.
    let (env, dir) = Synth {
        edits: vec![VersionEdit {
            comparer_name: Some("oxia-slash-spans".to_string()),
            next_file_num: Some(10),
            last_seq_num: Some(20),
            ..Default::default()
        }],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Unsupported(m)) => assert!(
            m.contains("oxia-slash-spans") && m.contains("leveldb.BytewiseComparator"),
            "got: {m}"
        ),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[compio::test]
async fn a_table_in_the_manifest_but_not_on_disk_is_refused() {
    // The shape of a checkpoint that was streamed short.
    let (env, dir) = Synth {
        edits: vec![VersionEdit {
            comparer_name: Some(BYTEWISE.to_string()),
            next_file_num: Some(10),
            last_seq_num: Some(20),
            new_tables: vec![table(0, 7, b"a", b"b", 5)],
            ..Default::default()
        }],
        tables: Vec::new(),
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(
            m.contains("table 7 is in the MANIFEST but not in"),
            "got: {m}"
        ),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn a_compaction_replays_as_a_move_between_levels() {
    // The edit shape a compaction writes: the table leaves L0 and arrives in
    // L6 in one edit. Applying the addition before the deletion would make
    // this look like a duplicate, and dropping the deletion would leave the
    // table in both levels.
    let (env, dir) = Synth {
        edits: vec![
            VersionEdit {
                comparer_name: Some(BYTEWISE.to_string()),
                next_file_num: Some(10),
                last_seq_num: Some(20),
                new_tables: vec![table(0, 7, b"a", b"b", 5)],
                ..Default::default()
            },
            VersionEdit {
                deleted_tables: vec![(0, 7)],
                new_tables: vec![table(6, 7, b"a", b"b", 5)],
                ..Default::default()
            },
        ],
        tables: vec![7],
        ..Default::default()
    }
    .build()
    .await;

    let db = roble::open(&env, &dir).await.expect("open");
    assert!(
        db.versions().version.level(0).is_empty(),
        "L0 should be empty"
    );
    assert_eq!(db.versions().version.level(6).len(), 1, "L6 should hold it");
    assert_eq!(db.versions().version.table_count(), 1, "and only once");
}

#[compio::test]
async fn an_edit_deleting_a_table_the_version_does_not_hold_is_refused() {
    let (env, dir) = Synth {
        edits: vec![
            VersionEdit {
                comparer_name: Some(BYTEWISE.to_string()),
                next_file_num: Some(10),
                last_seq_num: Some(20),
                ..Default::default()
            },
            VersionEdit {
                deleted_tables: vec![(0, 7)],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(
            m.contains("deletes table 7 from L0, which does not hold it"),
            "got: {m}"
        ),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn overlapping_tables_below_l0_are_refused() {
    // Pebble never leaves two overlapping tables in a level below L0, so this
    // is a check on the replay: getting deletions and additions wrong shows up
    // here rather than as a wrong answer to a read.
    let (env, dir) = Synth {
        edits: vec![VersionEdit {
            comparer_name: Some(BYTEWISE.to_string()),
            next_file_num: Some(10),
            last_seq_num: Some(20),
            new_tables: vec![table(6, 7, b"a", b"m", 5), table(6, 8, b"f", b"z", 5)],
            ..Default::default()
        }],
        tables: vec![7, 8],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(m.contains("L6 tables 7 and 8 overlap"), "got: {m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

// The rest of what the replay refuses. These are the checks that only ever run
// on a MANIFEST that is already wrong, so without a case each they are a claim
// rather than a guard — and a guard that fires for the wrong reason, or not at
// all, is how a corrupt database gets opened and read as if it were sound.

#[compio::test]
async fn a_comparer_that_changes_mid_manifest_is_refused() {
    // Pebble writes the comparer once, in the MANIFEST's first edit. A second
    // and different one would mean the key encoding changed underneath a
    // database that had already been written with the first.
    let (env, dir) = Synth {
        edits: vec![
            VersionEdit {
                comparer_name: Some(BYTEWISE.to_string()),
                next_file_num: Some(10),
                last_seq_num: Some(20),
                ..Default::default()
            },
            VersionEdit {
                comparer_name: Some("pebble.internal.testkeys".to_string()),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(
            m.contains("comparer changes from") && m.contains("mid-MANIFEST"),
            "got: {m}"
        ),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn repeating_the_same_comparer_is_not_a_change() {
    // The check is for a *different* name, not for a second mention: an edit
    // that names the comparer it already had must still open, or the refusal
    // above would fire on databases that are perfectly sound.
    let (env, dir) = Synth {
        edits: vec![
            VersionEdit {
                comparer_name: Some(BYTEWISE.to_string()),
                next_file_num: Some(10),
                last_seq_num: Some(20),
                ..Default::default()
            },
            VersionEdit {
                comparer_name: Some(BYTEWISE.to_string()),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
    .build()
    .await;
    roble::open(&env, &dir)
        .await
        .expect("naming the same comparer twice is not a change");
}

#[compio::test]
async fn an_edit_adding_a_table_the_level_already_holds_is_refused() {
    // The mirror of deleting a table a level does not hold. Letting it through
    // would leave the level holding one file number twice, which every lookup
    // below would then have to cope with.
    let (env, dir) = Synth {
        edits: vec![VersionEdit {
            comparer_name: Some(BYTEWISE.to_string()),
            next_file_num: Some(10),
            last_seq_num: Some(20),
            new_tables: vec![table(6, 7, b"a", b"c", 5), table(6, 7, b"d", b"f", 5)],
            ..Default::default()
        }],
        tables: vec![7],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(
            m.contains("adds table 7 to L6, which already holds it"),
            "got: {m}"
        ),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn a_manifest_naming_no_comparer_is_refused() {
    // Every other check in the replay compares keys. Without a comparer there
    // is nothing to compare them with, so this refuses rather than assuming
    // bytewise and checking an order the database may not be in.
    let (env, dir) = Synth {
        edits: vec![VersionEdit {
            next_file_num: Some(10),
            last_seq_num: Some(20),
            ..Default::default()
        }],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(m.contains("MANIFEST names no comparer"), "got: {m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn a_table_whose_sequence_numbers_run_backwards_is_refused() {
    // A table's bounds and its sequence numbers are decoded from the same
    // edit, so a pair in the wrong order means one of the two was read wrongly
    // — and reading them wrongly is how a table ends up invisible to a
    // snapshot that should see it.
    let (env, dir) = Synth {
        edits: vec![VersionEdit {
            comparer_name: Some(BYTEWISE.to_string()),
            next_file_num: Some(10),
            last_seq_num: Some(20),
            new_tables: vec![NewTable {
                smallest_seq_num: 9,
                largest_seq_num: 4,
                ..table(6, 7, b"a", b"m", 5)
            }],
            ..Default::default()
        }],
        tables: vec![7],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(
            m.contains("L6 table 7: sequence numbers 9..4 run backwards"),
            "got: {m}"
        ),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn a_table_above_the_manifests_last_sequence_number_is_refused() {
    // The MANIFEST's last sequence number is what every snapshot is taken
    // against. A table holding one above it was written after the edit that
    // claims to describe the database, so the two disagree about what exists.
    let (env, dir) = Synth {
        edits: vec![VersionEdit {
            comparer_name: Some(BYTEWISE.to_string()),
            next_file_num: Some(10),
            last_seq_num: Some(20),
            new_tables: vec![NewTable {
                largest_seq_num: 21,
                ..table(6, 7, b"a", b"m", 5)
            }],
            ..Default::default()
        }],
        tables: vec![7],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(
            m.contains("L6 table 7: holds sequence number 21, above the MANIFEST's last 20"),
            "got: {m}"
        ),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn overlapping_tables_in_l0_are_normal() {
    // The same overlap one level up is what a flush produces every time.
    let (env, dir) = Synth {
        edits: vec![VersionEdit {
            comparer_name: Some(BYTEWISE.to_string()),
            next_file_num: Some(10),
            last_seq_num: Some(20),
            new_tables: vec![table(0, 7, b"a", b"m", 5), table(0, 8, b"f", b"z", 6)],
            ..Default::default()
        }],
        tables: vec![7, 8],
        ..Default::default()
    }
    .build()
    .await;
    let db = roble::open(&env, &dir).await.expect("L0 may overlap");
    assert_eq!(db.versions().version.level(0).len(), 2);
}

#[compio::test]
async fn a_failover_log_is_refused_by_name_alone() {
    // WAL failover is above roble's pin (§3), and the only sign of it in a
    // directory is a log name with an index in it.
    let (env, dir) = Synth {
        extra: vec![("000004-001.log".to_string(), Vec::new())],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Unsupported(m)) => assert!(
            m.contains("WAL failover") && m.contains("000004-001.log"),
            "got: {m}"
        ),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[compio::test]
async fn a_blob_file_is_refused_by_name_alone() {
    let (env, dir) = Synth {
        extra: vec![("000004.blob".to_string(), Vec::new())],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Unsupported(m)) => assert!(m.contains("blob file"), "got: {m}"),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[compio::test]
async fn the_lock_is_held_for_as_long_as_the_database_is_open() {
    let (env, dir) = Synth::default().build().await;
    let db = roble::open(&env, &dir).await.expect("open");

    // A second open of the same directory has to be refused, not queued.
    match roble::open(&env, &dir).await {
        Err(Error::Locked) => {}
        other => panic!("expected Locked, got {other:?}"),
    }

    // And released by dropping it, so a reopen works.
    drop(db);
    roble::open(&env, &dir)
        .await
        .expect("the lock should be free once the database is dropped");
}

// What the markers and the MANIFEST are refused for, which until now only the
// cases that reach the version replay had tests. These fire earlier: before
// an edit has been looked at, and in two of them before the MANIFEST has been
// opened at all.

#[compio::test]
async fn a_database_with_no_format_marker_predates_markers_and_is_refused() {
    // Pebble reads a missing marker as FormatDefault, which only happens for a
    // directory it is about to create. One with a MANIFEST and no marker was
    // written before Pebble recorded the version at all, and roble cannot tell
    // which version that was.
    let (env, dir) = Synth {
        format_marker: false,
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Unsupported(m)) => {
            assert!(m.contains("no marker.format-version.* file"), "got: {m}")
        }
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[compio::test]
async fn a_format_marker_that_is_not_a_number_is_refused() {
    // The marker's value is a filename suffix, so anything can be in it. A
    // version that is not a number is a corrupt marker rather than a version
    // roble does not serve — which is the difference between the two refusals
    // this sits beside.
    let (env, dir) = Synth {
        format_version: "sixteen".to_string(),
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(
            m.contains("\"sixteen\"") && m.contains("not a version number"),
            "got: {m}"
        ),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn a_manifest_marker_naming_something_that_is_not_a_file_name_is_refused() {
    let (env, dir) = Synth {
        manifest_marker: Some((1, "not a file name".to_string())),
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(m.contains("is not a file name"), "got: {m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn a_manifest_marker_naming_a_table_is_refused() {
    // A marker that parses but names the wrong kind of file. Opening it as a
    // MANIFEST would read an sstable's bytes as version edits, which is the
    // misreading §3 asks roble to refuse instead.
    let (env, dir) = Synth {
        manifest_marker: Some((1, "000007.sst".to_string())),
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(
            m.contains("000007.sst") && m.contains("rather than a MANIFEST"),
            "got: {m}"
        ),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn a_manifest_holding_no_edits_is_refused() {
    // An empty MANIFEST is not an empty database: a database has at least the
    // edit that named its comparer. A replay that accepted this would open a
    // version with no comparer and no tables, which is indistinguishable from
    // a database whose files have all been lost.
    let (env, dir) = Synth {
        edits: Vec::new(),
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(m.contains("holds no version edits"), "got: {m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn a_manifest_record_that_does_not_frame_is_refused_with_its_number() {
    // A record whose checksum does not match its bytes. The number says which
    // one, and that the records before it were read — a MANIFEST is a
    // sequence, and where it stops being readable is the useful fact.
    let edit = VersionEdit {
        comparer_name: Some(BYTEWISE.to_string()),
        next_file_num: Some(10),
        last_seq_num: Some(20),
        ..Default::default()
    };
    let mut writer = RecordWriter::legacy();
    writer.write_record(&edit.encode()).expect("frame");
    writer.write_record(&edit.encode()).expect("frame");
    let mut bytes = writer.finish();
    // Corrupt the payload of the second record, leaving its checksum stale.
    let at = bytes.len() - 1;
    bytes[at] ^= 0xff;

    let (env, dir) = Synth {
        extra: vec![("MANIFEST-000001".to_string(), bytes)],
        ..Default::default()
    }
    .build()
    .await;
    match roble::open(&env, &dir).await {
        Err(Error::Corrupt(m)) => assert!(m.contains("record 1"), "got: {m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[compio::test]
async fn an_opened_database_describes_itself() {
    // `Db`'s Debug is written out rather than derived, because the lock
    // has no business in a debug line and a `Fs::Lock` need not be Debug at
    // all. Written-out means it can drift from the struct, so it is checked
    // for naming the things someone debugging an open would look for.
    let (env, dir) = Synth::default().build().await;
    let db = roble::open(&env, &dir).await.expect("open");
    let text = format!("{db:?}");
    for expected in [
        "Db",
        "dir",
        "format_version",
        "manifest_filename",
        "markers",
        "versions",
    ] {
        assert!(
            text.contains(expected),
            "the debug line should name {expected:?}, got {text}"
        );
    }
    assert!(
        !text.contains("lock"),
        "the lock has no business in a debug line, got {text}"
    );
}

#[compio::test]
async fn a_write_to_a_database_with_no_log_is_refused() {
    // A directory built by hand has markers, a MANIFEST and nothing else. A
    // database opened from one can be read — everything a read needs is in the
    // MANIFEST and the tables — but a write has nowhere to record itself, and
    // a write that is not recoverable is one a crash loses silently (§4).
    // Refusing is the only answer that does not lie about durability.
    let (env, dir) = Synth::default().build().await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut batch = roble_format::batch::Batch::new();
    batch.put(b"k", b"v").expect("put");
    match db.write_raw(&mut batch, true).await {
        Err(Error::Unsupported(m)) => assert!(
            m.contains("holds no log file") && m.contains("could not be recovered"),
            "got: {m}"
        ),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

/// A store can be owned by a task of its own.
///
/// Which is what a store keeping its filesystem rather than borrowing it is
/// for (§10). [`roble::Env::spawn`] takes `'static` futures, and a store that
/// borrowed its filesystem could be owned by nothing but the stack frame it
/// was opened on — so a compaction could only ever run inside the call that
/// started it. The `'static` bound below is the half of this the compiler
/// checks; the flush is the half that says the store still works when it
/// belongs to the task rather than to this test.
#[compio::test]
async fn a_store_can_be_owned_by_a_task_of_its_own() {
    fn needs_static<T: 'static>(_: &T) {}

    let (tmp, dir) = fixture("db-twin-a");
    let db = std::rc::Rc::new(roble::open(&roble::DefaultEnv, &dir).await.expect("open"));
    needs_static(&db);

    let mut batch = roble_format::batch::Batch::new();
    batch.put(b"spawned", b"yes").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");

    let flushed = std::rc::Rc::new(std::cell::Cell::new(None));
    let (task_db, report) = (std::rc::Rc::clone(&db), std::rc::Rc::clone(&flushed));
    roble::Env::spawn(&roble::DefaultEnv, async move {
        report.set(Some(task_db.flush().await.expect("flush")));
    });

    // A detached task cannot be joined, so the runtime is given real work
    // until it has had its chance. Bounded, so a task that never runs fails
    // the test rather than hanging it.
    let tick = tmp.path().join("tick");
    for _ in 0..64 {
        if flushed.get().is_some() {
            break;
        }
        // Real I/O, so that this loop actually reaches the runtime: an
        // operation that finishes without awaiting anything would spin here
        // and never give the task its turn.
        let disk = roble::FileFs.create(&tick).await.expect("create");
        disk.sync(()).await.expect("sync");
        disk.close().await.expect("close");
    }

    assert!(
        flushed.get().flatten().is_some(),
        "the spawned task should have flushed a table"
    );
    let found = db
        .get(b"spawned", roble::Comparison::Equal)
        .await
        .expect("get")
        .expect("the key the task flushed");
    assert_eq!(
        found.value,
        b"yes".to_vec(),
        "and the store it flushed is the store this test is holding"
    );
}

/// Real I/O, so that a loop waiting on a detached task actually reaches the
/// runtime: an operation that finishes without awaiting anything would spin
/// here and never give the task its turn.
async fn give_the_runtime_a_turn(at: &Path) {
    let disk = roble::FileFs.create(at).await.expect("create");
    disk.sync(()).await.expect("sync");
    disk.close().await.expect("close");
}

#[compio::test]
async fn the_store_runs_its_own_merges() {
    // A store opened to write spawns a task that waits for a merge worth
    // running, and a flush that finds it waiting leaves the merge to it: the
    // write that caused the flush pays for none of it (§9).
    //
    // `tests/schedule.rs` pins which of the two does the merge, by hand and
    // deterministically. What this adds is that the store's own task does it
    // against a real runtime and a real disk, with nobody calling for it.
    let (tmp, dir) = fixture("db-reads");
    // The fixture arrives four layers deep, so five is the line the flush
    // below crosses. The key it writes falls inside a range L0 already
    // holds, so its table lands a layer above rather than beside (§9).
    let db = roble::open_with(
        &roble::DefaultEnv,
        &dir,
        roble::Options::new().l0_compaction_threshold(5),
    )
    .await
    .expect("open");
    let in_l0 = db.versions().version.level(0).len();

    // The task has to be parked before there is work, or the flush would
    // find nobody and run the merge itself; it is spawned by the open, and
    // these turns are where it first gets polled.
    let tick = tmp.path().join("tick");
    for _ in 0..8 {
        give_the_runtime_a_turn(&tick).await;
    }

    let mut batch = roble_format::batch::Batch::new();
    batch.put(b"background", b"yes").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");
    assert_eq!(
        db.versions().version.level(0).len(),
        in_l0 + 1,
        "the flush had somebody waiting, so it should have left the merge to them"
    );

    // Until the merge has landed, which is bounded by the clock and not by a
    // count of turns: it spans the inputs opened, the output written, the
    // MANIFEST and the sweep, and how many turns that I/O takes depends on
    // what else the machine's disk is doing (#119). The minute is only a
    // bound past which it is not coming.
    let waiting = std::time::Instant::now();
    while !db.versions().version.level(0).is_empty()
        && waiting.elapsed() < std::time::Duration::from_secs(60)
    {
        give_the_runtime_a_turn(&tick).await;
    }
    assert_eq!(
        db.versions().version.level(0).len(),
        0,
        "the store's own task should have merged the whole of L0"
    );
    let found = db
        .get(b"background", roble::Comparison::Equal)
        .await
        .expect("get");
    assert_eq!(found.map(|f| f.value), Some(b"yes".to_vec()));
    db.close().await.expect("close");
}

#[compio::test]
async fn a_task_of_its_own_can_be_the_one_that_flushes() {
    // The other half of the pattern, and the half that needs the write path
    // to cooperate (§5, §9). A store opened with the WAL off awaits nothing
    // that is ever pending on a write — no log append, no disk — so a
    // writer in a loop polls to completion every time. If the write path did
    // not hand the thread back, the task below would never be polled after
    // it parked, every flush would land back inside the write that found the
    // budget full, and this test would see a task that flushed nothing.
    //
    // `tests/stall.rs` pins which of the two writes the table, by hand and
    // deterministically. What this adds is that it happens at all against a
    // real runtime, a real disk, and a loop that never waits for either.
    let (tmp, dir) = fixture("db-reads");
    // Small enough that a few hundred writes cross the line many times over,
    // and merges left to the caller so that what the task is doing is the
    // flush and nothing else.
    let db = std::rc::Rc::new(
        roble::open_with(
            &roble::DefaultEnv,
            &dir,
            roble::Options::new()
                .disable_wal(true)
                .disable_automatic_compactions(true)
                .memtable_size(16 << 10)
                // This test is the task, run by hand so that it can count
                // what it flushed; the store's own would race it.
                .background_tasks(false),
        )
        .await
        .expect("open"),
    );

    let started = std::rc::Rc::new(std::cell::Cell::new(false));
    let flushed = std::rc::Rc::new(std::cell::Cell::new(0usize));
    let (task_db, began, count) = (
        std::rc::Rc::clone(&db),
        std::rc::Rc::clone(&started),
        std::rc::Rc::clone(&flushed),
    );
    roble::Env::spawn(&roble::DefaultEnv, async move {
        began.set(true);
        while task_db.running() {
            task_db.flush_wanted().await;
            if let Ok(Some(_)) = task_db.flush().await {
                count.set(count.get() + 1);
            }
        }
    });

    // Until the task has had its first turn, which is the turn it parks on.
    // Real I/O, because nothing else here reaches the runtime; after this
    // the writes are on their own.
    let tick = tmp.path().join("tick");
    for _ in 0..64 {
        if started.get() {
            break;
        }
        give_the_runtime_a_turn(&tick).await;
    }
    assert!(
        started.get(),
        "the task should have been polled, and so be waiting to be told"
    );
    assert_eq!(flushed.get(), 0, "and should have had nothing to do yet");

    let value = vec![b'v'; 256];
    // What one of these writes adds to an arena: the user key, the eight-byte
    // trailer, and the value. The budget is a bound on what the store holds
    // *before* a batch is applied, so the batch that crosses it is the one
    // thing allowed past — `Options::memtable_size` says so.
    let entry = (b"b000000".len() + 8 + value.len()) as u64;
    let budget = db.memtable_size() + entry;
    for i in 0..2000usize {
        let mut batch = roble_format::batch::Batch::new();
        batch
            .put(format!("b{i:06}").as_bytes(), &value)
            .expect("put");
        db.write_raw(&mut batch, false).await.expect("write");
        assert!(
            db.unflushed_bytes() as u64 <= budget,
            "after {} writes the store holds {} bytes, past the {budget} its budget and \
             one batch allow — a memtable swapped out and not written is still memory",
            i + 1,
            db.unflushed_bytes()
        );
    }

    assert!(
        flushed.get() > 0,
        "the writes never handed the thread over, so the task was never polled and \
         every flush went back into a write"
    );

    // And the caller can get its store back. The task holds it while it
    // waits, so until it is told there is nothing more coming there is
    // nothing to drop (§9).
    db.stop();
    for _ in 0..64 {
        if std::rc::Rc::strong_count(&db) == 1 {
            break;
        }
        give_the_runtime_a_turn(&tick).await;
    }
    assert_eq!(
        std::rc::Rc::strong_count(&db),
        1,
        "a store that said it was finished should have been let go of"
    );
}
