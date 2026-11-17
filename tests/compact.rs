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

//! Compacting tables into the level below.
//!
//! A compaction is the first thing roble does that throws data away, so what
//! every test here asks is the same question twice: did the database still
//! answer the same afterwards, and did it actually throw anything away. The
//! second half matters as much as the first — a compaction that kept
//! everything would pass the first on its own.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

async fn load(name: &str) -> (SimEnv, PathBuf) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let fs: &SimFs = env.sim();
    fs.mkdir_all(&dir).await.expect("mkdir");
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        let bytes = std::fs::read(entry.path()).expect("read file");
        let disk = fs
            .create(&dir.join(entry.file_name()))
            .await
            .expect("create");
        let (result, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
        result.expect("write");
        disk.sync(()).await.expect("sync");
        disk.close().await.expect("close");
    }
    fs.sync_dir(&dir).await.expect("sync dir");
    (env, dir)
}

async fn contents<F: Fs>(db: &roble::Db<F>) -> Model {
    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    let mut out = Model::new();
    while scan.is_valid() {
        out.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    out
}

/// Opens with compaction left to the caller.
///
/// A flush runs the compaction the picker chose unless it is told not to,
/// and the tests that use this name the tables to merge themselves: a flush
/// that compacted for itself would have moved them first.
/// `tests/schedule.rs` is where the scheduling is the subject.
async fn open_unscheduled(env: &SimEnv, dir: &Path) -> roble::Db<SimFs> {
    roble::open_with(env, dir, Options::new().disable_automatic_compactions(true))
        .await
        .expect("open")
}

/// Writes a batch and flushes it, giving one L0 table.
async fn flush_batch<F: Fs>(db: &roble::Db<F>, batch: &mut Batch) -> u64 {
    db.write(batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table")
}

/// The one table a compaction was expected to write.
fn one_output(outputs: Vec<u64>) -> u64 {
    assert_eq!(
        outputs.len(),
        1,
        "expected one output table, got {outputs:?}"
    );
    outputs[0]
}

/// How many bytes every table of the version takes.
fn table_bytes<F: Fs>(db: &roble::Db<F>) -> u64 {
    db.versions()
        .version
        .levels()
        .flat_map(|(_, tables)| tables.iter().map(|t| t.size))
        .sum()
}

#[compio::test]
async fn a_compaction_changes_no_answer() {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut first = Batch::new();
    for i in 0..8 {
        first
            .put(format!("c{i:02}").as_bytes(), b"first")
            .expect("put");
    }
    let a = flush_batch(&db, &mut first).await;

    let mut second = Batch::new();
    for i in 4..12 {
        second
            .put(format!("c{i:02}").as_bytes(), b"second")
            .expect("put");
    }
    let b = flush_batch(&db, &mut second).await;

    let before = contents(&db).await;
    assert_eq!(
        before.get(b"c05".as_slice()).map(|v| v.as_slice()),
        Some(b"second".as_slice()),
        "the later write should already win"
    );

    let out = one_output(db.compact(0, &[a, b]).await.expect("compact"));

    assert_eq!(
        contents(&db).await,
        before,
        "a compaction changed an answer"
    );
    // The fixture arrived with a table of its own in L0, which this
    // compaction never named; what has to have left is the two inputs.
    let in_l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    assert!(
        !in_l0.contains(&a) && !in_l0.contains(&b),
        "both inputs should have left L0, which still holds {in_l0:?}"
    );
    assert!(
        db.versions()
            .version
            .level(1)
            .iter()
            .any(|t| t.file_num == out),
        "and the output should be in L1"
    );
}

#[compio::test]
async fn a_compaction_drops_what_it_may() {
    // The half a "no answer changed" test cannot see. Four tables holding
    // the same eight keys over and over: the merge has to keep one version
    // of each and throw away the other twenty-four entries.
    let (env, dir) = load("db-twin-a").await;
    let db = open_unscheduled(&env, &dir).await;

    let mut inputs = Vec::new();
    for round in 0..4 {
        let mut batch = Batch::new();
        for i in 0..8 {
            batch
                .put(
                    format!("dup{i}").as_bytes(),
                    format!("round-{round}").as_bytes(),
                )
                .expect("put");
        }
        inputs.push(flush_batch(&db, &mut batch).await);
    }

    let before = contents(&db).await;
    let bytes_before = table_bytes(&db);

    let out = one_output(db.compact(0, &inputs).await.expect("compact"));

    assert_eq!(
        contents(&db).await,
        before,
        "a compaction changed an answer"
    );
    for i in 0..8 {
        assert_eq!(
            db.get(format!("dup{i}").as_bytes(), Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(b"round-3".to_vec()),
            "the newest version should be the one that survived"
        );
    }
    let after = db
        .versions()
        .version
        .level(1)
        .iter()
        .find(|t| t.file_num == out)
        .expect("the output")
        .size;
    assert!(
        after * 2 < bytes_before,
        "four copies of the same keys compacted to {after} bytes from {bytes_before}, \
         which is not a compaction"
    );
}

#[compio::test]
async fn a_deletion_with_nothing_below_it_goes() {
    // A tombstone exists to hide an older version. Once nothing below the
    // output holds the key there is nothing left to hide, and keeping it
    // would cost every later read a comparison for ever.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut written = Batch::new();
    // A second key, so the compaction has something to write and the output
    // table's bounds are a statement about what it holds.
    written.put(b"aaa", b"survivor").expect("put");
    written.put(b"doomed", b"value").expect("put");
    let a = flush_batch(&db, &mut written).await;

    let mut deleted = Batch::new();
    deleted.delete(b"doomed").expect("delete");
    let b = flush_batch(&db, &mut deleted).await;

    assert_eq!(
        db.get(b"doomed", Comparison::Equal).await.expect("get"),
        None
    );

    let out = one_output(db.compact(0, &[a, b]).await.expect("compact"));

    assert_eq!(
        db.get(b"doomed", Comparison::Equal).await.expect("get"),
        None,
        "the key must stay deleted"
    );
    // Both the value and the tombstone went, so the output holds neither.
    let table = db
        .versions()
        .version
        .level(1)
        .iter()
        .find(|t| t.file_num == out)
        .expect("the output")
        .clone();
    assert!(
        !bounds_cover(&table, b"doomed"),
        "the output still holds something for the deleted key: {:02x?}..{:02x?}",
        table.smallest_user_key().expect("smallest"),
        table.largest_user_key().expect("largest")
    );
}

fn bounds_cover(table: &roble::Table, user: &[u8]) -> bool {
    table.smallest_user_key().expect("smallest") <= user
        && user <= table.largest_user_key().expect("largest")
}

#[compio::test]
async fn a_deletion_with_something_below_it_stays() {
    // The other half of the rule, and the half that resurrects keys when it
    // is wrong: with an older version in a level below the output, the
    // tombstone is the only thing keeping it hidden.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // An older version pushed down to L2 first, so the deletion below will
    // be compacted into L1 with something still underneath it.
    let mut old = Batch::new();
    old.put(b"buried", b"old value").expect("put");
    let a = flush_batch(&db, &mut old).await;
    let in_l1 = one_output(db.compact(0, &[a]).await.expect("compact"));
    let in_l2 = one_output(db.compact(1, &[in_l1]).await.expect("compact"));
    assert!(
        db.versions()
            .version
            .level(2)
            .iter()
            .any(|t| t.file_num == in_l2),
        "the old version should be sitting in L2"
    );

    let mut gone = Batch::new();
    gone.delete(b"buried").expect("delete");
    let b = flush_batch(&db, &mut gone).await;

    db.compact(0, &[b]).await.expect("compact");
    assert_eq!(
        db.get(b"buried", Comparison::Equal).await.expect("get"),
        None,
        "the tombstone was dropped and the buried value came back"
    );

    // And it survives a reopen, which is where a lost tombstone shows up.
    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        db.get(b"buried", Comparison::Equal).await.expect("get"),
        None,
    );
}

#[compio::test]
async fn overlapping_range_tombstones_are_fragmented() {
    // Two inputs whose tombstones overlap at neither bound. Concatenating
    // them builds a range-deletion block the format refuses, so the merge
    // has to cut them into fragments.
    let (env, dir) = load("db-twin-a").await;
    let db = open_unscheduled(&env, &dir).await;

    let mut keys = Batch::new();
    for i in 0..12 {
        keys.put(format!("r{i:02}").as_bytes(), b"value")
            .expect("put");
    }
    let a = flush_batch(&db, &mut keys).await;

    let mut first = Batch::new();
    first.delete_range(b"r02", b"r08").expect("delete_range");
    first.put(b"r04", b"back again").expect("put");
    let b = flush_batch(&db, &mut first).await;

    let mut second = Batch::new();
    second.delete_range(b"r05", b"r11").expect("delete_range");
    let c = flush_batch(&db, &mut second).await;

    let before = contents(&db).await;
    assert!(
        before.contains_key(b"r04".as_slice()),
        "the put after the first tombstone should have survived it"
    );
    assert!(
        !before.contains_key(b"r06".as_slice()),
        "and the overlap should be covered"
    );

    db.compact(0, &[a, b, c]).await.expect("compact");
    assert_eq!(
        contents(&db).await,
        before,
        "fragmenting the tombstones changed an answer"
    );

    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        contents(&db).await,
        before,
        "and the fragments should read back the same"
    );
}

#[compio::test]
async fn a_key_a_tombstone_covers_is_dropped() {
    // Keeping a covered key changes no answer — the tombstone goes into the
    // output too, and readers apply it — so nothing a scan or a `get` says
    // can tell the two apart. What can is the size of what was written, and
    // a compaction that carries keys it has already decided are invisible is
    // doing the one job it has badly.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let value = vec![b'v'; 200];
    let mut keys = Batch::new();
    for i in 0..40 {
        keys.put(format!("t{i:02}").as_bytes(), &value)
            .expect("put");
    }
    let a = flush_batch(&db, &mut keys).await;

    let mut sweep = Batch::new();
    sweep.delete_range(b"t00", b"t35").expect("delete_range");
    let b = flush_batch(&db, &mut sweep).await;

    let before = contents(&db).await;
    let covered = (0..35).map(|i| format!("t{i:02}")).count();
    assert_eq!(covered, 35);
    let bytes_before = db
        .versions()
        .version
        .level(0)
        .iter()
        .filter(|t| t.file_num == a || t.file_num == b)
        .map(|t| t.size)
        .sum::<u64>();

    let out = one_output(db.compact(0, &[a, b]).await.expect("compact"));

    assert_eq!(
        contents(&db).await,
        before,
        "a compaction changed an answer"
    );
    let after = db
        .versions()
        .version
        .level(1)
        .iter()
        .find(|t| t.file_num == out)
        .expect("the output")
        .size;
    assert!(
        after * 3 < bytes_before,
        "thirty-five of forty keys were covered, but the output is {after} bytes \
         against {bytes_before} in: the covered keys were carried along"
    );
}

#[compio::test]
async fn a_compaction_that_drops_everything_writes_no_table() {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // A key written and deleted, with nothing below: both go.
    let mut batch = Batch::new();
    batch.put(b"ephemeral", b"value").expect("put");
    batch.delete(b"ephemeral").expect("delete");
    let only = flush_batch(&db, &mut batch).await;

    let before = contents(&db).await;
    assert!(
        db.compact(0, &[only]).await.expect("compact").is_empty(),
        "nothing survived, so there is no table to write"
    );
    assert_eq!(contents(&db).await, before);
    let in_l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    assert!(
        !in_l0.contains(&only),
        "the input should be gone: L0 holds {in_l0:?}"
    );
    assert!(
        db.versions().version.level(1).is_empty(),
        "and nothing should have been put in its place"
    );
}

#[compio::test]
async fn a_compaction_collects_the_tables_it_replaced() {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut batch = Batch::new();
    batch.put(b"collected", b"yes").expect("put");
    let a = flush_batch(&db, &mut batch).await;

    let out = one_output(db.compact(0, &[a]).await.expect("compact"));

    let names: Vec<String> = env
        .sim()
        .list(&dir)
        .await
        .expect("list")
        .iter()
        .map(|e| e.name.to_string_lossy().into_owned())
        .collect();
    let input = roble_format::files::table_filename(a);
    let output = roble_format::files::table_filename(out);
    assert!(
        !names.contains(&input),
        "the input table should have been collected: {names:?}"
    );
    assert!(
        names.contains(&output),
        "and the output should be there: {names:?}"
    );
}

#[compio::test]
async fn a_compaction_pulls_in_the_tables_it_would_overlap() {
    // Pebble leaves no overlap below L0, so a table of L1 whose range meets
    // the inputs' has to be rewritten with them rather than left beside the
    // output.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut low = Batch::new();
    for i in 0..6 {
        low.put(format!("o{i:02}").as_bytes(), b"first")
            .expect("put");
    }
    let a = flush_batch(&db, &mut low).await;
    let settled = one_output(db.compact(0, &[a]).await.expect("compact"));

    let mut overlapping = Batch::new();
    for i in 3..9 {
        overlapping
            .put(format!("o{i:02}").as_bytes(), b"second")
            .expect("put");
    }
    let b = flush_batch(&db, &mut overlapping).await;

    let before = contents(&db).await;
    db.compact(0, &[b]).await.expect("compact");

    assert_eq!(
        contents(&db).await,
        before,
        "a compaction changed an answer"
    );
    assert!(
        !db.versions()
            .version
            .level(1)
            .iter()
            .any(|t| t.file_num == settled),
        "the overlapping L1 table should have been rewritten, not left beside the output"
    );
    assert_eq!(
        db.versions().version.level(1).len(),
        1,
        "L1 should hold one table: {:?}",
        db.versions()
            .version
            .level(1)
            .iter()
            .map(|t| t.file_num)
            .collect::<Vec<_>>()
    );
}

#[compio::test]
async fn a_compaction_survives_a_reopen() {
    let (env, dir) = load("db-twin-a").await;
    let expected;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        let mut first = Batch::new();
        for i in 0..6 {
            first
                .put(format!("s{i:02}").as_bytes(), b"one")
                .expect("put");
        }
        let a = flush_batch(&db, &mut first).await;

        let mut second = Batch::new();
        for i in 3..9 {
            second
                .put(format!("s{i:02}").as_bytes(), b"two")
                .expect("put");
        }
        second.delete(b"s01").expect("delete");
        let b = flush_batch(&db, &mut second).await;

        db.compact(0, &[a, b]).await.expect("compact");
        expected = contents(&db).await;
    }

    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        contents(&db).await,
        expected,
        "the compaction was recorded, so a reopen should find the same database"
    );
}

#[compio::test]
async fn a_scan_open_across_a_compaction_keeps_reading() {
    // Why a compaction may drop every version but the newest without
    // consulting a list of registered snapshots: a scan holds the version it
    // was made on, so it goes on reading those tables whatever the
    // compaction writes and the sweep afterwards deletes. If that were not
    // so, dropping superseded versions would change an open scan's answers
    // underneath it.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut first = Batch::new();
    for i in 0..8 {
        first
            .put(format!("w{i:02}").as_bytes(), b"one")
            .expect("put");
    }
    let a = flush_batch(&db, &mut first).await;

    let mut second = Batch::new();
    for i in 0..8 {
        second
            .put(format!("w{i:02}").as_bytes(), b"two")
            .expect("put");
    }
    let b = flush_batch(&db, &mut second).await;

    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    let started_on = scan.key().to_vec();

    db.compact(0, &[a, b]).await.expect("compact");

    // The scan walks to the end on tables the compaction has replaced and
    // the sweep has deleted.
    let mut seen = Model::new();
    while scan.is_valid() {
        seen.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    assert!(
        seen.contains_key(&started_on),
        "the scan lost the key it was standing on"
    );
    for i in 0..8 {
        assert_eq!(
            seen.get(format!("w{i:02}").as_bytes())
                .map(|v| v.as_slice()),
            Some(b"two".as_slice()),
            "the scan should still read every key it was opened over"
        );
    }
}

/// A scan that outlives a compaction reads the tables it had not yet opened
/// when the compaction retired them.
///
/// The test above would pass on open readers alone: its two L0 tables are
/// both opened by the scan's first seek. A level below L0 opens one table at
/// a time, as the scan reaches it (`roble::scan`), so here the scan stands on
/// the first table of a level of several when a compaction moves the whole
/// level down and the sweep runs. Every later table of the level is then one
/// nothing has open, and the only thing keeping it on disk is the version
/// the scan holds (`roble::obsolete`). The second half says that was what
/// kept them: once the scan has gone, the next sweep collects every one.
#[compio::test]
async fn a_scan_reads_the_tables_it_had_not_opened_when_a_compaction_retired_them() {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .target_file_size(4096)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open");

    let value = vec![b'v'; 200];
    let mut batch = Batch::new();
    for i in 0..400u64 {
        batch
            .put(format!("k{i:04}").as_bytes(), &value)
            .expect("put");
    }
    let table = flush_batch(&db, &mut batch).await;
    db.compact(0, &[table]).await.expect("compact into L1");
    let level: Vec<u64> = db
        .versions()
        .version
        .level(1)
        .iter()
        .map(|t| t.file_num)
        .collect();
    assert!(
        level.len() > 2,
        "L1 has to hold tables past the one a scan starts on, holds {}",
        level.len()
    );
    let before = contents(&db).await;

    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");

    db.compact(1, &level).await.expect("compact L1 into L2");
    assert!(
        db.versions().version.level(1).is_empty(),
        "the whole of L1 went into the compaction"
    );

    let mut seen = Model::new();
    while scan.is_valid() {
        seen.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    assert_eq!(
        seen, before,
        "the scan should read the store it was made on to the end"
    );

    drop(scan);
    let removed = db.remove_obsolete_files().await.expect("sweep");
    for file_num in &level {
        let name = roble_format::files::table_filename(*file_num);
        assert!(
            removed.contains(&name),
            "{name} should have been kept for the scan alone, and collected once it \
             went: {removed:?}"
        );
    }
}

/// An output table written in several chunks reads back whole.
///
/// A merge hands its output's bytes to the disk as the writer finishes each
/// block, so a table larger than the megabyte a write gathers is several
/// writes at increasing offsets rather than one at zero (§6,
/// `roble::compact`). Every offset in a table — a data block's handle in the
/// index, the index's and the metaindex's in the footer — is an offset in the
/// *file*, so a merge that lost count of what it had already written would
/// produce a table unreadable past its first chunk.
///
/// Nothing else here notices. Every other target in these tests is
/// kilobytes, so every other output is written in the single write `finish`
/// makes and the handles come out the same either way. Hence a test that is
/// large on purpose, and that checks an output really did take more than one
/// write before believing what it read.
#[compio::test]
async fn a_chunked_compaction_output_reads_back_whole() {
    /// What a write gathers before making it, in `roble::flush`.
    const CHUNK: u64 = 1 << 20;
    const RECORDS: usize = 32 * 1024;

    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            // L1's target is twice this: 4 MiB, so an output spans four
            // chunks where every other test's spans none.
            .target_file_size(2 << 20)
            // Larger than the job, so the write does not flush part way
            // through and leave two L0 tables.
            .memtable_size(64 << 20),
    )
    .await
    .expect("create");

    let value = vec![b'v'; 256];
    let mut batch = Batch::new();
    for i in 0..RECORDS {
        batch
            .put(format!("q{i:08}").as_bytes(), &value)
            .expect("put");
    }
    let a = flush_batch(&db, &mut batch).await;
    drop(batch);

    let before = contents(&db).await;
    assert_eq!(before.len(), RECORDS, "every record should be in the store");

    let outputs = db.compact(0, &[a]).await.expect("compact");
    let largest = db
        .versions()
        .version
        .level(1)
        .iter()
        .map(|t| t.size)
        .max()
        .expect("an output table");
    assert!(
        outputs.len() > 1,
        "this test wants several outputs, got {outputs:?}"
    );
    assert!(
        largest > CHUNK,
        "this test is only about an output of several chunks, and {largest} bytes is not one"
    );

    // The memtable was emptied by the flush and the inputs are gone, so this
    // reads the merge's own tables — at the offsets its chunked writes put
    // them.
    assert_eq!(
        contents(&db).await,
        before,
        "a chunked compaction changed an answer"
    );
    for key in [0, RECORDS / 2, RECORDS - 1] {
        let key = format!("q{key:08}");
        assert_eq!(
            db.get(key.as_bytes(), Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(value.clone()),
            "{key} should be readable from the merged table"
        );
    }
}

/// Opens with a target file size small enough that a test can cross it.
async fn open_splitting(env: &SimEnv, dir: &Path, target: u64) -> roble::Db<SimFs> {
    roble::open_with(env, dir, roble::Options::new().target_file_size(target))
        .await
        .expect("open")
}

#[compio::test]
async fn a_compaction_splits_its_output_at_the_target_size() {
    let (env, dir) = load("db-twin-a").await;
    let db = open_splitting(&env, &dir, 4096).await;

    let value = vec![b'v'; 200];
    let mut batch = Batch::new();
    for i in 0..200 {
        batch
            .put(format!("p{i:04}").as_bytes(), &value)
            .expect("put");
    }
    let a = flush_batch(&db, &mut batch).await;

    let before = contents(&db).await;
    let outputs = db.compact(0, &[a]).await.expect("compact");
    assert!(
        outputs.len() > 3,
        "40 KiB of keys against a 4 KiB target should be several tables, got {outputs:?}"
    );
    assert_eq!(contents(&db).await, before, "splitting changed an answer");
    assert!(
        db.versions().next_file_num > *outputs.iter().max().expect("an output"),
        "the edit has to spend a file number for every output, or the next \
         table written takes a name one of these already has"
    );
    // Each level below aims at twice the one above: deeper levels hold more
    // and are rewritten less often, so larger files there cost fewer of them.
    assert_eq!(
        db.target_file_size(2),
        db.target_file_size(1) * 2,
        "the target should double with depth"
    );
    assert_eq!(db.target_file_size(0), 4096);

    // In key order, and every table but the last at least the target.
    let in_l1: Vec<roble::Table> = db.versions().version.level(1).to_vec();
    assert_eq!(
        in_l1.iter().map(|t| t.file_num).collect::<Vec<_>>(),
        outputs,
        "the version should hold the outputs, in the order they were written"
    );
    for pair in in_l1.windows(2) {
        assert!(
            pair[0].largest_user_key().expect("largest")
                < pair[1].smallest_user_key().expect("smallest"),
            "tables {} and {} are not in key order",
            pair[0].file_num,
            pair[1].file_num
        );
    }
    for t in &in_l1[..in_l1.len() - 1] {
        assert!(
            t.size >= 2048,
            "table {} is {} bytes against a 4096 target, so the split is not the target's",
            t.file_num,
            t.size
        );
    }

    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        contents(&db).await,
        before,
        "and it should read back the same"
    );
}

#[compio::test]
async fn a_tombstone_outside_a_table_is_not_written_into_it() {
    // A tombstone over a narrow range, and keys spread far wider: most of
    // the outputs lie entirely outside it, and clipping leaves them nothing
    // at all. Writing that nothing is a range deletion whose start is not
    // below its end, which the block builder refuses outright.
    let (env, dir) = load("db-twin-a").await;
    let db = open_splitting(&env, &dir, 2048).await;

    let mut below = Batch::new();
    for i in 0..200 {
        below
            .put(format!("n{i:04}").as_bytes(), b"old")
            .expect("put");
    }
    let deep = flush_batch(&db, &mut below).await;
    db.compact(0, &[deep]).await.expect("compact");

    let value = vec![b'v'; 200];
    let mut batch = Batch::new();
    batch
        .delete_range(b"n0100", b"n0102")
        .expect("delete_range");
    for i in 0..200 {
        batch
            .put(format!("n{i:04}").as_bytes(), &value)
            .expect("put");
    }
    let a = flush_batch(&db, &mut batch).await;

    let before = contents(&db).await;
    let outputs = db.compact(0, &[a]).await.expect("compact");
    assert!(
        outputs.len() > 4,
        "it should have split well past the tombstone: {outputs:?}"
    );
    assert_eq!(contents(&db).await, before, "splitting changed an answer");

    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(contents(&db).await, before);
}

#[compio::test]
async fn a_tombstone_across_a_split_leaves_tables_that_touch() {
    // The shape this whole change turns on. A range deletion that spans a
    // split is clipped at the boundary, so the table before it ends at the
    // exclusive sentinel — the end key at the largest sequence number there
    // is — and the table after begins at that very user key. The two meet at
    // one key without overlapping, and a check that reads the bound as
    // inclusive refuses a database Pebble itself writes.
    let (env, dir) = load("db-twin-a").await;
    let db = open_splitting(&env, &dir, 4096).await;

    // Something below, so the tombstone is not elided for having nothing
    // left to hide.
    let mut old = Batch::new();
    for i in 0..20 {
        old.put(format!("q{i:04}").as_bytes(), b"old").expect("put");
    }
    let deep = flush_batch(&db, &mut old).await;
    for level in 0..3 {
        let tables: Vec<u64> = db
            .versions()
            .version
            .level(level)
            .iter()
            .filter(|t| level > 0 || t.file_num == deep)
            .map(|t| t.file_num)
            .collect();
        if !tables.is_empty() {
            db.compact(level, &tables).await.expect("compact");
        }
    }

    // The tombstone, then keys newer than it inside its range, so there is
    // something left to split among.
    let mut sweep = Batch::new();
    sweep
        .delete_range(b"q0000", b"q9999")
        .expect("delete_range");
    let b = flush_batch(&db, &mut sweep).await;

    let value = vec![b'v'; 200];
    let mut fresh = Batch::new();
    for i in 0..200 {
        fresh
            .put(format!("q{i:04}").as_bytes(), &value)
            .expect("put");
    }
    let c = flush_batch(&db, &mut fresh).await;

    let before = contents(&db).await;
    let outputs = db.compact(0, &[b, c]).await.expect("compact");
    assert!(outputs.len() > 1, "it should have split: {outputs:?}");

    let in_l1: Vec<roble::Table> = db.versions().version.level(1).to_vec();
    let mut touching = 0;
    for pair in in_l1.windows(2) {
        let (before_t, after_t) = (&pair[0], &pair[1]);
        if before_t.largest_user_key().expect("largest")
            == after_t.smallest_user_key().expect("smallest")
        {
            touching += 1;
            assert!(
                before_t.largest_is_exclusive().expect("exclusive"),
                "tables {} and {} share a bound that is not an exclusive sentinel",
                before_t.file_num,
                after_t.file_num
            );
        }
    }
    assert!(
        touching > 0,
        "no two outputs met at a key, so nothing here tests the rule: {:?}",
        in_l1.iter().map(|t| t.file_num).collect::<Vec<_>>()
    );

    assert_eq!(contents(&db).await, before, "splitting changed an answer");
    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        contents(&db).await,
        before,
        "and the version with touching bounds should replay"
    );
}

#[compio::test]
async fn a_compaction_refuses_what_it_cannot_do() {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    assert!(
        db.compact(0, &[]).await.is_err(),
        "a compaction of no tables has nothing to do"
    );
    assert!(
        db.compact(0, &[999]).await.is_err(),
        "L0 does not hold table 999"
    );
    assert!(
        db.compact(6, &[1]).await.is_err(),
        "there is nowhere below the last level"
    );
}

/// Two levels of several tables each, their ranges interleaved, read every
/// way.
///
/// The shape a deep store settles into, and the one nothing else here builds:
/// L1 and L2 both holding several tables whose keys alternate, so a read has
/// to merge two levels that each need a seek of their own. A scan reads a
/// level as one source (`crate::scan`), which is sound because the tables of
/// a level cannot reach the same key — and a source that reached past its own
/// level would be reading tables in no key order at all. Nothing catches that
/// until two levels hold several tables at once, which until this test
/// nothing did.
#[compio::test]
async fn two_levels_of_several_tables_each_answer_what_a_btreemap_would() {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .target_file_size(4096)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open");

    // The even keys pushed down to L2 and the odd ones left in L1, so the two
    // levels interleave rather than sitting one after the other: a read that
    // mistook them for one run would be answering out of order at once.
    let value = vec![b'v'; 200];
    let mut model = contents(&db).await;
    for parity in [0u64, 1] {
        let mut batch = Batch::new();
        for i in (parity..400).step_by(2) {
            let key = format!("p{i:04}");
            batch.put(key.as_bytes(), &value).expect("put");
            model.insert(key.into_bytes(), value.clone());
        }
        let table = flush_batch(&db, &mut batch).await;
        let into_l1 = db.compact(0, &[table]).await.expect("compact into L1");
        if parity == 0 {
            db.compact(1, &into_l1).await.expect("compact into L2");
        }
    }
    for level in [1, 2] {
        let held = db.versions().version.level(level).len();
        assert!(
            held > 1,
            "L{level} has to hold several tables for this to be testing anything, holds {held}"
        );
    }

    assert_eq!(contents(&db).await, model, "a forward scan lost its way");

    let mut backward = Model::new();
    let mut scan = db.scan().await.expect("scan");
    scan.last().await.expect("last");
    while scan.is_valid() {
        backward.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.prev().await.expect("prev");
    }
    assert_eq!(backward, model, "a reverse scan lost its way");

    // Every comparison, at the ends of the range and at keys either side of a
    // table boundary — which is wherever one level's key sits next to the
    // other's, since they alternate.
    for probe in [
        "o", "p0000", "p0001", "p0002", "p0199", "p0200", "p0399", "q",
    ] {
        for how in [
            Comparison::Equal,
            Comparison::Floor,
            Comparison::Ceiling,
            Comparison::Lower,
            Comparison::Higher,
        ] {
            let p = probe.as_bytes().to_vec();
            let pair = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
            let want = match how {
                Comparison::Equal => model.get(&p).map(|v| (p.clone(), v.clone())),
                Comparison::Floor => model.range(..=p.clone()).next_back().map(pair),
                Comparison::Ceiling => model.range(p.clone()..).next().map(pair),
                Comparison::Lower => model.range(..p.clone()).next_back().map(pair),
                Comparison::Higher => model
                    .range((
                        std::ops::Bound::Excluded(p.clone()),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
                    .map(pair),
            };
            let found = db
                .get(&p, how)
                .await
                .expect("get")
                .map(|f| (f.key, f.value));
            assert_eq!(found, want, "{how:?} of {probe}");
        }
    }
}

/// Every key, and every gap between keys, against a level deep enough that a
/// lookup has to search it rather than walk it.
///
/// A level below L0 holds disjoint ranges in key order, so a lookup finds its
/// table with a binary search (`read::table_containing`). The way that goes
/// wrong is at a boundary: one off-by-one and a probe lands on the table
/// beside the right one, which still answers — with the wrong record, or with
/// none at all. A handful of hand-picked probes can miss that; every key and
/// both sides of it cannot.
///
/// The level has to be deep enough for the search to have somewhere to go
/// wrong, which is what the assertion on the table count is for.
#[compio::test]
async fn every_key_and_every_gap_find_their_table_in_a_deep_level() {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .target_file_size(4096)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open");

    let value = vec![b'v'; 200];
    let mut model = contents(&db).await;
    let mut batch = Batch::new();
    for i in 0..400u64 {
        let key = format!("k{i:04}");
        batch.put(key.as_bytes(), &value).expect("put");
        model.insert(key.into_bytes(), value.clone());
    }
    let table = flush_batch(&db, &mut batch).await;
    db.compact(0, &[table]).await.expect("compact into L1");

    let held = db.versions().version.level(1).len();
    assert!(
        held > 4,
        "L1 has to hold several tables for a search to be testing anything, holds {held}"
    );

    // The level's tables are in key order, which is what the search assumes.
    let level = db.versions().version.level(1).to_vec();
    for pair in level.windows(2) {
        assert!(
            pair[0].largest_user_key().expect("largest")
                < pair[1].smallest_user_key().expect("smallest"),
            "L1 tables {} and {} are out of key order",
            pair[0].file_num,
            pair[1].file_num
        );
    }

    // Every stored key, and a probe either side of it: `...a` sorts just
    // below the next key and just above this one, so between any two
    // neighbours there is a probe that belongs to neither.
    let mut probes: Vec<Vec<u8>> = vec![b"j".to_vec(), b"l".to_vec()];
    for key in model.keys() {
        probes.push(key.clone());
        let mut above = key.clone();
        above.push(b'a');
        probes.push(above);
    }

    for probe in &probes {
        for how in [
            Comparison::Equal,
            Comparison::Floor,
            Comparison::Ceiling,
            Comparison::Lower,
            Comparison::Higher,
        ] {
            let pair = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
            let p = probe.clone();
            let want = match how {
                Comparison::Equal => model.get(&p).map(|v| (p.clone(), v.clone())),
                Comparison::Floor => model.range(..=p.clone()).next_back().map(pair),
                Comparison::Ceiling => model.range(p.clone()..).next().map(pair),
                Comparison::Lower => model.range(..p.clone()).next_back().map(pair),
                Comparison::Higher => model
                    .range((
                        std::ops::Bound::Excluded(p.clone()),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
                    .map(pair),
            };
            let found = db
                .get(&p, how)
                .await
                .expect("get")
                .map(|f| (f.key, f.value));
            assert_eq!(found, want, "{how:?} of {}", String::from_utf8_lossy(probe));
        }
    }
}

/// A backward comparison whose probe lands in a table with nothing visible
/// left in it, so its answer is in the table before — once with every key in
/// it deleted one by one, once with a range deletion over all of them.
///
/// `floor` and `lower` seek backward once and step back over what is
/// deleted, and a level opens the table before only when the merge reaches
/// that table's bound (`roble::scan`). Here every key of one L1 table is
/// deleted, so every probe that lands in it has to step back across the
/// whole table and into the one before, where the answer is.
///
/// The deletions are flushed rather than left in the memtable, which every
/// scan reads anyway, and flushed as two L0 tables with the higher keys
/// first. That puts L0's file order against its key order, so a scan that
/// read L0 as it reads the levels below — as if it were in key order — would
/// miss one of the two, and answer with keys it deletes. This was written
/// for a window `floor` once tried first, which had the same two ways to go
/// wrong.
#[compio::test]
async fn a_floor_in_a_deleted_table_finds_its_answer_below() {
    for ranged in [false, true] {
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(
            &env,
            &dir,
            Options::new()
                .target_file_size(4096)
                .disable_automatic_compactions(true),
        )
        .await
        .expect("open");

        let value = vec![b'v'; 200];
        let mut model = contents(&db).await;
        let mut batch = Batch::new();
        for i in 0..400u64 {
            let key = format!("k{i:04}");
            batch.put(key.as_bytes(), &value).expect("put");
            model.insert(key.into_bytes(), value.clone());
        }
        let table = flush_batch(&db, &mut batch).await;
        db.compact(0, &[table]).await.expect("compact into L1");

        // A table from the middle of the level, so there is one before it
        // for the answer to come from.
        let level = db.versions().version.level(1).to_vec();
        assert!(
            level.len() > 4,
            "L1 has to hold several tables for one to be in the middle, holds {}",
            level.len()
        );
        let emptied = &level[level.len() / 2];
        let first = emptied.smallest_user_key().expect("smallest").to_vec();
        let last = emptied.largest_user_key().expect("largest").to_vec();
        let doomed: Vec<Vec<u8>> = model
            .range(first.clone()..=last.clone())
            .map(|(k, _)| k.clone())
            .collect();
        assert!(
            doomed.len() > 2,
            "the emptied table has to hold keys to probe, holds {}",
            doomed.len()
        );

        // The upper half first, so it is the older of the two L0 tables. The
        // fixture brings an L0 table of its own, hence counting from it.
        let in_l0 = db.versions().version.level(0).len();
        let middle = doomed[doomed.len() / 2].clone();
        for (from, to) in [(middle.clone(), last.clone()), (first.clone(), middle)] {
            let mut batch = Batch::new();
            if ranged {
                // Ending just past `to`, or at `to` itself where the upper
                // half begins: the two halves meet without overlapping.
                let mut end = to.clone();
                if to == last {
                    end.push(0);
                }
                batch.delete_range(&from, &end).expect("delete range");
            } else {
                for key in doomed
                    .iter()
                    .filter(|k| from <= **k && (**k < to || to == last))
                {
                    batch.delete(key).expect("delete");
                }
            }
            flush_batch(&db, &mut batch).await;
        }
        assert_eq!(
            db.versions().version.level(0).len(),
            in_l0 + 2,
            "the deletions as two L0 tables"
        );
        for key in &doomed {
            model.remove(key);
        }

        // Every deleted key and a probe just above each, all of them inside
        // the emptied table's range.
        let mut probes = Vec::new();
        for key in &doomed {
            probes.push(key.clone());
            let mut above = key.clone();
            above.push(b'a');
            probes.push(above);
        }
        for probe in &probes {
            for how in [Comparison::Floor, Comparison::Lower] {
                let want = match how {
                    Comparison::Floor => model.range(..=probe.clone()).next_back(),
                    _ => model.range(..probe.clone()).next_back(),
                }
                .map(|(k, v)| (k.clone(), v.clone()));
                assert!(
                    want.as_ref().is_some_and(|(k, _)| *k < first),
                    "the answer has to be below the emptied table for the test to mean anything"
                );
                let found = db
                    .get(probe, how)
                    .await
                    .expect("get")
                    .map(|f| (f.key, f.value));
                assert_eq!(
                    found,
                    want,
                    "{how:?} of {} with the table {}",
                    String::from_utf8_lossy(probe),
                    if ranged {
                        "range-deleted"
                    } else {
                        "deleted key by key"
                    }
                );
            }
        }
    }
}

/// A merge whose inputs interleave and repeat the same user keys, against a
/// `BTreeMap` of what was written.
///
/// The merge picks the least key with a heap (`compact::Heap`), and what a
/// heap can do that the scan before it could not is pick a *different* one of
/// two equal keys: it is not stable, and `least` left the first input holding
/// the least key on top because it only ever replaced its best on a strictly
/// smaller one. That matters because dropping superseded versions depends on
/// the newest version of a user key arriving first — so a merge that ordered
/// two versions the other way round would keep the older value and drop the
/// newer, and the store would answer with it.
///
/// So the inputs are built to make both halves of the order load-bearing.
/// Every table owns one residue class of the keyspace, which puts the least
/// key in a different input at nearly every step and keeps the heap's top
/// moving; and every table writes the multiples of sixteen again, so those
/// user keys reach the merge in as many versions as there are inputs, with a
/// value that says which batch wrote it. A heap that yielded versions out of
/// order would answer with the wrong batch's value, which no amount of
/// key-order checking would catch.
///
/// The job is made wide on purpose — the whole of L0 plus the whole
/// overlapping run of L1 — because a heap over two inputs is a heap that
/// barely sifts.
#[compio::test]
async fn a_merge_of_interleaved_and_repeated_keys_answers_what_a_btreemap_would() {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .target_file_size(4096)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open");

    let mut model = contents(&db).await;
    let mut batches = 0u64;
    // Two rounds, so the second merge reads the run the first one left in L1
    // alongside a fresh L0: a compaction's inputs are not all one level's.
    for round in 0..2u64 {
        let mut tables = Vec::new();
        for slot in 0..8u64 {
            let b = batches;
            batches += 1;
            let mut batch = Batch::new();
            // This table's own keys, and then the keys every table writes.
            let mut keys: Vec<u64> = (0..400).filter(|i| i % 8 == slot).collect();
            keys.extend((0..400).step_by(16));
            keys.sort_unstable();
            keys.dedup();
            for i in keys {
                let key = format!("p{i:04}");
                let value = format!("b{b:02}-p{i:04}-{}", "v".repeat(60));
                batch.put(key.as_bytes(), value.as_bytes()).expect("put");
                model.insert(key.into_bytes(), value.into_bytes());
            }
            // A deletion whose key another table in the same job still holds,
            // so the merge has to drop what is under it rather than beside
            // it, and a span of them on the last batch of the round.
            let gone = format!("p{:04}", slot * 8 + 3);
            batch.delete(gone.as_bytes()).expect("delete");
            model.remove(gone.as_bytes());
            if slot == 7 {
                let (lo, hi) = (
                    format!("p{:04}", 100 + round * 20),
                    format!("p{:04}", 112 + round * 20),
                );
                batch
                    .delete_range(lo.as_bytes(), hi.as_bytes())
                    .expect("delete range");
                model.retain(|k, _| k.as_slice() < lo.as_bytes() || k.as_slice() >= hi.as_bytes());
            }
            tables.push(flush_batch(&db, &mut batch).await);
        }

        // Every L0 table and the whole L1 run it meets, which is the job the
        // picker makes of L0 and the only one here with a wide merge in it.
        let inputs = tables.len() + db.versions().version.level(1).len();
        assert!(
            inputs >= 8,
            "round {round} has to merge at least eight inputs to be testing a heap, merges {inputs}"
        );
        if round == 1 {
            assert!(
                db.versions().version.level(1).len() > 1,
                "the second round has to read an L1 run of several tables, reads {}",
                db.versions().version.level(1).len()
            );
        }
        let outputs = db.compact(0, &tables).await.expect("compact into L1");
        assert!(
            outputs.len() > 1,
            "round {round} has to split its output for the boundary to be tested, wrote {}",
            outputs.len()
        );
    }

    assert_eq!(
        contents(&db).await,
        model,
        "the merge kept the wrong version of a key, or lost one"
    );

    // The output is one run in key order. A merge that yielded two keys the
    // wrong way round could still have been read back in order by a scan
    // that merged the tables again, so the level is checked as well as the
    // answer. Two outputs may meet at one user key, which is a clipped
    // tombstone's exclusive sentinel and not an overlap
    // (`a_tombstone_across_a_split_leaves_tables_that_touch`).
    let level = db.versions().version.level(1).to_vec();
    for pair in level.windows(2) {
        let (lhs, rhs) = (&pair[0], &pair[1]);
        let (largest, smallest) = (
            lhs.largest_user_key().expect("largest"),
            rhs.smallest_user_key().expect("smallest"),
        );
        assert!(
            largest <= smallest,
            "L1 tables {} and {} overlap or are out of order",
            lhs.file_num,
            rhs.file_num
        );
        assert!(
            largest < smallest || lhs.largest_is_exclusive().expect("exclusive"),
            "L1 tables {} and {} share a bound that is not an exclusive sentinel",
            lhs.file_num,
            rhs.file_num
        );
    }
}
