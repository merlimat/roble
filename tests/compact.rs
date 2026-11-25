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
use std::ops::Bound;
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
    let mut scan = db.range(..).expect("scan");
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
    db.write_raw(batch, true).await.expect("write");
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

/// A new store, with compaction left to the caller.
async fn create_unscheduled(env: &SimEnv, dir: &Path) -> roble::Db<SimFs> {
    roble::open_with(
        env,
        dir,
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("create")
}

/// The file numbers a level holds, in its order.
fn level<F: Fs>(db: &roble::Db<F>, level: u32) -> Vec<u64> {
    db.versions()
        .version
        .level(level)
        .iter()
        .map(|t| t.file_num)
        .collect()
}

#[compio::test]
async fn part_of_l0_does_not_leave_without_an_older_table_it_overlaps() {
    // RocksDB's `IntraL0CompactionDoesNotObsoleteDeletions`
    // (facebook/rocksdb#2722), as a pick of part of L0. The older version is beside the deletion in L0
    // rather than below it, so a compaction of the deletion alone into L1
    // found nothing below, dropped it, and the older version came back.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = create_unscheduled(&env, &dir).await;

    let mut old = Batch::new();
    old.put(b"k", b"old value").expect("put");
    let a = flush_batch(&db, &mut old).await;
    let mut gone = Batch::new();
    gone.delete(b"k").expect("delete");
    let b = flush_batch(&db, &mut gone).await;

    let refused = db
        .compact(0, &[b])
        .await
        .expect_err("the deletion cannot leave the older version above it");
    assert!(
        refused
            .to_string()
            .contains(&format!("L0 table {a} overlaps {b}")),
        "the refusal should name the table left behind: {refused}"
    );
    assert_eq!(level(&db, 0), [a, b], "a refused pick moves nothing");
    assert_eq!(db.get(b"k", Comparison::Equal).await.expect("get"), None);

    // The older table may go alone: what stays in L0 is newer than it.
    db.compact(0, &[a]).await.expect("compact the older table");
    assert_eq!(level(&db, 0), [b]);
    assert_eq!(db.get(b"k", Comparison::Equal).await.expect("get"), None);

    // Then the deletion, which meets the older version in L1.
    db.compact(0, &[b]).await.expect("compact the deletion");
    assert!(level(&db, 0).is_empty());
    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(db.get(b"k", Comparison::Equal).await.expect("get"), None);
}

#[compio::test]
async fn a_scan_and_get_agree_on_a_key_written_after_a_range_deletion() {
    // The same pick with a range deletion as the older table, carried on to
    // where a scan shows it. The key written after the deletion used to go
    // down to L1 alone, and then on to L2. The deletion followed it into L1
    // and sat above it. A scan that meets a key the deletion hides in a
    // deeper level passes that level's keys to the end of the span without
    // asking their age, so it passed the newer key too. `get` compares
    // sequence numbers and found it.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = create_unscheduled(&env, &dir).await;

    // In L2 under the span, older than the deletion: what sets a scan
    // skipping.
    let mut buried = Batch::new();
    buried.put(b"b", b"buried").expect("put");
    let deep = flush_batch(&db, &mut buried).await;
    let in_l1 = one_output(db.compact(0, &[deep]).await.expect("compact"));
    db.compact(1, &[in_l1]).await.expect("compact");

    let mut sweep = Batch::new();
    sweep.delete_range(b"a", b"z").expect("delete_range");
    flush_batch(&db, &mut sweep).await;
    let mut after = Batch::new();
    after.put(b"k", b"written after").expect("put");
    let newer = flush_batch(&db, &mut after).await;

    // Refused now, which is the fix: it once took `k` down alone.
    let _ = db.compact(0, &[newer]).await;
    let l1 = level(&db, 1);
    if !l1.is_empty() {
        db.compact(1, &l1).await.expect("compact L1");
    }
    db.compact(0, &level(&db, 0)).await.expect("compact L0");

    let found = db.get(b"k", Comparison::Equal).await.expect("get");
    assert_eq!(found.map(|f| f.value), Some(b"written after".to_vec()));
    assert_eq!(
        contents(&db).await,
        Model::from([(b"k".to_vec(), b"written after".to_vec())]),
        "a scan should find what get finds, and not the buried key"
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

    let mut scan = db.range(..).expect("scan");
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

    let mut scan = db.range(..).expect("scan");
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
            .memtable_size(64 << 20)
            // Values of one repeated byte, which compressed would fit the
            // whole job in a single chunk.
            .compression(roble::Codec::None),
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
///
/// Uncompressed, because these targets are smaller than a block: the split
/// counts the open block as built, so a compressed table would close at the
/// target and then shrink by whatever its one block compressed by.
/// `a_compressed_output_is_split_at_its_stored_size` is the compressed case,
/// at a target a block is small beside.
async fn open_splitting(env: &SimEnv, dir: &Path, target: u64) -> roble::Db<SimFs> {
    roble::open_with(
        env,
        dir,
        roble::Options::new()
            .target_file_size(target)
            .compression(roble::Codec::None),
    )
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
async fn a_get_at_the_key_a_split_tombstone_ends_at_reads_the_table_that_starts_there() {
    // The tables a split inside a range deletion leaves (the test below)
    // meet at one user key: the first ends at the exclusive sentinel there,
    // the second starts there, often with a live key. A lookup has to read
    // the second. It read the first, whose largest bound it took to reach the
    // key, found nothing, and answered from the level below: the key's older
    // version, deleted and written again since. A scan got it right, which
    // is all `contents` checks, so every comparison is asked of every key.
    let (env, dir) = load("db-twin-a").await;
    let db = open_splitting(&env, &dir, 4096).await;

    // Older versions below, then a tombstone over them and newer versions on
    // top, merged together so the output splits with the tombstone across
    // every boundary.
    let mut old = Batch::new();
    for i in 0..120 {
        old.put(format!("s{i:04}").as_bytes(), b"old").expect("put");
    }
    let deep = flush_batch(&db, &mut old).await;
    let l1 = db.compact(0, &[deep]).await.expect("compact");
    db.compact(1, &l1).await.expect("compact");

    let value = vec![b'v'; 200];
    let mut fresh = Batch::new();
    fresh
        .delete_range(b"s0000", b"s9999")
        .expect("delete_range");
    for i in 0..120 {
        fresh
            .put(format!("s{i:04}").as_bytes(), &value)
            .expect("put");
    }
    let top = flush_batch(&db, &mut fresh).await;
    let outputs = db.compact(0, &[top]).await.expect("compact");
    let in_l1: Vec<roble::Table> = db.versions().version.level(1).to_vec();
    let starts: Vec<Vec<u8>> = in_l1
        .windows(2)
        .filter(|p| p[0].largest_is_exclusive().expect("exclusive"))
        .map(|p| p[1].smallest_user_key().expect("smallest").to_vec())
        .collect();
    assert!(
        !starts.is_empty(),
        "no table ended at an exclusive sentinel, so nothing here tests the rule: {outputs:?}"
    );

    let model = contents(&db).await;
    let pair = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
    for i in 0..121 {
        let k = format!("s{i:04}").into_bytes();
        for probe in [k.clone(), [k.clone(), vec![0]].concat()] {
            for (how, want) in [
                (
                    Comparison::Equal,
                    model.get(&probe).map(|v| (probe.clone(), v.clone())),
                ),
                (
                    Comparison::Floor,
                    model.range(..=probe.clone()).next_back().map(pair),
                ),
                (
                    Comparison::Ceiling,
                    model.range(probe.clone()..).next().map(pair),
                ),
                (
                    Comparison::Lower,
                    model.range(..probe.clone()).next_back().map(pair),
                ),
                (
                    Comparison::Higher,
                    model
                        .range((
                            std::ops::Bound::Excluded(probe.clone()),
                            std::ops::Bound::Unbounded,
                        ))
                        .next()
                        .map(pair),
                ),
            ] {
                let got = db
                    .get(&probe, how)
                    .await
                    .expect("get")
                    .map(|f| (f.key, f.value));
                assert!(
                    got == want,
                    "{how:?} of {:?}{}: found {:?}",
                    String::from_utf8_lossy(&probe),
                    if starts.contains(&probe) {
                        ", where a table starts after a split"
                    } else {
                        ""
                    },
                    got.map(|(k, v)| (String::from_utf8_lossy(&k).into_owned(), v.len()))
                );
            }
        }
    }
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

/// RocksDB's `RangeTombstoneEndKeyAsSstableUpperBound`: two L1 tables that
/// meet inside a range deletion go down into L2 one at a time, the second
/// first. RocksDB once wrote overlapping L2 tables doing this, by reading the
/// deletion's end, where the first table stops, as a key the table holds.
///
/// roble pulls into a compaction every table below whose user keys meet the
/// inputs', touching ones included (`Table::overlaps`), so the first table's
/// turn takes the second along again; and it cuts each output's deletions to
/// the output's span. What the sequence has to leave either way is asserted
/// after each step ([`settled_as`]), and after a crash and a reopen.
#[compio::test]
async fn tables_meeting_inside_a_range_deletion_go_down_one_at_a_time_and_stay_apart() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    // Two of the 4 KiB values to an L1 table, whose target is twice the
    // base, so that L1 splits where RocksDB's does, at the third key.
    // Uncompressed, as `open_splitting` explains.
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            .target_file_size(3 << 10)
            .compression(roble::Codec::None),
    )
    .await
    .expect("create");
    // RocksDB's `Key(i)`.
    let key = |i: u32| format!("key{i:06}").into_bytes();
    let value = vec![b'v'; 4 << 10];

    // A key in L2 for the deletion to hide, so that it is not elided.
    let mut batch = Batch::new();
    batch.put(&key(0), b"").expect("put");
    let t = flush_batch(&db, &mut batch).await;
    let l1 = db.compact(0, &[t]).await.expect("compact into L1");
    db.compact(1, &l1).await.expect("compact into L2");

    // The deletion and then two L0 tables of keys written after it: the
    // first, holding the deletion, has key0 and key3, the second key1 and
    // key2.
    let mut batch = Batch::new();
    batch.delete_range(&key(0), &key(4)).expect("delete_range");
    batch.put(&key(0), &value).expect("put");
    batch.put(&key(3), &value).expect("put");
    let a = flush_batch(&db, &mut batch).await;
    let mut batch = Batch::new();
    batch.put(&key(1), &value).expect("put");
    batch.put(&key(2), &value).expect("put");
    let b = flush_batch(&db, &mut batch).await;
    db.compact(0, &[a, b]).await.expect("compact into L1");

    let l1 = db.versions().version.level(1).to_vec();
    assert!(
        l1.len() == 2
            && l1[0].largest_user_key().expect("bound") == key(2)
            && l1[0].largest_is_exclusive().expect("bound")
            && l1[1].smallest_user_key().expect("bound") == key(2),
        "L1 should be two tables meeting at key2, the first ending at the deletion's sentinel"
    );
    let model = contents(&db).await;
    assert_eq!(
        model.keys().cloned().collect::<Vec<_>>(),
        (0..4).map(key).collect::<Vec<_>>()
    );
    settled_as(&db, &model, "after the split").await;

    // The second table into L2 first, which meets nothing there; then the
    // first, which meets both L2 tables: key0's, and the second's at key2.
    db.compact(1, &[l1[1].file_num])
        .await
        .expect("compact the second table");
    assert_eq!(level(&db, 1), [l1[0].file_num], "the first table stays");
    settled_as(&db, &model, "after the second table went down").await;
    db.compact(1, &[l1[0].file_num])
        .await
        .expect("compact the first table");
    assert!(level(&db, 1).is_empty(), "both tables went down");
    settled_as(&db, &model, "after the first table went down").await;

    drop(db);
    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    settled_as(&db, &model, "after a crash and a reopen").await;
}

/// Asserts what a compaction has to leave, whatever it chose to merge: no two
/// tables of a level below L0 overlapping, the levels in order
/// (`Db::check_levels`), and the store reading as `model` does, with every
/// comparison asked about every key it holds and about the key just past
/// each.
async fn settled_as<F: Fs>(db: &roble::Db<F>, model: &Model, when: &str) {
    let versions = db.versions();
    for (level, tables) in versions.version.levels().filter(|(l, _)| *l > 0) {
        for pair in tables.windows(2) {
            assert!(
                !pair[0].overlaps(&pair[1]).expect("bounds"),
                "{when}: L{level}'s tables {} and {} overlap",
                pair[0].file_num,
                pair[1].file_num
            );
        }
    }
    db.check_levels()
        .await
        .unwrap_or_else(|e| panic!("{when}: {e}"));
    assert_eq!(&contents(db).await, model, "{when}: what a scan reads");
    for k in model.keys() {
        let past = [k.as_slice(), b"\0"].concat();
        for probe in [k.as_slice(), past.as_slice()] {
            let from = |lo: Bound<&[u8]>| model.range::<[u8], _>((lo, Bound::Unbounded)).next();
            let to = |hi: Bound<&[u8]>| model.range::<[u8], _>((Bound::Unbounded, hi)).next_back();
            for (how, want) in [
                (Comparison::Equal, model.get_key_value(probe)),
                (Comparison::Floor, to(Bound::Included(probe))),
                (Comparison::Ceiling, from(Bound::Included(probe))),
                (Comparison::Lower, to(Bound::Excluded(probe))),
                (Comparison::Higher, from(Bound::Excluded(probe))),
            ] {
                let got = db.get(probe, how).await.expect("get");
                assert_eq!(
                    got.map(|f| f.key),
                    want.map(|(k, _)| k.clone()),
                    "{when}: {how:?} of {}",
                    probe.escape_ascii()
                );
            }
        }
    }
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
    let mut scan = db.range(..).expect("scan");
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

/// Every codec, end to end: a flush writes L0 uncompressed whatever the
/// option says, a compaction writes L1 with the codec, and the store answers
/// the same from the compressed table — forwards, backwards, by key, and
/// again after a reopen, when every block is a miss and has to be
/// decompressed rather than found in the cache.
#[compio::test]
async fn a_compaction_writes_the_codec_it_was_given_and_reads_it_back() {
    use roble::{Access, Codec};
    use roble_format::block::Compression;
    use roble_format::sst::Table;

    for (codec, stored) in [
        (Codec::None, Compression::None),
        (Codec::Snappy, Compression::Snappy),
        (Codec::Zstd(Codec::ZSTD_DEFAULT_LEVEL), Compression::Zstd),
    ] {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        env.sim().mkdir_all(&dir).await.expect("mkdir");
        let options = || {
            Options::new()
                .create_if_missing(true)
                .disable_automatic_compactions(true)
                .compression(codec)
        };
        let db = roble::open_with(&env, &dir, options()).await.expect("open");

        // Serialized records, as Oxia's values are, so a block compresses
        // well past the 12% Pebble requires before it keeps the result.
        let mut model = Model::new();
        let mut batch = Batch::new();
        for i in 0..3000u32 {
            let key = format!("/oxia/default/shard-0001/users/{i:08}").into_bytes();
            let value =
                format!(r#"{{"user":{i},"name":"user-{i}","status":"active"}}"#).into_bytes();
            batch.put(&key, &value).expect("put");
            model.insert(key, value);
        }
        let l0 = flush_batch(&db, &mut batch).await;
        let l1 = one_output(db.compact(0, &[l0]).await.expect("compact"));

        let read = async |file_num: u64| {
            let disk = env
                .sim()
                .open(
                    &dir.join(roble_format::files::table_filename(file_num)),
                    Access::ReadOnly,
                )
                .await
                .expect("open table");
            roble::__private::read_all(&disk, ()).await.expect("read")
        };
        let kinds = |bytes: &[u8]| -> Vec<Compression> {
            let table = Table::open(bytes).expect("table");
            let n = table.data_block_handles().expect("index").len();
            assert!(n > 4, "{n} blocks in {} bytes", bytes.len());
            table
                .data_block_handles()
                .expect("index")
                .into_iter()
                .map(|h| table.block_compression(h).expect("trailer"))
                .collect()
        };
        assert!(
            kinds(&read(l1).await).iter().all(|&c| c == stored),
            "{codec:?}: L1 holds {stored:?} blocks"
        );
        assert_eq!(contents(&db).await, model, "{codec:?}");
        drop(db);

        // The L0 table is gone by now, so check the rule on a fresh flush:
        // whatever the codec, L0 is written uncompressed.
        let db = roble::open_with(&env, &dir, options())
            .await
            .expect("reopen");
        let mut more = Batch::new();
        for i in 0..3000u32 {
            let key = format!("/oxia/default/shard-0002/users/{i:08}").into_bytes();
            let value = format!(r#"{{"user":{i},"status":"active"}}"#).into_bytes();
            more.put(&key, &value).expect("put");
            model.insert(key, value);
        }
        let fresh = flush_batch(&db, &mut more).await;
        assert!(
            kinds(&read(fresh).await)
                .iter()
                .all(|&c| c == Compression::None),
            "{codec:?}: L0 is written uncompressed"
        );

        // Cold reads of the compressed table: by key, then backwards.
        for i in [0u32, 1, 1499, 2999] {
            let key = format!("/oxia/default/shard-0001/users/{i:08}").into_bytes();
            let found = db
                .get(&key, Comparison::Equal)
                .await
                .expect("get")
                .expect("present");
            assert_eq!(found.value, model[&key], "{codec:?} {i}");
        }
        let mut scan = db.range(..).expect("scan");
        scan.last().await.expect("last");
        let mut backwards = Vec::new();
        while scan.is_valid() {
            backwards.push(scan.key().to_vec());
            scan.prev().await.expect("prev");
        }
        backwards.reverse();
        assert_eq!(
            backwards,
            model.keys().cloned().collect::<Vec<_>>(),
            "{codec:?}"
        );
    }
}

/// A compressed output is split at the size it takes on disk, not at the
/// bytes of keys and values that went into it. Counting those instead closed
/// every table at a fraction of its target once its blocks compressed, which
/// for the same data means several times as many tables.
#[compio::test]
async fn a_compressed_output_is_split_at_its_stored_size() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    env.sim().mkdir_all(&dir).await.expect("mkdir");
    // L1's target is twice this, 128 KiB: eight blocks, so one block of
    // estimate is small beside it.
    const TARGET: u64 = 128 << 10;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            .memtable_size(64 << 20)
            .target_file_size(TARGET / 2)
            .compression(roble::Codec::Snappy),
    )
    .await
    .expect("open");

    let mut batch = Batch::new();
    let mut raw = 0u64;
    for i in 0..40_000u32 {
        let key = format!("/oxia/default/shard-0001/users/{i:08}");
        let value = format!(r#"{{"user":{i},"name":"user-{i}","status":"active"}}"#);
        raw += (key.len() + value.len()) as u64;
        batch.put(key.as_bytes(), value.as_bytes()).expect("put");
    }
    let a = flush_batch(&db, &mut batch).await;
    let before = contents(&db).await;
    let outputs = db.compact(0, &[a]).await.expect("compact");
    assert_eq!(contents(&db).await, before);

    let in_l1: Vec<roble::Table> = db.versions().version.level(1).to_vec();
    let stored: u64 = in_l1.iter().map(|t| t.size).sum();
    assert!(
        stored * 2 < raw,
        "the values should compress: {stored} bytes stored from {raw}"
    );
    assert!(outputs.len() > 2, "several outputs, got {outputs:?}");
    for t in &in_l1[..in_l1.len() - 1] {
        assert!(
            t.size >= TARGET - roble_format::sst::DEFAULT_BLOCK_SIZE as u64,
            "table {} is {} bytes against a {TARGET} target",
            t.file_num,
            t.size
        );
    }
}

/// What kind each user key's entry in a table has, from the format crate's
/// dump: `entry <seq> <kind> <key> [value]`, keyed by the key as text.
fn kinds_in(dump: &str) -> BTreeMap<String, String> {
    dump.lines()
        .filter_map(|l| l.strip_prefix("entry "))
        .map(|l| {
            let fields: Vec<&str> = l.split_whitespace().collect();
            let key = (0..fields[2].len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&fields[2][i..i + 2], 16).expect("hex") as char)
                .collect();
            (key, fields[1].to_string())
        })
        .collect()
}

#[compio::test]
async fn a_set_that_buries_a_deletion_in_a_compaction_is_written_as_setwithdel() {
    // Pebble's `setNext`: a SET that meets an older DEL, SINGLEDEL or
    // DELSIZED of its key in one compaction is written as SETWITHDEL. That
    // is what tells a later SingleDelete there may be more below the Set it
    // meets, so it acts as a Delete rather than cancelling one Set. Written
    // as a plain SET with an older value still in a deeper level, a
    // SingleDelete Pebble is allowed to issue cancels the Set and brings the
    // older value back (`tests/read.rs` reaches that shape from a database
    // Pebble wrote). Each version is in a table of its own, so the meeting
    // happens in the merge and not in a flush.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("create");

    let mut oldest = Batch::new();
    oldest.put(b"d", b"old").expect("put");
    oldest.put(b"s", b"old").expect("put");
    oldest.put(b"z", b"old").expect("put");
    oldest.delete(b"m").expect("delete");
    oldest.put(b"p", b"old").expect("put");
    let mut middle = Batch::new();
    middle.delete(b"d").expect("delete");
    middle.single_delete(b"s").expect("single_delete");
    middle.delete_sized(b"z", 3).expect("delete_sized");
    // A Set between the newest and the deletion does not stop it: Pebble
    // walks every older entry of the key until it meets one.
    middle.put(b"m", b"mid").expect("put");
    let mut newest = Batch::new();
    for key in [&b"d"[..], b"s", b"z", b"m", b"p", b"n"] {
        newest.put(key, b"new").expect("put");
    }
    let tables = vec![
        flush_batch(&db, &mut oldest).await,
        flush_batch(&db, &mut middle).await,
        flush_batch(&db, &mut newest).await,
    ];
    let out = one_output(db.compact(0, &tables).await.expect("compact"));

    let bytes = env
        .sim()
        .durable(&dir.join(format!("{out:06}.sst")))
        .expect("the table is on disk");
    let dump = roble_format::dump_table(&bytes).expect("dump");
    let want: BTreeMap<String, String> = [
        ("d", "setwithdel"),
        ("m", "setwithdel"),
        // Neither met a deletion, so both stay what they were written.
        ("n", "set"),
        ("p", "set"),
        ("s", "setwithdel"),
        ("z", "setwithdel"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    assert_eq!(kinds_in(&dump), want, "the compacted table:\n{dump}");

    // And the store answers what it did, a SETWITHDEL reading as its Set.
    for key in [&b"d"[..], b"m", b"n", b"p", b"s", b"z"] {
        assert_eq!(
            db.get(key, Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(b"new".to_vec()),
        );
    }
}

/// Flushes one batch and compacts its table down to `level`, a level at a
/// time, so that what it holds lies below anything flushed after it.
async fn settle<F: Fs>(db: &roble::Db<F>, batch: &mut Batch, level: u32) {
    let mut tables = vec![flush_batch(db, batch).await];
    for from in 0..level {
        tables = db.compact(from, &tables).await.expect("compact");
    }
}

#[compio::test]
async fn a_single_delete_that_buries_an_older_version_in_a_compaction_is_written_as_del() {
    // The history Pebble allows a SingleDelete on: `k` Set, overwritten, then
    // deleted, then Set once more, then SingleDeleted. The first two values
    // sit in L3 and L2. Pebble's `singleDeleteNext` turns a SINGLEDEL that
    // meets the DEL — or the SETWITHDEL the DEL left behind it — into a DEL.
    // Kept a SINGLEDEL over it, the next Pebble compaction lets it cancel
    // `b` and nothing else, and `a` comes back. Twice: once with the Set and
    // the DEL compacted together first, which leaves a SETWITHDEL for the
    // SINGLEDEL to meet, and once with all three in one merge.
    for staged in [true, false] {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        let db = roble::open_with(
            &env,
            &dir,
            Options::new()
                .create_if_missing(true)
                .disable_automatic_compactions(true),
        )
        .await
        .expect("create");

        let mut first = Batch::new();
        first.put(b"k", b"a").expect("put");
        // Beside it, so that the L1 output below has a SINGLEDEL with
        // nothing older in the merge, which stays one.
        first.put(b"r", b"a").expect("put");
        settle(&db, &mut first, 3).await;
        let mut second = Batch::new();
        second.put(b"k", b"b").expect("put");
        settle(&db, &mut second, 2).await;

        let mut deleted = Batch::new();
        deleted.delete(b"k").expect("delete");
        let mut again = Batch::new();
        again.put(b"k", b"d").expect("put");
        let mut l0 = vec![
            flush_batch(&db, &mut deleted).await,
            flush_batch(&db, &mut again).await,
        ];
        if staged {
            db.compact(0, &l0)
                .await
                .expect("compact the DEL and the Set");
            l0.clear();
        }
        let mut single = Batch::new();
        single.single_delete(b"k").expect("single_delete");
        single.single_delete(b"r").expect("single_delete");
        l0.push(flush_batch(&db, &mut single).await);
        let out = one_output(db.compact(0, &l0).await.expect("compact"));

        let bytes = env
            .sim()
            .durable(&dir.join(format!("{out:06}.sst")))
            .expect("the table is on disk");
        let dump = roble_format::dump_table(&bytes).expect("dump");
        let want: BTreeMap<String, String> = [("k", "del"), ("r", "singledel")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(
            kinds_in(&dump),
            want,
            "staged {staged}, the compacted table:\n{dump}"
        );
        for key in [&b"k"[..], b"r"] {
            assert_eq!(db.get(key, Comparison::Equal).await.expect("get"), None);
        }
    }
}

/// Every point entry of a table, in order: its user key, its kind, and
/// whether the obsolete bit is set on it.
fn marks(bytes: &[u8]) -> Vec<(String, &'static str, bool)> {
    let table = roble_format::sst::Table::open(bytes).expect("open the table");
    let mut out = Vec::new();
    table
        .for_each_entry(|key, _| {
            out.push((
                String::from_utf8(key.user_key.to_vec()).expect("utf-8"),
                key.kind.name(),
                key.obsolete,
            ));
            Ok(())
        })
        .expect("entries");
    out
}

#[compio::test]
async fn a_compaction_drops_every_key_pebble_marked_obsolete() {
    // Pebble sets the obsolete bit on a key no reader without a snapshot can
    // see, and keeps such a key only while a snapshot can: an older version
    // below a newer one in the same table, a point deletion in a table
    // written to L6, and a key a range deletion in the same table covers.
    // `db-obsolete` holds all three (`buildObsolete` in `harness/db-fixture`).
    // roble reads the bit and never writes it (DESIGN.md §3): its table
    // writer refuses a key that carries one. So once no snapshot is left, a
    // compaction of these tables has to drop every marked key, and one it
    // kept would fail the compaction rather than reach a table.
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-obsolete");
    // Each entry with the reason Pebble marked it, if it did.
    let spelled =
        |entries: &[(&str, &'static str, Option<&str>)]| -> Vec<(String, &'static str, bool)> {
            entries
                .iter()
                .map(|&(key, kind, why)| (key.to_string(), kind, why.is_some()))
                .collect()
        };
    let older = Some("an older version");
    let deletion_in_l6 = Some("a deletion in L6");
    let covered = Some("covered by the range deletion");

    // What Pebble wrote, so that a fixture regenerated without the marks
    // fails here rather than passing on tables with nothing to drop. Each
    // table starts with a live key because Pebble never marks a table's
    // first key as a deletion in L6.
    let in_l6 = spelled(&[
        ("l6-alive", "set", None),
        ("l6-deleted", "del", deletion_in_l6),
        ("l6-deleted", "set", older),
        ("l6-overwritten", "set", None),
        ("l6-overwritten", "set", older),
        ("l6-range-1", "set", covered),
        ("l6-range-2", "set", covered),
        // Where the range deletion ends, so not under it.
        ("l6-range-3", "set", None),
        ("l6-single-deleted", "singledel", deletion_in_l6),
        ("l6-single-deleted", "set", older),
        ("l6-size-deleted", "delsized", deletion_in_l6),
        ("l6-size-deleted", "set", older),
    ]);
    // A flush writes into L0, so its deletions are not marked.
    let in_l0 = spelled(&[
        ("l0-alive", "set", None),
        ("l0-deleted", "del", None),
        ("l0-deleted", "set", older),
        ("l0-overwritten", "set", None),
        ("l0-overwritten", "set", older),
        ("l0-range-1", "set", covered),
        ("l0-range-2", "set", covered),
        ("l0-range-3", "set", None),
        ("l0-single-deleted", "singledel", None),
        ("l0-single-deleted", "set", older),
        ("l0-size-deleted", "delsized", None),
        ("l0-size-deleted", "set", older),
    ]);
    for (name, want) in [("000006.sst", &in_l6), ("000007.sst", &in_l0)] {
        let bytes = std::fs::read(fixture.join(name)).expect("read the fixture");
        assert_eq!(&marks(&bytes), want, "{name}");
    }

    let (env, dir) = load("db-obsolete").await;
    let db = open_unscheduled(&env, &dir).await;
    let in_level = |n: u32| -> Vec<u64> {
        db.versions()
            .version
            .level(n)
            .iter()
            .map(|t| t.file_num)
            .collect()
    };
    // The deletions are marked only because their table is in L6.
    assert_eq!(in_level(6), [6]);
    assert_eq!(in_level(0), [7]);

    /// Asks for every key Pebble wrote, the deleted ones included.
    async fn answers(db: &roble::Db<SimFs>, keys: &[String]) -> Model {
        let mut found = Model::new();
        for key in keys {
            if let Some(f) = db
                .get(key.as_bytes(), Comparison::Equal)
                .await
                .expect("get")
            {
                found.insert(f.key, f.value);
            }
        }
        found
    }
    let keys: Vec<String> = in_l0.iter().chain(&in_l6).map(|e| e.0.clone()).collect();
    // What Pebble's own reader found when the generator reopened it.
    let live: Model = [
        ("l0-alive", "old-l0-alive"),
        ("l0-overwritten", "new-l0-overwritten"),
        ("l0-range-3", "old-l0-range-3"),
        ("l6-alive", "old-l6-alive"),
        ("l6-overwritten", "new-l6-overwritten"),
        ("l6-range-3", "old-l6-range-3"),
    ]
    .into_iter()
    .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
    .collect();
    assert_eq!(contents(&db).await, live);
    assert_eq!(answers(&db, &keys).await, live);

    // Keys either side of all of Pebble's, flushed and pushed down a level at
    // a time, so that each merge takes every table of the level below it: the
    // first takes Pebble's L0 table and the last its L6 one. A merge's error
    // is this test's failure, which is where a marked key kept would show.
    let mut around = Batch::new();
    around.put(b"a", b"first").expect("put");
    around.put(b"z", b"last").expect("put");
    let mut tables = vec![flush_batch(&db, &mut around).await, 7];
    for from in 0..6 {
        tables = db
            .compact(from, &tables)
            .await
            .unwrap_or_else(|e| panic!("compacting L{from}: {e}"));
    }
    let mut after = live.clone();
    after.insert(b"a".to_vec(), b"first".to_vec());
    after.insert(b"z".to_vec(), b"last".to_vec());

    // Both of Pebble's tables were merged away, and what replaced them holds
    // what a reader sees and nothing else: every marked key was dropped, not
    // carried with its bit cleared.
    let left: Vec<u64> = db
        .versions()
        .version
        .levels()
        .flat_map(|(_, tables)| tables.iter().map(|t| t.file_num))
        .collect();
    assert_eq!(left, tables, "only the last merge's output is left");
    let mut written = Vec::new();
    for n in &tables {
        let bytes = env
            .sim()
            .durable(&dir.join(format!("{n:06}.sst")))
            .expect("the table is on disk");
        written.extend(marks(&bytes));
    }
    let unmarked: Vec<(String, &'static str, bool)> = after
        .keys()
        .map(|k| (String::from_utf8(k.clone()).expect("utf-8"), "set", false))
        .collect();
    assert_eq!(written, unmarked);

    assert_eq!(contents(&db).await, after);
    assert_eq!(answers(&db, &keys).await, live);
    drop(db);
    let db = open_unscheduled(&env, &dir).await;
    assert_eq!(contents(&db).await, after);
    assert_eq!(answers(&db, &keys).await, live);
}
