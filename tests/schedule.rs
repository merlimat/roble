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

//! When a compaction runs (§9).
//!
//! `tests/pick.rs` is about which compaction the picker chooses and
//! `tests/compact.rs` about what the merge then does. This is about the
//! thing in between, which until now nothing did: a flush that has written a
//! table runs the compaction the picker chose, so a store that is written to
//! keeps itself in shape without the caller ever naming a table.
//!
//! `db-reads` is the fixture throughout because it arrives wanting a
//! compaction — ten tables in L0 against an allowance of four — so the
//! picker has an answer before this file has done anything.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, Options, WriteBuf};
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

/// Every key the database holds.
async fn contents<F: Fs>(db: &roble::Opened<'_, F>) -> Model {
    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    let mut out = Model::new();
    while scan.is_valid() {
        out.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    out
}

/// How many tables each level holds.
fn levels<F: Fs>(db: &roble::Opened<'_, F>) -> Vec<usize> {
    let versions = db.versions();
    (0..7).map(|l| versions.version.level(l).len()).collect()
}

/// Writes one key and flushes it, giving one L0 table — and whatever the
/// flush then decided to do about it.
async fn write_and_flush<F: Fs>(db: &roble::Opened<'_, F>, key: &[u8]) {
    let mut batch = Batch::new();
    batch.put(key, b"value").expect("put");
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");
}

#[compio::test]
async fn a_flush_runs_the_compaction_the_picker_chose() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // The fixture arrives over its allowance, and nothing has asked for a
    // compaction.
    let before = levels(&db);
    assert_eq!(
        before[0], 10,
        "db-reads should arrive with ten tables in L0"
    );
    assert!(db.pick().is_some(), "and with a compaction worth running");

    write_and_flush(&db, b"scheduled").await;

    // L0 is compacted whole, so the flush's own table went down with the
    // ten it joined.
    let after = levels(&db);
    assert_eq!(
        after[0], 0,
        "the flush should have run the compaction the picker chose: {after:?}"
    );
    assert_eq!(after[1], 1, "and left its output in L1: {after:?}");
    assert_eq!(db.pick(), None, "with nothing left worth compacting");
}

#[compio::test]
async fn a_flush_that_wrote_nothing_compacts_nothing() {
    // A flush of an empty memtable changes nothing, so there is nothing new
    // for the picker to see: its answer is the one it already gave, which
    // the flush that wrote the last table already ran. Draining what is left
    // is the caller's to ask for, until something runs behind the caller's
    // back at all (§9).
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let before = levels(&db);
    assert!(db.pick().is_some(), "the fixture arrives wanting one");

    assert!(
        db.flush().await.expect("flush").is_none(),
        "an empty memtable is not a table"
    );

    assert_eq!(
        levels(&db),
        before,
        "a flush that wrote nothing should have compacted nothing"
    );
    assert!(db.pick().is_some(), "and left the picker's answer standing");
}

#[compio::test]
async fn a_flush_pays_for_one_compaction_and_not_the_backlog() {
    // One per flush. A flush adds a single table to L0 and a compaction of
    // L0 takes all of it, so one keeps up with the level a flush fills;
    // draining a backlog the caller did not create is what it must not do,
    // because the caller is waiting on the flush.
    //
    // `lbase_max_bytes(1)` is what makes a backlog out of this fixture: once
    // L0's tables are in L1, L1 is over an allowance of one byte, so the
    // picker still has an answer after the flush has run one compaction.
    let (env, dir) = load("db-reads").await;
    let db = roble::open_with(&env, &dir, Options::new().lbase_max_bytes(1))
        .await
        .expect("open");

    write_and_flush(&db, b"scheduled").await;

    let after = levels(&db);
    assert_eq!(after[0], 0, "L0 was the compaction it ran: {after:?}");
    assert_eq!(
        db.pick().map(|p| p.level),
        Some(1),
        "and L1 is what it left for the next flush: {after:?}"
    );
}

#[compio::test]
async fn the_caller_can_keep_compaction_to_itself() {
    // Pebble's `DisableAutomaticCompactions`, and what most of this
    // repository's other tests are opened with: a store that holds still.
    let (env, dir) = load("db-reads").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new().disable_automatic_compactions(true),
    )
    .await
    .expect("open");

    write_and_flush(&db, b"unscheduled").await;

    let after = levels(&db);
    assert_eq!(after[0], 11, "nothing should have compacted: {after:?}");
    assert!(
        db.pick().is_some(),
        "and the picker should still want one — it is only not being asked"
    );

    // And the caller's own call still does it.
    assert!(
        !db.compact_once().await.expect("compact").is_empty(),
        "compact_once should run what the picker chose"
    );
    assert_eq!(levels(&db)[0], 0, "which drains L0");
}

#[compio::test]
async fn a_store_that_keeps_flushing_settles_by_itself() {
    // The whole point, and the one test here that would notice if the
    // scheduling kept up with nothing: twenty rounds of writes and flushes,
    // with no compaction ever asked for. L0 has to stay inside its
    // allowance, and the answers have to be the ones that were written.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut model = contents(&db).await;
    for round in 0..20 {
        let mut batch = Batch::new();
        for i in 0..20 {
            let key = format!("s{round:02}{i:02}").into_bytes();
            batch.put(&key, b"value").expect("put");
            model.insert(key, b"value".to_vec());
        }
        db.write(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");

        let held = levels(&db)[0];
        assert!(
            held as u64 <= db.l0_compaction_threshold(),
            "after round {round} L0 held {held} tables, past its allowance of {}",
            db.l0_compaction_threshold()
        );
    }

    assert_eq!(
        contents(&db).await,
        model,
        "a store that compacted for itself should hold what was written to it"
    );

    // And it reopens as what it became.
    drop(db);
    let reopened = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        contents(&reopened).await,
        model,
        "and should still hold it after a reopen"
    );
}
