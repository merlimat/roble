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

//! Choosing what to compact.
//!
//! The picker is the one component whose whole job is to make the same
//! decision twice, so most of what follows is about *which* level it chose
//! and why, rather than about what the compaction then did. The last test is
//! the one that matters in practice: a store that keeps flushing has to
//! settle rather than pile up, once something compacts what the picker
//! names.
//!
//! Every store here is opened with automatic compactions off. The picker's
//! answer is a statement about a version, and these tests build their
//! versions by flushing: a flush that ran the compaction it chose would
//! have changed the version before the assertion could read it. What runs
//! the picker is `tests/schedule.rs`.

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

async fn contents<F: Fs>(db: &roble::Opened<F>) -> Model {
    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    let mut out = Model::new();
    while scan.is_valid() {
        out.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    out
}

/// Writes `count` keys from `at` and flushes, giving one L0 table.
async fn flush_keys<F: Fs>(db: &roble::Opened<F>, prefix: &str, at: usize, count: usize) -> u64 {
    let mut batch = Batch::new();
    for i in at..at + count {
        batch
            .put(format!("{prefix}{i:04}").as_bytes(), b"value")
            .expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table")
}

#[compio::test]
async fn a_level_inside_its_allowance_is_left_alone() {
    let (env, dir) = load("db-twin-a").await;
    // The fixture arrives with one table in L0, so three more reach four.
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .l0_compaction_threshold(4),
    )
    .await
    .expect("open");
    assert_eq!(db.versions().version.level(0).len(), 1);

    assert_eq!(db.pick(), None, "one table in L0 is not worth compacting");
    for round in 0..2 {
        flush_keys(&db, "a", round * 10, 4).await;
        assert_eq!(
            db.pick(),
            None,
            "{} tables in L0 is still inside the allowance",
            db.versions().version.level(0).len()
        );
    }

    flush_keys(&db, "a", 100, 4).await;
    let pick = db.pick().expect("four tables in L0 is past the allowance");
    assert_eq!(pick.level, 0);
    assert_eq!(pick.held, 4);
    assert_eq!(pick.allowance, 4);
    assert_eq!(pick.score(), 1.0);
}

#[compio::test]
async fn l0_is_picked_whole() {
    // L0's tables overlap, so compacting one means pulling in every one that
    // meets it, and in L0 that usually spreads to the lot.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .l0_compaction_threshold(3),
    )
    .await
    .expect("open");

    flush_keys(&db, "b", 0, 4).await;
    flush_keys(&db, "b", 2, 4).await;

    let mut in_l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    in_l0.sort();
    let mut picked = db.pick().expect("a pick").tables;
    picked.sort();
    assert_eq!(picked, in_l0, "the pick should be all of L0");
}

#[compio::test]
async fn the_worst_level_is_the_one_picked() {
    // Two levels past their allowances. The one further past is the one that
    // costs the most to leave alone.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .l0_compaction_threshold(2)
            .lbase_max_bytes(64),
    )
    .await
    .expect("open");

    // Put something in L1 first, far past a 64-byte allowance.
    flush_keys(&db, "c", 0, 40).await;
    let l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    db.compact(0, &l0).await.expect("compact");
    let l1_bytes: u64 = db.versions().version.level(1).iter().map(|t| t.size).sum();
    assert!(l1_bytes > 64, "L1 should be past its allowance");

    // And L0 barely past its own.
    flush_keys(&db, "d", 0, 1).await;
    flush_keys(&db, "d", 1, 1).await;
    assert_eq!(db.versions().version.level(0).len(), 2);

    let pick = db.pick().expect("a pick");
    assert_eq!(
        pick.level,
        1,
        "L1 at {}/{} is further past than L0 at {}/{}",
        l1_bytes,
        64,
        db.versions().version.level(0).len(),
        2
    );
    assert!(pick.score() > 2.0 / 2.0);
}

#[compio::test]
async fn a_tie_goes_to_the_level_nearest_l0() {
    // Both exactly at their allowance. L0 wins, because a table being in L0
    // costs every read and a table in L1 only the reads that get that far.
    let (env, dir) = load("db-twin-a").await;
    let l0_tables;
    let l1_bytes;

    {
        let db = roble::open_with(
            &env,
            &dir,
            Options::new()
                .disable_automatic_compactions(true)
                .l0_compaction_threshold(1)
                .lbase_max_bytes(1),
        )
        .await
        .expect("open");

        flush_keys(&db, "e", 0, 4).await;
        let l0: Vec<u64> = db
            .versions()
            .version
            .level(0)
            .iter()
            .map(|t| t.file_num)
            .collect();
        db.compact(0, &l0).await.expect("compact");
        flush_keys(&db, "f", 0, 4).await;
        flush_keys(&db, "f", 4, 4).await;

        l0_tables = db.versions().version.level(0).len() as u64;
        l1_bytes = db.versions().version.level(1).iter().map(|t| t.size).sum();
    }

    // Allowances set to exactly what each level holds, so both score one and
    // only the tie rule can decide between them.
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .l0_compaction_threshold(l0_tables)
            .lbase_max_bytes(l1_bytes),
    )
    .await
    .expect("reopen");
    let pick = db.pick().expect("a pick");
    assert_eq!(
        pick.score(),
        1.0,
        "L{} should be exactly at one",
        pick.level
    );
    assert_eq!(
        pick.level, 0,
        "L0 at {l0_tables}/{l0_tables} ties L1 at {l1_bytes}/{l1_bytes}, and L0 wins"
    );
}

#[compio::test]
async fn a_level_below_l0_is_picked_one_table_at_a_time() {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .l0_compaction_threshold(1)
            .lbase_max_bytes(1),
    )
    .await
    .expect("open");

    // Two tables in L1, which do not overlap, so they stay two.
    for (prefix, at) in [("g", 0), ("h", 0)] {
        flush_keys(&db, prefix, at, 6).await;
        let l0: Vec<u64> = db
            .versions()
            .version
            .level(0)
            .iter()
            .map(|t| t.file_num)
            .collect();
        db.compact(0, &l0).await.expect("compact");
    }
    let in_l1: Vec<(u64, u64)> = db
        .versions()
        .version
        .level(1)
        .iter()
        .map(|t| (t.file_num, t.size))
        .collect();
    assert_eq!(in_l1.len(), 2, "L1 should hold two tables: {in_l1:?}");

    // L0 is empty now, so L1 is the only level past its allowance.
    assert!(db.versions().version.level(0).is_empty());
    let pick = db.pick().expect("a pick");
    assert_eq!(pick.level, 1);
    assert_eq!(pick.tables.len(), 1, "one table at a time below L0");

    let largest = in_l1
        .iter()
        .max_by_key(|(n, s)| (*s, std::cmp::Reverse(*n)));
    assert_eq!(
        pick.tables[0],
        largest.expect("a largest").0,
        "the largest table moves the most bytes down for one merge: {in_l1:?}"
    );
}

#[compio::test]
async fn the_last_level_is_never_picked() {
    // There is nowhere below it, so saying it is past its allowance would be
    // saying something nothing can act on.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .l0_compaction_threshold(1)
            .lbase_max_bytes(1),
    )
    .await
    .expect("open");

    // Enough bytes that the last level really is past its allowance, which
    // with an `lbase` of one is a hundred thousand: a handful of keys would
    // sit inside it and the test would pass whatever the rule said.
    let value = vec![b'v'; 200];
    let mut batch = Batch::new();
    for i in 0..700 {
        batch
            .put(format!("i{i:04}").as_bytes(), &value)
            .expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");

    // Push it all the way down.
    for level in 0..6 {
        let tables: Vec<u64> = db
            .versions()
            .version
            .level(level)
            .iter()
            .map(|t| t.file_num)
            .collect();
        if tables.is_empty() {
            continue;
        }
        db.compact(level, &tables).await.expect("compact");
    }
    let last: u64 = db.versions().version.level(6).iter().map(|t| t.size).sum();
    assert!(
        last > 100_000,
        "the last level holds {last} bytes against an allowance of 100000, so it is \
         not past it and this test would pass either way"
    );
    assert_eq!(
        db.pick(),
        None,
        "the last level has nowhere to go, however far past its allowance"
    );
}

#[compio::test]
async fn the_allowance_grows_with_depth() {
    // Each level may hold ten times what the one above it may. A level that
    // would be past a flat allowance has to be left alone when the rule is
    // the right one.
    let (env, dir) = load("db-twin-a").await;
    let in_l2;

    {
        let db = roble::open_with(
            &env,
            &dir,
            Options::new()
                .disable_automatic_compactions(true)
                .l0_compaction_threshold(1)
                .lbase_max_bytes(1),
        )
        .await
        .expect("open");
        flush_keys(&db, "m", 0, 40).await;
        let l0: Vec<u64> = db
            .versions()
            .version
            .level(0)
            .iter()
            .map(|t| t.file_num)
            .collect();
        let l1 = db.compact(0, &l0).await.expect("compact");
        db.compact(1, &l1).await.expect("compact");
        in_l2 = db
            .versions()
            .version
            .level(2)
            .iter()
            .map(|t| t.size)
            .sum::<u64>();
        assert!(in_l2 > 2, "L2 should hold something: {in_l2}");
    }

    // An allowance L2 is comfortably inside — but only because it grows:
    // flat, L2 would be at twice its allowance and picked at once.
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .l0_compaction_threshold(1000)
            .lbase_max_bytes(in_l2 / 2),
    )
    .await
    .expect("reopen");
    assert!(
        db.versions().version.level(0).len() < 1000 && db.versions().version.level(1).is_empty(),
        "only L2 should be in the running"
    );
    assert_eq!(
        db.pick(),
        None,
        "L2 holds {in_l2} against an allowance of {} and should be left alone",
        (in_l2 / 2) * 10
    );
}

#[compio::test]
async fn the_same_version_always_picks_the_same_thing() {
    // §9: a picker that read a clock or drew a random number would stop a
    // seeded run replaying. Asking twice is the cheapest way to say so.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .l0_compaction_threshold(2)
            .lbase_max_bytes(64),
    )
    .await
    .expect("open");
    flush_keys(&db, "j", 0, 8).await;
    flush_keys(&db, "j", 4, 8).await;

    let first = db.pick().expect("a pick");
    for _ in 0..8 {
        assert_eq!(
            db.pick().as_ref(),
            Some(&first),
            "the pick moved on its own"
        );
    }
}

#[compio::test]
async fn compacting_until_it_settles_changes_no_answer() {
    // What the picker is for. Flush repeatedly with allowances small enough
    // to be crossed, compact whatever it names, and the database has to end
    // up inside every allowance with exactly the keys it started with.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .l0_compaction_threshold(3)
            .lbase_max_bytes(256),
    )
    .await
    .expect("open");

    let mut model = contents(&db).await;
    let mut deepest = 0;
    for round in 0..12 {
        let mut batch = Batch::new();
        for i in 0..6 {
            let key = format!("k{:04}", (round * 3 + i) % 20);
            let value = format!("round-{round}");
            batch.put(key.as_bytes(), value.as_bytes()).expect("put");
            model.insert(key.into_bytes(), value.into_bytes());
        }
        if round % 4 == 3 {
            let key = format!("k{:04}", round);
            batch.delete(key.as_bytes()).expect("delete");
            model.remove(key.as_bytes());
        }
        db.write(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");

        let mut ran = 0;
        while let Some(pick) = db.pick() {
            deepest = deepest.max(pick.level);
            db.compact_once().await.expect("compact_once");
            ran += 1;
            assert!(ran < 50, "the picker never settled");
        }
        assert_eq!(db.pick(), None, "it should be inside every allowance now");
        assert_eq!(
            contents(&db).await,
            model,
            "compacting to a settled state changed an answer in round {round}"
        );
    }

    // It really did compact, and not only out of L0: a loop that never
    // picked a level below it would leave `compact_once`'s choice of level
    // untested.
    let below_l0: usize = (1..7).map(|l| db.versions().version.level(l).len()).sum();
    assert!(below_l0 > 0, "nothing ever got out of L0");
    assert!(
        deepest > 0,
        "every pick was L0, so nothing here says which level a pick compacts"
    );

    env.sim().crash();
    let db = roble::open_with(&env, &dir, Options::default())
        .await
        .expect("reopen");
    assert_eq!(
        contents(&db).await,
        model,
        "and it should read back the same"
    );
    assert_eq!(
        db.get(b"k0005", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        model.get(b"k0005".as_slice()).cloned(),
    );
}
