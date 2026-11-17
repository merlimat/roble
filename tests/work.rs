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

//! The long jobs a store does, handing the thread back as they go.
//!
//! Two of them: the merge a compaction runs, and the walk of the memtable a
//! flush writes out. Both are work rather than waiting, and both now happen
//! beside the writes rather than inside one (§9), which is what makes the
//! pacing load-bearing — an unpaced job on the owning thread is a job
//! nothing else on that core runs during.
//!
//! "Cooperative" is a claim about something that does not happen — nothing
//! else waits — so a test of it has to look at the future itself rather than
//! at the answer it eventually gives. These poll the job by hand and
//! count how often it goes to sleep, which is exactly how often the executor
//! would have been free to run something else.
//!
//! The simulated filesystem never blocks: its operations finish in the call
//! that starts them. So every `Pending` below comes from the pacer, and
//! counting them counts yields and nothing else.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

/// Runs `future` to completion, returning its value and how many times it
/// yielded on the way.
fn drive<F: Future>(future: F) -> (F::Output, usize) {
    // The waker does nothing: this loop polls again whatever it is told.
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    let mut yields = 0;
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return (value, yields),
            Poll::Pending => {
                yields += 1;
                assert!(yields < 100_000, "this future is not making progress");
            }
        }
    }
}

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

/// Opens with `unit`, and leaves 300 keys in one L0 table to compact.
async fn ready_to_compact(env: &SimEnv, dir: &Path, unit: u64) -> (roble::Db<SimFs>, u64) {
    let db = roble::open_with(env, dir, Options::new().compaction_unit(unit))
        .await
        .expect("open");
    let mut batch = Batch::new();
    for i in 0..300 {
        batch
            .put(format!("y{i:04}").as_bytes(), b"value")
            .expect("put");
    }
    db.write_raw(&mut batch, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");
    (db, table)
}

/// Opens with `unit` and leaves 300 keys in the memtable for a flush to
/// walk, with nothing left over from the fixture's own logs.
async fn ready_to_flush(env: &SimEnv, dir: &Path, unit: u64) -> roble::Db<SimFs> {
    let db = roble::open_with(
        env,
        dir,
        Options::new()
            .compaction_unit(unit)
            // So that the only thing the flush below is paced for is its own
            // walk: a merge after it would be counted in the same yields.
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open");
    // Whatever the fixture's logs replayed into the memtable, written out
    // first, so the flush that is measured walks the 300 keys and no others.
    db.flush().await.expect("the fixture's own flush");
    let mut batch = Batch::new();
    for i in 0..300 {
        batch
            .put(format!("y{i:04}").as_bytes(), b"value")
            .expect("put");
    }
    db.write_raw(&mut batch, true).await.expect("write");
    db
}

#[compio::test]
async fn a_flush_hands_the_thread_back_as_it_goes() {
    // The walk of a 64 MiB memtable is a few hundred thousand entries to
    // read and encode, and left in one piece it is tens of milliseconds in
    // which nothing else on the core runs. That is invisible while a flush
    // happens inside the write that asked for it, and is the whole of what
    // is left once it does not (§5, `crate::memtable::FLUSH_RESERVE`).
    let (env, dir) = load("db-twin-a").await;
    let db = ready_to_flush(&env, &dir, 50).await;

    let (flushed, yields) = drive(db.flush());
    flushed.expect("flush").expect("a table");
    // 300 keys at 50 to a unit is six slices and the five gaps between
    // them: the walk stops when the cursor runs out rather than handing the
    // thread back on its way to finding that out. The upper bound matters as
    // much as the lower for the reason the merge's does — a counter that
    // never reset would yield on every entry after the first unit.
    assert!(
        (5..=7).contains(&yields),
        "a walk of 300 keys at 50 to a unit is six slices and five gaps, not {yields}"
    );
}

#[compio::test]
async fn a_flush_with_a_unit_of_zero_walks_straight_through() {
    let (env, dir) = load("db-twin-a").await;
    let db = ready_to_flush(&env, &dir, 0).await;

    let (flushed, yields) = drive(db.flush());
    flushed.expect("flush").expect("a table");
    assert_eq!(
        yields, 0,
        "a caller that asked for no pacing should get none"
    );
}

#[compio::test]
async fn a_paced_flush_writes_the_table_an_unpaced_one_would() {
    // The whole of it: a walk cut into slices has to write the table a walk
    // in one go would have. The memtable is frozen before the walk starts
    // and nothing adds to a frozen one (§5), so a yield in the middle of it
    // cannot change what there is to read.
    let mut built = Vec::new();
    for unit in [0, 1, 7, 1000] {
        let (env, dir) = load("db-twin-a").await;
        let db = ready_to_flush(&env, &dir, unit).await;
        db.flush().await.expect("flush").expect("a table");

        let mut scan = db.range(..).expect("scan");
        scan.first().await.expect("first");
        let mut seen = Vec::new();
        while scan.is_valid() {
            seen.push((scan.key().to_vec(), scan.value().to_vec()));
            scan.next().await.expect("next");
        }
        assert!(!seen.is_empty());
        let sizes: Vec<u64> = db
            .versions()
            .version
            .level(0)
            .iter()
            .map(|t| t.size)
            .collect();
        built.push((unit, seen, sizes));
    }

    let (_, first, first_sizes) = &built[0];
    for (unit, seen, sizes) in &built[1..] {
        assert_eq!(seen, first, "a unit of {unit} changed what the store holds");
        assert_eq!(
            sizes, first_sizes,
            "a unit of {unit} changed the tables it was written into"
        );
    }
}

#[compio::test]
async fn a_compaction_hands_the_thread_back_as_it_goes() {
    let (env, dir) = load("db-twin-a").await;
    let (db, table) = ready_to_compact(&env, &dir, 50).await;

    let (outputs, yields) = drive(db.compact(0, &[table]));
    outputs.expect("compact");
    // 300 keys at 50 to a unit is six slices. The upper bound matters as
    // much as the lower: a pacer that never reset its counter would yield on
    // every entry after the first unit, which is cooperative to the point of
    // uselessness and passes any test that only asks for "enough".
    assert!(
        (6..=8).contains(&yields),
        "a merge of 300 keys at 50 to a unit is about six slices, not {yields}"
    );
}

#[compio::test]
async fn a_smaller_unit_hands_it_back_more_often() {
    // The unit is the lever, so it has to move the thing it is a lever for.
    // Each run gets its own copy of the same database, so the two merges
    // read the same keys and differ only in how they were paced.
    let mut measured = Vec::new();
    for unit in [100, 10] {
        let (env, dir) = load("db-twin-a").await;
        let (db, table) = ready_to_compact(&env, &dir, unit).await;
        let (outputs, yields) = drive(db.compact(0, &[table]));
        measured.push((unit, outputs.expect("compact").len(), yields));
    }
    let [(_, coarse_tables, coarse), (_, fine_tables, fine)] = measured[..] else {
        unreachable!("two runs")
    };

    assert_eq!(
        coarse_tables, fine_tables,
        "the unit should change the pacing and nothing else"
    );
    assert!(
        fine > coarse,
        "ten to a unit should yield more often than a hundred: {fine} against {coarse}"
    );
}

#[compio::test]
async fn a_unit_of_zero_merges_straight_through() {
    let (env, dir) = load("db-twin-a").await;
    let (db, table) = ready_to_compact(&env, &dir, 0).await;

    let (outputs, yields) = drive(db.compact(0, &[table]));
    outputs.expect("compact");
    assert_eq!(
        yields, 0,
        "a caller that asked for no pacing should get none"
    );
}

#[compio::test]
async fn pacing_changes_no_answer() {
    // The whole of it: a merge cut into slices has to produce the table a
    // merge in one go would have.
    let mut contents = Vec::new();
    for unit in [0, 1, 7, 1000] {
        let (env, dir) = load("db-reads").await;
        // The compaction is this test's own: it names the tables and
        // compares what came out, so the flush that fed it is left to do
        // nothing but flush.
        let db = roble::open_with(
            &env,
            &dir,
            Options::new()
                .compaction_unit(unit)
                .disable_automatic_compactions(true),
        )
        .await
        .expect("open");

        let mut batch = Batch::new();
        for i in 0..100 {
            batch
                .put(format!("z{i:04}").as_bytes(), b"value")
                .expect("put");
        }
        batch.delete_range(b"z0010", b"z0050").expect("range");
        db.write_raw(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");

        let in_l0: Vec<u64> = db
            .versions()
            .version
            .level(0)
            .iter()
            .map(|t| t.file_num)
            .collect();
        db.compact(0, &in_l0).await.expect("compact");

        let mut scan = db.range(..).expect("scan");
        scan.first().await.expect("first");
        let mut seen = Vec::new();
        while scan.is_valid() {
            seen.push((scan.key().to_vec(), scan.value().to_vec()));
            scan.next().await.expect("next");
        }
        assert!(!seen.is_empty());
        contents.push((unit, seen));
    }

    let (_, first) = &contents[0];
    for (unit, seen) in &contents[1..] {
        assert_eq!(
            seen, first,
            "a compaction paced at {unit} gave a different database"
        );
    }
}
