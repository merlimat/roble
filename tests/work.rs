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

//! A compaction that hands the thread back.
//!
//! "Cooperative" is a claim about something that does not happen — nothing
//! else waits — so a test of it has to look at the future itself rather than
//! at the answer it eventually gives. These poll the compaction by hand and
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
async fn ready_to_compact<'a>(
    env: &'a SimEnv,
    dir: &Path,
    unit: u64,
) -> (roble::Opened<'a, SimFs>, u64) {
    let db = roble::open_with(env, dir, Options::new().compaction_unit(unit))
        .await
        .expect("open");
    let mut batch = Batch::new();
    for i in 0..300 {
        batch
            .put(format!("y{i:04}").as_bytes(), b"value")
            .expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");
    (db, table)
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
        let db = roble::open_with(&env, &dir, Options::new().compaction_unit(unit))
            .await
            .expect("open");

        let mut batch = Batch::new();
        for i in 0..100 {
            batch
                .put(format!("z{i:04}").as_bytes(), b"value")
                .expect("put");
        }
        batch.delete_range(b"z0010", b"z0050").expect("range");
        db.write(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");

        let in_l0: Vec<u64> = db
            .versions()
            .version
            .level(0)
            .iter()
            .map(|t| t.file_num)
            .collect();
        db.compact(0, &in_l0).await.expect("compact");

        let mut scan = db.scan().await.expect("scan");
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
