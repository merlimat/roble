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

//! The tasks a store spawns for itself, and how it shuts them down (§9).
//!
//! A store opened to write flushes and merges in two tasks of its own. They
//! hold the store's state rather than the caller's handle, so closing and
//! dropping are where this can go wrong: a directory released while a task
//! is still writing in it, or one never released at all.
//!
//! On the simulated filesystem, which never suspends unless told to, so
//! "in flight" is a place these tests choose with `pause_at`.

use std::path::{Path, PathBuf};
use std::task::Poll;

use roble::sim::{SimEnv, SimFs};
use roble::{Db, Error, Fs, Options};
use roble_format::batch::Batch;

const VALUE: &[u8] = &[b'v'; 1024];

/// A budget of a few dozen entries, so the line is a few writes away.
const BUDGET: u64 = 48 << 10;

fn options() -> Options<SimFs> {
    // The WAL off, which is Oxia's configuration and the one where a write
    // does no I/O at all, so the only operations counted are the flush's.
    // Merges left out, so the tasks running are the flush task alone and
    // the tables counted are the ones it wrote.
    Options::new()
        .create_if_missing(true)
        .wal(roble::Wal::Off)
        .disable_automatic_compactions(true)
        .memtable_size(BUDGET)
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

async fn put(db: &Db<SimFs>, i: usize) -> roble::Result<()> {
    let mut batch = Batch::new();
    batch.put(&key(i), VALUE).expect("put");
    db.write_raw(&mut batch, false).await
}

fn in_l0(db: &Db<SimFs>) -> usize {
    db.versions().version.level(0).len()
}

/// Hands the thread to whatever else is ready, once.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

/// Lets the tasks the open spawned run until they park.
async fn settle() {
    for _ in 0..8 {
        yield_now().await;
    }
}

/// Writes until the store holds enough to want a flush, then once more, and
/// returns how many keys that took.
///
/// The one more is the write that tells the flush task and hands it the
/// thread: a write looks at the line before it applies its own batch, so
/// the write that crosses it is not the one that sees it crossed.
async fn cross_the_line(db: &Db<SimFs>) -> usize {
    let mut i = 0;
    while (db.unflushed_bytes() as u64) < db.flush_line() {
        put(db, i).await.expect("write");
        i += 1;
    }
    put(db, i).await.expect("the write that tells the task");
    i + 1
}

/// Leaves the store's flush task inside a flush: one file operation done,
/// and suspended at the next.
///
/// The flush starts on the write that tells the task, but its first
/// operation can come a few turns later, once the writes it froze the
/// memtable under have finished. So the pause is on the second operation,
/// and the turns are given until the first has run: from there the task is
/// parked on the pause, mid-flush, and nothing else is pending to change
/// that until somebody yields again.
async fn into_a_flush(env: &SimEnv, db: &Db<SimFs>) -> usize {
    let before = env.sim().ops();
    env.sim().pause_at(2);
    let written = cross_the_line(db).await;
    for _ in 0..64 {
        if env.sim().ops() > before {
            break;
        }
        yield_now().await;
    }
    assert_eq!(
        env.sim().ops(),
        before + 1,
        "the flush should have done one operation and be paused at its second"
    );
    written
}

async fn open(env: &SimEnv, dir: &Path) -> roble::Result<Db<SimFs>> {
    roble::open_with(env, dir, options()).await
}

#[compio::test]
async fn the_store_writes_out_a_full_memtable_by_itself() {
    // Nobody calls `flush`, and no write reaches the budget: the write that
    // crosses the line tells the store's own task, and that task is what
    // writes the table.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open(&env, &dir).await.expect("open");
    settle().await;

    let mut i = 0;
    while in_l0(&db) == 0 {
        assert!(
            (db.unflushed_bytes() as u64) < BUDGET,
            "a write reached the budget, so the flush was the write's and not the task's"
        );
        put(&db, i).await.expect("write");
        i += 1;
        assert!(i < 1000, "the store never wrote a table out");
    }
    assert_eq!(in_l0(&db), 1);
    db.close().await.expect("close");
}

#[compio::test]
async fn closing_waits_for_the_flush_in_flight_and_then_lets_go() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open(&env, &dir).await.expect("open");
    settle().await;

    let written = into_a_flush(&env, &db).await;
    assert_eq!(in_l0(&db), 0, "the flush should not have landed yet");

    db.close().await.expect("close");

    // Closed, so the directory is free at once, and the flush finished
    // rather than being abandoned: its table is there to open.
    let db = open(&env, &dir).await.expect("reopen right after close");
    assert_eq!(in_l0(&db), 1, "the flush in flight should have landed");
    for i in 0..written - 1 {
        assert!(
            db.get(&key(i), roble::Comparison::Equal)
                .await
                .expect("get")
                .is_some(),
            "key {i} was in the memtable the flush wrote out"
        );
    }
    db.close().await.expect("close");
}

#[compio::test]
async fn dropping_an_idle_store_lets_go_of_the_directory_at_once() {
    // Its tasks are parked, not working, so nothing more will be written in
    // the directory and the drop releases it there and then.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open(&env, &dir).await.expect("open");
    settle().await;
    put(&db, 0).await.expect("write");
    drop(db);

    let db = open(&env, &dir).await.expect("reopen right after the drop");
    drop(db);
}

#[compio::test]
async fn dropping_a_busy_store_keeps_the_directory_until_its_work_is_done() {
    // A task inside a flush holds the lock past the drop: another open now
    // would let a second store into a directory files are still being
    // written in.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open(&env, &dir).await.expect("open");
    settle().await;
    into_a_flush(&env, &db).await;
    assert_eq!(in_l0(&db), 0, "the flush should not have landed yet");
    drop(db);

    // Asked of the lock itself, which does not suspend: an open would hand
    // the task the turns it needs to finish first, and then succeed. The
    // pause the task is parked on is moved out of the way first, or this
    // call, reaching for the same operation number, would yield to it too.
    env.sim().pause_at(1 << 40);
    let held = env.sim().lock(&dir.join("LOCK")).await;
    assert!(
        matches!(held, Err(Error::Locked)),
        "the flush in flight still owns the directory: {held:?}"
    );

    // Once the task has had its turns, the flush is done, the task has seen
    // the store stopped and ended, and the lock went with the state.
    let mut reopened = None;
    for _ in 0..64 {
        yield_now().await;
        if let Ok(db) = open(&env, &dir).await {
            reopened = Some(db);
            break;
        }
    }
    let db = reopened.expect("the directory should be released once the flush is done");
    assert_eq!(in_l0(&db), 1, "and the flush should have landed");
    db.close().await.expect("close");
}

#[compio::test]
async fn a_background_flush_that_fails_is_tried_again_after_the_next_write() {
    // Nobody is there to be handed the error. The store takes note, does not
    // ask for the same flush again until something has changed, and the
    // next write is that change.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open(&env, &dir).await.expect("open");
    settle().await;

    env.sim().fail_at(1);
    let written = cross_the_line(&db).await;
    settle().await;
    assert_eq!(in_l0(&db), 0, "the flush should have failed");

    put(&db, written).await.expect("the next write");
    settle().await;
    assert_eq!(in_l0(&db), 1, "and been written out after the next write");
    db.close().await.expect("close");
}

#[compio::test]
async fn destroying_a_store_waits_for_its_flush_before_removing_files() {
    // A flush that finished after the files were removed would write a
    // table into a database that is meant to be gone.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open(&env, &dir).await.expect("open");
    settle().await;
    into_a_flush(&env, &db).await;
    assert_eq!(in_l0(&db), 0, "the flush should not have landed yet");

    db.destroy().await.expect("destroy");
    settle().await;
    let left = env.sim().list(&dir).await.expect("list");
    assert!(
        left.is_empty(),
        "nothing should be written after a destroy: {:?}",
        left.iter().map(|e| &e.name).collect::<Vec<_>>()
    );
}

#[compio::test]
async fn a_store_opened_to_read_runs_no_tasks() {
    // A reader never flushes or compacts, so it has nothing to spawn and
    // nothing for closing to wait on.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open(&env, &dir).await.expect("open");
    put(&db, 0).await.expect("write");
    db.close().await.expect("close");

    let ro = roble::open_read_only(&env, &dir, Options::new())
        .await
        .expect("open read-only");
    ro.close().await.expect("close");
}

#[compio::test]
async fn closing_finishes_the_flush_in_flight_without_starting_a_merge() {
    // A flush that lands with nobody waiting for the merge it makes due runs
    // that merge where it stands. While the store is closing nobody is: the
    // merge task has been told to end. So the flush closing waits for has to
    // stop at the flush, or closing would wait out a whole merge it started.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let merging = || {
        options()
            .disable_automatic_compactions(false)
            .l0_compaction_threshold(2)
    };
    let db = roble::open_with(&env, &dir, merging()).await.expect("open");
    settle().await;

    // One table in L0, one short of the line, written by hand. Its key falls
    // inside the range the next flush writes, so the two overlap and count
    // as two layers rather than one (§9).
    put(&db, 5).await.expect("write");
    db.flush().await.expect("flush").expect("a table");
    settle().await;
    assert_eq!(
        in_l0(&db),
        1,
        "one table is below the line, so nothing merged"
    );

    // The second, in flight when the store closes.
    into_a_flush(&env, &db).await;
    assert_eq!(in_l0(&db), 1, "the flush should not have landed yet");
    db.close().await.expect("close");

    let db = roble::open_with(&env, &dir, merging().background_tasks(false))
        .await
        .expect("reopen");
    assert_eq!(
        in_l0(&db),
        2,
        "the flush landed and nothing merged it: L0 holds both tables"
    );
}
