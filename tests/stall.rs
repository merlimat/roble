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

//! A write that waits for room somebody else is making (§9).
//!
//! Two writes on one store, both finding it at its budget. One of them makes
//! the room; the other has nothing useful to do until that is finished, so it
//! waits and then asks again — Pebble's loop in `maybeInduceWriteStall`. The
//! second look is what these tests are about: a write that queued for the
//! permit instead would take it the moment it was free and flush a memtable
//! the first write had just emptied, which is a table nobody needed and a
//! merge nobody asked for.
//!
//! Swept rather than aimed at one operation: where the first write suspends
//! decides which of these two is waiting on what, and that is not these
//! tests' business to know.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// The value every test writes, long enough that a budget can be stated in
/// whole entries.
const VALUE: &[u8] = b"0123456789abcdef0123456789abcdef";

/// How many entries the budget in these tests holds.
const ENTRIES: u64 = 10;

/// Polls two futures turn and turn about until both are done.
///
/// The worst interleaving a single-threaded executor can produce, and the one
/// a caller with two tasks on one store will eventually find. The simulated
/// filesystem never blocks, so every `Pending` below comes from roble.
fn interleave<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    // The waker does nothing: this loop polls again whatever it is told, so
    // nothing here depends on wake-ups being delivered. That they are is
    // `tests/change.rs`'s subject.
    let mut cx = Context::from_waker(Waker::noop());
    let mut a = pin!(a);
    let mut b = pin!(b);
    let (mut first, mut second) = (None, None);
    let mut polls = 0;
    loop {
        if first.is_none()
            && let Poll::Ready(v) = a.as_mut().poll(&mut cx)
        {
            first = Some(v);
        }
        if second.is_none()
            && let Poll::Ready(v) = b.as_mut().poll(&mut cx)
        {
            second = Some(v);
        }
        (first, second) = match (first, second) {
            (Some(a), Some(b)) => return (a, b),
            pair => pair,
        };
        polls += 1;
        assert!(polls < 100_000, "one of these two is not making progress");
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

fn key_at(i: usize) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

/// What one `put` adds to the arena: the internal key, which is the user key
/// and an eight-byte trailer, and the value.
fn entry_cost() -> u64 {
    (key_at(0).len() + 8 + VALUE.len()) as u64
}

/// Compaction left to the caller, so that the tables these tests count are
/// the ones the flushes wrote.
fn opened(wal: bool, budget: u64) -> Options<SimFs> {
    let o = Options::new()
        .memtable_size(budget)
        .disable_automatic_compactions(true);
    if wal { o } else { o.disable_wal(true) }
}

async fn put<F: Fs>(db: &roble::Db<F>, i: usize, wal: bool) -> roble::Result<()> {
    let mut batch = Batch::new();
    batch.put(&key_at(i), VALUE).expect("put");
    db.write_raw(&mut batch, wal).await
}

/// Writes one key of a range the writes keep coming back to, so that every
/// flush covers the same keys and its table lands a layer above the last.
///
/// L0 is measured in the layers its tables form (§9), so a test that wants a
/// deep L0 has to write a deep one. Keys that march forward make tables that
/// do not overlap, and any number of those is one layer and one seek.
async fn put_overlapping<F: Fs>(db: &roble::Db<F>, i: usize, wal: bool) -> roble::Result<()> {
    put(db, i % ENTRIES as usize, wal).await
}

/// How deep L0 is, which is what the thresholds are counted in.
fn l0_depth<F: Fs>(db: &roble::Db<F>) -> u64 {
    db.versions().version.l0_sublevels().expect("the sublevels") as u64
}

/// Writes until the store is at `budget`, and says how many keys that took.
///
/// Capped, because a store that flushed on every write would never reach the
/// line and a test that waited for it would hang rather than fail.
async fn fill_to<F: Fs>(db: &roble::Db<F>, budget: u64, wal: bool) -> usize {
    let mut i = 0;
    while (db.unflushed_bytes() as u64) < budget {
        put(db, i, wal).await.expect("write");
        i += 1;
        assert!(
            i <= 4 * ENTRIES as usize,
            "{i} writes have not filled a budget of {budget} bytes"
        );
    }
    i
}

#[compio::test]
async fn two_writes_at_once_make_room_once_between_them() {
    for wal in [true, false] {
        let budget = ENTRIES * entry_cost();

        // How long the write that makes room takes, so the sweep knows how
        // many places there are to suspend it.
        let total = {
            let (env, dir) = load("db-twin-a").await;
            let db = roble::open_with(&env, &dir, opened(wal, budget))
                .await
                .expect("open");
            let i = fill_to(&db, budget, wal).await;
            let before = env.sim().ops();
            put(&db, i, wal).await.expect("the write that makes room");
            env.sim().ops() - before
        };
        assert!(
            total >= 4,
            "wal={wal}: a write of {total} operations is too thin to sweep"
        );

        for n in 1..=total {
            let (env, dir) = load("db-twin-a").await;
            let db = roble::open_with(&env, &dir, opened(wal, budget))
                .await
                .expect("open");
            let mut model = contents(&db).await;
            let filled = fill_to(&db, budget, wal).await;
            for j in 0..filled {
                model.insert(key_at(j), VALUE.to_vec());
            }
            let tables_before = db.versions().version.table_count();
            env.sim().pause_at(n);

            let (first, second) = interleave(put(&db, filled, wal), put(&db, filled + 1, wal));
            first.unwrap_or_else(|e| panic!("wal={wal}: the first write, paused at {n}: {e}"));
            second.unwrap_or_else(|e| panic!("wal={wal}: the second write, paused at {n}: {e}"));
            model.insert(key_at(filled), VALUE.to_vec());
            model.insert(key_at(filled + 1), VALUE.to_vec());

            assert_eq!(
                db.versions().version.table_count(),
                tables_before + 1,
                "wal={wal}: paused at {n} of {total}, one write made the room and the other \
                 waited for it, so between them they should have written one table"
            );
            assert_eq!(
                contents(&db).await,
                model,
                "wal={wal}: paused at {n} of {total}"
            );

            if wal {
                drop(db);
                let reopened = roble::open_with(&env, &dir, opened(wal, u64::MAX))
                    .await
                    .unwrap_or_else(|e| panic!("paused at {n} of {total}, the reopen: {e}"));
                assert_eq!(
                    contents(&reopened).await,
                    model,
                    "paused at {n} of {total}, reopened"
                );
            }
        }
    }
}

/// Whether a task waiting to be told there is a memtable to write out would
/// be told now (§5).
fn flush_wanted_now<F: Fs>(db: &roble::Db<F>) -> bool {
    let mut cx = Context::from_waker(Waker::noop());
    pin!(db.flush_wanted()).as_mut().poll(&mut cx).is_ready()
}

/// Writes until the store is at `budget`, and says how many keys that took —
/// without letting the line below the budget write anything out.
///
/// The same as [`fill_to`] for a store nobody is waiting on, which is what
/// every caller of it here is.
async fn fill_past_the_line<F: Fs>(db: &roble::Db<F>, wal: bool) -> usize {
    assert!(
        !flush_wanted_now(db),
        "an empty store should have nothing to write out"
    );
    let mut i = 0;
    while (db.unflushed_bytes() as u64) < db.flush_line() {
        put(db, i, wal).await.expect("write");
        i += 1;
        assert!(
            i <= 4 * ENTRIES as usize,
            "{i} writes have not reached the line"
        );
    }
    i
}

#[compio::test]
async fn a_write_says_there_is_a_memtable_to_write_out_before_the_budget() {
    // The line, which is what takes a flush off the write that would
    // otherwise have had to make the room (§5). An eighth of the budget
    // before it fills, a write says so — and says it without writing
    // anything out itself, because the whole point is that somebody else
    // does that while the writes carry on into what is left.
    for wal in [true, false] {
        let budget = ENTRIES * entry_cost();
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(&env, &dir, opened(wal, budget))
            .await
            .expect("open");

        let filled = fill_past_the_line(&db, wal).await;
        let tables = db.versions().version.table_count();
        let held = db.unflushed_bytes() as u64;

        assert!(
            flush_wanted_now(&db),
            "wal={wal}: {held} bytes is at the line of {}, so a task waiting to be told \
             there is a memtable to write out should have been",
            db.flush_line()
        );
        assert!(
            held < budget,
            "wal={wal}: and should have been told at {held} bytes, before the budget of \
             {budget} a write stalls at"
        );
        assert_eq!(
            db.versions().version.table_count(),
            tables,
            "wal={wal}: while the writes that said so wrote no table themselves"
        );
        assert!(
            filled > 0,
            "wal={wal}: the line should take more than no writes to reach"
        );
    }
}

#[compio::test]
async fn a_write_hands_the_flush_to_whoever_is_waiting_for_one() {
    // The handoff itself. A task parked on `flush_wanted` writes the
    // memtable out while the write that told it about it carries on, so the
    // store never reaches the budget and no write ever waits.
    //
    // Driven by hand rather than spawned, because what is in question is
    // which of the two wrote the table and not whether a runtime will poll
    // a task.
    for wal in [true, false] {
        let budget = ENTRIES * entry_cost();
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(&env, &dir, opened(wal, budget))
            .await
            .expect("open");
        let mut model = contents(&db).await;
        let filled = fill_past_the_line(&db, wal).await;
        for j in 0..filled {
            model.insert(key_at(j), VALUE.to_vec());
        }
        let tables = db.versions().version.table_count();
        assert!(
            (db.unflushed_bytes() as u64) < budget,
            "wal={wal}: the line has to come before the budget, or the write below would \
             be making its own room rather than being handed room"
        );

        let worker = async {
            db.flush_wanted().await;
            db.flush().await
        };
        let (flushed, written) = interleave(worker, put(&db, filled, wal));
        written.unwrap_or_else(|e| panic!("wal={wal}: the write: {e}"));
        flushed
            .unwrap_or_else(|e| panic!("wal={wal}: the waiting task's flush: {e}"))
            .unwrap_or_else(|| panic!("wal={wal}: the task should have written a table"));
        model.insert(key_at(filled), VALUE.to_vec());

        assert_eq!(
            db.versions().version.table_count(),
            tables + 1,
            "wal={wal}: one table between them, and it was the task's"
        );
        assert!(
            (db.unflushed_bytes() as u64) < budget,
            "wal={wal}: and the store never reached the budget of {budget}"
        );
        assert_eq!(
            contents(&db).await,
            model,
            "wal={wal}: and no answer changed"
        );
    }
}

#[compio::test]
async fn a_store_with_nobody_waiting_keeps_the_whole_budget_for_one_memtable() {
    // The line is room kept back for a flush running *beside* the writes,
    // so a store with nowhere to put that flush should not keep it back at
    // all: it fills the budget as it always did, and the write that finds it
    // full writes the memtable out. Keeping an eighth back here would be a
    // store writing tables an eighth smaller for nothing, which costs it
    // compactions it did not need (`crate::memtable::FLUSH_RESERVE`).
    for wal in [true, false] {
        let budget = ENTRIES * entry_cost();
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(&env, &dir, opened(wal, budget))
            .await
            .expect("open");
        let tables = db.versions().version.table_count();

        // Every write up to the budget, with nobody parked on the line.
        let filled = fill_to(&db, budget, wal).await;
        assert_eq!(
            db.versions().version.table_count(),
            tables,
            "wal={wal}: nothing should have been written out below the budget"
        );
        assert_eq!(
            db.unflushed_bytes() as u64 / entry_cost(),
            ENTRIES,
            "wal={wal}: and the memtable should hold the whole budget, not an eighth less"
        );

        put(&db, filled, wal)
            .await
            .expect("the write that made room");
        assert_eq!(
            db.versions().version.table_count(),
            tables + 1,
            "wal={wal}: and the write that found it full should have written it out"
        );
    }
}

#[compio::test]
async fn the_budget_stops_a_store_whose_flushes_nobody_takes() {
    // The backstop, which is the half of this that is not negotiable (§9).
    // A task is parked on the line and then never polled again — a worker
    // that has stopped, or one whose runtime is busy elsewhere — so every
    // write after the line is a write nobody is making room for. The store
    // has to refuse to grow past its budget rather than pile memtables up,
    // which means the write that finds it full writes one out itself, as it
    // did before there was anywhere else for a flush to go.
    for wal in [true, false] {
        let budget = ENTRIES * entry_cost();
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(&env, &dir, opened(wal, budget))
            .await
            .expect("open");
        let mut model = contents(&db).await;

        // Parked once and abandoned: the waker is a no-op, so nothing that
        // follows can wake it and nothing polls it again.
        let mut parked = pin!(db.flush_wanted());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            parked.as_mut().poll(&mut cx).is_pending(),
            "wal={wal}: an empty store has no memtable to write out"
        );

        let mut tables = db.versions().version.table_count();
        let mut wrote = 0;
        for i in 0..4 * ENTRIES as usize {
            put(&db, i, wal).await.expect("write");
            model.insert(key_at(i), VALUE.to_vec());
            let held = db.unflushed_bytes() as u64;
            assert!(
                held <= budget,
                "wal={wal}: after {} writes the store holds {held} bytes, past the budget \
                 of {budget} — a swap nobody takes is still a swap that has to stop",
                i + 1
            );
            let now = db.versions().version.table_count();
            wrote += usize::from(now > tables);
            tables = now;
        }

        assert!(
            wrote >= 2,
            "wal={wal}: the writes should have written their own memtables out, and {wrote} \
             tables is not a store keeping itself inside a budget"
        );
        assert_eq!(
            contents(&db).await,
            model,
            "wal={wal}: and every key answers"
        );
    }
}

#[compio::test]
async fn a_task_waits_for_the_flush_underway_rather_than_queueing_to_repeat_it() {
    // The same second look `make_room` takes, from the task's side (§5).
    // A change already underway may be the very flush this task would run,
    // so it waits for that one and asks again — Pebble's loop in
    // `maybeInduceWriteStall`. A task that queued for the permit instead
    // would take it the moment it was free and write out a memtable the
    // flush in front had just emptied, which is a table nobody needed and a
    // merge nobody asked for.
    let budget = ENTRIES * entry_cost();
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, opened(false, budget))
        .await
        .expect("open");
    fill_past_the_line(&db, false).await;
    assert!(flush_wanted_now(&db), "the line was reached");

    // A flush suspended at its first operation, which is a flush holding the
    // permit: the simulated filesystem never blocks, so this is the only way
    // a change stays underway across a poll (§12).
    env.sim().pause_at(1);
    let mut cx = Context::from_waker(Waker::noop());
    let mut flushing = pin!(db.flush());
    assert!(
        flushing.as_mut().poll(&mut cx).is_pending(),
        "the flush should have paused part-way, with the permit in its hand"
    );

    assert!(
        !flush_wanted_now(&db),
        "the store is still at the line, but what is at it is the memtable the flush \
         underway has already frozen — so the answer is to wait for that one"
    );

    let mut polls = 0;
    let table = loop {
        if let Poll::Ready(done) = flushing.as_mut().poll(&mut cx) {
            break done;
        }
        polls += 1;
        assert!(polls < 1_000, "the paused flush is not making progress");
    };
    table.expect("the flush").expect("a table");

    assert!(
        !flush_wanted_now(&db),
        "and asking again once it has finished finds the room already made"
    );
}

#[compio::test]
async fn a_flush_that_failed_is_not_asked_for_again_until_a_write() {
    // The same rule a compaction that failed lives by (§9), for the same
    // reason: the picker — here, the line — gives the same answer against
    // the same state, so a task told to go again at once would spin rather
    // than retry. A write is the only thing that changes what a flush would
    // write, which is what makes the next one a retry and not a repeat, and
    // it is "later" with no clock to say when (§10).
    let budget = ENTRIES * entry_cost();
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, opened(false, budget))
        .await
        .expect("open");
    let filled = fill_past_the_line(&db, false).await;
    assert!(flush_wanted_now(&db), "the line was reached");

    // The first thing the flush touches is the table it creates.
    env.sim().fail_at(1);
    db.flush().await.expect_err("the flush should have failed");
    env.sim().stop_failing();

    assert!(
        (db.unflushed_bytes() as u64) >= db.flush_line(),
        "the failed flush should have left the memtable it froze behind, so the store is \
         still at the line and would say yes again on that alone"
    );
    assert!(
        !flush_wanted_now(&db),
        "and asking for the same flush again against the same store would be a spin"
    );

    put(&db, filled, false).await.expect("write");
    assert!(
        flush_wanted_now(&db),
        "a write is what makes it a retry, and the store has taken one"
    );
}

#[compio::test]
async fn a_write_with_room_to_spare_never_waits_for_anybody() {
    // The other half. A store under its budget makes no room and so has
    // nothing for a second write to wait on, whatever else is going on: a
    // stall every write paid would be a store that serialized its writes
    // behind each other for no reason.
    for wal in [true, false] {
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(&env, &dir, opened(wal, u64::MAX))
            .await
            .expect("open");
        let mut model = contents(&db).await;
        let tables_before = db.versions().version.table_count();
        env.sim().pause_at(1);

        let (first, second) = interleave(put(&db, 0, wal), put(&db, 1, wal));
        first.expect("the first write");
        second.expect("the second write");
        model.insert(key_at(0), VALUE.to_vec());
        model.insert(key_at(1), VALUE.to_vec());

        assert_eq!(
            db.versions().version.table_count(),
            tables_before,
            "wal={wal}: neither write should have flushed anything"
        );
        assert_eq!(contents(&db).await, model, "wal={wal}");
    }
}

/// Where L0's depth stops a write in the tests below.
const LINE: u64 = 3;

#[compio::test]
async fn l0_never_passes_the_line_a_write_waits_at() {
    // Writes that keep flushing, with the picker told to *want* L0 compacted
    // only long after a write would wait for it — so the line is the only
    // thing that can bring L0 down, and whether it does is the whole
    // question.
    let budget = ENTRIES * entry_cost();
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .memtable_size(budget)
            .l0_compaction_threshold(100)
            .l0_stop_writes_threshold(LINE),
    )
    .await
    .expect("open");
    let mut model = contents(&db).await;

    for i in 0..8 * ENTRIES as usize {
        put_overlapping(&db, i, true).await.expect("write");
        model.insert(key_at(i % ENTRIES as usize), VALUE.to_vec());
        let in_l0 = l0_depth(&db);
        assert!(
            in_l0 < LINE,
            "after {} writes L0 is {in_l0} layers deep, which is at or past the line of \
             {LINE} a write is supposed to wait at",
            i + 1
        );
    }

    let below = {
        let versions = db.versions();
        versions
            .version
            .levels()
            .any(|(level, tables)| level > 0 && !tables.is_empty())
    };
    assert!(below, "nothing was ever merged out of L0");
    assert_eq!(contents(&db).await, model, "and a merge changed an answer");
}

#[compio::test]
async fn a_write_drains_an_l0_it_opened_onto() {
    // A store can be handed a deep L0 rather than build one: a directory
    // another store filled with its compactions turned off, or one Pebble
    // left that way. Nothing runs on open, so the first write is what finds
    // it — which is the backstop the line is there to be, and the one path
    // to it that a flush's own compaction has not already taken.
    let budget = ENTRIES * entry_cost();
    let (env, dir) = load("db-twin-a").await;

    let deep = {
        let db = roble::open_with(&env, &dir, opened(true, budget))
            .await
            .expect("open");
        for i in 0..5 * ENTRIES as usize {
            put_overlapping(&db, i, true).await.expect("write");
        }
        db.flush().await.expect("flush");
        let n = l0_depth(&db);
        assert!(
            n >= 4,
            "the setup should have left L0 deeper than this: {n} layers"
        );
        n
    };

    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .memtable_size(budget)
            .l0_compaction_threshold(100)
            .l0_stop_writes_threshold(deep),
    )
    .await
    .expect("reopen");
    let mut model = contents(&db).await;
    assert_eq!(
        l0_depth(&db),
        deep,
        "the reopen should have found L0 as deep as it was left"
    );

    put(&db, 500, true).await.expect("write");
    model.insert(key_at(500), VALUE.to_vec());

    assert!(
        l0_depth(&db) < deep,
        "the write should have drained L0 before its batch was accepted, and L0 is {} \
         layers deep",
        l0_depth(&db)
    );
    assert_eq!(
        contents(&db).await,
        model,
        "and the drain changed an answer"
    );
}

#[compio::test]
async fn a_write_drains_an_l0_exactly_at_the_line() {
    // Every table in L0 spans the same keys, so each is a layer of its own
    // and L0 is exactly as many layers deep as it has tables. A write skips
    // counting the layers when there are fewer tables than the line, because
    // there cannot be more layers than tables; at the line itself it may
    // not, and the write has to find L0 too deep and drain it.
    const TABLES: u64 = 4;
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, opened(true, 64 << 20))
        .await
        .expect("open");
    let mut model = contents(&db).await;
    let before = db.versions().version.level(0).len() as u64;
    for round in 0..TABLES {
        let mut batch = Batch::new();
        for key in [&b"a-first"[..], b"z-last"] {
            let value = format!("round {round}");
            batch.put(key, value.as_bytes()).expect("put");
            model.insert(key.to_vec(), value.into_bytes());
        }
        db.write_raw(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
    }
    drop(db);

    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .l0_compaction_threshold(100)
            .l0_stop_writes_threshold(before + TABLES),
    )
    .await
    .expect("reopen");
    let tables = db.versions().version.level(0).len() as u64;
    assert_eq!(
        (tables, l0_depth(&db)),
        (before + TABLES, before + TABLES),
        "the setup should leave L0 one layer per table, at the line"
    );

    put(&db, 500, true).await.expect("write");
    model.insert(key_at(500), VALUE.to_vec());
    assert!(
        l0_depth(&db) < before + TABLES,
        "a write at the line should have drained L0, and it is {} layers deep",
        l0_depth(&db)
    );
    assert_eq!(
        contents(&db).await,
        model,
        "and the drain changed an answer"
    );
}

#[compio::test]
async fn a_store_that_owns_its_compactions_is_not_stopped_by_l0() {
    // A caller that turned compaction off owns L0's depth as well. A write
    // that waited here would be waiting for a merge nobody was going to run,
    // which is what Pebble does in this configuration and is a worse answer
    // than letting the caller have what it asked for.
    let budget = ENTRIES * entry_cost();
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .memtable_size(budget)
            .disable_automatic_compactions(true)
            .l0_stop_writes_threshold(LINE),
    )
    .await
    .expect("open");
    let mut model = contents(&db).await;

    for i in 0..5 * ENTRIES as usize {
        put_overlapping(&db, i, true).await.expect("write");
        model.insert(key_at(i % ENTRIES as usize), VALUE.to_vec());
    }

    let in_l0 = l0_depth(&db);
    assert!(
        in_l0 > LINE,
        "L0 should have been left to grow past the line, and is {in_l0} layers deep"
    );
    assert_eq!(contents(&db).await, model, "and still answer every key");
}

#[compio::test]
async fn the_default_line_is_the_one_a_deep_l0_stops_at() {
    // The default carrying its weight, which is why it is a default and not
    // only a knob. The picker is told to want L0 compacted only long after a
    // write would wait for it, so L0 grows with every flush and the line is
    // the only thing that can turn the picker round — and what this pins is
    // where that line is.
    {
        // The two lines in the right order, on a store that overrode
        // neither. A line below the one the picker starts wanting L0
        // compacted at would make every flush an L0 merge and nothing else
        // would ever be compacted at all.
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open(&env, &dir).await.expect("open");
        assert!(
            db.l0_stop_writes_threshold() > db.l0_compaction_threshold(),
            "the line a write waits at, {}, has to sit above the one the picker starts \
             wanting L0 compacted at, {}",
            db.l0_stop_writes_threshold(),
            db.l0_compaction_threshold()
        );
    }

    let budget = ENTRIES * entry_cost();
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .memtable_size(budget)
            .l0_compaction_threshold(100),
    )
    .await
    .expect("open");
    let line = db.l0_stop_writes_threshold();
    let mut deepest = 0;

    for i in 0..20 * ENTRIES as usize {
        put_overlapping(&db, i, true).await.expect("write");
        let in_l0 = l0_depth(&db);
        deepest = deepest.max(in_l0);
        assert!(
            in_l0 < line,
            "after {} writes L0 is {in_l0} layers deep, at or past the default line of \
             {line}",
            i + 1
        );
    }
    assert!(
        deepest + 2 >= line,
        "L0 never came near the default line of {line}, reaching {deepest}, so nothing \
         here was stopped by it"
    );
}

#[compio::test]
async fn a_budget_no_flush_can_meet_does_not_spin() {
    // Zero is a budget nothing can meet: the memtable is already empty, so a
    // flush would write no table and there is no room to be made. Saying so
    // is what keeps the loop from asking for ever.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, opened(true, 0))
        .await
        .expect("open");
    // Whatever the fixture's logs left, written out, so the memtable really
    // is empty when the write below asks for room.
    db.flush().await.expect("flush");
    let mut model = contents(&db).await;

    for i in 0..3 {
        put(&db, i, true).await.expect("write");
        model.insert(key_at(i), VALUE.to_vec());
    }
    assert_eq!(
        contents(&db).await,
        model,
        "every write should have gone through"
    );
}

#[compio::test]
async fn a_line_no_merge_can_meet_does_not_spin() {
    // The same for L0: a line of zero is one an empty L0 is already at, and
    // there is no merge that would bring it lower. The picker says there is
    // nothing to pick, and a write that did not take that for an answer
    // would ask for ever.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .memtable_size(u64::MAX)
            .l0_compaction_threshold(100)
            .l0_stop_writes_threshold(0),
    )
    .await
    .expect("open");

    // L0 emptied, so that the line of zero is met and nothing is pickable.
    let l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    if !l0.is_empty() {
        db.compact(0, &l0).await.expect("compact");
    }
    assert!(
        db.versions().version.level(0).is_empty(),
        "L0 should be empty for this"
    );
    let mut model = contents(&db).await;

    for i in 0..3 {
        put(&db, i, true).await.expect("write");
        model.insert(key_at(i), VALUE.to_vec());
    }
    assert_eq!(
        contents(&db).await,
        model,
        "every write should have gone through"
    );
}

#[compio::test]
async fn a_wide_l0_stops_no_write_however_many_tables_it_holds() {
    // The other side of `l0_never_passes_the_line_a_write_waits_at`, and the
    // reason the line is counted in layers: writes that march forward leave
    // L0 wide rather than deep, and a read crosses the lot in one seek. A
    // store that stalled on the table count would be stopping its writer for
    // a backlog that costs a read nothing.
    let budget = ENTRIES * entry_cost();
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .memtable_size(budget)
            .l0_compaction_threshold(100)
            .l0_stop_writes_threshold(LINE),
    )
    .await
    .expect("open");
    let mut model = contents(&db).await;

    for i in 0..8 * ENTRIES as usize {
        put(&db, i, true).await.expect("write");
        model.insert(key_at(i), VALUE.to_vec());
    }

    let tables = db.versions().version.level(0).len() as u64;
    assert!(
        tables > LINE,
        "L0 should have grown wider than the line, and holds {tables} tables"
    );
    assert_eq!(l0_depth(&db), 1, "all of them in the one layer");
    assert_eq!(contents(&db).await, model, "and every key still answers");
}

#[compio::test]
async fn a_wide_l0_the_picker_wants_is_still_not_something_a_write_waits_for() {
    // The line a write waits at is counted in layers, and this is where that
    // shows. L0 here is wide and shallow — more tables than the line, fewer
    // layers — and the picker does want it compacted. A write that measured
    // the line in tables would find itself past it, ask the picker, get an
    // answer, and pay for a merge that no read was waiting on.
    let (env, dir) = load("db-twin-a").await;
    let wide = {
        let db = roble::open_with(&env, &dir, opened(true, u64::MAX))
            .await
            .expect("open");
        for i in 0..6 {
            let mut batch = Batch::new();
            batch
                .put(format!("w{i:02}").as_bytes(), VALUE)
                .expect("put");
            db.write_raw(&mut batch, true).await.expect("write");
            db.flush().await.expect("flush").expect("a table");
        }
        db.versions().version.level(0).len() as u64
    };

    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .memtable_size(u64::MAX)
            .l0_compaction_threshold(1)
            .l0_stop_writes_threshold(5),
    )
    .await
    .expect("reopen");
    assert_eq!(l0_depth(&db), 1, "wide, and one layer deep");
    assert!(wide >= 5, "and wider than the line of five: {wide} tables");
    assert!(
        db.pick().is_some(),
        "the picker does want it compacted, so a write that asked would be given a merge"
    );

    let mut batch = Batch::new();
    batch.put(b"after", VALUE).expect("put");
    db.write_raw(&mut batch, true).await.expect("write");

    assert_eq!(
        db.versions().version.level(0).len() as u64,
        wide,
        "the write neither flushed nor waited, so it should have compacted nothing"
    );
}
