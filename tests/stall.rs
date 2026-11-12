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

async fn put<F: Fs>(db: &roble::Opened<F>, i: usize, wal: bool) -> roble::Result<()> {
    let mut batch = Batch::new();
    batch.put(&key_at(i), VALUE).expect("put");
    db.write(&mut batch, wal).await
}

/// Writes until the store is at `budget`, and says how many keys that took.
///
/// Capped, because a store that flushed on every write would never reach the
/// line and a test that waited for it would hang rather than fail.
async fn fill_to<F: Fs>(db: &roble::Opened<F>, budget: u64, wal: bool) -> usize {
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
        put(&db, i, true).await.expect("write");
        model.insert(key_at(i), VALUE.to_vec());
        let in_l0 = db.versions().version.level(0).len() as u64;
        assert!(
            in_l0 < LINE,
            "after {} writes L0 holds {in_l0} tables, which is at or past the line of \
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
            put(&db, i, true).await.expect("write");
        }
        db.flush().await.expect("flush");
        let n = db.versions().version.level(0).len() as u64;
        assert!(
            n >= 4,
            "the setup should have left L0 deeper than this: {n}"
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
        db.versions().version.level(0).len() as u64,
        deep,
        "the reopen should have found L0 as it was left"
    );

    put(&db, 500, true).await.expect("write");
    model.insert(key_at(500), VALUE.to_vec());

    assert!(
        (db.versions().version.level(0).len() as u64) < deep,
        "the write should have drained L0 before its batch was accepted, and L0 holds {}",
        db.versions().version.level(0).len()
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
        put(&db, i, true).await.expect("write");
        model.insert(key_at(i), VALUE.to_vec());
    }

    let in_l0 = db.versions().version.level(0).len() as u64;
    assert!(
        in_l0 > LINE,
        "L0 should have been left to grow past the line, and holds {in_l0}"
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
        put(&db, i, true).await.expect("write");
        let in_l0 = db.versions().version.level(0).len() as u64;
        deepest = deepest.max(in_l0);
        assert!(
            in_l0 < line,
            "after {} writes L0 holds {in_l0} tables, at or past the default line of {line}",
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
