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
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

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

/// How many tables each level holds.
/// How deep L0 is, which is what its allowance is counted in (§9).
fn l0_depth<F: Fs>(db: &roble::Db<F>) -> u64 {
    db.versions().version.l0_sublevels().expect("the sublevels") as u64
}

fn levels<F: Fs>(db: &roble::Db<F>) -> Vec<usize> {
    let versions = db.versions();
    (0..7).map(|l| versions.version.level(l).len()).collect()
}

/// Writes one key and flushes it, giving one L0 table — and whatever the
/// flush then decided to do about it.
async fn write_and_flush<F: Fs>(db: &roble::Db<F>, key: &[u8]) {
    let mut batch = Batch::new();
    batch.put(key, b"value").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");
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
            // The same keys every round, with the round in the value. Every
            // flush then lands a layer above the last, which is what L0's
            // allowance is counted in — twenty flushes of twenty different
            // ranges would be one layer and nothing to settle. And the
            // newest value is what has to come through the merges.
            let key = format!("s{i:02}").into_bytes();
            let value = format!("round {round}").into_bytes();
            batch.put(&key, &value).expect("put");
            model.insert(key, value);
        }
        db.write_raw(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");

        let held = l0_depth(&db);
        assert!(
            held <= db.l0_compaction_threshold(),
            "after round {round} L0 was {held} layers deep, past its allowance of {}",
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

/// Polls two futures turn and turn about until both are done.
fn interleave<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
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

/// Whether a task waiting to be told there is a compaction would be told now.
fn wanted_now<F: Fs>(db: &roble::Db<F>) -> bool {
    let mut cx = Context::from_waker(Waker::noop());
    pin!(db.compaction_wanted())
        .as_mut()
        .poll(&mut cx)
        .is_ready()
}

#[compio::test]
async fn a_flush_hands_the_merge_to_whoever_is_waiting_for_one() {
    // Background compaction, from the store's side of it (§9). A flush that
    // finds somebody waiting to be told there is work tells them and
    // returns, instead of merging where it stands — which is what takes the
    // merge out of the write that caused the flush.
    //
    // Driven by hand rather than spawned, because what is in question is
    // which of the two did the merge and not whether a runtime will poll a
    // task. `tests/open.rs` does it with a real one.
    let (env, dir) = load("db-reads").await;
    // The fixture arrives four layers deep, so five is the line this flush
    // crosses: a store that already wanted a compaction could not show whose
    // the merge was.
    let db = roble::open_with(&env, &dir, Options::new().l0_compaction_threshold(5))
        .await
        .expect("open");
    let mut model = contents(&db).await;
    assert_eq!(
        l0_depth(&db),
        4,
        "the fixture should arrive four layers deep"
    );
    assert!(db.pick().is_none(), "and want nothing yet");

    // Inside a range L0 already holds, so the table this flush writes lands
    // a layer above it rather than beside it (§9).
    let mut batch = Batch::new();
    batch.put(b"a1handed", b"over").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");
    model.insert(b"a1handed".to_vec(), b"over".to_vec());

    let worker = async {
        db.compaction_wanted().await;
        db.compact_once().await
    };
    let (merged, flushed) = interleave(worker, db.flush());
    let merged = merged.expect("the waiting task's compaction");
    flushed.expect("the flush").expect("a table");

    assert!(
        !merged.is_empty(),
        "a task was waiting to be told, so the merge was its to do and the flush should \
         not have done it first"
    );
    assert_eq!(
        db.versions().version.level(0).len(),
        0,
        "and the merge it did should have taken all of L0"
    );
    assert_eq!(contents(&db).await, model, "and changed no answer");
}

#[compio::test]
async fn a_compaction_that_failed_is_not_asked_for_again_until_the_version_moves() {
    // "Later" with no clock to say when (§10). The picker gives the same
    // answer against the same version, so a task told to go again at once
    // would spin rather than retry — the next flush is what makes it a
    // retry.
    let (env, dir) = load("db-reads").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new().disable_automatic_compactions(true),
    )
    .await
    .expect("open");
    assert!(
        db.pick().is_some(),
        "the fixture arrives wanting a compaction"
    );
    assert!(wanted_now(&db), "so a task should be told at once");

    env.sim().fail_at(1);
    db.compact_once()
        .await
        .expect_err("the compaction should have failed");
    env.sim().stop_failing();

    assert!(
        db.pick().is_some(),
        "the picker still wants the very same compaction"
    );
    assert!(
        !wanted_now(&db),
        "but a task should not be told to run it again against the same version"
    );

    // A flush is what moves the version on, and so what makes it a retry.
    let mut batch = Batch::new();
    batch.put(b"moved", b"on").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");

    assert!(
        wanted_now(&db),
        "and a version that has moved on is worth asking the picker about again"
    );
}

#[compio::test]
async fn a_task_waits_for_a_merge_already_running_rather_than_standing_down() {
    // The picker is not told what another compaction has claimed, so it goes
    // on naming those tables and `compact_once` goes on standing down (§9).
    // A task told there was work each time would take that answer, do
    // nothing with it, and be told again at once — a spin, not a wait. So it
    // is not told while somebody else is on it.
    let (env, dir) = load("db-reads").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .disable_automatic_compactions(true)
            .compaction_unit(1),
    )
    .await
    .expect("open");
    assert!(
        db.pick().is_some(),
        "the fixture arrives wanting a compaction"
    );
    assert!(wanted_now(&db), "and nobody is on it yet");

    // One compaction, driven by hand as far as its first suspension, which
    // is past the point where it claims what it is reading.
    let mut cx = Context::from_waker(Waker::noop());
    env.sim().pause_at(1);
    let mut running = pin!(db.compact_once());
    assert!(
        running.as_mut().poll(&mut cx).is_pending(),
        "the compaction should have suspended with its tables claimed"
    );

    assert!(
        db.pick().is_some(),
        "the picker still names the tables it has claimed"
    );
    assert!(
        !wanted_now(&db),
        "but a task should wait for that merge rather than be told to run one"
    );

    // And once it is done, the answer is whatever the picker says again.
    let mut polls = 0;
    let merged = loop {
        if let Poll::Ready(result) = running.as_mut().poll(&mut cx) {
            break result.expect("the compaction");
        }
        polls += 1;
        assert!(polls < 100_000, "the compaction is not making progress");
    };
    assert!(!merged.is_empty(), "it should have written something");
    assert!(
        !wanted_now(&db),
        "and it took all of L0, so there is nothing left to want"
    );
}

/// Whether a task waiting to be told there is a memtable to write out would
/// be told now (§5).
fn flush_wanted_now<F: Fs>(db: &roble::Db<F>) -> bool {
    let mut cx = Context::from_waker(Waker::noop());
    pin!(db.flush_wanted()).as_mut().poll(&mut cx).is_ready()
}

#[compio::test]
async fn a_store_that_says_it_is_finished_stops_making_its_tasks_wait() {
    // The half of "the caller owns stopping them" that the caller cannot
    // write for itself (§9). A parked task is woken by whatever it is parked
    // on and by nothing else, and both of these park on the store — so a
    // store with no way to say "nothing more is coming" is a store its own
    // tasks hold for ever, and a directory nothing unlocks.
    let (env, dir) = load("db-reads").await;
    // The fixture arrives wanting a compaction, and this one is about the
    // waits rather than the work, so neither of them is going to be run.
    let db = roble::open_with(
        &env,
        &dir,
        Options::new().disable_automatic_compactions(true),
    )
    .await
    .expect("open");

    assert!(db.running(), "a store nobody has stopped is running");
    assert!(
        wanted_now(&db),
        "the fixture arrives over its allowance, so this one is told on its merits"
    );
    assert!(
        !flush_wanted_now(&db),
        "and has nothing to write out, so this one waits"
    );

    db.stop();

    assert!(!db.running(), "a store that was stopped is not running");
    assert!(
        flush_wanted_now(&db),
        "and the wait that would otherwise park for ever is over"
    );
    assert!(
        wanted_now(&db),
        "both of them, since both are tasks holding the store"
    );
    assert!(
        db.pick().is_some(),
        "the work itself is untouched — a stopped store is one nobody is listening to, \
         not one that has forgotten what it owes"
    );
}
