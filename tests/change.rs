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

//! Two of a store's own operations at once (§5).
//!
//! Every test here drives two futures by hand, a poll each, turn and turn
//! about — which is the worst interleaving a single-threaded executor can
//! produce and the one a caller with two tasks on one store will eventually
//! find. The simulated filesystem never blocks, so every `Pending` below
//! comes from roble: from the pacer, or from the permit one of the two is
//! waiting on.
//!
//! What each test asserts in the end is the same thing: the database still
//! opens, and it holds what was written to it. The interesting assertions
//! are the ones in between, about which of the two did the work.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// Polls two futures turn and turn about until both are done.
fn interleave<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    // The waker does nothing: this loop polls again whatever it is told, so
    // nothing here depends on the permit's wake-ups being delivered.
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

/// A waker that only records that it was woken.
struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Drives `futures` the way an executor does: each is polled when it has been
/// woken and not otherwise, and once more besides.
///
/// The extra poll is not padding. A `select!` polls every one of its branches
/// when any one of them wakes, so a future that is waiting registers its
/// waker again and again — and a permit that woke only the next waker in its
/// list could then spend its one wake-up on a task that was going to run
/// anyway, leaving the rest asleep for ever. The panic below is what that
/// looks like from outside.
fn drive_awake<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut waiting: Vec<_> = futures
        .into_iter()
        .map(|f| (Box::pin(f), Arc::new(Flag(AtomicBool::new(true)))))
        .collect();
    let mut done = Vec::new();
    while !waiting.is_empty() {
        let mut polled = false;
        let mut again = Vec::new();
        for (mut future, flag) in waiting {
            if !flag.0.swap(false, Ordering::SeqCst) {
                again.push((future, flag));
                continue;
            }
            polled = true;
            let waker = Waker::from(Arc::clone(&flag));
            let mut cx = Context::from_waker(&waker);
            let mut ready = future.as_mut().poll(&mut cx);
            if ready.is_pending() {
                ready = future.as_mut().poll(&mut cx);
            }
            match ready {
                Poll::Ready(value) => done.push(value),
                Poll::Pending => again.push((future, flag)),
            }
        }
        assert!(
            polled,
            "nothing was woken and nothing finished: {} left waiting",
            again.len()
        );
        waiting = again;
    }
    done
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

/// Writes `n` keys under `prefix`, and says what they were.
async fn write_keys<F: Fs>(db: &roble::Opened<F>, prefix: &str, n: usize) -> Model {
    let mut batch = Batch::new();
    let mut written = Model::new();
    for i in 0..n {
        let key = format!("{prefix}{i:03}").into_bytes();
        batch.put(&key, b"value").expect("put");
        written.insert(key, b"value".to_vec());
    }
    db.write(&mut batch, true).await.expect("write");
    written
}

/// Sets up a store with keys waiting in its memtable, pausing at `n`.
async fn ready_to_flush(env: &SimEnv, dir: &Path, n: u64) -> (roble::Opened<SimFs>, Model) {
    let db = roble::open_with(env, dir, Options::new().disable_automatic_compactions(true))
        .await
        .expect("open");
    let mut model = contents(&db).await;
    model.extend(write_keys(&db, "t", 40).await);
    env.sim().pause_at(n);
    (db, model)
}

#[compio::test]
async fn two_flushes_at_once_never_write_the_same_table() {
    // Both read `next_file_num` before either applies an edit, so without a
    // permit both write the same table file — the second over the first —
    // and the MANIFEST names a table whose bytes belong to the other. Both
    // also swap in a fresh memtable, so a write that landed between the two
    // swaps is in a memtable nothing holds and whose log the second edit
    // retires.
    //
    // Swept rather than aimed at one operation: where a flush suspends is
    // not this test's business, and a sweep goes on being a test of the
    // right thing when the flush's I/O changes.
    let total = {
        let (env, dir) = load("db-twin-a").await;
        let (db, _) = ready_to_flush(&env, &dir, 0).await;
        let before = env.sim().ops();
        db.flush().await.expect("flush").expect("a table");
        env.sim().ops() - before
    };
    assert!(
        total >= 4,
        "a flush of {total} operations is too thin to sweep"
    );

    for n in 1..=total {
        let (env, dir) = load("db-twin-a").await;
        let (db, model) = ready_to_flush(&env, &dir, n).await;

        let (first, second) = interleave(db.flush(), db.flush());
        let first = first.unwrap_or_else(|e| panic!("the first flush, paused at {n}: {e}"));
        let second = second.unwrap_or_else(|e| panic!("the second flush, paused at {n}: {e}"));

        // One of them wrote the memtable and the other found it empty.
        // Which is which is the permit's business, not this test's.
        let wrote: Vec<u64> = [first, second].into_iter().flatten().collect();
        assert_eq!(
            wrote.len(),
            1,
            "paused at {n} of {total}, {} flushes had something to write: {wrote:?}",
            wrote.len()
        );
        assert_eq!(
            db.versions().version.level(0).len(),
            2,
            "paused at {n}: the fixture's table and one new one"
        );
        assert_eq!(contents(&db).await, model, "paused at {n}");

        drop(db);
        let reopened = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("paused at {n} of {total}, the reopen: {e}"));
        assert_eq!(contents(&reopened).await, model, "paused at {n}, reopened");
    }
}

/// Opens `db-reads` with compaction left to the caller, writes keys, and says
/// which tables are in L0.
async fn ready_to_compact(
    env: &SimEnv,
    dir: &Path,
    prefix: &str,
) -> (roble::Opened<SimFs>, Model, Vec<u64>) {
    let db = roble::open_with(env, dir, Options::new().disable_automatic_compactions(true))
        .await
        .expect("open");
    let mut model = contents(&db).await;
    model.extend(write_keys(&db, prefix, 30).await);
    let in_l0 = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    (db, model, in_l0)
}

#[compio::test]
async fn a_flush_and_a_compaction_at_once_never_collide() {
    // A compaction takes the file numbers for its output when it starts and
    // writes them when it ends, so a flush that ran in between took the same
    // one — and the file that is left is whichever of the two wrote last.
    let total = {
        let (env, dir) = load("db-reads").await;
        let (db, _, in_l0) = ready_to_compact(&env, &dir, "u").await;
        let before = env.sim().ops();
        db.compact(0, &in_l0).await.expect("compact");
        env.sim().ops() - before
    };
    assert!(
        total >= 4,
        "a compaction of {total} operations is too thin to sweep"
    );

    for n in 1..=total {
        let (env, dir) = load("db-reads").await;
        let (db, model, in_l0) = ready_to_compact(&env, &dir, "u").await;
        env.sim().pause_at(n);

        let (compacted, flushed) = interleave(db.compact(0, &in_l0), db.flush());
        compacted.unwrap_or_else(|e| panic!("the compaction, paused at {n}: {e}"));
        flushed
            .unwrap_or_else(|e| panic!("the flush, paused at {n}: {e}"))
            .unwrap_or_else(|| panic!("paused at {n}, the flush wrote nothing"));
        assert_eq!(contents(&db).await, model, "paused at {n}");

        drop(db);
        let reopened = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("paused at {n} of {total}, the reopen: {e}"));
        assert_eq!(contents(&reopened).await, model, "paused at {n}, reopened");
    }
}

#[compio::test]
async fn a_sweep_never_collects_a_table_a_flush_just_added() {
    // A flush writes its table before the edit that claims it, so in between
    // the file is on disk and no version names it — which is precisely what
    // the sweep collects. A sweep that lists the directory in that window
    // finds the flush's table and takes it for an interrupted flush's
    // leftovers, and the edit that follows names a file that is gone.
    //
    // So the flush is what is paused here and the sweep is what runs in the
    // gap. The other way round is not interesting: the sweep lists the
    // directory first, so a table created after that list is not a name it
    // ever considers.
    let total = {
        let (env, dir) = load("db-twin-a").await;
        let (db, _) = ready_to_flush(&env, &dir, 0).await;
        let before = env.sim().ops();
        db.flush().await.expect("flush").expect("a table");
        env.sim().ops() - before
    };
    assert!(
        total >= 4,
        "a flush of {total} operations is too thin to sweep"
    );

    for n in 1..=total {
        let (env, dir) = load("db-twin-a").await;
        let (db, model) = ready_to_flush(&env, &dir, n).await;

        let (flushed, swept) = interleave(db.flush(), db.remove_obsolete_files());
        let swept = swept.unwrap_or_else(|e| panic!("the sweep, paused at {n}: {e}"));
        let table = flushed
            .unwrap_or_else(|e| panic!("the flush, paused at {n}: {e}"))
            .unwrap_or_else(|| panic!("paused at {n}, the flush wrote nothing"));

        assert!(
            !swept.contains(&roble_format::files::table_filename(table)),
            "paused at {n}, the sweep collected the table the flush had just added: {swept:?}"
        );
        assert_eq!(contents(&db).await, model, "paused at {n}");

        drop(db);
        let reopened = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("paused at {n} of {total}, the reopen: {e}"));
        assert_eq!(contents(&reopened).await, model, "paused at {n}, reopened");
    }
}

#[compio::test]
async fn a_flush_that_compacts_does_not_wait_on_itself() {
    // The permit is taken by `flush` and held across the compaction it
    // schedules, because the two are one change. A compaction that asked for
    // the permit again would wait for a permit its own caller is holding,
    // which is a deadlock and not a subtle one — this is here so that
    // splitting the two apart later cannot reintroduce it quietly.
    //
    // `db-reads` arrives wanting a compaction, so the first flush schedules
    // one and the second waits for both.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let mut model = contents(&db).await;
    model.extend(write_keys(&db, "w", 30).await);
    assert!(db.pick().is_some(), "the fixture should want a compaction");

    let (first, second) = interleave(db.flush(), db.flush());
    first.expect("the first flush");
    second.expect("the second flush");

    assert_eq!(
        db.versions().version.level(0).len(),
        0,
        "the compaction the first flush scheduled should have run"
    );
    assert_eq!(contents(&db).await, model);
}

#[compio::test]
async fn every_waiting_change_gets_its_turn() {
    // The permit wakes every waiter rather than the next one in its list,
    // and this is why. A task polled more often than it must — which is what
    // a `select!` does to every branch when any one of them wakes —
    // registers its waker more than once, so waking "the next waker" can
    // mean waking a task that was going to run anyway. Then nothing wakes
    // the others, and a permit that is never taken again is a store that has
    // stopped flushing.
    //
    // Three, because two cannot tell the difference: with one waiter left
    // there is nobody to leave asleep. And `db-reads` with a unit of one,
    // because the first flush has to *hold* the permit across many polls for
    // the other two to queue behind it at all — the compaction it schedules
    // yields after every entry it merges.
    let (env, dir) = load("db-reads").await;
    let db = roble::open_with(&env, &dir, Options::new().compaction_unit(1))
        .await
        .expect("open");
    let mut model = contents(&db).await;
    model.extend(write_keys(&db, "x", 40).await);
    assert!(db.pick().is_some(), "the fixture should want a compaction");

    let flushes = drive_awake(vec![db.flush(), db.flush(), db.flush()]);
    let wrote: Vec<u64> = flushes
        .into_iter()
        .filter_map(|r| r.expect("flush"))
        .collect();
    assert_eq!(
        wrote.len(),
        1,
        "one of three flushes had something to write: {wrote:?}"
    );
    assert_eq!(
        db.versions().version.level(0).len(),
        0,
        "and the compaction it scheduled ran"
    );
    assert_eq!(contents(&db).await, model);
}
