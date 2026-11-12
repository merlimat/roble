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

use std::cell::RefCell;
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
    // `wake_by_ref` is the trait's own default, which clones and calls this.
    fn wake(self: Arc<Self>) {
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
    // A compaction reads the version to decide what to merge and reads it
    // again to number what it wrote, and a flush changes the version in
    // between. Both of a compaction's reads are under the permit (§9), so
    // what this sweeps for is a flush landing in the gap between them.
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
    // Three flushes at once, with a merge in the middle of it: one of them
    // writes the table, the other two find an empty memtable, and the
    // compaction the first scheduled runs to the end.
    //
    // This used to be where the permit's wake-up was pinned, and it cannot
    // be any more: the first flush held the permit across the many polls of
    // the merge its compaction ran, and a merge holds a claim and not the
    // permit now (§9). With nothing holding the permit across a poll, there
    // is no waiter to leave asleep. That claim moved to `crate::change`'s own
    // unit tests, where the permit can be held by hand.
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

/// Writes one key, and says what it wrote.
async fn put<F: Fs>(db: &roble::Opened<F>, key: &[u8], value: &[u8]) -> Model {
    let mut batch = Batch::new();
    batch.put(key, value).expect("put");
    db.write(&mut batch, true).await.expect("write");
    Model::from([(key.to_vec(), value.to_vec())])
}

/// How many operations one synced write takes, with nothing failing.
async fn ops_of_a_write() -> u64 {
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let before = env.sim().ops();
    put(&db, b"count", b"v").await;
    let total = env.sim().ops() - before;
    assert!(
        total >= 2,
        "a write of {total} operations is too thin to sweep"
    );
    total
}

#[compio::test]
async fn two_writes_at_once_both_reach_the_log() {
    // Each reads the offset it will append its record at, waits for the
    // disk, and only then applies to the memtable. Without the permit both
    // read the same offset and the second's record lands on top of the
    // first's — and nothing looks wrong until the store is reopened and the
    // first write is simply not there, having been acknowledged with a sync.
    //
    // Swept rather than aimed at one operation: where the first write
    // suspends is not this test's business, and a sweep goes on testing the
    // right thing when the write's I/O changes.
    let total = ops_of_a_write().await;

    for n in 1..=total {
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open(&env, &dir).await.expect("open");
        let mut model = contents(&db).await;
        env.sim().pause_at(n);

        let (first, second) = interleave(put(&db, b"one", b"1"), put(&db, b"two", b"2"));
        model.extend(first);
        model.extend(second);

        assert_eq!(contents(&db).await, model, "paused at {n} of {total}");

        drop(db);
        let reopened = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("paused at {n} of {total}, the reopen: {e}"));
        assert_eq!(
            contents(&reopened).await,
            model,
            "paused at {n} of {total}: both writes were acknowledged with a sync, so both \
             have to be there"
        );
    }
}

#[compio::test]
async fn two_writes_at_once_do_not_share_a_sequence_number() {
    // The other half of the same overlap, and the half a reopen cannot show.
    // A write reads the sequence number it will stamp before waiting for the
    // disk, so two of them read the same one. Then two entries for one key
    // are one entry, a snapshot cannot tell the two writes apart, and the
    // MANIFEST's last sequence number is behind what the memtable holds —
    // which is how a write replayed later loses to a tombstone that was
    // written before it.
    let total = ops_of_a_write().await;

    for n in 1..=total {
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(
            &env,
            &dir,
            Options::new().disable_automatic_compactions(true),
        )
        .await
        .expect("open");
        // A flush first, so the baseline is the store's own count and not
        // whatever the fixture's logs left for the replay to find.
        db.flush().await.expect("flush");
        let before = db.versions().last_seq_num;
        env.sim().pause_at(n);

        interleave(put(&db, b"one", b"1"), put(&db, b"two", b"2"));
        db.flush().await.expect("flush").expect("a table");

        assert_eq!(
            db.versions().last_seq_num,
            before + 2,
            "paused at {n} of {total}: two writes of one entry each are two sequence numbers"
        );
    }
}

/// Opens `db-reads` with a merge that hands the thread back every entry, so
/// that a compaction of it suspends often enough to interleave with.
async fn ready_to_merge(env: &SimEnv, dir: &Path, prefix: &str) -> (roble::Opened<SimFs>, Model) {
    let db = roble::open_with(
        env,
        dir,
        Options::new()
            .disable_automatic_compactions(true)
            .compaction_unit(1),
    )
    .await
    .expect("open");
    let mut model = contents(&db).await;
    model.extend(write_keys(&db, prefix, 30).await);
    (db, model)
}

#[compio::test]
async fn a_flush_does_not_wait_for_a_compactions_merge() {
    // What the narrowing is for (§9). A compaction holds the permit to decide
    // what it will read and to record what it wrote, and not for the merge in
    // between — so a flush that starts while one is merging finishes first,
    // rather than waiting out work that is none of its business.
    let (env, dir) = load("db-reads").await;
    let (db, mut model) = ready_to_merge(&env, &dir, "u").await;
    let in_l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    // Something for the flush to write, after the compaction's inputs were
    // named: the flush must not be the one that produced them.
    model.extend(write_keys(&db, "v", 5).await);

    let done: RefCell<Vec<&'static str>> = RefCell::new(Vec::new());
    let (compacted, flushed) = interleave(
        async {
            let result = db.compact(0, &in_l0).await;
            done.borrow_mut().push("the compaction");
            result
        },
        async {
            let result = db.flush().await;
            done.borrow_mut().push("the flush");
            result
        },
    );
    compacted.expect("the compaction");
    flushed.expect("the flush").expect("a table");

    assert_eq!(
        done.borrow().first().copied(),
        Some("the flush"),
        "the flush waited for the merge to finish: {:?}",
        done.borrow()
    );
    assert_eq!(
        contents(&db).await,
        model,
        "and one of them changed an answer"
    );
}

#[compio::test]
async fn two_compactions_at_once_never_choose_the_same_tables() {
    // The permit used to make this impossible by being held across the merge.
    // The claim is what makes it impossible now that it is not: without it
    // the second's edit deletes tables the first already deleted, which is
    // the refusal that ends a store's writing life (§9).
    let total = {
        let (env, dir) = load("db-reads").await;
        let (db, _) = ready_to_merge(&env, &dir, "u").await;
        let before = env.sim().ops();
        db.compact_once().await.expect("compact");
        env.sim().ops() - before
    };
    assert!(
        total >= 4,
        "a compaction of {total} operations is too thin to sweep"
    );

    for n in 1..=total {
        let (env, dir) = load("db-reads").await;
        let (db, model) = ready_to_merge(&env, &dir, "u").await;
        env.sim().pause_at(n);

        let (first, second) = interleave(db.compact_once(), db.compact_once());
        let first = first.unwrap_or_else(|e| panic!("the first, paused at {n}: {e}"));
        let second = second.unwrap_or_else(|e| panic!("the second, paused at {n}: {e}"));

        // One of them merged and the other stood down. Which is which is the
        // claim's business, not this test's.
        let merged = [&first, &second].iter().filter(|o| !o.is_empty()).count();
        assert_eq!(
            merged, 1,
            "paused at {n} of {total}, {merged} compactions wrote tables: \
             {first:?} and {second:?}"
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
async fn a_compaction_that_failed_leaves_its_tables_free() {
    // The claim is a guard and not a pair of calls. A merge that failed half
    // way through, or whose future was thrown away, would otherwise leave its
    // inputs claimed for ever — and then the picker would go on choosing them
    // and every compaction after it would stand down.
    let total = {
        let (env, dir) = load("db-reads").await;
        let (db, _) = ready_to_merge(&env, &dir, "u").await;
        let before = env.sim().ops();
        db.compact_once().await.expect("compact");
        env.sim().ops() - before
    };
    let mut retried = 0;

    for n in 1..=total {
        let (env, dir) = load("db-reads").await;
        let (db, model) = ready_to_merge(&env, &dir, "u").await;

        env.sim().fail_at(n);
        let first = db.compact_once().await;
        env.sim().stop_failing();
        if first.is_ok() {
            // The injection landed somewhere this compaction did not mind.
            continue;
        }

        // Whatever it left behind, the tables it had are the picker's again.
        if self_picks(&db) {
            let second = db.compact_once().await;
            if let Ok(outputs) = &second {
                assert!(
                    !outputs.is_empty(),
                    "failing at {n} of {total}: the picker still wants a compaction and \
                     the next one stood down, so the tables are claimed by one that is \
                     not happening"
                );
                retried += 1;
            }
        }
        assert_eq!(contents(&db).await, model, "failing at {n} of {total}");
    }

    assert!(
        retried > 0,
        "no failed compaction was followed by one that ran, so nothing here exercised \
         the claim being given back"
    );
}

/// Whether the picker still wants something compacted.
fn self_picks<F: Fs>(db: &roble::Opened<F>) -> bool {
    db.pick().is_some()
}

#[compio::test]
async fn compacting_tables_a_compaction_already_has_is_refused() {
    // The explicit call says so rather than standing down. A caller that
    // named file numbers asked for something definite and has nothing to
    // fall back on; `compact_once` has a picker to ask again, which is why
    // that one stands down instead (§9).
    let (env, dir) = load("db-reads").await;
    let (db, model) = ready_to_merge(&env, &dir, "u").await;
    let in_l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();

    let (merged, refused) = interleave(db.compact_once(), db.compact(0, &in_l0));
    let merged = merged.expect("the compaction");
    assert!(
        !merged.is_empty(),
        "the first should have written something"
    );
    let err = refused.expect_err("the second asked for tables the first had");
    assert!(
        format!("{err}").contains("already being compacted"),
        "and should say why: {err}"
    );
    assert_eq!(contents(&db).await, model, "and nothing changed an answer");
}
