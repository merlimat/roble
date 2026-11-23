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

//! A write that arrives in the middle of a flush (§5).
//!
//! A flush is a long operation with awaits all through it, and nothing stops
//! a caller writing while one runs: a write does not take the permit that
//! serializes a store's own changes, and is left out of it on purpose —
//! taking it would make every write wait on whatever merge happened to be
//! going on (`crate::change`).
//!
//! So the memtable a flush writes out is frozen before the first await, and
//! the store carries on into a fresh one and, with a WAL, a fresh log. Three
//! things follow, one test each: the write is not lost, a read still finds
//! what is being written out, and a flush that failed hands its memtable to
//! the next one rather than freezing over it.
//!
//! Swept rather than aimed at one operation: where a flush suspends is not
//! these tests' business, and a sweep goes on testing the right thing when
//! the flush's I/O changes. And swept twice, with the log and without it,
//! because what the two configurations promise is not the same (§12).

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// Polls two futures turn and turn about until both are done.
///
/// An interleaving a caller with two tasks on one store will eventually
/// find, and enough for the read below, which has looked at the store by the
/// time it first gives the thread up. Not for a write, which gives it up
/// before it lands (`land_during`). The simulated filesystem never blocks,
/// so every `Pending` below comes from roble.
fn interleave<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    // The waker does nothing: this loop polls again whatever it is told, so
    // nothing here depends on wake-ups being delivered.
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

/// Polls `op` until it suspends, then runs `landing` to its end while `op`
/// stays suspended, then `op` to its end; and says whether `landing` ran
/// wholly inside `op`.
///
/// What a write in the middle of a flush needs, and `interleave` does not
/// give it. A write hands the thread over once while a flush or a merge is
/// under way (`share_the_thread`), and polled turn and turn about, the flush
/// takes that turn and runs to its end with the write not yet applied — so
/// a write driven that way lands after the flush wherever the flush was
/// paused. Here a yield of the landing's is passed by polling it again. Only
/// a landing still not done after a hundred polls is waiting on the
/// operation, and then the operation is polled once. The simulated
/// filesystem never blocks, so every `Pending` comes from roble.
fn land_during<A: Future, B: Future>(op: A, landing: B) -> (A::Output, B::Output, bool) {
    let mut cx = Context::from_waker(Waker::noop());
    let mut op = pin!(op);
    let mut landing = pin!(landing);
    let mut done = match op.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    };
    let mut polls = 0;
    let landed = loop {
        if let Poll::Ready(v) = landing.as_mut().poll(&mut cx) {
            break v;
        }
        polls += 1;
        assert!(polls < 100_000, "the landing is not making progress");
        if polls % 100 == 0
            && done.is_none()
            && let Poll::Ready(v) = op.as_mut().poll(&mut cx)
        {
            done = Some(v);
        }
    };
    let inside = done.is_none();
    let done = match done {
        Some(v) => v,
        None => loop {
            if let Poll::Ready(v) = op.as_mut().poll(&mut cx) {
                break v;
            }
            polls += 1;
            assert!(polls < 100_000, "the operation is not making progress");
        },
    };
    (done, landed, inside)
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

/// Compaction left to the caller, so that the version these tests read is the
/// one the flush left: a merge following it would move the tables about while
/// the assertions were asking about them. `tests/schedule.rs` is where the
/// scheduling is the subject.
fn options(wal: bool) -> Options<SimFs> {
    let o = Options::new().disable_automatic_compactions(true);
    if wal { o } else { o.wal(roble::Wal::Off) }
}

/// Writes `n` keys under `prefix`, and says what they were.
async fn write_keys<F: Fs>(db: &roble::Db<F>, prefix: &str, n: usize, wal: bool) -> Model {
    let mut batch = Batch::new();
    let mut written = Model::new();
    for i in 0..n {
        let key = format!("{prefix}{i:03}").into_bytes();
        batch.put(&key, b"value").expect("put");
        written.insert(key, b"value".to_vec());
    }
    // Asking for a sync with no log to sync is refused, and rightly: a store
    // should not promise a durability it has no way to keep.
    db.write_raw(&mut batch, wal).await.expect("write");
    written
}

/// Opens the fixture with keys waiting in its memtable, and says what the
/// database then holds.
async fn ready(env: &SimEnv, dir: &Path, wal: bool) -> (roble::Db<SimFs>, Model) {
    let db = roble::open_with(env, dir, options(wal))
        .await
        .expect("open");
    let mut model = contents(&db).await;
    model.extend(write_keys(&db, "t", 40, wal).await);
    (db, model)
}

/// How many operations a flush of that memtable takes, with nothing failing.
async fn ops_of_a_flush(wal: bool) -> u64 {
    let (env, dir) = load("db-twin-a").await;
    let (db, _) = ready(&env, &dir, wal).await;
    let before = env.sim().ops();
    db.flush().await.expect("flush").expect("a table");
    let total = env.sim().ops() - before;
    assert!(
        total >= 4,
        "wal={wal}: a flush of {total} operations is too thin to sweep"
    );
    total
}

/// What a read in the middle of a flush came back with.
struct Seen {
    /// Everything a scan found.
    scanned: Model,
    /// Whether a point lookup found one chosen key.
    found: bool,
    /// Whether the store says anything is still waiting to be flushed.
    waiting: bool,
    /// Whether the flush's table is in the version yet.
    recorded: bool,
}

/// Reads the store every way that has to cross the memtables.
///
/// Three ways, because they are three pieces of code: a scan merges the
/// memtables as sources, a `get` walks each of them with a cursor of its own,
/// and `unflushed_bytes` adds up their arenas.
async fn read_through<F: Fs>(db: &roble::Db<F>, tables_before: usize) -> Seen {
    let found = db
        .get(b"t007", Comparison::Equal)
        .await
        .expect("get")
        .is_some();
    // Together, because what one of them means depends on the other.
    let (waiting, recorded) = (
        db.unflushed_bytes() > 0,
        db.versions().version.table_count() > tables_before,
    );
    Seen {
        scanned: contents(db).await,
        found,
        waiting,
        recorded,
    }
}

#[compio::test]
async fn a_write_in_the_middle_of_a_flush_is_not_lost() {
    // Without the freeze the write lands in the memtable the flush is about
    // to drop, and in the log its edit is about to retire — so it is gone
    // from memory and gone from the next open, having been acknowledged.
    for wal in [true, false] {
        let total = ops_of_a_flush(wal).await;
        // The points at which the late write landed before the flush's edit.
        let mut landed = 0;

        for n in 1..=total {
            let (env, dir) = load("db-twin-a").await;
            let (db, mut model) = ready(&env, &dir, wal).await;
            let tables_before = db.versions().version.table_count();
            env.sim().pause_at(n);

            let late = async {
                let mut written = write_keys(&db, "late", 3, wal).await;

                // One of the keys the flush is writing out, written again
                // and read straight back. The two memtables now disagree
                // about it — the newer value in the one in front, the older
                // in the one behind — and a read that answered with
                // whichever it came to first would hand back the stale one.
                let mut batch = Batch::new();
                batch.put(b"t007", b"newer").expect("put");
                db.write_raw(&mut batch, wal).await.expect("write");
                written.insert(b"t007".to_vec(), b"newer".to_vec());
                let found = db
                    .get(b"t007", Comparison::Equal)
                    .await
                    .expect("get")
                    .expect("the key just written");
                assert_eq!(
                    found.value, b"newer",
                    "a read in the middle of a flush answered with the stale value"
                );

                // Whether all of that came before the flush's edit. Asked of
                // the version rather than of whether the flush had finished:
                // paused after its edit, in the sweep that follows it, a
                // flush has not finished and has no middle left for a write
                // to land in.
                let before_the_edit = db.versions().version.table_count() == tables_before;
                (written, before_the_edit)
            };
            let (flushed, (late, before_the_edit), _) = land_during(db.flush(), late);
            flushed
                .unwrap_or_else(|e| panic!("wal={wal}: the flush, paused at {n}: {e}"))
                .expect("a table");
            model.extend(late);
            if before_the_edit {
                landed += 1;
            }

            assert_eq!(
                contents(&db).await,
                model,
                "wal={wal}: paused at {n} of {total}"
            );
            if wal {
                // The edit retires every log below the one the store is
                // writing to, so those two had better be the same one: a
                // flush that left the minimum behind the current log would
                // replay records a table already holds, and one that left it
                // ahead would retire a log holding this write.
                assert_eq!(
                    Some(db.versions().min_unflushed_log_num),
                    db.current_log(),
                    "paused at {n} of {total}, the log the edit left unflushed"
                );
            }

            // With a log the late write was acknowledged as durable, so a
            // reopen must still have it. Without one, nothing but a flush
            // makes a write durable and a reopen is allowed to have lost it.
            if wal {
                drop(db);
                let reopened = roble::open_with(&env, &dir, options(wal))
                    .await
                    .unwrap_or_else(|e| panic!("paused at {n} of {total}, the reopen: {e}"));
                assert_eq!(
                    contents(&reopened).await,
                    model,
                    "paused at {n} of {total}, reopened"
                );
            }
        }

        // The guard that keeps the sweep from passing for the wrong reason:
        // a write that lands only once the flush has recorded its table
        // passes everything above without the freeze having anything to do
        // with it. Not every point can hold one. With the log on, a flush
        // holds writes off from the sync of the log it moves past to the
        // freeze, and a write that waited there lands after the flush; and
        // past the edit there is no middle left.
        assert!(
            landed > 2,
            "wal={wal}: the late write landed before the flush's edit at {landed} of \
             {total} points, so this sweep was testing a write after a flush"
        );
    }
}

#[compio::test]
async fn a_read_in_the_middle_of_a_flush_finds_what_is_being_written_out() {
    // Between the freeze and the edit, the keys a flush is writing out are in
    // neither the memtable a write goes to nor any table of the version. A
    // read that consulted only those two would answer that they are not
    // there, and they were acknowledged before the flush began.
    for wal in [true, false] {
        let total = ops_of_a_flush(wal).await;

        for n in 1..=total {
            let (env, dir) = load("db-twin-a").await;
            let (db, model) = ready(&env, &dir, wal).await;
            let tables_before = db.versions().version.table_count();
            env.sim().pause_at(n);

            let (flushed, seen) = interleave(db.flush(), read_through(&db, tables_before));
            flushed
                .unwrap_or_else(|e| panic!("wal={wal}: the flush, paused at {n}: {e}"))
                .expect("a table");

            assert_eq!(
                seen.scanned, model,
                "wal={wal}: paused at {n} of {total}, what a scan found"
            );
            assert!(
                seen.found,
                "wal={wal}: paused at {n} of {total}, a point lookup lost a key"
            );
            // The number a caller watches to decide when to flush has to
            // account for the memtable being written out, right up to the
            // edit that puts those bytes in a table — and for nothing once
            // it has. A flush that reported nothing waiting halfway through
            // would be telling a caller its memtable had emptied while the
            // bytes were still only in memory.
            assert_eq!(
                seen.waiting, !seen.recorded,
                "wal={wal}: paused at {n} of {total}, what the store says is waiting"
            );
        }
    }
}

#[compio::test]
async fn a_flush_that_failed_leaves_its_memtable_to_the_next_flush() {
    // A freeze is the one part of a flush that a failure does not undo: the
    // store is writing to the fresh memtable and the fresh log from then on,
    // whatever becomes of the rest. So the next flush writes out the one left
    // frozen as it stands — freezing over it would move the log past the
    // writes that arrived in between, and then retire it — and only then
    // freezes the one those writes went to, and writes that out too.
    for wal in [true, false] {
        let total = ops_of_a_flush(wal).await;
        let mut retried = 0;
        let mut froze = 0;

        for n in 1..=total {
            let (env, dir) = load("db-twin-a").await;
            let (db, mut model) = ready(&env, &dir, wal).await;
            let log_before = db.current_log();

            env.sim().fail_at(n);
            let first = db.flush().await;
            env.sim().stop_failing();
            if first.is_err() && db.current_log() != log_before {
                froze += 1;
            }

            // Whatever became of the flush, nothing acknowledged has moved:
            // the memtable it froze is still read from.
            assert_eq!(
                contents(&db).await,
                model,
                "wal={wal}: failing at {n} of {total}, after the flush"
            );

            // And a write after the failure is readable too, which is the
            // part the leftover path has to get right.
            model.extend(write_keys(&db, "after", 3, wal).await);

            let second = db.flush().await;
            if first.is_err() && second.as_ref().is_ok_and(|t| t.is_some()) {
                retried += 1;
            }
            assert_eq!(
                contents(&db).await,
                model,
                "wal={wal}: failing at {n} of {total}, after the retry"
            );
            // A number the store has handed out is never handed out again,
            // whatever the file was for. The freeze spends its log's number
            // there and then, because from that moment the store is writing
            // to that log whatever becomes of the rest of the flush — so a
            // retry that read the counter afresh must not land on it.
            if let Some(log) = db.current_log() {
                let versions = db.versions();
                let clash = versions
                    .version
                    .levels()
                    .any(|(_, tables)| tables.iter().any(|t| t.file_num == log));
                drop(versions);
                assert!(
                    !clash,
                    "wal={wal}: failing at {n} of {total}, a table took the live log's number"
                );
            }

            if second.is_err() {
                // The injection landed on the MANIFEST append, which ends
                // this store's recording life (`crate::flush`): the retry
                // was refused rather than run, so there is no leftover path
                // for the reopen below to be about. That a store in that
                // state still leaves a database which opens is
                // `tests/failure.rs`'s sweep, and not this one's.
                continue;
            }

            // The retry wrote out the writes that arrived after the failure as
            // well as the memtable left frozen. A flush that returned with
            // them still waiting would have returned without making them
            // durable, and with the log off nothing else does.
            assert_eq!(
                db.unflushed_bytes(),
                0,
                "wal={wal}: failing at {n} of {total}, the retry left writes waiting"
            );

            drop(db);
            env.sim().crash();
            let reopened = roble::open_with(&env, &dir, options(wal))
                .await
                .unwrap_or_else(|e| {
                    panic!("wal={wal}: failing at {n} of {total}, the reopen: {e}")
                });
            // With the log off only a flush makes a write durable, and the
            // retry covered everything, so both configurations expect the
            // whole model back after a crash.
            assert_eq!(
                contents(&reopened).await,
                model,
                "wal={wal}: failing at {n} of {total}, reopened"
            );
        }

        // The guards that keep the sweep from passing for the wrong reason.
        assert!(
            retried > 0,
            "wal={wal}: no failure was followed by a flush that wrote a table, so nothing \
             above exercised the leftover path"
        );
        if wal {
            assert!(
                froze > 0,
                "no failure landed after the freeze, so nothing above left a memtable \
                 frozen for the next flush to find"
            );
        }
    }
}
