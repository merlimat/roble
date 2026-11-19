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

//! What a store does when the disk does not cooperate (DESIGN.md §12).
//!
//! §12 asks for each scenario to be run once to count its operations, then
//! once per operation with an error injected there, and once more with a
//! crash. `tests/env.rs` does that for the simulated filesystem itself, which
//! proves the injector works; this does it to the engine, which is the claim
//! §12 is making. And §12 asks for it twice — "`disable_wal`, which is Oxia's
//! configuration, gets its own pass of every scenario" — because the
//! guarantee is a different one without a log.
//!
//! Three invariants, none of which is about the failing call's return value.
//! A failed write is allowed to fail:
//!
//! 1. **It still opens.** "A half-written table, a torn log record or a
//!    half-rotated marker must leave the database openable." A store that
//!    cannot be reopened after a failed write has turned a recoverable error
//!    into a lost database.
//! 2. **What survives is a prefix.** Durability is ordered: a store may lose
//!    the tail of what it was told and may not lose the middle. "The second
//!    key is there and the first is not" is the shape that says a crash was
//!    handled wrongly, whatever the count.
//! 3. **With the log off, the tail does not survive.** §4's guarantee becomes
//!    the last `flush()`, so a write that no flush covered must be gone — and
//!    a store that kept it would be passing by luck, having promised nothing.
//!
//! `tests/no_wal.rs` already pins the third claim at one chosen moment. What
//! a sweep adds is the same claim at every operation, which is where a
//! durability bug actually lives: not in the ordinary path, but at the one
//! point between a write and the sync that makes it real.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// Copies a committed fixture into a simulated filesystem.
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

/// The keys the scenario adds, in the order it makes them durable.
const WRITTEN: &[&[u8]] = &[b"injected-1", b"injected-2", b"injected-tail"];

/// A store that compacts as soon as it can, with or without a log.
///
/// `l0_compaction_threshold(1)` is what makes the compaction in the scenario
/// do anything: the default of four leaves the picker with nothing to pick
/// over a fixture this small, and an operation that does nothing injects
/// nothing.
fn options(wal: bool) -> Options<SimFs> {
    let o = Options::new().l0_compaction_threshold(1);
    if wal { o } else { o.wal(roble::Wal::Off) }
}

/// Open; twice over, write a batch, flush it and compact; then one last write
/// that no flush covers.
///
/// Every step touches the disk a different way — the open reads markers, a
/// MANIFEST and the tables it names; a write appends to a log and syncs it; a
/// flush writes a table, syncs it, appends a version edit and syncs the
/// directory; a compaction reads tables, writes one and retires the rest.
///
/// The last write is what separates the two configurations. With a log it is
/// durable once synced; without one it lives only in the memtable.
async fn scenario(env: &SimEnv, dir: &Path, wal: bool) -> roble::Result<()> {
    let db = roble::open_with(env, dir, options(wal)).await?;
    for key in &WRITTEN[..2] {
        let mut batch = Batch::new();
        batch.put(key, b"written")?;
        // Asking for a sync with no log to sync is refused, and rightly: a
        // store should not promise a durability it has no way to keep.
        db.write_raw(&mut batch, wal).await?;
        db.flush().await?;
        db.compact_once().await?;
    }
    let mut tail = Batch::new();
    tail.put(WRITTEN[2], b"written")?;
    db.write_raw(&mut tail, wal).await?;
    Ok(())
}

/// How many of the scenario's keys survived, or `None` if what survived is
/// not a prefix of the order they were written in.
fn durable_prefix(found: &Model) -> Option<usize> {
    let present: Vec<bool> = WRITTEN
        .iter()
        .map(|k| found.contains_key(&k.to_vec()))
        .collect();
    let n = present.iter().take_while(|p| **p).count();
    present[n..].iter().all(|p| !*p).then_some(n)
}

/// Runs the scenario once with nothing failing, and returns its operation
/// count.
async fn count_operations(wal: bool) -> u64 {
    let (env, dir) = load("db-simple").await;
    let before = env.sim().ops();
    scenario(&env, &dir, wal)
        .await
        .expect("the scenario must pass when nothing fails");
    env.sim().ops() - before
}

#[compio::test]
async fn a_failure_at_any_single_operation_leaves_a_database_that_opens() {
    for wal in [true, false] {
        let total = count_operations(wal).await;
        assert!(
            total >= 20,
            "a scenario of {total} operations is too thin to walk the I/O paths"
        );

        let mut refused = 0u64;
        for n in 1..=total {
            let (env, dir) = load("db-simple").await;
            env.sim().fail_at(n);
            // The scenario is allowed to fail. What is not allowed is a
            // panic, or a database that cannot be opened afterwards.
            if scenario(&env, &dir, wal).await.is_err() {
                refused += 1;
            }
            env.sim().stop_failing();

            let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
                panic!("wal={wal}: failing operation {n} of {total} left a database that will not open: {e}")
            });
            // Opened and read, not merely opened: a version naming a table
            // that is not there opens and fails on the first scan.
            let found = contents(&db).await;
            assert!(
                durable_prefix(&found).is_some(),
                "wal={wal}: failing operation {n} of {total} left a gap in what survived"
            );
        }

        // The guard that keeps this from passing for the wrong reason. If the
        // injector stopped reaching the engine — a path that no longer goes
        // through `Fs`, a scenario that stopped touching the disk — every run
        // would succeed and every reopen would trivially work, and the loop
        // would still be green.
        //
        // A third rather than a half, because a flush runs the compaction the
        // picker chose and swallows its failure on purpose: the flush is
        // durable, and reporting it as failed because the maintenance behind
        // it did not run would be the larger lie (§9). So a good share of
        // what this sweeps now happens inside a call meant to absorb it, and
        // an injection landing there is not a scenario that failed. It is
        // still a scenario that was *swept* — the reopen below is what this
        // test is for, and a compaction that failed quietly is held to it
        // like every other operation.
        assert!(
            refused * 3 >= total,
            "wal={wal}: only {refused} of {total} injections made the scenario fail; \
             the engine is barely meeting the failing disk"
        );
    }
}

#[compio::test]
async fn a_crash_at_any_single_operation_keeps_a_prefix_of_what_was_written() {
    for wal in [true, false] {
        let total = count_operations(wal).await;
        let mut seen = std::collections::BTreeSet::new();

        for n in 1..=total {
            let (env, dir) = load("db-simple").await;
            env.sim().fail_at(n);
            let _ = scenario(&env, &dir, wal).await;
            env.sim().stop_failing();

            // The machine dies where the operation did: everything unsynced
            // is gone, and so is every name no directory sync made durable.
            env.sim().crash();

            let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
                panic!("wal={wal}: a crash at operation {n} of {total} left a database that will not open: {e}")
            });
            let found = contents(&db).await;
            let kept = durable_prefix(&found).unwrap_or_else(|| {
                panic!(
                    "wal={wal}: a crash at operation {n} of {total} kept a later write and \
                     lost an earlier one"
                )
            });
            seen.insert(kept);

            if !wal {
                // §4: with no log, a crash takes the store back to its last
                // flush, and the tail write had none. Keeping it would be
                // passing by luck — the store promised nothing about it.
                assert!(
                    kept < WRITTEN.len(),
                    "wal=false: a crash at operation {n} of {total} kept a write that \
                     no flush covered"
                );
            }
        }

        // More than one outcome has to turn up, or the prefix check is
        // satisfied by a single answer and proves only that the others never
        // happen. Early operations crash before anything is durable and late
        // ones after, so a sweep that saw one length would mean the scenario
        // stopped spanning the syncs that divide them.
        assert!(
            seen.len() > 1,
            "wal={wal}: the crash sweep only ever saw {seen:?} keys survive; \
             it has to see more than one to be comparing anything"
        );
    }
}

/// Replays the scenario up to a failure at `n` and then crashes, leaving the
/// store exactly as a machine that died there would.
///
/// The simulated filesystem is deterministic, so this is reproducible: the
/// same `n` gives the same bytes on disk every time, which is what lets the
/// test below compare two recoveries of the same crash.
async fn crashed_at(n: u64, wal: bool) -> (SimEnv, PathBuf) {
    let (env, dir) = load("db-simple").await;
    env.sim().fail_at(n);
    let _ = scenario(&env, &dir, wal).await;
    env.sim().stop_failing();
    env.sim().crash();
    (env, dir)
}

/// A recovery that fails partway must leave the store where it found it.
///
/// This is the case a crash loop makes: a machine dies, comes back, fails
/// while recovering, and dies again. Opening is not a read-only act — it
/// replays a log, it can rotate a marker, it deletes files the version no
/// longer names — so a recovery that gets halfway and stops has done some of
/// that and not the rest.
///
/// What must hold is that it has not *destroyed* anything: the next open
/// finds exactly what a clean recovery would have found. A recovery that
/// truncated a log before reading it, or retired a table before the version
/// edit naming its replacement was durable, would pass every test in this
/// file except this one — it only shows up when the recovery itself is
/// interrupted.
#[compio::test]
async fn a_recovery_that_fails_partway_loses_nothing_that_was_durable() {
    for wal in [true, false] {
        let total = count_operations(wal).await;
        let mut interrupted = 0u64;

        for n in 1..=total {
            // What an uninterrupted recovery of this crash finds.
            let (env, dir) = crashed_at(n, wal).await;
            let expected = {
                let db = roble::open(&env, &dir).await.expect("a clean recovery");
                durable_prefix(&contents(&db).await).expect("a prefix")
            };

            // And how many operations that recovery takes, so that each one
            // can be failed in turn.
            let (counting, counting_dir) = crashed_at(n, wal).await;
            let before = counting.sim().ops();
            {
                let _db = roble::open(&counting, &counting_dir)
                    .await
                    .expect("a clean recovery");
            }
            let recovery_ops = counting.sim().ops() - before;

            for m in 1..=recovery_ops {
                let (env, dir) = crashed_at(n, wal).await;
                env.sim().fail_at(m);
                if roble::open(&env, &dir).await.is_err() {
                    interrupted += 1;
                }
                env.sim().stop_failing();

                let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
                    panic!(
                        "wal={wal}: a recovery of the crash at {n} that failed at its own \
                         operation {m} left a database that will not open: {e}"
                    )
                });
                let found = durable_prefix(&contents(&db).await).unwrap_or_else(|| {
                    panic!(
                        "wal={wal}: a failed recovery of the crash at {n} left a gap in \
                         what survived"
                    )
                });
                assert_eq!(
                    found, expected,
                    "wal={wal}: a recovery of the crash at {n} that failed at its own \
                     operation {m} changed what survived"
                );
            }
        }

        // The same guard as the sweeps above: if failing an operation of the
        // recovery stopped stopping it, every inner run would be an ordinary
        // open and the comparison would be against itself.
        assert!(
            interrupted > 0,
            "wal={wal}: no injected failure ever interrupted a recovery, so this \
             compared a clean open against a clean open"
        );
    }
}

/// A disk that keeps the buffer it was handed, at any one operation.
///
/// This is §12's third pass, and the one the simulator could not do until it
/// grew a seam: "once more with the operation returning other buffers than it
/// was given, which the disk keeps until the run ends."
///
/// A disk is allowed to do this. `Disk::read` and `Disk::write` take a buffer
/// by value and return one, and nothing in the trait says it is the same
/// buffer — a kernel that still owns the memory returns another. What the
/// engine must not do is read the buffer it got back as though it were the
/// one it sent, which is why the one it gets back is poisoned rather than
/// zeroed: `0xa5` through a key or a block header is visibly wrong, where
/// zeroes read as a plausible empty.
///
/// **What this does not yet catch.** §8's size-classed frames do not exist —
/// they arrive with direct I/O — so a withheld buffer is a `Vec` whose memory
/// is merely kept, not a frame held out of a pool. The accounting half of
/// this pass therefore has nothing to account for yet. What it does catch
/// today is an engine that uses a buffer after the call that borrowed it
/// failed, and that is worth having before the frames arrive rather than
/// after.
#[compio::test]
async fn a_disk_that_keeps_its_buffer_is_survived() {
    for wal in [true, false] {
        let total = count_operations(wal).await;
        let mut kept_any = 0u64;

        for n in 1..=total {
            let (env, dir) = load("db-simple").await;
            env.sim().withhold_at(n);
            // Allowed to fail. Not allowed to read the buffer it did not get
            // back, or to leave a database that will not open.
            let _ = scenario(&env, &dir, wal).await;
            kept_any += env.sim().withheld() as u64;
            env.sim().stop_withholding();

            let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
                panic!(
                    "wal={wal}: withholding the buffer of operation {n} of {total} left a \
                     database that will not open: {e}"
                )
            });
            let found = contents(&db).await;
            assert!(
                durable_prefix(&found).is_some(),
                "wal={wal}: withholding the buffer of operation {n} of {total} left a gap \
                 in what survived"
            );
            // The poison must not have reached the store. A key or a value
            // made of it would mean the engine read a buffer it had handed
            // away, and the scan above would have carried it here.
            for (key, value) in &found {
                assert!(
                    !key.iter().all(|b| *b == 0xa5) && !value.iter().all(|b| *b == 0xa5),
                    "wal={wal}: withholding the buffer of operation {n} of {total} let a \
                     withheld buffer reach the store"
                );
            }
        }

        // Most operations are not reads or writes — an open, a sync, a
        // listing — so this does not expect every one to withhold. It does
        // expect the seam to have fired, or the sweep is an ordinary run.
        assert!(
            kept_any > 0,
            "wal={wal}: no operation ever withheld a buffer, so this swept nothing"
        );
    }
}

/// Whether an error is this store refusing to record anything more.
fn refuses_to_record(e: &roble::Error) -> bool {
    matches!(e, roble::Error::Corrupt(m) if m.contains("records no more"))
}

/// The scenario again, carried on through its failures rather than stopped at
/// the first, saying how many calls were refused outright.
async fn careless(env: &SimEnv, dir: &Path, wal: bool) -> roble::Result<usize> {
    let db = roble::open_with(env, dir, options(wal)).await?;
    let mut refused = 0;
    for key in &WRITTEN[..2] {
        let mut batch = Batch::new();
        batch.put(key, b"written")?;
        let _ = db.write_raw(&mut batch, wal).await;
        // Both called, and neither allowed to end the run.
        let failed = [db.flush().await.err(), db.compact_once().await.err()];
        refused += failed
            .iter()
            .flatten()
            .filter(|e| refuses_to_record(e))
            .count();
    }
    let mut tail = Batch::new();
    tail.put(WRITTEN[2], b"written")?;
    let _ = db.write_raw(&mut tail, wal).await;
    Ok(refused)
}

/// A store told to carry on after a failure still leaves a database that
/// opens.
///
/// The three sweeps above stop at the first error, which is what a careful
/// caller does, and they are the stronger test of everything that happens
/// *before* a version edit is written. This one is the careless caller — the
/// one that logs the error and asks again — and it is the only one that
/// reaches what happens after. A failed append leaves its record in the
/// MANIFEST, because nothing truncates one and no crash would undo it, and
/// the store carries on from a version that record changed and it did not.
/// What the next edit then says contradicts it. This is the sweep with the
/// log on, failing the sync of a flush's edit:
///
/// ```text
/// edit 6: new=[(0, 8)]                   the flush's edit; its sync failed
/// edit 7: deleted=[(0, 7)] new=[(1, 8)]  the compaction, reusing number 8
/// ```
///
/// A file number handed out twice, naming a table at two levels, and the
/// compaction's output written over the flush's table. Three edits later the
/// sweep collects the file and the database stops opening: "L0 table 8 is in
/// the MANIFEST but not in /db". Every step of the way the store had been
/// handed an error it could have stopped at, so the store has to stop
/// itself.
///
/// What this does **not** check is §12's prefix. A caller that ignores a
/// failed write and goes on to the next key has asked for the gap it gets;
/// the invariant it has not waived is that the database still opens.
#[compio::test]
async fn a_store_that_is_told_to_carry_on_still_leaves_a_database_that_opens() {
    for wal in [true, false] {
        let total = count_operations(wal).await;
        let mut refusals = 0;

        for n in 1..=total {
            let (env, dir) = load("db-simple").await;
            env.sim().fail_at(n);
            refusals += careless(&env, &dir, wal).await.unwrap_or(0);
            env.sim().stop_failing();

            let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
                panic!(
                    "wal={wal}: carrying on past a failure at operation {n} of {total} \
                     left a database that will not open: {e}"
                )
            });
            // Read, not merely opened: a version naming a table that is not
            // there opens and fails on the first scan.
            let _ = contents(&db).await;
        }

        // The guard that keeps this from passing for the wrong reason. If no
        // injection ever landed on a MANIFEST append, nothing above reached
        // the door this test is about and every reopen succeeded trivially.
        assert!(
            refusals > 0,
            "wal={wal}: no injection made a version edit fail to append, so nothing \
             here exercised the refusal that keeps the MANIFEST honest"
        );
    }
}

/// Two flushes in a row that each fail before their edit lands, with a
/// crash after each, and a synced write between them.
///
/// The first failure is what sets the trap. A flush creates the log the
/// store moves to before it writes the edit that retires the old one, so a
/// crash in between leaves that log in the directory and its number in no
/// MANIFEST. The reopen replays it and carries on writing to it; the number
/// is still the MANIFEST's next one, unless the open ratchets past the files
/// it lists as Pebble does (`TestOpenRatchetsNextFileNum`).
///
/// The second flush is where the trap springs. It takes that same number for
/// its new log and creates it — over the log holding the write it is meant
/// to be flushing. A crash before its edit lands leaves the MANIFEST
/// pointing at the older log, and the replay finds the write gone though it
/// was acknowledged and synced.
///
/// Says whether the first reopen was writing to a log its MANIFEST has not
/// heard of, which is the state the trap needs.
async fn flush_fails_twice(env: &SimEnv, dir: &Path, first: u64, second: u64) -> bool {
    let db = roble::open_with(env, dir, flush_options())
        .await
        .expect("create");
    db.put(b"a", b"written").await.expect("put a");
    db.flush().await.expect("flush a");
    db.put(b"b", b"written").await.expect("put b");

    env.sim().fail_at(first);
    let _ = db.flush().await;
    env.sim().stop_failing();
    drop(db);
    env.sim().crash();

    let db = roble::open_with(env, dir, flush_options())
        .await
        .expect("the first reopen");
    let current = db
        .current_log()
        .expect("a store with a WAL writes to a log");
    let orphan = current > db.versions().min_unflushed_log_num;
    db.put(b"c", b"written").await.expect("put c");
    env.sim().fail_at(second);
    let _ = db.flush().await;
    env.sim().stop_failing();
    drop(db);
    env.sim().crash();
    orphan
}

/// A store whose flushes run only when [`flush_fails_twice`] asks for one.
fn flush_options() -> Options<SimFs> {
    Options::new()
        .create_if_missing(true)
        .background_tasks(false)
}

/// How many operations the flush in [`flush_fails_twice`] makes when nothing
/// fails.
async fn flush_operations() -> u64 {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, flush_options())
        .await
        .expect("create");
    db.put(b"a", b"written").await.expect("put a");
    db.flush().await.expect("flush a");
    db.put(b"b", b"written").await.expect("put b");
    let before = env.sim().ops();
    db.flush().await.expect("flush b");
    env.sim().ops() - before
}

#[compio::test]
async fn a_reopen_after_an_interrupted_flush_never_writes_over_a_log_it_replayed() {
    let total = flush_operations().await;
    assert!(
        total >= 8,
        "a flush of {total} operations is too thin to sweep"
    );

    let mut orphaned = 0u64;
    for first in 1..=total {
        for second in 1..=total {
            let env = SimEnv::new();
            let dir = PathBuf::from("/db");
            if flush_fails_twice(&env, &dir, first, second).await {
                orphaned += 1;
            }

            let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
                panic!("flushes failed at {first} and {second}: the reopen: {e}")
            });
            let found = contents(&db).await;
            let keys: Vec<&[u8]> = found.keys().map(Vec::as_slice).collect();
            // Every one of them was synced before it was acknowledged, so a
            // crash may take none of them.
            assert_eq!(
                keys,
                [b"a".as_slice(), b"b", b"c"],
                "flushes failed at operations {first} and {second} of {total}"
            );
        }
    }

    // The guard that keeps this from passing for the wrong reason: if no
    // first failure ever left the reopen writing to a log the MANIFEST does
    // not name, the second flush was never near the number it could reuse.
    assert!(
        orphaned > 0,
        "no failure of the first flush left its new log behind, so nothing here \
         tested the reopen that writes to it"
    );
}

/// A store that wrote `a` and closed, with bytes of an earlier life left past
/// the log's last record — what a log Pebble recycled holds after its
/// records, or a record torn by a kill. Returns the log's number.
async fn with_a_stale_tail(env: &SimEnv, dir: &Path) -> u64 {
    let db = roble::open_with(env, dir, flush_options())
        .await
        .expect("create");
    db.put(b"a", b"written").await.expect("put a");
    let log = db
        .current_log()
        .expect("a store with a WAL writes to a log");
    db.close().await.expect("close");

    let path = dir.join(roble_format::files::log_filename(log));
    let disk = env
        .sim()
        .open(&path, roble::Access::ReadWrite)
        .await
        .expect("open the log");
    let end = disk.len().await.expect("len");
    // Letters, as the middle of an old value is: no header a reader could
    // mistake for a record, only bytes a reader has to stop at.
    let stale: Vec<u8> = (0..300u32).map(|i| b'a' + (i * 7 % 26) as u8).collect();
    let (result, _) = disk.write(WriteBuf::new(stale), end, ()).await;
    result.expect("write the stale tail");
    disk.sync(()).await.expect("sync");
    disk.close().await.expect("close");
    log
}

/// A log the store carries on writing to after a reopen ends where its
/// records do, so that a flush interrupted at any operation leaves a
/// directory that opens.
///
/// The reopen resumes the log where the replay stopped, before the stale
/// tail. A flush then creates a newer log before its edit retires this one,
/// and a crash in between leaves this one behind a newer log, where Pebble
/// and roble both read it strictly: the stale bytes past `b` were taken for
/// a record that went wrong, and neither would open the directory. Found by
/// compat/differential killing roble in a flush after it resumed a log
/// Pebble had recycled.
#[compio::test]
async fn a_flush_interrupted_after_a_reopen_over_a_stale_log_tail_leaves_a_database_that_opens() {
    let total = {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        with_a_stale_tail(&env, &dir).await;
        let db = roble::open_with(&env, &dir, flush_options())
            .await
            .expect("reopen");
        db.put(b"b", b"written").await.expect("put b");
        let before = env.sim().ops();
        db.flush().await.expect("flush");
        env.sim().ops() - before
    };

    let mut behind = 0u64;
    for n in 1..=total {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        let stale = with_a_stale_tail(&env, &dir).await;
        let db = roble::open_with(&env, &dir, flush_options())
            .await
            .expect("the reopen over the stale tail");
        db.put(b"b", b"written").await.expect("put b");
        env.sim().fail_at(n);
        let _ = db.flush().await;
        env.sim().stop_failing();
        // The state the trap needs: the resumed log, no longer the newest.
        if db.current_log().is_some_and(|current| current > stale)
            && db.versions().min_unflushed_log_num <= stale
        {
            behind += 1;
        }
        drop(db);
        env.sim().crash();

        let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
            panic!("the flush failed at operation {n} of {total}: the reopen: {e}")
        });
        let found = contents(&db).await;
        let keys: Vec<&[u8]> = found.keys().map(Vec::as_slice).collect();
        assert_eq!(
            keys,
            [b"a".as_slice(), b"b"],
            "the flush failed at operation {n} of {total}"
        );
    }
    assert!(
        behind > 0,
        "no failure left the resumed log behind a newer one, so nothing here tested it"
    );
}

/// A store that writes without syncing, and flushes only when asked.
fn buffered_options() -> Options<SimFs> {
    flush_options().wal(roble::Wal::Buffered)
}

/// Writes `a` and flushes it, writes `b` without a sync, then flushes again
/// with operation `fail` of that flush made to fail, and crashes.
///
/// Returns the log `b` went to.
async fn buffered_flush_fails(env: &SimEnv, dir: &Path, fail: u64) -> u64 {
    let db = roble::open_with(env, dir, buffered_options())
        .await
        .expect("create");
    db.put(b"a", b"written").await.expect("put a");
    db.flush().await.expect("flush a");
    db.put(b"b", b"written").await.expect("put b");
    let log = db
        .current_log()
        .expect("a store with a WAL writes to a log");

    env.sim().fail_at(fail);
    let _ = db.flush().await;
    env.sim().stop_failing();
    drop(db);
    env.sim().crash();
    log
}

/// The logs in `dir`, by number.
async fn logs(env: &SimEnv, dir: &Path) -> Vec<u64> {
    env.sim()
        .list(dir)
        .await
        .expect("list")
        .iter()
        .filter_map(|e| roble_format::files::parse_filename(e.name.to_str()?))
        .filter(|p| p.file_type == roble_format::files::FileType::Log)
        .map(|p| p.file_num)
        .collect()
}

/// A log that a later one follows is replayed strictly, by roble and by
/// Pebble alike: a bad record in it is corruption, because a log is only
/// ever left behind once it is complete. A flush is what leaves one behind,
/// by creating the next log, so the one it retires has to be durable before
/// the new one's name is.
///
/// Without a sync of every write, that is the flush's to do. Skipped, a
/// machine crash after the new log's directory sync keeps the new log and
/// only the part of the old one the disk happened to write — which a real
/// disk may cut in the middle of a record, and which the strict replay then
/// refuses. The simulated filesystem drops unsynced bytes whole rather than
/// tearing them, so the open goes through; what shows here is the write the
/// old log lost although a later log survived it.
#[compio::test]
async fn a_flush_makes_the_log_it_moves_past_durable_before_the_next_one_is() {
    let total = {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        let db = roble::open_with(&env, &dir, buffered_options())
            .await
            .expect("create");
        db.put(b"a", b"written").await.expect("put a");
        db.flush().await.expect("flush a");
        db.put(b"b", b"written").await.expect("put b");
        let before = env.sim().ops();
        db.flush().await.expect("flush b");
        env.sim().ops() - before
    };

    let mut moved_past = 0u64;
    for fail in 1..=total {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        let log = buffered_flush_fails(&env, &dir, fail).await;
        let later = logs(&env, &dir).await.into_iter().any(|n| n > log);

        let db = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("the flush failed at operation {fail}: the reopen: {e}"));
        let found = contents(&db).await;
        assert!(found.contains_key(b"a".as_slice()), "a was flushed");
        if later {
            moved_past += 1;
            assert!(
                found.contains_key(b"b".as_slice()),
                "the flush failed at operation {fail} of {total}, after a log later than \
                 {log} was durable, and {log} had lost the write it held"
            );
        }
    }

    // The guard against passing for the wrong reason: some failure has to
    // come after the new log's name was durable, or nothing was tested.
    assert!(
        moved_past > 0,
        "no failure of the flush left a later log behind"
    );
}
