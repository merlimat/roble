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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, Options, Wal, WriteBuf};
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
/// itself — and before the compaction, not at its edit: a store that only
/// refused edit 7 had already let the compaction write over table 8, which
/// leaves a database that opens with the flush's keys gone. That is for the
/// next sweep to see.
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

/// One write to one key, as the caller saw it.
struct Write {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    /// Whether the batch it was in returned `Ok`.
    acked: bool,
}

/// Every write a run made, in order, and how many of them the store
/// promised to keep.
#[derive(Default)]
struct History {
    writes: Vec<Write>,
    /// Every acknowledged write before this index is one the store promised
    /// to keep: synced to the log, or written out by a flush or a checkpoint
    /// that returned.
    durable: usize,
}

impl History {
    /// The values `key` may read as, when every acknowledged write before
    /// `required` must be there and no write from `upto` on can be.
    ///
    /// The last such write, or the fixture's value if there is none, and any
    /// write to the key after it: one that failed, or one nothing has made
    /// durable yet, may have reached the disk or not.
    fn allowed(
        &self,
        initial: &Model,
        key: &[u8],
        required: usize,
        upto: usize,
    ) -> Vec<Option<Vec<u8>>> {
        let mut allowed = vec![initial.get(key).cloned()];
        for (i, write) in self.writes[..upto].iter().enumerate() {
            if write.key != key {
                continue;
            }
            if write.acked && i < required {
                allowed.clear();
            }
            allowed.push(write.value.clone());
        }
        allowed
    }

    /// Each key whose value in `found` is not one [`History::allowed`]
    /// allows, with what it read as and what it could have.
    fn wrong(&self, initial: &Model, found: &Model, required: usize, upto: usize) -> Vec<String> {
        let keys: BTreeSet<&Vec<u8>> = initial
            .keys()
            .chain(found.keys())
            .chain(self.writes.iter().map(|w| &w.key))
            .collect();
        let text =
            |v: &Option<Vec<u8>>| v.as_ref().map(|v| String::from_utf8_lossy(v).into_owned());
        let mut wrong = Vec::new();
        for key in keys {
            let got = found.get(key).cloned();
            let allowed = self.allowed(initial, key, required, upto);
            if !allowed.contains(&got) {
                wrong.push(format!(
                    "{} = {:?}, allowed {:?}",
                    String::from_utf8_lossy(key),
                    text(&got),
                    allowed.iter().map(text).collect::<Vec<_>>()
                ));
            }
        }
        wrong
    }
}

/// What a run of [`carry_on`] did, as its caller saw it.
#[derive(Default)]
struct Run {
    history: History,
    /// Each checkpoint that returned `Ok`: where, and how far into the
    /// history it was taken.
    checkpoints: Vec<(PathBuf, usize)>,
    /// Whether a call has returned an error yet.
    failed: bool,
    /// Whether a call was refused because the store records no more.
    refused: bool,
    /// How many checkpoints returned `Ok` after a call had failed.
    checkpoints_after_a_failure: usize,
}

impl Run {
    /// Takes account of what a call returned, and says whether it was `Ok`.
    fn saw<T>(&mut self, result: &roble::Result<T>) -> bool {
        if let Err(e) = result {
            self.failed = true;
            self.refused |= refuses_to_record(e);
        }
        result.is_ok()
    }
}

/// A store that rotates its MANIFEST on every flush and compaction, so that
/// a failure lands on a rotation as often as on an append, and merges L0 as
/// soon as it holds a table, so that every compaction [`carry_on`] asks for
/// has work.
fn carry_on_options(wal: Wal) -> Options<SimFs> {
    Options::new()
        .wal(wal)
        .background_tasks(false)
        .l0_compaction_threshold(1)
        .max_manifest_bytes(1)
}

/// Writes one batch, and records each of its writes as acknowledged or not.
async fn write_batch(
    db: &roble::Db<SimFs>,
    run: &mut Run,
    wal: Wal,
    writes: &[(String, Option<String>)],
) {
    let mut batch = Batch::new();
    for (key, value) in writes {
        match value {
            Some(value) => batch.put(key.as_bytes(), value.as_bytes()).expect("put"),
            None => batch.delete(key.as_bytes()).expect("delete"),
        }
    }
    let acked = run.saw(&db.write_raw(&mut batch, wal != Wal::Off).await);
    for (key, value) in writes {
        run.history.writes.push(Write {
            key: key.as_bytes().to_vec(),
            value: value.as_ref().map(|v| v.as_bytes().to_vec()),
            acked,
        });
    }
    if acked && wal != Wal::Off {
        // Synced before it returned.
        run.history.durable = run.history.writes.len();
    }
}

/// Three rounds of a batch that adds, overwrites and deletes; a flush; a
/// write the flush did not cover; a checkpoint; a compaction; and a sweep,
/// each carried on past whatever it returned. Then one last batch nothing
/// covers.
///
/// Every value is written once, so a key that reads as the wrong one says
/// which write it came from.
async fn carry_on(env: &SimEnv, dir: &Path, wal: Wal) -> (Option<roble::Db<SimFs>>, Run) {
    let mut run = Run::default();
    let opened = roble::open_with(env, dir, carry_on_options(wal)).await;
    run.saw(&opened);
    let Ok(db) = opened else {
        return (None, run);
    };
    let mut written = 0;
    let mut value = || {
        written += 1;
        Some(format!("w{written}"))
    };
    for round in 0..3 {
        let mut batch = vec![
            (format!("r{round}-a"), value()),
            (format!("r{round}-b"), value()),
            ("shared".to_string(), value()),
        ];
        if round > 0 {
            batch.push((format!("r{}-a", round - 1), None));
        }
        write_batch(&db, &mut run, wal, &batch).await;

        let before = run.history.writes.len();
        if run.saw(&db.flush().await) {
            run.history.durable = run.history.durable.max(before);
        }

        // After the flush and before the checkpoint, so that after a flush
        // that failed the checkpoint's own has two memtables to write out.
        let between = [
            (format!("r{round}-c"), value()),
            ("shared".to_string(), value()),
        ];
        write_batch(&db, &mut run, wal, &between).await;

        let at = PathBuf::from(format!("/cp{round}"));
        let before = run.history.writes.len();
        let failed_before = run.failed;
        if run.saw(&db.checkpoint(&at).await) {
            run.history.durable = run.history.durable.max(before);
            run.checkpoints.push((at, before));
            run.checkpoints_after_a_failure += usize::from(failed_before);
        }
        run.saw(&db.compact_once().await);
        run.saw(&db.remove_obsolete_files().await);
    }
    write_batch(&db, &mut run, wal, &[("tail".to_string(), value())]).await;
    (Some(db), run)
}

/// How a run ends before its directory is opened again.
#[derive(Clone, Copy, Debug)]
enum End {
    /// Closed, which leaves every byte a write put down, synced or not.
    Close,
    /// Crashed, which leaves only what a sync covered.
    Crash,
}

/// Everything `db` holds, or the error that stopped the scan.
async fn scanned<F: Fs>(db: &roble::Db<F>) -> roble::Result<Model> {
    let mut scan = db.range(..)?;
    scan.first().await?;
    let mut out = Model::new();
    while scan.is_valid() {
        out.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await?;
    }
    Ok(out)
}

/// Opens `dir` and reads everything in it.
async fn read_back(env: &SimEnv, dir: &Path, wal: Wal) -> roble::Result<Model> {
    let db = roble::open_with(env, dir, carry_on_options(wal)).await?;
    scanned(&db).await
}

/// Runs [`carry_on`] with operation `n` failing, if there is one, and ends
/// it as `end` says. Returns what is wrong with what it left — live, in each
/// checkpoint that returned, and reopened — and the run.
async fn carried_on(wal: Wal, n: Option<u64>, end: End, initial: &Model) -> (Vec<String>, Run) {
    let (env, dir) = load("db-simple").await;
    if let Some(n) = n {
        env.sim().fail_at(n);
    }
    let (db, run) = carry_on(&env, &dir, wal).await;
    env.sim().stop_failing();

    let history = &run.history;
    let all = history.writes.len();
    let mut wrong = Vec::new();
    let mut check =
        |what: &str, found: roble::Result<Model>, required: usize, upto: usize| match found {
            Ok(found) => wrong.extend(
                history
                    .wrong(initial, &found, required, upto)
                    .into_iter()
                    .map(|w| format!("{what}: {w}")),
            ),
            Err(e) => wrong.push(format!("{what}: {e}")),
        };
    if let Some(db) = db {
        // Every acknowledged write, whatever became of the disk.
        check("live", scanned(&db).await, all, all);
        match end {
            End::Close => db.close().await.expect("close"),
            End::Crash => drop(db),
        }
    }
    if let End::Crash = end {
        env.sim().crash();
    }
    for (at, upto) in &run.checkpoints {
        let found = read_back(&env, at, wal).await;
        check(&format!("checkpoint {}", at.display()), found, *upto, *upto);
    }
    let found = read_back(&env, &dir, wal).await;
    check("reopened", found, history.durable, all);
    (wrong, run)
}

/// A store told to carry on keeps what it acknowledged.
///
/// The careless caller again, held to more than a database that opens: to
/// what the store told it along the way. Every acknowledged write is there
/// live; every write before a checkpoint that returned is in that
/// checkpoint; and a reopen, with a crash first or without one, finds every
/// write that a synced log or a flush that returned made durable. A write
/// that failed, or one nothing has made durable yet, may be there or not —
/// so long as nothing older shows through it.
///
/// The scenario rotates the MANIFEST on every flush and compaction, and goes
/// on writing, flushing, checkpointing, compacting and sweeping past each
/// error, so that the one injected failure lands on each of those in turn
/// and what runs after it finds out what it left. What it found:
///
/// - after a failed flush, the next one wrote out the memtable left frozen
///   and not the writes since, so a checkpoint left them out, and with the
///   log off so did a reopen after a crash;
/// - after a failed append, the next merge wrote its output under the number
///   the failed record gave its table, and a clean reopen, which replays
///   that record, read the merge's keys where the flush's had been;
/// - a sweep after a failed append deleted the table the record names, and
///   one after a failed move to a new MANIFEST deleted the MANIFEST the new
///   marker names, and neither database would open.
#[compio::test]
async fn a_store_told_to_carry_on_keeps_what_it_acknowledged() {
    let initial = {
        let (env, dir) = load("db-simple").await;
        let db = roble::open(&env, &dir).await.expect("open");
        contents(&db).await
    };
    for wal in [Wal::Synced, Wal::Off] {
        let total = {
            let (env, dir) = load("db-simple").await;
            let before = env.sim().ops();
            let (_db, run) = carry_on(&env, &dir, wal).await;
            assert!(
                !run.failed,
                "wal={wal:?}: with nothing failing, every call should have returned Ok"
            );
            env.sim().ops() - before
        };
        let (wrong, _) = carried_on(wal, None, End::Close, &initial).await;
        assert!(
            wrong.is_empty(),
            "wal={wal:?}: with nothing failing: {wrong:#?}"
        );

        let mut runs = Vec::new();
        let mut refused = 0;
        let mut checkpoints_after_a_failure = 0;
        for n in 1..=total {
            for end in [End::Close, End::Crash] {
                let (wrong, run) = carried_on(wal, Some(n), end, &initial).await;
                if !wrong.is_empty() {
                    runs.push(format!(
                        "failing at {n} of {total}, then {end:?}:\n    {}",
                        wrong.join("\n    ")
                    ));
                }
                refused += usize::from(run.refused);
                checkpoints_after_a_failure += run.checkpoints_after_a_failure;
            }
        }
        assert!(
            runs.is_empty(),
            "wal={wal:?}: {} runs lost or invented a write:\n{}",
            runs.len(),
            runs.join("\n")
        );
        // The guards that keep this from passing for want of reaching
        // anything: a failure that left the store refusing to record, and a
        // checkpoint that returned after a failure, which is the one with
        // something to make up for.
        assert!(
            refused > 0,
            "wal={wal:?}: no failure left the store refusing to record"
        );
        assert!(
            checkpoints_after_a_failure > 0,
            "wal={wal:?}: no checkpoint returned after a failure"
        );
    }
}

/// Three synced writes, for the flush beside a merge to write out.
async fn beside_a_merge(db: &roble::Db<SimFs>) -> Model {
    let mut written = Model::new();
    for i in 0..3 {
        let key = format!("beside-{i}").into_bytes();
        let mut batch = Batch::new();
        batch.put(&key, b"synced").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
        written.insert(key, b"synced".to_vec());
    }
    written
}

/// A merge running when a flush's edit fails takes no number the edit gave
/// out.
///
/// A merge holds no permit, so a flush can run while it reads, and it
/// numbers each output only as the output begins (`take_file_num`). The
/// flush read the counter and left moving it to its edit; when the edit's
/// sync fails the record is in the MANIFEST all the same, and the counter
/// still offers the number it gave the flush's table. The merge was past
/// any check at its start before anything failed, so it is the number
/// itself that has to be refused. A merge that took it would write over the
/// flush's table, and a clean reopen — replaying the record — would read
/// the merge's keys in its place, and not the synced writes whose log the
/// record retired.
///
/// The carry-on sweep cannot get here: nothing on the simulated filesystem
/// suspends unless told to, so there a merge always starts after the
/// failure or finishes before it.
#[compio::test]
async fn a_merge_running_when_an_edit_fails_takes_no_number_the_edit_gave_out() {
    // `db-reads` arrives wanting a compaction, which is the merge.
    let options = || {
        Options::new()
            .background_tasks(false)
            .disable_automatic_compactions(true)
    };
    let initial = {
        let (env, dir) = load("db-reads").await;
        let db = roble::open_with(&env, &dir, options()).await.expect("open");
        contents(&db).await
    };
    let mut cx = Context::from_waker(Waker::noop());
    let total = {
        let (env, dir) = load("db-reads").await;
        let db = roble::open_with(&env, &dir, options()).await.expect("open");
        beside_a_merge(&db).await;
        env.sim().pause_at(1);
        let mut merging = pin!(db.compact_once());
        assert!(
            merging.as_mut().poll(&mut cx).is_pending(),
            "the merge should have suspended"
        );
        let before = env.sim().ops();
        db.flush().await.expect("flush").expect("a table");
        env.sim().ops() - before
    };

    let mut refused = 0;
    for n in 1..=total {
        let (env, dir) = load("db-reads").await;
        let db = roble::open_with(&env, &dir, options()).await.expect("open");
        let mut model = initial.clone();
        model.extend(beside_a_merge(&db).await);

        let (flushed, merged) = {
            // Driven by hand as far as its first operation: its tables
            // claimed, no permit held, and no output numbered yet.
            let counter = db.versions().next_file_num;
            env.sim().pause_at(1);
            let mut merging = pin!(db.compact_once());
            assert!(
                merging.as_mut().poll(&mut cx).is_pending(),
                "failing at {n}: the merge should have suspended at its first operation"
            );
            assert_eq!(
                db.versions().next_file_num,
                counter,
                "failing at {n}: the merge should not have numbered an output yet"
            );

            env.sim().fail_at(n);
            let flushed = db.flush().await;
            env.sim().stop_failing();

            let mut polls = 0;
            let merged = loop {
                if let Poll::Ready(merged) = merging.as_mut().poll(&mut cx) {
                    break merged;
                }
                polls += 1;
                assert!(
                    polls < 100_000,
                    "failing at {n}: the merge is not making progress"
                );
            };
            (flushed, merged)
        };
        // Only a failure that left the store recording no more is this
        // test's business; after any other, the merge is free to land.
        match (flushed, merged) {
            (Err(_), Err(e)) if refuses_to_record(&e) => refused += 1,
            _ => continue,
        }

        drop(db);
        let reopened = roble::open_with(&env, &dir, options())
            .await
            .unwrap_or_else(|e| panic!("failing at {n} of {total}, the reopen: {e}"));
        assert_eq!(
            contents(&reopened).await,
            model,
            "failing at {n} of {total}: a clean reopen replays the edit that failed, and should \
             read the table it names as the flush wrote it"
        );
    }
    assert!(
        refused > 0,
        "no failure in the flush left the merge beside it refused, so nothing here reached a \
         merge running when an edit failed"
    );
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

/// A store that wrote `a` and closed, with `stale` left past the log's last
/// record — what a log Pebble recycled holds after its records, or a record
/// torn by a kill. Returns the log's number.
async fn with_a_stale_tail(env: &SimEnv, dir: &Path, stale: &[u8]) -> u64 {
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
    let (result, _) = disk.write(WriteBuf::new(stale.to_vec()), end, ()).await;
    result.expect("write the stale tail");
    disk.sync(()).await.expect("sync");
    disk.close().await.expect("close");
    log
}

/// The stale tails a reopen may find past a log's last record:
///
/// - Letters, as the middle of an old value is: no header a reader could
///   mistake for a record, only bytes it has to stop at.
/// - Fewer bytes than any header, letters or zeros, ending the file: too
///   few for a reader to take for a chunk at all.
/// - Seven to eighteen zeros ending the file, which a reader takes for a
///   block's padding.
fn stale_tails() -> Vec<Vec<u8>> {
    let letters = |n: u32| -> Vec<u8> { (0..n).map(|i| b'a' + (i * 7 % 26) as u8).collect() };
    vec![
        letters(300),
        letters(1),
        letters(4),
        letters(6),
        vec![0; 4],
        vec![0; 7],
        vec![0; 12],
        vec![0; 18],
    ]
}

/// A write made after a reopen over a stale log tail is still there at the
/// next open.
///
/// The reopen carries on in the log where its records end, cut there first.
/// A tail too short for a header, or zeros a reader steps over as padding,
/// reads as the log's end, which once moved the place the reopen carried on
/// from to past the tail: nothing was cut, the next record went after the
/// tail, and no reader gets past the tail to it.
#[compio::test]
async fn a_write_after_a_reopen_over_a_stale_log_tail_is_there_at_the_next_open() {
    for stale in stale_tails() {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        with_a_stale_tail(&env, &dir, &stale).await;
        let db = roble::open_with(&env, &dir, flush_options())
            .await
            .expect("the reopen over the stale tail");
        db.put(b"b", b"written").await.expect("put b");
        drop(db);
        env.sim().crash();

        let db = roble::open(&env, &dir).await.expect("reopen");
        let found = contents(&db).await;
        let keys: Vec<&[u8]> = found.keys().map(Vec::as_slice).collect();
        assert_eq!(
            keys,
            [b"a".as_slice(), b"b"],
            "a stale tail of {} bytes, from {:02x?}",
            stale.len(),
            &stale[..stale.len().min(8)]
        );
    }
}

/// A log whose last record ends closer to a block's end than a header's
/// length, which its writer pads with zeros to the block's end, is carried
/// on in after a reopen, whether the padding is there in full, in part or
/// not at all, and whether a block of an earlier life follows it.
///
/// The reopen carries on where the record ends, and a record framed from
/// there has to go to the next block, behind the padding, since no chunk
/// starts where its header does not fit.
#[compio::test]
async fn a_reopen_carries_on_in_a_log_whose_last_record_ends_near_a_block_boundary() {
    use roble_format::record::{BLOCK_SIZE, RECYCLABLE_HEADER_LEN, RecordWriter};
    let block = BLOCK_SIZE as u64;
    // A put whose record ends five bytes short of the first block's end:
    // the chunk's header, and the batch's twelve bytes of header, one of
    // kind, one of key length, the key, and three of value length.
    let value = vec![b'v'; BLOCK_SIZE - 5 - RECYCLABLE_HEADER_LEN - 18];

    for (case, keep, stale_block) in [
        ("without its padding", block - 5, false),
        ("with two of its five bytes of padding", block - 3, false),
        ("with its padding", block, false),
        (
            "with its padding and a block of an earlier life",
            block,
            true,
        ),
    ] {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        let db = roble::open_with(&env, &dir, flush_options())
            .await
            .expect("create");
        db.put(b"a", &value).await.expect("put a");
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
        assert_eq!(
            disk.len().await.expect("len"),
            block,
            "the record and its padding fill the first block"
        );
        disk.set_len(keep).await.expect("cut the log");
        if stale_block {
            let mut earlier = RecordWriter::recyclable((log as u32).wrapping_sub(1));
            earlier
                .write_record(b"a record of the file's earlier life")
                .expect("frame");
            let (result, _) = disk.write(WriteBuf::new(earlier.finish()), keep, ()).await;
            result.expect("write the earlier life");
        }
        disk.sync(()).await.expect("sync");
        disk.close().await.expect("close");

        let db = roble::open_with(&env, &dir, flush_options())
            .await
            .unwrap_or_else(|e| panic!("{case}: the reopen: {e}"));
        db.put(b"b", b"written").await.expect("put b");
        drop(db);
        env.sim().crash();

        let db = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("{case}: the open after it: {e}"));
        let found: Vec<(Vec<u8>, usize)> = contents(&db)
            .await
            .into_iter()
            .map(|(k, v)| (k, v.len()))
            .collect();
        assert_eq!(
            found,
            [(b"a".to_vec(), value.len()), (b"b".to_vec(), 7)],
            "the record {case}"
        );
    }
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
    for tail in stale_tails() {
        let total = {
            let env = SimEnv::new();
            let dir = PathBuf::from("/db");
            with_a_stale_tail(&env, &dir, &tail).await;
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
            let stale = with_a_stale_tail(&env, &dir, &tail).await;
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
                panic!(
                    "a stale tail of {} bytes, the flush failed at operation {n} of {total}: \
                     the reopen: {e}",
                    tail.len()
                )
            });
            let found = contents(&db).await;
            let keys: Vec<&[u8]> = found.keys().map(Vec::as_slice).collect();
            assert_eq!(
                keys,
                [b"a".as_slice(), b"b"],
                "a stale tail of {} bytes, the flush failed at operation {n} of {total}",
                tail.len()
            );
        }
        assert!(
            behind > 0,
            "a stale tail of {} bytes: no failure left the resumed log behind a newer one, \
             so nothing here tested it",
            tail.len()
        );
    }
}

/// A store that writes without syncing, and flushes only when asked.
fn buffered_options() -> Options<SimFs> {
    flush_options().wal(roble::Wal::Buffered)
}

/// Writes `a` and flushes it, writes `b` without a sync, then flushes again
/// with operation `fail` of that flush made to fail, writes `c` without a
/// sync after it, and crashes.
///
/// Returns each unflushed key with the log it went to.
async fn buffered_flush_fails(env: &SimEnv, dir: &Path, fail: u64) -> Vec<(&'static [u8], u64)> {
    let db = roble::open_with(env, dir, buffered_options())
        .await
        .expect("create");
    db.put(b"a", b"written").await.expect("put a");
    db.flush().await.expect("flush a");
    let mut written = Vec::new();
    db.put(b"b", b"written").await.expect("put b");
    written.push((b"b".as_slice(), db.current_log().expect("a log")));

    env.sim().fail_at(fail);
    let _ = db.flush().await;
    env.sim().stop_failing();
    db.put(b"c", b"written").await.expect("put c");
    written.push((b"c".as_slice(), db.current_log().expect("a log")));
    drop(db);
    env.sim().crash();
    written
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
///
/// And a flush that fails after creating the new log, but before the store
/// moved to it, leaves the store writing to a log that may already have a
/// later one behind it. The write after the failed flush is there for that:
/// it must be as durable as the one before.
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

    // Per key: how many failures left a later log durable behind the one it
    // went to.
    let mut moved_past = [0u64; 2];
    for fail in 1..=total {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        let written = buffered_flush_fails(&env, &dir, fail).await;
        let logs = logs(&env, &dir).await;

        let db = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("the flush failed at operation {fail}: the reopen: {e}"));
        let found = contents(&db).await;
        assert!(found.contains_key(b"a".as_slice()), "a was flushed");
        for (i, (key, log)) in written.iter().enumerate() {
            if logs.iter().any(|n| n > log) {
                moved_past[i] += 1;
                assert!(
                    found.contains_key(*key),
                    "the flush failed at operation {fail} of {total}; a log later than {log} \
                     was durable, and {log} had lost {}",
                    String::from_utf8_lossy(key)
                );
            }
        }
    }

    // The guard against passing for the wrong reason: some failure has to
    // come after the new log's name was durable, or nothing was tested.
    // `b` is written before the flush and `c` after it, into a log the flush
    // failed to move the store off: both have to be reached.
    assert!(
        moved_past.iter().all(|n| *n > 0),
        "no failure of the flush left a later log behind every write: {moved_past:?}"
    );
}

/// What the write that goes wrong puts under `k`: longer than what the write
/// after it puts there, so that a record written over its record leaves a
/// tail.
const LONGER: &[u8] = b"old-and-much-longer-than-the-next-one";

/// How many bytes log `log` in `dir` holds, synced or not.
async fn log_len(env: &SimEnv, dir: &Path, log: u64) -> u64 {
    let name = roble_format::files::log_filename(log);
    env.sim()
        .list(dir)
        .await
        .expect("list")
        .iter()
        .find(|e| e.name == Path::new(&name))
        .map_or(0, |e| e.size)
}

/// How the first write of `k` goes wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wrong {
    /// Its `n`th operation fails, and it hands back the error.
    Fails(u64),
    /// It is dropped while it waits to sync, its record written: a write its
    /// caller gave up on, which never gets to cut that record away.
    Dropped,
}

/// What comes first after the write that goes wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Next {
    /// `k` again, shorter, and then the flush: the next record goes where the
    /// one that went wrong began.
    Write,
    /// The flush, and then a delete of `k`: the flush syncs the log it moves
    /// past with nothing written after the record that went wrong.
    Flush,
}

/// What a run of [`two_faults`] did.
struct TwoFaults {
    /// How many operations the flush made.
    flush_ops: u64,
    /// Whether the run reached the state the trap needs: the record of the
    /// write that went wrong left in the log, and the flush leaving that log
    /// behind a newer one without retiring it.
    trapped: bool,
}

/// Writes `warm`; writes `k`, which goes wrong as `wrong` says; then, in the
/// order `next` says, flushes — with operation `flush` of the flush made to
/// fail, if one is given — and makes the write that follows the one that
/// went wrong; and crashes.
///
/// `None` if the first write of `k` was to fail and did not, which leaves
/// nothing to test.
async fn two_faults(
    env: &SimEnv,
    dir: &Path,
    next: Next,
    wrong: Wrong,
    flush: Option<u64>,
) -> Option<TwoFaults> {
    let db = roble::open_with(env, dir, flush_options())
        .await
        .expect("create");
    db.put(b"warm", b"x").await.expect("put warm");
    let log = db
        .current_log()
        .expect("a store with a WAL writes to a log");
    let before = log_len(env, dir, log).await;

    match wrong {
        Wrong::Fails(write) => {
            env.sim().fail_at(write);
            let failed = db.put(b"k", LONGER).await.is_err();
            env.sim().stop_failing();
            if !failed {
                return None;
            }
        }
        Wrong::Dropped => {
            // Its first operation appends the record and its second syncs
            // it, which is made to wait once before it runs: polled once, the
            // write is waiting there, and dropped there.
            env.sim().pause_at(2);
            let mut put = std::pin::pin!(db.put(b"k", LONGER));
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(
                put.as_mut().poll(&mut cx).is_pending(),
                "the write of k did not wait on its sync"
            );
        }
    }
    let left = log_len(env, dir, log).await > before;

    // The write after the one that went wrong is acknowledged and synced,
    // before the flush or after it, so no crash may take it.
    if next == Next::Write {
        db.put(b"k", b"new")
            .await
            .expect("the write after the one that went wrong");
    }
    if let Some(flush) = flush {
        env.sim().fail_at(flush);
    }
    let ops = env.sim().ops();
    let _ = db.flush().await;
    let flush_ops = env.sim().ops() - ops;
    env.sim().stop_failing();
    let behind = db.current_log().is_some_and(|current| current > log)
        && db.versions().min_unflushed_log_num <= log;
    if next == Next::Flush {
        db.delete(b"k")
            .await
            .expect("the write after the one that went wrong");
    }
    drop(db);
    env.sim().crash();
    Some(TwoFaults {
        flush_ops,
        trapped: left && behind,
    })
}

/// A write that failed, or was dropped while it waited on the disk, leaves
/// nothing of itself in the log for what comes after it, a write or a flush:
/// interrupt the flush anywhere and crash, and the directory opens with every
/// write the store acknowledged.
///
/// The record of a write whose sync fails is already in the file, past the
/// end the store keeps for the log, and nothing was applied for it: the next
/// write takes its sequence number and its place. Written over by that write,
/// a shorter record left the failed one's tail behind it. Harmless while the
/// log is the newest, whose end both engines read leniently; but a flush
/// creates a newer log before its edit retires this one, and a crash in
/// between left a log that was finished, by the rule both engines replay it
/// by, and did not read to its end: "record chunk type 117", a letter of the
/// value that failed. And synced by a flush before any write came — a flush
/// syncs the log it moves past — the failed record was durable, and the
/// replay brought it back at the sequence number the delete after the flush
/// had taken too. A delete loses to a write at the same number, so the
/// reopen read the value whose write had failed and lost a delete the store
/// had acknowledged.
///
/// A write that fails cuts its record away before it returns. A write dropped
/// while it waits on its sync never gets to, and leaves its record for the
/// write or the flush after it to cut: that is the run the guard below counts
/// on.
///
/// Two faults, which is why no sweep of one reached it: the write, to leave
/// its record past the log's end, and an operation of the flush, to leave the
/// log behind a newer one.
#[compio::test]
async fn a_write_that_failed_or_was_dropped_neither_tears_the_log_nor_comes_back() {
    let writes = write_operations().await;

    for next in [Next::Write, Next::Flush] {
        let mut expected = Model::from([(b"warm".to_vec(), b"x".to_vec())]);
        if next == Next::Write {
            expected.insert(b"k".to_vec(), b"new".to_vec());
        }
        let mut trapped = 0u64;
        for wrong in (1..=writes).map(Wrong::Fails).chain([Wrong::Dropped]) {
            // How many operations the flush makes, with nothing of it
            // failing, after a write that went wrong as this one does: a
            // flush that has a record to cut makes one more.
            let Some(dry) = two_faults(&SimEnv::new(), Path::new("/db"), next, wrong, None).await
            else {
                continue;
            };
            // One past the flush's last operation is a flush that succeeds.
            for flush in 1..=dry.flush_ops + 1 {
                let env = SimEnv::new();
                let dir = PathBuf::from("/db");
                let run = two_faults(&env, &dir, next, wrong, Some(flush))
                    .await
                    .expect("the write goes wrong as it did in the dry run");
                trapped += u64::from(run.trapped);

                let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
                    panic!(
                        "{next:?} next, the write {wrong:?}, the flush failing at {flush} of \
                         {}: the reopen: {e}",
                        dry.flush_ops
                    )
                });
                let k = db
                    .get(b"k", roble::Comparison::Equal)
                    .await
                    .expect("get k")
                    .map(|found| found.value);
                assert_eq!(
                    k.as_ref(),
                    expected.get(b"k".as_slice()),
                    "{next:?} next, the write {wrong:?}, the flush failing at {flush} of {}",
                    dry.flush_ops
                );
                assert_eq!(
                    contents(&db).await,
                    expected,
                    "{next:?} next, the write {wrong:?}, the flush failing at {flush} of {}",
                    dry.flush_ops
                );
            }
        }

        // The guard that keeps this from passing for the wrong reason: some
        // run has to have left the record of the write that went wrong in
        // the log, and the log behind a newer one, or nothing here met such
        // a record in a log read strictly.
        assert!(
            trapped > 0,
            "{next:?} next: no run left the record of the write that went wrong in a log a \
             newer one follows"
        );
    }
}

/// How many operations the write of `k` after `warm` makes, with nothing
/// failing.
async fn write_operations() -> u64 {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, flush_options())
        .await
        .expect("create");
    db.put(b"warm", b"x").await.expect("put warm");
    let before = env.sim().ops();
    db.put(b"k", LONGER).await.expect("put k");
    env.sim().ops() - before
}

/// A write that failed is not there when the store is opened again, after it
/// was dropped or closed rather than crashed.
///
/// The record of a write whose sync fails is already in the file, past the
/// end the store keeps for the log, and nothing was applied for it. Cut away
/// only once the log was next written to or synced, it was still there for a
/// store that went away first, and the open, which replays the last log up to
/// the first record that does not read, applied it: the reopen read back the
/// value whose write had failed, which the store before it never showed.
///
/// No crash here, which would pass without any cut: a crash keeps only what a
/// sync made durable, and the sync is what failed.
#[compio::test]
async fn a_write_that_failed_is_not_there_when_the_store_is_opened_again() {
    let writes = write_operations().await;
    assert!(
        writes >= 2,
        "a write of {writes} operations does not both append its record and sync it"
    );

    let expected = Model::from([(b"warm".to_vec(), b"x".to_vec())]);
    for close in [false, true] {
        let how = if close { "closed" } else { "dropped" };
        for write in 1..=writes {
            let env = SimEnv::new();
            let dir = PathBuf::from("/db");
            let db = roble::open_with(&env, &dir, flush_options())
                .await
                .expect("create");
            db.put(b"warm", b"x").await.expect("put warm");
            env.sim().fail_at(write);
            let failed = db.put(b"k", LONGER).await.is_err();
            env.sim().stop_failing();
            assert!(
                failed,
                "failing operation {write} of {writes} did not fail the write"
            );
            if close {
                db.close().await.expect("close");
            } else {
                drop(db);
            }

            let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
                panic!("{how}: the write failed at operation {write} of {writes}: the reopen: {e}")
            });
            assert_eq!(
                contents(&db).await,
                expected,
                "{how}: the write failed at operation {write} of {writes}"
            );
        }
    }
}

/// A store that edits its MANIFEST only when told to.
fn edit_options(wal: Wal) -> Options<SimFs> {
    Options::new()
        .create_if_missing(true)
        .disable_automatic_compactions(true)
        .background_tasks(false)
        .wal(wal)
}

/// A store with `a` and `b` flushed to a table each in L0, and `c` written
/// after them.
async fn written(env: &SimEnv, dir: &Path, wal: Wal) -> roble::Db<SimFs> {
    let db = roble::open_with(env, dir, edit_options(wal))
        .await
        .expect("create");
    for k in [b"a", b"b"] {
        db.put(k, b"written").await.expect("put");
        db.flush().await.expect("flush");
    }
    db.put(b"c", b"written").await.expect("put c");
    db
}

/// The two edits a store appends to its MANIFEST: a flush's, which retires
/// a log, and a compaction's, which retires tables.
#[derive(Clone, Copy, Debug)]
enum Edit {
    Flush,
    Compaction,
}

async fn edit(db: &roble::Db<SimFs>, edit: Edit) -> roble::Result<()> {
    match edit {
        Edit::Flush => db.flush().await.map(|_| ()),
        Edit::Compaction => {
            let l0: Vec<u64> = db
                .versions()
                .version
                .level(0)
                .iter()
                .map(|t| t.file_num)
                .collect();
            db.compact(0, &l0).await.map(|_| ())
        }
    }
}

/// Every table of the version, by level.
fn tables(db: &roble::Db<SimFs>) -> Vec<(u32, u64)> {
    db.versions()
        .version
        .levels()
        .flat_map(|(level, tables)| tables.iter().map(move |t| (level, t.file_num)))
        .collect()
}

#[compio::test]
async fn a_crash_after_a_reopen_keeps_everything_the_reopen_found() {
    // An edit is written to the MANIFEST and then synced. A process killed
    // between the two, or whose sync fails, leaves the record in the file
    // but not on the disk — and the next open, in a process that has the
    // same page cache, reads it back. What that open then does on the
    // strength of it has to survive the machine going down afterwards:
    // above all, deleting the log or the tables the record retired.
    let dir = PathBuf::from("/db");
    for wal in [Wal::Synced, Wal::Off] {
        for which in [Edit::Flush, Edit::Compaction] {
            let total = {
                let env = SimEnv::new();
                let db = written(&env, &dir, wal).await;
                let before = env.sim().ops();
                edit(&db, which).await.expect("edit");
                env.sim().ops() - before
            };
            let mut unsynced = 0;

            for n in 1..=total {
                for killed in [false, true] {
                    let env = SimEnv::new();
                    let db = written(&env, &dir, wal).await;
                    let before = tables(&db);
                    let finished = if killed {
                        // Stopped where operation `n` was about to run, and
                        // never resumed: the process is gone.
                        env.sim().pause_at(n);
                        let mut running = pin!(edit(&db, which));
                        let mut cx = Context::from_waker(Waker::noop());
                        matches!(running.as_mut().poll(&mut cx), Poll::Ready(Ok(())))
                    } else {
                        env.sim().fail_at(n);
                        let result = edit(&db, which).await;
                        env.sim().stop_failing();
                        result.is_ok()
                    };
                    drop(db);
                    let what = format!(
                        "{wal:?}, a {which:?} {} operation {n} of {total}",
                        if killed {
                            "killed before"
                        } else {
                            "failing at"
                        }
                    );

                    let db = roble::open_with(&env, &dir, edit_options(wal))
                        .await
                        .unwrap_or_else(|e| panic!("{what}: the reopen: {e}"));
                    let seen = tables(&db);
                    let found = contents(&db).await;
                    if seen != before && !finished {
                        unsynced += 1;
                    }
                    drop(db);
                    env.sim().crash();

                    let db = roble::open_with(&env, &dir, edit_options(wal))
                        .await
                        .unwrap_or_else(|e| {
                            panic!("{what}: reopened, crashed, and the open after it: {e}")
                        });
                    assert_eq!(
                        tables(&db),
                        seen,
                        "{what}: the version the reopen read did not survive the crash"
                    );
                    assert_eq!(
                        contents(&db).await,
                        found,
                        "{what}: the crash took what the reopen had found"
                    );
                }
            }

            // The guard: some interruption left the edit in the file and
            // not finished, and the reopen read it back — the window this
            // is about.
            assert!(
                unsynced > 0,
                "{wal:?}, a {which:?}: no interruption left a reopen reading back an \
                 edit that had not finished, so nothing here tested it"
            );
        }
    }
}

/// Interrupts `which` on a fresh store by a kill before its operation `n`,
/// and says whether it had finished.
async fn killed_editing(env: &SimEnv, dir: &Path, wal: Wal, which: Edit, n: u64) -> bool {
    let db = written(env, dir, wal).await;
    env.sim().pause_at(n);
    let finished = {
        let mut running = pin!(edit(&db, which));
        let mut cx = Context::from_waker(Waker::noop());
        matches!(running.as_mut().poll(&mut cx), Poll::Ready(Ok(())))
    };
    drop(db);
    finished
}

#[compio::test]
async fn a_crash_while_a_reopen_moves_to_a_fresh_manifest_loses_no_durable_write() {
    // The same interrupted edit, and then the reopen itself killed before
    // each of its operations and the machine crashed. Whatever the reopen
    // has done by then — rotated or not, swept or not — nothing durable may
    // be gone: a sweep that deleted what the edit retired before the new
    // MANIFEST was synced would be exactly that.
    let dir = PathBuf::from("/db");
    for wal in [Wal::Synced, Wal::Off] {
        for which in [Edit::Flush, Edit::Compaction] {
            let edits = {
                let env = SimEnv::new();
                let db = written(&env, &dir, wal).await;
                let before = env.sim().ops();
                edit(&db, which).await.expect("edit");
                env.sim().ops() - before
            };
            for n in 1..=edits {
                let opens = {
                    let env = SimEnv::new();
                    killed_editing(&env, &dir, wal, which, n).await;
                    let before = env.sim().ops();
                    roble::open_with(&env, &dir, edit_options(wal))
                        .await
                        .expect("reopen");
                    env.sim().ops() - before
                };
                for m in 1..=opens {
                    let env = SimEnv::new();
                    let finished = killed_editing(&env, &dir, wal, which, n).await;
                    env.sim().pause_at(m);
                    {
                        let mut opening = pin!(roble::open_with(&env, &dir, edit_options(wal)));
                        let mut cx = Context::from_waker(Waker::noop());
                        if let Poll::Ready(opened) = opening.as_mut().poll(&mut cx) {
                            panic!(
                                "the reopen should have paused at {m} of {opens}: {:?}",
                                opened.err()
                            );
                        }
                    }
                    env.sim().crash();
                    let what = format!(
                        "{wal:?}, a {which:?} killed before operation {n} of {edits}, \
                         the reopen killed before {m} of {opens}"
                    );
                    let db = roble::open_with(&env, &dir, edit_options(wal))
                        .await
                        .unwrap_or_else(|e| panic!("{what}: the open after the crash: {e}"));
                    let found = contents(&db).await;
                    // `a` and `b` were flushed; with the WAL on `c` was synced
                    // to the log, and with it off a finished flush took it.
                    let durable: &[&[u8]] = match (wal, which, finished) {
                        (Wal::Off, Edit::Flush, true) | (Wal::Synced, _, _) => &[b"a", b"b", b"c"],
                        _ => &[b"a", b"b"],
                    };
                    for k in durable {
                        assert!(
                            found.contains_key(*k),
                            "{what}: {} is gone, though it was durable",
                            String::from_utf8_lossy(k)
                        );
                    }
                }
            }
        }
    }
}
