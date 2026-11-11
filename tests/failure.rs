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

async fn contents<F: Fs>(db: &roble::Opened<'_, F>) -> Model {
    let mut scan = db.scan().await.expect("scan");
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
    if wal { o } else { o.disable_wal(true) }
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
        db.write(&mut batch, wal).await?;
        db.flush().await?;
        db.compact_once().await?;
    }
    let mut tail = Batch::new();
    tail.put(WRITTEN[2], b"written")?;
    db.write(&mut tail, wal).await?;
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
        assert!(
            refused * 2 >= total,
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
