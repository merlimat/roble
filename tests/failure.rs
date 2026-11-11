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
//! crash. `tests/env.rs` already does that for the simulated filesystem
//! itself, which proves the injector works; this does it to **the engine**,
//! which is the claim §12 is actually making.
//!
//! The two invariants are the ones §12 states, and neither is about the
//! failing call's return value — a failed write is allowed to fail:
//!
//! 1. **It still opens.** "A half-written table, a torn log record or a
//!    half-rotated marker must leave the database openable." A store that
//!    cannot be reopened after a failed write has turned a recoverable error
//!    into a lost database.
//! 2. **It holds one of two states.** After a crash, reopening yields the
//!    state as of the last synced write — so the contents are either what was
//!    there before the scenario or what is there after it, and never anything
//!    in between. A batch is one record, so there is no third answer that
//!    could be correct.
//!
//! Neither is checked by asserting on an error. What the suite had before
//! this was tests that drive a store over a disk that always works, which
//! cannot distinguish a store that handles failure from one that has never
//! met any.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, WriteBuf};
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

/// Open, write a batch, flush it to a table.
///
/// Every step touches the disk in a different way — the open reads markers, a
/// MANIFEST and the tables it names; the write appends to a log and syncs it;
/// the flush writes a table, syncs it, appends a version edit and syncs the
/// directory. An operation count over this scenario therefore walks the whole
/// width of §12's "all I/O goes through `Fs` and `Disk`".
async fn scenario(env: &SimEnv, dir: &Path) -> roble::Result<()> {
    let db = roble::open(env, dir).await?;
    let mut batch = Batch::new();
    batch.put(b"injected-1", b"one")?;
    batch.put(b"injected-2", b"two")?;
    batch.delete(b"alpha")?;
    db.write(&mut batch, true).await?;
    db.flush().await?;
    Ok(())
}

/// Runs the scenario once with nothing failing, and returns how many disk
/// operations it took.
async fn count_operations() -> u64 {
    let (env, dir) = load("db-simple").await;
    let before = env.sim().ops();
    scenario(&env, &dir)
        .await
        .expect("the scenario must pass when nothing fails");
    env.sim().ops() - before
}

/// The contents before the scenario and after it, which are the only two
/// answers a reopened database may give.
async fn the_two_legal_states() -> (Model, Model) {
    let (env, dir) = load("db-simple").await;
    let before = {
        let db = roble::open(&env, &dir).await.expect("open");
        contents(&db).await
    };
    scenario(&env, &dir).await.expect("the scenario");
    let after = {
        let db = roble::open(&env, &dir).await.expect("reopen");
        contents(&db).await
    };
    assert_ne!(before, after, "the scenario must change something");
    (before, after)
}

#[compio::test]
async fn a_failure_at_any_single_operation_leaves_a_database_that_opens() {
    let total = count_operations().await;
    assert!(
        total >= 10,
        "a scenario of {total} operations is too thin to walk the I/O paths"
    );

    let mut refused = 0u64;
    for n in 1..=total {
        let (env, dir) = load("db-simple").await;
        env.sim().fail_at(n);
        // The scenario is allowed to fail. What is not allowed is a panic, or
        // a database that cannot be opened afterwards.
        let failed = scenario(&env, &dir).await.is_err();
        if failed {
            refused += 1;
        }
        env.sim().stop_failing();

        let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
            panic!("failing operation {n} of {total} left a database that will not open: {e}")
        });
        // And one that can be read, not merely opened: a version naming a
        // table that is not there opens and then fails on the first scan.
        let _ = contents(&db).await;
    }

    // The guard that keeps this from passing for the wrong reason. If the
    // injector stopped reaching the engine — a path that no longer goes
    // through `Fs`, a scenario that stopped touching the disk — every run
    // would succeed and every reopen would trivially work, and the loop above
    // would still be green.
    assert!(
        refused * 2 >= total,
        "only {refused} of {total} injections made the scenario fail; \
         the engine is barely meeting the failing disk"
    );
}

#[compio::test]
async fn a_crash_at_any_single_operation_leaves_one_of_two_states() {
    let total = count_operations().await;
    let (before, after) = the_two_legal_states().await;

    let (mut kept, mut lost) = (0u64, 0u64);
    for n in 1..=total {
        let (env, dir) = load("db-simple").await;
        env.sim().fail_at(n);
        let _ = scenario(&env, &dir).await;
        env.sim().stop_failing();

        // The machine dies where the operation did: everything not synced is
        // gone, and so is every name no directory sync made durable.
        env.sim().crash();

        let db = roble::open(&env, &dir).await.unwrap_or_else(|e| {
            panic!("a crash at operation {n} of {total} left a database that will not open: {e}")
        });
        let found = contents(&db).await;
        assert!(
            found == before || found == after,
            "a crash at operation {n} of {total} left a third state: {} keys, \
             where before has {} and after has {}",
            found.len(),
            before.len(),
            after.len()
        );
        if found == after {
            kept += 1;
        } else {
            lost += 1;
        }
    }

    // Both answers have to turn up, or the assertion above is satisfied by
    // one of them and proves only that the other never happens. Early
    // operations crash before the write is durable and late ones after it, so
    // a sweep that saw only one end would mean the scenario stopped spanning
    // the sync that divides them.
    assert!(
        kept > 0 && lost > 0,
        "the crash sweep saw {kept} crashes keeping the write and {lost} losing it; \
         it has to see both to be comparing anything"
    );
}
