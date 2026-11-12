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

//! The memtable's size budget (§4, §8).
//!
//! The only thing that bounds a store's memory. Nothing else writes a
//! memtable out unless a caller asks: `Memtable::bytes()` reported and
//! `flush()` acted, and between them there was nothing that decided — so a
//! store written to and never flushed grew until the process ran out of
//! memory.
//!
//! The bound is not a ceiling on the arena, and these tests check the one it
//! actually claims rather than the one that would be nicer to have. The batch
//! that takes a memtable over the line is applied whole, so a memtable holds
//! at most the budget plus one batch; a store holds at most two of them, the
//! second being one a flush is writing out (§5).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// The value every test writes. Long enough that a budget can be stated in
/// whole entries without the key lengths mattering much.
const VALUE: &[u8] = b"0123456789abcdef0123456789abcdef";

/// How many entries the budget in these tests holds.
const ENTRIES: u64 = 10;

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

/// What one `put` of `key_at(i)` adds to the arena: the internal key, which
/// is the user key and an eight-byte trailer, and the value
/// (`crate::memtable::Memtable::bytes`).
fn entry_cost() -> u64 {
    (key_at(0).len() + 8 + VALUE.len()) as u64
}

/// One key per batch, so that "the budget plus one batch" is a figure this
/// file can state exactly rather than estimate.
async fn put<F: Fs>(db: &roble::Opened<F>, i: usize, wal: bool) -> roble::Result<()> {
    let mut batch = Batch::new();
    batch.put(&key_at(i), VALUE).expect("put");
    db.write(&mut batch, wal).await
}

/// Writes until the store is at `budget`, and says how many keys that took.
///
/// Capped, and the cap is not padding: a store that flushed on every write
/// would never reach the line, and a test that waited for it would hang
/// rather than fail. A sweep that hangs tells nobody anything.
async fn fill_to<F: Fs>(db: &roble::Opened<F>, budget: u64, wal: bool) -> usize {
    let mut i = 0;
    while (db.unflushed_bytes() as u64) < budget {
        put(db, i, wal).await.expect("write");
        i += 1;
        assert!(
            i <= 4 * ENTRIES as usize,
            "{i} writes have not filled a budget of {budget} bytes, and {} are waiting",
            db.unflushed_bytes()
        );
    }
    i
}

/// A budget of `budget` bytes, and compaction left to the caller.
///
/// A flush a write asked for runs the compaction the picker chose, which
/// merges the tables these tests are counting. Leaving it to the caller is
/// what lets one flush be one table; that the merge happens at all is
/// `a_flush_a_write_asked_for_still_compacts` below, and
/// `tests/schedule.rs` is where the scheduling is the subject.
fn opened(wal: bool, budget: u64) -> Options<SimFs> {
    let o = Options::new()
        .memtable_size(budget)
        .disable_automatic_compactions(true);
    if wal { o } else { o.disable_wal(true) }
}

/// The keys of `written` that `found` holds, as a count, or `None` when what
/// it holds is not a prefix of the order they were written in (§12).
fn durable_prefix(found: &Model, written: usize) -> Option<usize> {
    let present: Vec<bool> = (0..written)
        .map(|i| found.contains_key(&key_at(i)))
        .collect();
    let n = present.iter().take_while(|p| **p).count();
    present[n..].iter().all(|p| !*p).then_some(n)
}

#[compio::test]
async fn a_write_that_finds_the_memtable_full_writes_it_out() {
    for wal in [true, false] {
        let budget = ENTRIES * entry_cost();
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(&env, &dir, opened(wal, budget))
            .await
            .expect("open");
        let mut model = contents(&db).await;
        let tables_before = db.versions().version.table_count();

        let written = 4 * ENTRIES as usize;
        for i in 0..written {
            put(&db, i, wal).await.expect("write");
            model.insert(key_at(i), VALUE.to_vec());
        }

        assert!(
            db.versions().version.table_count() >= tables_before + 3,
            "wal={wal}: {written} writes over a budget of {ENTRIES} entries should have \
             written out at least three memtables, and the version holds {} tables where \
             it held {tables_before}",
            db.versions().version.table_count()
        );
        assert!(
            (db.unflushed_bytes() as u64) < written as u64 * entry_cost(),
            "wal={wal}: and should not still be holding all of it"
        );
        assert_eq!(
            contents(&db).await,
            model,
            "wal={wal}: flushing on its own changed an answer"
        );

        drop(db);
        let reopened = roble::open_with(&env, &dir, opened(wal, budget))
            .await
            .expect("reopen");
        let found = contents(&reopened).await;
        if wal {
            assert_eq!(
                found, model,
                "every write was synced, so all of it comes back"
            );
        } else {
            // With the log off a write is durable only once a flush covers
            // it, and the flushes here are the ones the budget asked for —
            // so what comes back is a prefix, and a prefix is the whole
            // claim: "the second key is there and the first is not" is the
            // shape that says a crash was handled wrongly (§12).
            let kept = durable_prefix(&found, written)
                .unwrap_or_else(|| panic!("what survived is not a prefix of what was written"));
            assert!(
                kept >= ENTRIES as usize,
                "the budget's own flushes should have made at least one memtable durable, \
                 and only {kept} of {written} keys came back"
            );
        }
    }
}

#[compio::test]
async fn the_memtable_stays_inside_its_budget_and_one_batch() {
    for wal in [true, false] {
        let cost = entry_cost();
        let budget = ENTRIES * cost;
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(&env, &dir, opened(wal, budget))
            .await
            .expect("open");

        for i in 0..4 * ENTRIES as usize {
            put(&db, i, wal).await.expect("write");
            // Room is made before the batch is applied, so what is waiting
            // when a write returns is a memtable that was under the budget
            // plus the one batch that went into it.
            assert!(
                (db.unflushed_bytes() as u64) < budget + cost,
                "wal={wal}: after {} writes, {} bytes are waiting on a budget of {budget}",
                i + 1,
                db.unflushed_bytes()
            );
        }
    }
}

#[compio::test]
async fn a_budget_nothing_reaches_leaves_the_memtable_alone() {
    // The other half of the claim. A store told to hold everything holds
    // everything: this is what a caller doing its own flushing gets, and
    // what every test written before the budget existed relies on.
    for wal in [true, false] {
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(&env, &dir, opened(wal, u64::MAX))
            .await
            .expect("open");
        let tables_before = db.versions().version.table_count();
        let waiting_before = db.unflushed_bytes() as u64;

        let written = 4 * ENTRIES as usize;
        for i in 0..written {
            put(&db, i, wal).await.expect("write");
        }

        assert_eq!(
            db.versions().version.table_count(),
            tables_before,
            "wal={wal}: no write should have decided to flush"
        );
        assert_eq!(
            db.unflushed_bytes() as u64,
            waiting_before + written as u64 * entry_cost(),
            "wal={wal}: and every one of them should still be waiting"
        );
    }
}

#[compio::test]
async fn the_room_is_made_before_the_batch_is_accepted() {
    // The ordering, and why it is worth a test of its own: a caller handed an
    // error has nothing of its batch in the store and can write the same
    // batch again. The other way round, the batch would be in the log and in
    // the memtable and reported as failed, and a caller doing the obvious
    // thing would write bytes the store already had.
    for wal in [true, false] {
        let budget = ENTRIES * entry_cost();
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open_with(&env, &dir, opened(wal, budget))
            .await
            .expect("open");
        let mut model = contents(&db).await;

        // Up to the line, without crossing it.
        let i = fill_to(&db, budget, wal).await;
        for j in 0..i {
            model.insert(key_at(j), VALUE.to_vec());
        }

        // The next write has to make room, and cannot.
        let refused = key_at(i);
        let mut batch = Batch::new();
        batch.put(&refused, VALUE).expect("put");
        let waiting = db.unflushed_bytes();
        env.sim().fail_at(1);
        let result = db.write(&mut batch, wal).await;
        env.sim().stop_failing();

        assert!(
            result.is_err(),
            "wal={wal}: a write that could not make room should say so"
        );
        assert_eq!(
            db.unflushed_bytes(),
            waiting,
            "wal={wal}: and should have applied nothing"
        );
        assert_eq!(
            contents(&db).await,
            model,
            "wal={wal}: nor changed any answer"
        );

        // The same batch again, which is what a caller would do.
        db.write(&mut batch, wal).await.expect("the write again");
        model.insert(refused.clone(), VALUE.to_vec());
        assert_eq!(
            db.get(&refused, Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(VALUE.to_vec()),
            "wal={wal}: a batch refused for want of room should write unchanged"
        );
        assert_eq!(
            contents(&db).await,
            model,
            "wal={wal}: and leave the store holding exactly what was written"
        );
    }
}

#[compio::test]
async fn a_flush_a_write_asked_for_still_compacts() {
    // A flush and not half of one. A store filled only through `write` would
    // otherwise pile L0 up for ever, which is the thing a flush was taught to
    // prevent (§9) — and nothing else would ever notice, because the caller
    // never calls `flush` and so never reaches the place that looks.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .memtable_size(ENTRIES * entry_cost())
            .l0_compaction_threshold(1),
    )
    .await
    .expect("open");
    let mut model = contents(&db).await;

    let written = 4 * ENTRIES as usize;
    for i in 0..written {
        put(&db, i, true).await.expect("write");
        model.insert(key_at(i), VALUE.to_vec());
    }

    // Read in a block, so the borrow is gone before the scan below waits.
    let (below, in_l0) = {
        let versions = db.versions();
        (
            versions
                .version
                .levels()
                .any(|(level, tables)| level > 0 && !tables.is_empty()),
            versions.version.level(0).len(),
        )
    };
    assert!(
        below,
        "every table is still in L0, so no flush ran the compaction it chose"
    );
    assert!(
        in_l0 <= 1,
        "L0 holds {in_l0} tables on a threshold of one, so the merges are not keeping up \
         with the flushes the budget asks for"
    );
    assert_eq!(contents(&db).await, model, "and a merge changed an answer");
}

#[compio::test]
async fn the_bound_holds_even_after_a_flush_has_failed() {
    // Every memtable counts toward the budget, the one a failed flush left
    // frozen included — so the write after such a flush is the one that
    // retries it. Weighing only the memtable in front would let a store hold
    // a frozen budget's worth *and* fill a fresh one behind it.
    //
    // Swept rather than aimed at one operation: where in a flush the failure
    // lands decides whether a memtable is left frozen at all, and that is not
    // this test's business to know.
    for wal in [true, false] {
        let cost = entry_cost();
        let budget = ENTRIES * cost;
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
            "wal={wal}: a flush of {total} operations is too thin to sweep"
        );

        for n in 1..=total {
            let (env, dir) = load("db-twin-a").await;
            let db = roble::open_with(&env, &dir, opened(wal, budget))
                .await
                .expect("open");
            let mut i = fill_to(&db, budget, wal).await;

            // The write that has to make room, and cannot.
            env.sim().fail_at(n);
            let _ = put(&db, i, wal).await;
            env.sim().stop_failing();

            // From here the bound is the whole claim, through writes that
            // succeed and writes that are refused alike.
            for _ in 0..2 * ENTRIES {
                let _ = put(&db, i, wal).await;
                i += 1;
                assert!(
                    (db.unflushed_bytes() as u64) < budget + cost,
                    "wal={wal}: failing at {n} of {total}, {} bytes are waiting on a \
                     budget of {budget}",
                    db.unflushed_bytes()
                );
            }

            // And it still opens, holding what it said it held (§12).
            let held = contents(&db).await;
            drop(db);
            let reopened = roble::open_with(&env, &dir, opened(wal, u64::MAX))
                .await
                .unwrap_or_else(|e| panic!("wal={wal}: failing at {n} of {total}: {e}"));
            let found = contents(&reopened).await;
            assert!(
                found.iter().all(|(k, v)| held.get(k) == Some(v)),
                "wal={wal}: failing at {n} of {total}, a reopen invented a value"
            );
        }
    }
}

#[compio::test]
async fn the_default_budget_is_not_something_ordinary_writes_reach() {
    // The default is the one setting here that changes what a store does
    // without anybody asking, so what it must not do is surprise a caller
    // that was flushing on its own schedule. Pebble's `MemTableSize` and the
    // same 4 MiB, which a few dozen small records are nowhere near.
    for wal in [true, false] {
        let (env, dir) = load("db-twin-a").await;
        let o = Options::new();
        let db = roble::open_with(&env, &dir, if wal { o } else { o.disable_wal(true) })
            .await
            .expect("open");
        let tables_before = db.versions().version.table_count();

        for i in 0..4 * ENTRIES as usize {
            put(&db, i, wal).await.expect("write");
        }

        assert_eq!(
            db.versions().version.table_count(),
            tables_before,
            "wal={wal}: {} bytes of small records should not have reached the default \
             budget of {} and flushed on their own",
            db.unflushed_bytes(),
            db.memtable_size()
        );
    }
}

#[compio::test]
async fn a_store_that_sets_nothing_still_flushes_on_its_own() {
    // The default carrying its weight, which is the only reason the budget is
    // a default and not a knob: a caller that asks for nothing and never
    // calls `flush` must still have its memory bounded. Large records rather
    // than many, so that 4 MiB is a few dozen writes and not a hundred
    // thousand.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, Options::new())
        .await
        .expect("open");
    let mut model = contents(&db).await;
    let budget = db.memtable_size();
    let tables_before = db.versions().version.table_count();

    // Stated before it is used, because the rest of this test is a loop
    // whose length comes out of it: a default of no budget at all would be
    // an unbounded loop rather than a failure.
    assert!(
        budget <= 16 << 20,
        "the default budget should be a few MiB, and is {budget}"
    );

    let big = vec![b'v'; 64 << 10];
    let cost = (key_at(0).len() + 8 + big.len()) as u64;
    let written = (budget / cost + 2) as usize;
    for i in 0..written {
        let mut batch = Batch::new();
        batch.put(&key_at(i), &big).expect("put");
        db.write(&mut batch, true).await.expect("write");
        model.insert(key_at(i), big.clone());
    }

    assert!(
        db.versions().version.table_count() > tables_before,
        "{written} records of {} bytes are past the default budget of {budget}, and the \
         store wrote nothing out",
        big.len()
    );
    assert!(
        (db.unflushed_bytes() as u64) < budget + cost,
        "and {} bytes are still waiting",
        db.unflushed_bytes()
    );
    assert_eq!(contents(&db).await, model, "and an answer changed");
}
