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
