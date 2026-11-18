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

//! What each `Wal` setting promises about a write once it has returned.
//!
//! The setting is the store's, not each write's, so these open a store in
//! each mode, write through every public path — `put`, `delete`,
//! `delete_range` and a batch's `commit` — and then end the store two ways:
//! a clean drop, which is the process going away, and the simulated
//! filesystem's crash, which is the machine going away and keeps only what
//! was synced.

use std::path::PathBuf;

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Db, Options, Wal};

async fn open(env: &SimEnv, wal: Wal) -> Db<SimFs> {
    roble::open_with(
        env,
        PathBuf::from("/db"),
        Options::new().create_if_missing(true).wal(wal),
    )
    .await
    .expect("open")
}

/// Writes through every public path, leaving `/b` and `/d` and nothing else.
async fn write(db: &Db<SimFs>) {
    db.put(b"/a", b"1").await.expect("put");
    db.put(b"/b", b"2").await.expect("put");
    db.delete(b"/a").await.expect("delete");
    let mut batch = db.indexed_batch();
    batch.put(b"/c", b"3").expect("put");
    batch.put(b"/d", b"4").expect("put");
    batch.commit().await.expect("commit");
    db.delete_range(b"/c", b"/d").await.expect("delete_range");
}

/// The keys the store holds, in order.
async fn keys(db: &Db<SimFs>) -> Vec<Vec<u8>> {
    let mut scan = db.range(..).expect("range");
    scan.first().await.expect("first");
    let mut out = Vec::new();
    while scan.is_valid() {
        out.push(scan.key().to_vec());
        scan.next().await.expect("next");
    }
    out
}

fn written() -> Vec<Vec<u8>> {
    vec![b"/b".to_vec(), b"/d".to_vec()]
}

#[compio::test]
async fn the_default_is_synced() {
    assert_eq!(Wal::default(), Wal::Synced);
}

#[compio::test]
async fn a_synced_write_survives_the_machine_going_away() {
    let env = SimEnv::new();
    let db = open(&env, Wal::Synced).await;
    write(&db).await;
    env.sim().crash();
    drop(db);

    let db = open(&env, Wal::Synced).await;
    assert_eq!(
        keys(&db).await,
        written(),
        "every write was synced before it returned"
    );
}

#[compio::test]
async fn a_buffered_write_survives_the_process_but_not_the_machine() {
    // The process going away: what was appended is still in the log.
    let env = SimEnv::new();
    let db = open(&env, Wal::Buffered).await;
    write(&db).await;
    db.close().await.expect("close");
    let db = open(&env, Wal::Buffered).await;
    assert_eq!(
        keys(&db).await,
        written(),
        "appended to the log, which a reopen replays"
    );
    db.close().await.expect("close");

    // The machine going away: the log was never synced, so nothing in it is
    // left — which is the promise, not a loss the store should have avoided.
    let env = SimEnv::new();
    let db = open(&env, Wal::Buffered).await;
    write(&db).await;
    env.sim().crash();
    drop(db);
    let db = open(&env, Wal::Buffered).await;
    assert!(keys(&db).await.is_empty(), "nothing was synced");
}

#[compio::test]
async fn with_the_wal_off_a_write_is_durable_once_flushed() {
    // Written to the memtable and nowhere else: a crash takes the store back
    // to its last flush, which here is none.
    let env = SimEnv::new();
    let db = open(&env, Wal::Off).await;
    write(&db).await;
    env.sim().crash();
    drop(db);
    let db = open(&env, Wal::Off).await;
    assert!(keys(&db).await.is_empty(), "no log and no flush");
    db.close().await.expect("close");

    // A flush is the durability point.
    let env = SimEnv::new();
    let db = open(&env, Wal::Off).await;
    write(&db).await;
    db.flush().await.expect("flush");
    env.sim().crash();
    drop(db);
    let db = open(&env, Wal::Off).await;
    assert_eq!(
        keys(&db).await,
        written(),
        "the flush wrote them to a table"
    );
    assert!(
        db.get(b"/b", Comparison::Equal)
            .await
            .expect("get")
            .is_some()
    );
}
