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

//! What the public types print with `{:?}`.
//!
//! Every one of them can be printed, so a caller's own `#[derive(Debug)]`
//! over a struct holding one compiles (the `missing_debug_implementations`
//! lint keeps it so). What they print is the part worth pinning: what
//! identifies the value, and not the bytes or the internals behind it.

use std::path::PathBuf;

use roble::sim::{SimEnv, SimFs};
use roble::{Caches, Comparison, Options};

async fn open(env: &SimEnv) -> roble::Db<SimFs> {
    roble::open_with(
        env,
        PathBuf::from("/db"),
        Options::new()
            .create_if_missing(true)
            .memtable_size(1 << 20),
    )
    .await
    .expect("open")
}

#[compio::test]
async fn options_print_every_setting() {
    let printed = format!(
        "{:?}",
        Options::<SimFs>::new()
            .wal(roble::Wal::Off)
            .memtable_size(4096)
    );
    for want in [
        "Options",
        "caches: Caches",
        "wal: Off",
        "memtable_size: 4096",
        "target_file_size",
    ] {
        assert!(printed.contains(want), "{want:?} missing from {printed}");
    }
}

#[compio::test]
async fn caches_print_what_they_hold_against_their_budgets() {
    let printed = format!("{:?}", Caches::<SimFs>::new(7, 1 << 20));
    for want in [
        "tables_held: 0",
        "open_tables: 7",
        "block_bytes_held: 0",
        "block_bytes: 1048576",
    ] {
        assert!(printed.contains(want), "{want:?} missing from {printed}");
    }

    // The per-store view the engine hands its tables, and the clock behind it.
    let caches = Caches::<SimFs>::new(7, 1 << 20);
    let printed = format!("{:?}", roble::__private::block_cache(&caches, 3));
    assert!(
        printed.contains("store: 3") && printed.contains("budget: 1048576"),
        "{printed}"
    );
}

#[compio::test]
async fn a_scan_prints_its_bounds_and_where_it_is() {
    let env = SimEnv::new();
    let db = open(&env).await;
    db.put(b"b", b"1").await.expect("put");

    let mut scan = db.range(&b"a"[..]..&b"c"[..]).expect("range");
    let before = format!("{scan:?}");
    assert!(before.contains("key: None"), "not positioned yet: {before}");
    assert!(before.contains("lower: Some([97])"), "{before}");
    assert!(before.contains("upper: Some([99])"), "{before}");

    scan.first().await.expect("first");
    let after = format!("{scan:?}");
    assert!(after.contains("key: Some([98])"), "on b: {after}");
    db.close().await.expect("close");
}

#[compio::test]
async fn a_snapshot_and_a_batch_print_how_much_they_hold() {
    let env = SimEnv::new();
    let db = open(&env).await;
    db.put(b"a", b"1").await.expect("put");

    let snap = db.snapshot().expect("snapshot");
    let printed = format!("{snap:?}");
    assert!(
        printed.contains("Snapshot") && printed.contains("memtables: 1"),
        "{printed}"
    );
    assert!(
        snap.get(b"a", Comparison::Equal)
            .await
            .expect("get")
            .is_some()
    );
    drop(snap);

    let mut batch = db.indexed_batch();
    batch.put(b"x", b"y").expect("put");
    batch.delete(b"z").expect("delete");
    let printed = format!("{batch:?}");
    assert!(printed.contains("count: 2"), "{printed}");
    assert!(
        !printed.contains("[120]"),
        "no entries, only counts: {printed}"
    );
    drop(batch);
    db.close().await.expect("close");
}

#[compio::test]
async fn a_memtable_prints_its_size_and_not_its_entries() {
    let mut memtable = roble::__private::Memtable::new(1);
    memtable
        .insert(
            &roble_format::key::encode(b"k", 1, roble_format::key::Kind::Set),
            b"v",
        )
        .expect("insert");
    let printed = format!("{memtable:?}");
    assert!(printed.contains("len: 1"), "{printed}");
    assert!(printed.contains("range_deletions: 0"), "{printed}");
}

#[compio::test]
async fn a_get_buffer_says_whether_it_holds_a_parked_scan() {
    let env = SimEnv::new();
    let db = open(&env).await;
    db.put(b"a", b"1").await.expect("put");

    let mut buf = roble::GetBuf::new();
    assert!(format!("{buf:?}").contains("KeptScan(none)"), "{buf:?}");
    // An ordered comparison is answered through a scan, which the buffer
    // keeps, parked, for the next one.
    assert!(
        db.get_into(b"b", Comparison::Floor, &mut buf)
            .await
            .expect("floor")
    );
    assert!(format!("{buf:?}").contains("KeptScan(parked)"), "{buf:?}");
    db.close().await.expect("close");
}
