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

//! Writes, and reading them back.
//!
//! The model is a `BTreeMap` seeded from the database as Pebble left it — so
//! the starting state is still Pebble's, and only what roble writes on top of
//! it is roble's own. Put, delete and delete-range have exact `BTreeMap`
//! equivalents, which is what makes the comparison worth anything.
//!
//! Nothing here is durable yet: these writes live in the memtable until a
//! flush exists. What is tested is that they are *readable*, and that a scan
//! already open does not see them.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

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
        disk.close().await.expect("close");
    }
    (env, dir)
}

/// Everything the database shows, in order.
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

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[compio::test]
async fn what_is_written_is_what_is_read() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // The model starts as the database Pebble left, read through roble's own
    // scan — which `tests/scan.rs` has already pinned against Pebble's.
    let mut model = contents(&db).await;
    assert!(!model.is_empty(), "the fixture should not be empty");

    let users: Vec<Vec<u8>> = (0..10u8)
        .map(|i| format!("k{i}").into_bytes())
        .chain(model.keys().take(8).cloned())
        .collect();

    let mut rng = Rng(20_261_111);
    for round in 0..25 {
        let mut batch = Batch::new();
        for i in 0..6 {
            let k = users[rng.below(users.len() as u64) as usize].clone();
            match rng.below(6) {
                0 => {
                    batch.delete(&k).expect("delete");
                    model.remove(&k);
                }
                1 => {
                    let mut a = users[rng.below(users.len() as u64) as usize].clone();
                    let mut b = users[rng.below(users.len() as u64) as usize].clone();
                    if a > b {
                        std::mem::swap(&mut a, &mut b);
                    }
                    if a != b {
                        batch.delete_range(&a, &b).expect("delete_range");
                        model.retain(|key, _| !(key >= &a && key < &b));
                    }
                }
                _ => {
                    let v = format!("r{round}-{i}").into_bytes();
                    batch.put(&k, &v).expect("put");
                    model.insert(k, v);
                }
            }
        }
        db.write(&mut batch).expect("write");

        assert_eq!(
            contents(&db).await,
            model,
            "round {round}: the scan and the model disagree"
        );
    }

    // And every key the model holds is found by a point lookup too, which
    // takes a different path: `Equal` keeps its own, for the bloom filter.
    for (k, v) in model.iter().take(40) {
        let found = db.get(k, Comparison::Equal).await.expect("get");
        assert_eq!(
            found.map(|f| f.value),
            Some(v.clone()),
            "get disagrees with the scan about {k:02x?}"
        );
    }
}

#[compio::test]
async fn a_write_shadows_what_a_table_holds() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // A key the fixture holds.
    let existing = contents(&db).await;
    let (key, old) = existing.iter().next().expect("a key");
    let (key, old) = (key.clone(), old.clone());

    let mut batch = Batch::new();
    batch.put(&key, b"written-later").expect("put");
    db.write(&mut batch).expect("write");

    let found = db.get(&key, Comparison::Equal).await.expect("get");
    assert_eq!(
        found.map(|f| f.value),
        Some(b"written-later".to_vec()),
        "the memtable's version should win over the table's {old:02x?}"
    );

    // And a delete hides it entirely.
    let mut batch = Batch::new();
    batch.delete(&key).expect("delete");
    db.write(&mut batch).expect("write");
    assert!(
        db.get(&key, Comparison::Equal)
            .await
            .expect("get")
            .is_none(),
        "a delete in the memtable should hide the table's value"
    );
    assert!(
        !contents(&db).await.contains_key(&key),
        "and the scan should not show it either"
    );
}

#[compio::test]
async fn a_range_deletion_in_the_memtable_hides_a_table() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let before = contents(&db).await;

    // A span that covers at least a couple of the fixture's keys.
    let keys: Vec<Vec<u8>> = before.keys().cloned().collect();
    let (start, end) = (keys[1].clone(), keys[4].clone());
    let covered: Vec<Vec<u8>> = keys
        .iter()
        .filter(|k| **k >= start && **k < end)
        .cloned()
        .collect();
    assert!(covered.len() >= 2, "the span should cover something");

    let mut batch = Batch::new();
    batch.delete_range(&start, &end).expect("delete_range");
    db.write(&mut batch).expect("write");

    let after = contents(&db).await;
    for k in &covered {
        assert!(!after.contains_key(k), "{k:02x?} should be covered");
        assert!(
            db.get(k, Comparison::Equal).await.expect("get").is_none(),
            "get should agree about {k:02x?}"
        );
    }
    assert!(
        after.contains_key(&end),
        "the end of the range is excluded, so it survives"
    );
}

#[compio::test]
async fn a_scan_does_not_see_a_write_that_came_after_it() {
    // §4: iterators are over a snapshot — a sequence number taken when the
    // scan is made, above which nothing is its business.
    //
    // Reaching that rule at all takes some care, and two earlier versions of
    // this test did not. The writes have to land **ahead of where the scan has
    // reached**, or the cursor's position hides them by itself; and the
    // memtable has to be **non-empty when the scan starts**, or its cursor is
    // parked invalid and never looks again. Both versions passed with the
    // snapshot check deleted.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // Something in the memtable before the scan begins.
    let mut batch = Batch::new();
    batch.put(b"m1", b"first").expect("put");
    batch.put(b"m3", b"first").expect("put");
    db.write(&mut batch).expect("write");

    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    let started_on = scan.key().to_vec();
    assert!(
        started_on.as_slice() < b"m1".as_slice(),
        "the scan must start before the keys it will be shown later"
    );

    // Now write between them — ahead of the cursor, into a memtable the scan
    // is already walking.
    let mut batch = Batch::new();
    batch.put(b"m2", b"later").expect("put");
    batch.put(b"m3", b"changed").expect("put");
    db.write(&mut batch).expect("write");

    let mut seen = Model::new();
    while scan.is_valid() {
        seen.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }

    assert_eq!(
        seen.get(b"m1".as_slice()).map(|v| v.as_slice()),
        Some(b"first".as_slice()),
        "what was there when the scan began is what it shows"
    );
    assert!(
        !seen.contains_key(b"m2".as_slice()),
        "a key written after the scan began must not appear in it"
    );
    assert_eq!(
        seen.get(b"m3".as_slice()).map(|v| v.as_slice()),
        Some(b"first".as_slice()),
        "and a key rewritten after it began must read as it was"
    );

    // A scan made now sees all of it.
    let fresh = contents(&db).await;
    assert_eq!(
        fresh.get(b"m2".as_slice()).map(|v| v.as_slice()),
        Some(b"later".as_slice())
    );
    assert_eq!(
        fresh.get(b"m3".as_slice()).map(|v| v.as_slice()),
        Some(b"changed".as_slice())
    );
}

#[compio::test]
async fn sequence_numbers_carry_on_from_the_manifest() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let last = db.versions.last_seq_num;

    let mut batch = Batch::new();
    batch.put(b"one", b"1").expect("put");
    batch.put(b"two", b"2").expect("put");
    db.write(&mut batch).expect("write");

    assert_eq!(
        batch.seq_num(),
        last + 1,
        "a batch commits at the sequence number after the MANIFEST's last"
    );
}
