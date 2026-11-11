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

//! What the table cache is supposed to buy.
//!
//! `tests/read.rs` already shows that reads give the same answers as Pebble's.
//! These check the things an answer cannot show: that a read fetches the
//! blocks it needs instead of whole files, that a table is opened once, and
//! that two stores sharing a cache cannot read each other's tables.
//!
//! The simulated filesystem is what makes the first measurable — it counts
//! the bytes it hands back, which a real file system will not tell us.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Caches, Comparison, Disk, Fs, WriteBuf};

/// Loads a committed fixture directory into a simulated filesystem.
async fn load(name: &str) -> (SimEnv, PathBuf, u64) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let fs: &SimFs = env.sim();
    fs.mkdir_all(&dir).await.expect("mkdir");

    let mut total = 0u64;
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        let bytes = std::fs::read(entry.path()).expect("read file");
        total += bytes.len() as u64;
        let disk = fs
            .create(&dir.join(entry.file_name()))
            .await
            .expect("create");
        let (result, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
        result.expect("write");
        disk.close().await.expect("close");
    }
    (env, dir, total)
}

#[compio::test]
async fn a_read_fetches_blocks_rather_than_whole_tables() {
    let (env, dir, total) = load("db-reads").await;
    let before = env.sim().bytes_read();

    let db = roble::open(&env, &dir).await.expect("open");
    db.get(b"a0", Comparison::Equal).await.expect("get");

    let read = env.sim().bytes_read() - before;
    assert!(read > 0, "a read should touch the disk at all");
    assert!(
        read < total / 2,
        "reading one key took {read} of {total} bytes in the directory, which is the \
         whole-file read this cache exists to stop"
    );
}

#[compio::test]
async fn a_table_is_opened_once_and_then_held() {
    let (env, dir, _) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    db.get(b"a0", Comparison::Equal).await.expect("get");
    let opened = db.caches_for_test().open_tables();
    assert!(opened > 0, "the read should have opened a table");
    let after_first = env.sim().bytes_read();

    // The same read again: the structures are held, so only data blocks move.
    db.get(b"a0", Comparison::Equal).await.expect("get");
    let second = env.sim().bytes_read() - after_first;

    assert_eq!(
        db.caches_for_test().open_tables(),
        opened,
        "the second read should reuse the open tables, not add more"
    );
    assert!(
        second < after_first,
        "the second read moved {second} bytes against the first's {after_first}; the \
         footer, index and filter should not have been read again"
    );
}

#[compio::test]
async fn a_cache_bounded_to_one_table_still_answers() {
    // Eviction has to be survivable, not just correct: with room for a single
    // reader every table but one is evicted between reads, and the answers
    // must not change.
    let (env, dir, _) = load("db-reads").await;
    let caches = Caches::new(1);
    let db = roble::open_with(&env, &dir, caches.clone())
        .await
        .expect("open");

    let roomy = {
        let (env2, dir2, _) = load("db-reads").await;
        let db2 = roble::open(&env2, &dir2).await.expect("open");
        let mut out = Vec::new();
        for probe in [b"a0".as_slice(), b"c2", b"f3", b"u2", b"s1"] {
            out.push(db2.get(probe, Comparison::Floor).await.expect("get"));
        }
        out
    };

    let mut cramped = Vec::new();
    for probe in [b"a0".as_slice(), b"c2", b"f3", b"u2", b"s1"] {
        cramped.push(db.get(probe, Comparison::Floor).await.expect("get"));
    }

    assert_eq!(cramped, roomy, "eviction changed an answer");
    assert!(
        caches.open_tables() <= 1,
        "the cache held {} tables against a budget of one",
        caches.open_tables()
    );
}

#[compio::test]
async fn two_stores_sharing_a_cache_do_not_read_each_others_tables() {
    // File numbers are per database. `db-twin-a` and `db-twin-b` are built by
    // exactly the same sequence and so both hold a table `000004.sst`, with
    // different keys inside — which is what two shards on one thread look
    // like. A cache keyed by file number alone hands one store the other's
    // table: a wrong answer, not an error.
    //
    // An earlier version of this test used db-reads and db-simple, whose file
    // numbers happen never to overlap, so it passed whether or not the store
    // was part of the key. The generator now asserts the twins collide.
    let (env_a, dir_a, _) = load("db-twin-a").await;
    let (env_b, dir_b, _) = load("db-twin-b").await;
    let shared: Caches<SimFs> = Caches::new(64);

    let a = roble::open_with(&env_a, &dir_a, shared.clone())
        .await
        .expect("open a");
    let b = roble::open_with(&env_b, &dir_b, shared.clone())
        .await
        .expect("open b");

    // Whichever store reads first puts table 000004 in the cache; the other
    // must not find it there.
    for (name, db, mine, theirs) in [
        ("a", &a, b"a0".as_slice(), b"b0".as_slice()),
        ("b", &b, b"b0".as_slice(), b"a0".as_slice()),
    ] {
        assert!(
            db.get(mine, Comparison::Equal)
                .await
                .expect("get")
                .is_some(),
            "store {name} should hold its own key"
        );
        assert!(
            db.get(theirs, Comparison::Equal)
                .await
                .expect("get")
                .is_none(),
            "store {name} must not read the other store's table through a shared file number"
        );
    }

    assert_eq!(
        shared.open_tables(),
        2,
        "both tables should be held separately in the one cache"
    );
}
