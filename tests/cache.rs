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
use roble::{Caches, Comparison, Disk, Fs, Options, WriteBuf};
use roble::{DEFAULT_BLOCK_BYTES, DEFAULT_OPEN_TABLES};

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
    let caches: Caches<SimFs> = Caches::default();
    let db = roble::open_with(&env, &dir, Options::new().caches(caches.clone()))
        .await
        .expect("open");

    db.get(b"a0", Comparison::Equal).await.expect("get");
    let opened = caches.tables_held();
    assert!(opened > 0, "the read should have opened a table");
    let after_first = env.sim().bytes_read();

    // The same read again: the structures are held, so only data blocks move.
    db.get(b"a0", Comparison::Equal).await.expect("get");
    let second = env.sim().bytes_read() - after_first;

    assert_eq!(
        caches.tables_held(),
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
    let caches = Caches::new(1, DEFAULT_BLOCK_BYTES);
    let db = roble::open_with(&env, &dir, Options::new().caches(caches.clone()))
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
        caches.tables_held() <= 1,
        "the cache held {} tables against a budget of one",
        caches.tables_held()
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
    let shared: Caches<SimFs> = Caches::new(64, DEFAULT_BLOCK_BYTES);

    let a = roble::open_with(&env_a, &dir_a, Options::new().caches(shared.clone()))
        .await
        .expect("open a");
    let b = roble::open_with(&env_b, &dir_b, Options::new().caches(shared.clone()))
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
        shared.tables_held(),
        2,
        "both tables should be held separately in the one cache"
    );
}

#[compio::test]
async fn a_repeated_read_touches_no_disk_at_all() {
    // With the table open and its blocks held, the second identical read has
    // nothing left to fetch. Zero rather than "less" is the right assertion:
    // anything above it means something is being re-read that was already
    // held.
    let (env, dir, _) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let first = db.get(b"a0", Comparison::Equal).await.expect("get");
    let after_first = env.sim().bytes_read();

    let second = db.get(b"a0", Comparison::Equal).await.expect("get");
    assert_eq!(
        env.sim().bytes_read(),
        after_first,
        "the second read of the same key went to the disk again"
    );
    assert_eq!(first, second, "and it had better be the same answer");
}

#[compio::test]
async fn the_block_budget_is_kept_and_the_answers_do_not_change() {
    // A budget small enough that blocks are evicted between reads. Eviction
    // has to be survivable, not merely correct.
    let roomy = {
        let (env, dir, _) = load("db-reads").await;
        let db = roble::open(&env, &dir).await.expect("open");
        let mut out = Vec::new();
        for probe in [b"a0".as_slice(), b"c2", b"f3", b"u2", b"w0"] {
            out.push(db.get(probe, Comparison::Ceiling).await.expect("get"));
        }
        out
    };

    let (env, dir, _) = load("db-reads").await;
    let caches: Caches<SimFs> = Caches::new(DEFAULT_OPEN_TABLES, 4096);
    let db = roble::open_with(&env, &dir, Options::new().caches(caches.clone()))
        .await
        .expect("open");

    let mut cramped = Vec::new();
    for probe in [b"a0".as_slice(), b"c2", b"f3", b"u2", b"w0"] {
        cramped.push(db.get(probe, Comparison::Ceiling).await.expect("get"));
        assert!(
            caches.block_bytes_held() <= 4096,
            "the cache held {} bytes against a budget of 4096",
            caches.block_bytes_held()
        );
    }
    assert_eq!(cramped, roomy, "eviction changed an answer");
}

#[compio::test]
async fn a_block_is_cached_decoded_rather_than_as_it_was_read() {
    // §8: a compressed block is decompressed once and kept that way, so what
    // is cached is no longer the bytes that came off the disk. The snappy
    // fixture is the one place that difference is visible — in an
    // uncompressed table the two are the same and the claim is untestable.
    let sst = Path::new(env!("CARGO_MANIFEST_DIR")).join("format/tests/golden/snappy.sst");
    let bytes = std::fs::read(&sst).expect("read the snappy fixture");

    let env = SimEnv::new();
    let fs: &SimFs = env.sim();
    fs.mkdir_all(Path::new("/t")).await.expect("mkdir");
    let path = PathBuf::from("/t/000001.sst");
    let disk = fs.create(&path).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
    result.expect("write");
    disk.close().await.expect("close");

    let caches: Caches<SimFs> = Caches::new(DEFAULT_OPEN_TABLES, DEFAULT_BLOCK_BYTES);
    let blocks = roble::__private::block_cache(&caches, 0);
    let disk = fs.open(&path, roble::Access::ReadOnly).await.expect("open");
    let table = roble::__private::OpenTable::open(1, disk, blocks.clone())
        .await
        .expect("open table");

    let before = fs.bytes_read();
    let target = roble_format::key::search_key(b"");
    table
        .seek_ge(&target)
        .await
        .expect("seek")
        .expect("an entry");
    let read = fs.bytes_read() - before;

    assert!(read > 0, "the seek should have fetched a block");
    assert!(
        blocks.bytes_held() as u64 > read,
        "the cache holds {} bytes for a block read as {read}; a compressed block kept \
         as it was read would hold no more than it cost to fetch",
        blocks.bytes_held()
    );
}

#[compio::test]
async fn two_stores_blocks_do_not_collide_either() {
    // The twins share file number 000004, and their blocks share offsets too.
    // Keying blocks by file number and offset alone would alias them.
    let (env_a, dir_a, _) = load("db-twin-a").await;
    let (env_b, dir_b, _) = load("db-twin-b").await;
    let shared: Caches<SimFs> = Caches::new(64, DEFAULT_BLOCK_BYTES);

    let a = roble::open_with(&env_a, &dir_a, Options::new().caches(shared.clone()))
        .await
        .expect("open a");
    let b = roble::open_with(&env_b, &dir_b, Options::new().caches(shared.clone()))
        .await
        .expect("open b");

    let from_a = a.get(b"a0", Comparison::Equal).await.expect("get");
    let from_b = b.get(b"b0", Comparison::Equal).await.expect("get");
    assert!(
        from_a.is_some() && from_b.is_some(),
        "each holds its own key"
    );
    assert_ne!(from_a, from_b);
    assert!(
        shared.blocks_held() >= 2,
        "the two stores' blocks should be held apart, not aliased: {} held",
        shared.blocks_held()
    );

    // And still right when read again, now from the cache.
    assert_eq!(a.get(b"a0", Comparison::Equal).await.expect("get"), from_a);
    assert_eq!(b.get(b"b0", Comparison::Equal).await.expect("get"), from_b);
    assert!(
        a.get(b"b0", Comparison::Equal)
            .await
            .expect("get")
            .is_none(),
        "store a must not find store b's key through a shared block"
    );
}

/// Forgetting an entry gives its room back.
///
/// `remove` has to return the cost along with the key. A cache told about
/// enough deletions would otherwise stop holding anything, its budget spent
/// on entries that are no longer there — and the obsolete sweep tells the
/// table cache about every table it collects.
#[test]
fn forgetting_an_entry_gives_its_room_back() {
    let mut clock: roble::__private::Clock<u64, u64> = roble::__private::Clock::new(2);
    clock.insert(1, 10, 1);
    clock.insert(2, 20, 1);

    clock.remove(&1);
    // Nothing, and not a panic: the sweep names files, not cache entries.
    clock.remove(&99);
    clock.insert(3, 30, 1);

    assert_eq!(clock.get(&1), None, "the forgotten entry is gone");
    assert_eq!(
        clock.get(&2),
        Some(20),
        "the one that was not forgotten stays"
    );
    assert_eq!(
        clock.get(&3),
        Some(30),
        "and the new one fits in the room the forgotten one gave back"
    );
}
