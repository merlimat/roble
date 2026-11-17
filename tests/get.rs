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

//! A point lookup over a range deletion: what it reads, and what it answers.
//!
//! A `get` of a key a range tombstone deletes has its answer as soon as it
//! meets the tombstone, so it reads no data block in a table the tombstone
//! makes too old — wherever the tombstone is, a memtable, L0 or a level below.

use std::path::{Path, PathBuf};

use roble::Comparison;
use roble::sim::{SimEnv, SimFs};
use roble_format::batch::Batch;

/// Opens the store at `dir` with no automatic compactions, so a test decides
/// which level each table ends up in.
async fn open_manual(env: &SimEnv, dir: &Path) -> roble::Db<SimFs> {
    roble::open_with(
        env,
        dir,
        roble::Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open")
}

/// Writes `keys` with a value of `value`, flushes them and compacts the table
/// down to `level`.
async fn table_at(
    db: &roble::Db<SimFs>,
    keys: impl Iterator<Item = String>,
    value: &[u8],
    level: u32,
) {
    let mut batch = Batch::new();
    for key in keys {
        batch.put(key.as_bytes(), value).expect("put");
    }
    db.write_raw(&mut batch, true).await.expect("write");
    let mut tables = vec![db.flush().await.expect("flush").expect("a table")];
    for from in 0..level {
        tables = db.compact(from, &tables).await.expect("compact");
    }
}

/// Where a test puts the range tombstone.
#[derive(Clone, Copy, Debug)]
enum Tombstone {
    /// Left in the memtable.
    Memtable,
    /// Flushed into an L0 table of its own.
    L0,
    /// Flushed and compacted into L1.
    L1,
}

/// A `get` of a key a newer range tombstone deletes reads no data block of
/// the older table that holds the key, wherever the tombstone is.
///
/// 4,000 keys of 100 bytes are some 27 blocks of 16 KiB in L2, and the
/// tombstone covers all but the first and last hundred. A `get` of `k0200`
/// first opens every table the lookup will consult, so that what `k2000`'s
/// `get` is charged for is its own reads alone: before the tombstone was
/// checked first, that was `k2000`'s block, found and then thrown away. A
/// snapshot taken before the tombstone then reads the same key, which is the
/// control: it shows the block was not already in the cache, so a lookup that
/// had searched L2 would have had to read it.
#[compio::test]
async fn a_get_of_a_key_a_memtable_tombstone_deletes_reads_no_block() {
    get_of_a_deleted_key_reads_no_block(Tombstone::Memtable).await;
}

/// The same, with the tombstone in an L0 table.
#[compio::test]
async fn a_get_of_a_key_an_l0_tombstone_deletes_reads_no_block() {
    get_of_a_deleted_key_reads_no_block(Tombstone::L0).await;
}

/// The same, with the tombstone in L1, above the table it deletes in.
#[compio::test]
async fn a_get_of_a_key_an_l1_tombstone_deletes_reads_no_block() {
    get_of_a_deleted_key_reads_no_block(Tombstone::L1).await;
}

async fn get_of_a_deleted_key_reads_no_block(at: Tombstone) {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open_manual(&env, &dir).await;
    table_at(&db, (0..4000).map(|i| format!("k{i:04}")), &[b'v'; 100], 2).await;
    let before = db.snapshot().expect("snapshot");

    let mut batch = Batch::new();
    batch
        .delete_range(b"k0100", b"k3900")
        .expect("delete range");
    db.write_raw(&mut batch, true).await.expect("write");
    match at {
        Tombstone::Memtable => {}
        Tombstone::L0 => {
            db.flush().await.expect("flush").expect("a table");
        }
        Tombstone::L1 => {
            let table = db.flush().await.expect("flush").expect("a table");
            db.compact(0, &[table]).await.expect("compact into L1");
        }
    }
    {
        let versions = db.versions();
        let levels: Vec<usize> = (0..3).map(|l| versions.version.level(l).len()).collect();
        let want = match at {
            Tombstone::Memtable => [0, 0, 1],
            Tombstone::L0 => [1, 0, 1],
            Tombstone::L1 => [0, 1, 1],
        };
        assert_eq!(levels, want, "{at:?}: tables in L0, L1 and L2");
    }

    assert_eq!(
        db.get(b"k0200", Comparison::Equal).await.expect("get"),
        None,
        "{at:?}: k0200"
    );
    let fs: &SimFs = env.sim();
    let start = fs.bytes_read();
    let found = db.get(b"k2000", Comparison::Equal).await.expect("get");
    let read = fs.bytes_read() - start;
    assert_eq!(found, None, "{at:?}: k2000");
    assert_eq!(
        read, 0,
        "{at:?}: a get of a key the tombstone deletes read {read} bytes"
    );

    let start = fs.bytes_read();
    let found = before.get(b"k2000", Comparison::Equal).await.expect("get");
    let read = fs.bytes_read() - start;
    assert_eq!(
        found.map(|f| f.value),
        Some(vec![b'v'; 100]),
        "{at:?}: k2000 at the snapshot"
    );
    assert!(
        read > 0,
        "{at:?}: the snapshot's get read nothing, so k2000's block was cached \
         and the get above proves nothing"
    );
}

/// What a lookup that stops at a tombstone must still find: a key written
/// into the deleted range after it — in the tombstone's own table, in a newer
/// table, in the memtable — and the key as a snapshot from before it saw it.
/// And what it must not: a key written before it in its own table, which that
/// table is searched for because of the rewrite beside it.
#[compio::test]
async fn a_get_over_a_range_deletion_finds_what_was_written_after_it() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open_manual(&env, &dir).await;
    table_at(&db, (0..100).map(|i| format!("k{i:03}")), b"old", 2).await;
    let before = db.snapshot().expect("snapshot");

    // One table holding a write before the tombstone, the tombstone, and a
    // write after it, so that its newest entry is newer than the tombstone.
    let mut batch = Batch::new();
    batch.put(b"k030", b"before").expect("put");
    batch.delete_range(b"k010", b"k090").expect("delete range");
    batch.put(b"k020", b"same table").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");
    let tombstone = db.flush().await.expect("flush").expect("a table");
    let mut batch = Batch::new();
    batch.put(b"k050", b"newer table").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");
    let mut batch = Batch::new();
    batch.put(b"k060", b"memtable").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");

    let want: [(&str, Option<&str>); 10] = [
        ("k005", Some("old")),
        ("k009", Some("old")),
        ("k010", None),
        ("k020", Some("same table")),
        ("k030", None),
        ("k040", None),
        ("k050", Some("newer table")),
        ("k060", Some("memtable")),
        ("k089", None),
        ("k090", Some("old")),
    ];
    // With the tombstone's table in L0, and then in L1 under the newer one.
    for stage in ["tombstone in L0", "tombstone in L1"] {
        if stage == "tombstone in L1" {
            db.compact(0, &[tombstone]).await.expect("compact into L1");
        }
        for (key, value) in want {
            let found = db
                .get(key.as_bytes(), Comparison::Equal)
                .await
                .expect("get");
            assert_eq!(
                found.map(|f| String::from_utf8(f.value).expect("utf8")),
                value.map(str::to_string),
                "{stage}: {key}"
            );
            let found = before
                .get(key.as_bytes(), Comparison::Equal)
                .await
                .expect("get");
            assert_eq!(
                found.map(|f| f.value),
                Some(b"old".to_vec()),
                "{stage}: {key} at the snapshot"
            );
        }
    }
}
