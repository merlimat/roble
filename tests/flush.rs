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

//! Flush: the memtable becomes a table, and the MANIFEST says so.
//!
//! What a flush must not do is change any answer. Everything here reads the
//! database before and after and compares — the point of a flush is that it
//! moves bytes about and leaves the contents alone.

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

#[compio::test]
async fn a_flush_changes_no_answer() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut batch = Batch::new();
    batch.put(b"f1", b"one").expect("put");
    batch.put(b"f2", b"two").expect("put");
    batch.delete(b"a0").expect("delete");
    batch.delete_range(b"c0", b"c3").expect("delete_range");
    db.write(&mut batch, true).await.expect("write");

    let before = contents(&db).await;
    let levels_before = db.versions().version.table_count();

    let table = db
        .flush()
        .await
        .expect("flush")
        .expect("something to flush");

    assert_eq!(contents(&db).await, before, "a flush changed an answer");
    assert_eq!(
        db.versions().version.table_count(),
        levels_before + 1,
        "the version should hold one more table"
    );
    assert!(
        db.versions()
            .version
            .level(0)
            .iter()
            .any(|t| t.file_num == table),
        "and it should be in L0"
    );
    assert_eq!(
        db.unflushed_bytes(),
        0,
        "the memtable should have been emptied into it"
    );
}

#[compio::test]
async fn a_flush_survives_a_crash_without_the_log() {
    // The point of a flush: what it wrote is in a table, so the log it
    // replaces is no longer needed. The MANIFEST's min_unflushed moves past
    // the old log, so even with that log destroyed the data is there.
    let (env, dir) = load("db-reads").await;
    let old_log;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        // The log the store is *writing* to, which is not
        // `min_unflushed_log_num`: this fixture has that at zero, naming a
        // log that was never created. An earlier version of this test
        // truncated `000000.log` and so destroyed nothing at all.
        old_log = db.current_log().expect("a log to write to");

        let mut batch = Batch::new();
        batch.put(b"survives", b"yes").expect("put");
        db.write(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");

        assert!(
            db.versions().min_unflushed_log_num > old_log,
            "a flush should move past the log it replaced"
        );
        assert_ne!(db.current_log(), Some(old_log), "and write somewhere else");
    }

    // Destroy the log the flush retired. Nothing should miss it.
    let fs = env.sim();
    let path = dir.join(format!("{old_log:06}.log"));
    let disk = fs.create(&path).await.expect("truncate the old log");
    disk.sync(()).await.expect("sync");
    disk.close().await.expect("close");
    fs.sync_dir(&dir).await.expect("sync dir");
    env.sim().crash();

    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        db.get(b"survives", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"yes".to_vec()),
        "the write was flushed, so the log it came from no longer matters"
    );
}

#[compio::test]
async fn a_flushed_range_deletion_still_covers() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let keys: Vec<Vec<u8>> = contents(&db).await.into_keys().collect();
    let (start, end) = (keys[1].clone(), keys[4].clone());
    let covered: Vec<Vec<u8>> = keys
        .iter()
        .filter(|k| **k >= start && **k < end)
        .cloned()
        .collect();
    assert!(covered.len() >= 2);

    let mut batch = Batch::new();
    batch.delete_range(&start, &end).expect("delete_range");
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");

    let after = contents(&db).await;
    for k in &covered {
        assert!(
            !after.contains_key(k),
            "{k:02x?} should still be covered after the flush"
        );
        // `get` is the one that notices a wrong bound: it skips a table whose
        // key range excludes the key, where a scan consults every table's
        // tombstones. A flushed table holding only a range deletion gets its
        // upper bound from the sentinel, and without it the table would
        // claim to end at the tombstone's start — so every key past that
        // would be served as though the tombstone were not there.
        assert!(
            db.get(k, Comparison::Equal).await.expect("get").is_none(),
            "get should agree that {k:02x?} is covered"
        );
    }
    assert!(after.contains_key(&end), "and the end is still excluded");
    assert!(
        db.get(&end, Comparison::Equal)
            .await
            .expect("get")
            .is_some(),
        "get should agree that the end is excluded"
    );
}

#[compio::test]
async fn flushing_nothing_writes_nothing() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let before = db.versions().next_file_num;

    assert!(
        db.flush().await.expect("flush").is_none(),
        "an empty memtable is not a table"
    );
    assert_eq!(
        db.versions().next_file_num,
        before,
        "and it should not have spent a file number"
    );
}

#[compio::test]
async fn a_scan_open_across_a_flush_keeps_its_snapshot() {
    // A flush replaces the memtable. A scan that was already reading the old
    // one must keep reading it, or its snapshot would empty out underneath
    // it mid-walk.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut batch = Batch::new();
    batch.put(b"m1", b"before").expect("put");
    batch.put(b"m9", b"before").expect("put");
    db.write(&mut batch, true).await.expect("write");

    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");

    db.flush().await.expect("flush").expect("a table");

    let mut seen = Model::new();
    while scan.is_valid() {
        seen.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    assert_eq!(
        seen.get(b"m9".as_slice()).map(|v| v.as_slice()),
        Some(b"before".as_slice()),
        "the scan should still find what it was reading when the flush happened"
    );
}

#[compio::test]
async fn a_manifest_that_outgrows_a_block_still_reads() {
    // A MANIFEST is framed into 32 KiB blocks like a log, so an append has to
    // know how far into the file it is. Until this test every MANIFEST under
    // test was a kilobyte or so, and a writer that framed against its own
    // buffer gave identical bytes.
    let (env, dir) = load("db-reads").await;
    let flushes;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        let path = dir.join(db.manifest_filename());
        let mut rounds = 0;
        loop {
            let mut batch = Batch::new();
            batch
                .put(format!("many-{rounds:04}").as_bytes(), b"v")
                .expect("put");
            db.write(&mut batch, true).await.expect("write");
            db.flush().await.expect("flush").expect("a table");
            rounds += 1;

            let disk = fs_open(&env, &path).await;
            let len = disk.len().await.expect("len");
            if len > 40 * 1024 {
                break;
            }
            assert!(rounds < 2000, "the MANIFEST is not growing");
        }
        flushes = rounds;
    }

    env.sim().crash();
    let db = roble::open(&env, &dir)
        .await
        .expect("reopen a long MANIFEST");
    for i in 0..flushes {
        assert_eq!(
            db.get(format!("many-{i:04}").as_bytes(), Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(b"v".to_vec()),
            "round {i} went missing after the MANIFEST crossed a block"
        );
    }
    assert!(
        db.versions().version.table_count() >= flushes,
        "every flush should have left a table in the version"
    );
}

async fn fs_open(env: &SimEnv, path: &Path) -> roble::sim::SimDisk {
    use roble::Env;
    env.fs()
        .open(path, roble::Access::ReadOnly)
        .await
        .expect("open the manifest")
}

#[compio::test]
async fn overlapping_range_deletions_can_be_flushed() {
    // Two `delete_range` calls that overlap at neither bound are ordinary
    // use, and the memtable keeps them as they were asked for. A
    // range-deletion block holds fragments, though, so emitting them straight
    // out of the memtable built a block the writer refuses:
    //
    //     overlapping tombstones are not fragmented: "/a".."/d" vs "/b".."/f"
    //
    // which is worse than a failed flush. The memtable is never emptied, so
    // every later flush fails the same way and the store can never shed it.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    for key in [&b"/a1"[..], b"/b1", b"/d1", b"/f1"] {
        let mut batch = Batch::new();
        batch.put(key, b"v").expect("put");
        db.write(&mut batch, true).await.expect("write");
    }

    // Separate batches, so both tombstones are live in the one memtable.
    let mut batch = Batch::new();
    batch.delete_range(b"/a", b"/d").expect("first range");
    db.write(&mut batch, true).await.expect("write");

    let mut batch = Batch::new();
    batch.delete_range(b"/b", b"/f").expect("overlapping range");
    db.write(&mut batch, true).await.expect("write");

    let before = contents(&db).await;
    assert!(
        !before.contains_key(b"/a1".as_slice())
            && !before.contains_key(b"/b1".as_slice())
            && !before.contains_key(b"/d1".as_slice()),
        "the ranges should have deleted what they cover: {before:?}"
    );
    assert!(
        before.contains_key(b"/f1".as_slice()),
        "and nothing past them: {before:?}"
    );

    db.flush().await.expect("flush").expect("a table");

    assert_eq!(contents(&db).await, before, "the flush changed an answer");
    assert_eq!(
        db.unflushed_bytes(),
        0,
        "the memtable should have been emptied into the table"
    );

    // And the table reads back the same on its own, from disk.
    drop(db);
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(contents(&db).await, before, "a reopen changed an answer");
}
