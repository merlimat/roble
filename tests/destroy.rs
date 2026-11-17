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

//! Removing a database.
//!
//! The thing worth testing is not that files go — one `remove` per name does
//! that — but what is left afterwards: nothing that a later open would try to
//! replay, no reader still holding bytes the filesystem could reclaim, and no
//! table written by a flush nobody asked for.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Caches, Disk, Fs, Options, WriteBuf};
use roble::{DEFAULT_BLOCK_BYTES, DEFAULT_OPEN_TABLES};
use roble_format::batch::Batch;

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

/// What the directory holds, sorted.
async fn listing(fs: &SimFs, dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs
        .list(dir)
        .await
        .expect("list")
        .iter()
        .map(|e| e.name.to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[compio::test]
async fn destroy_removes_every_file_the_database_had() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let before = listing(env.sim(), &dir).await;
    assert!(
        before.iter().any(|n| n.starts_with("LOCK")),
        "the open store should have taken a lock file: {before:?}"
    );

    let mut removed = db.destroy().await.expect("destroy");
    removed.sort();
    assert_eq!(
        removed, before,
        "destroy should name every file that was there, LOCK included"
    );
    assert!(
        listing(env.sim(), &dir).await.is_empty(),
        "and leave none of them"
    );
}

#[compio::test]
async fn destroy_does_not_flush_what_was_still_in_the_memtable() {
    // Oxia's `Delete` goes through `Close`, which flushes; a table written
    // and then removed in the same call is work for nobody.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut batch = Batch::new();
    batch.put(b"/unflushed", b"v").expect("put");
    db.write(&mut batch, true).await.expect("write");

    let before = listing(env.sim(), &dir).await;
    let mut removed = db.destroy().await.expect("destroy");
    removed.sort();
    assert_eq!(
        removed, before,
        "nothing should have been written between the listing and the removal"
    );
}

#[compio::test]
async fn a_destroyed_directory_is_not_a_database_any_more() {
    // What a destroy has to leave behind is a directory indistinguishable
    // from one that never held a database: the lock released, and nothing a
    // replay could find. roble cannot create a database from nothing — an
    // `open` reads `marker.format-version.*` and refuses without it (§6) —
    // so the statement is that both directories are refused in the same
    // words, rather than that the empty one opens.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    db.destroy().await.expect("destroy");

    let destroyed = roble::open(&env, &dir)
        .await
        .expect_err("a destroyed directory is not a database");

    let empty = PathBuf::from("/never-used");
    env.sim().mkdir_all(&empty).await.expect("mkdir");
    let fresh = roble::open(&env, &empty)
        .await
        .expect_err("nor is an empty one");

    assert_eq!(
        destroyed.to_string(),
        fresh.to_string(),
        "a destroyed database should be refused exactly as an empty directory is"
    );
}

#[compio::test]
async fn destroy_lets_go_of_the_table_readers() {
    // A reader is an open file, and no filesystem reclaims the bytes of one.
    // A destroy that left the table cache holding readers would free the
    // names and not the space.
    let (env, dir) = load("db-reads").await;
    let caches: Caches<SimFs> = Caches::new(DEFAULT_OPEN_TABLES, DEFAULT_BLOCK_BYTES);
    let db = roble::open_with(&env, &dir, Options::new().caches(caches.clone()))
        .await
        .expect("open");

    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    drop(scan);
    assert!(
        caches.tables_held() > 0,
        "reading should have left tables in the cache"
    );

    db.destroy().await.expect("destroy");
    assert_eq!(
        caches.tables_held(),
        0,
        "and a destroy should have let go of every one of them"
    );
}

#[compio::test]
async fn destroy_leaves_a_subdirectory_alone() {
    // On a real filesystem, which is the only one that reports directories at
    // all: `SimFs::list` returns files and nothing else, so this is the one
    // place the check can be made. The fixture is copied in with `std::fs`
    // because an `open` cannot make a database, only find one.
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db");
    std::fs::create_dir(&dir).expect("mkdir");
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-reads");
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), dir.join(entry.file_name())).expect("copy");
    }
    let db = roble::open(&roble::DefaultEnv, &dir).await.expect("open");

    let kept = dir.join("not-a-database-file");
    std::fs::create_dir(&kept).expect("mkdir");
    std::fs::write(kept.join("inside"), b"x").expect("write");

    let removed = db.destroy().await.expect("destroy");
    assert!(
        !removed.iter().any(|n| n == "not-a-database-file"),
        "a directory is not a file this call removes: {removed:?}"
    );
    assert!(kept.join("inside").exists(), "and it should still be there");

    let left: Vec<String> = std::fs::read_dir(&dir)
        .expect("read dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        left,
        vec!["not-a-database-file".to_string()],
        "everything else should be gone, and the directory itself should stay"
    );
}
