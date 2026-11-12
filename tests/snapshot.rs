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

//! A view of the store at one instant, scanned and looked up.
//!
//! Two things are being tested, and they pull in opposite directions. A
//! snapshot has to show what it saw and nothing later, which is a sequence
//! number. And it has to still be able to *read* what it saw, which is every
//! file of the version kept alive — the thing a bounded scan deliberately
//! does not do (§5, `tests/obsolete.rs`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;
use roble_format::files;

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

/// A store whose flushes do not go on to compact, so a version stays put
/// while a test reads it.
async fn open_unscheduled(env: &SimEnv, dir: &Path) -> roble::Opened<SimFs> {
    roble::open_with(env, dir, Options::new().disable_automatic_compactions(true))
        .await
        .expect("open")
}

/// Everything a scan walks, forwards.
async fn walk<D: Disk>(scan: &mut roble::Scan<D>) -> Model {
    scan.first().await.expect("first");
    let mut out = Model::new();
    while scan.is_valid() {
        out.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    out
}

async fn contents<F: Fs>(db: &roble::Opened<F>) -> Model {
    walk(&mut db.scan().await.expect("scan")).await
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

async fn write<F: Fs>(db: &roble::Opened<F>, build: impl FnOnce(&mut Batch)) {
    let mut batch = Batch::new();
    build(&mut batch);
    db.write(&mut batch, true).await.expect("write");
}

#[compio::test]
async fn a_snapshot_reads_what_it_saw_and_not_what_came_after() {
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    let before = contents(&db).await;
    let (key, old) = before
        .iter()
        .next()
        .map(|(k, v)| (k.clone(), v.clone()))
        .expect("the fixture should hold something");

    let snap = db.snapshot().await.expect("snapshot");

    write(&db, |b| {
        b.put(&key, b"written after the snapshot").expect("put");
        b.put(b"a-key-that-did-not-exist", b"new").expect("put");
    })
    .await;

    assert_eq!(
        snap.lookup(&key).await.expect("lookup"),
        Some(old),
        "the snapshot should hold the value it was made with"
    );
    assert_eq!(
        snap.lookup(b"a-key-that-did-not-exist")
            .await
            .expect("lookup"),
        None,
        "and should not see a key written after it"
    );
    assert_eq!(
        walk(&mut snap.scan().expect("scan")).await,
        before,
        "nor should a scan over it"
    );

    // While the store itself has moved on, which is what makes the above a
    // statement about the snapshot rather than about an idle database.
    assert_eq!(
        db.get(b"a-key-that-did-not-exist", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"new".to_vec()),
    );
}

#[compio::test]
async fn a_lookup_reaches_a_key_a_bounded_scan_cannot() {
    // The whole reason this type exists. Oxia's prefix iterator walks one
    // range of keys and looks up values that sort anywhere, so the handle has
    // to answer about a key its cursor could never step onto.
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    let all = contents(&db).await;
    let outside = all
        .keys()
        .find(|k| k.as_slice() >= b"g".as_slice())
        .cloned()
        .expect("the fixture should hold a key at or above \"g\"");

    let snap = db.snapshot().await.expect("snapshot");
    let seen = walk(&mut snap.scan_range(Some(b"a"), Some(b"g")).expect("scan")).await;

    assert!(
        !seen.contains_key(&outside),
        "the scan's bounds should have kept {outside:?} out of it"
    );
    assert_eq!(
        snap.lookup(&outside).await.expect("lookup").as_ref(),
        all.get(&outside),
        "and the lookup should find it anyway"
    );
}

#[compio::test]
async fn a_snapshot_holds_every_table_against_the_sweep() {
    // The mirror of `a_table_a_scan_cannot_reach_is_collected_while_it_is_open`
    // in tests/obsolete.rs. A bounded scan lets the tables outside its bounds
    // go; a snapshot may not let any of them go, because a lookup can still
    // ask about them.
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    let beyond: Vec<u64> = {
        let versions = db.versions();
        versions
            .version
            .level(0)
            .iter()
            .filter(|t| t.smallest_user_key().expect("smallest") >= b"g".as_slice())
            .map(|t| t.file_num)
            .collect()
    };
    assert!(
        !beyond.is_empty(),
        "the fixture should arrive with L0 tables starting above \"g\""
    );

    let all = contents(&db).await;
    let high: Vec<Vec<u8>> = all
        .keys()
        .filter(|k| k.as_slice() >= b"g".as_slice())
        .cloned()
        .collect();
    assert!(!high.is_empty(), "and with keys in them");

    let snap = db.snapshot().await.expect("snapshot");
    db.compact(0, &beyond).await.expect("compact");

    let names = listing(env.sim(), &dir).await;
    for file_num in &beyond {
        let name = files::table_filename(*file_num);
        assert!(
            names.contains(&name),
            "{name} went into a compaction while a snapshot was open and was \
             collected: {names:?}"
        );
    }

    for key in &high {
        assert_eq!(
            snap.lookup(key).await.expect("lookup").as_ref(),
            all.get(key),
            "the snapshot should still read {key:?} out of the table the \
             compaction retired"
        );
    }
}

#[compio::test]
async fn a_snapshot_holds_the_tables_of_every_level() {
    // A snapshot that gathered only L0 would read a database whose keys had
    // been compacted downwards as an empty one, and the fixtures all arrive
    // with everything in L0, so the level has to be moved for this to be a
    // test at all.
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    let all = contents(&db).await;
    let in_l0: Vec<u64> = {
        let versions = db.versions();
        versions
            .version
            .level(0)
            .iter()
            .map(|t| t.file_num)
            .collect()
    };
    assert!(
        !in_l0.is_empty(),
        "the fixture should arrive with L0 tables"
    );
    db.compact(0, &in_l0).await.expect("compact");
    {
        let versions = db.versions();
        assert!(
            versions.version.level(0).is_empty(),
            "every L0 table went into the compaction"
        );
        assert!(
            !versions.version.level(1).is_empty(),
            "and L1 holds what came out"
        );
    }

    let snap = db.snapshot().await.expect("snapshot");
    assert_eq!(
        walk(&mut snap.scan().expect("scan")).await,
        all,
        "a scan over the snapshot should read the whole store"
    );
    for (key, value) in &all {
        assert_eq!(
            snap.lookup(key).await.expect("lookup").as_ref(),
            Some(value),
            "and a lookup should find {key:?}, which is in L1"
        );
    }
}

#[compio::test]
async fn a_lookup_steps_past_a_newer_version_in_the_memtable() {
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    write(&db, |b| b.put(b"/k", b"first").expect("put")).await;
    let snap = db.snapshot().await.expect("snapshot");
    write(&db, |b| b.put(b"/k", b"second").expect("put")).await;

    assert_eq!(
        snap.lookup(b"/k").await.expect("lookup"),
        Some(b"first".to_vec()),
        "the newer version is above the snapshot, so the one below it answers"
    );
}

#[compio::test]
async fn a_lookup_takes_the_newest_version_a_table_holds() {
    // The table's iterator rather than the memtable's cursor, over a table
    // holding two versions of one key. The snapshot cannot be *between* them
    // here, and no snapshot ever can be: every entry in a table it holds was
    // written before it was made. So this says the other half — that the
    // newest of the versions in a table is the one that answers.
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    write(&db, |b| b.put(b"/k", b"first").expect("put")).await;
    write(&db, |b| b.put(b"/k", b"second").expect("put")).await;
    db.flush().await.expect("flush");

    let snap = db.snapshot().await.expect("snapshot");
    assert_eq!(
        snap.lookup(b"/k").await.expect("lookup"),
        Some(b"second".to_vec()),
    );
}

#[compio::test]
async fn a_deletion_after_the_snapshot_does_not_hide_the_value() {
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    write(&db, |b| {
        b.put(b"/point", b"p").expect("put");
        b.put(b"/ranged", b"r").expect("put");
    })
    .await;
    let snap = db.snapshot().await.expect("snapshot");
    write(&db, |b| {
        b.delete(b"/point").expect("delete");
        b.delete_range(b"/ranged", b"/ranged0")
            .expect("delete range");
    })
    .await;

    assert_eq!(
        snap.lookup(b"/point").await.expect("lookup"),
        Some(b"p".to_vec()),
        "a point delete above the snapshot is not its business"
    );
    assert_eq!(
        snap.lookup(b"/ranged").await.expect("lookup"),
        Some(b"r".to_vec()),
        "nor is a range deletion above it"
    );
    assert_eq!(
        db.get(b"/point", Comparison::Equal).await.expect("get"),
        None,
        "while the store sees both deletions"
    );
    assert_eq!(
        db.get(b"/ranged", Comparison::Equal).await.expect("get"),
        None,
    );
}

#[compio::test]
async fn a_deletion_before_the_snapshot_hides_the_value() {
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    write(&db, |b| {
        b.put(b"/point", b"p").expect("put");
        b.put(b"/ranged", b"r").expect("put");
    })
    .await;
    write(&db, |b| {
        b.delete(b"/point").expect("delete");
        b.delete_range(b"/ranged", b"/ranged0")
            .expect("delete range");
    })
    .await;
    let snap = db.snapshot().await.expect("snapshot");

    assert_eq!(snap.lookup(b"/point").await.expect("lookup"), None);
    assert_eq!(snap.lookup(b"/ranged").await.expect("lookup"), None);
}

#[compio::test]
async fn a_tombstone_in_a_table_whose_filter_rejects_the_key_still_applies() {
    // The asymmetry a lookup has to get right. A bloom filter answers about
    // point keys, so a lookup consults it to skip tables outright — but a
    // range deletion is in a block of its own and no filter knows about it.
    // Here the tombstone is in a table whose filter has never heard of the
    // key it covers, and applying the filter to the tombstone search would
    // bring a deleted key back to life.
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    write(&db, |b| b.put(b"/k", b"v").expect("put")).await;
    db.flush().await.expect("flush");

    // A table with a filter that holds "/zzz" and not "/k", and a tombstone
    // over "/k".
    write(&db, |b| {
        b.put(b"/zzz", b"z").expect("put");
        b.delete_range(b"/k", b"/k0").expect("delete range");
    })
    .await;
    db.flush().await.expect("flush");

    let snap = db.snapshot().await.expect("snapshot");
    assert_eq!(
        snap.lookup(b"/k").await.expect("lookup"),
        None,
        "the tombstone is in a table the filter would have skipped"
    );
    assert_eq!(
        snap.lookup(b"/zzz").await.expect("lookup"),
        Some(b"z".to_vec()),
        "and the point key in that same table is still readable"
    );
}

#[compio::test]
async fn a_snapshots_scan_is_bounded_the_way_a_stores_is() {
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    let all = contents(&db).await;
    let snap = db.snapshot().await.expect("snapshot");

    for (lower, upper) in [
        (None, None),
        (Some(&b"c"[..]), None),
        (None, Some(&b"m"[..])),
        (Some(&b"c"[..]), Some(&b"m"[..])),
    ] {
        let expected: Model = all
            .iter()
            .filter(|(k, _)| lower.is_none_or(|l| k.as_slice() >= l))
            .filter(|(k, _)| upper.is_none_or(|u| k.as_slice() < u))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(
            walk(&mut snap.scan_range(lower, upper).expect("scan")).await,
            expected,
            "bounds {lower:?}..{upper:?}"
        );
    }
}
