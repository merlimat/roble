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
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;
use roble_format::files;

mod common;
use common::land_during;

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
async fn open_unscheduled(env: &SimEnv, dir: &Path) -> roble::Db<SimFs> {
    roble::open_with(env, dir, Options::new().disable_automatic_compactions(true))
        .await
        .expect("open")
}

/// Everything a scan walks, forwards.
async fn walk<F: Fs>(scan: &mut roble::Scan<F>) -> Model {
    scan.first().await.expect("first");
    let mut out = Model::new();
    while scan.is_valid() {
        out.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    out
}

async fn contents<F: Fs>(db: &roble::Db<F>) -> Model {
    walk(&mut db.range(..).expect("scan")).await
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

async fn write<F: Fs>(db: &roble::Db<F>, build: impl FnOnce(&mut Batch)) {
    let mut batch = Batch::new();
    build(&mut batch);
    db.write_raw(&mut batch, true).await.expect("write");
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

    let snap = db.snapshot().expect("snapshot");

    write(&db, |b| {
        b.put(&key, b"written after the snapshot").expect("put");
        b.put(b"a-key-that-did-not-exist", b"new").expect("put");
    })
    .await;

    assert_eq!(
        snap.get(&key, roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
        Some(old),
        "the snapshot should hold the value it was made with"
    );
    assert_eq!(
        snap.get(b"a-key-that-did-not-exist", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
        None,
        "and should not see a key written after it"
    );
    assert_eq!(
        walk(&mut snap.range(..).expect("scan")).await,
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

/// Every comparison a snapshot's `get` takes answers what the store's
/// answered when the snapshot was made, however the store changes after.
///
/// The probes are each key, the least key after it, and keys below and above
/// everything, so every comparison is asked both where it lands on the probe
/// and where it has to step off it.
#[compio::test]
async fn a_snapshot_gets_under_every_comparison_what_the_store_had() {
    use roble::Comparison::{Ceiling, Equal, Floor, Higher, Lower};

    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;
    let keys: Vec<Vec<u8>> = contents(&db).await.into_keys().collect();
    assert!(keys.len() > 1, "the fixture should hold several keys");
    let mut probes = vec![Vec::new(), vec![0xff; 8]];
    for key in &keys {
        probes.push(key.clone());
        probes.push([key.as_slice(), &[0]].concat());
    }

    let mut expected = Vec::new();
    for probe in &probes {
        for how in [Equal, Floor, Ceiling, Lower, Higher] {
            let found = db.get(probe, how).await.expect("get");
            expected.push((probe.clone(), how, found));
        }
    }
    let snap = db.snapshot().expect("snapshot");

    write(&db, |b| {
        for key in &keys {
            b.put(&[key.as_slice(), &[0]].concat(), b"after")
                .expect("put");
        }
        b.delete(&keys[0]).expect("delete");
        b.put(&keys[1], b"overwritten").expect("put");
    })
    .await;

    for (probe, how, found) in expected {
        assert_eq!(
            snap.get(&probe, how).await.expect("get"),
            found,
            "{how:?} of {probe:?}"
        );
    }
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

    let snap = db.snapshot().expect("snapshot");
    let seen = walk(&mut snap.range(&b"a"[..]..&b"g"[..]).expect("scan")).await;

    assert!(
        !seen.contains_key(&outside),
        "the scan's bounds should have kept {outside:?} out of it"
    );
    assert_eq!(
        snap.get(&outside, roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value)
            .as_ref(),
        all.get(&outside),
        "and the lookup should find it anyway"
    );
}

#[compio::test]
async fn a_snapshot_holds_every_table_against_the_sweep() {
    // A snapshot opens nothing when it is made, so the tables a lookup asks
    // for after the compaction are ones nothing had open when the sweep ran:
    // only the version the snapshot holds keeps them (roble::obsolete).
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

    let snap = db.snapshot().expect("snapshot");
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
            snap.get(key, roble::Comparison::Equal)
                .await
                .expect("get")
                .map(|found| found.value)
                .as_ref(),
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

    let snap = db.snapshot().expect("snapshot");
    assert_eq!(
        walk(&mut snap.range(..).expect("scan")).await,
        all,
        "a scan over the snapshot should read the whole store"
    );
    for (key, value) in &all {
        assert_eq!(
            snap.get(key, roble::Comparison::Equal)
                .await
                .expect("get")
                .map(|found| found.value)
                .as_ref(),
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
    let snap = db.snapshot().expect("snapshot");
    write(&db, |b| b.put(b"/k", b"second").expect("put")).await;

    assert_eq!(
        snap.get(b"/k", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
        Some(b"first".to_vec()),
        "the newer version is above the snapshot, so the one below it answers"
    );
}

#[compio::test]
async fn a_lookup_takes_the_newest_version_a_table_holds() {
    // The table's iterator rather than the memtable's cursor. A snapshot can
    // never be *between* two versions in a table it holds: every entry in
    // such a table was written before the snapshot was made. That is also
    // why a flush keeps only the newest version of each key, so the table
    // here holds the second write and not the first — and this says the
    // answer comes from it. A table holding several versions of a key is one
    // Pebble flushed with a snapshot of its own open; the golden
    // `valueblocks` table is one, and the format crate reads it.
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    write(&db, |b| b.put(b"/k", b"first").expect("put")).await;
    write(&db, |b| b.put(b"/k", b"second").expect("put")).await;
    db.flush().await.expect("flush");

    let snap = db.snapshot().expect("snapshot");
    assert_eq!(
        snap.get(b"/k", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
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
    let snap = db.snapshot().expect("snapshot");
    write(&db, |b| {
        b.delete(b"/point").expect("delete");
        b.delete_range(b"/ranged", b"/ranged0")
            .expect("delete range");
    })
    .await;

    assert_eq!(
        snap.get(b"/point", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
        Some(b"p".to_vec()),
        "a point delete above the snapshot is not its business"
    );
    assert_eq!(
        snap.get(b"/ranged", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
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
    let snap = db.snapshot().expect("snapshot");

    assert_eq!(
        snap.get(b"/point", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
        None
    );
    assert_eq!(
        snap.get(b"/ranged", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
        None
    );
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

    let snap = db.snapshot().expect("snapshot");
    assert_eq!(
        snap.get(b"/k", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
        None,
        "the tombstone is in a table the filter would have skipped"
    );
    assert_eq!(
        snap.get(b"/zzz", roble::Comparison::Equal)
            .await
            .expect("get")
            .map(|found| found.value),
        Some(b"z".to_vec()),
        "and the point key in that same table is still readable"
    );
}

#[compio::test]
async fn a_snapshots_scan_is_bounded_the_way_a_stores_is() {
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    let all = contents(&db).await;
    let snap = db.snapshot().expect("snapshot");

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
            walk(
                &mut snap
                    .range((
                        lower.map_or(Unbounded, Included),
                        upper.map_or(Unbounded, Excluded),
                    ))
                    .expect("scan")
            )
            .await,
            expected,
            "bounds {lower:?}..{upper:?}"
        );
    }
}

/// RocksDB's `TombstoneVisibleInSnapshot` (`db/db_flush_test.cc`, the race
/// its 7.1.0 fixed), swept over every operation of a flush and of a merge.
/// With the old value of a key on its way into a table, the key is deleted,
/// a snapshot is taken, and the key is written again. The snapshot has to go
/// on saying the key is not there: through the flush or merge that was under
/// way, through the flush after it, which writes out only the newer value,
/// and through compactions down to the last level, after which no table
/// holds the deletion at all.
///
/// A snapshot holds the memtables and the version as they were, and its
/// sequence number keeps the later put out. Taken while a flush is
/// suspended, the memtables are the frozen one as well as the one the
/// writes go to. One that held only the newer memtable would lose the frozen
/// one's keys until their table landed; one that read the store's tables at
/// its sequence number would find the old value again once the deletion had
/// been flushed away.
///
/// Swept rather than aimed, with the log and without it, as
/// `tests/freeze.rs` sweeps writes and reads in the middle of a flush: the
/// operation is suspended before its `n`th operation, the three writes land
/// there while it stays suspended (`land_during`), and it then runs to its
/// end.
#[compio::test]
async fn a_snapshot_taken_while_a_flush_or_a_merge_is_suspended_keeps_the_deletion_before_it() {
    /// A store holding forty keys and `foo` in its memtable, or, for a merge,
    /// in the one L0 table a flush made of them.
    async fn ready(wal: bool, merge: bool) -> (SimEnv, roble::Db<SimFs>, Model, Vec<u64>) {
        let env = SimEnv::new();
        let options = Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            .background_tasks(false);
        let options = if wal {
            options
        } else {
            options.wal(roble::Wal::Off)
        };
        let db = roble::open_with(&env, Path::new("/db"), options)
            .await
            .expect("open");
        let mut model = Model::new();
        let mut batch = Batch::new();
        for i in 0..40 {
            let (k, v) = (
                format!("k{i:03}").into_bytes(),
                format!("v{i}").into_bytes(),
            );
            batch.put(&k, &v).expect("put");
            model.insert(k, v);
        }
        batch.put(b"foo", b"old").expect("put");
        model.insert(b"foo".to_vec(), b"old".to_vec());
        db.write_raw(&mut batch, wal).await.expect("write");
        let tables = if merge {
            vec![db.flush().await.expect("flush").expect("a table")]
        } else {
            Vec::new()
        };
        (env, db, model, tables)
    }

    /// The flush, or the merge of that table into L1.
    async fn under_way(db: &roble::Db<SimFs>, tables: &[u64]) -> roble::Result<()> {
        if tables.is_empty() {
            db.flush().await.map(|_| ())
        } else {
            db.compact(0, tables).await.map(|_| ())
        }
    }

    async fn write_with(db: &roble::Db<SimFs>, wal: bool, build: impl FnOnce(&mut Batch)) {
        let mut batch = Batch::new();
        build(&mut batch);
        // With the WAL off a synced write is refused: there is no log to sync.
        db.write_raw(&mut batch, wal).await.expect("write");
    }

    /// Every table, level by level, down to the last.
    async fn to_the_last_level(db: &roble::Db<SimFs>) {
        for level in 0..6 {
            let tables: Vec<u64> = db
                .versions()
                .version
                .level(level)
                .iter()
                .map(|t| t.file_num)
                .collect();
            if !tables.is_empty() {
                db.compact(level, &tables).await.expect("compact");
            }
        }
    }

    for (what, merge) in [("flush", false), ("merge", true)] {
        for wal in [true, false] {
            let total = {
                let (env, db, _, tables) = ready(wal, merge).await;
                let before = env.sim().ops();
                under_way(&db, &tables).await.expect("the operation");
                env.sim().ops() - before
            };
            assert!(
                total >= 4,
                "{what}, wal={wal}: {total} operations are too few to sweep"
            );

            let mut suspended = 0;
            for n in 1..=total {
                let at = format!("{what}, wal={wal}, suspended at {n} of {total}");
                let (env, db, model, tables) = ready(wal, merge).await;
                env.sim().pause_at(n);

                let landing = async {
                    write_with(&db, wal, |b| b.delete(b"foo").expect("delete")).await;
                    let snap = db.snapshot().expect("snapshot");
                    write_with(&db, wal, |b| b.put(b"foo", b"new").expect("put")).await;
                    snap
                };
                let (done, snap, inside) = land_during(under_way(&db, &tables), landing);
                done.unwrap_or_else(|e| panic!("{at}: {e}"));
                if inside {
                    suspended += 1;
                }

                let mut seen = model.clone();
                seen.remove(b"foo".as_slice());
                let check = async |when: &str| {
                    assert_eq!(
                        snap.get(b"foo", Comparison::Equal)
                            .await
                            .expect("get")
                            .map(|found| found.value),
                        None,
                        "{at}, {when}: the snapshot's lookup"
                    );
                    assert_eq!(
                        walk(&mut snap.range(..).expect("scan")).await,
                        seen,
                        "{at}, {when}: the snapshot's scan"
                    );
                };
                check("with the operation done").await;

                // The deletion and the put are in one memtable, and its flush
                // writes out only the put; the merges then leave the old
                // value nowhere either.
                db.flush().await.expect("flush").expect("a table");
                to_the_last_level(&db).await;
                check("with the deletion gone from every table").await;

                let mut now = model.clone();
                now.insert(b"foo".to_vec(), b"new".to_vec());
                assert_eq!(contents(&db).await, now, "{at}: the store itself");
            }
            assert!(
                suspended > 2,
                "{what}, wal={wal}: the operation should have been caught part way more than \
                 twice, was {suspended}"
            );
        }
    }
}

#[compio::test]
async fn a_scan_made_from_a_snapshot_reads_on_once_the_snapshot_is_gone() {
    // RocksDB's `ReleaseSnapshotAfterIteratorCreation`: two L1 tables, each
    // a range deletion over keys in L2, a scan made from a snapshot, and the
    // snapshot dropped before the scan has read anything. The scan holds the
    // version it was made on, not the snapshot, so a compaction that retires
    // every table of that version, and the sweep after it, leave them on disk
    // for it, and it reads what the snapshot saw.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("create");
    let level = |db: &roble::Db<SimFs>, level: u32| -> Vec<u64> {
        let versions = db.versions();
        versions
            .version
            .level(level)
            .iter()
            .map(|t| t.file_num)
            .collect()
    };

    write(&db, |b| {
        for i in 0..10 {
            b.put(format!("k{i}").as_bytes(), b"old").expect("put");
        }
    })
    .await;
    let flushed = db.flush().await.expect("flush").expect("a table");
    let in_l1 = db.compact(0, &[flushed]).await.expect("compact");
    db.compact(1, &in_l1).await.expect("compact");
    for (from, to) in [("k2", "k4"), ("k6", "k8")] {
        write(&db, |b| {
            b.delete_range(from.as_bytes(), to.as_bytes())
                .expect("delete_range")
        })
        .await;
        let flushed = db.flush().await.expect("flush").expect("a table");
        db.compact(0, &[flushed]).await.expect("compact");
    }
    assert_eq!(level(&db, 1).len(), 2, "a table for each range deletion");
    assert_eq!(level(&db, 2).len(), 1);
    let seen = contents(&db).await;
    assert_eq!(seen.len(), 6, "four of the ten keys deleted: {seen:?}");

    let snap = db.snapshot().expect("snapshot");
    let mut scan = snap.range(..).expect("scan");
    drop(snap);

    // Every key written again, and everything merged down into L2.
    write(&db, |b| {
        for i in 0..10 {
            b.put(format!("k{i}").as_bytes(), b"new").expect("put");
        }
    })
    .await;
    let flushed = db.flush().await.expect("flush").expect("a table");
    let in_l1 = db.compact(0, &[flushed]).await.expect("compact");
    db.compact(1, &in_l1).await.expect("compact");
    assert!(level(&db, 1).is_empty(), "L1 merged into L2");
    db.remove_obsolete_files().await.expect("sweep");

    assert_eq!(
        walk(&mut scan).await,
        seen,
        "the scan should read what the snapshot saw, after the snapshot went"
    );
}
