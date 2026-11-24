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

//! A checkpoint: a directory holding what a store holds (§6, §11).
//!
//! What it is for is somebody else opening it — Oxia moves a shard by
//! streaming one to another node — so the claim that matters is that the
//! directory is a database. Here that is checked by roble opening it;
//! `compat/` checks it by handing the same directory to Go Pebble, which is
//! the half roble cannot check about itself.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, Options, Wal, WriteBuf};
use roble_format::batch::Batch;
use roble_format::files::{self, FileType};

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

async fn contents<F: Fs>(db: &roble::Db<F>) -> Model {
    let mut scan = db.range(..).expect("scan");
    scan.first().await.expect("first");
    let mut out = Model::new();
    while scan.is_valid() {
        out.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    out
}

/// Every filename in `dir`, sorted.
async fn listing(fs: &SimFs, dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs
        .list(dir)
        .await
        .expect("list")
        .into_iter()
        .map(|e| e.name.to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// Writes `keys` under `prefix` and says what they were.
///
/// `sync` is the store's own: asking for one with no log to sync is refused,
/// and rightly (§4).
async fn write_keys<F: Fs>(db: &roble::Db<F>, prefix: &str, keys: usize, sync: bool) -> Model {
    let mut batch = Batch::new();
    let mut written = Model::new();
    for i in 0..keys {
        let key = format!("{prefix}{i:03}").into_bytes();
        batch.put(&key, b"value").expect("put");
        written.insert(key, b"value".to_vec());
    }
    db.write_raw(&mut batch, sync).await.expect("write");
    written
}

#[compio::test]
async fn a_checkpoint_opens_as_the_store_it_was_taken_of() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new().disable_automatic_compactions(true),
    )
    .await
    .expect("open");
    let mut model = contents(&db).await;
    model.extend(write_keys(&db, "c", 20, true).await);

    let at = PathBuf::from("/snapshot");
    db.checkpoint(&at).await.expect("checkpoint");

    let copy = roble::open(&env, &at).await.expect("open the checkpoint");
    assert_eq!(
        contents(&copy).await,
        model,
        "a checkpoint should answer every key the store it came from answers"
    );
}

#[compio::test]
async fn a_checkpoint_takes_the_memtable_with_it() {
    // It flushes first, which is the whole reason it can: with the WAL off
    // there is no log in the directory to carry what the memtable held, and
    // Oxia runs with the WAL off (§4).
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, Options::new().wal(roble::Wal::Off))
        .await
        .expect("open");
    let mut model = contents(&db).await;
    model.extend(write_keys(&db, "unflushed", 5, false).await);
    assert!(
        db.unflushed_bytes() > 0,
        "the setup should have left something in the memtable"
    );

    let at = PathBuf::from("/snapshot");
    db.checkpoint(&at).await.expect("checkpoint");
    assert_eq!(
        db.unflushed_bytes(),
        0,
        "and the checkpoint should have written it out on the way"
    );

    let copy = roble::open_with(&env, &at, Options::new().wal(roble::Wal::Off))
        .await
        .expect("open the checkpoint");
    assert_eq!(
        contents(&copy).await,
        model,
        "so the keys that were only in the memtable are in the checkpoint"
    );
}

#[compio::test]
async fn a_checkpoint_after_a_failed_flush_holds_every_write_made_before_it() {
    // A flush that fails leaves its memtable frozen, and the writes after it
    // go to the one that replaced it. A checkpoint carries no log, so it holds
    // a write only if the flush it starts with writes it out — and after a
    // failure that flush has two memtables to write, not one. With the log on
    // as much as off: the log would keep the later writes, but not in the
    // checkpoint.
    for wal in [roble::Wal::Synced, roble::Wal::Off] {
        let options = || {
            Options::new()
                .wal(wal)
                .background_tasks(false)
                .disable_automatic_compactions(true)
        };
        let sync = wal != roble::Wal::Off;
        let total = {
            let (env, dir) = load("db-twin-a").await;
            let db = roble::open_with(&env, &dir, options()).await.expect("open");
            write_keys(&db, "before", 5, sync).await;
            let before = env.sim().ops();
            db.flush().await.expect("flush");
            env.sim().ops() - before
        };

        let mut checked = 0;
        for n in 1..=total {
            let (env, dir) = load("db-twin-a").await;
            let db = roble::open_with(&env, &dir, options()).await.expect("open");
            let mut model = contents(&db).await;
            model.extend(write_keys(&db, "before", 5, sync).await);

            env.sim().fail_at(n);
            let failed = db.flush().await.is_err();
            env.sim().stop_failing();
            if !failed {
                // The injection landed somewhere the flush lets go of, such
                // as the sweep after its edit, and there is nothing frozen.
                continue;
            }
            model.extend(write_keys(&db, "after", 5, sync).await);

            let at = PathBuf::from("/snapshot");
            if db.checkpoint(&at).await.is_err() {
                // The failure was the MANIFEST append, after which the store
                // records nothing more and says so (`crate::flush`).
                continue;
            }
            let copy = roble::open_with(&env, &at, options())
                .await
                .unwrap_or_else(|e| {
                    panic!("wal={wal:?}: failing at {n} of {total}, opening the checkpoint: {e}")
                });
            assert_eq!(
                contents(&copy).await,
                model,
                "wal={wal:?}: failing at {n} of {total}, the checkpoint is missing writes made \
                 before it"
            );
            checked += 1;
        }
        assert!(
            checked > 0,
            "wal={wal:?}: no failed flush was followed by a checkpoint, so nothing above \
             checked one"
        );
    }
}

#[compio::test]
async fn a_checkpoint_holds_its_tables_a_manifest_and_two_markers() {
    // And nothing else. A log would have nothing to carry, an `OPTIONS-*` is
    // something roble neither writes nor reads and Pebble writes for itself,
    // and a `LOCK` belongs to whoever opens the directory rather than to
    // whoever wrote it.
    let (env, dir) = load("db-reads").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new().disable_automatic_compactions(true),
    )
    .await
    .expect("open");
    write_keys(&db, "c", 20, true).await;

    let at = PathBuf::from("/snapshot");
    db.checkpoint(&at).await.expect("checkpoint");

    let names = listing(env.sim(), &at).await;
    let tables = db.versions().version.table_file_nums().len();
    let kinds: BTreeMap<&str, usize> = names.iter().fold(BTreeMap::new(), |mut counts, name| {
        let kind = if name.starts_with("marker.manifest.") {
            "marker.manifest"
        } else if name.starts_with("marker.format-version.") {
            "marker.format-version"
        } else {
            match files::parse_filename(name).map(|p| p.file_type) {
                Some(FileType::Table) => "table",
                Some(FileType::Manifest) => "manifest",
                Some(FileType::Log) => "log",
                Some(FileType::Options) => "options",
                _ => "other",
            }
        };
        *counts.entry(kind).or_default() += 1;
        counts
    });

    assert_eq!(kinds.get("table").copied(), Some(tables), "{names:?}");
    assert_eq!(kinds.get("manifest").copied(), Some(1), "{names:?}");
    assert_eq!(kinds.get("marker.manifest").copied(), Some(1), "{names:?}");
    assert_eq!(
        kinds.get("marker.format-version").copied(),
        Some(1),
        "{names:?}"
    );
    // Spelled as Pebble spells it, zero-padded to three digits: the value is
    // parsed as a number, so `16` was readable and still left a directory
    // distinguishable from one Pebble wrote.
    assert!(
        names
            .iter()
            .any(|n| n == "marker.format-version.000001.016"),
        "the format version should be spelled Pebble's way: {names:?}"
    );
    assert_eq!(kinds.get("log").copied(), None, "no logs: {names:?}");
    assert_eq!(kinds.get("options").copied(), None, "no OPTIONS: {names:?}");
    assert_eq!(kinds.get("other").copied(), None, "nothing else: {names:?}");

    // And the file number it says comes next is past every number it holds,
    // its own MANIFEST included. A store opened on this writes its first
    // table under that number, and a number handed out twice is the hazard
    // a freeze spends its log's number to avoid (§5).
    let copy = roble::open(&env, &at).await.expect("open the checkpoint");
    let highest = names
        .iter()
        .filter_map(|n| files::parse_filename(n).map(|p| p.file_num))
        .max()
        .expect("something numbered");
    assert!(
        copy.versions().next_file_num > highest,
        "the checkpoint says {} comes next, and already holds {highest}: {names:?}",
        copy.versions().next_file_num
    );
}

#[compio::test]
async fn a_checkpoint_that_failed_leaves_the_store_it_came_from_alone() {
    // §12's sweep, aimed at the one thing a half-written checkpoint must not
    // cost: the database it was taken of. The checkpoint itself is allowed
    // to be rubbish — it is a directory the caller asked for and can ask for
    // again — but the store is somebody's shard.
    let total = {
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open(&env, &dir).await.expect("open");
        write_keys(&db, "c", 10, true).await;
        let before = env.sim().ops();
        db.checkpoint(Path::new("/snapshot"))
            .await
            .expect("checkpoint");
        env.sim().ops() - before
    };
    assert!(
        total >= 4,
        "a checkpoint of {total} operations is too thin to sweep"
    );

    for n in 1..=total {
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open(&env, &dir).await.expect("open");
        let mut model = contents(&db).await;
        model.extend(write_keys(&db, "c", 10, true).await);

        env.sim().fail_at(n);
        let _ = db.checkpoint(Path::new("/snapshot")).await;
        env.sim().stop_failing();

        assert_eq!(
            contents(&db).await,
            model,
            "failing at {n} of {total}, the store should answer what it always did"
        );
        drop(db);
        let reopened = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("failing at {n} of {total}, the reopen: {e}"));
        assert_eq!(
            contents(&reopened).await,
            model,
            "failing at {n} of {total}, reopened"
        );
    }
}

#[compio::test]
async fn a_half_written_checkpoint_never_reads_as_a_database() {
    // An unfinished checkpoint is rubbish, and allowed to be — the test above
    // says the store it came from is what must survive. What it may not be is
    // a directory that *claims* to be a database, which is what the manifest
    // marker makes it.
    //
    // So the two markers are not interchangeable in order. Written manifest
    // first, a checkpoint interrupted between them left a directory an open
    // read as a database whose format version had never been recorded — the
    // one case Pebble reserves that message for, a database older than
    // markers themselves. Written format-version first, the only marker that
    // can be there alone is the one that claims nothing.
    //
    // Which is checked here directly rather than through the refusal: a
    // failure before either marker leaves a directory with no markers at all,
    // and that is refused for its missing format version whatever this
    // function does, exactly as an empty directory is (`tests/create.rs`).
    let total = {
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open(&env, &dir).await.expect("open");
        write_keys(&db, "c", 10, true).await;
        let before = env.sim().ops();
        db.checkpoint(Path::new("/snapshot"))
            .await
            .expect("checkpoint");
        env.sim().ops() - before
    };

    let mut saw_one_marker = 0;
    for n in 1..=total {
        let (env, dir) = load("db-twin-a").await;
        let db = roble::open(&env, &dir).await.expect("open");
        write_keys(&db, "c", 10, true).await;

        let at = Path::new("/snapshot");
        env.sim().fail_at(n);
        let _ = db.checkpoint(at).await;
        env.sim().stop_failing();
        drop(db);

        let names = listing(env.sim(), at).await;
        let manifest = names.iter().any(|n| n.starts_with("marker.manifest."));
        let format = names
            .iter()
            .any(|n| n.starts_with("marker.format-version."));
        if format && !manifest {
            saw_one_marker += 1;
        }
        assert!(
            !manifest || format,
            "failing at {n} of {total} left a directory claiming to be a database \
             with no format version: {names:?}"
        );
        // And a directory that does claim it has to mean it: every file the
        // marker's MANIFEST names is already there, because the marker was
        // the last thing written.
        if manifest {
            roble::open(&env, at)
                .await
                .unwrap_or_else(|e| panic!("failing at {n} of {total}, the checkpoint: {e}"));
        }
    }
    assert!(
        saw_one_marker > 0,
        "no failure landed between the two markers, so the order is untested"
    );
}

/// Runs `op` until it is about to run its operation `n`, counting from one,
/// and stops it there for good, as a kill would; says whether it finished
/// first, which it does if it has fewer operations than that.
///
/// Polled until it gets there rather than once, since a flush yields between
/// the slices of the memtable it walks, and a yield before operation `n` is
/// not yet the kill.
fn killed_before(env: &SimEnv, n: u64, op: impl Future<Output = roble::Result<()>>) -> bool {
    let start = env.sim().ops();
    env.sim().pause_at(n);
    let mut op = pin!(op);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100_000 {
        match op.as_mut().poll(&mut cx) {
            Poll::Ready(result) => return result.is_ok(),
            Poll::Pending if env.sim().ops() - start + 1 >= n => return false,
            Poll::Pending => {}
        }
    }
    panic!("the operation is not making progress");
}

#[compio::test]
async fn a_checkpoint_survives_a_crash_that_drops_what_no_sync_covered() {
    // RocksDB's `CheckpointWithUnsyncedDataDropped`: writes no sync covered,
    // a checkpoint, and a crash, which drops whatever no sync covered. A
    // checkpoint that returned has to open holding every one of them: its
    // flush wrote them out, and everything it then put in the directory was
    // synced, names included, before it returned.
    //
    // Then the same checkpoint killed before each of its operations, and the
    // machine crashed. The store comes back with the writes or without them,
    // and the checkpoint is either no database or one holding them all, never
    // one holding writes the store it was taken of lost.
    for wal in [Wal::Buffered, Wal::Off] {
        let options = || {
            Options::new()
                .wal(wal)
                .background_tasks(false)
                .disable_automatic_compactions(true)
        };
        let total = {
            let (env, dir) = load("db-twin-a").await;
            let db = roble::open_with(&env, &dir, options()).await.expect("open");
            write_keys(&db, "c", 10, false).await;
            let before = env.sim().ops();
            db.checkpoint(Path::new("/snapshot"))
                .await
                .expect("checkpoint");
            env.sim().ops() - before
        };

        let (mut kept, mut lost) = (0, 0);
        // One past the last operation is no kill at all.
        for n in 1..=total + 1 {
            let (env, dir) = load("db-twin-a").await;
            let db = roble::open_with(&env, &dir, options()).await.expect("open");
            let before = contents(&db).await;
            let mut after = before.clone();
            after.extend(write_keys(&db, "c", 10, false).await);

            let at = Path::new("/snapshot");
            let finished = killed_before(&env, n, db.checkpoint(at));
            let what = if finished {
                format!("wal={wal:?}, a checkpoint that returned")
            } else {
                format!("wal={wal:?}, a checkpoint killed before {n} of {total}")
            };
            assert_eq!(finished, n > total, "{what}");
            drop(db);
            env.sim().crash();

            let db = roble::open_with(&env, &dir, options())
                .await
                .unwrap_or_else(|e| panic!("{what}: reopening the store: {e}"));
            let found = contents(&db).await;
            assert!(
                found == before || found == after,
                "{what}: the store came back with part of the batch"
            );
            if !finished {
                if found == after {
                    kept += 1;
                } else {
                    lost += 1;
                }
            }

            // Killed before it made the directory, there is none.
            let names: Vec<String> = env
                .sim()
                .list(at)
                .await
                .map(|entries| {
                    entries
                        .iter()
                        .map(|e| e.name.to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            let database = names.iter().any(|n| n.starts_with("marker.manifest."));
            assert!(
                database || !finished,
                "{what}: the checkpoint is no database after the crash: {names:?}"
            );
            if database {
                let copy = roble::open_with(&env, at, options())
                    .await
                    .unwrap_or_else(|e| panic!("{what}: opening the checkpoint: {e}"));
                assert_eq!(
                    contents(&copy).await,
                    after,
                    "{what}: the checkpoint is missing writes made before it"
                );
                assert_eq!(
                    found, after,
                    "{what}: the checkpoint holds writes the store it was taken of lost"
                );
            }
        }
        // Kills either side of the flush that made the writes durable, so
        // that both of what a store may come back as were seen.
        assert!(
            kept > 0 && lost > 0,
            "wal={wal:?}: of {total} kills, {kept} kept the writes and {lost} lost them"
        );
    }
}
