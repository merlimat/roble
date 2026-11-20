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

//! Writing with the WAL off, which is the configuration Oxia runs (§1).
//!
//! The WAL being off is not an absence of a feature; it moves where
//! durability comes from. A write reaches the memtable and nothing else, and
//! the only promise the store makes is the one `flush` keeps. So the two
//! things worth testing are the promise it does keep — a crash takes the
//! database back to its last flush, exactly — and the one it refuses to make.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Access, Comparison, Disk, Error, Fs, Options, WriteBuf};
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

fn no_wal() -> Options<SimFs> {
    Options::new().wal(roble::Wal::Off)
}

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

fn of_type(names: &[String], want: FileType) -> Vec<u64> {
    names
        .iter()
        .filter_map(|n| files::parse_filename(n))
        .filter(|p| p.file_type == want)
        .map(|p| p.file_num)
        .collect()
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

#[compio::test]
async fn a_crash_takes_the_database_back_to_its_last_flush() {
    // §13's other M3 gate, in the configuration Oxia runs. With the WAL on
    // the line is the last synced write; with it off the line is the last
    // flush, and everything after it is gone.
    let (env, dir) = load("db-twin-a").await;
    let flushed;

    {
        let db = roble::open_with(&env, &dir, no_wal()).await.expect("open");

        let mut batch = Batch::new();
        batch.put(b"before", b"the flush").expect("put");
        batch.delete(b"a0").expect("delete");
        db.write_raw(&mut batch, false).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
        flushed = contents(&db).await;

        // Past the line. None of this has anywhere to be recovered from.
        let mut batch = Batch::new();
        batch.put(b"after", b"the flush").expect("put");
        batch.put(b"before", b"overwritten").expect("put");
        batch.delete_range(b"a", b"b").expect("delete_range");
        db.write_raw(&mut batch, false).await.expect("write");
        assert_ne!(
            contents(&db).await,
            flushed,
            "the writes after the flush should be readable while the store is up"
        );
    }

    env.sim().crash();
    let db = roble::open_with(&env, &dir, no_wal())
        .await
        .expect("reopen");
    assert_eq!(
        contents(&db).await,
        flushed,
        "a crash should land exactly on the last flush"
    );
}

#[compio::test]
async fn a_write_is_refused_the_sync_it_cannot_keep() {
    // Ignoring `sync` would be the silent half of a data loss: the caller
    // asked for durability, got `Ok`, and has none. Pebble refuses it too.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, no_wal()).await.expect("open");
    let before = contents(&db).await;

    let mut batch = Batch::new();
    batch.put(b"durable", b"please").expect("put");
    match db.write_raw(&mut batch, true).await {
        Err(Error::Unsupported(m)) => assert!(m.contains("opened with the WAL off"), "got: {m}"),
        other => panic!("expected Unsupported, got {other:?}"),
    }

    assert_eq!(
        contents(&db).await,
        before,
        "a refused write should have changed nothing"
    );
    // And the same batch goes through once it stops asking for what the
    // store cannot give.
    db.write_raw(&mut batch, false).await.expect("write");
    assert_eq!(
        db.get(b"durable", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"please".to_vec()),
    );
}

#[compio::test]
async fn nothing_reaches_the_log() {
    // The fixture arrives with an empty log, as a database Pebble wrote with
    // the WAL off does — eleven bytes of EOF trailer and nothing else. It
    // should still be those eleven bytes once the writes are in, and the
    // flush should then retire it rather than write to it.
    let (env, dir) = load("db-twin-a").await;
    let logs = of_type(&listing(env.sim(), &dir).await, FileType::Log);
    assert_eq!(logs.len(), 1, "the fixture should hold one log: {logs:?}");
    let log_path = dir.join(files::log_filename(logs[0]));
    let before = env.sim().durable(&log_path).expect("the log");

    let db = roble::open_with(&env, &dir, no_wal()).await.expect("open");
    assert_eq!(db.current_log(), None, "the store should hold no log");
    assert!(db.wal_disabled());

    for i in 0..4 {
        let mut batch = Batch::new();
        batch
            .put(format!("key-{i}").as_bytes(), b"value")
            .expect("put");
        db.write_raw(&mut batch, false).await.expect("write");
    }

    // What the file holds, synced or not: a write that reached the log is
    // there whether or not anything has synced it yet.
    let log = env
        .sim()
        .open(&log_path, Access::ReadOnly)
        .await
        .expect("open the log");
    let now = roble::__private::read_all(&log, ())
        .await
        .expect("read the log");
    log.close().await.expect("close the log");
    assert_eq!(
        now, before,
        "something was written to the log of a store that has none"
    );

    db.flush().await.expect("flush").expect("a table");
    assert_eq!(
        env.sim().durable(&log_path),
        None,
        "the flush should have retired the log"
    );
}

#[compio::test]
async fn a_flush_with_the_wal_off_makes_no_log_and_retires_every_one() {
    // With the WAL off a write goes to no log, so once a flush has landed
    // no log holds anything a table does not, and its edit moves the minimum
    // unflushed log number past every log there is: here the empty one the
    // fixture's Pebble left behind.
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, no_wal()).await.expect("open");

    let before = listing(env.sim(), &dir).await;
    assert_eq!(
        of_type(&before, FileType::Log).len(),
        1,
        "the fixture should hold one log: {before:?}"
    );
    let next_file_num = db.versions().next_file_num;

    let mut batch = Batch::new();
    batch.put(b"one", b"table").expect("put");
    db.write_raw(&mut batch, false).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");

    let after = listing(env.sim(), &dir).await;
    assert_eq!(
        of_type(&after, FileType::Log),
        Vec::<u64>::new(),
        "a flush with the WAL off should make no log, and retire the one there was"
    );
    assert_eq!(
        db.versions().min_unflushed_log_num,
        table,
        "and should move the minimum unflushed log number to its table's, \
         which is past every log the open found"
    );
    assert_eq!(
        db.versions().next_file_num,
        next_file_num + 1,
        "it should spend one file number, for the table, and not two"
    );
    assert_eq!(table, next_file_num, "and the table should have the first");
}

#[compio::test]
async fn a_log_written_with_a_wal_is_still_replayed_without_one() {
    // Turning the WAL off says nothing about the records already in a log:
    // they were acknowledged when they were written, and an open that
    // dropped them would be losing writes on a configuration change.
    let (env, dir) = load("db-reads").await;
    let with_wal;

    {
        let db = roble::open(&env, &dir).await.expect("open");

        // A flush with the WAL on first, so the MANIFEST's log bound is a
        // real log number. The fixture opens with it at zero, where leaving
        // the bound alone and writing a zero over it are the same thing —
        // and a test that cannot tell those apart is no test of this.
        let mut batch = Batch::new();
        batch
            .put(b"first", b"flushed with the wal on")
            .expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
        with_wal = db.versions().min_unflushed_log_num;
        assert!(
            with_wal > 0,
            "the flush should have left a real log bound, not {with_wal}"
        );

        let mut batch = Batch::new();
        batch.put(b"logged", b"with the wal on").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
    }
    env.sim().crash();

    let db = roble::open_with(&env, &dir, no_wal())
        .await
        .expect("reopen without a WAL");
    assert_eq!(
        db.get(b"logged", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"with the wal on".to_vec()),
        "a record in the log should be replayed whatever this store was asked to do"
    );

    // The open keeps the MANIFEST's minimum unflushed log number where the
    // WAL left it, rather than writing back the zero a database that never
    // had a WAL carries — which would make every retired log live again, and
    // replayed. And a flush here retires the log it has just written out:
    // left behind the bound, it would be replayed at every open beneath the
    // table that holds its records.
    assert_eq!(
        db.versions().min_unflushed_log_num,
        with_wal,
        "the reopen should have kept the bound the WAL left"
    );
    db.flush().await.expect("flush").expect("a table");
    assert!(
        db.versions().min_unflushed_log_num > with_wal,
        "a flush with the WAL off should move the log bound past the log it wrote out, \
         not leave it at {with_wal}"
    );
    assert_eq!(
        env.sim().durable(&dir.join(files::log_filename(with_wal))),
        None,
        "and that log should be gone"
    );

    env.sim().crash();
    let db = roble::open_with(&env, &dir, no_wal())
        .await
        .expect("reopen again");
    assert_eq!(
        db.get(b"logged", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"with the wal on".to_vec()),
        "and it should still be there, now that only a table holds it"
    );
}

#[compio::test]
async fn a_flushed_delete_stays_deleted_after_the_wal_is_turned_off() {
    // A put logged with the WAL on, then deleted, flushed and compacted with
    // it off. The compaction drops the tombstone, because nothing below it
    // holds the key, so from there only the log still names the key. A log
    // the flush left unretired would be replayed at the next open and bring
    // the put back, though the delete was flushed and is durable by
    // everything `Wal::Off` promises.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let options = |wal| {
        Options::new()
            .wal(wal)
            .create_if_missing(true)
            .background_tasks(false)
    };
    let get = async |db: &roble::Db<SimFs>| {
        db.get(b"k", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value)
    };

    let logged_to = {
        let db = roble::open_with(&env, &dir, options(roble::Wal::Synced))
            .await
            .expect("create");
        db.put(b"k", b"v").await.expect("put");
        db.current_log().expect("a log")
    };
    env.sim().crash();

    {
        let db = roble::open_with(&env, &dir, options(roble::Wal::Off))
            .await
            .expect("reopen without a WAL");
        assert_eq!(get(&db).await, Some(b"v".to_vec()), "the log is replayed");

        db.delete(b"k").await.expect("delete");
        let table = db.flush().await.expect("flush").expect("a table");
        let out = db.compact(0, &[table]).await.expect("compact");
        assert!(
            out.is_empty(),
            "the compaction should have dropped the tombstone, not written {out:?}"
        );
        assert_eq!(get(&db).await, None, "deleted, flushed and compacted");
        db.close().await.expect("close");
    }
    env.sim().crash();

    let db = roble::open_with(&env, &dir, options(roble::Wal::Off))
        .await
        .expect("reopen again");
    assert_eq!(
        get(&db).await,
        None,
        "a flushed delete came undone: the reopen replayed the log the flush wrote out"
    );
    // And why: the flush moved the bound past the log the put went to, and
    // the log went with it, so there was nothing left to replay.
    assert!(
        db.versions().min_unflushed_log_num > logged_to,
        "the log bound should be past log {logged_to}"
    );
    assert_eq!(
        of_type(&listing(env.sim(), &dir).await, FileType::Log),
        Vec::<u64>::new(),
        "and no log should be left for an open to replay"
    );
}
