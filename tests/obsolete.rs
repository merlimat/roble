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

//! Deleting what the database no longer refers to.
//!
//! The deletions are the small half of this file. Deleting a file roble was
//! still using is the one mistake it cannot take back, so most of what
//! follows is about the files that must survive: the ones a database Pebble
//! wrote arrived with, the log holding writes no table has yet, and every
//! name roble cannot parse.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, WriteBuf};
use roble_format::batch::Batch;
use roble_format::files::{self, FileType};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// The database fixtures Pebble left with nothing to collect.
///
/// `db-unflushed-wal` is not among them: Pebble kept an obsolete log in it to
/// recycle, which is its own test below.
const SETTLED: [&str; 4] = ["db-reads", "db-simple", "db-twin-a", "db-twin-b"];

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

/// Every table file in a listing.
fn tables(names: &[String]) -> Vec<u64> {
    names
        .iter()
        .filter_map(|n| files::parse_filename(n))
        .filter(|p| p.file_type == FileType::Table)
        .map(|p| p.file_num)
        .collect()
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
async fn opening_a_database_pebble_wrote_deletes_nothing() {
    // The strongest statement this code can make, and the one that matters:
    // on a directory Pebble itself wrote and considers finished, roble
    // deletes nothing at all. Every fixture here has its tables, its logs,
    // its OPTIONS, its MANIFEST and its markers, and all of them are still
    // there afterwards.
    for fixture in SETTLED {
        let (env, dir) = load(fixture).await;
        let before = listing(env.sim(), &dir).await;
        let db = roble::open(&env, &dir).await.expect("open");
        let after = listing(env.sim(), &dir).await;
        assert_eq!(
            after, before,
            "opening {fixture} changed what the directory holds"
        );
        // And asking for it outright changes nothing either.
        let removed = db
            .remove_obsolete_files()
            .await
            .expect("remove obsolete files");
        assert!(
            removed.is_empty(),
            "{fixture} holds nothing obsolete, but roble deleted {removed:?}"
        );
    }
}

#[compio::test]
async fn a_log_pebble_kept_for_recycling_is_collected() {
    // Pebble does not delete every log it has flushed past: it keeps one to
    // reuse, so that the next log can be written into a file that already
    // exists. `db-unflushed-wal` was closed cleanly and still holds
    // `000002.log`, below a `min_unflushed_log_num` of four.
    //
    // roble does not recycle, so that log is nothing but space — and it is
    // below the bound, which is the only question the rule asks. What makes
    // this safe is not roble's reading of it: `compat/wal-replay` has Go
    // Pebble open this very directory after roble has swept it.
    let (env, dir) = load("db-unflushed-wal").await;
    let before = listing(env.sim(), &dir).await;
    let db = roble::open(&env, &dir).await.expect("open");
    let after = listing(env.sim(), &dir).await;

    assert!(
        db.versions().min_unflushed_log_num > 2,
        "the fixture should have flushed past 000002.log"
    );
    let gone: Vec<&String> = before.iter().filter(|n| !after.contains(n)).collect();
    assert_eq!(
        gone,
        vec![&"000002.log".to_string()],
        "opening should collect the recycled log and nothing else"
    );

    // And the database still reads, including the key only the live log
    // holds.
    for (key, value) in [
        (b"flushed".as_slice(), b"in a table".as_slice()),
        (b"logged".as_slice(), b"only in the log".as_slice()),
    ] {
        assert_eq!(
            db.get(key, Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(value.to_vec()),
            "{key:02x?} went missing when the recycled log was collected"
        );
    }
}

#[compio::test]
async fn a_flush_retires_the_log_it_replaced() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let old_log = db.current_log().expect("a log to write to");

    let mut batch = Batch::new();
    batch.put(b"retire", b"the log").expect("put");
    db.write(&mut batch, true).await.expect("write");

    let before = listing(env.sim(), &dir).await;
    db.flush().await.expect("flush").expect("a table");
    let after = listing(env.sim(), &dir).await;

    let retired = files::log_filename(old_log);
    assert!(
        before.contains(&retired),
        "the fixture should hold {retired}"
    );
    assert!(
        !after.contains(&retired),
        "a flush should retire {retired}; the directory still holds {after:?}"
    );
    let current = files::log_filename(db.current_log().expect("a log"));
    assert!(
        after.contains(&current),
        "and should leave the log it now writes to; the directory holds {after:?}"
    );

    // Nothing else went. In particular every table the version names is
    // still a file, including the one this flush has just written.
    let live = db.versions().version.table_file_nums();
    for file_num in &live {
        let name = files::table_filename(*file_num);
        assert!(
            after.contains(&name),
            "table {name} is in the version but no longer in the directory"
        );
    }
    let gone: Vec<&String> = before.iter().filter(|n| !after.contains(n)).collect();
    assert_eq!(gone, vec![&retired], "a flush deleted more than the log");
}

#[compio::test]
async fn the_log_holding_unflushed_writes_is_not_deleted() {
    // `min_unflushed_log_num` names the first log whose records no table
    // holds — so the log *at* that number is the one carrying live writes,
    // and deleting it loses them. Reading the bound as "at or below" rather
    // than "below" is the whole difference.
    let (env, dir) = load("db-reads").await;

    {
        let db = roble::open(&env, &dir).await.expect("open");

        // A flush first, so the store is writing to a log the MANIFEST
        // actually names: the fixture opens with `min_unflushed_log_num` at
        // zero, where the two readings agree.
        let mut batch = Batch::new();
        batch.put(b"flushed", b"yes").expect("put");
        db.write(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
        assert_eq!(
            db.current_log(),
            Some(db.versions().min_unflushed_log_num),
            "the store should be writing to the first unflushed log"
        );

        // Now a write that only the log holds.
        let mut batch = Batch::new();
        batch.put(b"unflushed", b"yes").expect("put");
        db.write(&mut batch, true).await.expect("write");

        let removed = db.remove_obsolete_files().await.expect("remove");
        assert!(
            removed.is_empty(),
            "nothing is obsolete here, but roble deleted {removed:?}"
        );
    }

    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        db.get(b"unflushed", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"yes".to_vec()),
        "the write was only in the log, and the log should still be there"
    );
    assert_eq!(
        db.get(b"flushed", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"yes".to_vec()),
        "and the flushed write should have survived too"
    );
}

#[compio::test]
async fn a_file_roble_does_not_own_is_left_alone() {
    // A database directory may hold files that are none of roble's business
    // — the compat harness writes one into it — and a name roble cannot
    // reason about is one it must not decide about.
    let (env, dir) = load("db-reads").await;
    let strangers = [
        "roble-wrote.txt",
        "notes.md",
        "000099",
        "000099.sstable",
        "marker.format-version.000001.013",
        // Half-written files Pebble was about to rename into place. Pebble's
        // own obsolete-file scan leaves these alone, and being no more
        // aggressive than Pebble is the posture.
        "temporary.000001.dbtmp",
        "CURRENT.000001.dbtmp",
    ];
    for name in strangers {
        let disk = env.sim().create(&dir.join(name)).await.expect("create");
        let (result, _) = disk
            .write(WriteBuf::new(b"not yours".to_vec()), 0, ())
            .await;
        result.expect("write");
        disk.sync(()).await.expect("sync");
        disk.close().await.expect("close");
    }
    env.sim().sync_dir(&dir).await.expect("sync dir");

    let db = roble::open(&env, &dir).await.expect("open");
    let mut batch = Batch::new();
    batch.put(b"a-write", b"to flush").expect("put");
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");

    let after = listing(env.sim(), &dir).await;
    for name in strangers {
        assert!(
            after.contains(&name.to_string()),
            "{name} is not roble's to delete; the directory holds {after:?}"
        );
    }
    // Nor are the files a database has that roble never writes.
    for name in ["LOCK", "OPTIONS-000003", "MANIFEST-000001"] {
        assert!(
            after.contains(&name.to_string()),
            "{name} should still be there; the directory holds {after:?}"
        );
    }
}

#[compio::test]
async fn an_interrupted_flush_leaves_a_table_the_next_open_collects() {
    // A flush writes its table and syncs it before the MANIFEST edit that
    // claims it (§6), so a crash in between leaves a complete table that no
    // version names. Nothing else will ever go looking for it.
    //
    // Rather than pick one moment to be interrupted at, this cuts the flush
    // at every operation it performs, and demands the same two things each
    // time: the database still answers exactly as it did before the flush,
    // and the reopen leaves no table the version does not name.
    let cut_points = {
        let (env, dir) = load("db-simple").await;
        let db = roble::open(&env, &dir).await.expect("open");
        let mut batch = Batch::new();
        batch.put(b"cut", b"here").expect("put");
        db.write(&mut batch, true).await.expect("write");
        let before = env.sim().ops();
        db.flush().await.expect("flush").expect("a table");
        env.sim().ops() - before
    };
    assert!(
        cut_points > 8,
        "a flush takes {cut_points} operations, which is too few to be the whole of one"
    );

    let mut collected = 0;
    for n in 1..=cut_points {
        let (env, dir) = load("db-simple").await;
        let expected;
        {
            let db = roble::open(&env, &dir).await.expect("open");
            let mut batch = Batch::new();
            batch.put(b"cut", b"here").expect("put");
            db.write(&mut batch, true).await.expect("write");
            expected = contents(&db).await;

            env.sim().fail_at(n);
            // Failing is the point; succeeding is allowed too, since the
            // last few operations of a flush are the cleanup after it.
            let _ = db.flush().await;
            env.sim().stop_failing();
        }
        env.sim().crash();

        let orphaned = listing(env.sim(), &dir).await;
        let db = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("reopen after a flush cut at operation {n}: {e}"));
        let swept = listing(env.sim(), &dir).await;

        assert_eq!(
            contents(&db).await,
            expected,
            "a flush cut at operation {n} changed an answer"
        );
        let live = db.versions().version.table_file_nums();
        for file_num in tables(&swept) {
            assert!(
                live.contains(&file_num),
                "a flush cut at operation {n} left table {file_num:06} behind"
            );
        }
        for file_num in &live {
            assert!(
                tables(&swept).contains(file_num),
                "the reopen after a cut at operation {n} deleted live table {file_num:06}"
            );
        }
        if tables(&orphaned).len() > tables(&swept).len() {
            collected += 1;
        }
    }
    assert!(
        collected > 0,
        "no cut point left an orphan table, so nothing was collected and this \
         test proves nothing"
    );
}
