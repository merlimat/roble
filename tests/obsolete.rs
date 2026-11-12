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
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
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

async fn contents<F: Fs>(db: &roble::Opened<F>) -> Model {
    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    let mut out = Model::new();
    while scan.is_valid() {
        out.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    out
}

/// Opens with compaction left to the caller.
///
/// A flush runs the compaction the picker chose unless it is told not to.
/// The tests that use this are about what a flush does on its own — the
/// table it adds, the log it retires — and a compaction following it would
/// change the version before the assertion could read it.
/// `tests/schedule.rs` is where the scheduling is the subject.
async fn open_unscheduled(env: &SimEnv, dir: &Path) -> roble::Opened<SimFs> {
    roble::open_with(env, dir, Options::new().disable_automatic_compactions(true))
        .await
        .expect("open")
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
    let db = open_unscheduled(&env, &dir).await;
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

/// The tables of L0, which the fixtures leave with something to compact.
fn in_l0<F: Fs>(db: &roble::Opened<F>) -> Vec<u64> {
    let found: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    assert!(
        found.len() > 1,
        "the fixture should arrive with more than one table in L0 to compact"
    );
    found
}

/// A table a scan is still reading is not collected, however obsolete.
///
/// The version is where a read starts, so a table it no longer names is one
/// nothing can newly ask for — and that is the whole of the old rule, which
/// is where it went wrong. A scan holds open readers for the tables of the
/// version it *began* on, and goes on asking them for blocks; a compaction
/// that retired those tables and let the sweep delete them left the scan
/// reading files that were not there.
#[compio::test]
async fn a_table_a_scan_is_reading_is_not_collected() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let retired = in_l0(&db);

    // Opened before the compaction, and so holding readers for exactly the
    // tables the compaction is about to retire.
    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");

    db.compact(0, &retired).await.expect("compact");

    let names = listing(env.sim(), &dir).await;
    for file_num in &retired {
        let name = files::table_filename(*file_num);
        assert!(
            names.contains(&name),
            "{name} was collected while a scan was reading it: {names:?}"
        );
    }

    // And the point of the files being there: the scan reads to its end, and
    // reads the database it started on.
    let mut seen = Model::new();
    while scan.is_valid() {
        seen.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    assert_eq!(
        seen,
        contents(&db).await,
        "the scan should have read every key the compaction left"
    );
}

/// And the file goes as soon as the last reader does.
///
/// The other half of the claim: leaving a file alone is a delay, not a leak.
/// A sweep that skipped it has to collect it once the scan it was waiting
/// for has gone — and the sweep a flush runs for itself would do this
/// without being asked.
#[compio::test]
async fn the_table_goes_once_the_scan_does() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let retired = in_l0(&db);

    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    db.compact(0, &retired).await.expect("compact");
    drop(scan);

    let removed = db.remove_obsolete_files().await.expect("sweep");
    for file_num in &retired {
        let name = files::table_filename(*file_num);
        assert!(
            removed.contains(&name),
            "{name} should have been collected once nothing was reading it: {removed:?}"
        );
    }
}

/// And the sweep lets go of only the readers the version has stopped naming.
///
/// Letting go of the rest as well changes no answer, which is exactly why it
/// needs saying: every flush would empty the table cache, and the read after
/// one would reopen every table it touched. The point of letting go at all
/// is that a collected file should wait for whoever is reading it rather
/// than for cache pressure — not that a sweep is a reason to forget a table
/// the database is still built on.
#[compio::test]
async fn a_sweep_keeps_the_readers_the_version_still_names() {
    let (env, dir) = load("db-reads").await;
    let db = open_unscheduled(&env, &dir).await;

    // A read leaves a reader open for the table that answered it.
    db.get(b"a0", Comparison::Equal).await.expect("get");
    let held = db.caches_for_test().open_tables();
    assert!(held > 0, "a read should have left a reader open");

    // A flush sweeps for itself, and retires none of the tables that reader
    // belongs to.
    let mut batch = Batch::new();
    batch.put(b"swept", b"yes").expect("put");
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");

    assert_eq!(
        db.caches_for_test().open_tables(),
        held,
        "the sweep let go of a reader for a table the version still names"
    );
}

#[compio::test]
async fn a_table_a_scan_cannot_reach_is_collected_while_it_is_open() {
    // The mirror of the test above, and the reason that one is not the whole
    // story. A scan pins every table it opens, so a scan that opened every
    // table in the database would hold every table in the database against
    // the sweep — a handful of keys keeping a shard's worth of files alive
    // for as long as the cursor is open. It opens what its bounds can reach
    // and nothing else.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // The L0 tables that start above the bounds below, which is what makes
    // this a test rather than a restatement.
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

    // Opened before the compaction, and bounded well below the tables it is
    // about to retire.
    let mut scan = db.scan_range(Some(b"a"), Some(b"g")).await.expect("scan");
    scan.first().await.expect("first");

    db.compact(0, &beyond).await.expect("compact");

    let names = listing(env.sim(), &dir).await;
    for file_num in &beyond {
        let name = files::table_filename(*file_num);
        assert!(
            !names.contains(&name),
            "{name} is outside the scan's bounds and was kept alive by it anyway: \
             {names:?}"
        );
    }

    // And the scan reads its range to the end, which is the half that says
    // nothing it *did* need was collected.
    let mut seen = Model::new();
    while scan.is_valid() {
        seen.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    let expected: Model = contents(&db)
        .await
        .into_iter()
        .filter(|(k, _)| k.as_slice() >= b"a".as_slice() && k.as_slice() < b"g".as_slice())
        .collect();
    assert_eq!(
        seen, expected,
        "the scan should have read every key inside its bounds"
    );
}
