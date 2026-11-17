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

//! Creating a database where there is none.
//!
//! The claim is not that files appear — it is that what appears is a
//! database: one an `open` reads back without being told to create anything,
//! one that takes a write with the WAL on, and one whose first key is
//! numbered where Pebble numbers its first key.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

/// An open that may create, which is the only way to get a database from
/// nothing.
fn creating() -> Options<SimFs> {
    Options::new().create_if_missing(true)
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
async fn an_open_that_may_create_makes_a_database_an_ordinary_open_reads() {
    // The whole claim in one test: nothing in the directory, then a database
    // that opens again with `create_if_missing` off — so what the creation
    // wrote satisfies the same reader that refuses an empty directory.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");

    let refused = roble::open(&env, &dir)
        .await
        .expect_err("there is nothing there yet");

    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("create");
    drop(db);

    let db = roble::open(&env, &dir)
        .await
        .expect("the created database should open without being created again");
    assert_eq!(db.format_version(), 16, "written at roble's pin");
    assert!(
        db.versions().version.levels().all(|(_, t)| t.is_empty()),
        "and hold no tables"
    );
    drop(db);

    // Which refusal it was: the format-version marker, because an open reads
    // that before it looks for a MANIFEST (§6) — the same words
    // `tests/destroy.rs` pins for a destroyed directory, since a destroyed
    // one and a never-used one are the same directory.
    assert!(
        refused.to_string().contains("marker.format-version"),
        "an empty directory should be refused for its missing format version: {refused}"
    );
}

#[compio::test]
async fn creation_writes_the_least_a_database_can_be() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("create");
    drop(db);

    assert_eq!(
        listing(env.sim(), &dir).await,
        vec![
            "000002.log".to_string(),
            "LOCK".to_string(),
            "MANIFEST-000001".to_string(),
            "marker.format-version.000001.016".to_string(),
            "marker.manifest.000001.MANIFEST-000001".to_string(),
        ],
        "a MANIFEST, the log it names as unflushed, and one iteration of each marker"
    );
}

#[compio::test]
async fn a_created_database_takes_a_write_with_the_wal_on() {
    // The reason creation writes an empty log at all. A store whose log is
    // `None` refuses a write it could not recover, and nothing but a flush
    // ever opens a log — so a creation that left the log out would make a
    // database that cannot be written to until it has been flushed, and
    // cannot be flushed until it has been written to.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("create");

    let mut batch = Batch::new();
    batch.put(b"/a", b"one").expect("put");
    db.write(&mut batch, true).await.expect("write");

    let found = db
        .get(b"/a", Comparison::Equal)
        .await
        .expect("get")
        .expect("the key should be there");
    assert_eq!(found.value, b"one");
    drop(db);

    // And durably, which is what the log is for: reopened without a flush,
    // the write comes back out of the log the creation left.
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        db.get(b"/a", Comparison::Equal)
            .await
            .expect("get")
            .expect("replayed")
            .value,
        b"one",
        "the write should have been replayed from the log"
    );
}

#[compio::test]
async fn the_first_key_of_a_created_database_is_numbered_where_pebbles_is() {
    // Pebble starts handing out sequence numbers at 10, keeping 1 to 9 for
    // itself, and clamps up to 10 when it loads a MANIFEST that says less.
    // A created database says zero, so without the same clamp roble would
    // write its first keys into the range Pebble has set aside.
    //
    // `tests/golden/db-created` is the database Pebble made and wrote one key
    // to, and the test below reads the number it gave that key straight out
    // of it.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("create");

    let mut batch = Batch::new();
    batch.put(b"/first", b"v").expect("put");
    db.write(&mut batch, false).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");

    let smallest = db
        .versions()
        .version
        .level(0)
        .iter()
        .find(|t| t.file_num == table)
        .expect("the flushed table")
        .smallest_seq_num;
    assert_eq!(
        smallest, 10,
        "the first key written should be numbered as Pebble numbers its first"
    );
}

#[compio::test]
async fn only_an_open_that_may_create_makes_the_directory() {
    // On a real filesystem, which is the only one that minds: `SimFs` will
    // take a lock at a path whose directory was never made, so the
    // difference between these two is invisible to it.
    //
    // The claim is the reason `create_if_missing` is off by default — a
    // mistyped path says so rather than becoming an empty store — and the
    // reason the `mkdir_all` sits where it does, ahead of a lock that is a
    // file inside the directory it makes.
    let tmp = tempfile::tempdir().expect("temp dir");

    let refused = tmp.path().join("not-made");
    roble::open(&roble::DefaultEnv, &refused)
        .await
        .expect_err("a directory that is not there is not a database");
    assert!(
        !refused.exists(),
        "and an open that may not create should not have made it"
    );

    let made = tmp.path().join("made");
    let db = roble::open_with(
        &roble::DefaultEnv,
        &made,
        Options::new().create_if_missing(true),
    )
    .await
    .expect("create");
    drop(db);
    assert!(made.join("MANIFEST-000001").exists(), "a database in it");
}

#[compio::test]
async fn an_unfinished_creation_is_finished() {
    // Everything a creation writes except the marker that makes it a
    // database: the state a failure or a crash between the two markers
    // leaves. It holds nothing anyone wrote, so the next open that may create
    // finishes the job rather than refusing a directory no open would ever
    // read again.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("create");
    drop(db);
    let created = listing(env.sim(), &dir).await;
    env.sim()
        .remove(&dir.join("marker.manifest.000001.MANIFEST-000001"))
        .await
        .expect("remove");

    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("the leftovers of a creation are finished");
    let mut batch = Batch::new();
    batch.put(b"key", b"value").expect("put");
    db.write(&mut batch, true).await.expect("write");
    drop(db);
    assert_eq!(
        listing(env.sim(), &dir).await,
        created,
        "finishing a creation should leave what an uninterrupted one does"
    );
}

#[compio::test]
async fn an_unfinished_creation_is_finished_on_a_real_disk() {
    // The leftovers are recognised by name, so the names a real directory
    // lists are the ones that matter: anything `FileFs` shows that `SimFs`
    // does not would make every leftover look like a database.
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db");
    let db = roble::open_with(
        &roble::DefaultEnv,
        &dir,
        Options::new().create_if_missing(true),
    )
    .await
    .expect("create");
    drop(db);
    std::fs::remove_file(dir.join("marker.manifest.000001.MANIFEST-000001")).expect("remove");

    roble::open_with(
        &roble::DefaultEnv,
        &dir,
        Options::new().create_if_missing(true),
    )
    .await
    .expect("the leftovers of a creation are finished");
    assert!(
        dir.join("marker.manifest.000001.MANIFEST-000001").exists(),
        "a database in it again"
    );
}

#[compio::test]
async fn creation_will_not_write_over_a_log_that_holds_a_write() {
    // The same files, but a write reached the log before the marker went
    // missing. That is a database, whatever it lacks, and creating one in
    // its place would empty the log of a write that was acknowledged.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("create");
    let mut batch = Batch::new();
    batch.put(b"key", b"value").expect("put");
    db.write(&mut batch, true).await.expect("write");
    drop(db);
    env.sim()
        .remove(&dir.join("marker.manifest.000001.MANIFEST-000001"))
        .await
        .expect("remove");

    let err = roble::open_with(&env, &dir, creating())
        .await
        .expect_err("a log holding a write is not a creation's leftovers");
    assert!(
        err.to_string().contains("marker.format-version.000001.016"),
        "the refusal should name what is in the way: {err}"
    );
}

#[compio::test]
async fn creation_will_not_write_over_a_database_missing_its_marker() {
    // A database whose writes were flushed: they are in a table, and the log
    // a creation would find empty holds none of them. What tells it from a
    // creation's leftovers is the table, a name no creation writes, and
    // creating over it would orphan the table and lose the write.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("create");
    let mut batch = Batch::new();
    batch.put(b"key", b"value").expect("put");
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");
    drop(db);
    let fs: &SimFs = env.sim();
    let names = listing(fs, &dir).await;
    let manifest_marker = names
        .iter()
        .find(|n| n.starts_with("marker.manifest."))
        .expect("a manifest marker")
        .clone();
    fs.remove(&dir.join(manifest_marker)).await.expect("remove");
    let before = listing(fs, &dir).await;
    assert!(
        before.iter().any(|n| n.ends_with(".sst")),
        "the write should be in a table: {before:?}"
    );
    // So that it is the table that refuses this, and not a log with a write
    // in it, which the test above is for.
    if before.iter().any(|n| n == "000002.log") {
        let log = fs
            .open(&dir.join("000002.log"), roble::Access::ReadOnly)
            .await
            .expect("open log");
        assert_eq!(log.len().await.expect("len"), 0, "the flushed log");
    }

    let err = roble::open_with(&env, &dir, creating())
        .await
        .expect_err("a database missing its marker is not a directory to create in");
    assert!(
        err.to_string().contains("marker.format-version"),
        "the refusal should name what is in the way: {err}"
    );
    assert_eq!(
        listing(fs, &dir).await,
        before,
        "a refused creation should have written nothing"
    );
}

#[compio::test]
async fn creating_does_not_touch_a_database_that_is_already_there() {
    // `create_if_missing` is about the missing case and nothing else: an
    // open that finds a database opens it, keys and all.
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-reads");
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

    let before = listing(fs, &dir).await;
    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("open");
    assert!(
        db.get(b"a0", Comparison::Equal)
            .await
            .expect("get")
            .is_some(),
        "the fixture's keys should still be readable"
    );
    assert_eq!(
        listing(fs, &dir).await,
        before,
        "the directory should hold exactly what it did: no MANIFEST and no \
         markers of a creation's own"
    );
}

#[compio::test]
async fn pebble_numbers_the_first_key_of_a_created_database_ten() {
    // Where the number above comes from. `db-created` is what
    // `harness/db-fixture` gets when Pebble creates a database and writes one
    // key to it, and the dump beside it is Pebble's own reader describing the
    // result — so a Pebble release that moved `base.SeqNumStart` would land
    // here, on a fixture that no longer matches, rather than in a roble that
    // had quietly diverged.
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-created");
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db-created");
    std::fs::create_dir(&dir).expect("create");
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), dir.join(entry.file_name())).expect("copy");
    }

    let db = roble::open(&roble::DefaultEnv, &dir)
        .await
        .expect("open the database Pebble created");
    let expected = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-created.dump"),
    )
    .expect("read dump");
    assert_eq!(db.dump().expect("dump"), expected);

    let table = db.versions().version.level(0)[0].clone();
    assert_eq!(
        (table.smallest_seq_num, table.largest_seq_num),
        (10, 10),
        "the one key Pebble wrote should be numbered 10"
    );
}

#[compio::test]
async fn a_created_database_hands_out_numbers_past_the_files_it_holds() {
    // The hazard §5 names: a file number handed out twice. The creation
    // spends 1 on the MANIFEST and 2 on the log, so the next number has to be
    // 3 — give it 2 and the first flush writes `000002.sst` beside
    // `000002.log`, which is a directory Pebble would never write.
    // `tests/checkpoint.rs` makes the same check of a checkpoint.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("create");

    let next = db.versions().next_file_num;
    let held: Vec<u64> = listing(env.sim(), &dir)
        .await
        .iter()
        .filter_map(|n| roble_format::files::parse_filename(n))
        .map(|p| p.file_num)
        .collect();
    assert!(
        held.iter().all(|&n| n < next),
        "the next file number {next} should be past every one of {held:?}"
    );

    let mut batch = Batch::new();
    batch.put(b"/a", b"v").expect("put");
    db.write(&mut batch, false).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");
    assert!(
        !held.contains(&table),
        "and the first flush should not reuse one of {held:?}, but took {table}"
    );
}

#[compio::test]
async fn an_open_told_not_to_create_does_not() {
    // The flag is read rather than assumed on: `create_if_missing(false)` is
    // the same as not asking, which is what makes it a setting a caller can
    // turn back off.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    env.sim().mkdir_all(&dir).await.expect("mkdir");

    roble::open_with(&env, &dir, Options::new().create_if_missing(false))
        .await
        .expect_err("an open told not to create should not have created one");
    assert!(
        !listing(env.sim(), &dir)
            .await
            .iter()
            .any(|n| n.starts_with("MANIFEST")),
        "and should have left the directory without one"
    );
}

#[compio::test]
async fn a_created_database_survives_a_crash() {
    // What the syncs are for. A creation that has returned has to be a
    // database afterwards, not merely before — so a crash the moment it
    // returns may take nothing with it. The simulated filesystem drops every
    // write no `sync` covered and every name no `sync_dir` covered, which is
    // what makes the claim checkable at all.
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, creating())
        .await
        .expect("create");
    drop(db);

    env.sim().crash();

    let db = roble::open(&env, &dir)
        .await
        .expect("the created database should have survived the crash");
    assert!(
        db.versions().version.levels().all(|(_, t)| t.is_empty()),
        "as the empty database it was"
    );
}

#[compio::test]
async fn a_half_written_creation_never_reads_as_a_database() {
    // The same claim `tests/checkpoint.rs` makes about a checkpoint, and the
    // reason both write the manifest marker last: it is the marker that makes
    // a directory read as a database, so until everything it names is there
    // it must not be written. An open interrupted anywhere may leave rubbish
    // behind — the directory was the caller's to ask for and can be asked for
    // again — but never rubbish that claims to be a database.
    let total = {
        let env = SimEnv::new();
        let before = env.sim().ops();
        let db = roble::open_with(&env, Path::new("/db"), creating())
            .await
            .expect("create");
        drop(db);
        env.sim().ops() - before
    };

    let mut saw_one_marker = 0;
    for n in 1..=total {
        let env = SimEnv::new();
        let dir = PathBuf::from("/db");
        env.sim().fail_at(n);
        let _ = roble::open_with(&env, &dir, creating()).await;
        env.sim().stop_failing();

        let names = listing(env.sim(), &dir).await;
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
        if manifest {
            roble::open(&env, &dir)
                .await
                .unwrap_or_else(|e| panic!("failing at {n} of {total}, the created database: {e}"));
        }
        // And whatever it left, the next open that may create gets a
        // database from it: an interrupted creation is one to finish, not a
        // directory no open will touch again.
        roble::open_with(&env, &dir, creating())
            .await
            .unwrap_or_else(|e| panic!("failing at {n} of {total}, then creating again: {e}"));
    }
    assert!(
        saw_one_marker > 0,
        "no failure landed between the two markers, so the order is untested"
    );
}
