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

//! Rotating the MANIFEST, and moving the marker that names it.
//!
//! The claim a rotation makes is narrow and total: the new file, replayed on
//! its own, is the same database. So most of what follows replays it and
//! compares, rather than checking that a file of the right name appeared.
//!
//! The other half is the marker. It is the one-way door, and a crash on
//! either side of it has to leave a database that opens — which is why the
//! rotation is cut at every operation it performs and reopened each time.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Access, Disk, Fs, VersionSet, WriteBuf};
use roble_format::batch::Batch;
use roble_format::files::{self, MARKER_MANIFEST};
use roble_format::manifest::VersionEdit;
use roble_format::record::RecordReader;

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

/// Replays a MANIFEST on its own, the way an open would.
async fn replay(fs: &SimFs, dir: &Path, name: &str) -> VersionSet {
    let file_num = files::parse_filename(name)
        .expect("a MANIFEST name")
        .file_num;
    let disk = fs
        .open(&dir.join(name), Access::ReadOnly)
        .await
        .expect("open the manifest");
    let bytes = roble::env::read_all(&disk, ()).await.expect("read");
    let mut reader = RecordReader::new(&bytes, file_num as u32);
    let mut edits = Vec::new();
    while let Some(record) = reader.next() {
        edits.push(VersionEdit::decode(&record.expect("a record")).expect("an edit"));
    }
    VersionSet::replay(&edits).expect("replay")
}

/// Everything a version set says, rendered so two can be compared.
fn shape(v: &VersionSet) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "comparer {}", v.comparer_name);
    let _ = writeln!(out, "min_unflushed_log_num {}", v.min_unflushed_log_num);
    let _ = writeln!(out, "next_file_num {}", v.next_file_num);
    let _ = writeln!(out, "last_seq_num {}", v.last_seq_num);
    for (level, tables) in v.version.levels() {
        for t in tables {
            let _ = writeln!(out, "L{level} {t:?}");
        }
    }
    out
}

/// The current manifest marker in a listing.
fn manifest_marker(names: &[String]) -> String {
    files::current_marker(names.iter().map(String::as_str), MARKER_MANIFEST)
        .expect("markers parse")
        .expect("a manifest marker")
        .filename
}

#[compio::test]
async fn a_rotated_manifest_replays_to_the_same_database() {
    // The whole claim, stated directly: the file rotation writes, replayed by
    // itself, is the version set it was built from. Not "a MANIFEST
    // appeared" — the same database.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // Something to snapshot beyond what the fixture holds, so the comparison
    // is not between two copies of the same untouched version.
    let mut batch = Batch::new();
    batch.put(b"rotate-me", b"please").expect("put");
    batch.delete_range(b"b2", b"b4").expect("delete_range");
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");

    let before = shape(&db.versions());
    let spent = db.versions().next_file_num;
    let old_manifest = db.manifest_filename();
    let old_marker = manifest_marker(&listing(env.sim(), &dir).await);

    let new_manifest = db.rotate_manifest().await.expect("rotate");
    assert_ne!(
        new_manifest, old_manifest,
        "rotation should start a new file"
    );
    assert_eq!(
        db.manifest_filename(),
        new_manifest,
        "and the store should be appending to it"
    );

    // The same database, down to the one thing a rotation does change: it
    // spends a file number on the MANIFEST it just wrote.
    let expected = before.replace(
        &format!("next_file_num {spent}\n"),
        &format!("next_file_num {}\n", spent + 1),
    );
    assert_ne!(expected, before, "the file number should have moved");
    let replayed = replay(env.sim(), &dir, &new_manifest).await;
    assert_eq!(
        shape(&replayed),
        expected,
        "the rotated MANIFEST replays to a different database"
    );
    assert_eq!(
        shape(&db.versions()),
        expected,
        "and the store in memory should say the same"
    );

    // The marker moved, and to the new file.
    let marker = manifest_marker(&listing(env.sim(), &dir).await);
    assert_ne!(marker, old_marker, "the marker should have moved");
    assert_eq!(
        files::parse_marker(&marker).expect("parse").value,
        new_manifest,
        "and should name the new MANIFEST"
    );
    assert!(
        files::parse_marker(&marker).expect("parse").iter
            > files::parse_marker(&old_marker).expect("parse").iter,
        "at a higher iteration, or the old one would still win"
    );
}

#[compio::test]
async fn a_rotation_survives_being_reopened() {
    let (env, dir) = load("db-reads").await;
    let expected;
    let rotated;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        let mut batch = Batch::new();
        batch.put(b"across", b"the rotation").expect("put");
        db.write(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
        rotated = db.rotate_manifest().await.expect("rotate");
        expected = contents(&db).await;

        // A write after the rotation has to land in the new MANIFEST, which
        // is the thing an in-memory update left half-done would break.
        let mut batch = Batch::new();
        batch.put(b"after", b"the rotation").expect("put");
        db.write(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
    }

    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        db.manifest_filename(),
        rotated,
        "the reopen should follow the marker to the rotated MANIFEST"
    );
    let mut after = expected.clone();
    after.insert(b"after".to_vec(), b"the rotation".to_vec());
    assert_eq!(
        contents(&db).await,
        after,
        "a flush after the rotation went somewhere the reopen could not see"
    );
}

#[compio::test]
async fn a_rotation_collects_what_it_replaced() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let old_manifest = db.manifest_filename();
    let old_marker = manifest_marker(&listing(env.sim(), &dir).await);

    let new_manifest = db.rotate_manifest().await.expect("rotate");
    let during = listing(env.sim(), &dir).await;
    assert!(
        during.contains(&old_manifest) && during.contains(&old_marker),
        "both should still be there until the sweep: {during:?}"
    );

    let removed = db.remove_obsolete_files().await.expect("sweep");
    let after = listing(env.sim(), &dir).await;
    assert!(
        removed.contains(&old_manifest) && removed.contains(&old_marker),
        "the sweep should collect both, but took {removed:?}"
    );
    assert!(
        after.contains(&new_manifest),
        "and leave the one the marker names: {after:?}"
    );
    assert_eq!(
        db.markers().len(),
        2,
        "the marker list should have moved with the directory: {:?}",
        db.markers()
    );
    assert!(
        db.markers().iter().any(|m| *m == manifest_marker(&after)),
        "and hold the current manifest marker: {:?}",
        db.markers()
    );
}

#[compio::test]
async fn a_flush_rotates_a_manifest_that_outgrows_its_limit() {
    // The automatic trigger. 128 MiB of MANIFEST is not something a test can
    // write, so the limit comes down instead; what is under test is that a
    // flush notices and rotates, not the number.
    let (env, dir) = load("db-reads").await;
    let options = roble::Options::new().max_manifest_bytes(8 * 1024);
    let db = roble::open_with(&env, &dir, options).await.expect("open");
    let first = db.manifest_filename();

    let mut rounds = 0;
    while db.manifest_filename() == first {
        let mut batch = Batch::new();
        batch
            .put(format!("grow-{rounds:04}").as_bytes(), b"v")
            .expect("put");
        db.write(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
        rounds += 1;
        assert!(rounds < 500, "the MANIFEST never rotated");
    }

    // The file it rotated away from is gone, swept by the same flush.
    let after = listing(env.sim(), &dir).await;
    assert!(
        !after.contains(&first),
        "the flush that rotated should also have collected {first}: {after:?}"
    );
    // And the new one is small again: a snapshot, not a copy of the old file.
    let replayed = replay(env.sim(), &dir, &db.manifest_filename()).await;
    assert_eq!(
        shape(&replayed),
        shape(&db.versions()),
        "the rotated MANIFEST replays to a different database"
    );

    // Everything written before the rotation is still readable afterwards.
    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    for i in 0..rounds {
        assert_eq!(
            db.get(format!("grow-{i:04}").as_bytes(), roble::Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(b"v".to_vec()),
            "round {i} went missing across the rotation"
        );
    }
}

#[compio::test]
async fn a_rotation_cut_at_any_point_leaves_the_database_openable() {
    // §12: a half-rotated marker must leave the database openable. The
    // dangerous moment is between creating the new marker and removing the
    // old, and the dangerous mistake is doing those in an order where a crash
    // can leave neither.
    let cut_points = {
        let (env, dir) = load("db-simple").await;
        let db = roble::open(&env, &dir).await.expect("open");
        let before = env.sim().ops();
        db.rotate_manifest().await.expect("rotate");
        env.sim().ops() - before
    };
    assert!(
        cut_points > 4,
        "a rotation takes {cut_points} operations, too few to be the whole of one"
    );

    let mut cut_before_the_door = 0;
    for n in 1..=cut_points {
        let (env, dir) = load("db-simple").await;
        let expected;
        let was;
        {
            let db = roble::open(&env, &dir).await.expect("open");
            let mut batch = Batch::new();
            batch.put(b"cut", b"here").expect("put");
            db.write(&mut batch, true).await.expect("write");
            expected = contents(&db).await;
            was = db.manifest_filename();

            env.sim().fail_at(n);
            let _ = db.rotate_manifest().await;
            env.sim().stop_failing();
        }
        env.sim().crash();

        let orphaned = listing(env.sim(), &dir).await;
        let db = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("reopen after a rotation cut at operation {n}: {e}"));
        assert_eq!(
            contents(&db).await,
            expected,
            "a rotation cut at operation {n} changed an answer"
        );
        // Whichever MANIFEST is current, it is the one the marker names and
        // the only one left on disk.
        let after = listing(env.sim(), &dir).await;
        let manifests: Vec<&String> = after
            .iter()
            .filter(|n| {
                files::parse_filename(n).is_some_and(|p| p.file_type == files::FileType::Manifest)
            })
            .collect();
        assert_eq!(
            manifests,
            vec![&db.manifest_filename()],
            "a rotation cut at operation {n} left {manifests:?} behind"
        );
        assert_eq!(
            files::parse_marker(&manifest_marker(&after))
                .expect("parse")
                .value,
            db.manifest_filename(),
            "the marker and the MANIFEST disagree after a cut at operation {n}"
        );
        // Every cut lands *before* the door, because the door is the last
        // thing a rotation does: the directory sync that makes the new
        // marker's name durable. A rotation that moved the marker earlier
        // would show up right here, as a cut whose reopen followed a marker
        // to a MANIFEST that was never finished.
        assert_eq!(
            db.manifest_filename(),
            was,
            "a rotation cut at operation {n} still moved the marker"
        );
        if manifests.len() < count_manifests(&orphaned) {
            cut_before_the_door += 1;
        }
    }
    assert!(
        cut_before_the_door > 0,
        "no cut left a half-written MANIFEST for the reopen to collect, so this \
         test proves nothing"
    );
}

/// How many MANIFESTs a listing holds.
fn count_manifests(names: &[String]) -> usize {
    names
        .iter()
        .filter(|n| {
            files::parse_filename(n).is_some_and(|p| p.file_type == files::FileType::Manifest)
        })
        .count()
}
