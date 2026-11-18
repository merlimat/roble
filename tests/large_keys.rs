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

//! Keys far larger than a block, a log block, or anything Oxia writes.
//!
//! Every other test writes keys of a few bytes, so a data block holds many
//! of them, an index separator is short, and a MANIFEST edit fits in one log
//! record chunk. Pebble's `testdata/large_keys` and
//! `TestCrashDuringManifestWrite_LargeKeys` write keys sharing a megabyte of
//! prefix, which turns each of those around: a block holds one key or two, a
//! separator is a megabyte, a table's bounds
//! are a megabyte each, and the edit that records them spans dozens of the
//! MANIFEST's 32 KiB blocks. These are the same keys, Pebble's, held to a
//! `BTreeMap` through flushes, compactions that split between them, a
//! MANIFEST rotation and reopens; and the MANIFEST torn at every block
//! boundary inside such an edit.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, Wal, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// The shared prefix, as long as Pebble's: `a` and a million `p`s.
fn prefix() -> Vec<u8> {
    let mut p = vec![b'a'];
    p.resize(1_000_001, b'p');
    p
}

/// Pebble's keys from `testdata/large_keys`: the prefix and a word.
fn large(word: &str) -> Vec<u8> {
    [prefix(), word.as_bytes().to_vec()].concat()
}

const WORDS: [&str; 29] = [
    "arition",
    "alling",
    "eal",
    "ellate",
    "endectomy",
    "etizers",
    "etizing",
    "laude",
    "lauding",
    "le",
    "les",
    "letini",
    "letinis",
    "lebottomjeans",
    "lication",
    "ly",
    "lying",
    "ollo",
    "raisal",
    "raisals",
    "raiser",
    "raisers",
    "raising",
    "rentice",
    "rentices",
    "renticing",
    "roval",
    "rovals",
    "rove",
];

async fn create(env: &SimEnv, dir: &Path, options: Options<SimFs>) -> roble::Db<SimFs> {
    roble::open_with(env, dir, options.create_if_missing(true))
        .await
        .expect("create")
}

async fn write(db: &roble::Db<SimFs>, batch: &mut Batch) {
    db.write_raw(batch, true).await.expect("write");
}

/// What the model says each comparison answers for `probe`.
fn expected(model: &Model, probe: &[u8], how: Comparison) -> Option<(Vec<u8>, Vec<u8>)> {
    let p = probe.to_vec();
    let pair = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
    match how {
        Comparison::Equal => model.get(&p).map(|v| (p.clone(), v.clone())),
        Comparison::Floor => model.range(..=p).next_back().map(pair),
        Comparison::Ceiling => model.range(p..).next().map(pair),
        Comparison::Lower => model.range(..p).next_back().map(pair),
        Comparison::Higher => model
            .range((Bound::Excluded(p), Bound::Unbounded))
            .next()
            .map(pair),
    }
}

/// A key, shortened for a failure message: a million `p`s say nothing.
fn show(key: &[u8]) -> String {
    let p = prefix();
    match key.strip_prefix(p.as_slice()) {
        Some(rest) => format!("a(p,1000000){}", String::from_utf8_lossy(rest)),
        None => format!("{:?}", String::from_utf8_lossy(key)),
    }
}

/// Every comparison of every key, of the keys either side of each, and of
/// the prefix and the keys around it, and both scans end to end, against
/// the model.
async fn check(db: &roble::Db<SimFs>, model: &Model, when: &str) {
    let p = prefix();
    let mut probes = vec![
        b"a".to_vec(),
        b"b".to_vec(),
        p.clone(),
        [p.clone(), vec![0]].concat(),
    ];
    for word in WORDS {
        let k = large(word);
        let mut before = k.clone();
        *before.last_mut().unwrap() -= 1;
        probes.extend([[k.clone(), vec![0]].concat(), before, k]);
    }
    for probe in &probes {
        for how in [
            Comparison::Equal,
            Comparison::Floor,
            Comparison::Ceiling,
            Comparison::Lower,
            Comparison::Higher,
        ] {
            let found = db
                .get(probe, how)
                .await
                .unwrap_or_else(|e| panic!("{when}: {how:?} of {}: {e}", show(probe)))
                .map(|f| (f.key, f.value));
            let want = expected(model, probe, how);
            assert!(
                found == want,
                "{when}: {how:?} of {} found {:?}, the model {:?}",
                show(probe),
                found.as_ref().map(|(k, _)| show(k)),
                want.as_ref().map(|(k, _)| show(k)),
            );
        }
    }

    let mut scan = db.range(..).expect("scan");
    scan.first().await.expect("first");
    let mut forward = Vec::new();
    while scan.is_valid() {
        forward.push((scan.key().to_vec(), scan.value().to_vec()));
        scan.next().await.expect("next");
    }
    let mut backward = Vec::new();
    scan.last().await.expect("last");
    while scan.is_valid() {
        backward.push((scan.key().to_vec(), scan.value().to_vec()));
        scan.prev().await.expect("prev");
    }
    backward.reverse();
    let all: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert!(
        forward == all,
        "{when}: a forward scan disagrees with the model"
    );
    assert!(
        backward == all,
        "{when}: a reverse scan disagrees with the model"
    );
}

#[compio::test]
async fn keys_far_larger_than_a_block_read_back_as_a_btreemap_would() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    // Pebble's target file size for these keys: a few of them to a table,
    // so a compaction's output splits between megabyte keys.
    let options = || {
        Options::new()
            .disable_automatic_compactions(true)
            .target_file_size(4_000_000)
    };
    let db = create(&env, &dir, options()).await;
    let mut model = Model::new();

    let mut batch = Batch::new();
    for word in WORDS {
        batch.put(&large(word), b"value").expect("put");
        model.insert(large(word), b"value".to_vec());
    }
    write(&db, &mut batch).await;
    check(&db, &model, "in the memtable").await;
    db.flush().await.expect("flush").expect("a table");
    check(&db, &model, "flushed").await;

    // Over the first table: an overwrite, deletes, and a range deletion
    // whose bounds are megabyte keys themselves.
    let mut batch = Batch::new();
    batch.put(&large("eal"), b"again").expect("put");
    model.insert(large("eal"), b"again".to_vec());
    for word in ["le", "rove", "arition"] {
        batch.delete(&large(word)).expect("delete");
        model.remove(&large(word));
    }
    batch
        .delete_range(&large("lication"), &large("raisal"))
        .expect("delete_range");
    model.retain(|k, _| !(large("lication") <= *k && *k < large("raisal")));
    write(&db, &mut batch).await;
    db.flush().await.expect("flush").expect("a table");
    check(&db, &model, "after the deletions").await;

    let l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    let out = db.compact(0, &l0).await.expect("compact into L1");
    assert!(
        out.len() >= 2,
        "the merge should split between megabyte keys, wrote {out:?}"
    );
    check(&db, &model, "compacted into L1").await;
    let below = db.compact(1, &out).await.expect("compact into L2");
    check(&db, &model, "compacted into L2").await;

    // The MANIFEST written from nothing: one edit naming every table, each
    // with megabyte bounds, which spans many of the log's blocks.
    db.rotate_manifest().await.expect("rotate");
    drop(db);
    let db = roble::open_with(&env, &dir, options())
        .await
        .expect("reopen");
    check(&db, &model, "reopened after the rotation").await;
    assert_eq!(
        db.versions()
            .version
            .level(2)
            .iter()
            .map(|t| t.file_num)
            .collect::<Vec<_>>(),
        below,
        "the rotated MANIFEST should name the tables the compaction wrote"
    );

    // And a crash, with the last writes only in the log.
    let mut batch = Batch::new();
    batch.put(&large("ollo"), b"logged").expect("put");
    model.insert(large("ollo"), b"logged".to_vec());
    write(&db, &mut batch).await;
    drop(db);
    env.sim().crash();
    let db = roble::open_with(&env, &dir, options())
        .await
        .expect("reopen after a crash");
    check(&db, &model, "replayed after a crash").await;
}

/// The name of the store's MANIFEST.
async fn manifest(env: &SimEnv, dir: &Path) -> PathBuf {
    let names: Vec<PathBuf> = env
        .sim()
        .list(dir)
        .await
        .expect("list")
        .into_iter()
        .map(|e| e.name)
        .filter(|n| n.to_string_lossy().starts_with("MANIFEST-"))
        .collect();
    let [name] = &names[..] else {
        panic!("one MANIFEST was expected, found {names:?}");
    };
    dir.join(name)
}

/// Replaces a file's contents, durably.
async fn put_file(fs: &SimFs, path: &Path, bytes: &[u8]) {
    let disk = fs.create(path).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(bytes.to_vec()), 0, ()).await;
    result.expect("write");
    disk.sync(()).await.expect("sync");
    disk.close().await.expect("close");
    fs.sync_dir(path.parent().expect("a directory"))
        .await
        .expect("sync dir");
}

/// Two megabyte keys, written to the log and flushed. The flush's edit names
/// a table whose bounds are both of them, so its record runs to some two
/// megabytes, sixty-odd of the MANIFEST's 32 KiB blocks.
async fn write_and_flush(env: &SimEnv, dir: &Path, flush: bool) -> Model {
    let db = create(
        env,
        dir,
        Options::new()
            .disable_automatic_compactions(true)
            .wal(Wal::Synced),
    )
    .await;
    let mut model = Model::new();
    let mut batch = Batch::new();
    for word in ["alling", "rove"] {
        batch.put(&large(word), word.as_bytes()).expect("put");
        model.insert(large(word), word.as_bytes().to_vec());
    }
    write(&db, &mut batch).await;
    if flush {
        db.flush().await.expect("flush").expect("a table");
    }
    model
}

#[compio::test]
async fn a_manifest_torn_anywhere_in_a_large_edit_opens_to_the_edit_before_it() {
    // What `TestCrashDuringManifestWrite_LargeKeys` reaches by crashing at
    // random during MANIFEST writes, laid out exhaustively. The simulated
    // filesystem's crash keeps a write whole or drops it, so the tear is
    // made by hand: the MANIFEST as it was before the flush, plus each
    // prefix of the edit the flush appended, cut at and either side of
    // every 32 KiB block boundary and the chunk headers after them. The log
    // the flush would have retired is still there, as it would be, since
    // the edit that retires it never became durable. Every cut must open,
    // to the version before the edit, with the keys replayed from the log.
    let dir = PathBuf::from("/db");

    // The whole edit, from a flush that finished.
    let finished = SimEnv::new();
    let model = write_and_flush(&finished, &dir, true).await;
    let full = finished
        .sim()
        .durable(&manifest(&finished, &dir).await)
        .expect("the MANIFEST");

    // The store before it: the keys in the log, the MANIFEST without the edit.
    let before = SimEnv::new();
    assert_eq!(write_and_flush(&before, &dir, false).await, model);
    before.sim().crash();
    let path = manifest(&before, &dir).await;
    let start = before.sim().durable(&path).expect("the MANIFEST").len();
    assert!(
        full.len() > start + 32 * 32 * 1024
            && full.starts_with(&before.sim().durable(&path).unwrap()),
        "the flush should have appended one large edit, {} bytes after {start}",
        full.len() - start
    );

    const BLOCK: usize = 32 * 1024;
    let mut cuts: Vec<usize> = (start / BLOCK + 1..=full.len() / BLOCK)
        .flat_map(|b| {
            let at = b * BLOCK;
            [at - 1, at, at + 1, at + 6, at + 7, at + 8]
        })
        .chain([start + 1, start + 7, start + 100, full.len() - 1])
        .filter(|c| *c > start && *c < full.len())
        .collect();
    cuts.sort_unstable();
    cuts.dedup();

    for cut in cuts {
        let env = SimEnv::new();
        for name in before.sim().list(&dir).await.expect("list") {
            let from = dir.join(&name.name);
            let bytes = before.sim().durable(&from).expect("a file");
            let bytes = if from == path {
                full[..cut].to_vec()
            } else {
                bytes
            };
            put_file(env.sim(), &from, &bytes).await;
        }
        let db = roble::open_with(
            &env,
            &dir,
            Options::new().disable_automatic_compactions(true),
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "cut at {cut} of {}: the store did not open: {e}",
                full.len()
            )
        });
        assert!(
            db.versions().version.level(0).is_empty(),
            "cut at {cut}: a torn edit was applied"
        );
        for (k, v) in &model {
            let found = db.get(k, Comparison::Equal).await.expect("get");
            assert!(
                found.map(|f| f.value).as_ref() == Some(v),
                "cut at {cut}: {} did not come back from the log",
                show(k)
            );
        }
        // And it can be written to and opened again: the edits after the
        // tear go to a fresh MANIFEST, not after the torn bytes.
        db.flush().await.expect("flush").expect("a table");
        drop(db);
        let db = roble::open(&env, &dir)
            .await
            .unwrap_or_else(|e| panic!("cut at {cut}: the store did not open a second time: {e}"));
        assert_eq!(
            db.versions().version.level(0).len(),
            1,
            "cut at {cut}: the flush was lost"
        );
    }
}
