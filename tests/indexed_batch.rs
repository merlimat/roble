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

//! A batch read back through itself.
//!
//! The oracle is a `BTreeMap`, as §12 asks: the batch and the store merged by
//! hand, with the batch winning, is what a read through the batch has to
//! agree with. A skiplist of stand-in sequence numbers reaching the same
//! answer by a different route is the only reason that comparison is worth
//! anything.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, WriteBuf};
use roble_format::batch::Batch;

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

async fn store_contents<F: Fs>(db: &roble::Opened<'_, F>) -> Model {
    walk(&mut db.scan().await.expect("scan")).await
}

#[compio::test]
async fn a_batch_reads_back_what_it_holds() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let store = store_contents(&db).await;
    let existing: Vec<Vec<u8>> = store.keys().cloned().collect();
    assert!(existing.len() > 6, "the fixture should have keys to shadow");

    let mut batch = db.batch();
    batch.put(b"batch-only", b"new").expect("put");
    batch.put(&existing[0], b"shadowed").expect("put");
    batch.delete(&existing[1]).expect("delete");

    // Its own.
    assert_eq!(
        batch
            .get(b"batch-only", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"new".to_vec()),
    );
    // Over the store's.
    assert_eq!(
        batch
            .get(&existing[0], Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"shadowed".to_vec()),
        "the batch's value should win over the table's"
    );
    // And a deletion is an answer too.
    assert_eq!(
        batch
            .get(&existing[1], Comparison::Equal)
            .await
            .expect("get"),
        None,
        "the batch's delete should hide the table's value"
    );
    // Through to the store for everything it does not hold.
    assert_eq!(
        batch
            .get(&existing[2], Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        store.get(&existing[2]).cloned(),
    );

    // None of which the store can see yet.
    assert_eq!(
        store_contents(&db).await,
        store,
        "an uncommitted batch changed the store"
    );
}

#[compio::test]
async fn a_later_entry_wins_over_an_earlier_one() {
    // The reason the stand-in numbers ascend: within one batch the last
    // write for a key is the one that counts, exactly as it is within one
    // committed batch.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut batch = db.batch();
    batch.put(b"k", b"first").expect("put");
    batch.put(b"k", b"second").expect("put");
    batch.put(b"k", b"third").expect("put");
    assert_eq!(
        batch
            .get(b"k", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"third".to_vec()),
    );

    batch.delete(b"k").expect("delete");
    assert_eq!(
        batch.get(b"k", Comparison::Equal).await.expect("get"),
        None,
        "the delete came last, so it wins"
    );

    batch.put(b"k", b"again").expect("put");
    assert_eq!(
        batch
            .get(b"k", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"again".to_vec()),
        "and a put after the delete wins over it"
    );

    // And the committed batch agrees with what reading it through itself
    // said, which is the only claim that matters.
    batch.commit(true).await.expect("commit");
    assert_eq!(
        db.get(b"k", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"again".to_vec()),
    );
}

#[compio::test]
async fn a_range_deletion_in_a_batch_covers_the_store() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let store = store_contents(&db).await;
    let keys: Vec<Vec<u8>> = store.keys().cloned().collect();
    let (start, end) = (keys[1].clone(), keys[4].clone());
    let covered: Vec<Vec<u8>> = keys
        .iter()
        .filter(|k| **k >= start && **k < end)
        .cloned()
        .collect();
    assert!(covered.len() >= 2, "the range should cover something");

    let mut batch = db.batch();
    batch.delete_range(&start, &end).expect("delete_range");

    for k in &covered {
        assert_eq!(
            batch.get(k, Comparison::Equal).await.expect("get"),
            None,
            "{k:02x?} should be covered by the batch's tombstone"
        );
    }
    assert!(
        batch
            .get(&end, Comparison::Equal)
            .await
            .expect("get")
            .is_some(),
        "and the end should still be excluded"
    );

    // A key written after the tombstone, inside its range, is not covered:
    // the stand-in numbers ascend, so the put is newer.
    let inside = covered[0].clone();
    batch.put(&inside, b"after the tombstone").expect("put");
    assert_eq!(
        batch
            .get(&inside, Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"after the tombstone".to_vec()),
    );
}

#[compio::test]
async fn reading_through_a_batch_agrees_with_the_model() {
    // The oracle. The batch and the store merged by hand, with the batch
    // winning, against every comparison and a full scan both ways.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // Something in the *store's* memtable first, tombstone and all, and
    // never flushed. A read through a batch has to honour every memtable's
    // tombstones and not just the batch's own — the store's are the ones
    // that hide keys a table holds.
    let table_keys: Vec<Vec<u8>> = store_contents(&db).await.into_keys().collect();
    let mut uncommitted = Batch::new();
    uncommitted
        .delete_range(&table_keys[11], &table_keys[13])
        .expect("delete_range");
    uncommitted
        .put(b"in-the-memtable", b"not in a table")
        .expect("put");
    db.write(&mut uncommitted, true).await.expect("write");

    let store = store_contents(&db).await;
    assert!(
        !store.contains_key(&table_keys[11]),
        "the store's own tombstone should already have hidden it"
    );
    let keys: Vec<Vec<u8>> = store.keys().cloned().collect();

    let mut model = store.clone();
    let mut batch = db.batch();

    // A spread of writes: new keys before, between and after what the store
    // holds; shadows; deletes; a range.
    for (k, v) in [
        (b"\x00first".as_slice(), b"before everything".as_slice()),
        (b"zzz-last".as_slice(), b"after everything".as_slice()),
    ] {
        batch.put(k, v).expect("put");
        model.insert(k.to_vec(), v.to_vec());
    }
    for k in [&keys[0], &keys[3], &keys[6]] {
        batch.put(k, b"shadowed").expect("put");
        model.insert(k.clone(), b"shadowed".to_vec());
    }
    for k in [&keys[2], &keys[5]] {
        batch.delete(k).expect("delete");
        model.remove(k.as_slice());
    }
    let (start, end) = (keys[7].clone(), keys[9].clone());
    batch.delete_range(&start, &end).expect("delete_range");
    model.retain(|k, _| !(*k >= start && *k < end));

    // A scan, forwards.
    assert_eq!(
        walk(&mut batch.scan().await.expect("scan")).await,
        model,
        "a scan through the batch disagrees with the model"
    );

    // And backwards, which is a different code path through the merge.
    let mut scan = batch.scan().await.expect("scan");
    scan.last().await.expect("last");
    let mut reverse = Model::new();
    while scan.is_valid() {
        reverse.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.prev().await.expect("prev");
    }
    assert_eq!(reverse, model, "a reverse scan disagrees with the model");

    // Every comparison, on every key the model mentions and on the gaps
    // between them.
    let mut probes: Vec<Vec<u8>> = model.keys().cloned().collect();
    probes.extend(keys.iter().cloned());
    probes.push(b"\x00".to_vec());
    probes.push(b"zzzzzz".to_vec());
    probes.sort();
    probes.dedup();

    for probe in &probes {
        for how in [
            Comparison::Equal,
            Comparison::Floor,
            Comparison::Ceiling,
            Comparison::Lower,
            Comparison::Higher,
        ] {
            let got = batch.get(probe, how).await.expect("get");
            let want = expect(&model, probe, how);
            assert_eq!(
                got.map(|f| (f.key, f.value)),
                want,
                "{how:?} of {probe:02x?} through the batch"
            );
        }
    }
}

/// What the model says the answer is.
fn expect(model: &Model, key: &[u8], how: Comparison) -> Option<(Vec<u8>, Vec<u8>)> {
    let pair = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
    match how {
        Comparison::Equal => model.get_key_value(key).map(pair),
        Comparison::Ceiling => model.range(key.to_vec()..).next().map(pair),
        Comparison::Floor => model.range(..=key.to_vec()).next_back().map(pair),
        Comparison::Higher => model
            .range((
                std::ops::Bound::Excluded(key.to_vec()),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .map(pair),
        Comparison::Lower => model.range(..key.to_vec()).next_back().map(pair),
    }
}

#[compio::test]
async fn committing_applies_exactly_what_the_batch_read_back() {
    let (env, dir) = load("db-reads").await;
    let expected;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        let keys: Vec<Vec<u8>> = store_contents(&db).await.into_keys().collect();

        let mut batch = db.batch();
        batch.put(b"committed", b"yes").expect("put");
        batch.put(&keys[0], b"shadowed").expect("put");
        batch.delete(&keys[1]).expect("delete");
        batch
            .delete_range(&keys[4], &keys[6])
            .expect("delete_range");
        expected = walk(&mut batch.scan().await.expect("scan")).await;

        batch.commit(true).await.expect("commit");
        assert_eq!(
            store_contents(&db).await,
            expected,
            "the store after the commit should be what the batch read back"
        );
    }

    // And it is durable, since the commit synced.
    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        store_contents(&db).await,
        expected,
        "the commit was synced, so a crash should not have touched it"
    );
}

#[compio::test]
async fn a_batch_sees_a_write_that_happened_while_it_was_open() {
    // The batch is not a snapshot of the store; it is a layer over whatever
    // the store says now, as Pebble's indexed batch is. What is fixed is
    // that the batch's own entries win.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut batch = db.batch();
    batch.put(b"mine", b"from the batch").expect("put");

    let mut other = Batch::new();
    other.put(b"theirs", b"from the store").expect("put");
    other.put(b"mine", b"from the store").expect("put");
    db.write(&mut other, true).await.expect("write");

    assert_eq!(
        batch
            .get(b"theirs", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"from the store".to_vec()),
        "a write the batch does not shadow should show through"
    );
    assert_eq!(
        batch
            .get(b"mine", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"from the batch".to_vec()),
        "and the batch's own entry should still win, though it was written first"
    );
}

#[compio::test]
async fn put_with_indexes_what_it_wrote() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut batch = db.batch();
    batch
        .put_with(b"marshalled", 5, |buf| buf.copy_from_slice(b"value"))
        .expect("put_with");
    assert_eq!(
        batch
            .get(b"marshalled", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"value".to_vec()),
        "the value written in place should be in the index too"
    );

    batch.commit(true).await.expect("commit");
    assert_eq!(
        db.get(b"marshalled", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"value".to_vec()),
        "and the repr should carry the same bytes"
    );
}

#[compio::test]
async fn an_empty_batch_reads_like_the_store() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let batch = db.batch();
    assert!(batch.is_empty());
    assert_eq!(batch.count(), 0);
    assert_eq!(
        walk(&mut batch.scan().await.expect("scan")).await,
        store_contents(&db).await,
    );
}
