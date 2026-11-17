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

//! Writes, and reading them back.
//!
//! The model is a `BTreeMap` seeded from the database as Pebble left it — so
//! the starting state is still Pebble's, and only what roble writes on top of
//! it is roble's own. Put, delete and delete-range have exact `BTreeMap`
//! equivalents, which is what makes the comparison worth anything.
//!
//! Nothing here is durable yet: these writes live in the memtable until a
//! flush exists. What is tested is that they are *readable*, and that a scan
//! already open does not see them.

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
        // Synced, and the directory with it: a fixture that was never made
        // durable is discarded by `crash()` along with everything else, and
        // the test would be about the loader rather than about roble.
        disk.sync(()).await.expect("sync");
        disk.close().await.expect("close");
    }
    fs.sync_dir(&dir).await.expect("sync dir");
    (env, dir)
}

/// Everything the database shows, in order.
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

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[compio::test]
async fn what_is_written_is_what_is_read() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // The model starts as the database Pebble left, read through roble's own
    // scan — which `tests/scan.rs` has already pinned against Pebble's.
    let mut model = contents(&db).await;
    assert!(!model.is_empty(), "the fixture should not be empty");

    let users: Vec<Vec<u8>> = (0..10u8)
        .map(|i| format!("k{i}").into_bytes())
        .chain(model.keys().take(8).cloned())
        .collect();

    let mut rng = Rng(20_261_111);
    for round in 0..25 {
        let mut batch = Batch::new();
        for i in 0..6 {
            let k = users[rng.below(users.len() as u64) as usize].clone();
            match rng.below(6) {
                0 => {
                    batch.delete(&k).expect("delete");
                    model.remove(&k);
                }
                1 => {
                    let mut a = users[rng.below(users.len() as u64) as usize].clone();
                    let mut b = users[rng.below(users.len() as u64) as usize].clone();
                    if a > b {
                        std::mem::swap(&mut a, &mut b);
                    }
                    if a != b {
                        batch.delete_range(&a, &b).expect("delete_range");
                        model.retain(|key, _| !(key >= &a && key < &b));
                    }
                }
                _ => {
                    let v = format!("r{round}-{i}").into_bytes();
                    batch.put(&k, &v).expect("put");
                    model.insert(k, v);
                }
            }
        }
        db.write_raw(&mut batch, false).await.expect("write");

        assert_eq!(
            contents(&db).await,
            model,
            "round {round}: the scan and the model disagree"
        );
    }

    // And every key the model holds is found by a point lookup too, which
    // takes a different path: `Equal` keeps its own, for the bloom filter.
    for (k, v) in model.iter().take(40) {
        let found = db.get(k, Comparison::Equal).await.expect("get");
        assert_eq!(
            found.map(|f| f.value),
            Some(v.clone()),
            "get disagrees with the scan about {k:02x?}"
        );
    }
}

#[compio::test]
async fn a_write_shadows_what_a_table_holds() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // A key the fixture holds.
    let existing = contents(&db).await;
    let (key, old) = existing.iter().next().expect("a key");
    let (key, old) = (key.clone(), old.clone());

    let mut batch = Batch::new();
    batch.put(&key, b"written-later").expect("put");
    db.write_raw(&mut batch, false).await.expect("write");

    let found = db.get(&key, Comparison::Equal).await.expect("get");
    assert_eq!(
        found.map(|f| f.value),
        Some(b"written-later".to_vec()),
        "the memtable's version should win over the table's {old:02x?}"
    );

    // And a delete hides it entirely.
    let mut batch = Batch::new();
    batch.delete(&key).expect("delete");
    db.write_raw(&mut batch, false).await.expect("write");
    assert!(
        db.get(&key, Comparison::Equal)
            .await
            .expect("get")
            .is_none(),
        "a delete in the memtable should hide the table's value"
    );
    assert!(
        !contents(&db).await.contains_key(&key),
        "and the scan should not show it either"
    );
}

#[compio::test]
async fn a_range_deletion_in_the_memtable_hides_a_table() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let before = contents(&db).await;

    // A span that covers at least a couple of the fixture's keys.
    let keys: Vec<Vec<u8>> = before.keys().cloned().collect();
    let (start, end) = (keys[1].clone(), keys[4].clone());
    let covered: Vec<Vec<u8>> = keys
        .iter()
        .filter(|k| **k >= start && **k < end)
        .cloned()
        .collect();
    assert!(covered.len() >= 2, "the span should cover something");

    let mut batch = Batch::new();
    batch.delete_range(&start, &end).expect("delete_range");
    db.write_raw(&mut batch, false).await.expect("write");

    let after = contents(&db).await;
    for k in &covered {
        assert!(!after.contains_key(k), "{k:02x?} should be covered");
        assert!(
            db.get(k, Comparison::Equal).await.expect("get").is_none(),
            "get should agree about {k:02x?}"
        );
    }
    assert!(
        after.contains_key(&end),
        "the end of the range is excluded, so it survives"
    );
}

#[compio::test]
async fn a_scan_does_not_see_a_write_that_came_after_it() {
    // §4: iterators are over a snapshot — a sequence number taken when the
    // scan is made, above which nothing is its business.
    //
    // Reaching that rule at all takes some care, and two earlier versions of
    // this test did not. The writes have to land **ahead of where the scan has
    // reached**, or the cursor's position hides them by itself; and the
    // memtable has to be **non-empty when the scan starts**, or its cursor is
    // parked invalid and never looks again. Both versions passed with the
    // snapshot check deleted.
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");

    // Something in the memtable before the scan begins.
    let mut batch = Batch::new();
    batch.put(b"m1", b"first").expect("put");
    batch.put(b"m3", b"first").expect("put");
    db.write_raw(&mut batch, false).await.expect("write");

    let mut scan = db.range(..).expect("scan");
    scan.first().await.expect("first");
    let started_on = scan.key().to_vec();
    assert!(
        started_on.as_slice() < b"m1".as_slice(),
        "the scan must start before the keys it will be shown later"
    );

    // Now write between them — ahead of the cursor, into a memtable the scan
    // is already walking.
    let mut batch = Batch::new();
    batch.put(b"m2", b"later").expect("put");
    batch.put(b"m3", b"changed").expect("put");
    db.write_raw(&mut batch, false).await.expect("write");

    let mut seen = Model::new();
    while scan.is_valid() {
        seen.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }

    assert_eq!(
        seen.get(b"m1".as_slice()).map(|v| v.as_slice()),
        Some(b"first".as_slice()),
        "what was there when the scan began is what it shows"
    );
    assert!(
        !seen.contains_key(b"m2".as_slice()),
        "a key written after the scan began must not appear in it"
    );
    assert_eq!(
        seen.get(b"m3".as_slice()).map(|v| v.as_slice()),
        Some(b"first".as_slice()),
        "and a key rewritten after it began must read as it was"
    );

    // A scan made now sees all of it.
    let fresh = contents(&db).await;
    assert_eq!(
        fresh.get(b"m2".as_slice()).map(|v| v.as_slice()),
        Some(b"later".as_slice())
    );
    assert_eq!(
        fresh.get(b"m3".as_slice()).map(|v| v.as_slice()),
        Some(b"changed".as_slice())
    );
}

#[compio::test]
async fn sequence_numbers_carry_on_from_the_manifest() {
    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let last = db.versions().last_seq_num;

    let mut batch = Batch::new();
    batch.put(b"one", b"1").expect("put");
    batch.put(b"two", b"2").expect("put");
    db.write_raw(&mut batch, false).await.expect("write");

    assert_eq!(
        batch.seq_num(),
        last + 1,
        "a batch commits at the sequence number after the MANIFEST's last"
    );
}

// ------------------------------------------------------------------ durability

#[compio::test]
async fn a_crash_replays_to_the_last_synced_write() {
    // §12's second gate for M3. The rule is exact: a write that was synced is
    // there after the crash, and one that was not is not. Anything looser
    // would be a store that either loses acknowledged writes or pretends to
    // have lost unacknowledged ones.
    let (env, dir) = load("db-reads").await;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        let mut synced = Batch::new();
        synced.put(b"durable-1", b"kept").expect("put");
        synced.put(b"durable-2", b"kept").expect("put");
        db.write_raw(&mut synced, true).await.expect("write");

        let mut unsynced = Batch::new();
        unsynced.put(b"lost-1", b"gone").expect("put");
        db.write_raw(&mut unsynced, false).await.expect("write");

        // Both are readable before the crash: the memtable has them.
        for k in [b"durable-1".as_slice(), b"lost-1"] {
            assert!(
                db.get(k, Comparison::Equal).await.expect("get").is_some(),
                "{k:02x?} should be readable before the crash"
            );
        }
    }

    env.sim().crash();

    let db = roble::open(&env, &dir)
        .await
        .expect("reopen after the crash");
    for k in [b"durable-1".as_slice(), b"durable-2"] {
        assert_eq!(
            db.get(k, Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(b"kept".to_vec()),
            "{k:02x?} was synced, so the crash must not have taken it"
        );
    }
    assert!(
        db.get(b"lost-1", Comparison::Equal)
            .await
            .expect("get")
            .is_none(),
        "an unsynced write should not survive a crash"
    );
}

#[compio::test]
async fn a_replayed_write_keeps_its_sequence_number() {
    // Replay must use the sequence number the batch was committed at, not a
    // fresh one: a key that came back younger than it was written could
    // outlive a delete that should have buried it.
    let (env, dir) = load("db-reads").await;
    let key: Vec<u8>;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        key = contents(&db).await.keys().next().expect("a key").clone();

        // Overwrite a key the tables hold, then delete it — both logged.
        let mut batch = Batch::new();
        batch.put(&key, b"newer").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");

        let mut batch = Batch::new();
        batch.delete(&key).expect("delete");
        db.write_raw(&mut batch, true).await.expect("write");
    }

    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert!(
        db.get(&key, Comparison::Equal)
            .await
            .expect("get")
            .is_none(),
        "the delete was written after the put, and must still be the newer of the two"
    );
}

#[compio::test]
async fn writes_carry_on_in_the_same_log_after_a_reopen() {
    // The second open has to pick up where the first left off. Writing from
    // the start again would overwrite what was replayed; writing past the end
    // of the valid records would leave a hole.
    let (env, dir) = load("db-reads").await;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        let mut batch = Batch::new();
        batch.put(b"round-1", b"a").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
    }
    {
        let db = roble::open(&env, &dir).await.expect("reopen");
        let mut batch = Batch::new();
        batch.put(b"round-2", b"b").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
    }

    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        db.get(b"round-1", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"a".to_vec()),
        "the first round's write should still be there"
    );
    assert_eq!(
        db.get(b"round-2", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"b".to_vec()),
        "and the second round's"
    );
}

#[compio::test]
async fn a_write_after_a_reopen_outranks_what_replay_brought_back() {
    // §6: the next sequence number never goes backwards. If a reopen started
    // numbering from the MANIFEST's last again, a fresh write would be
    // numbered at or below one replay had just restored, and the older entry
    // would win.
    let (env, dir) = load("db-reads").await;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        let mut batch = Batch::new();
        batch.put(b"contested", b"written-before").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
    }
    env.sim().crash();

    {
        let db = roble::open(&env, &dir).await.expect("reopen");
        assert_eq!(
            db.get(b"contested", Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(b"written-before".to_vec()),
            "replay should have brought it back"
        );
        let mut batch = Batch::new();
        batch.put(b"contested", b"written-after").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
        assert_eq!(
            db.get(b"contested", Comparison::Equal)
                .await
                .expect("get")
                .map(|f| f.value),
            Some(b"written-after".to_vec()),
            "the new write must outrank the replayed one"
        );
    }

    // And still, after another crash and replay of both.
    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        db.get(b"contested", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"written-after".to_vec()),
        "and the ordering must survive being replayed again"
    );
}

#[compio::test]
async fn a_record_that_spans_a_block_survives_a_crash() {
    // A log is framed into 32 KiB blocks and a chunk never crosses one, so a
    // large value is split. Where the second chunk goes depends on how far
    // into the **file** the first one started — a writer that framed against
    // its own buffer instead would put it in the wrong place, and every chunk
    // would still have a valid checksum.
    let (env, dir) = load("db-reads").await;
    let big = vec![b'q'; 40 * 1024];

    {
        let db = roble::open(&env, &dir).await.expect("open");
        // A small record first, so the big one does not start at offset zero.
        let mut batch = Batch::new();
        batch.put(b"small", b"s").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");

        let mut batch = Batch::new();
        batch.put(b"big", &big).expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
    }

    env.sim().crash();
    let db = roble::open(&env, &dir).await.expect("reopen");
    assert_eq!(
        db.get(b"small", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"s".to_vec())
    );
    assert_eq!(
        db.get(b"big", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(big),
        "a record split across blocks must come back whole"
    );
}

#[compio::test]
async fn a_torn_record_in_an_earlier_log_is_refused() {
    // A later log exists only if the earlier one was finished, so damage
    // there is corruption rather than the tail of a crash — and opening
    // without it would discard writes that were acknowledged.
    let (env, dir) = load("db-unflushed-wal").await;
    let fs = env.sim();

    // A second, later log with a record in it, so log 000004 is no longer
    // the last one replayed.
    let mut batch = Batch::new();
    batch.put(b"later", b"v").expect("put");
    batch.set_seq_num(100);
    let mut writer = roble_format::record::RecordWriter::recyclable(5);
    writer.write_record(batch.as_bytes()).expect("frame");
    let framed = writer.finish();
    let disk = fs.create(&dir.join("000005.log")).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(framed), 0, ()).await;
    result.expect("write");
    disk.sync(()).await.expect("sync");
    disk.close().await.expect("close");

    // And cut log 000004 off part-way through its record.
    let disk = fs
        .open(&dir.join("000004.log"), roble::Access::ReadOnly)
        .await
        .expect("open");
    let whole = roble::__private::read_all(&disk, ()).await.expect("read");
    drop(disk);
    let disk = fs.create(&dir.join("000004.log")).await.expect("recreate");
    let (result, _) = disk
        .write(WriteBuf::new(whole[..whole.len() - 4].to_vec()), 0, ())
        .await;
    result.expect("write");
    disk.sync(()).await.expect("sync");
    disk.close().await.expect("close");
    fs.sync_dir(&dir).await.expect("sync dir");

    match roble::open(&env, &dir).await {
        Err(roble::Error::Corrupt(m)) => assert!(
            m.contains("000004.log") && m.contains("A later log exists"),
            "the refusal should name the log and say why it is not a torn tail: {m}"
        ),
        other => panic!("expected the earlier log's damage to be refused, got {other:?}"),
    }
}

#[compio::test]
async fn a_delete_after_a_reopen_buries_what_replay_brought_back() {
    // The sharp edge of "the next sequence number never goes backwards".
    //
    // If a reopen numbered from the MANIFEST's last again, this delete would
    // land on the same sequence number as the put replay had just restored.
    // They are different internal keys — the kind differs — so nothing
    // replaces anything, and within one sequence number `SET`'s kind byte
    // sorts ahead of `DEL`'s. The put would win and the key would stay
    // visible. A reader cannot tell that from a delete that never happened.
    let (env, dir) = load("db-reads").await;

    {
        let db = roble::open(&env, &dir).await.expect("open");
        let mut batch = Batch::new();
        batch.put(b"buried", b"value").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
    }
    env.sim().crash();

    let db = roble::open(&env, &dir).await.expect("reopen");
    assert!(
        db.get(b"buried", Comparison::Equal)
            .await
            .expect("get")
            .is_some(),
        "replay should have brought the put back"
    );

    let mut batch = Batch::new();
    batch.delete(b"buried").expect("delete");
    db.write_raw(&mut batch, true).await.expect("write");
    assert!(
        db.get(b"buried", Comparison::Equal)
            .await
            .expect("get")
            .is_none(),
        "the delete was written later and must bury the replayed put"
    );
}

/// `put`, `delete` and `delete_range` on the store write what a batch of one
/// would, and refuse `sync` on a store with the WAL off as a batch does.
#[compio::test]
async fn single_writes_on_the_store_read_back_as_a_model_would() {
    let (env, dir) = load("db-simple").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let mut model = contents(&db).await;

    db.put(b"/a", b"1", roble::Durability::NoSync)
        .await
        .expect("put");
    db.put(b"/b", b"2", roble::Durability::Sync)
        .await
        .expect("put");
    db.put(b"/c", b"3", roble::Durability::NoSync)
        .await
        .expect("put");
    db.put(b"/d", b"4", roble::Durability::NoSync)
        .await
        .expect("put");
    for (k, v) in [(b"/a", b"1"), (b"/b", b"2"), (b"/c", b"3"), (b"/d", b"4")] {
        model.insert(k.to_vec(), v.to_vec());
    }
    db.delete(b"/a", roble::Durability::NoSync)
        .await
        .expect("delete");
    model.remove(&b"/a"[..]);
    db.delete_range(b"/b", b"/d", roble::Durability::Sync)
        .await
        .expect("delete_range");
    model.retain(|k, _| !(k.as_slice() >= &b"/b"[..] && k.as_slice() < &b"/d"[..]));

    assert_eq!(contents(&db).await, model);
    assert_eq!(
        db.get(b"/d", Comparison::Equal)
            .await
            .expect("get")
            .map(|f| f.value),
        Some(b"4".to_vec())
    );
    drop(db);

    let no_wal = roble::open_with(&env, &dir, roble::Options::new().disable_wal(true))
        .await
        .expect("open without the WAL");
    for refused in [
        no_wal.put(b"/e", b"5", roble::Durability::Sync).await,
        no_wal.delete(b"/d", roble::Durability::Sync).await,
        no_wal
            .delete_range(b"/a", b"/z", roble::Durability::Sync)
            .await,
    ] {
        assert!(
            matches!(refused, Err(roble::Error::Unsupported(_))),
            "a synced write with the WAL off should be refused: {refused:?}"
        );
    }
    assert_eq!(contents(&no_wal).await, model, "and should change nothing");
}
