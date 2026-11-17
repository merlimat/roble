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

//! A store against a `BTreeMap`, over operations nobody chose (DESIGN.md §12).
//!
//! §12 asks for "random sequences of writes, deletes, range deletes, gets
//! under every comparison, scans forward and reverse, flushes, compactions
//! and reopens, checked against a `BTreeMap`". `tests/memtable.rs` does that
//! for the memtable alone; every test of the whole store writes a sequence
//! somebody thought of.
//!
//! That is the difference this is for. A hand-written sequence tests the
//! cases its author imagined, and the ones that break a store are the ones
//! nobody did — a delete of a key that only exists in a table two levels
//! down, a range deletion whose end lands exactly on a flushed boundary, a
//! reopen between a write and the flush that would have made it a table.
//!
//! The alphabet is deliberately small, so that overwrites and deletions pile
//! up on the same keys and a range deletion covers several of them. The
//! flushes, compactions and reopens are frequent for the same reason: a
//! sequence that never flushes is a memtable test, and `tests/memtable.rs`
//! already is one.
//!
//! The seed is fixed, so a failure is reproducible rather than a story about
//! a run nobody has any more.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// A small, reproducible generator: the same seed gives the same run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Sixteen keys, so that writes land on each other and a range covers many.
fn key(n: u64) -> Vec<u8> {
    format!("/oxia/k{:02}", n % 16).into_bytes()
}

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

async fn forward<F: Fs>(db: &roble::Db<F>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    let mut out = Vec::new();
    while scan.is_valid() {
        out.push((scan.key().to_vec(), scan.value().to_vec()));
        scan.next().await.expect("next");
    }
    out
}

async fn backward<F: Fs>(db: &roble::Db<F>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut scan = db.scan().await.expect("scan");
    scan.last().await.expect("last");
    let mut out = Vec::new();
    while scan.is_valid() {
        out.push((scan.key().to_vec(), scan.value().to_vec()));
        scan.prev().await.expect("prev");
    }
    out.reverse();
    out
}

/// What the model says each comparison should answer for `probe`.
fn expected(model: &Model, probe: &[u8], how: Comparison) -> Option<(Vec<u8>, Vec<u8>)> {
    let p = probe.to_vec();
    let pair = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
    match how {
        Comparison::Equal => model.get(&p).map(|v| (p.clone(), v.clone())),
        Comparison::Floor => model.range(..=p).next_back().map(pair),
        Comparison::Ceiling => model.range(p..).next().map(pair),
        Comparison::Lower => model.range(..p).next_back().map(pair),
        Comparison::Higher => model
            .range((std::ops::Bound::Excluded(p), std::ops::Bound::Unbounded))
            .next()
            .map(pair),
    }
}

/// Every comparison of every key in the alphabet, and of keys either side of
/// it, against the model.
async fn check_gets<F: Fs>(db: &roble::Db<F>, model: &Model, step: usize, seed: u64) {
    for n in 0..16u64 {
        for probe in [key(n), [key(n), vec![b'!']].concat()] {
            for how in [
                Comparison::Equal,
                Comparison::Floor,
                Comparison::Ceiling,
                Comparison::Lower,
                Comparison::Higher,
            ] {
                let found = db
                    .get(&probe, how)
                    .await
                    .expect("get")
                    .map(|f| (f.key, f.value));
                assert_eq!(
                    found,
                    expected(model, &probe, how),
                    "seed {seed} step {step}: {how:?} of {:?}",
                    String::from_utf8_lossy(&probe)
                );
            }
        }
    }
}

#[compio::test]
async fn a_store_answers_what_a_btreemap_would() {
    for seed in [1u64, 7, 99, 20_261_111] {
        let mut rng = Rng(seed);
        let (env, dir) = load("db-simple").await;
        // Compact at the first opportunity and write small tables, so that a
        // sequence this short still moves keys between levels.
        let options = || {
            Options::new()
                .l0_compaction_threshold(1)
                .target_file_size(512)
        };

        let mut db = roble::open_with(&env, &dir, options()).await.expect("open");
        let mut model: Model = forward(&db).await.into_iter().collect();

        for step in 0..60usize {
            let mut batch = Batch::new();
            match rng.below(10) {
                0..=4 => {
                    let k = key(rng.next());
                    let v = format!("v{step}").into_bytes();
                    batch.put(&k, &v).expect("put");
                    model.insert(k, v);
                }
                5..=7 => {
                    let k = key(rng.next());
                    batch.delete(&k).expect("delete");
                    model.remove(&k);
                }
                _ => {
                    // A range, always non-empty and in order.
                    let a = rng.below(16);
                    let b = a + 1 + rng.below(4);
                    let (start, end) = (key(a), key(b));
                    if start < end {
                        batch.delete_range(&start, &end).expect("delete_range");
                        model.retain(|k, _| !(*k >= start && *k < end));
                    }
                }
            }
            db.write(&mut batch, true).await.expect("write");

            // Flush, compact and reopen often enough that the answer comes
            // from a different place each time: the memtable, a fresh table,
            // a compacted one, and a replayed log.
            match rng.below(4) {
                0 => {
                    db.flush().await.expect("flush");
                }
                1 => {
                    db.flush().await.expect("flush");
                    db.compact_once().await.expect("compact_once");
                }
                2 => {
                    drop(db);
                    db = roble::open_with(&env, &dir, options())
                        .await
                        .expect("reopen");
                }
                _ => {}
            }

            let scanned = forward(&db).await;
            let want: Vec<(Vec<u8>, Vec<u8>)> =
                model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            assert_eq!(
                scanned, want,
                "seed {seed} step {step}: a forward scan disagrees with the model"
            );
            assert_eq!(
                backward(&db).await,
                want,
                "seed {seed} step {step}: a reverse scan disagrees with the forward one"
            );
            check_gets(&db, &model, step, seed).await;
        }

        // And across one more reopen at the end, so that the last state is
        // read back from disk rather than from what is still in memory.
        drop(db);
        let db = roble::open_with(&env, &dir, options())
            .await
            .expect("final reopen");
        let want: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(
            forward(&db).await,
            want,
            "seed {seed}: a reopen disagrees with the model"
        );
    }
}
