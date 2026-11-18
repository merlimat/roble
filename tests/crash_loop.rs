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

//! A real process, on the real filesystem, killed at random.
//!
//! Every other crash test runs on `SimFs`, which fails and crashes on
//! command but is not the code a store runs on. This one runs `FileFs` and
//! `FileDisk` — and on Linux, compio's io_uring driver — in a child process
//! with the store's own background flushes and compactions on, small
//! memtables and tables so that they happen constantly, and a MANIFEST
//! small enough to be rotated over and over. The child writes synced batches
//! and records each one the store acknowledged; the parent kills it with
//! SIGKILL at a random moment, sometimes before it has even finished
//! recovering, then opens the store and requires everything acknowledged to
//! be there, in every round so far.
//!
//! A kill is a process crash, not a machine crash: the page cache survives
//! it, so what this tests is not durability, which `SimFs` does, but that
//! no moment a process can die at — mid-write, mid-flush, mid-compaction,
//! mid-rotation, mid-sweep, mid-recovery — leaves a directory the store
//! cannot open or one that has lost or invented a write.
//!
//! `ROBLE_CRASH_LOOP_ROUNDS` sets how many kills (20 by default), and
//! `ROBLE_CRASH_LOOP_SEED` replays a run's timings.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

/// Set in the child: the directory and the round, as `dir|round`.
const CHILD: &str = "ROBLE_CRASH_LOOP_CHILD";
/// This test's name, which the child is run as.
const NAME: &str = "a_store_killed_at_random_reopens_with_every_write_it_acknowledged";

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn options() -> roble::Options {
    roble::Options::new()
        .create_if_missing(true)
        .wal(roble::Wal::Synced)
        .memtable_size(64 << 10)
        .l0_compaction_threshold(2)
        .lbase_max_bytes(256 << 10)
        .target_file_size(64 << 10)
        .max_manifest_bytes(16 << 10)
}

fn key(round: u64, i: u64) -> Vec<u8> {
    format!("r{round:04}-{i:08}").into_bytes()
}

fn value(round: u64, i: u64) -> Vec<u8> {
    let mut v = format!("{round}-{i}-").into_bytes();
    v.resize(v.len() + 100, b'x');
    v
}

/// What batch `i` of `round` does, the same in the child that writes it and
/// the model that checks it: a put, a delete of a key two batches back every
/// fifth batch, and a range deletion over ten keys every hundredth.
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
}

fn batch(round: u64, i: u64) -> Vec<Op> {
    let mut ops = vec![Op::Put(key(round, i), value(round, i))];
    if i % 5 == 4 {
        ops.push(Op::Delete(key(round, i - 2)));
    }
    if i % 100 == 99 {
        ops.push(Op::DeleteRange(key(round, i - 30), key(round, i - 20)));
    }
    ops
}

fn apply(model: &mut Model, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Put(k, v) => {
                model.insert(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                model.remove(k);
            }
            Op::DeleteRange(lo, hi) => model.retain(|k, _| !(lo <= k && k < hi)),
        }
    }
}

/// The child: opens the store and writes batches until it is killed,
/// appending to `acked` the number of each one the store acknowledged.
async fn child(spec: &str) {
    let (dir, round) = spec.split_once('|').expect("dir|round");
    let dir = PathBuf::from(dir);
    let round: u64 = round.parse().expect("a round");
    let db = roble::open_with(&roble::DefaultEnv, &dir, options())
        .await
        .expect("the child opens the store");
    let mut acked = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.with_extension(format!("acked-{round}")))
        .expect("the ack file");
    // Until it is killed, which is the only way this ends.
    let mut i = 0u64;
    loop {
        let mut b = db.indexed_batch();
        for op in batch(round, i) {
            match op {
                Op::Put(k, v) => b.put(&k, &v),
                Op::Delete(k) => b.delete(&k),
                Op::DeleteRange(lo, hi) => b.delete_range(&lo, &hi),
            }
            .expect("batch");
        }
        b.commit().await.expect("the child's write");
        // After the store returned, so a line here is an acknowledged
        // write. A process kill leaves the page cache, so no sync is needed.
        writeln!(acked, "{i}").expect("ack");
        i += 1;
    }
}

/// How many batches the child acknowledged before it died.
fn acked(dir: &Path, round: u64) -> u64 {
    std::fs::read_to_string(dir.with_extension(format!("acked-{round}")))
        .map(|s| s.lines().count() as u64)
        .unwrap_or(0)
}

/// The keys of `round`, as the store holds them.
fn round_of(found: &Model, round: u64) -> Model {
    let prefix = format!("r{round:04}-").into_bytes();
    found
        .iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// A small, seeded generator for the kill times: no dependency, and a seed
/// that replays a failing run's timings.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

#[compio::test]
async fn a_store_killed_at_random_reopens_with_every_write_it_acknowledged() {
    if let Ok(spec) = std::env::var(CHILD) {
        child(&spec).await;
        return;
    }

    let rounds: u64 = std::env::var("ROBLE_CRASH_LOOP_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let seed: u64 = std::env::var("ROBLE_CRASH_LOOP_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos() as u64
        })
        | 1;
    eprintln!("crash loop: {rounds} rounds, ROBLE_CRASH_LOOP_SEED={seed}");
    let mut rng = Rng(seed);

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db");
    // Each round's keys as they were found after the round's kill: the
    // acknowledged batches, and perhaps the one in flight when it died.
    let mut settled: Vec<Model> = Vec::new();
    let mut total_acked = 0;
    for round in 0..rounds {
        let mut child = Command::new(std::env::current_exe().expect("this test"))
            .args([NAME, "--exact", "--nocapture", "--test-threads=1", "-q"])
            .env(CHILD, format!("{}|{round}", dir.display()))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the child");
        // Sometimes before the open has finished, which kills a recovery.
        let after = match rng.below(4) {
            0 => rng.below(20),
            _ => 20 + rng.below(400),
        };
        std::thread::sleep(Duration::from_millis(after));
        if let Some(status) = child.try_wait().expect("wait") {
            let mut err = String::new();
            std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut err).ok();
            panic!("seed {seed} round {round}: the child exited by itself, {status}:\n{err}");
        }
        child.kill().expect("kill");
        child.wait().expect("reap");

        let n = acked(&dir, round);
        total_acked += n;
        let db = roble::open_with(
            &roble::DefaultEnv,
            &dir,
            options().disable_automatic_compactions(true),
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "seed {seed} round {round}, killed after {after} ms: the store did not open: {e}"
            )
        });
        let mut found = Model::new();
        let mut scan = db.range(..).expect("scan");
        scan.first().await.expect("first");
        while scan.is_valid() {
            found.insert(scan.key().to_vec(), scan.value().to_vec());
            scan.next().await.expect("next");
        }
        drop(scan);
        // Closed rather than dropped: the reopen may have started a flush of
        // what it replayed, and a dropped store keeps the directory locked
        // until that ends, which nothing here would wait for while the next
        // child tries to open it.
        db.close().await.expect("close");

        // This round: every acknowledged batch, and at most the one after.
        let mut model = Model::new();
        for i in 0..n {
            apply(&mut model, &batch(round, i));
        }
        let mine = round_of(&found, round);
        if mine != model {
            apply(&mut model, &batch(round, n));
            assert!(
                mine == model,
                "seed {seed} round {round}, killed after {after} ms with {n} batches \
                 acknowledged: the store holds {} of the round's keys, which is neither \
                 those batches nor those and the next",
                mine.len()
            );
        }
        settled.push(mine);
        // And every earlier round exactly as it was found.
        for (r, was) in settled.iter().enumerate() {
            assert!(
                round_of(&found, r as u64) == *was,
                "seed {seed} round {round}: round {r}'s keys changed after a later kill"
            );
        }
    }
    eprintln!("crash loop: {rounds} kills, {total_acked} acknowledged batches, all there");
    assert!(
        total_acked > 0,
        "no round acknowledged a write, so nothing was tested"
    );
}
