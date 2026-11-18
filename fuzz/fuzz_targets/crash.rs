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

//! A store whose disk fails, keeps a buffer or loses power, at an operation
//! the fuzzer chose, in a sequence the fuzzer chose.
//!
//! `tests/failure.rs` sweeps every operation of one hand-written scenario.
//! Here the scenario is the input too — writes synced or not, flushes and
//! compactions in any order, with the WAL on or off, from any golden
//! database — and so is what goes wrong: one operation fails or withholds
//! its buffer, the machine may then crash, and the recovery may itself be
//! failed partway. The same invariants are held:
//!
//! 1. **It still opens**, and reads, whatever happened.
//! 2. **What survives is a prefix.** The store comes back as it was after
//!    some number of the batches it was given — no fewer than were made
//!    durable (a synced write with the WAL on, a flush either way), and no
//!    more than were attempted. With the WAL off, durability is the flush,
//!    so the state it comes back as is one a flush was asked to make.
//! 3. **A recovery that fails partway changes nothing**: the next open
//!    finds what a clean recovery of the same crash finds.
//!
//! A careless caller, one that carries on past an error, has asked for any
//! gap it gets, and is held to the first invariant only.

#![no_main]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use roble::sim::{SimEnv, SimFs};
use roble::{Db, Fs, Options};
use roble_format::batch::Batch;
use roble_fuzz::{Files, fixtures, install};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Arbitrary, Debug)]
struct Case {
    fixture: u8,
    config: Config,
    steps: Vec<Step>,
    fault: Fault,
    /// Carries on past an error rather than stopping at it.
    careless: bool,
    /// Loses everything unsynced once the run has ended.
    crash: bool,
    /// Fails an operation of the first recovery as well.
    recovery_fault: Option<u16>,
}

#[derive(Arbitrary, Debug)]
struct Config {
    wal: bool,
    automatic_compactions: bool,
    l0_compaction_threshold: u8,
    target_file_size: u16,
    /// Only with the WAL on, as in `model`: with it off a flush is a
    /// durability point, and this has to know where each one fell.
    small_memtable: Option<u16>,
}

/// What goes wrong, counting operations from the open.
#[derive(Arbitrary, Debug)]
enum Fault {
    Nothing,
    Fail(u16),
    Withhold(u16),
}

#[derive(Arbitrary, Debug)]
enum Step {
    Write { ops: Vec<Op>, sync: bool },
    Flush,
    Compact,
}

#[derive(Arbitrary, Debug)]
enum Op {
    Put(u8, u16),
    Delete(u8),
    DeleteRange(u8, u8),
}

fn key(n: u8) -> Vec<u8> {
    format!("/oxia/k{}", n % 40).into_bytes()
}

/// Operations are counted from the open, which takes a few dozen, and a
/// sequence of 32 steps takes a few hundred.
fn at(n: u16) -> u64 {
    1 + u64::from(n % 512)
}

fn options(c: &Config) -> Options<SimFs> {
    let mut o = Options::new()
        .create_if_missing(true)
        .wal(if c.wal {
            roble::Wal::Synced
        } else {
            roble::Wal::Off
        })
        .disable_automatic_compactions(!c.automatic_compactions)
        .l0_compaction_threshold(1 + u64::from(c.l0_compaction_threshold % 4))
        .target_file_size(256 + u64::from(c.target_file_size % 8192));
    if let (true, Some(bytes)) = (c.wal, c.small_memtable) {
        o = o.memtable_size(16 * 1024 + u64::from(bytes));
    }
    o
}

/// What the store was told, and how much of it it promised to keep.
struct History {
    /// The store after each batch: `states[0]` before any, `states[k]` after
    /// the `k`th.
    states: Vec<Model>,
    /// How many batches were made durable.
    durable: usize,
    /// How many batches each flush asked to cover.
    flushes: Vec<usize>,
}

impl History {
    /// Whether `found` is a state the store may come back as.
    fn allows(&self, found: &Model, wal: bool) -> bool {
        if wal {
            self.states[self.durable..].contains(found)
        } else {
            std::iter::once(self.durable)
                .chain(self.flushes.iter().copied().filter(|&p| p >= self.durable))
                .any(|k| self.states[k] == *found)
        }
    }
}

async fn contents<F: Fs>(db: &Db<F>) -> Model {
    let mut scan = db.range(..).expect("scan");
    let mut forward = Model::new();
    scan.first().await.expect("first");
    while scan.is_valid() {
        forward.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.next().await.expect("next");
    }
    let mut backward = Model::new();
    scan.last().await.expect("last");
    while scan.is_valid() {
        backward.insert(scan.key().to_vec(), scan.value().to_vec());
        scan.prev().await.expect("prev");
    }
    assert_eq!(
        forward, backward,
        "a reverse scan disagrees with the forward one"
    );
    forward
}

/// Runs the steps, stopping at the first error unless the case is careless.
async fn scenario(case: &Case, env: &SimEnv, dir: &Path, h: &mut History) -> roble::Result<()> {
    let wal = case.config.wal;
    let db = roble::open_with(env, dir, options(&case.config)).await?;
    for step in case.steps.iter().take(32) {
        let result = match step {
            Step::Write { ops, sync } => {
                let mut batch = Batch::new();
                let mut next = h.states.last().expect("a state").clone();
                for op in ops.iter().take(8) {
                    match op {
                        Op::Put(k, len) => {
                            let k = key(*k);
                            let mut v = format!("v{}.", h.states.len()).into_bytes();
                            v.resize(v.len() + usize::from(*len % 1024), b'x');
                            batch.put(&k, &v)?;
                            next.insert(k, v);
                        }
                        Op::Delete(k) => {
                            let k = key(*k);
                            batch.delete(&k)?;
                            next.remove(&k);
                        }
                        Op::DeleteRange(a, b) => {
                            let (a, b) = (key(*a), key(*b));
                            if a < b {
                                batch.delete_range(&a, &b)?;
                                next.retain(|k, _| !(*k >= a && *k < b));
                            }
                        }
                    }
                }
                if batch.is_empty() {
                    continue;
                }
                // With the WAL off a synced write is refused: there is
                // nothing to sync.
                let sync = *sync && wal;
                h.states.push(next);
                let result = db.write_raw(&mut batch, sync).await;
                if result.is_ok() && sync {
                    h.durable = h.states.len() - 1;
                }
                result
            }
            Step::Flush => {
                let covers = h.states.len() - 1;
                h.flushes.push(covers);
                let result = db.flush().await.map(drop);
                if result.is_ok() {
                    h.durable = covers;
                }
                result
            }
            Step::Compact => db.compact_once().await.map(drop),
        };
        if !case.careless {
            result?;
        }
    }
    Ok(())
}

/// Puts the database on a fresh filesystem, runs the case against it with
/// its fault, and crashes the machine if the case says to. Deterministic:
/// the same case gives the same bytes on disk every time.
async fn ran(case: &Case, files: Option<&Files>, before: &Model) -> (SimEnv, History) {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    if let Some(files) = files {
        install(env.sim(), &dir, files).await;
    }
    match case.fault {
        Fault::Nothing => {}
        Fault::Fail(n) => env.sim().fail_at(at(n)),
        Fault::Withhold(n) => env.sim().withhold_at(at(n)),
    }
    let mut h = History {
        states: vec![before.clone()],
        durable: 0,
        flushes: Vec::new(),
    };
    let _ = scenario(case, &env, &dir, &mut h).await;
    env.sim().stop_failing();
    env.sim().stop_withholding();
    if case.crash {
        env.sim().crash();
    }
    (env, h)
}

async fn run(case: Case) {
    let all = fixtures();
    let files = all.get(usize::from(case.fixture) % (all.len() + 1));
    let config = &case.config;
    let dir = PathBuf::from("/db");

    // What the store holds before anything is done to it, from a clean open
    // of its own: the run below may fail before its open has finished.
    let before = match files {
        Some(files) => {
            let env = SimEnv::new();
            install(env.sim(), &dir, files).await;
            let db = roble::open_with(&env, &dir, options(config))
                .await
                .expect("a golden database opens");
            contents(&db).await
        }
        None => Model::new(),
    };

    let (env, h) = ran(&case, files, &before).await;
    let found = match case.recovery_fault {
        None => {
            let db = roble::open_with(&env, &dir, options(config))
                .await
                .unwrap_or_else(|e| panic!("the store does not open: {e}"));
            contents(&db).await
        }
        Some(m) => {
            let expected = {
                let db = roble::open_with(&env, &dir, options(config))
                    .await
                    .unwrap_or_else(|e| panic!("the store does not open: {e}"));
                contents(&db).await
            };
            // The same crash again, recovered with a failure partway.
            let (env, _) = ran(&case, files, &before).await;
            env.sim().fail_at(at(m));
            drop(roble::open_with(&env, &dir, options(config)).await);
            env.sim().stop_failing();
            let db = roble::open_with(&env, &dir, options(config))
                .await
                .unwrap_or_else(|e| {
                    panic!("a failed recovery left a store that does not open: {e}")
                });
            let found = contents(&db).await;
            assert_eq!(found, expected, "a failed recovery changed what survived");
            found
        }
    };

    if !case.careless {
        assert!(
            h.allows(&found, config.wal),
            "the store came back as none of the states it may: {} batches, {} durable, \
             flushes covering {:?}",
            h.states.len() - 1,
            h.durable,
            h.flushes
        );
    }
}

thread_local! {
    static RUNTIME: compio::runtime::Runtime =
        compio::runtime::Runtime::new().expect("a compio runtime");
}

fuzz_target!(|case: Case| {
    RUNTIME.with(|rt| rt.block_on(run(case)));
});
