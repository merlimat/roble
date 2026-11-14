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

//! Scanning, judged against Pebble's iterator.
//!
//! `harness/read-probes` walks the same database end to end in both
//! directions, bounded and unbounded, and then — the part a seek corpus
//! cannot reach — seeks and *steps*: three forward, three back, and a turn
//! that goes one step forward and one back again. A merging iterator's real
//! failure is a seek that lands correctly and then cannot be stepped away
//! from, or a direction change that loses its place.
//!
//! This reproduces that file from roble's own scan and compares the whole
//! text.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Disk, Fs, Options, Scan, WriteBuf};
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
        disk.close().await.expect("close");
    }
    (env, dir)
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd-length hex {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A small, reproducible generator: the same seed gives the same walk.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

/// The keys the many-source walk below writes.
fn probe_key(n: u64) -> Vec<u8> {
    format!("s{n:04}").into_bytes()
}

/// Everything a scan returns, which is also the model's starting point.
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

/// One source's worth of writes, into `batch` and into the model alike.
///
/// An eighth of the keyspace of its own, so that the extreme key sits in a
/// different source at nearly every step and the heap's top keeps moving; and
/// then the keys *every* source writes, so those user keys reach the merge in
/// as many versions as there are sources, each value naming the round that
/// wrote it. A deletion on top of a key another source still holds, so a step
/// has to walk past a live older version to find the key gone, and on one
/// round a range deletion that a later round writes back into — which only
/// the sequence numbers can settle.
fn write_round(batch: &mut Batch, model: &mut Model, round: u64) {
    let mut keys: Vec<u64> = (0..256).filter(|i| i % 8 == round % 8).collect();
    keys.extend((0..256).step_by(32));
    keys.sort_unstable();
    keys.dedup();
    for i in keys {
        let key = probe_key(i);
        let value = format!("r{round}-s{i:04}").into_bytes();
        batch.put(&key, &value).expect("put");
        model.insert(key, value);
    }
    let gone = probe_key(round * 8 + 3);
    batch.delete(&gone).expect("delete");
    model.remove(&gone);
    if round == 7 {
        let (lo, hi) = (probe_key(100), probe_key(112));
        batch.delete_range(&lo, &hi).expect("delete range");
        model.retain(|k, _| *k < lo || *k >= hi);
    }
}

/// What the scan is on, spelled so that a failure reads.
fn on_key<D: Disk>(scan: &Scan<D>) -> Option<(String, String)> {
    scan.is_valid().then(|| {
        (
            String::from_utf8_lossy(scan.key()).into_owned(),
            String::from_utf8_lossy(scan.value()).into_owned(),
        )
    })
}

/// The same, from the model.
fn model_at(entries: &[(Vec<u8>, Vec<u8>)], place: Option<usize>) -> Option<(String, String)> {
    place.map(|p| {
        (
            String::from_utf8_lossy(&entries[p].0).into_owned(),
            String::from_utf8_lossy(&entries[p].1).into_owned(),
        )
    })
}

/// Where a scan is, as the harness spells it.
fn where_it_is<D: Disk>(scan: &Scan<D>) -> String {
    if !scan.is_valid() {
        return "none".to_string();
    }
    format!("{} {}", hex(scan.key()), hex(scan.value()))
}

#[compio::test]
async fn roble_scans_where_go_pebble_scans() {
    let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let path = golden.join("db-reads.scans");
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{}: {e}; regenerate with 'cd harness && go run ./read-probes ../tests/golden'",
            path.display()
        )
    });

    let (env, dir) = load("db-reads").await;
    let db = roble::open(&env, &dir).await.expect("open");
    let mut out = String::new();

    // The four whole-database walks.
    for (name, lower, upper, forward) in [
        ("forward", None, None, true),
        ("reverse", None, None, false),
        ("bounded-forward", Some(&b"b2"[..]), Some(&b"d3"[..]), true),
        ("bounded-reverse", Some(&b"b2"[..]), Some(&b"d3"[..]), false),
    ] {
        out.push_str(&format!("{name}\n"));
        let mut scan = db.scan_range(lower, upper).await.expect("scan");
        if forward {
            scan.first().await.expect("first");
        } else {
            scan.last().await.expect("last");
        }
        while scan.is_valid() {
            out.push_str(&format!("  {} {}\n", hex(scan.key()), hex(scan.value())));
            if forward {
                scan.next().await.expect("next");
            } else {
                scan.prev().await.expect("prev");
            }
        }
    }

    // Seeks that fall outside the bounds, which a walk never makes.
    {
        let mut scan = db
            .scan_range(Some(&b"b2"[..]), Some(&b"d3"[..]))
            .await
            .expect("scan");
        out.push_str("bounded-seeks\n");
        for (name, seek_ge, key) in [
            ("below", true, &b"a0"[..]),
            ("above", false, &b"zz"[..]),
            ("at-lower", true, &b"b2"[..]),
            ("at-upper", false, &b"d3"[..]),
        ] {
            if seek_ge {
                scan.seek_ge(key).await.expect("seek_ge");
            } else {
                scan.seek_lt(key).await.expect("seek_lt");
            }
            out.push_str(&format!("  {name} {}\n", where_it_is(&scan)));
        }
    }

    // Seek, then step. The probe list is taken from the file itself.
    let probes: Vec<Vec<u8>> = expected
        .lines()
        .filter_map(|l| l.strip_prefix("from "))
        .map(unhex)
        .collect();
    assert!(!probes.is_empty(), "no probes in {}", path.display());

    let mut scan = db.scan().await.expect("scan");
    for probe in &probes {
        out.push_str(&format!("from {}\n", hex(probe)));

        scan.seek_ge(probe).await.expect("seek_ge");
        for _ in 0..3 {
            out.push_str(&format!("  fwd {}\n", where_it_is(&scan)));
            if !scan.is_valid() {
                break;
            }
            scan.next().await.expect("next");
        }

        scan.seek_lt(probe).await.expect("seek_lt");
        for _ in 0..3 {
            out.push_str(&format!("  rev {}\n", where_it_is(&scan)));
            if !scan.is_valid() {
                break;
            }
            scan.prev().await.expect("prev");
        }

        // One forward, then back again: the direction change.
        scan.seek_ge(probe).await.expect("seek_ge");
        if scan.is_valid() {
            scan.next().await.expect("next");
        }
        if scan.is_valid() {
            scan.prev().await.expect("prev");
        }
        out.push_str(&format!("  turn {}\n", where_it_is(&scan)));

        // And the other way round, which is the only path through the
        // re-seek `next` does after a reverse step.
        scan.seek_lt(probe).await.expect("seek_lt");
        if scan.is_valid() {
            scan.prev().await.expect("prev");
        }
        if scan.is_valid() {
            scan.next().await.expect("next");
        }
        out.push_str(&format!("  turn-back {}\n", where_it_is(&scan)));
    }

    if out != expected {
        let (a, b) = (
            out.lines().collect::<Vec<_>>(),
            expected.lines().collect::<Vec<_>>(),
        );
        let at = a
            .iter()
            .zip(&b)
            .position(|(x, y)| x != y)
            .unwrap_or(a.len().min(b.len()));
        let from = at.saturating_sub(8);
        panic!(
            "scans differ at line {}:\n  roble:  {:?}\n  pebble: {:?}\ncontext:\n{}",
            at + 1,
            a.get(at),
            b.get(at),
            b[from..(at + 2).min(b.len())].join("\n")
        );
    }
}

/// A scan over many sources, walked forward, backward and through turns,
/// against a `BTreeMap` of what was written.
///
/// The merge picks its next key with a heap (`scan::Heap`) and the heap turns
/// round rather than being replaced, as Pebble's `mergingIterHeap.reverse`
/// does. Three things can go wrong there that nothing about key order alone
/// would catch.
///
/// **A heap is not stable**, where the two linear scans it replaces were: each
/// only ever took a strictly more extreme key, so the *first* source holding
/// the extreme one was the answer. The order among equal keys decides which
/// source's value a step reads, so every source here writes the same shared
/// keys with a value naming the batch that wrote it — a merge that ordered
/// two versions of a key the other way round answers with the wrong batch's
/// value while keeping every key in its place.
///
/// **A flipped comparator is half right.** Reversing the user keys and not
/// the trailers, or the other way about, still walks the keys in the right
/// order and still picks the newest version of most of them; it picks the
/// wrong one exactly where a user key has several versions, which is why the
/// shared keys are written nine times over.
///
/// **A turn re-seeks and re-heapifies** (`Scan::sought_ge`, `Scan::sought_lt`).
/// A heap left in its old order, or rebuilt over sources that were not sought
/// first, is wrong only at the step after the turn — so the walk turns round
/// constantly, and the loop after it turns at *every* key in both directions.
///
/// L0 is where the sources are: its tables overlap, so each stays a source of
/// its own, where a level below L0 is collapsed into one run (`Scan::new`).
/// Compaction is off so they stay there, and the last round is left in the
/// memtable, which is a source whose contents are not fixed and which holds
/// the newest version of nearly every shared key.
#[compio::test]
async fn a_scan_over_many_sources_walks_where_a_btreemap_would() {
    let (env, dir) = load("db-simple").await;
    let db = roble::open_with(
        &env,
        &dir,
        Options::new().disable_automatic_compactions(true),
    )
    .await
    .expect("open");

    let mut model: Model = contents(&db).await;
    for round in 0..8u64 {
        let mut batch = Batch::new();
        write_round(&mut batch, &mut model, round);
        db.write(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
    }
    // The ninth round stays in the memtable rather than becoming a table.
    let mut batch = Batch::new();
    write_round(&mut batch, &mut model, 8);
    db.write(&mut batch, true).await.expect("write");

    let l0 = db.versions().version.level(0).len();
    assert!(
        l0 >= 8,
        "the merge has to be wide to be testing a heap: L0 holds {l0} tables"
    );

    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    // Every stored key, a key just past each one, and one either side of the
    // whole store, so a seek lands on a key, between two, and off both ends.
    let mut probes: Vec<Vec<u8>> = Vec::with_capacity(2 * entries.len() + 2);
    for (k, _) in &entries {
        probes.push(k.clone());
        probes.push([k.as_slice(), b"!"].concat());
    }
    probes.push(Vec::new());
    probes.push(vec![0xff]);

    let mut scan = db.scan().await.expect("scan");
    let mut rng = Rng(20_261_113);
    let mut place: Option<usize> = None;
    let mut forward = true;
    // Counted because a walk that never turned round would pass every
    // assertion below without testing the thing this is here for.
    let mut turns = 0usize;
    for step in 0..4000usize {
        let what = rng.below(10);
        let was = place;
        let now_forward = match what {
            0 => {
                scan.first().await.expect("first");
                place = (!entries.is_empty()).then_some(0);
                true
            }
            1 => {
                scan.last().await.expect("last");
                place = entries.len().checked_sub(1);
                false
            }
            2..=3 => {
                let probe = &probes[rng.below(probes.len() as u64) as usize];
                scan.seek_ge(probe).await.expect("seek_ge");
                place = entries.iter().position(|(k, _)| k >= probe);
                true
            }
            4..=5 => {
                let probe = &probes[rng.below(probes.len() as u64) as usize];
                scan.seek_lt(probe).await.expect("seek_lt");
                place = entries
                    .iter()
                    .rposition(|(k, _)| k.as_slice() < probe.as_slice());
                false
            }
            6..=7 => {
                scan.next().await.expect("next");
                // A scan on no key stays on none: `next` and `prev` return at
                // once, so a walk that fell off an end needs a seek to get
                // back on. The model has to do the same or it drifts.
                place = place.and_then(|p| (p + 1 < entries.len()).then_some(p + 1));
                true
            }
            _ => {
                scan.prev().await.expect("prev");
                place = place.and_then(|p| p.checked_sub(1));
                false
            }
        };
        if what >= 6 && was.is_some() && now_forward != forward {
            turns += 1;
        }
        forward = now_forward;

        assert_eq!(
            on_key(&scan),
            model_at(&entries, place),
            "step {step}: op {what} left the scan somewhere the model did not"
        );
    }
    assert!(
        turns > 100,
        "a walk that barely turns round is not testing a direction change, turned {turns} times"
    );

    // And a turn at every key, in both directions, which the random walk
    // reaches only where it happens to.
    for p in 0..entries.len() {
        if p + 1 < entries.len() {
            scan.seek_ge(&entries[p].0).await.expect("seek_ge");
            assert_eq!(on_key(&scan), model_at(&entries, Some(p)), "seek to {p}");
            scan.next().await.expect("next");
            assert_eq!(
                on_key(&scan),
                model_at(&entries, Some(p + 1)),
                "a step forward from {p}"
            );
            scan.prev().await.expect("prev");
            assert_eq!(
                on_key(&scan),
                model_at(&entries, Some(p)),
                "a turn back to {p}"
            );
        }
        if p > 0 {
            scan.seek_ge(&entries[p].0).await.expect("seek_ge");
            scan.prev().await.expect("prev");
            assert_eq!(
                on_key(&scan),
                model_at(&entries, Some(p - 1)),
                "a step back from {p}"
            );
            scan.next().await.expect("next");
            assert_eq!(
                on_key(&scan),
                model_at(&entries, Some(p)),
                "a turn forward to {p}"
            );
        }
    }
}
