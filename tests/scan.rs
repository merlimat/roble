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
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, Scan, WriteBuf};
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
fn on_key<F: Fs>(scan: &Scan<F>) -> Option<(String, String)> {
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
fn where_it_is<F: Fs>(scan: &Scan<F>) -> String {
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
        ("forward", Unbounded, Unbounded, true),
        ("reverse", Unbounded, Unbounded, false),
        (
            "bounded-forward",
            Included(&b"b2"[..]),
            Excluded(&b"d3"[..]),
            true,
        ),
        (
            "bounded-reverse",
            Included(&b"b2"[..]),
            Excluded(&b"d3"[..]),
            false,
        ),
    ] {
        out.push_str(&format!("{name}\n"));
        let mut scan = db.range((lower, upper)).expect("scan");
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
        let mut scan = db.range(&b"b2"[..]..&b"d3"[..]).expect("scan");
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

    let mut scan = db.range(..).expect("scan");
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
/// its own, where a level below L0 is read as one source (`Scan::rebuild`).
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
        db.write_raw(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
    }
    // The ninth round stays in the memtable rather than becoming a table.
    let mut batch = Batch::new();
    write_round(&mut batch, &mut model, 8);
    db.write_raw(&mut batch, true).await.expect("write");

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

    let mut scan = db.range(..).expect("scan");
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

/// A range deletion reaching past the last point key of its table still
/// hides what lies under it once the level has stepped into the next table,
/// in both directions, and after a seek that lands back in the same table.
///
/// A level below L0 reads one table at a time, and its range deletions are
/// those of the table it last opened (`roble::scan`). That is only enough if
/// the level does not open its next table while the merge is still deciding
/// keys the last one's tombstones cover. Pebble keeps it from doing so by
/// interleaving the tombstones' bounds among the point keys; roble leaves the
/// next table unopened until the merge reaches its bound. A level that opened
/// it as soon as it ran out of point keys would answer with keys a range
/// deletion hides, and only where a table's tombstone reaches past its last
/// point key — forward — or starts before its first — backward — which is
/// the shape built here.
///
/// L1 holds two tables, each the output of compacting one flush on its own:
/// `a` ends in a range deletion past its last point key and `b` starts with
/// one before its first, and L2 below them holds a key under each deletion
/// and a key beside it.
#[compio::test]
async fn a_tombstone_past_its_tables_last_key_hides_what_is_below_it() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open");

    // L2: a key under each deletion to come, and one beside each.
    let mut below = Batch::new();
    for key in ["k25", "k35", "k65", "k75"] {
        below.put(key.as_bytes(), b"old").expect("put");
    }
    db.write_raw(&mut below, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");
    db.compact(0, &[table]).await.expect("compact into L1");
    let l1: Vec<u64> = db
        .versions()
        .version
        .level(1)
        .iter()
        .map(|t| t.file_num)
        .collect();
    db.compact(1, &l1).await.expect("compact into L2");

    // L1: `a`, ending in a deletion past its last point key, and `b`, starting
    // with one before its first. Compacted one at a time, so each is a table
    // of its own and both deletions stay, with L2 under them.
    let builds: [fn(&mut Batch); 2] = [
        |b: &mut Batch| {
            for key in ["k00", "k05", "k10"] {
                b.put(key.as_bytes(), b"a").expect("put");
            }
            b.delete_range(b"k20", b"k30").expect("delete range");
        },
        |b: &mut Batch| {
            b.delete_range(b"k60", b"k70").expect("delete range");
            for key in ["k80", "k90"] {
                b.put(key.as_bytes(), b"b").expect("put");
            }
        },
    ];
    for build in builds {
        let mut batch = Batch::new();
        build(&mut batch);
        db.write_raw(&mut batch, true).await.expect("write");
        let table = db.flush().await.expect("flush").expect("a table");
        db.compact(0, &[table]).await.expect("compact into L1");
    }
    {
        let versions = db.versions();
        let (l1, l2) = (versions.version.level(1), versions.version.level(2));
        assert_eq!(l1.len(), 2, "two tables in L1");
        assert_eq!(l2.len(), 1, "one table in L2");
        assert!(
            l1[0].largest_user_key().expect("largest") == b"k30".as_slice()
                && l1[1].smallest_user_key().expect("smallest") == b"k60".as_slice(),
            "each L1 table's bounds should reach past its point keys to its deletion"
        );
    }

    let visible: Vec<&str> = vec!["k00", "k05", "k10", "k35", "k75", "k80", "k90"];

    let mut scan = db.range(..).expect("scan");
    let mut forward = Vec::new();
    scan.first().await.expect("first");
    while scan.is_valid() {
        forward.push(String::from_utf8(scan.key().to_vec()).expect("utf8"));
        scan.next().await.expect("next");
    }
    assert_eq!(forward, visible, "forward");

    let mut backward = Vec::new();
    scan.last().await.expect("last");
    while scan.is_valid() {
        backward.push(String::from_utf8(scan.key().to_vec()).expect("utf8"));
        scan.prev().await.expect("prev");
    }
    backward.reverse();
    assert_eq!(backward, visible, "backward");

    // A seek into the first table, and then one that lands in it again: the
    // level reuses the table it is on rather than opening it twice, and has
    // to gather its tombstones all the same.
    scan.seek_ge(b"k00").await.expect("seek");
    scan.seek_ge(b"k20").await.expect("seek again");
    assert_eq!(
        scan.is_valid().then(|| scan.key().to_vec()),
        Some(b"k35".to_vec()),
        "k25 is under the first table's deletion"
    );
    // And the same backward, in the second.
    scan.seek_lt(b"k99").await.expect("seek");
    scan.seek_lt(b"k70").await.expect("seek again");
    assert_eq!(
        scan.is_valid().then(|| scan.key().to_vec()),
        Some(b"k35".to_vec()),
        "k65 is under the second table's deletion"
    );
}

/// Every kind of bound `range` takes, on either side, reads what
/// `BTreeMap::range` reads, forwards and backwards.
///
/// A scan is built with Pebble's bounds, an included lower and an excluded
/// upper, so an excluded lower and an included upper are each turned into the
/// key's successor. The keys `b\0` and `d\0` are the ones that successor
/// lands on: the first is the least key after `b`, and so inside a range that
/// excludes `b`; the second is the least key after `d`, and so outside one
/// that includes `d`.
#[compio::test]
async fn a_range_reads_what_a_btreemap_range_reads_for_every_kind_of_bound() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(&env, &dir, Options::new().create_if_missing(true))
        .await
        .expect("open");
    let mut model = Model::new();
    let mut batch = db.indexed_batch();
    for key in [&b"a"[..], b"b", b"b\0", b"c", b"d", b"d\0", b"e"] {
        batch.put(key, b"v").expect("put");
        model.insert(key.to_vec(), b"v".to_vec());
    }
    batch
        .commit(roble::Durability::NoSync)
        .await
        .expect("commit");

    let (b, d) = (&b"b"[..], &b"d"[..]);
    for lower in [Unbounded, Included(b), Excluded(b)] {
        for upper in [Unbounded, Included(d), Excluded(d)] {
            let expected: Vec<Vec<u8>> = model
                .range::<[u8], _>((lower, upper))
                .map(|(k, _)| k.clone())
                .collect();

            let mut scan = db.range((lower, upper)).expect("range");
            let mut forward = Vec::new();
            scan.first().await.expect("first");
            while scan.is_valid() {
                forward.push(scan.key().to_vec());
                scan.next().await.expect("next");
            }
            assert_eq!(forward, expected, "forward over {lower:?}..{upper:?}");

            let mut backward = Vec::new();
            scan.last().await.expect("last");
            while scan.is_valid() {
                backward.push(scan.key().to_vec());
                scan.prev().await.expect("prev");
            }
            backward.reverse();
            assert_eq!(backward, expected, "backward over {lower:?}..{upper:?}");
        }
    }
}

/// Opens the store at `dir` with no automatic compactions, so a test decides
/// which level each table ends up in.
async fn open_manual(env: &SimEnv, dir: &Path) -> roble::Db<SimFs> {
    roble::open_with(
        env,
        dir,
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open")
}

/// Writes `keys` with a value of `value`, flushes them and compacts the table
/// down to `level`.
async fn table_at(
    db: &roble::Db<SimFs>,
    keys: impl Iterator<Item = String>,
    value: &[u8],
    level: u32,
) {
    let mut batch = Batch::new();
    for key in keys {
        batch.put(key.as_bytes(), value).expect("put");
    }
    db.write_raw(&mut batch, true).await.expect("write");
    let mut tables = vec![db.flush().await.expect("flush").expect("a table")];
    for from in 0..level {
        tables = db.compact(from, &tables).await.expect("compact");
    }
}

/// A floor landing inside a deleted tenth-to-nine-tenths of a table, and a
/// ceiling the same, read a block or two and not the whole of what was
/// deleted: the levels the tombstone hides are moved past it at once, rather
/// than stepped through key by key with every block on the way read.
///
/// 4,000 keys of 100 bytes are some 27 blocks of 16 KiB in L2, and the
/// tombstone in L0 covers all but the first and last hundred keys. Before the
/// skip, the floor of `k2000` read the 13 blocks from there down to `k0099`.
/// Measured after a reopen, with the tables opened and the first and last
/// blocks read by two gets, so what is counted is the seek alone.
#[compio::test]
async fn a_floor_or_ceiling_over_a_range_deletion_reads_past_it_not_through_it() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    {
        let db = open_manual(&env, &dir).await;
        table_at(&db, (0..4000).map(|i| format!("k{i:04}")), &[b'v'; 100], 2).await;
        let mut batch = Batch::new();
        batch
            .delete_range(b"k0100", b"k3900")
            .expect("delete range");
        db.write_raw(&mut batch, true).await.expect("write");
        db.flush().await.expect("flush").expect("a table");
    }
    let db = open_manual(&env, &dir).await;
    for key in [b"k0000", b"k3999"] {
        db.get(key, Comparison::Equal)
            .await
            .expect("get")
            .expect("there");
    }
    let fs: &SimFs = env.sim();

    let before = fs.bytes_read();
    let floor = db.get(b"k2000", Comparison::Floor).await.expect("floor");
    let floor_read = fs.bytes_read() - before;
    assert_eq!(floor.map(|f| f.key), Some(b"k0099".to_vec()), "floor");

    let before = fs.bytes_read();
    let ceiling = db
        .get(b"k2000", Comparison::Ceiling)
        .await
        .expect("ceiling");
    let ceiling_read = fs.bytes_read() - before;
    assert_eq!(ceiling.map(|f| f.key), Some(b"k3900".to_vec()), "ceiling");

    // The block the seek lands in, and at most one more where the level comes
    // out of the span; the 13 blocks of the span itself are not read.
    for (what, read) in [("floor", floor_read), ("ceiling", ceiling_read)] {
        assert!(
            read <= 2 * 17 * 1024,
            "a {what} through the deleted span read {read} bytes, more than two blocks"
        );
    }
}

/// What a skip past a range tombstone must leave alone, in both directions:
/// keys written into the span after the tombstone — in the memtable, and in
/// a table of their own flushed after the one holding it — and a snapshot
/// taken before the tombstone, which does not see it at all.
#[compio::test]
async fn a_skip_past_a_range_deletion_keeps_what_the_deletion_does_not_hide() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = open_manual(&env, &dir).await;
    let all: Vec<String> = (0..100).map(|i| format!("k{i:03}")).collect();
    table_at(&db, all.iter().cloned(), b"old", 2).await;
    let before = db.snapshot().expect("snapshot");

    // The tombstone in an L0 table of its own, a rewrite in a newer L0 table,
    // and another in the memtable.
    let mut batch = Batch::new();
    batch.delete_range(b"k010", b"k090").expect("delete range");
    db.write_raw(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");
    let mut batch = Batch::new();
    batch.put(b"k050", b"new").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");
    let mut batch = Batch::new();
    batch.put(b"k060", b"new").expect("put");
    db.write_raw(&mut batch, true).await.expect("write");

    let mut visible: Vec<String> = all[..10].to_vec();
    visible.extend(["k050".to_string(), "k060".to_string()]);
    visible.extend(all[90..].iter().cloned());

    async fn walk<F: Fs>(mut scan: Scan<F>) -> (Vec<String>, Vec<String>) {
        let mut forward = Vec::new();
        scan.first().await.expect("first");
        while scan.is_valid() {
            forward.push(String::from_utf8(scan.key().to_vec()).expect("utf8"));
            scan.next().await.expect("next");
        }
        let mut backward = Vec::new();
        scan.last().await.expect("last");
        while scan.is_valid() {
            backward.push(String::from_utf8(scan.key().to_vec()).expect("utf8"));
            scan.prev().await.expect("prev");
        }
        backward.reverse();
        (forward, backward)
    }
    let (forward, backward) = walk(db.range(..).expect("scan")).await;
    assert_eq!(forward, visible, "forward");
    assert_eq!(backward, visible, "backward");
    let (forward, backward) = walk(before.range(..).expect("scan")).await;
    assert_eq!(forward, all, "forward, at the snapshot");
    assert_eq!(backward, all, "backward, at the snapshot");

    for (probe, how, want) in [
        ("k080", Comparison::Floor, "k060"),
        ("k055", Comparison::Floor, "k050"),
        ("k049", Comparison::Floor, "k009"),
        ("k011", Comparison::Ceiling, "k050"),
        ("k051", Comparison::Ceiling, "k060"),
        ("k061", Comparison::Ceiling, "k090"),
    ] {
        let found = db.get(probe.as_bytes(), how).await.expect("get");
        assert_eq!(
            found.map(|f| String::from_utf8(f.key).expect("utf8")),
            Some(want.to_string()),
            "{how:?} of {probe}"
        );
        let found = before.get(probe.as_bytes(), how).await.expect("get");
        assert_eq!(
            found.map(|f| String::from_utf8(f.key).expect("utf8")),
            Some(probe.to_string()),
            "{how:?} of {probe} at the snapshot"
        );
    }
}

/// The skip past a tombstone in L1 passes L2 whole, the table at the span's
/// far edge included, although that table also holds a key written after the
/// tombstone: one just past the span, compacted down beside the keys the
/// tombstone hides. Its newest sequence number is above the tombstone's, so
/// only the level order says the keys it holds inside the span are older —
/// and only the level order lets a seek that lands in it, or crosses into it,
/// out without stepping through them.
///
/// Tables of 256 KiB in L2, so that it is several of them and the skip
/// crosses more than one, and the edge table holds well over a thousand of
/// the hidden keys: stepped, a floor from the top of its share of the span
/// reads a dozen blocks.
#[compio::test]
async fn a_skip_past_a_range_deletion_passes_a_deeper_table_holding_a_later_key_beside_it() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let options = || {
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            .target_file_size(64 << 10)
    };
    {
        let db = roble::open_with(&env, &dir, options()).await.expect("open");
        let mut batch = Batch::new();
        for i in 0..8000 {
            batch
                .put(format!("k{i:04}").as_bytes(), &[b'v'; 100])
                .expect("put");
        }
        db.write_raw(&mut batch, true).await.expect("write");
        let table = db.flush().await.expect("flush").expect("a table");
        let l1 = db.compact(0, &[table]).await.expect("compact into L1");
        db.compact(1, &l1).await.expect("compact into L2");

        // The tombstone, into L1.
        let mut batch = Batch::new();
        batch
            .delete_range(b"k0100", b"k7900")
            .expect("delete range");
        db.write_raw(&mut batch, true).await.expect("write");
        let table = db.flush().await.expect("flush").expect("a table");
        db.compact(0, &[table]).await.expect("compact into L1");

        // A later key past the span, into L1 beside the tombstone's table and
        // then down into the L2 table at the span's edge.
        let mut batch = Batch::new();
        batch.put(b"k7950", b"later").expect("put");
        db.write_raw(&mut batch, true).await.expect("write");
        let table = db.flush().await.expect("flush").expect("a table");
        let beside = db.compact(0, &[table]).await.expect("compact into L1");
        db.compact(1, &beside).await.expect("compact into L2");
    }
    let db = roble::open_with(&env, &dir, options()).await.expect("open");
    {
        let versions = db.versions();
        let (l1, l2) = (versions.version.level(1), versions.version.level(2));
        assert_eq!(l1.len(), 1, "the tombstone's table alone in L1");
        assert!(l2.len() >= 3, "L2 is several tables, not {}", l2.len());
        let edge = l2
            .iter()
            .find(|t| t.largest_seq_num > l1[0].largest_seq_num)
            .expect("an L2 table holding the later key");
        let smallest = edge.smallest_user_key().expect("smallest");
        assert!(
            smallest > b"k0100".as_slice() && smallest < b"k7300".as_slice(),
            "the later key's table reaches well into the span: it starts at {:?}",
            String::from_utf8_lossy(smallest)
        );
    }
    for key in [b"k0000", b"k7999"] {
        db.get(key, Comparison::Equal)
            .await
            .expect("get")
            .expect("there");
    }
    let fs: &SimFs = env.sim();

    // A floor from the top of the edge table's share of the span, and a
    // ceiling that has to cross into it.
    for (key, how, want) in [
        (b"k7899~".as_slice(), Comparison::Floor, b"k0099".as_slice()),
        (
            b"k4000".as_slice(),
            Comparison::Ceiling,
            b"k7900".as_slice(),
        ),
    ] {
        let before = fs.bytes_read();
        let found = db.get(key, how).await.expect("get");
        let read = fs.bytes_read() - before;
        assert_eq!(found.map(|f| f.key), Some(want.to_vec()), "{how:?}");
        // A block or two, and a table the seek lands in opened: 17 and 20
        // KB, against 115 KB for the floor stepped through the edge table.
        assert!(
            read <= 2 * 17 * 1024,
            "a {how:?} through the deleted span read {read} bytes"
        );
    }
}

/// A tombstone cut across several L1 tables, with keys written after it
/// under every part, and the last part compacted down into L2: the later
/// keys there stay in reach of every scan and seek.
///
/// The skip trusts the span of the tombstone the open table holds, so this
/// is the case that would break if a table's part reached past its own
/// bounds — the first part, still in L1, would then send L2 past the later
/// keys under the last. Compaction clips each part to its table (§5), which
/// this pins.
#[compio::test]
async fn a_skip_past_part_of_a_split_range_deletion_keeps_the_later_keys_under_the_rest() {
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let db = roble::open_with(
        &env,
        &dir,
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            .target_file_size(16 << 10),
    )
    .await
    .expect("open");
    let mut model = Model::new();
    let mut batch = Batch::new();
    for i in 0..4000 {
        let key = format!("k{i:04}").into_bytes();
        batch.put(&key, &[b'o'; 100]).expect("put");
        model.insert(key, vec![b'o'; 100]);
    }
    db.write_raw(&mut batch, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");
    let l1 = db.compact(0, &[table]).await.expect("compact into L1");
    db.compact(1, &l1).await.expect("compact into L2");

    // The tombstone and, after it in the same batch, a key in every four of
    // the span: enough to split the L1 output, with the tombstone cut across
    // the pieces.
    let mut batch = Batch::new();
    batch
        .delete_range(b"k0100", b"k3900")
        .expect("delete range");
    model.retain(|k, _| k.as_slice() < b"k0100".as_slice() || k.as_slice() >= b"k3900".as_slice());
    for i in (101..3900).step_by(4) {
        let key = format!("k{i:04}").into_bytes();
        batch.put(&key, &[b'n'; 100]).expect("put");
        model.insert(key, vec![b'n'; 100]);
    }
    db.write_raw(&mut batch, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");
    let l1 = db.compact(0, &[table]).await.expect("compact into L1");
    assert!(
        l1.len() >= 3,
        "the span cut across L1, not {} tables",
        l1.len()
    );
    db.compact(1, &l1[l1.len() - 1..])
        .await
        .expect("compact the last part into L2");

    let want: Vec<String> = model
        .keys()
        .map(|k| String::from_utf8(k.clone()).expect("utf8"))
        .collect();
    let mut scan = db.range(..).expect("scan");
    let mut forward = Vec::new();
    scan.first().await.expect("first");
    while scan.is_valid() {
        forward.push(String::from_utf8(scan.key().to_vec()).expect("utf8"));
        scan.next().await.expect("next");
    }
    assert_eq!(forward, want, "forward");
    let mut backward = Vec::new();
    scan.last().await.expect("last");
    while scan.is_valid() {
        backward.push(String::from_utf8(scan.key().to_vec()).expect("utf8"));
        scan.prev().await.expect("prev");
    }
    backward.reverse();
    assert_eq!(backward, want, "backward");

    // Every seek a floor or a ceiling makes from inside the span, against
    // the model.
    for i in (100..3900).step_by(7) {
        let probe = format!("k{i:04}~").into_bytes();
        let floor = db.get(&probe, Comparison::Floor).await.expect("floor");
        let want = model
            .range(..probe.clone())
            .next_back()
            .map(|(k, _)| k.clone());
        assert_eq!(floor.map(|f| f.key), want, "floor of k{i:04}~");
        let ceiling = db.get(&probe, Comparison::Ceiling).await.expect("ceiling");
        let want = model.range(probe.clone()..).next().map(|(k, _)| k.clone());
        assert_eq!(ceiling.map(|f| f.key), want, "ceiling of k{i:04}~");
    }
}

/// A table whose keys are out of order ends a scan that skips a range
/// tombstone over it, with an error or without one, rather than sending it
/// round the same keys forever.
///
/// Found by the `open_corrupt` fuzz target, which skips checksums, as one bit
/// pattern flipped in a key of `db-reads`' L6 table. Reproduced here with the
/// block's checksum computed again, as a table written wrongly would carry
/// one. The flipped key sorts after everything after it in the table, so a
/// skip that seeks past an L0 tombstone lands on it, and the next step goes
/// back into the span the tombstone hides: before the scan noticed it had
/// been there, that was a skip, the same landing and the same step, for good.
#[compio::test]
async fn a_scan_over_keys_out_of_order_ends_rather_than_skipping_round_a_tombstone_forever() {
    let (env, dir) = load("db-reads").await;
    let fs: &SimFs = env.sim();
    let path = dir.join("000019.sst");
    let mut bytes = std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-reads/000019.sst"),
    )
    .expect("the L6 table");

    // The fuzz case's damage, and the checksum of the data block it lands
    // in, found through the table's index.
    let (at, mask) = (127, 0xd2);
    bytes[at] ^= mask;
    let footer = roble_format::Footer::decode(
        &bytes[bytes.len() - roble_format::FOOTER_LEN..],
        bytes.len() as u64,
    )
    .expect("footer");
    let index = bytes
        [footer.index.offset as usize..(footer.index.offset + footer.index.length) as usize]
        .to_vec();
    let mut entries = roble_format::block::Block::new(&index)
        .expect("index")
        .iter();
    let mut fixed = false;
    while let Some(entry) = entries.next() {
        let (handle, _) =
            roble_format::BlockHandle::decode(entry.expect("index entry").value).expect("handle");
        let (start, end) = (
            handle.offset as usize,
            (handle.offset + handle.length) as usize,
        );
        if (start..end).contains(&at) {
            let sum = footer.checksum.compute(&bytes[start..end], bytes[end]);
            bytes[end + 1..end + 5].copy_from_slice(&sum.to_le_bytes());
            fixed = true;
        }
    }
    assert!(fixed, "byte {at} is in a data block");
    let disk = fs.create(&path).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
    result.expect("write");
    disk.close().await.expect("close");

    let db = roble::open_with(
        &env,
        &dir,
        Options::new().disable_automatic_compactions(true),
    )
    .await
    .expect("open");
    let mut scan = db.range(..).expect("scan");
    // Forward and backward, a step at a time, each bounded well past the
    // keys the store holds.
    for forward in [true, false] {
        let mut moved = if forward {
            scan.first().await
        } else {
            scan.last().await
        };
        let mut steps = 0;
        while moved.is_ok() && scan.is_valid() {
            steps += 1;
            assert!(
                steps < 10_000,
                "a {} scan is still going",
                if forward { "forward" } else { "backward" }
            );
            moved = if forward {
                scan.next().await
            } else {
                scan.prev().await
            };
        }
    }
}
