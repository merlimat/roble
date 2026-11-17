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

//! How often a read allocates, which is a property and not a measurement.
//!
//! A point lookup through a [`roble::GetBuf`] the caller hands back is meant
//! to allocate a bounded number of times however much data the store holds —
//! not "usually", and not "a bit more each release". That only stays true if
//! something fails when it stops being true: the cost it is guarding against
//! grew for months without anyone noticing, because a benchmark reports a
//! rate and a rate hides an allocation.
//!
//! This file has a global allocator of its own, so it has to be its own test
//! binary. The count is taken as the **difference** across a run of lookups,
//! after a warm-up, so nothing start-up does is charged to it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use roble::{Comparison, GetBuf};

static ALLOCS: AtomicU64 = AtomicU64::new(0);

struct Counting;

// SAFETY: every method forwards to `System` with the layout it was given and
// returns what `System` returned, so the allocator contract is `System`'s.
// The counter is the only addition.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        unsafe { System.realloc(ptr, layout, new) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Held by each test for the whole of its run. The counter is global and the
/// harness runs tests on threads of their own, so two at once would each
/// count the other's allocations.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn fixture(name: &str) -> (tempfile::TempDir, PathBuf) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    let tmp = tempfile::tempdir().expect("temp dir");
    let dst = tmp.path().join(name);
    std::fs::create_dir(&dst).expect("create");
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), dst.join(entry.file_name())).expect("copy");
    }
    (tmp, dst)
}

/// A warm `get_into` allocates **nothing at all**.
///
/// The fixture is the database the read probes are asked of: tables that
/// overlap in L0 above one in L6, so a lookup searches more than one of them
/// and a per-table allocation would show up as more than one per lookup.
#[compio::test]
// Held across the awaits on purpose: `SERIAL` is what keeps the other test
// out of the count, and each test runs on a runtime of its own thread.
#[allow(clippy::await_holding_lock)]
async fn a_warm_point_lookup_allocates_nothing() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (_tmp, dir) = fixture("db-reads");
    let db = roble::open(&roble::DefaultEnv, &dir)
        .await
        .expect("open the database Go wrote");

    // Whatever the fixture's first key is, reached without assuming one.
    let probe = {
        let mut scan = db.range(..).expect("scan");
        scan.seek_ge(b"").await.expect("seek");
        assert!(scan.is_valid(), "the fixture should hold at least one key");
        scan.key().to_vec()
    };

    let mut buf = GetBuf::new();
    // Warm: the buffers reach the sizes this key needs, the tables open, and
    // the blocks enter the cache. None of that is what is being counted.
    for _ in 0..500 {
        assert!(
            db.get_into(&probe, Comparison::Equal, &mut buf)
                .await
                .expect("get_into"),
            "the key a scan handed back should be readable"
        );
    }

    const RUNS: u64 = 2_000;
    let before = ALLOCS.load(Relaxed);
    for _ in 0..RUNS {
        db.get_into(&probe, Comparison::Equal, &mut buf)
            .await
            .expect("get_into");
    }
    let made = ALLOCS.load(Relaxed) - before;

    // Zero, not "few". The buffers are the caller's and warm, the tables are
    // open, the blocks are cached, and the block cursor is handed back with
    // them — so there is nothing left for a repeated lookup to ask the
    // allocator for. Anything above zero here is a new allocation on the
    // serving path, and the number says how often.
    assert_eq!(
        made,
        0,
        "{RUNS} warm point lookups allocated {made} times, {:.3} each",
        made as f64 / RUNS as f64
    );
}

/// A warm `floor` or `ceiling` through a kept `GetBuf` allocates nothing
/// either.
///
/// The four ordered comparisons are a scan positioned once and read, and a
/// scan has far more to build than a point lookup: a source for each level
/// and each memtable, a heap over them, its bounds, the internal key it
/// seeks to, and a block cursor for every table it opens. A `GetBuf` keeps
/// one scan parked between calls and rebuilds it in its own allocations, so
/// once the first few calls have grown every buffer to the size the data
/// needs there is nothing left to ask the allocator for — this is what says
/// so. Both directions, because a backward seek and a forward one reach
/// their answer through different code, and on keys either side of every
/// stored one, so a probe lands on a key and between two.
#[compio::test]
// Held across the awaits on purpose: `SERIAL` is what keeps the other test
// out of the count, and each test runs on a runtime of its own thread.
#[allow(clippy::await_holding_lock)]
async fn a_warm_ordered_lookup_allocates_nothing() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (_tmp, dir) = fixture("db-reads");
    let db = roble::open(&roble::DefaultEnv, &dir)
        .await
        .expect("open the database Go wrote");

    let mut probes = Vec::new();
    {
        let mut scan = db.range(..).expect("scan");
        scan.first().await.expect("first");
        while scan.is_valid() && probes.len() < 64 {
            probes.push(scan.key().to_vec());
            probes.push([scan.key(), b"!"].concat());
            scan.next().await.expect("next");
        }
    }
    assert!(probes.len() > 2, "the fixture should hold keys to probe");

    let mut buf = GetBuf::new();
    let comparisons = [Comparison::Floor, Comparison::Ceiling];
    // Warm, as the point lookup above is: every buffer grown, every table
    // open, every block cached.
    for _ in 0..20 {
        for probe in &probes {
            for how in comparisons {
                db.get_into(probe, how, &mut buf).await.expect("get_into");
            }
        }
    }

    const ROUNDS: u64 = 20;
    let before = ALLOCS.load(Relaxed);
    for _ in 0..ROUNDS {
        for probe in &probes {
            for how in comparisons {
                db.get_into(probe, how, &mut buf).await.expect("get_into");
            }
        }
    }
    let made = ALLOCS.load(Relaxed) - before;
    let lookups = ROUNDS * probes.len() as u64 * comparisons.len() as u64;
    assert_eq!(
        made,
        0,
        "{lookups} warm floors and ceilings allocated {made} times, {:.3} each",
        made as f64 / lookups as f64
    );
}

/// A warm write into a memtable with room allocates **nothing** either, with
/// the WAL off and nothing recording — Oxia's configuration.
///
/// Its bytes go into the memtable's arena, which past its first megabyte has
/// the whole budget (`crate::memtable`), and everything else a write does is
/// bookkeeping: whether there is room, whether L0 is too deep to accept more,
/// what the metrics would count. None of that has anything to keep, and two
/// pieces of it allocated every time — L0's depth was counted by building
/// and sorting a list of its tables, and the batch was read a second time
/// into a list for counters nothing was recording. The fixture's L0 holds
/// tables, so the first would show here.
#[compio::test]
#[allow(clippy::await_holding_lock)]
async fn a_warm_write_allocates_nothing() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (_tmp, dir) = fixture("db-reads");
    let db = roble::open_with(
        &roble::DefaultEnv,
        &dir,
        // Compactions left on, which is what makes a write look at L0's
        // depth at all; nothing here spawns one, and L0 is far from the line.
        roble::Options::new()
            .disable_wal(true)
            .memtable_size(4 << 20),
    )
    .await
    .expect("open the database Go wrote");
    assert!(
        !db.versions().version.level(0).is_empty(),
        "the fixture should hold tables in L0, for a write to count their depth"
    );

    const RUNS: usize = 2_000;
    let keys: Vec<Vec<u8>> = (0..1_000 + RUNS)
        .map(|i| format!("w{i:06}").into_bytes())
        .collect();
    let value = [b'v'; 100];
    let mut batch = roble_format::batch::Batch::new();

    // Warm: past the megabyte at which the arena takes its budget, and the
    // batch's buffer at its size.
    for _ in 0..12 {
        for key in &keys[..1_000] {
            batch.clear();
            batch.put(key, &value).expect("put");
            db.write(&mut batch, false).await.expect("write");
        }
    }

    let before = ALLOCS.load(Relaxed);
    for key in &keys[1_000..] {
        batch.clear();
        batch.put(key, &value).expect("put");
        db.write(&mut batch, false).await.expect("write");
    }
    let made = ALLOCS.load(Relaxed) - before;
    assert_eq!(
        made,
        0,
        "{RUNS} warm writes allocated {made} times, {:.3} each",
        made as f64 / RUNS as f64
    );
}
