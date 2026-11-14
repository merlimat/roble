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

//! How much a compaction holds at once, which is a property and not a
//! measurement.
//!
//! A merge splits its output at the target file size, and on its own that
//! decides only how many tables there are: buffering them all until the
//! merge is over costs the same bytes whether they are in one allocation or
//! twenty. What the property is, is that each output is written and let go
//! of as it is finished, so the peak is one table's and not the whole job's
//! — and the job's is bounded by L0's depth times a memtable (§9,
//! `crate::pick`), which is far more than a table and is per store on a node
//! running hundreds of them. A benchmark of one store cannot see that; this
//! can.
//!
//! The counter is a global allocator, which is per test binary, and its
//! numbers are only this test's if nothing else is allocating beside it — so
//! this is a file with one test in it, and a second one would have to go in
//! a file of its own. The peak is taken as the **difference** across one
//! compaction, so nothing the writing before it did is charged to the merge.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use roble_format::batch::Batch;

/// Bytes allocated and not yet freed.
static LIVE: AtomicU64 = AtomicU64::new(0);
/// The most [`LIVE`] has held since the test last stored to it.
static PEAK: AtomicU64 = AtomicU64::new(0);

/// Takes account of live bytes changing by `delta`, negative for a block
/// freed or shrunk.
///
/// `LIVE` wraps around rather than saturating, which is what makes a signed
/// delta on an unsigned counter come out right: the true total never goes
/// below zero, so every value it passes through is the one it should be.
fn grew(delta: i64) {
    let live = LIVE
        .fetch_add(delta as u64, Relaxed)
        .wrapping_add(delta as u64);
    PEAK.fetch_max(live, Relaxed);
}

struct Tracking;

// SAFETY: every method forwards to `System` with the layout it was given and
// returns what `System` returned, so the allocator contract is `System`'s.
// The counters are the only addition.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grew(layout.size() as i64);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        grew(-(layout.size() as i64));
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(ptr, layout, new) };
        if !moved.is_null() {
            grew(new as i64 - layout.size() as i64);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Tracking = Tracking;

/// A compaction holds one output table, not all of them.
#[compio::test]
async fn a_compaction_holds_one_output_table_at_a_time() {
    const BASE: u64 = 32 * 1024;

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db");
    let db = roble::open_with(
        &roble::DefaultEnv,
        &dir,
        roble::Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            .target_file_size(BASE),
    )
    .await
    .expect("open");

    // The target this compaction splits on: L1's, which is twice L0's,
    // because every level below aims at twice the one above.
    let target = db.target_file_size(1);
    // Enough for sixteen tables, so that holding them all is sixteen times
    // holding one and no threshold in between is a judgement call.
    let job = 16 * target;

    // One L0 table holding the lot, which compacts into several.
    let value = vec![b'v'; 256];
    let mut batch = Batch::new();
    for i in 0..(job / 256) {
        batch
            .put(format!("k{i:08}").as_bytes(), &value)
            .expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    drop(batch);
    let table = db.flush().await.expect("flush").expect("a table");

    // From here, and not from the start: the batch and the memtable are the
    // writing's bytes, not the merge's.
    let before = LIVE.load(Relaxed);
    PEAK.store(before, Relaxed);
    let outputs = db.compact(0, &[table]).await.expect("compact");
    let peak = PEAK.load(Relaxed) - before;

    assert!(
        outputs.len() >= 8,
        "{job} bytes against a {target} target should be several outputs, got {outputs:?}"
    );
    // Six times the target and not once, because one table costs more than
    // its bytes: a `TableWriter` builds the whole thing before it says what
    // the bytes are, and grows its buffer by doubling, and the finished
    // bytes are held until the write is done with them — three times the
    // target, measured. What this refuses is a peak that counts the
    // *outputs*: sixteen tables' worth is thirty-five times the target, so
    // it would fail here even if two thirds of them were let go of.
    let allowed = 6 * target;
    assert!(
        peak < allowed,
        "a compaction of {} outputs held {peak} bytes at once, against a {target} target and \
         {allowed} allowed — {:.1} tables' worth, so they are not being let go of",
        outputs.len(),
        peak as f64 / target as f64
    );
}
