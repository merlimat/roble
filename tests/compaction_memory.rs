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
//! twenty. The first half of the property is that each output is written and
//! let go of as it is finished, so the peak is one table's and not the whole
//! job's — and the job's is bounded by L0's depth times a memtable (§9,
//! `crate::pick`), which is far more than a table and is per store on a node
//! running hundreds of them. The second half is that it is not even one
//! table's: the bytes go to the disk as the writer finishes each block, so
//! what is held is a chunk of one output plus the index and filter the format
//! makes the writer keep to the end.
//!
//! The second half is why the target here is megabytes rather than the
//! kilobytes that would show the first. A target below the chunk a write
//! gathers is written in the single write `finish` makes, so an output has to
//! be larger than that chunk before streaming it is even possible — and the
//! targets that matter are: one doubles per level, so L6's is 128 MiB. A
//! benchmark of one store cannot see any of this; this can.
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

/// A compaction holds part of one output table, not all of them and not one.
#[compio::test]
async fn a_compaction_holds_part_of_one_output_table() {
    // L1's target is twice this, so 4 MiB: comfortably above the megabyte a
    // write gathers, so each output is written in several chunks rather than
    // in one.
    const BASE: u64 = 2 << 20;

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db");
    let db = roble::open_with(
        &roble::DefaultEnv,
        &dir,
        roble::Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            .target_file_size(BASE)
            // Larger than the job below, so the write that loads it does not
            // flush part way through and leave two L0 tables.
            .memtable_size(64 << 20),
    )
    .await
    .expect("open");

    // The target this compaction splits on: L1's, which is twice L0's,
    // because every level below aims at twice the one above.
    let target = db.target_file_size(1);
    // Enough for eight tables, so that holding them all is eight times
    // holding one and no threshold in between is a judgement call. Eight
    // rather than sixteen because the target is now megabytes: the job is
    // what has to be written and merged, and this is a debug build.
    let job = 8 * target;

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
        outputs.len() >= 4,
        "{job} bytes against a {target} target should be several outputs, got {outputs:?}"
    );
    // Less than *one* target, which is the whole of what streaming the blocks
    // buys and is three numbers in a row: buffering every output held
    // thirty-five times the target, letting each go as it was finished held
    // three, and writing each block as the writer finishes it holds 0.6 —
    // measured. Three times the target is what this refuses now, and what it
    // used to allow.
    //
    // 0.6 and not 0.25: at the moment one output ends and the next begins,
    // the bytes of the one closing and the buffer handed on to the one
    // opening are both alive, so the chunk is paid for twice over.
    let allowed = target;
    assert!(
        peak < allowed,
        "a compaction of {} outputs held {peak} bytes at once, against a {target} target and \
         {allowed} allowed — {:.1} tables' worth, so a table is not reaching the disk as it is \
         built",
        outputs.len(),
        peak as f64 / target as f64
    );
}
