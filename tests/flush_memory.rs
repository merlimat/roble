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

//! How much a flush holds at once, which is a property and not a
//! measurement.
//!
//! A flush writes exactly one table, so there is no splitting to be done and
//! nothing to hand onward: the only question is whether its bytes leave the
//! process as they are finished or at the end. Buffering them all costs the
//! memtable bound — 64 MiB as Oxia configures it, and more than that while a
//! `Vec` doubles — on a node running hundreds of stores, each of which may be
//! flushing. A benchmark of one store cannot see that; this can.
//!
//! What should be held is what Pebble's writer holds, named by the comment on
//! `RawRowWriter.EstimatedSize`: the open data block, the open index block,
//! the open range-deletion block — plus, here, the filter's hashes and the
//! finished bytes still waiting for a write (§6, `roble::flush`).
//!
//! The counter is a global allocator, which is per test binary, and its
//! numbers are only this test's if nothing else is allocating beside it — so
//! this is a file with one test in it, and a second one would have to go in a
//! file of its own. The peak is taken as the **difference** across the flush,
//! so nothing the writing before it did is charged to it.

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

/// A flush holds a block of its table, not the table.
#[compio::test]
async fn a_flush_holds_a_chunk_of_its_table_and_not_the_table() {
    /// Roughly how large a table this flush writes. Small beside Oxia's
    /// 64 MiB memtable, and still sixteen times the chunk a flush gathers
    /// before writing, so holding the table and holding a chunk are far
    /// enough apart that no threshold in between is a judgement call.
    const TABLE: u64 = 16 << 20;
    /// The value size roble is built for: Oxia's records are small.
    const VALUE: usize = 256;

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db");
    let db = roble::open_with(
        &roble::DefaultEnv,
        &dir,
        roble::Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            // Four times what is written below, so the write that fills the
            // memtable is not the thing that flushes it: this measures the
            // flush it asks for, by itself.
            .memtable_size(4 * TABLE),
    )
    .await
    .expect("open");

    let value = vec![b'v'; VALUE];
    let mut batch = Batch::new();
    for i in 0..(TABLE / VALUE as u64) {
        batch
            .put(format!("k{i:08}").as_bytes(), &value)
            .expect("put");
    }
    db.write_raw(&mut batch, true).await.expect("write");
    drop(batch);

    // From here, and not from the start: the batch and the memtable are the
    // writing's bytes, not the flush's. The frozen memtable is among them —
    // it is the one that was already live — so what this measures is what
    // the flush adds on top of the data it is writing out.
    let before = LIVE.load(Relaxed);
    PEAK.store(before, Relaxed);
    db.flush().await.expect("flush").expect("a table");
    let peak = PEAK.load(Relaxed) - before;

    // A quarter of the table and not a sixteenth, because a flush holds more
    // than the chunk it is about to write: the index block grows with the
    // number of data blocks, the filter's hashes with the number of keys,
    // and both are the table's own and stay to the end — 1.6 MiB together
    // with the chunk, measured, for this table. What this refuses is a peak
    // that counts the *table*: buffering it was 16 MiB, and more while the
    // buffer doubled, so it would fail here by an order of magnitude.
    let allowed = TABLE / 4;
    assert!(
        peak < allowed,
        "a flush of a {TABLE}-byte table held {peak} bytes at once, against {allowed} allowed \
         — {:.0}% of the table, so its bytes are not reaching the disk as they are finished",
        100.0 * peak as f64 / TABLE as f64
    );
}
