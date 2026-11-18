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

//! What a memtable holds while it fills: its budget, not twice it.
//!
//! A store's memtable fills to the budget it is written out at, so how large
//! its arena gets is known before the first write. Grown by doubling the way a
//! `Vec` grows, the arena reallocated at each power of two and held the old
//! copy and the new together while it moved — at 64 MiB, an 86 MiB allocation
//! and 129 MiB held for a moment, by a memtable whose budget is 64. Grown
//! straight to all it will hold once it is past a megabyte — the budget and the
//! batch that crosses it, with each record's header and links — it moves once
//! and small.
//!
//! The counter is a global allocator, which is per test binary, and its
//! numbers are only this test's if nothing else is allocating beside it — so
//! this is a file with one test in it, as `flush_memory.rs` is.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use roble_format::batch::Batch;

/// Bytes allocated and not yet freed.
static LIVE: AtomicU64 = AtomicU64::new(0);
/// The most [`LIVE`] has held since the test last stored to it.
static PEAK: AtomicU64 = AtomicU64::new(0);

fn grew(delta: i64) {
    let live = LIVE
        .fetch_add(delta as u64, Relaxed)
        .wrapping_add(delta as u64);
    PEAK.fetch_max(live, Relaxed);
}

struct Tracking;

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
        // As the new allocation before the old one goes, which is what a
        // realloc that moves holds for the length of its copy — and that
        // moment is what this file is about.
        grew(new as i64);
        let moved = unsafe { System.realloc(ptr, layout, new) };
        grew(-(layout.size() as i64));
        if moved.is_null() {
            grew(layout.size() as i64 - new as i64);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Tracking = Tracking;

#[compio::test]
async fn a_memtable_filling_up_holds_its_budget_and_not_twice_it() {
    /// The store's budget. Large enough that the arena passes the megabyte
    /// below which it grows the ordinary way, and that doubling it would have
    /// crossed eight megabytes and reallocated to sixteen.
    const BUDGET: u64 = 16 << 20;
    /// What is written: three quarters of the budget, keys and values. The
    /// arena holds a little more, for each node's header and links, and still
    /// fits the budget — so nothing here crosses it, and nothing is written
    /// out, since a store with no flush task beside it fills the whole budget
    /// before it flushes.
    const WRITTEN: u64 = BUDGET * 3 / 4;
    /// The value size roble is built for: Oxia's records are small.
    const VALUE: usize = 256;
    /// What a record costs the arena beyond its key and value: a 17-byte
    /// header and four bytes for each level it stands in, a third of a level
    /// over one on average.
    const BOOKKEEPING: f64 = 17.0 + 4.0 * 4.0 / 3.0;

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db");
    let db = roble::open_with(
        &roble::DefaultEnv,
        &dir,
        roble::Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            .wal(roble::Wal::Off)
            .memtable_size(BUDGET),
    )
    .await
    .expect("open");

    // Twice: the memtable the store opened with, and then — after a flush —
    // the one it replaced it with, since the two are made in different places
    // and each has to be told its budget.
    let value = vec![b'v'; VALUE];
    let entry = (format!("k{:08}", 0).len() + 8 + VALUE) as u64;
    let mut i = 0u64;
    for which in [
        "the memtable the store opened with",
        "the memtable a flush left",
    ] {
        let before = LIVE.load(Relaxed);
        PEAK.store(before, Relaxed);
        let from = i;
        while (i - from) * entry < WRITTEN {
            // In batches of a hundred, so a batch's own bytes are no part of
            // what is being measured.
            let mut batch = Batch::new();
            for _ in 0..100 {
                batch
                    .put(format!("k{i:08}").as_bytes(), &value)
                    .expect("put");
                i += 1;
            }
            db.write_raw(&mut batch, false).await.expect("write");
        }
        let held = PEAK.load(Relaxed) - before;
        assert!(
            db.unflushed_bytes() as u64 >= WRITTEN,
            "the writes have to be in the memtable for this to measure it"
        );

        // What the arena reserves, which is the budget and an eighth for the
        // batch that crosses it, at what these records cost it; then the
        // megabyte and a half it moved from, and a little for what writing
        // allocates beside it. Doubling put the arena's copy at 8 + 16 =
        // 24 MiB here when it was reserved at the budget, and would put it at
        // over 38 MiB now.
        let rate = (entry as f64 + BOOKKEEPING) / entry as f64;
        let arena = ((BUDGET + (BUDGET >> 3)) as f64 * rate) as u64;
        let allowed = arena + (2 << 20);
        assert!(
            held <= allowed,
            "filling {which}, {} MiB of budget, to {} MiB held {:.1} MiB at its peak, over \
             the {:.1} MiB its budget allows",
            BUDGET >> 20,
            WRITTEN >> 20,
            held as f64 / 1048576.0,
            allowed as f64 / 1048576.0
        );
        db.flush().await.expect("flush").expect("a table");
    }
}
