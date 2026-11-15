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

//! What a lookup that misses the block cache allocates for the block.
//!
//! A block is read whole, trailer and all, and the bytes a lookup needs are
//! that read less its last five bytes. A miss used to copy them out to drop
//! the trailer, which made two block-sized allocations and a 16 KiB copy of
//! every block read where one allocation and no copy would do — and at the
//! sizes Oxia runs at, with the store several times the cache, a miss is the
//! common case rather than the exception.
//!
//! Counted by size rather than in all. The runtime's own allocations per read
//! are small and differ by platform — compio's driver makes more on macOS
//! than on Linux — and what this pins is the one the size of a block.
//!
//! The counter is a global allocator, which is per test binary, and its
//! numbers are only this test's if nothing else is allocating beside it — so
//! this is a file with one test in it, as `flush_memory.rs` is.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use roble::cache::DEFAULT_OPEN_TABLES;
use roble::{Caches, Comparison, Lookup};
use roble_format::batch::Batch;

/// An allocation at least this big is a block's: data blocks are written at
/// 16 KiB, and nothing else a warm lookup does comes near half that.
const BLOCK_SIZED: usize = 8 << 10;

/// How many block-sized allocations the process has made.
static BLOCK_SIZED_ALLOCS: AtomicU64 = AtomicU64::new(0);

struct Counting;

#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() >= BLOCK_SIZED {
            BLOCK_SIZED_ALLOCS.fetch_add(1, Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        if new >= BLOCK_SIZED {
            BLOCK_SIZED_ALLOCS.fetch_add(1, Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// A lookup that misses reads its block into the buffer it then keeps: one
/// block-sized allocation for each block read, not two.
///
/// The block cache is given a single byte, so it keeps nothing and every
/// lookup reads its block afresh. One table holds the keys, its index and
/// filter are held open with it, and a point lookup for a key it holds reads
/// exactly one data block — so the count is one per lookup.
///
/// Exactly one on Linux. On macOS compio's driver now and then makes a
/// block-sized allocation of its own — five in two hundred lookups on one run,
/// none on the next — so the bound is on what tells the two cases apart:
/// under one and a half a read, where the copy made it two.
#[compio::test]
async fn a_missed_block_is_kept_in_the_buffer_it_was_read_into() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("db");
    let db = roble::open_with(
        &roble::DefaultEnv,
        &dir,
        roble::Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            .caches(Caches::new(DEFAULT_OPEN_TABLES, 1)),
    )
    .await
    .expect("open");

    // About thirteen blocks' worth, so the probe's block is a full one.
    let value = vec![b'v'; 200];
    let mut batch = Batch::new();
    for i in 0..1000u64 {
        batch
            .put(format!("k{i:05}").as_bytes(), &value)
            .expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");

    let probe = b"k00500";
    let mut buf = Lookup::new();
    // Warm: the table opens and the buffer reaches the value's size. The
    // block cannot warm, which is the point.
    for _ in 0..50 {
        assert!(
            db.get_into(probe, Comparison::Equal, &mut buf)
                .await
                .expect("get_into"),
            "the probe is a key the table holds"
        );
    }

    const RUNS: u64 = 200;
    let before = BLOCK_SIZED_ALLOCS.load(Relaxed);
    for _ in 0..RUNS {
        db.get_into(probe, Comparison::Equal, &mut buf)
            .await
            .expect("get_into");
    }
    let made = BLOCK_SIZED_ALLOCS.load(Relaxed) - before;

    assert!(
        (RUNS..RUNS * 3 / 2).contains(&made),
        "{RUNS} lookups that each read one block made {made} block-sized \
         allocations: {:.2} a read, where the buffer the block was read into \
         is the one to keep",
        made as f64 / RUNS as f64
    );
}
