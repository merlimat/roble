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

//! Pacing a long job so the thread it runs on stays usable (§9).
//!
//! Compaction is the only background CPU work roble does, and it runs on the
//! owning thread. A merge that held the thread from its first key to its last
//! would stop every read and every write on that core for as long as it took
//! — which is the objection to cooperative compaction, and the answer is to
//! make it cooperative in fact and not only in name: merge a slice, hand the
//! executor back, resume.
//!
//! **A unit is work, not time.** The pacer counts entries, not milliseconds,
//! because §10 lets a store read the clock only where the env allows it and
//! a job that yielded on a timer would stop a seeded run replaying. Counting
//! what was done gives the same slices every run, on every machine.
//!
//! Yielding is not free — it is a wake and a trip back through the executor —
//! so the unit is large enough that the trip is noise against the merging
//! either side of it, and small enough that nothing waits long. What it is
//! *not* is a guarantee: a unit of 4096 entries takes as long as 4096 entries
//! take, and a store with enormous values will see longer slices than one
//! with small ones. The number a caller can set is the lever for that.

use std::future::poll_fn;
use std::task::Poll;

/// How many entries a compaction merges before it offers the thread back.
///
/// Roughly a few hundred microseconds of merging for Oxia's shape — small
/// records, and values of a few hundred bytes.
pub const COMPACTION_UNIT: u64 = 4096;

/// Counts work done, and yields each time a unit of it is.
pub(crate) struct Pacer {
    unit: u64,
    done: u64,
}

impl Pacer {
    /// A pacer that yields every `unit` of work, or never when `unit` is
    /// zero — which is what a caller asks for by setting it so.
    pub(crate) fn new(unit: u64) -> Self {
        Pacer { unit, done: 0 }
    }

    /// Takes account of one more piece of work, yielding if a unit is up.
    pub(crate) async fn did(&mut self, work: u64) {
        if self.unit == 0 {
            return;
        }
        self.done += work;
        if self.done >= self.unit {
            self.done = 0;
            yield_now().await;
        }
    }
}

/// Hands the executor back, to be woken straight away.
///
/// A ready task behind this one gets its turn before this one resumes, which
/// is the whole of "cooperative". Written out rather than taken from a
/// runtime crate because compio has none and the whole of it is four lines.
pub(crate) async fn yield_now() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        // Woken before returning `Pending`, so the executor runs whatever is
        // ready and then comes back here rather than parking.
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}
