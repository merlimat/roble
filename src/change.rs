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

//! One change at a time to what a database is made of (§5).
//!
//! §5 says "one flush and one compaction at a time per store", and until now
//! nothing made it so: it was true because a store's own calls never
//! overlapped, which is a fact about callers and not about roble. A caller
//! with two tasks on one store could interleave them at any await, and every
//! one of these operations reads the version, does I/O, and then writes an
//! answer derived from what it read:
//!
//! - **Two flushes.** Both read `next_file_num` before either applies an
//!   edit, so both write the same table file, the second over the first, and
//!   the MANIFEST ends up naming a table whose bytes belong to the other.
//!   Both also swap in a fresh memtable, so writes that landed between the
//!   two swaps belong to a memtable nothing holds any more — and the second
//!   edit retires the log that was keeping them.
//! - **A sweep against a flush.** The sweep decides what is obsolete from the
//!   version, then deletes across an await. A flush that installs a table in
//!   between has installed a table the sweep's answer does not know about,
//!   and the sweep deletes it.
//! - **A compaction against anything.** Its inputs come from the version it
//!   started on; two compactions can choose the same ones, and the second's
//!   edit then deletes tables the first already did — which is the refusal
//!   `crate::flush` turns into the end of a store's writing life.
//!
//! So there is one permit, and everything that changes what the database is
//! made of takes it: a flush, a compaction, a MANIFEST rotation, the obsolete
//! sweep. A [`Change`] is that permit, and `append_version_edit` will not
//! take an edit without one — the hazard cannot come back by someone adding
//! a path that forgets to ask.
//!
//! **A write does not take it**, and so is not yet safe against a flush: a
//! write that lands after a flush has read the memtable and before it swaps
//! in a fresh one is in a memtable the flush is about to drop. Serializing
//! writes against flushes would close that, and would be the wrong fix —
//! Pebble's is to make the memtable immutable at the *start* of a flush and
//! send writes to a new one, which is the same machinery the memtable write
//! stall needs (§9). Until then, one task per store is the rule, as it has
//! always in fact been.
//!
//! Waiting rather than refusing, because the second caller asked a
//! reasonable question: a flush that waits for the one in front of it finds
//! an empty memtable and says it had nothing to write, which is true. A
//! refusal would make a caller handle an error that is about roble's
//! scheduling rather than about its database.

use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::task::{Poll, Waker};

/// The one change at a time a store makes to what it is made of.
///
/// Written by hand because compio has no mutex of its own and this is forty
/// lines — the same reasoning as `crate::work::yield_now`, and the same
/// constraint from §10: no clock is read, so a seeded run replays.
#[derive(Default)]
pub(crate) struct Changes {
    underway: Cell<bool>,
    waiting: RefCell<Vec<Waker>>,
}

impl Changes {
    /// Waits for the permit, and holds it until the [`Change`] is dropped.
    pub(crate) async fn begin(&self) -> Change<'_> {
        poll_fn(|cx| {
            if self.underway.get() {
                self.waiting.borrow_mut().push(cx.waker().clone());
                return Poll::Pending;
            }
            self.underway.set(true);
            Poll::Ready(())
        })
        .await;
        Change { changes: self }
    }
}

/// A change underway, and the proof that no other one is.
///
/// Passed to the operations that may only happen one at a time, so that the
/// permit is not something a caller has to remember: a function that needs it
/// says so in its signature.
pub(crate) struct Change<'a> {
    changes: &'a Changes,
}

impl Drop for Change<'_> {
    fn drop(&mut self) {
        self.changes.underway.set(false);
        // Every waiter woken, not the first in line. A task polled twice
        // while waiting has registered twice, so waking one waker can mean
        // waking a task that was going to run anyway — and then nothing
        // wakes the others and the permit is never taken again. Waking all
        // of them costs a poll each, and cannot stall.
        //
        // Taken out of the cell before any of them is woken: a waker is
        // somebody else's code, and it must not find this borrowed.
        let waiting = std::mem::take(&mut *self.changes.waiting.borrow_mut());
        for waker in waiting {
            waker.wake();
        }
    }
}
