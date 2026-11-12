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
//! **A write does not take it**, and must not: a write that waited on the
//! permit would be waiting on whatever merge happened to be going on. What
//! makes a write safe against a flush is the other half of this module.
//!
//! # A write against a flush
//!
//! A flush freezes the memtable before its first await and sends the writes
//! that follow to a fresh one and a fresh log (`crate::flush`), which is
//! Pebble's answer and the machinery the memtable write stall will need
//! (§9). That leaves one window, and it is the narrow kind that loses an
//! acknowledged write:
//!
//! A write appends its record to the log, waits for that, and only then
//! applies it to the memtable — the order that keeps anything readable
//! recoverable (§6). A flush that froze in between would build its table
//! from a memtable the write has not reached yet, and retire the log holding
//! the record: the write returned `Ok`, and both copies of it are gone.
//!
//! So freezing waits. Nothing more is added to a frozen memtable, and a
//! write that has chosen one and not yet applied to it is something still
//! being added. [`Writers`] counts those, and a freeze does not swap until
//! the count is zero. It is a counter and not a permit because writes do not
//! exclude each other — there is nothing wrong with two writes at once, only
//! with freezing underneath one.
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

/// The writes that have chosen a memtable and not yet applied to it.
///
/// Read by a freeze, which cannot take a memtable away from a write that is
/// still adding to it (`crate::open::Opened::freeze_memtable`).
#[derive(Default)]
pub(crate) struct Writers {
    in_flight: Cell<usize>,
    waiting: RefCell<Vec<Waker>>,
}

impl Writers {
    /// Counts one write as under way, until the [`Writing`] is dropped.
    ///
    /// Dropped on the way out however the write ends, a failure or a
    /// cancellation included — which is the reason this is a guard and not a
    /// pair of calls. A write whose future is thrown away between its log
    /// append and its memtable would otherwise stall every later flush.
    pub(crate) fn begin(&self) -> Writing<'_> {
        self.in_flight.set(self.in_flight.get() + 1);
        Writing { writers: self }
    }

    /// Waits until every write that has chosen a memtable has applied to it.
    ///
    /// Asked again on every poll rather than trusted once: a write that
    /// started between a wake-up and the poll it woke is as much in flight
    /// as one that was already there, and the caller is about to freeze.
    pub(crate) async fn settled(&self) {
        poll_fn(|cx| {
            if self.in_flight.get() > 0 {
                self.waiting.borrow_mut().push(cx.waker().clone());
                return Poll::Pending;
            }
            Poll::Ready(())
        })
        .await
    }
}

/// A write under way, counted until it is dropped.
pub(crate) struct Writing<'a> {
    writers: &'a Writers,
}

impl Drop for Writing<'_> {
    fn drop(&mut self) {
        self.writers.in_flight.set(self.writers.in_flight.get() - 1);
        // Woken whether or not the count reached zero, and every waiter of
        // them, for the reasons written against `Change`'s drop below: a
        // waiter re-checks, so an extra poll is the cheap mistake and a
        // missed wake-up is the expensive one.
        let waiting = std::mem::take(&mut *self.writers.waiting.borrow_mut());
        for waker in waiting {
            waker.wake();
        }
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use super::Writers;

    /// A waker that only records that it was woken.
    struct Flag(AtomicBool);

    impl Wake for Flag {
        // `wake_by_ref` is the trait's own default, which clones and calls
        // this: one line fewer, and one line fewer that no test runs.
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Here rather than in `tests/freeze.rs`, where the rest of the freeze is
    /// pinned, because no test through the public API can reach this: a write
    /// stays in flight only across an await, the simulated filesystem never
    /// blocks, and so a write that is polled twice is a write that has
    /// finished (§10, §12). What that leaves untested from outside is the
    /// wake-up, and a freeze that waits and is never woken stalls every
    /// flush after it.
    #[test]
    fn a_freeze_waits_for_a_write_in_flight_and_is_woken_by_it() {
        let writers = Writers::default();
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&flag));
        let mut cx = Context::from_waker(&waker);

        let writing = writers.begin();
        let mut settled = pin!(writers.settled());
        assert!(
            matches!(settled.as_mut().poll(&mut cx), Poll::Pending),
            "a freeze must wait while a write is still adding to the memtable"
        );
        assert!(
            !flag.0.load(Ordering::SeqCst),
            "and must not be woken for nothing"
        );

        drop(writing);
        assert!(
            flag.0.load(Ordering::SeqCst),
            "the write that finished must wake the freeze waiting on it"
        );
        assert!(
            matches!(settled.as_mut().poll(&mut cx), Poll::Ready(())),
            "and the freeze must then go through"
        );
    }

    #[test]
    fn a_freeze_waits_for_every_write_and_not_for_one_of_them() {
        let writers = Writers::default();
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&flag));
        let mut cx = Context::from_waker(&waker);

        let first = writers.begin();
        let second = writers.begin();
        let mut settled = pin!(writers.settled());
        assert!(matches!(settled.as_mut().poll(&mut cx), Poll::Pending));

        drop(first);
        assert!(
            matches!(settled.as_mut().poll(&mut cx), Poll::Pending),
            "one of two writes finishing is not the memtable settling"
        );
        drop(second);
        assert!(
            flag.0.load(Ordering::SeqCst),
            "the last write to finish must wake the freeze"
        );
        assert!(matches!(settled.as_mut().poll(&mut cx), Poll::Ready(())));
    }
}
