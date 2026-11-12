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
//! being added. [`Writers`] is that permit, and a freeze does not swap until
//! no write holds it.
//!
//! # A write against a write
//!
//! The same permit is what keeps two writes off each other, and that is not
//! tidiness either. A write reads the sequence number it will stamp and the
//! offset it will append the record at, waits for the disk, and only then
//! applies to the memtable. Two writes overlapping there read the same of
//! each: the second's record lands on top of the first's, and `write`
//! returned `Ok` for a record the next open has no trace of — and both
//! batches are stamped with the same sequence number, so two entries for one
//! key become one.
//!
//! Pebble serializes the same span for the same reason: its WAL append
//! happens under `commitPipeline.mu`, and only the memtable apply is left
//! concurrent. roble has one thread, so there is nothing to gain by
//! splitting the two, and a write holds the permit from end to end.
//!
//! Waiting rather than refusing, because the second caller asked a
//! reasonable question: a flush that waits for the one in front of it finds
//! an empty memtable and says it had nothing to write, which is true. A
//! refusal would make a caller handle an error that is about roble's
//! scheduling rather than about its database.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
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
    /// Whether a change is underway, which is to say whether asking for the
    /// permit would mean waiting for it.
    ///
    /// What a write asks before deciding whether it is about to make room or
    /// about to wait for somebody else to (`crate::open`, §9). Nothing can
    /// run between the answer and what the caller does with it — there is no
    /// await in between — so the answer cannot go stale underneath them.
    pub(crate) fn underway(&self) -> bool {
        self.underway.get()
    }

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

/// Who is waiting to be told there is compaction to do (§9).
///
/// The store does not run background work itself: `Env::spawn` takes a
/// `'static` future and a store's methods take `&self`, so a task that
/// compacted would have to hold the store — and then dropping the store
/// would not release the directory until that task finished, which `Drop`
/// cannot wait for. So the task is the caller's, built around
/// `Opened::compaction_wanted`, and this is how a flush tells it.
#[derive(Default)]
pub(crate) struct Waiters {
    waiting: RefCell<Vec<Waker>>,
}

impl Waiters {
    /// Registers `cx` to be woken when there is something to do.
    pub(crate) fn park(&self, cx: &std::task::Context<'_>) {
        self.waiting.borrow_mut().push(cx.waker().clone());
    }

    /// Wakes everyone waiting, and says whether there was anyone.
    ///
    /// What the answer is for: a flush that handed the work to somebody runs
    /// none of it itself, and a flush that found nobody waiting runs it
    /// where it stands (§9, `crate::flush`).
    pub(crate) fn wake(&self) -> bool {
        // Out of the cell before any of them is woken, for the reasons
        // written against `Change`'s drop below.
        let waiting = std::mem::take(&mut *self.waiting.borrow_mut());
        let anybody = !waiting.is_empty();
        for waker in waiting {
            waker.wake();
        }
        anybody
    }
}

/// The tables the compaction underway is reading.
///
/// What the permit used to do by being held for the whole of a merge, and
/// what it cannot do now that it is not (§9). Two compactions must not
/// choose the same tables: the second's edit would delete tables the first
/// already deleted, which is the refusal that ends a store's writing life
/// (`crate::flush`). Pebble keeps the same fact as a `compacting` flag on
/// each file of its version.
///
/// A set rather than a flag per table because roble's version holds
/// `TableMeta` by value and clones it into every edit; a flag in there would
/// be a flag in the MANIFEST, which is a different thing that already
/// exists (`marked_for_compaction`).
#[derive(Default)]
pub(crate) struct Compacting {
    tables: RefCell<BTreeSet<u64>>,
    waiting: RefCell<Vec<Waker>>,
}

impl Compacting {
    /// Claims `tables` for a compaction, or nothing if any of them is
    /// already claimed.
    ///
    /// All or nothing. A compaction that took the tables it could and left
    /// the rest would be a compaction of a different set than the one the
    /// picker chose, and the picker chose that set for reasons — in L0,
    /// because its tables overlap each other and compacting one means
    /// compacting every one that meets it.
    pub(crate) fn claim(&self, tables: &[u64]) -> Option<Claim<'_>> {
        let mut held = self.tables.borrow_mut();
        if tables.iter().any(|t| held.contains(t)) {
            return None;
        }
        held.extend(tables.iter().copied());
        drop(held);
        Some(Claim {
            compacting: self,
            tables: tables.to_vec(),
        })
    }

    /// Whether a compaction is underway.
    pub(crate) fn underway(&self) -> bool {
        !self.tables.borrow().is_empty()
    }

    /// Registers `cx` to be woken when the compaction underway lets its
    /// tables go.
    pub(crate) fn park(&self, cx: &std::task::Context<'_>) {
        self.waiting.borrow_mut().push(cx.waker().clone());
    }

    /// Waits until no compaction is underway.
    ///
    /// What a write short of a shallow enough L0 waits on when the merge it
    /// needs is already running without the permit (§9, `crate::open`).
    /// Asked again on every poll, for the reason written against
    /// [`Writers::settled`].
    pub(crate) async fn settled(&self) {
        poll_fn(|cx| {
            if self.underway() {
                self.waiting.borrow_mut().push(cx.waker().clone());
                return Poll::Pending;
            }
            Poll::Ready(())
        })
        .await
    }
}

/// The tables one compaction has claimed, released when it is dropped.
///
/// Dropped on the way out however the compaction ends, which is the reason
/// this is a guard: a merge that failed half way through, or whose future was
/// thrown away, would otherwise leave its inputs claimed for ever and the
/// picker unable to choose them again.
pub(crate) struct Claim<'a> {
    compacting: &'a Compacting,
    tables: Vec<u64>,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        {
            let mut held = self.compacting.tables.borrow_mut();
            for table in &self.tables {
                held.remove(table);
            }
        }
        // Every waiter of them, and out of the cell first, for the reasons
        // written against `Change`'s drop below.
        let waiting = std::mem::take(&mut *self.compacting.waiting.borrow_mut());
        for waker in waiting {
            waker.wake();
        }
    }
}

/// The one write at a time a store takes.
///
/// Held from the sequence number a write stamps to the memtable it applies
/// to, which is the span two writes may not share. Read by a freeze too,
/// which cannot take a memtable away from a write that is still adding to it
/// (`crate::open::Opened::freeze_memtable`).
#[derive(Default)]
pub(crate) struct Writers {
    underway: Cell<bool>,
    waiting: RefCell<Vec<Waker>>,
}

impl Writers {
    /// Waits for the permit, and holds it until the [`Writing`] is dropped.
    ///
    /// Dropped on the way out however the write ends, a failure or a
    /// cancellation included — which is the reason this is a guard and not a
    /// pair of calls. A write whose future is thrown away part-way through
    /// would otherwise hold the permit for ever, and every write and every
    /// freeze after it would wait on a write that is not happening.
    pub(crate) async fn begin(&self) -> Writing<'_> {
        poll_fn(|cx| {
            if self.underway.get() {
                self.waiting.borrow_mut().push(cx.waker().clone());
                return Poll::Pending;
            }
            self.underway.set(true);
            Poll::Ready(())
        })
        .await;
        Writing { writers: self }
    }

    /// Waits until no write is part-way through, without taking the permit.
    ///
    /// Asked again on every poll rather than trusted once: a write that
    /// started between a wake-up and the poll it woke is as much under way
    /// as one that was already there, and the caller is about to freeze.
    pub(crate) async fn settled(&self) {
        poll_fn(|cx| {
            if self.underway.get() {
                self.waiting.borrow_mut().push(cx.waker().clone());
                return Poll::Pending;
            }
            Poll::Ready(())
        })
        .await
    }
}

/// The write under way, and the proof that no other one is.
pub(crate) struct Writing<'a> {
    writers: &'a Writers,
}

impl Drop for Writing<'_> {
    fn drop(&mut self) {
        self.writers.underway.set(false);
        // Every waiter of them, for the reasons written against `Change`'s
        // drop below: a waiter re-checks, so an extra poll is the cheap
        // mistake and a missed wake-up is the expensive one.
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

    use super::{Changes, Writers};

    /// A waker that only records that it was woken.
    struct Flag(AtomicBool);

    impl Wake for Flag {
        // `wake_by_ref` is the trait's own default, which clones and calls
        // this: one line fewer, and one line fewer that no test runs.
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Polls `future` once and expects it to be ready, which is what taking
    /// a free permit is.
    fn ready<F: Future>(future: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        match pin!(future).as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("a free permit should not have to be waited for"),
        }
    }

    /// Here rather than in `tests/change.rs`, where
    /// `every_waiting_change_gets_its_turn` used to pin this. That test could
    /// set it up because a flush held the permit across the merge its
    /// compaction ran, which was many polls; now that a merge holds a claim
    /// and not the permit (§9), nothing holds the permit across more than one
    /// poll on a filesystem that never blocks, so there is no way to make a
    /// waiter wait from outside (§10, §12).
    #[test]
    fn every_waiter_is_woken_and_not_just_one_of_them() {
        // A task polled more often than it must — which is what a `select!`
        // does to every branch when any one of them wakes — registers its
        // waker more than once. So waking "the next waker" can mean waking a
        // task that was going to run anyway, and then nothing wakes the
        // others: a permit that is never taken again is a store that has
        // stopped flushing.
        //
        // Three waiters, because two cannot tell the difference: with one
        // left there is nobody to leave asleep.
        let changes = Changes::default();
        let held = ready(changes.begin());

        let flags: Vec<_> = (0..3)
            .map(|_| Arc::new(Flag(AtomicBool::new(false))))
            .collect();
        let mut waiting: Vec<_> = flags.iter().map(|_| Box::pin(changes.begin())).collect();
        // Twice round, which is what registers each waker more than once.
        for _ in 0..2 {
            for (waiter, flag) in waiting.iter_mut().zip(&flags) {
                let waker = Waker::from(Arc::clone(flag));
                let mut cx = Context::from_waker(&waker);
                assert!(
                    waiter.as_mut().poll(&mut cx).is_pending(),
                    "the permit is held, so none of them can have it"
                );
            }
        }
        assert!(
            flags.iter().all(|f| !f.0.load(Ordering::SeqCst)),
            "and none of them should have been woken for nothing"
        );

        drop(held);
        for (i, flag) in flags.iter().enumerate() {
            assert!(
                flag.0.load(Ordering::SeqCst),
                "waiter {i} was left asleep, so the permit woke some of them and not all"
            );
        }
    }

    /// Here rather than in `tests/freeze.rs`, where the rest of the freeze is
    /// pinned, because no test through the public API can reach this: a write
    /// stays part-way through only across an await, the simulated filesystem
    /// never blocks, and so a write that is polled twice is a write that has
    /// finished (§10, §12). What that leaves untested from outside is the
    /// wake-up, and a freeze that waits and is never woken stalls every
    /// flush after it.
    #[test]
    fn a_freeze_waits_for_a_write_under_way_and_is_woken_by_it() {
        let writers = Writers::default();
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&flag));
        let mut cx = Context::from_waker(&waker);

        let writing = ready(writers.begin());
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

    /// The same wake-up, for the other thing that waits on the permit. Also
    /// unreachable from outside, and for the same reason.
    #[test]
    fn a_second_write_waits_for_the_first_and_is_woken_by_it() {
        let writers = Writers::default();
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&flag));
        let mut cx = Context::from_waker(&waker);

        let first = ready(writers.begin());
        let mut second = pin!(writers.begin());
        assert!(
            second.as_mut().poll(&mut cx).is_pending(),
            "two writes must not share the span where each reads the sequence number \
             it stamps and the offset it appends at"
        );
        assert!(
            !flag.0.load(Ordering::SeqCst),
            "and the second must not be woken for nothing"
        );

        drop(first);
        assert!(
            flag.0.load(Ordering::SeqCst),
            "the write that finished must wake the one waiting for it"
        );
        assert!(
            second.as_mut().poll(&mut cx).is_ready(),
            "and the second must then go through"
        );
    }
}
