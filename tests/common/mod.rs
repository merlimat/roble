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

//! Helpers shared by the integration tests.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Polls `op` until it suspends, then runs `landing` to its end while `op`
/// stays suspended, then `op` to its end; and says whether `landing` ran
/// wholly inside `op`.
///
/// Not turn and turn about, as `interleave` in `tests/freeze.rs` polls two
/// futures: a write hands the thread over once while a flush or a merge is
/// under way (`share_the_thread`), and given the turn, the operation would
/// run to its end with the write not yet applied. A yield is passed by
/// polling the landing again. Only a landing that is still not done after a
/// hundred polls is waiting on the operation, and then the operation is
/// polled once. The simulated filesystem never blocks, so every `Pending`
/// comes from roble.
pub fn land_during<A: Future, B: Future>(op: A, landing: B) -> (A::Output, B::Output, bool) {
    let mut cx = Context::from_waker(Waker::noop());
    let mut op = pin!(op);
    let mut landing = pin!(landing);
    let mut done = match op.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    };
    let mut polls = 0;
    let landed = loop {
        if let Poll::Ready(v) = landing.as_mut().poll(&mut cx) {
            break v;
        }
        polls += 1;
        assert!(polls < 100_000, "the landing is not making progress");
        if polls % 100 == 0
            && done.is_none()
            && let Poll::Ready(v) = op.as_mut().poll(&mut cx)
        {
            done = Some(v);
        }
    };
    let inside = done.is_none();
    let done = match done {
        Some(v) => v,
        None => loop {
            if let Poll::Ready(v) = op.as_mut().poll(&mut cx) {
                break v;
            }
            polls += 1;
            assert!(polls < 100_000, "the operation is not making progress");
        },
    };
    (done, landed, inside)
}
