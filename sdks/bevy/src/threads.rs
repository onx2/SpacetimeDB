//! Lending the core crate threads to decode long row lists on.
//!
//! A subscription's first message carries a row for every row of the table, and decoding them is
//! the one part of this crate's work that is neither ordered nor shared: rows are independent, so
//! a long list can be split. The core crate does the splitting and asks for the threads here,
//! because it knows nothing about Bevy.
//!
//! The pool is [`AsyncComputeTaskPool`], which is Bevy's pool for work that is not part of a
//! frame. [`ComputeTaskPool`](bevy_tasks::ComputeTaskPool) is the wrong one: its threads are the
//! ones the schedule runs systems on, and a message arriving mid-frame would take them from the
//! frame it arrived in. Decoding is already off the main thread, so what it can win is a
//! transaction's total time, never a frame's, and it must not be able to lose a frame's.
//!
//! No pool is made here. An app with `TaskPoolPlugin`, which every app built from Bevy's default
//! or minimal plugins has, already has one; an app without one decodes where it parses, as this
//! crate did everywhere before 2026-09-20, and so does a browser, where the pool is the page's
//! one thread and splitting a list would buy nothing.

use crate::protocol::{lend_threads, Threads};
use bevy_tasks::AsyncComputeTaskPool;

/// Bevy's async compute pool, as the core crate wants to see it. Both methods look the pool up
/// rather than hold it, so it is found whenever it is made, in whichever order the plugins were
/// added.
struct AsyncCompute;

impl Threads for AsyncCompute {
    fn count(&self) -> usize {
        // One means "do not split", which is the answer when the app has no pool.
        AsyncComputeTaskPool::try_get().map_or(1, |pool| pool.thread_num())
    }

    fn run<'a>(&self, jobs: &mut [Box<dyn FnMut() + Send + 'a>]) {
        let Some(pool) = AsyncComputeTaskPool::try_get() else {
            for job in jobs {
                job();
            }
            return;
        };
        pool.scope(|scope| {
            for job in jobs {
                scope.spawn(async move { job() });
            }
        });
    }
}

static ASYNC_COMPUTE: AsyncCompute = AsyncCompute;

/// Lends the async compute pool to the core crate. Called for every plugin; the first call is the
/// one that counts, and the rest cost a load of an already-set `OnceLock`.
pub(crate) fn lend_pool() {
    lend_threads(&ASYNC_COMPUTE);
}
