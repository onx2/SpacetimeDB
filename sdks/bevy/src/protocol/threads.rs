//! Lending this crate threads, for the one piece of its work that can use more than one.
//!
//! Decoding a row list is that piece: rows are independent of one another, and a subscription's
//! first message carries one for every row of the table. Everything else here is a stream that
//! has to stay in order, and stays on the thread it is parsed on.

use std::sync::OnceLock;

/// A pool of threads, lent to this crate by whoever runs it.
///
/// Nothing is decoded on another thread unless a pool is lent. Without one — a browser, a test,
/// a tool reading a capture — a row list is decoded on the thread that parses it, which is what
/// this crate did everywhere until 2026-09-20. Either way the rows come out in the order the
/// server sent them and a bad row fails the same way, so which of the two happened is not
/// something a program can tell.
pub trait Threads: Send + Sync {
    /// How many threads the pool has. A row list is split into at most this many parts, and is
    /// not split at all when this is one.
    fn count(&self) -> usize;

    /// Runs every job exactly once and returns only once all of them have returned. Jobs do not
    /// depend on each other, so they may run in any order and on any thread, including this one.
    ///
    /// Returning before a job has run, or without running one at all, is a contract this crate
    /// checks: it panics rather than read what the job was to have written. Letting a job's panic
    /// through is allowed, and is what Bevy's pools do.
    fn run<'a>(&self, jobs: &mut [Box<dyn FnMut() + Send + 'a>]);
}

static LENT: OnceLock<&'static dyn Threads> = OnceLock::new();

/// Lends `threads` to this crate for decoding. The first call is the one that counts; a later
/// one changes nothing and returns `false` to say so.
///
/// `spacetimedb_bevy`'s plugin calls this with Bevy's async compute pool, which is the pool for
/// work that is not part of a frame and must not hold one up. A program using this crate without
/// Bevy can lend its own pool, or lend none and have rows decoded where they are parsed.
pub fn lend_threads(threads: &'static dyn Threads) -> bool {
    LENT.set(threads).is_ok()
}

/// The pool [`lend_threads`] was given, if it was given one worth splitting work over.
pub(crate) fn lent() -> Option<&'static dyn Threads> {
    LENT.get().copied().filter(|threads| threads.count() > 1)
}
