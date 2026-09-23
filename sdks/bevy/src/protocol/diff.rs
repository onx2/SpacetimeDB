//! What a server message changes, once refcounting has worked it out.
//!
//! A diff names rows by handle rather than carrying the old ones: the storage that holds them
//! is on the main thread, and [`crate::protocol::RowSet`], which works the change out, is not. Deletes and
//! updates are therefore a handle each, and only new rows travel by value.

use crate::protocol::module::Table;

/// A row whose primary key stayed the same while its contents changed.
#[derive(Debug)]
pub struct RowUpdate<T: Table, H> {
    /// The handle the old row was stored under. The new row keeps it.
    pub handle: H,
    /// The row's new contents.
    pub new: T::Row,
}

/// The net change to one table after one server message.
///
/// No row appears in more than one list.
///
/// `H` is how the storage that will apply this names its rows. A [`RowSet`](crate::protocol::RowSet)
/// produces a diff of [`Ticket`](crate::protocol::Ticket)s, which is the name every storage shares; a
/// storage that keeps its rows somewhere the ticket only points at — entities — rewrites the
/// handles into its own before applying it.
#[derive(Debug)]
pub struct TableDiff<T: Table, H> {
    /// Handles of rows no subscription covers any more.
    pub deletes: Vec<H>,
    /// Always empty for tables without a primary key.
    pub updates: Vec<RowUpdate<T, H>>,
    /// Rows that became resident, to be stored under the tickets that came with them.
    pub inserts: Vec<T::Row>,
}

impl<T: Table, H> TableDiff<T, H> {
    /// Returns a diff that changes nothing.
    pub fn empty() -> Self {
        Self {
            deletes: Vec::new(),
            updates: Vec::new(),
            inserts: Vec::new(),
        }
    }

    /// Returns `true` if the diff changes nothing.
    pub fn is_empty(&self) -> bool {
        self.deletes.is_empty() && self.updates.is_empty() && self.inserts.is_empty()
    }
}
