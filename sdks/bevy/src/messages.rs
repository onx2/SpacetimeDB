//! Row changes as [`Message`]s.
//!
//! Read them in the [`crate::StdbTransaction`] schedule to see each transaction's changes against
//! exactly that transaction's state, or in any other schedule to see them against the latest state.

use crate::protocol::Table;
use bevy_ecs::message::Message;

/// A row of table `T` became resident.
#[doc(alias = "on_insert")]
pub struct RowInserted<T: Table> {
    /// Counts applied transactions. All changes of one transaction share it.
    pub seq: u64,
    /// The row as it now is.
    pub row: T::Row,
}

/// A row of table `T` changed while keeping its primary key.
#[doc(alias = "on_update")]
pub struct RowUpdated<T: Table> {
    /// Counts applied transactions. All changes of one transaction share it.
    pub seq: u64,
    /// The row as it was before this transaction.
    pub old: T::Row,
    /// The row as it now is.
    pub new: T::Row,
}

/// A row of table `T` stopped being resident, because it was deleted or no subscription covers it.
#[doc(alias = "on_delete")]
pub struct RowDeleted<T: Table> {
    /// Counts applied transactions. All changes of one transaction share it.
    pub seq: u64,
    /// The row as it last was.
    pub row: T::Row,
}

/// A row announced by event table `T`. Event rows are never resident.
pub struct RowEvent<T: Table> {
    /// Counts applied transactions. All changes of one transaction share it.
    pub seq: u64,
    /// The announced row. Nothing stores it.
    pub row: T::Row,
}

impl<T: Table> Message for RowInserted<T> {}
impl<T: Table> Message for RowUpdated<T> {}
impl<T: Table> Message for RowDeleted<T> {}
impl<T: Table> Message for RowEvent<T> {}
