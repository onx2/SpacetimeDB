//! Indexes on a table's unique columns, kept by whichever storage holds the table's rows.

use std::any::Any;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use crate::protocol::{Table, TableDiff, UniqueColumn, UniqueColumnVisitor};

/// One column's index, without its key type, so that a table can hold several in one list.
trait AnyIndex<T: Table, H>: Send + Sync + 'static {
    fn remove(&mut self, row: &T::Row);
    fn insert(&mut self, row: &T::Row, handle: H);
    fn as_any(&self) -> &dyn Any;
}

struct Index<C: UniqueColumn, H>(HashMap<C::Key, H>);

impl<C: UniqueColumn, H: Copy + Send + Sync + 'static> AnyIndex<C::Table, H> for Index<C, H> {
    fn remove(&mut self, row: &<C::Table as Table>::Row) {
        self.0.remove(C::key(row));
    }

    fn insert(&mut self, row: &<C::Table as Table>::Row, handle: H) {
        self.0.insert(C::key(row).clone(), handle);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// An index per unique column of table `T`, from column value to the row's handle.
/// Empty, and free, for the many tables whose only unique column is their primary key.
pub(crate) struct UniqueIndexes<T: Table, H>(Vec<Box<dyn AnyIndex<T, H>>>);

impl<T: Table, H: Copy + Send + Sync + 'static> UniqueIndexes<T, H> {
    pub fn new() -> Self {
        struct Collect<T: Table, H>(Vec<Box<dyn AnyIndex<T, H>>>);
        impl<T: Table, H: Copy + Send + Sync + 'static> UniqueColumnVisitor<T> for Collect<T, H> {
            fn column<C: UniqueColumn<Table = T>>(&mut self) {
                self.0.push(Box::new(Index::<C, H>(HashMap::new())));
            }
        }
        let mut collect = Collect(Vec::new());
        T::visit_unique_columns(&mut collect);
        Self(collect.0)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Takes out the values of every row that leaves or changes, before a diff is applied. All of
    /// them go before any new value enters, because two rows may swap values in one transaction.
    pub fn before<'r>(&mut self, diff: &TableDiff<T, H>, row: impl Fn(H) -> Option<&'r T::Row>) {
        let leaving = diff
            .deletes
            .iter()
            .copied()
            .chain(diff.updates.iter().map(|update| update.handle));
        for row in leaving.filter_map(row) {
            for index in &mut self.0 {
                index.remove(row);
            }
        }
    }

    pub fn insert(&mut self, row: &T::Row, handle: H) {
        for index in &mut self.0 {
            index.insert(row, handle);
        }
    }

    /// Takes out the values of a row that left its storage by some other way than a diff.
    pub fn remove(&mut self, row: &T::Row) {
        for index in &mut self.0 {
            index.remove(row);
        }
    }

    pub fn find<C, Q>(&self, key: &Q) -> Option<H>
    where
        C: UniqueColumn<Table = T>,
        C::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let index = self
            .0
            .iter()
            .find_map(|index| index.as_any().downcast_ref::<Index<C, H>>())?;
        index.0.get(key).copied()
    }
}
