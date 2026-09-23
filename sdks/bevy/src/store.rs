//! The default row storage: one resource per table, rows in contiguous memory.

use std::sync::OnceLock;

use crate::protocol::{RowUpdate, Table, TableDiff, UniqueColumn};
use bevy_ecs::change_detection::{CheckChangeTicks, Tick};
use bevy_ecs::prelude::*;
use bevy_ecs::system::{SystemChangeTick, SystemParam};
use spacetimedb_lib::bsatn;

use crate::pairing::{Paired, Ticket};

use crate::entity::{Row, RowEntities};
use crate::unique::UniqueIndexes;

/// Names a row of a [`TableStore`] for as long as it is resident: the row's [`Ticket`].
///
/// A slot number, like the index half of an `Entity`. It needs no generation half because it
/// never leaves this crate: a ticket is forgotten where it is given out in the same step that
/// frees it, so a stale handle cannot exist. `u32` rather than `usize` because two arrays and a
/// map hold one per row, and no client holds four billion rows of one table.
pub(crate) type RowHandle = Ticket;

/// What one transaction changed in one table, with the values row messages carry.
pub(crate) struct RowChanges<T: Table> {
    pub inserted: Vec<T::Row>,
    /// Old value then new, both of a row that kept its primary key.
    pub updated: Vec<(T::Row, T::Row)>,
    pub deleted: Vec<T::Row>,
}

impl<T: Table> Default for RowChanges<T> {
    fn default() -> Self {
        Self {
            inserted: Vec::new(),
            updated: Vec::new(),
            deleted: Vec::new(),
        }
    }
}

/// Drops the updates of a reconnect's snapshot whose rows did not in fact change.
///
/// `RowSet` reports every row that survived the reconnect as an update: it runs on the parsing
/// task, where the stored row cannot be reached, so it cannot tell which of them moved. The
/// storage can, and this is where it does — so an app hears `RowUpdated` only for what really
/// changed, and an entity-stored row that stayed put keeps everything hung on it.
///
/// A row that will not encode counts as changed, which is the safe way round: the app is told
/// about a row it may need to redraw rather than left with a stale one.
pub(crate) fn retain_changed<'r, T: Table, H: Copy>(
    updates: &mut Vec<RowUpdate<T, H>>,
    old: impl Fn(H) -> Option<&'r T::Row>,
) {
    updates.retain(|update| {
        let was = old(update.handle).map(bsatn::to_vec);
        !matches!((was, bsatn::to_vec(&update.new)), (Some(Ok(was)), Ok(new)) if was == new)
    });
}

/// Rows packed into a `Vec`, addressed through slots so that handles survive `swap_remove`.
struct DenseRows<T: Table> {
    rows: Vec<T::Row>,
    /// When each row was inserted or last updated. Parallel to `rows`.
    changed: Vec<Tick>,
    /// The slot of each row. Parallel to `rows`.
    slot_of: Vec<u32>,
    /// For each slot in use, the index of its row in `rows`.
    index_of: Vec<u32>,
    /// The tick rows changed by the next `apply` are stamped with.
    write_tick: Tick,
    /// Whether `changes` is filled in, for row messages to be written from.
    record: bool,
    changes: RowChanges<T>,
    unique: UniqueIndexes<T, RowHandle>,
}

impl<T: Table> Default for DenseRows<T> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            changed: Vec::new(),
            slot_of: Vec::new(),
            index_of: Vec::new(),
            write_tick: Tick::new(0),
            record: true,
            changes: RowChanges::default(),
            unique: UniqueIndexes::new(),
        }
    }
}

impl<T: Table> DenseRows<T> {
    fn row(&self, handle: RowHandle) -> &T::Row {
        &self.rows[self.index_of[handle as usize] as usize]
    }

    /// Applies `diff`, whose new rows go under `slots`. `by_pk` is kept in step, if there is one.
    fn apply(&mut self, diff: TableDiff<T, RowHandle>, slots: &[RowHandle], mut by_pk: Option<&mut PkIndex<T>>) {
        let indexed = !self.unique.is_empty();
        if indexed {
            let (rows, index_of) = (&self.rows, &self.index_of);
            self.unique.before(&diff, |handle: RowHandle| {
                Some(&rows[index_of[handle as usize] as usize])
            });
        }

        for handle in diff.deletes {
            let index = self.index_of[handle as usize] as usize;
            let row = self.rows.swap_remove(index);
            self.changed.swap_remove(index);
            self.slot_of.swap_remove(index);
            if let Some(&moved) = self.slot_of.get(index) {
                self.index_of[moved as usize] = index as u32;
            }
            if let (Some(by_pk), Some(pk)) = (&mut by_pk, T::pk(&row)) {
                by_pk.remove(pk);
            }
            if self.record {
                self.changes.deleted.push(row);
            }
        }

        for update in diff.updates {
            let index = self.index_of[update.handle as usize] as usize;
            self.changed[index] = self.write_tick;
            if indexed {
                self.unique.insert(&update.new, update.handle);
            }
            if self.record {
                let old = std::mem::replace(&mut self.rows[index], update.new.clone());
                self.changes.updated.push((old, update.new));
            } else {
                self.rows[index] = update.new;
            }
        }

        self.rows.reserve(diff.inserts.len());
        self.changed.reserve(diff.inserts.len());
        self.slot_of.reserve(diff.inserts.len());
        if self.record {
            self.changes.inserted.reserve(diff.inserts.len());
        }
        if let Some(by_pk) = &mut by_pk
            && by_pk.is_empty()
        {
            by_pk.reserve(diff.inserts.len());
        }
        for (row, &slot) in diff.inserts.into_iter().zip(slots) {
            let index = self.rows.len() as u32;
            if self.index_of.len() <= slot as usize {
                self.index_of.resize(slot as usize + 1, u32::MAX);
            }
            self.index_of[slot as usize] = index;
            if self.record {
                self.changes.inserted.push(row.clone());
            }
            if indexed {
                self.unique.insert(&row, slot);
            }
            if let (Some(by_pk), Some(pk)) = (&mut by_pk, T::pk(&row)) {
                by_pk.insert(pk.clone(), slot);
            }
            self.rows.push(row);
            self.changed.push(self.write_tick);
            self.slot_of.push(slot);
        }
    }
}

/// Keys come from a server the client chose to trust: speed over `SipHash`'s flood resistance.
type PkIndex<T> = std::collections::HashMap<<T as Table>::Pk, RowHandle, foldhash::fast::RandomState>;

/// The resident rows of table `T`.
///
/// Systems read it through [`Rows`]. Only the crate's apply step writes to it.
#[derive(Resource)]
#[doc(alias = "cache")]
pub struct TableStore<T: Table> {
    rows: DenseRows<T>,
    /// From primary key to row, made by the first [`Rows::get`] and kept in step from then on.
    ///
    /// Many tables are only ever read whole, and an index costs a hash insert per new row in the
    /// frame that applies it, and memory per row. The system that first looks a row up pays for
    /// indexing the rows there are, once: about a millisecond per 100,000.
    by_pk: OnceLock<PkIndex<T>>,
}

impl<T: Table> Default for TableStore<T> {
    fn default() -> Self {
        Self {
            rows: DenseRows::default(),
            by_pk: OnceLock::new(),
        }
    }
}

impl<T: Table> TableStore<T> {
    /// Creates an empty store. `row_messages` is false for a table set up with
    /// [`StdbPlugin::without_row_messages`](crate::StdbPlugin::without_row_messages),
    /// which then keeps no record of what changed.
    pub(crate) fn new(row_messages: bool) -> Self {
        let mut store = Self::default();
        store.rows.record = row_messages;
        store
    }

    /// Applies one message's changes, already paired against the resident rows, and returns what
    /// they came to.
    pub(crate) fn apply(&mut self, mut paired: Paired<T>, tick: Tick) -> RowChanges<T> {
        self.rows.write_tick = tick;
        if paired.reconciled {
            let rows = &self.rows;
            retain_changed(&mut paired.diff.updates, |handle| Some(rows.row(handle)));
        }
        self.rows.apply(paired.diff, &paired.tickets, self.by_pk.get_mut());
        std::mem::take(&mut self.rows.changes)
    }

    fn by_pk(&self) -> &PkIndex<T> {
        self.by_pk.get_or_init(|| {
            let rows = self.rows.rows.iter().zip(&self.rows.slot_of);
            rows.filter_map(|(row, &slot)| Some((T::pk(row)?.clone(), slot)))
                .collect()
        })
    }

    /// Removes every row, returning them if the table writes row messages.
    pub(crate) fn clear(&mut self) -> Vec<T::Row> {
        let record = self.rows.record;
        let rows = std::mem::take(&mut self.rows).rows;
        self.rows.record = record;
        // Still wanted, if it ever was.
        if let Some(by_pk) = self.by_pk.get_mut() {
            by_pk.clear();
        }
        if record {
            rows
        } else {
            Vec::new()
        }
    }

    /// Keeps per-row ticks within the age Bevy's change detection can compare.
    pub(crate) fn check_change_ticks(&mut self, check: CheckChangeTicks) {
        for tick in &mut self.rows.changed {
            tick.check_tick(check);
        }
    }
}

/// Read access to the resident rows of table `T`, whichever way the table is stored.
///
/// ```
/// # use spacetimedb_bevy::Rows;
/// # use spacetimedb_bevy::__codegen::core::{Module, NoPk, ParseError, RawTableRows, ReducerVisitor, Table, TableKind, TableVisitor, UpdateVisitor};
/// # struct Chat;
/// # impl Module for Chat {
/// #     type Update = ();
/// #     fn parse_table(_: &mut (), _: &str, _: RawTableRows) -> Result<(), ParseError> { Ok(()) }
/// #     fn visit_update<V: UpdateVisitor<Self>>(_: (), _: &mut V) {}
/// #     fn visit_tables<V: TableVisitor<Self>>(_: &mut V) {}
/// #     fn visit_reducers<V: ReducerVisitor<Self>>(_: &mut V) {}
/// # }
/// # struct User;
/// # impl Table for User {
/// #     type Module = Chat;
/// #     type Row = (u64, String);
/// #     type Pk = u64;
/// #     const NAME: &'static str = "user";
/// #     const KIND: TableKind = TableKind::Persistent;
/// #     fn pk(row: &Self::Row) -> Option<&u64> { Some(&row.0) }
/// # }
/// fn greet(users: Rows<User>) {
///     for user in users.iter_changed() { /* inserted or updated since this system last ran */ }
/// }
/// ```
///
/// For a table with [entity storage](crate::entity), `Query<&Row<T>>` reads the same rows and can
/// join them with other components; `Rows` is there so that a system does not have to care.
#[derive(SystemParam)]
#[doc(alias = "TableHandle")]
#[doc(alias = "table")]
#[doc(alias = "iter")]
pub struct Rows<'w, 's, T: Table> {
    store: Option<Res<'w, TableStore<T>>>,
    entities: Query<'w, 's, Ref<'static, Row<T>>>,
    index: Option<Res<'w, RowEntities<T>>>,
    ticks: SystemChangeTick,
}

impl<T: Table> Rows<'_, '_, T> {
    /// Returns the number of resident rows.
    pub fn len(&self) -> usize {
        match (&self.store, &self.index) {
            (Some(store), _) => store.rows.rows.len(),
            (None, Some(index)) => index.len(),
            (None, None) => 0,
        }
    }

    /// Returns `true` if no row of this table is resident.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns all resident rows as one contiguous slice, or `None` for a table with entity
    /// storage, whose rows are spread over entities.
    pub fn as_slice(&self) -> Option<&[T::Row]> {
        self.store.as_ref().map(|store| &store.rows.rows[..])
    }

    /// Returns an iterator over all resident rows, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &T::Row> {
        let stored = self.as_slice().unwrap_or_default().iter();
        let on_entities = self.entities.iter().map(|row| &**Ref::into_inner(row));
        stored.chain(on_entities)
    }

    /// Returns the row with primary key `pk`. Always `None` for tables without a primary key.
    #[doc(alias = "find_by_pk")]
    pub fn get(&self, pk: &T::Pk) -> Option<&T::Row> {
        if let Some(store) = &self.store {
            let handle = *store.by_pk().get(pk)?;
            return Some(store.rows.row(handle));
        }
        let entity = self.index.as_ref()?.get(pk)?;
        self.entities.get(entity).ok().map(|row| &**Ref::into_inner(row))
    }

    /// Returns the row holding `key` in unique column `column`, which is a marker type the
    /// generated bindings define per unique column: `users.find(UserName, "ada")`.
    #[doc(alias = "find_by")]
    pub fn find<C, Q>(&self, column: C, key: &Q) -> Option<&T::Row>
    where
        C: UniqueColumn<Table = T>,
        C::Key: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        let _ = column;
        if let Some(store) = &self.store {
            let handle = store.rows.unique.find::<C, Q>(key)?;
            return Some(store.rows.row(handle));
        }
        let entity = self.index.as_ref()?.unique.find::<C, Q>(key)?;
        self.entities.get(entity).ok().map(|row| &**Ref::into_inner(row))
    }

    /// Returns the entity holding the row with primary key `pk`, for a table with entity storage.
    pub fn entity(&self, pk: &T::Pk) -> Option<Entity> {
        let entity = self.index.as_ref()?.get(pk)?;
        // Not if the app has despawned it.
        self.entities.contains(entity).then_some(entity)
    }

    /// Returns an iterator over the rows inserted or updated since this system last ran.
    pub fn iter_changed(&self) -> impl Iterator<Item = &T::Row> {
        let (last_run, this_run) = (self.ticks.last_run(), self.ticks.this_run());
        let stored = self.store.iter().flat_map(move |store| {
            let rows = &store.rows;
            rows.rows
                .iter()
                .zip(&rows.changed)
                .filter(move |(_, changed)| changed.is_newer_than(last_run, this_run))
                .map(|(row, _)| row)
        });
        let on_entities = self
            .entities
            .iter()
            .filter(|row| row.is_changed())
            .map(|row| &**Ref::into_inner(row));
        stored.chain(on_entities)
    }
}
