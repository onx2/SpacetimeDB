//! Applying parsed server messages to the `World`, one transaction at a time.

use std::any::TypeId;
use std::marker::PhantomData;

use crate::protocol::{
    Module, Procedure, ProcedureVisitor, Reducer, ReducerVisitor, RowSetError, Table, TableKind, TableVisitor,
};
use bevy_app::App;
use bevy_ecs::change_detection::{CheckChangeTicks, Tick};
use bevy_ecs::prelude::*;
use bevy_ecs::schedule::{ScheduleLabel, Schedules};

use crate::connection::{ProcedureResult, ReducerResult, StdbConnection};
use crate::entity::{self, RowEntities};
use crate::messages::{RowDeleted, RowEvent, RowInserted, RowUpdated};
use crate::pairing::{pair, Paired, PairedUpdate};
use crate::store::TableStore;

/// Runs once after every applied transaction, before the next one is applied.
///
/// Systems here see the tables exactly as of that transaction, and a `MessageReader` of a row
/// message yields only that transaction's changes. This is the equivalent of a callback in the
/// official SDKs. It may run many times in one frame, or not at all.
///
/// **One schedule, shared by every module.** It runs once per transaction whoever sent it, so an
/// app with two connections runs every system here for both. That is right for a handler that
/// answers messages it read — the other module's are simply not there — and wrong for one that
/// does work per transaction regardless. [`from_module`] is how such a handler narrows itself.
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct StdbTransaction;

/// The number of `M`'s transactions applied so far. `M`'s row messages carry the value current
/// when they were written.
///
/// One per module: two connections count their own, because a transaction belongs to a database
/// and two databases' transactions have no order between them.
#[derive(Resource, Debug, PartialEq, Eq)]
pub struct TransactionSeq<M>(pub u64, PhantomData<fn() -> M>);

impl<M> TransactionSeq<M> {
    /// The count, as a plain number.
    pub fn get(&self) -> u64 {
        self.0
    }
}

impl<M> Default for TransactionSeq<M> {
    fn default() -> Self {
        Self(0, PhantomData)
    }
}

impl<M> Clone for TransactionSeq<M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M> Copy for TransactionSeq<M> {}

/// Whose transaction [`StdbTransaction`] is running for, while it is running.
///
/// Not part of the API: it is `pub` only because [`from_module`] names it in its return type,
/// and it is hidden from the documentation for the same reason `__codegen` is. [`from_module`]
/// is the whole of what it is for, and "we are inside module M's transaction" means nothing
/// outside that schedule. Set and cleared by [`run_transaction_schedule`], which every path that
/// runs the schedule goes through.
#[doc(hidden)]
#[derive(Resource, Default)]
pub struct ApplyingModule(Option<TypeId>);

/// A run condition that holds while the transaction being applied belongs to `M`.
///
/// [`StdbTransaction`] is one schedule shared by every module, so a system in it runs for every
/// connection's transactions. That is what an app with one module wants, and it is a trap for an
/// app with two: a handler that does work for each transaction, rather than in answer to messages
/// it read, would do that work for the other database's transactions as well.
///
/// ```
/// # use bevy_app::App;
/// # use bevy_ecs::prelude::*;
/// # use spacetimedb_bevy::{from_module, StdbTransaction};
/// # use spacetimedb_bevy::__codegen::core::{Module, ParseError, RawTableRows, ReducerVisitor, TableVisitor, UpdateVisitor};
/// # struct Arena;
/// # impl Module for Arena {
/// #     type Update = ();
/// #     fn parse_table(_: &mut (), _: &str, _: RawTableRows) -> Result<(), ParseError> { Ok(()) }
/// #     fn visit_update<V: UpdateVisitor<Self>>(_: (), _: &mut V) {}
/// #     fn visit_tables<V: TableVisitor<Self>>(_: &mut V) {}
/// #     fn visit_reducers<V: ReducerVisitor<Self>>(_: &mut V) {}
/// # }
/// # #[derive(Resource, Default)] struct History(Vec<u32>);
/// # fn snapshot(_: ResMut<History>) {}
/// # let mut app = App::new();
/// app.add_systems(StdbTransaction, snapshot.run_if(from_module::<Arena>()));
/// ```
///
/// It is false everywhere else, including in `Update`, because outside a transaction there is no
/// module to be inside of. A single-module app never needs it.
pub fn from_module<M: Module>() -> impl FnMut(Option<Res<ApplyingModule>>) -> bool + Clone {
    move |applying| applying.is_some_and(|applying| applying.0 == Some(TypeId::of::<M>()))
}

/// Applies one transaction's row changes, writes its messages, and runs [`StdbTransaction`].
///
/// # Errors
///
/// [`RowSetError`] if the server deleted a row the client does not hold, or one whose primary
/// key these bindings cannot read. The tables have not changed then, but what is resident is no
/// longer known: client and server disagree about what is subscribed, or about the module, so
/// the caller must drop the connection and clear the tables.
pub fn apply_update<M: Module>(world: &mut World, update: M::Update) -> Result<(), RowSetError> {
    let pairing = world.resource::<StdbConnection<M>>().pairing();
    let paired = {
        let mut state = pairing.lock().unwrap_or_else(|error| error.into_inner());
        pair::<M>(&mut state, update, false)?
    };
    apply_paired::<M>(world, paired);
    run_transaction_schedule::<M>(world);
    Ok(())
}

/// Apply what a message was worked out to change, and write its row messages.
/// The caller runs the transaction schedule, once it has written any messages of its own.
pub(crate) fn apply_paired<M: Module>(world: &mut World, paired: PairedUpdate) -> u64 {
    let seq = next_seq::<M>(world);
    let tick = world.change_tick();
    for table in paired {
        table.apply(world, seq, tick);
    }
    // Entities whose last row left, in any table of their group, go only now.
    entity::sweep_all(world);
    seq
}

/// One table's part of [`apply_paired`].
pub(crate) fn apply_table<T: Table>(world: &mut World, seq: u64, tick: Tick, mut paired: Paired<T>) {
    if T::KIND == TableKind::Event {
        let events = std::mem::take(&mut paired.events).into_iter();
        world.write_message_batch(events.map(|row| RowEvent::<T> { seq, row }));
        return;
    }
    let changes = if world.contains_resource::<RowEntities<T>>() {
        entity::apply::<T>(world, paired)
    } else {
        world.resource_mut::<TableStore<T>>().apply(paired, tick)
    };
    write_changes::<T>(world, seq, changes);
}

/// Returns the next [`TransactionSeq`]. One per transaction, whether it came from the server or,
/// like a reconnect's reconcile, was made up here.
pub(crate) fn next_seq<M: Module>(world: &mut World) -> u64 {
    let mut seq = world.resource_mut::<TransactionSeq<M>>();
    seq.0 += 1;
    seq.0
}

/// Runs [`StdbTransaction`] for a transaction of `M`, unless nothing is in it.
///
/// Every path that runs the schedule comes through here, which is what makes [`from_module`]
/// trustworthy: the module is recorded in one place, and cleared again afterwards, so no caller
/// can forget it and nothing outside a transaction sees a stale one.
pub(crate) fn run_transaction_schedule<M: Module>(world: &mut World) {
    // Apps that only read messages from the main schedules leave this one empty.
    // Skipping it then saves a schedule run per transaction.
    let has_systems = world
        .get_resource::<Schedules>()
        .and_then(|schedules| schedules.get(StdbTransaction))
        .is_some_and(|schedule| schedule.systems_len() > 0);
    if !has_systems {
        return;
    }
    if let Some(mut applying) = world.get_resource_mut::<ApplyingModule>() {
        applying.0 = Some(TypeId::of::<M>());
    }
    world.run_schedule(StdbTransaction);
    if let Some(mut applying) = world.get_resource_mut::<ApplyingModule>() {
        applying.0 = None;
    }
}

fn write_changes<T: Table>(world: &mut World, seq: u64, changes: crate::store::RowChanges<T>) {
    world.write_message_batch(changes.deleted.into_iter().map(|row| RowDeleted::<T> { seq, row }));
    world.write_message_batch(
        changes
            .updated
            .into_iter()
            .map(|(old, new)| RowUpdated::<T> { seq, old, new }),
    );
    world.write_message_batch(changes.inserted.into_iter().map(|row| RowInserted::<T> { seq, row }));
}

/// Empties every table of `M` as one transaction, with a `RowDeleted` per row.
///
/// What is resident is forgotten with them.
pub(crate) fn clear_tables<M: Module>(world: &mut World) {
    world.resource::<StdbConnection<M>>().forget_residents();
    let seq = next_seq::<M>(world);
    M::visit_tables(&mut Clearer { world, seq });
    entity::sweep_all(world);
    run_transaction_schedule::<M>(world);
}

struct Clearer<'w> {
    world: &'w mut World,
    seq: u64,
}

impl<M: Module> TableVisitor<M> for Clearer<'_> {
    fn table<T: Table<Module = M>>(&mut self) {
        if T::KIND == TableKind::Event {
            return;
        }
        let seq = self.seq;
        let deleted = if self.world.contains_resource::<RowEntities<T>>() {
            entity::clear::<T>(self.world)
        } else {
            self.world.resource_mut::<TableStore<T>>().clear()
        };
        self.world
            .write_message_batch(deleted.into_iter().map(|row| RowDeleted::<T> { seq, row }));
    }
}

/// Sets up storage and messages for every table of a module.
pub(crate) struct Registrar<'a, M> {
    pub app: &'a mut App,
    /// Tables the app asked to have stored as entities.
    /// To the table's key group, if it shares entities with other tables.
    pub entity_tables: &'a std::collections::HashMap<std::any::TypeId, Option<std::any::TypeId>>,
    pub all_entity_tables: bool,
    /// Entity tables whose entities outlive their rows.
    pub kept: &'a std::collections::HashSet<std::any::TypeId>,
    /// Tables that write no row messages.
    pub silent: &'a std::collections::HashSet<std::any::TypeId>,
    pub module: PhantomData<fn() -> M>,
}

impl<M: Module> TableVisitor<M> for Registrar<'_, M> {
    fn table<T: Table<Module = M>>(&mut self) {
        if T::KIND == TableKind::Event {
            self.app.add_message::<RowEvent<T>>();
            return;
        }
        self.app
            .add_message::<RowInserted<T>>()
            .add_message::<RowUpdated<T>>()
            .add_message::<RowDeleted<T>>();
        let table = std::any::TypeId::of::<T>();
        if self.all_entity_tables || self.entity_tables.contains_key(&table) {
            let group = self.entity_tables.get(&table).copied().flatten();
            if let Some(group) = group {
                let key = std::any::TypeId::of::<T::Pk>();
                assert!(
                    key != std::any::TypeId::of::<crate::protocol::NoPk>(),
                    "table `{}` has no primary key, so it cannot share entities by key",
                    T::NAME
                );
                self.app
                    .init_resource::<entity::KeyGroups<T::Pk>>()
                    .init_resource::<entity::GroupSweepers>();
                let mut groups = self.app.world_mut().resource_mut::<entity::GroupSweepers>();
                groups.sweepers.insert(key, entity::sweep::<T::Pk>);
                // The group's other tables may be another plugin's, so this is the one place
                // that sees them all.
                let (claimed, by) = *groups.key_of.entry(group).or_insert((key, T::NAME));
                assert!(
                    claimed == key,
                    "table `{}` cannot share entities with table `{by}`: they are in one key \
                     group but their primary keys are of different types, so no row of one could \
                     ever meet a row of the other",
                    T::NAME
                );
            }
            self.app
                .world_mut()
                .register_component_hooks::<entity::Row<T>>()
                .on_remove(entity::on_row_removed::<T>);
            self.app.insert_resource(RowEntities::<T>::new(
                group,
                self.kept.contains(&table),
                !self.silent.contains(&table),
            ));
            return;
        }
        self.app
            .insert_resource(TableStore::<T>::new(!self.silent.contains(&table)))
            .add_observer(|check: On<CheckChangeTicks>, mut store: ResMut<TableStore<T>>| {
                store.bypass_change_detection().check_change_ticks(*check);
            });
    }
}

impl<M: Module> ReducerVisitor<M> for Registrar<'_, M> {
    fn reducer<R: Reducer<Module = M> + Clone>(&mut self) {
        self.app
            .add_message::<ReducerResult<R>>()
            .add_observer(crate::call::on_add::<M, R>);
    }
}

impl<M: Module> ProcedureVisitor<M> for Registrar<'_, M> {
    fn procedure<P: Procedure<Module = M>>(&mut self) {
        self.app
            .add_message::<ProcedureResult<P>>()
            .add_observer(crate::call::on_add_procedure::<M, P>);
    }
}
