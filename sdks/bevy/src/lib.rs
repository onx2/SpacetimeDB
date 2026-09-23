//! A Bevy-native [SpacetimeDB](https://spacetimedb.com) client.
//!
//! The client cache lives in the Bevy `World` rather than behind a wrapper around
//! `spacetimedb-sdk`: tables are read through system params and queries, row changes arrive as
//! messages, reducers are called from systems, and the connection is a Bevy state. Nothing hands
//! an app a callback that runs on another thread, and no row is cloned to be read.
//!
//! # Getting started
//!
//! Generate bindings for your module with `spacetime generate --lang bevy`, add [`StdbPlugin`]
//! for it, and write ordinary systems:
//!
//! ```no_run
//! # use bevy_app::prelude::*;
//! # use bevy_ecs::prelude::*;
//! # use spacetimedb_bevy::{ConnectOptions, Reducers, Rows, StdbCommandsExt, StdbPlugin, connected};
//! # use spacetimedb_bevy::__codegen::core::{Module, NoPk, ParseError, RawTableRows, ReducerVisitor, Table, TableKind, TableVisitor, UpdateVisitor};
//! # struct RemoteModule;
//! # impl Module for RemoteModule {
//! #     type Update = ();
//! #     fn parse_table(_: &mut (), _: &str, _: RawTableRows) -> Result<(), ParseError> { Ok(()) }
//! #     fn visit_update<V: UpdateVisitor<Self>>(_: (), _: &mut V) {}
//! #     fn visit_tables<V: TableVisitor<Self>>(_: &mut V) {}
//! #     fn visit_reducers<V: ReducerVisitor<Self>>(_: &mut V) {}
//! # }
//! # struct Player;
//! # impl Table for Player {
//! #     type Module = RemoteModule;
//! #     type Row = (u64, String);
//! #     type Pk = u64;
//! #     const NAME: &'static str = "player";
//! #     const KIND: TableKind = TableKind::Persistent;
//! #     fn pk(row: &Self::Row) -> Option<&u64> { Some(&row.0) }
//! # }
//! App::new()
//!     .add_plugins(StdbPlugin::<RemoteModule>::new().connect_to(ConnectOptions::new(
//!         "http://127.0.0.1:3000",
//!         "my-database",
//!     )))
//!     .add_systems(Startup, |mut commands: Commands| {
//!         commands.subscribe::<RemoteModule, _>(["SELECT * FROM player"]);
//!     })
//!     .add_systems(Update, greet.run_if(connected::<RemoteModule>()))
//!     .run();
//!
//! fn greet(players: Rows<Player>) {
//!     for player in players.iter() { /* one copy of each row, read where it lives */ }
//! }
//! ```
//!
//! # What to reach for
//!
//! | You want to | Use |
//! | --- | --- |
//! | Read a table | [`Rows<T>`], whichever way the table is stored |
//! | Have rows as entities | [`StdbPlugin::entity_table`], then `Query<&Row<T>>`; see [`entity`] |
//! | React to a change | [`RowInserted<T>`], [`RowUpdated<T>`], [`RowDeleted<T>`], [`RowEvent<T>`] |
//! | Subscribe | [`StdbCommandsExt::subscribe`], or a [`Subscription<M>`] entity |
//! | Call a reducer | [`Reducers<M>`], or a [`ReducerCall<R>`] entity |
//! | Know where the connection is | [`StdbState<M>`], [`connected`], [`StdbConnection<M>`] |
//!
//! # When a system sees what
//!
//! Transactions are applied one at a time, in the order the server sent them, and each is
//! finished before the next begins. A system in the [`StdbTransaction`] schedule runs once per
//! transaction, against the tables exactly as of that transaction — the strict tier, and the
//! equivalent of a callback in the official SDKs. The same messages read in `Update` give the
//! frame's transactions together, against the tables as they now are. Both are supported, and the
//! difference is only when a system runs, never what it may see.
//!
//! # Reading further
//!
//! `sdks/bevy/README.md` in the `SpacetimeDB` repository holds the quick start and the guide to
//! the two storage kinds, the two tiers of change handling, and reconnecting.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]

mod apply;
mod call;
mod connection;

// The client core, free of Bevy. `pub` only so that `__codegen::core` can re-export it, which is
// the path generated bindings use; it was the `spacetimedb_bevy_core` crate until it was folded
// in, and it carries the same "internal, no stability promise" documentation it did then.
#[doc(hidden)]
pub mod protocol;

pub mod entity;
mod messages;
mod pairing;
pub mod reflect;
mod store;
mod subscription;
mod threads;
mod unique;

use std::marker::PhantomData;

use bevy_app::{App, Plugin, PreUpdate, Startup};
use bevy_ecs::schedule::{Schedule, Schedules, SingleThreadedExecutor};
use bevy_state::app::{AppExtStates, StatesPlugin};
use bevy_tasks::{IoTaskPool, TaskPool};

pub use apply::{apply_update, from_module, StdbTransaction, TransactionSeq};
pub use call::{ProcedureCall, ProcedureFinished, ReducerCall, ReducerFinished};
pub use connection::{
    connected, Connected, ConnectionLost, ConnectionState, Disconnected, ProcedureResult, ProcedureStatus, Procedures,
    Reconnect, ReconnectPolicy, ReducerResult, ReducerStatus, Reducers, RequestId, StdbConnection, StdbIdentity,
    StdbState,
};
pub use entity::{Row, RowEntities, RowLeft};
pub use messages::{RowDeleted, RowEvent, RowInserted, RowUpdated};
pub use protocol::{
    CloseReason, Compression, ConnectError, ConnectOptions, Module, NoPk, Procedure, ProtocolError, QuerySetId,
    Reducer, RowSetError, Table, TableKind, UniqueColumn,
};
pub use store::{Rows, TableStore};
pub use subscription::{StdbCommandsExt, Subscription, SubscriptionApplied, SubscriptionFailed, SubscriptionState};

/// Everything generated bindings refer to, under stable paths.
#[doc(hidden)]
pub mod __codegen {
    pub use crate::protocol as core;
    pub use bevy_reflect as reflect;
    pub use bytes;
    pub use spacetimedb_lib as lib;
    pub use spacetimedb_query_builder as query;
}

/// A typed query, built from the `query` module of generated bindings.
pub use spacetimedb_query_builder::Query as TypedQuery;

/// Adds the tables and messages of module `M` to an app.
///
/// An app may hold one of these per module: [`StdbState<M>`], [`StdbIdentity<M>`] and
/// [`Reconnect<M>`] all name their module, so two connections keep their own. What they share is
/// the [`StdbTransaction`] schedule, which runs once per transaction whoever sent it; a handler in
/// it reads one module's messages against that module's tables, which is the guarantee it was
/// always making. A handler that works per transaction rather than per message says which module
/// it is about with [`from_module`].
#[doc(alias = "connect")]
#[doc(alias = "SpacetimeDB")]
pub struct StdbPlugin<M> {
    connect: Option<ConnectOptions>,
    reconnect: ReconnectPolicy,
    entity_tables: std::collections::HashMap<std::any::TypeId, Option<std::any::TypeId>>,
    all_entity_tables: bool,
    kept: std::collections::HashSet<std::any::TypeId>,
    silent: std::collections::HashSet<std::any::TypeId>,
    module: PhantomData<fn() -> M>,
}

impl<M> StdbPlugin<M> {
    /// Creates the tables and messages of `M`, with the connection closed.
    ///
    /// Connect later with [`StdbConnection::connect`], or at startup with [`Self::connect_to`].
    pub fn new() -> Self {
        Self {
            connect: None,
            reconnect: ReconnectPolicy::default(),
            entity_tables: Default::default(),
            all_entity_tables: false,
            kept: Default::default(),
            silent: Default::default(),
            module: PhantomData,
        }
    }

    /// Sets what to do when an established connection drops. The default retries with backoff;
    /// [`ReconnectPolicy::Never`] turns that off. Systems can change the resource later.
    pub fn with_reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    /// Stores table `T` as one entity per row, with the row in a [`Row<T>`] component, instead of
    /// in a [`TableStore`]. See [`entity`] for when that is the better choice.
    pub fn entity_table<T: Table<Module = M>>(mut self) -> Self {
        self.entity_tables.insert(std::any::TypeId::of::<T>(), None);
        self
    }

    /// Stores table `T` as entities shared with the other tables of key group `G`.
    ///
    /// Tables in one group have the same primary key type and mean the same thing by it. Rows with
    /// equal keys sit on one entity, each in its own [`Row`] component, so a query over several of
    /// them is a join: `Query<(&Row<Position>, &Row<Health>)>`. The entity is spawned with the
    /// first row to arrive and despawned when the last has left. `G` is any type of the app's,
    /// used only to name the group.
    ///
    /// This is a claim by the app, and nothing checks it beyond the key type. `SpacetimeDB` has no
    /// way to declare that two tables' keys refer to the same things, so this crate never infers a
    /// group: tables share entities only when listed here. If the claim is wrong, for example two
    /// tables that each number their rows from 1 with `auto_inc`, unrelated rows end up on one
    /// entity. Use it only where the module itself writes one id into both tables.
    ///
    /// ```
    /// # use spacetimedb_bevy::StdbPlugin;
    /// # use spacetimedb_bevy::__codegen::core::{Module, ParseError, RawTableRows, ReducerVisitor, Table, TableKind, TableVisitor, UpdateVisitor};
    /// # struct RemoteModule;
    /// # impl Module for RemoteModule {
    /// #     type Update = ();
    /// #     fn parse_table(_: &mut (), _: &str, _: RawTableRows) -> Result<(), ParseError> { Ok(()) }
    /// #     fn visit_update<V: UpdateVisitor<Self>>(_: (), _: &mut V) {}
    /// #     fn visit_tables<V: TableVisitor<Self>>(_: &mut V) {}
    /// #     fn visit_reducers<V: ReducerVisitor<Self>>(_: &mut V) {}
    /// # }
    /// # macro_rules! table {
    /// #     ($name:ident, $wire:literal) => {
    /// #         struct $name;
    /// #         impl Table for $name {
    /// #             type Module = RemoteModule;
    /// #             type Row = (u64, u32);
    /// #             type Pk = u64;
    /// #             const NAME: &'static str = $wire;
    /// #             const KIND: TableKind = TableKind::Persistent;
    /// #             fn pk(row: &Self::Row) -> Option<&u64> { Some(&row.0) }
    /// #         }
    /// #     };
    /// # }
    /// # table!(Position, "position");
    /// # table!(Health, "health");
    /// struct Unit;
    /// StdbPlugin::<RemoteModule>::new()
    ///     .entity_table_in::<Position, Unit>()
    ///     .entity_table_in::<Health, Unit>();
    /// ```
    ///
    /// # Across two databases
    ///
    /// A group entity is identified by the marker `G` and the row's primary key, and by nothing
    /// else — in particular not by the module. Two `StdbPlugin`s naming the same `G` therefore put
    /// rows with the same key on the same entity, and `Query<(&Row<A>, &Row<B>)>` joins two
    /// databases. The two tables' keys must be of one type for that to mean anything, and that
    /// much is checked: a `u64` in one module and an `Identity` in the other is refused when the
    /// second plugin is added, rather than left as a join that is silently empty.
    ///
    /// That is deliberate, and it is how an app does inter-database communication by hand while
    /// `SpacetimeDB` has none: one module hands out ids, the modules holding the data key their rows
    /// by them, and the client assembles each thing from both. The entity lives while *either*
    /// database still has a row for the key, so one connection going quiet does not destroy what
    /// the other is still filling in.
    ///
    /// **What it does not give is atomicity across the two.** Their transactions have no order
    /// between them, so a system reading both can see one side updated and the other not yet; the
    /// strict tier promises order within one module's stream, which is all the server promises.
    /// A real inter-database feature would lift that, and nothing here stands in its way.
    ///
    /// The corollary is that a marker is a namespace, so reusing a convenient one for two
    /// unrelated things in two modules merges them silently. Markers are free: give each its own.
    /// `tests/shared_entities.rs` holds both halves of this.
    ///
    /// # Panics
    ///
    /// When the plugin is added to an app, if `T` has no primary key. A group shares entities by
    /// key, so a table without one cannot be in a group; use [`Self::entity_table`] instead.
    ///
    /// Also then, if another table of group `G` — of this plugin or of one added before it — has
    /// a primary key of a different type. Rows of the two could never meet, so the join would be
    /// empty forever; the panic names both tables.
    pub fn entity_table_in<T: Table<Module = M>, G: 'static>(mut self) -> Self {
        self.entity_tables
            .insert(std::any::TypeId::of::<T>(), Some(std::any::TypeId::of::<G>()));
        self
    }

    /// Keeps the entity of a row of table `T` when the row leaves, marking it [`RowLeft<T>`]
    /// instead of despawning it, so the app can play an exit animation and despawn it itself.
    /// The row's [`Row<T>`] is removed either way. In a key group, one table asking for this
    /// keeps the group's entities.
    pub fn keep_entities_of<T: Table<Module = M>>(mut self) -> Self {
        self.kept.insert(std::any::TypeId::of::<T>());
        self
    }

    /// Writes no [`RowInserted`], [`RowUpdated`] or [`RowDeleted`] for table `T`.
    ///
    /// Each of those messages carries its row by value, which costs a clone of every inserted
    /// and updated row: about 40% of the time it takes to apply a large snapshot. For a big table
    /// that the app only ever reads through [`Rows`], [`Rows::iter_changed`] included, this
    /// saves that. Event tables are unaffected; their message is the only place their rows go.
    ///
    /// What is given up, which is why it is not the default:
    ///
    /// - Deletes become invisible. [`Rows::iter_changed`] yields rows that are there; nothing
    ///   says that a row left, or what an updated row used to hold ([`RowUpdated`] has `old`).
    /// - Inserts and updates look the same, and changes are not tied to their transaction.
    /// - Finding changes is a scan of the whole table's change ticks rather than a read of the few
    ///   messages written. An app that asks after every small transaction pays for it: in the
    ///   benchmark, 1,000 one-row transactions on a 100,000-row table take over ten times as long.
    pub fn without_row_messages<T: Table<Module = M>>(mut self) -> Self {
        self.silent.insert(std::any::TypeId::of::<T>());
        self
    }

    /// Stores every table as entities. Convenient for a small module or a test;
    /// for anything else choose per table with [`Self::entity_table`].
    pub fn all_tables_as_entities(mut self) -> Self {
        self.all_entity_tables = true;
        self
    }

    /// Connects when the app starts.
    pub fn connect_to(mut self, options: ConnectOptions) -> Self {
        self.connect = Some(options);
        self
    }
}

impl<M> Default for StdbPlugin<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Module> Plugin for StdbPlugin<M> {
    fn build(&self, app: &mut App) {
        // Two plugins for the same module are refused by Bevy itself, which tracks a plugin by its
        // type and so sees `StdbPlugin<M>` twice; nothing is needed here for that. Two plugins for
        // two different modules are the point of this phase and are fine.
        //
        // Shared by every module, and added once: a second plugin must not replace the first's
        // schedule, which is what it did while this was `add_schedule` unconditionally.
        if !app
            .world()
            .get_resource::<Schedules>()
            .is_some_and(|schedules| schedules.contains(StdbTransaction))
        {
            // Single-threaded: this schedule can run hundreds of times in a catch-up frame,
            // where the multi-threaded executor's per-run overhead would dominate.
            let mut per_transaction = Schedule::new(StdbTransaction);
            per_transaction.set_executor(SingleThreadedExecutor::new());
            app.add_schedule(per_transaction);
        }
        app.init_resource::<TransactionSeq<M>>();
        // Says whose transaction the shared schedule is running for; `from_module` reads it.
        // Shared like the schedule, and initialised here so a second module reuses the first's.
        app.init_resource::<crate::apply::ApplyingModule>();

        if !app.is_plugin_added::<StatesPlugin>() {
            app.add_plugins(StatesPlugin);
        }
        IoTaskPool::get_or_init(TaskPool::default);
        threads::lend_pool();
        app.init_state::<StdbState<M>>()
            .insert_resource(StdbConnection::<M>::new(self.connect.clone()))
            .init_resource::<connection::PendingCalls<M>>()
            .insert_resource(Reconnect::<M>::new(self.reconnect.clone()))
            .init_resource::<subscription::SubscriptionIndex<M>>()
            .add_observer(subscription::on_insert::<M>)
            .add_observer(subscription::on_remove::<M>)
            .add_systems(Startup, connection::connect_eagerly::<M>)
            .add_systems(PreUpdate, connection::drain_inbound::<M>);

        let mut registrar = apply::Registrar {
            app,
            entity_tables: &self.entity_tables,
            all_entity_tables: self.all_entity_tables,
            kept: &self.kept,
            silent: &self.silent,
            module: PhantomData,
        };
        M::visit_tables(&mut registrar);
        M::visit_reducers(&mut registrar);
        M::visit_procedures(&mut registrar);
    }
}
