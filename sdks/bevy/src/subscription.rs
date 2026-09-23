//! Subscriptions as entities: spawn one to subscribe, despawn it to unsubscribe.
//!
//! A subscription entity outlives connections. Spawned while disconnected it waits; after a
//! reconnect it is sent again. Its lifetime can follow game structure, for example as a child of
//! a level entity.

use std::collections::HashMap;
use std::marker::PhantomData;

use crate::protocol::{Module, Procedure, QuerySetId, Reducer};
use bevy_ecs::prelude::*;

use bevy_ecs::system::EntityCommands;

use crate::connection::StdbConnection;

/// Rows matching these queries are kept in the tables of module `M` while this component exists.
///
/// ```
/// # use bevy_ecs::prelude::*;
/// # use spacetimedb_bevy::{Subscription, SubscriptionApplied};
/// # use spacetimedb_bevy::__codegen::core::{Module, ParseError, RawTableRows, ReducerVisitor, TableVisitor, UpdateVisitor};
/// # use spacetimedb_bevy::__codegen::lib as __lib;
/// # struct ChatModule;
/// # impl Module for ChatModule {
/// #     type Update = ();
/// #     fn parse_table(_: &mut (), _: &str, _: RawTableRows) -> Result<(), ParseError> { Ok(()) }
/// #     fn visit_update<V: UpdateVisitor<Self>>(_: (), _: &mut V) {}
/// #     fn visit_tables<V: TableVisitor<Self>>(_: &mut V) {}
/// #     fn visit_reducers<V: ReducerVisitor<Self>>(_: &mut V) {}
/// # }
/// fn subscribe(mut commands: Commands) {
///     commands
///         .spawn(Subscription::<ChatModule>::new(["SELECT * FROM user"]))
///         .observe(|applied: On<SubscriptionApplied>| println!("users are in"));
/// }
/// ```
#[derive(Component)]
#[component(immutable)]
#[require(SubscriptionState)]
#[doc(alias = "SubscriptionHandle")]
#[doc(alias = "subscribe")]
pub struct Subscription<M: Module> {
    queries: Vec<Box<str>>,
    module: PhantomData<fn() -> M>,
}

impl<M: Module> Subscription<M> {
    /// Creates one subscription to all of `queries`. They are applied together, as one set.
    pub fn new<Q: Into<Box<str>>>(queries: impl IntoIterator<Item = Q>) -> Self {
        Self {
            queries: queries.into_iter().map(Into::into).collect(),
            module: PhantomData,
        }
    }

    /// Creates a subscription to `SELECT * FROM` every table of the module: simple, and only
    /// sensible for small databases.
    pub fn all_tables() -> Self {
        struct Names(Vec<Box<str>>);
        impl<M: Module> crate::protocol::TableVisitor<M> for Names {
            fn table<T: crate::protocol::Table<Module = M>>(&mut self) {
                self.0.push(format!("SELECT * FROM {}", T::NAME).into());
            }
        }
        let mut names = Names(Vec::new());
        M::visit_tables(&mut names);
        Self {
            queries: names.0,
            module: PhantomData,
        }
    }

    /// Returns the SQL this subscription was made with.
    pub fn queries(&self) -> &[Box<str>] {
        &self.queries
    }
}

/// Where a [`Subscription`] stands. Changed only by this crate.
#[derive(Component, Default, Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionState {
    /// Not sent, because there is no connection. Sent when there is one.
    #[default]
    Waiting,
    /// Sent; the initial rows have not arrived.
    Sent,
    /// The initial rows are in the tables and updates are flowing.
    Applied,
    /// The server rejected or dropped it. Its rows, if any, have left the tables.
    Failed(Box<str>),
}

/// Triggered on a subscription entity once its initial rows are in the tables.
#[derive(EntityEvent, Clone, Debug)]
pub struct SubscriptionApplied {
    /// The subscription entity this is triggered on.
    pub entity: Entity,
}

/// Triggered on a subscription entity when the server rejects or drops it.
#[derive(EntityEvent, Clone, Debug)]
pub struct SubscriptionFailed {
    /// The subscription entity this is triggered on.
    pub entity: Entity,
    /// What the server says is wrong with it.
    pub error: Box<str>,
}

/// Which entity each live query set belongs to.
#[derive(Resource)]
pub(crate) struct SubscriptionIndex<M> {
    by_query_set: HashMap<QuerySetId, Entity>,
    by_entity: HashMap<Entity, QuerySetId>,
    module: PhantomData<fn() -> M>,
}

impl<M> Default for SubscriptionIndex<M> {
    fn default() -> Self {
        Self {
            by_query_set: HashMap::new(),
            by_entity: HashMap::new(),
            module: PhantomData,
        }
    }
}

impl<M> SubscriptionIndex<M> {
    fn insert(&mut self, query_set_id: QuerySetId, entity: Entity) {
        self.by_query_set.insert(query_set_id, entity);
        self.by_entity.insert(entity, query_set_id);
    }

    fn remove_entity(&mut self, entity: Entity) -> Option<QuerySetId> {
        let query_set_id = self.by_entity.remove(&entity)?;
        self.by_query_set.remove(&query_set_id);
        Some(query_set_id)
    }

    fn remove_query_set(&mut self, query_set_id: QuerySetId) -> Option<Entity> {
        let entity = self.by_query_set.remove(&query_set_id)?;
        self.by_entity.remove(&entity);
        Some(entity)
    }
}

/// A subscription was added, or replaced by inserting a new one on the same entity.
///
/// A replacement subscribes to the new queries before unsubscribing from the old ones. Rows both
/// cover are refcounted by the server's answers, so they stay resident throughout.
pub(crate) fn on_insert<M: Module>(
    insert: On<Insert<Subscription<M>>>,
    connection: Res<StdbConnection<M>>,
    mut index: ResMut<SubscriptionIndex<M>>,
    mut subscriptions: Query<(&Subscription<M>, &mut SubscriptionState)>,
) {
    let Ok((subscription, mut state)) = subscriptions.get_mut(insert.entity) else {
        return;
    };
    if !connection.is_established() {
        // Sent by `send_all` once there is a connection.
        state.set_if_neq(SubscriptionState::Waiting);
        return;
    }
    let replaced = index.remove_entity(insert.entity);
    index.insert(
        connection.subscribe(subscription.queries.iter().cloned()),
        insert.entity,
    );
    if let Some(replaced) = replaced {
        connection.unsubscribe(replaced);
    }
    *state = SubscriptionState::Sent;
}

/// A subscription went away: tell the server, which answers with the rows to drop.
pub(crate) fn on_remove<M: Module>(
    remove: On<Remove<Subscription<M>>>,
    connection: Res<StdbConnection<M>>,
    mut index: ResMut<SubscriptionIndex<M>>,
) {
    if let Some(query_set_id) = index.remove_entity(remove.entity) {
        connection.unsubscribe(query_set_id);
    }
}

/// The connection is up: send every subscription that exists.
pub(crate) fn send_all<M: Module>(world: &mut World) {
    let mut waiting = world.query::<(Entity, &Subscription<M>)>();
    let waiting: Vec<(Entity, Vec<Box<str>>)> = waiting
        .iter(world)
        .map(|(entity, subscription)| (entity, subscription.queries.clone()))
        .collect();
    for (entity, queries) in waiting {
        let query_set_id = world.resource::<StdbConnection<M>>().subscribe(queries);
        world
            .resource_mut::<SubscriptionIndex<M>>()
            .insert(query_set_id, entity);
        world.entity_mut(entity).insert(SubscriptionState::Sent);
    }
}

/// The connection is back after a drop: send every subscription that exists as one batch, which
/// the server answers with one consistent snapshot. `false` if there is nothing to subscribe to.
pub(crate) fn send_all_as_batch<M: Module>(world: &mut World) -> bool {
    let mut waiting = world.query::<(Entity, &Subscription<M>)>();
    let waiting: Vec<(Entity, Vec<Box<str>>)> = waiting
        .iter(world)
        .map(|(entity, subscription)| (entity, subscription.queries.clone()))
        .collect();
    if waiting.is_empty() {
        return false;
    }
    let mut sets = Vec::new();
    for (entity, queries) in waiting {
        let query_set_id = world.resource::<StdbConnection<M>>().next_query_set_id();
        sets.push((query_set_id, queries));
        world
            .resource_mut::<SubscriptionIndex<M>>()
            .insert(query_set_id, entity);
        world.entity_mut(entity).insert(SubscriptionState::Sent);
    }
    world.resource::<StdbConnection<M>>().subscribe_batch(sets);
    true
}

/// The connection is gone: every subscription waits for the next one.
pub(crate) fn reset_all<M: Module>(world: &mut World) {
    *world.resource_mut::<SubscriptionIndex<M>>() = SubscriptionIndex::default();
    let mut subscriptions = world.query_filtered::<&mut SubscriptionState, With<Subscription<M>>>();
    for mut state in subscriptions.iter_mut(world) {
        // A failed subscription is retried on the next connection too; the failure may have been the server's.
        state.set_if_neq(SubscriptionState::Waiting);
    }
}

/// Records that a subscription's initial rows are in the tables, and tells its observers.
pub(crate) fn mark_applied<M: Module>(world: &mut World, query_set_id: QuerySetId) {
    let Some(&entity) = world.resource::<SubscriptionIndex<M>>().by_query_set.get(&query_set_id) else {
        // Despawned while its initial rows were in flight. The unsubscribe is already on its way.
        return;
    };
    world.entity_mut(entity).insert(SubscriptionState::Applied);
    world.trigger(SubscriptionApplied { entity });
}

/// Records that the server rejected or dropped a subscription, and tells its observers.
/// The entity remains, and is sent again on the next connection.
pub(crate) fn mark_failed<M: Module>(world: &mut World, query_set_id: QuerySetId, error: Box<str>) {
    let Some(entity) = world
        .resource_mut::<SubscriptionIndex<M>>()
        .remove_query_set(query_set_id)
    else {
        return;
    };
    world
        .entity_mut(entity)
        .insert(SubscriptionState::Failed(error.clone()));
    world.trigger(SubscriptionFailed { entity, error });
}

/// Shorthand on [`Commands`] for the connection and the entities this crate works through.
pub trait StdbCommandsExt {
    /// Starts connecting, as [`StdbConnection::connect`](crate::StdbConnection::connect) does.
    fn connect<M: Module>(&mut self, options: crate::ConnectOptions);

    /// Closes the connection for good, as
    /// [`StdbConnection::disconnect`](crate::StdbConnection::disconnect) does.
    fn disconnect<M: Module>(&mut self);

    /// Spawns a [`Subscription`] to `queries`. Despawn the returned entity to unsubscribe.
    fn subscribe<M: Module, Q: Into<Box<str>>>(&mut self, queries: impl IntoIterator<Item = Q>) -> EntityCommands<'_>;

    /// Spawns a [`Subscription`] to one typed query, built from the `query` module of the
    /// generated bindings: `commands.subscribe_to::<RemoteModule, _>(query::user().filter(|user|
    /// user.online.eq(true)))`. For several queries in one subscription, pass their
    /// [`into_sql`](crate::TypedQuery::into_sql) to [`Self::subscribe`].
    fn subscribe_to<M: Module, Row>(&mut self, query: impl crate::TypedQuery<Row>) -> EntityCommands<'_>;

    /// Spawns a [`ReducerCall`](crate::ReducerCall). Add components for context and observe
    /// [`ReducerFinished`](crate::ReducerFinished) on the returned entity.
    fn call_reducer<R: Reducer + Clone>(&mut self, args: R) -> EntityCommands<'_>;

    /// Spawns a [`ProcedureCall`](crate::ProcedureCall). Observe
    /// [`ProcedureFinished`](crate::ProcedureFinished) on the returned entity for the return value.
    fn call_procedure<P: Procedure>(&mut self, args: P) -> EntityCommands<'_>;
}
