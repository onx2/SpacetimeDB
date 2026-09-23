//! Reducer calls as entities, for calls whose result needs context.
//!
//! [`Reducers`](crate::Reducers) is the cheap way to call: fire, and read [`ReducerResult`] later.
//! A call entity costs a spawn, and in return the caller's context rides along as ordinary
//! components, the result arrives at an observer on that entity, and the call is visible to queries
//! while it is in flight.

use crate::protocol::{Module, Procedure, Reducer};
use bevy_ecs::prelude::*;
use bevy_ecs::system::EntityCommands;
use spacetimedb_lib::Timestamp;

use crate::connection::{ProcedureStatus, ReducerStatus, RequestId, StdbConnection};
use crate::subscription::StdbCommandsExt;

/// A call to reducer `R` that has not finished. Present on its entity for exactly that long:
/// `Query<&ReducerCall<BuyItem>>` finds the purchases in flight.
///
/// ```
/// # use bevy_ecs::prelude::*;
/// # use spacetimedb_bevy::{ReducerFinished, StdbCommandsExt};
/// # use spacetimedb_bevy::__codegen::core::Reducer;
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
/// # #[derive(__lib::ser::Serialize, __lib::de::Deserialize, Clone, Debug)]
/// # #[sats(crate = __lib)]
/// # struct BuyItem { item_id: u64 }
/// # impl Reducer for BuyItem {
/// #     type Module = ChatModule;
/// #     const NAME: &'static str = "buy_item";
/// # }
/// # #[derive(Component)]
/// # struct PurchaseButton(Entity);
/// fn buy(mut commands: Commands, item_id: u64, button: Entity) {
///     commands
///         .call_reducer(BuyItem { item_id })
///         .insert(PurchaseButton(button))
///         .observe(|done: On<ReducerFinished<BuyItem>>, buttons: Query<&PurchaseButton>| { /* .. */ });
/// }
/// ```
///
/// The entity is despawned after [`ReducerFinished`] has been delivered. Despawning it earlier
/// drops interest in the result; the call itself cannot be taken back.
#[derive(Component)]
#[component(immutable)]
pub struct ReducerCall<R: Reducer> {
    args: R,
}

impl<R: Reducer> ReducerCall<R> {
    /// Creates the component whose spawning makes the call; see
    /// [`StdbCommandsExt::call_reducer`](crate::StdbCommandsExt::call_reducer).
    pub fn new(args: R) -> Self {
        Self { args }
    }

    /// Returns the arguments the call was made with.
    pub fn args(&self) -> &R {
        &self.args
    }
}

/// Triggered on a call entity when its result is known, after the rows it changed are in the tables.
#[derive(EntityEvent)]
pub struct ReducerFinished<R: Reducer> {
    /// The call entity this is triggered on.
    pub entity: Entity,
    /// The call this answers, to match it to a [`ReducerResult`](crate::ReducerResult).
    pub request_id: RequestId,
    /// The arguments the call was made with.
    pub args: R,
    /// How it ended.
    pub status: ReducerStatus,
    /// When the host ran the reducer. `None` if the connection was lost.
    pub timestamp: Option<Timestamp>,
}

/// The call is made when its component lands, so that spawning one is all an app has to do.
pub(crate) fn on_add<M: Module, R: Reducer<Module = M> + Clone>(
    add: On<Add<ReducerCall<R>>>,
    connection: Res<StdbConnection<M>>,
    calls: Query<&ReducerCall<R>>,
) {
    if let Ok(call) = calls.get(add.entity) {
        connection.call_reducer_from(call.args.clone(), Some(add.entity));
    }
}

/// A call to procedure `P` that has not finished. Works like [`ReducerCall`].
#[derive(Component)]
#[component(immutable)]
pub struct ProcedureCall<P: Procedure> {
    args: P,
}

impl<P: Procedure> ProcedureCall<P> {
    /// Creates the component whose spawning makes the call; see
    /// [`StdbCommandsExt::call_procedure`](crate::StdbCommandsExt::call_procedure).
    pub fn new(args: P) -> Self {
        Self { args }
    }

    /// Returns the arguments the call was made with.
    pub fn args(&self) -> &P {
        &self.args
    }
}

/// Triggered on a call entity when the procedure has returned or failed.
#[derive(EntityEvent)]
pub struct ProcedureFinished<P: Procedure> {
    /// The call entity this is triggered on.
    pub entity: Entity,
    /// The call this answers, to match it to a [`ProcedureResult`](crate::ProcedureResult).
    pub request_id: RequestId,
    /// The arguments the call was made with.
    pub args: P,
    /// How it ended, with the return value if it returned one.
    pub status: ProcedureStatus<P::Output>,
    /// When the host finished the procedure. `None` if the connection was lost.
    pub timestamp: Option<Timestamp>,
}

/// As [`on_add`], for procedures.
pub(crate) fn on_add_procedure<M: Module, P: Procedure<Module = M>>(
    add: On<Add<ProcedureCall<P>>>,
    connection: Res<StdbConnection<M>>,
    calls: Query<&ProcedureCall<P>>,
) {
    if let Ok(call) = calls.get(add.entity) {
        connection.call_procedure_from(call.args.clone(), Some(add.entity));
    }
}

impl StdbCommandsExt for Commands<'_, '_> {
    fn connect<M: Module>(&mut self, options: crate::ConnectOptions) {
        self.queue(crate::connection::connect_command::<M>(options));
    }

    fn disconnect<M: Module>(&mut self) {
        self.queue(|world: &mut World| world.resource::<StdbConnection<M>>().disconnect());
    }

    fn subscribe<M: Module, Q: Into<Box<str>>>(&mut self, queries: impl IntoIterator<Item = Q>) -> EntityCommands<'_> {
        self.spawn(crate::Subscription::<M>::new(queries))
    }

    fn subscribe_to<M: Module, Row>(&mut self, query: impl crate::TypedQuery<Row>) -> EntityCommands<'_> {
        self.spawn(crate::Subscription::<M>::new([query.into_sql()]))
    }

    fn call_reducer<R: Reducer + Clone>(&mut self, args: R) -> EntityCommands<'_> {
        self.spawn(ReducerCall::new(args))
    }

    fn call_procedure<P: Procedure>(&mut self, args: P) -> EntityCommands<'_> {
        self.spawn(ProcedureCall::new(args))
    }
}
