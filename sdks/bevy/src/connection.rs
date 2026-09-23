//! The connection as seen from the `World`: a resource to talk through, a state to gate systems on,
//! and one exclusive system that applies whatever arrived since last frame.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::protocol::{
    ClientMessage, CloseReason, ConnectOptions, Inbound, Module, Procedure, QuerySetId, Raw, Reducer, ReducerOutcome,
    Socket,
};
use async_channel::{Receiver, Sender};
use bevy_ecs::message::Message;
use bevy_ecs::prelude::*;
use bevy_ecs::system::SystemParam;
use bevy_platform::time::Instant;
use bevy_state::prelude::*;
use bevy_tasks::futures_lite::future;
use bevy_tasks::IoTaskPool;
use bytes::Bytes;
use derive_where::derive_where;
use spacetimedb_lib::{bsatn, ConnectionId, Identity, Timestamp};

use crate::apply::{apply_paired, clear_tables, next_seq, run_transaction_schedule};
use crate::call::{ProcedureFinished, ReducerFinished};
use crate::pairing::{parse_and_pair_loop, Arrived, SharedPairing};
use crate::subscription;

/// Where a connection is in its life, without saying whose.
///
/// [`StdbState<M>`] is the state a system gates on; this is what it holds, and what to match when a
/// system wants to tell the cases apart.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// No connection, and none being made.
    #[default]
    Disconnected,
    /// The socket is opening, or open and waiting for the server's first message.
    Connecting,
    /// The server has accepted the connection; subscriptions and calls go through.
    Connected,
    /// The connection was lost and [`ReconnectPolicy`] is retrying. Tables keep their last known
    /// rows and stay readable; calls made now fail with `ConnectionLost`.
    Reconnecting,
}

/// Where one module's connection is in its life.
///
/// There is one of these per module, so an app connected to two databases has two states and
/// neither can speak for the other. Gate a system on it with
/// `run_if(in_state(StdbState::<M>::connected()))`, or with the shorter
/// [`connected::<M>()`](connected), and reach the value itself through [`Self::connection`] when a
/// system wants to match on it.
///
/// It is a struct wrapping [`ConnectionState`] rather than an enum of its own because a generic
/// enum would need its module in one of the variants, and `StdbState::Connected(PhantomData)` is
/// not a thing anyone should have to write.
#[derive(States)]
#[derive_where(Clone, Copy, Default, Debug, PartialEq, Eq, Hash)]
pub struct StdbState<M: Module> {
    /// Where the connection is.
    pub connection: ConnectionState,
    module: PhantomData<M>,
}

impl<M: Module> StdbState<M> {
    /// Returns the state holding `connection`.
    pub const fn new(connection: ConnectionState) -> Self {
        Self {
            connection,
            module: PhantomData,
        }
    }

    /// Returns the state for no connection, and none being made.
    pub const fn disconnected() -> Self {
        Self::new(ConnectionState::Disconnected)
    }

    /// Returns the state for a connection being made.
    pub const fn connecting() -> Self {
        Self::new(ConnectionState::Connecting)
    }

    /// Returns the state for a connection the server has accepted.
    pub const fn connected() -> Self {
        Self::new(ConnectionState::Connected)
    }

    /// Returns the state for a lost connection that is being retried.
    pub const fn reconnecting() -> Self {
        Self::new(ConnectionState::Reconnecting)
    }
}

/// A run condition that holds while `M`'s connection is up.
///
/// `run_if(connected::<RemoteModule>())` is `run_if(in_state(StdbState::<RemoteModule>::connected()))`,
/// which is the same thing said at more length.
pub fn connected<M: Module>() -> impl FnMut(Option<Res<State<StdbState<M>>>>) -> bool + Clone {
    in_state(StdbState::<M>::connected())
}

/// What to do when a connection that was established ends without being asked to.
///
/// Set it on the plugin, and change it from any system at any time through the [`Reconnect<M>`]
/// resource that holds it. The value in force when the connection drops, and again after each
/// failed attempt, decides what happens next.
#[derive(Clone, Debug, PartialEq)]
pub enum ReconnectPolicy {
    /// Empty the tables and go to [`ConnectionState::Disconnected`].
    Never,
    /// Retry after `initial`, doubling up to `max` between attempts.
    Backoff {
        /// How long to wait before the first retry.
        initial: Duration,
        /// The longest the wait may grow to.
        max: Duration,
        /// Give up after this many failed attempts in a row. `None` retries forever.
        max_attempts: Option<u32>,
    },
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self::Backoff {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            max_attempts: Some(10),
        }
    }
}

impl ReconnectPolicy {
    /// Returns how long to wait before the next attempt, after `failed` attempts in a row,
    /// or `None` to give up.
    fn delay(&self, failed: u32) -> Option<Duration> {
        match *self {
            Self::Never => None,
            Self::Backoff { max_attempts, .. } if max_attempts.is_some_and(|most| failed >= most) => None,
            Self::Backoff { initial, max, .. } => Some(initial.saturating_mul(1 << failed.min(16)).min(max)),
        }
    }
}

/// The [`ReconnectPolicy`] in force for one module's connection.
///
/// A resource per module, so two connections can retry on different terms. Change it from any
/// system: `reconnect.policy = ReconnectPolicy::Never`.
#[derive(Resource)]
#[derive_where(Clone, Debug, PartialEq)]
pub struct Reconnect<M: Module> {
    /// What to do when this module's connection ends without being asked to.
    pub policy: ReconnectPolicy,
    module: PhantomData<M>,
}

impl<M: Module> Reconnect<M> {
    /// Returns the resource holding `policy`.
    pub const fn new(policy: ReconnectPolicy) -> Self {
        Self {
            policy,
            module: PhantomData,
        }
    }
}

impl<M: Module> Default for Reconnect<M> {
    fn default() -> Self {
        Self::new(ReconnectPolicy::default())
    }
}

/// Triggered each time [`ReconnectPolicy`] schedules another attempt for `M`'s connection.
#[derive(Event)]
#[derive_where(Clone, Debug)]
pub struct ConnectionLost<M: Module> {
    /// Why the connection, or the latest attempt at one, ended.
    pub reason: Arc<CloseReason>,
    /// 1 for the first attempt after the drop.
    pub attempt: u32,
    /// How long until that attempt.
    pub retry_in: Duration,
    module: PhantomData<M>,
}

impl<M: Module> ConnectionLost<M> {
    /// Returns the event for the `attempt`th try after `reason` ended the connection.
    pub(crate) const fn new(reason: Arc<CloseReason>, attempt: u32, retry_in: Duration) -> Self {
        Self {
            reason,
            attempt,
            retry_in,
            module: PhantomData,
        }
    }
}

/// Who this client is to one module's database. Present exactly while that module's connection is
/// [`ConnectionState::Connected`].
#[derive(Resource)]
#[derive_where(Clone, Debug)]
#[doc(alias = "identity")]
pub struct StdbIdentity<M: Module> {
    /// Who the server decided this client is.
    pub identity: Identity,
    /// Names this connection, as distinct from other connections of the same identity.
    pub connection_id: ConnectionId,
    /// Pass to [`ConnectOptions::token`] next time to connect as the same identity.
    pub token: Box<str>,
    module: PhantomData<M>,
}

impl<M: Module> StdbIdentity<M> {
    /// Returns the identity the server gave this connection.
    pub(crate) const fn new(identity: Identity, connection_id: ConnectionId, token: Box<str>) -> Self {
        Self {
            identity,
            connection_id,
            token,
            module: PhantomData,
        }
    }
}

/// Triggered when the server has accepted `M`'s connection.
#[derive(Event)]
#[derive_where(Clone, Debug)]
pub struct Connected<M: Module> {
    /// Who the server decided this client is.
    pub identity: Identity,
    module: PhantomData<M>,
}

impl<M: Module> Connected<M> {
    /// Returns the event for a connection accepted as `identity`.
    pub(crate) const fn new(identity: Identity) -> Self {
        Self {
            identity,
            module: PhantomData,
        }
    }
}

/// Triggered when `M`'s connection has ended for good, or could not be made. Its tables are empty
/// by then.
#[derive(Event)]
#[derive_where(Clone, Debug)]
pub struct Disconnected<M: Module> {
    /// Why it ended. [`CloseReason::Requested`] if the app asked for it.
    pub reason: Arc<CloseReason>,
    module: PhantomData<M>,
}

impl<M: Module> Disconnected<M> {
    /// Returns the event for a connection ended by `reason`.
    pub(crate) const fn new(reason: Arc<CloseReason>) -> Self {
        Self {
            reason,
            module: PhantomData,
        }
    }
}

/// Names one call, to match a result to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RequestId(pub u32);

/// How a reducer call ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReducerStatus {
    /// The rows it changed are already in the tables.
    Committed,
    /// The reducer returned this error and changed nothing.
    Failed(String),
    /// The host could not run the reducer.
    InternalError(String),
    /// The connection ended first. The reducer may or may not have run.
    ConnectionLost,
}

/// The result of calling reducer `R` from this client.
pub struct ReducerResult<R: Reducer> {
    /// The call this answers, as [`StdbConnection::call_reducer`] returned it.
    pub request_id: RequestId,
    /// The arguments the call was made with.
    pub args: R,
    /// How it ended.
    pub status: ReducerStatus,
    /// When the host ran the reducer. `None` if the connection was lost.
    pub timestamp: Option<Timestamp>,
}

impl<R: Reducer> Message for ReducerResult<R> {}

/// How a procedure call ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProcedureStatus<T> {
    /// The procedure returned this value.
    Returned(T),
    /// The procedure panicked, or the host could not run it.
    InternalError(String),
    /// The connection ended first. The procedure may or may not have run.
    ConnectionLost,
}

/// The result of calling procedure `P` from this client.
pub struct ProcedureResult<P: Procedure> {
    /// The call this answers, as [`StdbConnection::call_procedure`] returned it.
    pub request_id: RequestId,
    /// The arguments the call was made with.
    pub args: P,
    /// How it ended, with the return value if it returned one.
    pub status: ProcedureStatus<P::Output>,
    /// When the host finished the procedure. `None` if the connection was lost.
    pub timestamp: Option<Timestamp>,
}

impl<P: Procedure> Message for ProcedureResult<P> {}

/// What the server said about a call, before it is given the call's types.
pub(crate) enum Outcome {
    Reducer(ReducerStatus),
    /// The BSATN-encoded return value, or the host's error.
    Procedure(Result<Bytes, Box<str>>),
    ConnectionLost,
}

/// Writes a call's typed result once its outcome is known.
type Deliver = Box<dyn FnOnce(&mut World, Outcome, Option<Timestamp>) + Send + Sync>;

/// A call that has gone out and not been answered: what to do with the answer.
pub(crate) struct Pending {
    pub deliver: Deliver,
}

type NewCall = (u32, Pending);

/// The connection to the database of module `M`.
#[derive(Resource)]
#[doc(alias = "DbConnection")]
#[doc(alias = "connection")]
pub struct StdbConnection<M: Module> {
    /// `None` while disconnected.
    outbound: Option<Sender<ClientMessage>>,
    inbound: Option<Receiver<Arrived<M>>>,
    /// What is resident, as the task that parses keeps it. See [`crate::pairing`].
    pairing: SharedPairing<M>,
    /// Calls made since the drain system last ran, on their way into `pending`.
    /// A channel rather than a map so that calling needs only `&self`.
    new_calls: (Sender<NewCall>, Receiver<NewCall>),
    /// The server's first message has arrived, so requests can be sent.
    established: bool,
    next_request_id: AtomicU32,
    next_query_set_id: AtomicU32,
    /// Connect when the app starts.
    pub(crate) eager: Option<ConnectOptions>,
    /// What the current or latest connection was opened with, kept for reconnecting.
    last_options: Option<ConnectOptions>,
    /// [`Self::disconnect`] was called, so the next close is final whatever the policy says.
    close_requested: AtomicBool,
    /// Attempts that have failed since the connection was last established.
    failed_attempts: u32,
    /// When to try again. `Some` exactly while waiting between attempts.
    retry_at: Option<Instant>,
    /// A `SubscribeBatch` is out and its answer is not in: the tables hold rows from before
    /// the drop, waiting to be reconciled.
    awaiting_batch: bool,
    /// Payload bytes the socket has handed over, counted by the task that parses them.
    bytes_in: Arc<AtomicU64>,
    /// Payload bytes the socket has written, counted where it writes them.
    bytes_out: Arc<AtomicU64>,
}

impl<M: Module> StdbConnection<M> {
    /// Creates a closed connection. `eager` is the options to connect with when the app starts.
    pub(crate) fn new(eager: Option<ConnectOptions>) -> Self {
        Self {
            outbound: None,
            inbound: None,
            pairing: SharedPairing::<M>::default(),
            new_calls: async_channel::unbounded(),
            established: false,
            next_request_id: AtomicU32::new(1),
            next_query_set_id: AtomicU32::new(1),
            eager,
            last_options: None,
            close_requested: AtomicBool::new(false),
            failed_attempts: 0,
            retry_at: None,
            awaiting_batch: false,
            bytes_in: Arc::new(AtomicU64::new(0)),
            bytes_out: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns a handle on the record of what is resident, for the parsing task to pair against.
    pub(crate) fn pairing(&self) -> SharedPairing<M> {
        self.pairing.clone()
    }

    /// For when the tables are emptied. See [`clear_tables`].
    ///
    /// The record is emptied where it is, because the task of a connection that has just come
    /// up already holds it. No task is part-way through a message at any of the moments the
    /// tables are emptied: a connection's last message is what ends it, and a new connection
    /// is sent no rows before it subscribes.
    pub(crate) fn forget_residents(&self) {
        *self.pairing.lock().unwrap_or_else(|error| error.into_inner()) = Default::default();
    }

    /// Returns how many bytes of server messages have arrived since the app started.
    ///
    /// Counted where the socket hands payloads to the task that parses them, so it is what came
    /// off the wire before any decoding, and it does not start again at a reconnect. The `dev`
    /// feature reports a rate from it; on its own it is a plain running total.
    pub fn bytes_received(&self) -> u64 {
        self.bytes_in.load(Ordering::Relaxed)
    }

    /// Returns how many bytes of client messages have gone out since the app started.
    ///
    /// Counted where the socket writes them, so it is what went on the wire, and it does not
    /// start again at a reconnect.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_out.load(Ordering::Relaxed)
    }

    /// Returns how many parsed messages are waiting to be applied.
    ///
    /// Messages are parsed off the main thread and applied on it, so this is the depth of the
    /// queue between the two: steadily above zero means the frame cannot keep up with the server.
    pub fn queued_messages(&self) -> usize {
        self.inbound.as_ref().map_or(0, |inbound| inbound.len())
    }

    /// Returns `true` while a connection is open, opening, or closing.
    pub fn is_open(&self) -> bool {
        self.inbound.is_some()
    }

    /// Returns `true` once the server has accepted the connection: exactly while
    /// [`ConnectionState::Connected`] is entered or due.
    pub fn is_established(&self) -> bool {
        self.established
    }

    /// Starts connecting. Progress shows up as [`StdbState<M>`] changes and a [`Connected<M>`] or
    /// [`Disconnected<M>`] event. Does nothing if a connection is already open or opening.
    pub fn connect(&mut self, options: ConnectOptions, next_state: &mut NextState<StdbState<M>>) {
        if self.is_open() || self.retry_at.is_some() {
            return;
        }
        self.close_requested.store(false, Ordering::Relaxed);
        self.failed_attempts = 0;
        let mut options = options;
        // One session for this connection and every reconnect of it.
        options.session_id.get_or_insert_with(ConnectOptions::random_session_id);
        self.open(options);
        next_state.set(StdbState::connecting());
    }

    fn open(&mut self, options: ConnectOptions) {
        self.last_options = Some(options.clone());
        let (outbound_tx, outbound_rx) = async_channel::unbounded();
        let (raw_tx, raw_rx) = async_channel::unbounded();
        let (inbound_tx, inbound_rx) = async_channel::unbounded();

        let pool = IoTaskPool::get();
        let closed = outbound_tx.clone();
        let sent = self.bytes_out.clone();
        pool.spawn(async move {
            // `disconnect` closes `outbound`, which nothing reads until the socket runs. While
            // it is still opening, that is watched for here, so that the attempt ends at once
            // and not when the host answers or `connect_timeout` runs out.
            let connecting = future::or(async { Some(Socket::connect(&options).await) }, async move {
                closed.closed().await;
                None
            });
            let reason = match connecting.await {
                Some(Ok(socket)) => return socket.run(outbound_rx, raw_tx, sent).await,
                Some(Err(error)) => error.into(),
                None => CloseReason::Requested,
            };
            let _ = raw_tx.send(Raw::Closed(reason)).await;
        })
        .detach();
        pool.spawn(parse_and_pair_loop::<M>(
            raw_rx,
            inbound_tx,
            self.pairing.clone(),
            self.bytes_in.clone(),
        ))
        .detach();

        self.outbound = Some(outbound_tx);
        self.inbound = Some(inbound_rx);
    }

    /// Closes the connection, or stops trying to get it back. The tables are emptied when that
    /// completes, and [`ReconnectPolicy`] does not apply.
    ///
    /// Takes `&self` so that a system can hold [`Reducers`] as well; see also
    /// [`StdbCommandsExt::disconnect`](crate::StdbCommandsExt::disconnect).
    pub fn disconnect(&self) {
        self.close_requested.store(true, Ordering::Relaxed);
        if let Some(outbound) = &self.outbound {
            outbound.close();
        }
    }

    fn send(&self, message: ClientMessage) {
        if let Some(outbound) = &self.outbound {
            // Fails only if the socket task has ended, in which case `Closed` is on its way.
            let _ = outbound.try_send(message);
        }
    }

    fn next_request_id(&self) -> u32 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Returns an id for a subscription that is about to be sent. Apart from [`Self::subscribe`]
    /// so that a batch can be numbered before any of it goes out.
    pub(crate) fn next_query_set_id(&self) -> QuerySetId {
        QuerySetId::new(self.next_query_set_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Sends several subscriptions as one message, for the server to answer with one snapshot.
    /// Only after a drop; see [`drain_messages`].
    pub(crate) fn subscribe_batch(&self, sets: Vec<(QuerySetId, Vec<Box<str>>)>) {
        self.send(ClientMessage::subscribe_batch(self.next_request_id(), sets));
    }

    /// Sends one subscription and returns the id the server's answers will name it by.
    pub(crate) fn subscribe<Q: Into<Box<str>>>(&self, queries: impl IntoIterator<Item = Q>) -> QuerySetId {
        let query_set_id = self.next_query_set_id();
        self.send(ClientMessage::subscribe(self.next_request_id(), query_set_id, queries));
        query_set_id
    }

    /// Ends one subscription. The server answers with the rows only it covered.
    pub(crate) fn unsubscribe(&self, query_set_id: QuerySetId) {
        self.send(ClientMessage::unsubscribe(self.next_request_id(), query_set_id));
    }

    /// Calls a reducer. Its [`ReducerResult<R>`] message arrives after the rows it changed.
    pub fn call_reducer<R: Reducer<Module = M> + Clone>(&self, args: R) -> RequestId {
        self.call_reducer_from(args, None)
    }

    /// Calls a reducer on behalf of a call entity.
    ///
    /// `caller`, if it still exists when the result arrives, gets [`ReducerFinished`] and is despawned.
    pub(crate) fn call_reducer_from<R: Reducer<Module = M> + Clone>(
        &self,
        args: R,
        caller: Option<Entity>,
    ) -> RequestId {
        let request_id = RequestId(self.next_request_id());
        let message = ClientMessage::call_reducer(request_id.0, &args);

        let deliver: Deliver = Box::new(move |world, outcome, timestamp| {
            let status = match outcome {
                Outcome::Reducer(status) => status,
                Outcome::ConnectionLost => ReducerStatus::ConnectionLost,
                Outcome::Procedure(_) => {
                    ReducerStatus::InternalError("the server answered with a procedure result".into())
                }
            };
            let caller = caller.filter(|&entity| world.get_entity(entity).is_ok());
            world.write_message(ReducerResult {
                request_id,
                args: args.clone(),
                status: status.clone(),
                timestamp,
            });
            if let Some(entity) = caller {
                world.trigger(ReducerFinished {
                    entity,
                    request_id,
                    args,
                    status,
                    timestamp,
                });
                // An observer may have despawned it already.
                if let Ok(entity) = world.get_entity_mut(entity) {
                    entity.despawn();
                }
            }
        });
        let _ = self.new_calls.0.try_send((request_id.0, Pending { deliver }));
        self.send(message);
        request_id
    }

    /// Calls a procedure. Its [`ProcedureResult<P>`] message carries the typed return value.
    pub fn call_procedure<P: Procedure<Module = M>>(&self, args: P) -> RequestId {
        self.call_procedure_from(args, None)
    }

    /// Calls a procedure on behalf of a call entity.
    ///
    /// `caller`, if it still exists when the result arrives, gets [`ProcedureFinished`] and is despawned.
    pub(crate) fn call_procedure_from<P: Procedure<Module = M>>(&self, args: P, caller: Option<Entity>) -> RequestId {
        let request_id = RequestId(self.next_request_id());
        let message = ClientMessage::call_procedure(request_id.0, &args);

        let deliver: Deliver = Box::new(move |world, outcome, timestamp| {
            let status = match outcome {
                Outcome::Procedure(Ok(bytes)) => match bsatn::from_slice::<P::Output>(&bytes) {
                    Ok(value) => ProcedureStatus::Returned(value),
                    Err(error) => ProcedureStatus::InternalError(format!(
                        "could not decode the return value of `{}`: {error}",
                        P::NAME
                    )),
                },
                Outcome::Procedure(Err(error)) => ProcedureStatus::InternalError(error.into()),
                Outcome::ConnectionLost => ProcedureStatus::ConnectionLost,
                Outcome::Reducer(_) => {
                    ProcedureStatus::InternalError("the server answered with a reducer result".into())
                }
            };
            let caller = caller.filter(|&entity| world.get_entity(entity).is_ok());
            world.write_message(ProcedureResult {
                request_id,
                args: args.clone(),
                status: status.clone(),
                timestamp,
            });
            if let Some(entity) = caller {
                world.trigger(ProcedureFinished {
                    entity,
                    request_id,
                    args,
                    status,
                    timestamp,
                });
                // An observer may have despawned it already.
                if let Ok(entity) = world.get_entity_mut(entity) {
                    entity.despawn();
                }
            }
        });
        let _ = self.new_calls.0.try_send((request_id.0, Pending { deliver }));
        self.send(message);
        request_id
    }
}

/// Calls awaiting their result.
#[derive(Resource)]
pub(crate) struct PendingCalls<M> {
    calls: HashMap<u32, Pending>,
    module: PhantomData<fn() -> M>,
}

impl<M> PendingCalls<M> {
    /// Takes the call that answers `request_id`.
    fn answer(&mut self, request_id: u32) -> Option<Pending> {
        self.calls.remove(&request_id)
    }
}

impl<M> Default for PendingCalls<M> {
    fn default() -> Self {
        Self {
            calls: HashMap::new(),
            module: PhantomData,
        }
    }
}

/// Calls reducers of module `M`. Generated bindings add one method per reducer.
#[derive(SystemParam)]
#[doc(alias = "call_reducer")]
#[doc(alias = "reducer")]
pub struct Reducers<'w, M: Module> {
    connection: Res<'w, StdbConnection<M>>,
}

impl<M: Module> Reducers<'_, M> {
    /// Calls a reducer by its args type, as [`StdbConnection::call_reducer`] does.
    pub fn call<R: Reducer<Module = M> + Clone>(&self, args: R) -> RequestId {
        self.connection.call_reducer(args)
    }
}

/// Calls procedures of module `M`. Generated bindings add one method per procedure.
#[derive(SystemParam)]
#[doc(alias = "call_procedure")]
pub struct Procedures<'w, M: Module> {
    connection: Res<'w, StdbConnection<M>>,
}

impl<M: Module> Procedures<'_, M> {
    /// Calls a procedure by its args type, as [`StdbConnection::call_procedure`] does.
    pub fn call<P: Procedure<Module = M>>(&self, args: P) -> RequestId {
        self.connection.call_procedure(args)
    }
}

/// Connects at startup, for a plugin built with
/// [`StdbPlugin::connect_to`](crate::StdbPlugin::connect_to).
pub(crate) fn connect_eagerly<M: Module>(
    mut connection: ResMut<StdbConnection<M>>,
    mut next_state: ResMut<NextState<StdbState<M>>>,
) {
    if let Some(options) = connection.eager.take() {
        connection.connect(options, &mut next_state);
    }
}

/// Applies everything that arrived since this system last ran, one server message at a time.
pub(crate) fn drain_inbound<M: Module>(world: &mut World) {
    drain_messages::<M>(world);
}

/// Applies every message that has arrived, one transaction at a time.
fn drain_messages<M: Module>(world: &mut World) {
    let connection = world.resource::<StdbConnection<M>>();
    let Some(inbound) = connection.inbound.clone() else {
        // Calls made while disconnected can never complete.
        fail_pending::<M>(world);
        retry_if_due::<M>(world);
        return;
    };

    // Calls the frame made are taken up whether or not anything has arrived, so that a call that
    // has gone out is one the client knows about — which is what an inspector reports.
    collect_new_calls::<M>(world);

    while let Ok(arrived) = inbound.try_recv() {
        // A handler that ran for the previous message may have made calls.
        collect_new_calls::<M>(world);

        let (message, paired) = match arrived {
            Arrived::Message(message, paired) => (message, paired),
            Arrived::Unsound(error) => {
                // Nothing of the message was applied, but what is resident is no longer known,
                // so there is no cache to keep for a reconnect.
                clear_tables::<M>(world);
                return close::<M>(world, error.into());
            }
        };

        match message {
            Inbound::Connected {
                identity,
                connection_id,
                token,
            } => {
                world.insert_resource(StdbIdentity::<M>::new(identity, connection_id, token));
                let mut connection = world.resource_mut::<StdbConnection<M>>();
                connection.established = true;
                let back_after_a_drop = connection.failed_attempts > 0;
                world
                    .resource_mut::<NextState<StdbState<M>>>()
                    .set(StdbState::<M>::connected());
                if !back_after_a_drop {
                    subscription::send_all::<M>(world);
                } else if subscription::send_all_as_batch::<M>(world) {
                    // The rows kept for reading meanwhile stay until the server's one snapshot of
                    // every subscription arrives, and then only what differs changes.
                    world.resource_mut::<StdbConnection<M>>().awaiting_batch = true;
                } else {
                    // Nothing is subscribed, so there is nothing to reconcile: start clean.
                    world.resource_mut::<StdbConnection<M>>().failed_attempts = 0;
                    clear_tables::<M>(world);
                }
                world.trigger(Connected::<M>::new(identity));
            }
            Inbound::Transaction(_) => {
                apply_paired::<M>(world, paired);
                run_transaction_schedule::<M>(world);
            }
            Inbound::SubscribeBatchApplied { sets, .. } => {
                let mut connection = world.resource_mut::<StdbConnection<M>>();
                connection.awaiting_batch = false;
                connection.failed_attempts = 0;
                apply_paired::<M>(world, paired);
                for (query_set_id, error) in sets {
                    match error {
                        None => subscription::mark_applied::<M>(world, query_set_id),
                        Some(error) => subscription::mark_failed::<M>(world, query_set_id, error),
                    }
                }
                run_transaction_schedule::<M>(world);
            }
            Inbound::SubscribeApplied { query_set_id, .. } => {
                apply_paired::<M>(world, paired);
                subscription::mark_applied::<M>(world, query_set_id);
                run_transaction_schedule::<M>(world);
            }
            Inbound::UnsubscribeApplied { .. } => {
                apply_paired::<M>(world, paired);
                run_transaction_schedule::<M>(world);
            }
            Inbound::SubscriptionError { query_set_id, error } => {
                next_seq::<M>(world);
                subscription::mark_failed::<M>(world, query_set_id, error);
                run_transaction_schedule::<M>(world);
            }
            Inbound::ReducerResult {
                request_id,
                timestamp,
                outcome,
            } => {
                let status = match outcome {
                    ReducerOutcome::Committed(_) => {
                        apply_paired::<M>(world, paired);
                        ReducerStatus::Committed
                    }
                    ReducerOutcome::Failed(error) => ReducerStatus::Failed(error),
                    ReducerOutcome::InternalError(error) => ReducerStatus::InternalError(error),
                };
                let deliver = world.resource_mut::<PendingCalls<M>>().answer(request_id);
                if let Some(pending) = deliver {
                    (pending.deliver)(world, Outcome::Reducer(status), Some(timestamp));
                }
                run_transaction_schedule::<M>(world);
            }
            // A procedure's row changes arrived earlier, as ordinary transactions.
            Inbound::ProcedureResult {
                request_id,
                timestamp,
                result,
            } => {
                let deliver = world.resource_mut::<PendingCalls<M>>().answer(request_id);
                if let Some(pending) = deliver {
                    next_seq::<M>(world);
                    (pending.deliver)(world, Outcome::Procedure(result), Some(timestamp));
                    run_transaction_schedule::<M>(world);
                }
            }
            Inbound::Closed(reason) => return close::<M>(world, reason),
        }
    }
}

fn collect_new_calls<M: Module>(world: &mut World) {
    let new_calls = world.resource::<StdbConnection<M>>().new_calls.1.clone();
    let mut pending = world.resource_mut::<PendingCalls<M>>();
    while let Ok((request_id, call)) = new_calls.try_recv() {
        pending.calls.insert(request_id, call);
    }
}

fn fail_pending<M: Module>(world: &mut World) {
    collect_new_calls::<M>(world);
    let calls = std::mem::take(&mut world.resource_mut::<PendingCalls<M>>().calls);
    if calls.is_empty() {
        return;
    }
    for pending in calls.into_values() {
        (pending.deliver)(world, Outcome::ConnectionLost, None);
    }
    run_transaction_schedule::<M>(world);
}

/// While waiting between attempts: try again when it is time, or stop if asked to.
fn retry_if_due<M: Module>(world: &mut World) {
    let mut connection = world.resource_mut::<StdbConnection<M>>();
    let Some(retry_at) = connection.retry_at else {
        return;
    };
    if connection.close_requested.load(Ordering::Relaxed) {
        return finish::<M>(world, CloseReason::Requested);
    }
    if Instant::now() >= retry_at {
        connection.retry_at = None;
        let options = connection.last_options.clone().expect("set by every `open`");
        connection.open(options);
    }
}

/// The connection ended. Retry if that was not asked for and the policy allows, else finish.
fn close<M: Module>(world: &mut World, reason: CloseReason) {
    let policy = world
        .get_resource::<Reconnect<M>>()
        .map(|reconnect| reconnect.policy.clone())
        .unwrap_or(ReconnectPolicy::Never);
    let token = world
        .get_resource::<StdbIdentity<M>>()
        .map(|identity| identity.token.to_string());

    let mut connection = world.resource_mut::<StdbConnection<M>>();
    connection.awaiting_batch = false;
    let requested = connection.close_requested.load(Ordering::Relaxed) || matches!(reason, CloseReason::Requested);
    // A first connection that never came up is reported, not retried.
    let was_up = connection.established || connection.failed_attempts > 0;
    let retry_in = policy
        .delay(connection.failed_attempts)
        .filter(|_| was_up && !requested);
    let Some(retry_in) = retry_in else {
        return finish::<M>(world, reason);
    };

    if let Some(outbound) = connection.outbound.take() {
        outbound.close();
    }
    connection.inbound = None;
    connection.established = false;
    connection.failed_attempts += 1;
    connection.retry_at = Some(Instant::now() + retry_in);
    let attempt = connection.failed_attempts;
    // Come back as the same identity.
    if let (Some(token), Some(options)) = (token, connection.last_options.as_mut()) {
        options.token = Some(token);
    }

    subscription::reset_all::<M>(world);
    fail_pending::<M>(world);
    world.remove_resource::<StdbIdentity<M>>();
    world
        .resource_mut::<NextState<StdbState<M>>>()
        .set(StdbState::<M>::reconnecting());
    world.trigger(ConnectionLost::<M>::new(Arc::new(reason), attempt, retry_in));
}

/// The connection is over: empty the tables and report it.
fn finish<M: Module>(world: &mut World, reason: CloseReason) {
    let mut connection = world.resource_mut::<StdbConnection<M>>();
    // A first attempt that never came up left nothing to empty, and handlers in
    // `StdbTransaction` should not run for a connection that never had an identity.
    let was_up = connection.established || connection.failed_attempts > 0;
    if let Some(outbound) = connection.outbound.take() {
        outbound.close();
    }
    connection.inbound = None;
    connection.established = false;
    connection.failed_attempts = 0;
    connection.retry_at = None;
    connection.awaiting_batch = false;

    if was_up {
        clear_tables::<M>(world);
    }
    subscription::reset_all::<M>(world);
    fail_pending::<M>(world);
    world.remove_resource::<StdbIdentity<M>>();
    world
        .resource_mut::<NextState<StdbState<M>>>()
        .set(StdbState::<M>::disconnected());
    world.trigger(Disconnected::<M>::new(Arc::new(reason)));
}

/// Queued by [`StdbCommandsExt::connect`](crate::StdbCommandsExt::connect).
pub(crate) fn connect_command<M: Module>(options: ConnectOptions) -> impl Command {
    move |world: &mut World| {
        world.resource_scope(|world, mut connection: Mut<StdbConnection<M>>| {
            connection.connect(options, &mut world.resource_mut::<NextState<StdbState<M>>>());
        });
    }
}
