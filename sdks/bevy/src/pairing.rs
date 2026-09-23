//! Working out what a server message changes, away from the main thread.
//!
//! A message says what each subscription gained and lost. What the tables gain and lose is less:
//! rows that several subscriptions cover are counted, and a delete and an insert of one key are
//! an update. That is [`RowSet`]'s work, a few hash lookups per row in a map that outgrows the
//! processor's cache with the table, and none of it needs the `World`. So it is done where the
//! message is parsed, and the main thread is handed the net change with every row already named.
//!
//! The name is a [`Ticket`]: a small number, given to a row when it becomes resident and free
//! for reuse once it has left. The storage on the main thread keeps its rows by ticket.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::protocol::{
    parse_payload, Inbound, Module, Raw, ReducerOutcome, RowSet, RowSetError, Table, TableDiff, TableKind, TableUpdate,
    UpdateVisitor,
};
use async_channel::{Receiver, Sender};
use bevy_ecs::change_detection::Tick;
use bevy_ecs::world::World;

pub(crate) use crate::protocol::Ticket;

/// Which rows of module `M` are resident, and under which tickets.
pub(crate) struct PairingState<M> {
    tables: HashMap<TypeId, Box<dyn Any + Send>>,
    module: PhantomData<fn() -> M>,
}

impl<M> Default for PairingState<M> {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            module: PhantomData,
        }
    }
}

/// Shared by the main thread and the task that parses, which holds the lock while it works a
/// message out. The main thread takes it only to apply an update it was handed directly, and
/// starts a new one rather than wait when the tables are emptied.
pub(crate) type SharedPairing<M> = Arc<Mutex<PairingState<M>>>;

/// The net change one message makes to one table, ready for the table's storage.
pub(crate) struct Paired<T: Table> {
    pub diff: TableDiff<T, Ticket>,
    /// The ticket of each row of `diff.inserts`.
    pub tickets: Vec<Ticket>,
    pub events: Vec<T::Row>,
    /// From a reconnect's snapshot: every row that stayed is among the updates, and the storage
    /// is to leave out those whose value is what it has.
    pub reconciled: bool,
}

/// A [`Paired`] of some table.
pub(crate) trait PairedTable: Send {
    fn apply(self: Box<Self>, world: &mut World, seq: u64, tick: Tick);
}

impl<T: Table> PairedTable for Paired<T> {
    fn apply(self: Box<Self>, world: &mut World, seq: u64, tick: Tick) {
        crate::apply::apply_table::<T>(world, seq, tick, *self);
    }
}

/// One transaction's tables, each already paired against what is resident, waiting for the
/// main thread to apply them.
pub(crate) type PairedUpdate = Vec<Box<dyn PairedTable>>;

struct Pairer<'a, M> {
    state: &'a mut PairingState<M>,
    reconcile: bool,
    out: PairedUpdate,
    result: Result<(), RowSetError>,
}

impl<M: Module> UpdateVisitor<M> for Pairer<'_, M> {
    fn table<T: Table<Module = M>>(&mut self, mut update: TableUpdate<T::Row>) {
        if self.result.is_err() || (update.is_empty() && !self.reconcile) {
            return;
        }
        let events = std::mem::take(&mut update.events);
        let mut paired = Paired::<T> {
            diff: TableDiff::empty(),
            tickets: Vec::new(),
            events,
            reconciled: self.reconcile,
        };
        if T::KIND == TableKind::Persistent {
            let resident = self
                .state
                .tables
                .entry(TypeId::of::<T>())
                .or_insert_with(|| Box::new(RowSet::<T>::default()))
                .downcast_mut::<RowSet<T>>()
                .expect("keyed by the table's type");
            let applied = if self.reconcile {
                resident.reconcile(update)
            } else {
                match resident.apply(&mut update) {
                    Ok(applied) => applied,
                    Err(error) => {
                        self.result = Err(error);
                        return;
                    }
                }
            };
            if let Some(applied) = applied {
                paired.diff = applied.diff;
                paired.tickets = applied.tickets;
            }
        }
        if !paired.diff.is_empty() || !paired.events.is_empty() {
            self.out.push(Box::new(paired));
        }
    }
}

/// What `update` changes, given what is resident.
pub(crate) fn pair<M: Module>(
    state: &mut PairingState<M>,
    update: M::Update,
    reconcile: bool,
) -> Result<PairedUpdate, RowSetError> {
    let mut pairer = Pairer {
        state,
        reconcile,
        out: Vec::new(),
        result: Ok(()),
    };
    M::visit_update(update, &mut pairer);
    pairer.result.map(|()| pairer.out)
}

/// What the parsing task hands the main thread.
pub(crate) enum Arrived<M: Module> {
    /// A message, with the rows it carried taken out of it and worked out.
    Message(Inbound<M>, PairedUpdate),
    /// The server deleted a row that is not resident, or one whose key cannot be read. What is
    /// resident is no longer known. The error names the table, and becomes the reason the
    /// connection is closed with.
    Unsound(RowSetError),
}

/// Parse and pair payloads until the connection ends.
pub(crate) async fn parse_and_pair_loop<M: Module>(
    raw: Receiver<Raw>,
    inbound: Sender<Arrived<M>>,
    pairing: SharedPairing<M>,
    bytes_in: Arc<AtomicU64>,
) {
    while let Ok(item) = raw.recv().await {
        let payloads = match item {
            Raw::Payloads(payloads) => payloads,
            Raw::Closed(reason) => {
                let _ = inbound
                    .send(Arrived::Message(Inbound::Closed(reason), Vec::new()))
                    .await;
                return;
            }
        };
        for payload in payloads {
            // One add per payload, not per row: what it costs is lost in parsing the payload.
            bytes_in.fetch_add(payload.len() as u64, Ordering::Relaxed);
            let messages = match parse_payload::<M>(&payload) {
                Ok(messages) => messages,
                Err(error) => {
                    let closed = Inbound::Closed(error.into());
                    let _ = inbound.send(Arrived::Message(closed, Vec::new())).await;
                    return;
                }
            };
            for mut message in messages {
                let (rows, reconcile) = match &mut message {
                    Inbound::SubscribeApplied { rows, .. }
                    | Inbound::UnsubscribeApplied { rows, .. }
                    | Inbound::Transaction(rows)
                    | Inbound::ReducerResult {
                        outcome: ReducerOutcome::Committed(rows),
                        ..
                    } => (Some(std::mem::take(rows)), false),
                    Inbound::SubscribeBatchApplied { snapshot, .. } => (Some(std::mem::take(snapshot)), true),
                    _ => (None, false),
                };
                let paired = match rows {
                    None => Ok(Vec::new()),
                    Some(rows) => {
                        let mut state = pairing.lock().unwrap_or_else(|error| error.into_inner());
                        pair::<M>(&mut state, rows, reconcile)
                    }
                };
                let arrived = match paired {
                    Ok(paired) => Arrived::Message(message, paired),
                    Err(error) => Arrived::Unsound(error),
                };
                let unsound = matches!(arrived, Arrived::Unsound(_));
                if inbound.send(arrived).await.is_err() || unsound {
                    return;
                }
            }
        }
    }
}
