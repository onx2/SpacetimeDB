//! The Bevy-independent client core: websocket transport, protocol framing, row parsing, and
//! the refcounting that works out what a server message changes on net. Rows themselves are
//! stored by whoever runs this module: a [`RowSet`] names each resident row with a [`Ticket`]
//! and reports changes by it.
//!
//! Nothing here may name a Bevy crate. That used to be enforced by the compiler, when this was
//! the separate `spacetimedb_bevy_core` crate; inside one crate it is a review rule instead.
//!
//! Generated bindings reach this module as `spacetimedb_bevy::__codegen::core`, which is why
//! every name it re-exports is part of the compatibility surface even though the module itself
//! is private.

mod connection;
mod diff;
mod module;
mod row_set;
mod threads;

pub use connection::{
    parse_payload, ClientMessage, CloseReason, Compression, ConnectError, ConnectOptions, Inbound, Protocol,
    ProtocolError, QuerySetId, Raw, ReducerOutcome, Socket,
};
pub use diff::{RowUpdate, TableDiff};
pub use module::{
    Module, NoPk, ParseError, ParsedDelete, ParsedRow, Procedure, ProcedureVisitor, RawTableRows, Reducer,
    ReducerVisitor, Row, Table, TableKind, TableUpdate, TableVisitor, UniqueColumn, UniqueColumnVisitor, UpdateVisitor,
};
pub use row_set::{Applied, RowSet, RowSetError, Ticket};
pub use threads::{lend_threads, Threads};
