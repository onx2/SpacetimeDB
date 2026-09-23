//! The websocket connection to a database.
//!
//! [`Socket::run`] moves bytes, and [`parse_payload`] turns them into typed [`Inbound`]
//! messages. The two are meant to run as separate tasks, so that a large payload being parsed
//! does not stall the socket; order is preserved end to end: one socket, one queue between the
//! tasks, one queue out.
//!
//! [`Socket`] is the platform's: TCP, TLS and tungstenite natively, the browser's `WebSocket` on
//! `wasm32`. Everything else here is shared.

use std::io::Read as _;
use std::time::Duration;

use async_channel::Receiver;
use bytes::Bytes;
use http::Uri;
use spacetimedb_client_api_messages::websocket::{common as ws_common, v2 as ws, v3 as ws_v3};
use spacetimedb_lib::{bsatn, ConnectionId, Identity, Timestamp};

use crate::protocol::module::{parse_query_rows, parse_transaction_update, Module, ParseError, Procedure, Reducer};
use crate::protocol::row_set::RowSetError;

pub use ws_common::{Compression, QuerySetId};

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::{Socket, SocketError};

#[cfg(target_arch = "wasm32")]
mod web;
#[cfg(target_arch = "wasm32")]
pub use web::{Socket, SocketError};

/// A message to the server. Opaque: build one with its constructors.
pub struct ClientMessage(ws::ClientMessage);

impl ClientMessage {
    /// Registers several subscriptions in one step. The server evaluates them all at one point in
    /// time, sends no transaction in between, and answers with one
    /// [`Inbound::SubscribeBatchApplied`]: a consistent picture of everything subscribed to,
    /// which is what a reconnecting client needs to bring its tables up to date without emptying
    /// them.
    pub fn subscribe_batch(request_id: u32, sets: impl IntoIterator<Item = (QuerySetId, Vec<Box<str>>)>) -> Self {
        Self::released(ws::ClientMessage::SubscribeBatch(ws::SubscribeBatch {
            request_id,
            sets: sets
                .into_iter()
                .map(|(query_set_id, queries)| ws::SubscribeSet {
                    query_set_id,
                    query_strings: queries.into(),
                })
                .collect(),
        }))
    }

    fn released(message: ws::ClientMessage) -> Self {
        Self(message)
    }

    fn encode(&self, out: &mut Vec<u8>) {
        bsatn::to_writer(out, &self.0).expect("client messages always encode");
    }

    /// Registers one subscription. The server answers with its initial rows, and with every
    /// later transaction that changes them.
    pub fn subscribe<Q: Into<Box<str>>>(
        request_id: u32,
        query_set_id: QuerySetId,
        queries: impl IntoIterator<Item = Q>,
    ) -> Self {
        Self::released(ws::ClientMessage::Subscribe(ws::Subscribe {
            request_id,
            query_set_id,
            query_strings: queries.into_iter().map(Into::into).collect(),
        }))
    }

    /// Ends one subscription. The server answers with the rows that only it covered.
    pub fn unsubscribe(request_id: u32, query_set_id: QuerySetId) -> Self {
        Self::released(ws::ClientMessage::Unsubscribe(ws::Unsubscribe {
            request_id,
            query_set_id,
            flags: ws::UnsubscribeFlags::SendDroppedRows,
        }))
    }

    /// Calls a procedure. Its return value comes back as [`Inbound::ProcedureResult`].
    pub fn call_procedure<P: Procedure>(request_id: u32, args: &P) -> Self {
        Self::released(ws::ClientMessage::CallProcedure(ws::CallProcedure {
            request_id,
            flags: ws::CallProcedureFlags::Default,
            procedure: P::NAME.into(),
            args: bsatn::to_vec(args).expect("procedure arguments always encode").into(),
        }))
    }

    /// Calls a reducer. The rows it changed arrive before its [`Inbound::ReducerResult`].
    pub fn call_reducer<R: Reducer>(request_id: u32, args: &R) -> Self {
        Self::released(ws::ClientMessage::CallReducer(ws::CallReducer {
            request_id,
            flags: ws::CallReducerFlags::Default,
            reducer: R::NAME.into(),
            args: bsatn::to_vec(args).expect("reducer arguments always encode").into(),
        }))
    }
}

/// Where and how to connect.
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// The host, as `ws://`, `wss://`, `http://` or `https://`, with an optional path prefix.
    pub uri: String,
    /// The database's name or identity.
    pub database: String,
    /// A token from an earlier connection, to connect as the same identity.
    ///
    /// A browser cannot put it in a header of the websocket request, so there it is first traded
    /// over HTTP for a short-lived token that rides in the URL, as the official SDKs do.
    pub token: Option<String>,
    /// How the server should compress large messages.
    pub compression: Compression,
    /// Whether the server holds each update back until its transaction is on disk, so that the
    /// client never sees something a crash could undo. `None` leaves it to the server, which
    /// since protocol v2 means yes. `Some(false)` trades that for latency: by the time it takes
    /// the server's disk to sync, which on a local `NVMe` drive is too little to measure.
    pub confirmed_reads: Option<bool>,
    /// How long opening the connection may take, from resolving the host to the end of the
    /// websocket handshake. After that the attempt ends as [`ConnectError::TimedOut`]. A host
    /// that swallows packets would otherwise hold it for as long as the operating system lets
    /// it, which is minutes, and one that accepts and then says nothing, for good.
    pub connect_timeout: Duration,
    /// After this long without hearing anything, ping the server; after as long again without an
    /// answer, give the connection up as [`CloseReason::TimedOut`]. A dead network path is
    /// otherwise invisible to a client that is only listening.
    ///
    /// Unused in a browser, whose websocket cannot ping; the browser itself reports a dead socket.
    pub idle_timeout: Duration,
    /// Names this client across its reconnects: 128 bits, chosen at random by whoever connects
    /// and kept for every attempt. A server newer than 2.10.1 uses it to notice that a new
    /// connection replaces one it still believes open, closes the old one, and lets a retry in
    /// once the old one's `client_disconnected` has run. Older servers ignore it.
    pub session_id: Option<u128>,
    /// DER-encoded certificates to trust for `wss://` on top of the bundled web roots, for a
    /// server behind a private certificate authority. Unused in a browser, which does its own TLS.
    pub root_certificates: Vec<Vec<u8>>,
}

impl ConnectOptions {
    /// Returns a fresh value for [`Self::session_id`].
    pub fn random_session_id() -> u128 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            // Randomly keyed by the standard library, once per hasher.
            use std::hash::{BuildHasher, RandomState};
            let half = || RandomState::new().hash_one(0u8) as u128;
            half() << 64 | half()
        }
        #[cfg(target_arch = "wasm32")]
        {
            let half = || (js_sys::Math::random() * 2f64.powi(53)) as u128;
            half() << 64 | half() << 11 | half() & 0x7FF
        }
    }

    /// Creates options with the defaults most clients want: no token, no session id, and a
    /// 30-second idle timeout. Set the rest of the fields directly.
    pub fn new(uri: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            database: database.into(),
            token: None,
            compression: Compression::default(),
            confirmed_reads: None,
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            session_id: None,
            root_certificates: Vec::new(),
        }
    }
}

/// How messages are framed, as negotiated with the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    /// One message per websocket payload.
    V2,
    /// One or more messages per websocket payload.
    V3,
}

/// Why a connection could not be made. Every one of these ends the attempt.
#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    /// The URI in [`ConnectOptions::uri`] could not be parsed, or names no host.
    #[error("invalid URI `{uri}`: {reason}")]
    Uri {
        /// The URI as given.
        uri: String,
        /// What is wrong with it.
        reason: String,
    },
    /// The TCP connection to the host could not be opened.
    #[error("could not reach `{host}`")]
    Tcp {
        /// Host and port that were tried.
        host: String,
        /// What the operating system said.
        #[source]
        source: std::io::Error,
    },
    /// The TCP connection opened but the TLS handshake on top of it failed.
    #[error("TLS handshake with `{host}` failed")]
    Tls {
        /// Host and port that were tried.
        host: String,
        /// What the operating system said.
        #[source]
        source: std::io::Error,
    },
    /// The URI asks for `wss://` or `https://` and this build cannot do TLS.
    #[error("`{0}` needs TLS, and this build has the `tls` feature disabled")]
    TlsDisabled(String),
    /// The socket opened but the server refused the websocket upgrade.
    #[error("websocket handshake failed")]
    Handshake(#[source] SocketError),
    /// See [`ConnectOptions::connect_timeout`].
    #[error("the connection was not open after {0:?}")]
    TimedOut(Duration),
    /// Only in a browser; see [`ConnectOptions::token`].
    #[error("could not trade the token for a websocket token: {0}")]
    TokenExchange(String),
}

/// Why a connection ended.
#[derive(thiserror::Error, Debug)]
pub enum CloseReason {
    /// The client asked to disconnect. The only reason that is not a failure.
    #[error("closed at the client's request")]
    Requested,
    /// The server sent a close frame.
    #[error("closed by the server")]
    ClosedByServer,
    /// Nothing arrived for two idle timeouts, the second after a ping went unanswered.
    /// See [`ConnectOptions::idle_timeout`].
    #[error("the server stopped answering")]
    TimedOut,
    /// The connection was never established.
    #[error("could not connect")]
    Connect(#[from] ConnectError),
    /// The websocket itself failed once the connection was up.
    #[error("websocket error")]
    Socket(#[source] SocketError),
    /// A message arrived that could not be read; see [`ProtocolError`].
    #[error("the server sent something these bindings cannot read")]
    Protocol(#[from] ProtocolError),
    /// The client ended the connection itself: its tables can no longer be trusted, and a new
    /// connection brings them back in line with the server.
    #[error("the server's changes do not fit the rows this client holds")]
    Inconsistent(#[from] RowSetError),
}

/// Something the server sent that this client cannot read. Always fatal to the connection:
/// once a payload cannot be decoded, where the next message starts is no longer known.
#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    /// A websocket payload with no bytes in it, which carries not even a compression tag.
    #[error("empty payload")]
    EmptyPayload,
    /// The payload's leading tag names a compression scheme this client does not implement.
    #[error("unknown compression scheme {0}")]
    UnknownCompression(u8),
    /// The payload named a compression scheme this client has, and then did not decompress.
    #[error("failed to decompress a payload")]
    Decompress(#[source] std::io::Error),
    /// The payload decompressed but is not a server message.
    #[error("failed to decode a server message")]
    Decode(#[source] bsatn::DecodeError),
    /// The message decoded but its rows did not; see [`ParseError`].
    #[error(transparent)]
    Rows(#[from] ParseError),
    /// A message that only a client using features this one does not would receive.
    #[error("the server sent a `{0}`, which this client never asks for")]
    Unexpected(&'static str),
}

/// Bytes off the socket, or the end of the connection. The last item is always `Closed`.
#[derive(Debug)]
pub enum Raw {
    /// Websocket payloads in the order they arrived: as many as had arrived by the time the
    /// first was handed over, since a hand-over costs more than a small message does.
    Payloads(Vec<Bytes>),
    /// The connection has ended, and nothing follows.
    Closed(CloseReason),
}

/// Offered to the server in the handshake, newest first; it picks one. See [`Protocol`].
pub(crate) const OFFERED_PROTOCOLS: [&str; 2] = [ws_v3::BIN_PROTOCOL, ws::BIN_PROTOCOL];

/// So that one hand-over to the parsing task stays bounded however fast messages arrive.
const MAX_PAYLOADS_PER_BATCH: usize = 256;

/// Keep outbound v3 payloads bounded so one burst of calls does not monopolize the socket.
const MAX_OUTBOUND_PAYLOAD: usize = 256 * 1024;

/// Encodes `first` into `payload`, and under v3 everything else already queued with it, up to the
/// size cap. A message taken off the queue that did not fit is left in `carried` for next time.
fn encode_payload(
    payload: &mut Vec<u8>,
    first: ClientMessage,
    protocol: Protocol,
    outbound: &Receiver<ClientMessage>,
    carried: &mut Option<ClientMessage>,
) {
    payload.clear();
    first.encode(payload);
    if protocol == Protocol::V3 {
        while let Ok(next) = outbound.try_recv() {
            let len = payload.len();
            next.encode(payload);
            if payload.len() > MAX_OUTBOUND_PAYLOAD {
                payload.truncate(len);
                *carried = Some(next);
                break;
            }
        }
    }
}

fn protocol_named(chosen: Option<&[u8]>) -> Protocol {
    match chosen {
        Some(chosen) if chosen == ws_v3::BIN_PROTOCOL.as_bytes() => Protocol::V3,
        // A server that names no protocol predates v3.
        _ => Protocol::V2,
    }
}

/// Returns the websocket URI to subscribe at. `url_token` is for a browser; see
/// [`ConnectOptions::token`].
fn subscribe_uri(options: &ConnectOptions, url_token: Option<&str>) -> Result<Uri, ConnectError> {
    let invalid = |reason: String| ConnectError::Uri {
        uri: options.uri.clone(),
        reason,
    };
    let base: Uri = options.uri.parse().map_err(|error| invalid(format!("{error}")))?;
    let scheme = match base.scheme_str() {
        None | Some("ws") | Some("http") => "ws",
        Some("wss") | Some("https") => "wss",
        Some(other) => return Err(invalid(format!("unknown scheme `{other}`"))),
    };
    let authority = base.authority().ok_or_else(|| invalid("no host".into()))?;
    if base.query().is_some() {
        return Err(invalid("must not contain a query".into()));
    }

    let prefix = base.path().trim_end_matches('/');
    let compression = match options.compression {
        Compression::None => "None",
        Compression::Gzip => "Gzip",
        Compression::Brotli => "Brotli",
    };
    let mut path = format!(
        "{prefix}/v1/database/{}/subscribe?compression={compression}",
        options.database
    );
    if let Some(confirmed) = options.confirmed_reads {
        path.push_str(if confirmed {
            "&confirmed=true"
        } else {
            "&confirmed=false"
        });
    }
    if let Some(session_id) = options.session_id {
        path.push_str(&format!("&session_id={session_id:032x}"));
    }
    if let Some(token) = url_token {
        path.push_str("&token=");
        path.push_str(token);
    }

    Uri::builder()
        .scheme(scheme)
        .authority(authority.as_str())
        .path_and_query(path)
        .build()
        .map_err(|error| invalid(format!("{error}")))
}

/// The result of a reducer call, as seen by the caller.
#[derive(Debug)]
pub enum ReducerOutcome<U> {
    /// The reducer committed. Carries the caller's view of the rows it changed.
    Committed(U),
    /// The reducer returned an error and its transaction was rolled back.
    Failed(String),
    /// The host could not run the reducer.
    InternalError(String),
}

/// A server message with its rows parsed into the module's types.
#[derive(Debug)]
pub enum Inbound<M: Module> {
    /// The first message of every connection.
    Connected {
        /// Who the server decided this client is.
        identity: Identity,
        /// Names this connection, as distinct from other connections of the same identity.
        connection_id: ConnectionId,
        /// Pass to [`ConnectOptions::token`] next time to connect as the same identity.
        token: Box<str>,
    },
    /// A subscription is live; the rows are every row it covers as of one moment.
    SubscribeApplied {
        /// The set this answers, as [`ClientMessage::subscribe`] numbered it.
        query_set_id: QuerySetId,
        /// Every row the set covers, all as inserts.
        rows: M::Update,
    },
    /// A subscription has ended; the rows are those only it covered.
    UnsubscribeApplied {
        /// The set this answers, as [`ClientMessage::subscribe`] numbered it.
        query_set_id: QuerySetId,
        /// The rows only that set covered, all as deletes.
        rows: M::Update,
    },
    /// The server rejected a subscription, or dropped one that was live. Carries no rows.
    SubscriptionError {
        /// The set this answers, as [`ClientMessage::subscribe`] numbered it.
        query_set_id: QuerySetId,
        /// What the server says is wrong with it.
        error: Box<str>,
    },
    /// The answer to [`ClientMessage::subscribe_batch`].
    SubscribeBatchApplied {
        /// Every row of every set that applied, all as of one moment.
        snapshot: M::Update,
        /// Each set asked for, in the order asked, with the server's error if it did not apply.
        sets: Vec<(QuerySetId, Option<Box<str>>)>,
    },
    /// One committed transaction, as far as it touches what this client subscribed to.
    Transaction(M::Update),
    /// How a reducer this client called ended. Its rows, if it committed, are in the outcome.
    ReducerResult {
        /// The call this answers, as the client numbered it.
        request_id: u32,
        /// When the host ran the reducer.
        timestamp: Timestamp,
        /// Whether it committed, and its rows if it did.
        outcome: ReducerOutcome<M::Update>,
    },
    /// What a procedure this client called returned. Any rows it changed arrived earlier,
    /// as ordinary transactions.
    ProcedureResult {
        /// The call this answers, as the client numbered it.
        request_id: u32,
        /// When the host finished the procedure.
        timestamp: Timestamp,
        /// The BSATN-encoded return value, or the host's error.
        result: Result<Bytes, Box<str>>,
    },
    /// The last message of every connection.
    Closed(CloseReason),
}

/// Parses one websocket payload: a compression tag, then one message (v2) or several (v3).
pub fn parse_payload<M: Module>(payload: &[u8]) -> Result<Vec<Inbound<M>>, ProtocolError> {
    let (&tag, body) = payload.split_first().ok_or(ProtocolError::EmptyPayload)?;
    let decompressed;
    let mut body = match tag {
        ws_common::SERVER_MSG_COMPRESSION_TAG_NONE => body,
        ws_common::SERVER_MSG_COMPRESSION_TAG_BROTLI => {
            let mut out = Vec::new();
            brotli::BrotliDecompress(&mut &body[..], &mut out).map_err(ProtocolError::Decompress)?;
            decompressed = out;
            &decompressed[..]
        }
        ws_common::SERVER_MSG_COMPRESSION_TAG_GZIP => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(body)
                .read_to_end(&mut out)
                .map_err(ProtocolError::Decompress)?;
            decompressed = out;
            &decompressed[..]
        }
        other => return Err(ProtocolError::UnknownCompression(other)),
    };

    let mut messages = Vec::with_capacity(1);
    while !body.is_empty() {
        let message: ws::ServerMessage = bsatn::from_reader(&mut body).map_err(ProtocolError::Decode)?;
        messages.push(parse_message::<M>(message)?);
    }
    if messages.is_empty() {
        return Err(ProtocolError::EmptyPayload);
    }
    Ok(messages)
}

fn parse_message<M: Module>(message: ws::ServerMessage) -> Result<Inbound<M>, ProtocolError> {
    Ok(match message {
        ws::ServerMessage::InitialConnection(initial) => Inbound::Connected {
            identity: initial.identity,
            connection_id: initial.connection_id,
            token: initial.token,
        },
        ws::ServerMessage::SubscribeApplied(applied) => Inbound::SubscribeApplied {
            query_set_id: applied.query_set_id,
            rows: parse_query_rows::<M>(applied.rows, true)?,
        },
        ws::ServerMessage::UnsubscribeApplied(applied) => Inbound::UnsubscribeApplied {
            query_set_id: applied.query_set_id,
            rows: match applied.rows {
                Some(rows) => parse_query_rows::<M>(rows, false)?,
                None => M::Update::default(),
            },
        },
        ws::ServerMessage::SubscriptionError(error) => Inbound::SubscriptionError {
            query_set_id: error.query_set_id,
            error: error.error,
        },
        ws::ServerMessage::TransactionUpdate(update) => Inbound::Transaction(parse_transaction_update::<M>(update)?),
        ws::ServerMessage::ReducerResult(result) => Inbound::ReducerResult {
            request_id: result.request_id,
            timestamp: result.timestamp,
            outcome: match result.result {
                ws::ReducerOutcome::Ok(ok) => {
                    ReducerOutcome::Committed(parse_transaction_update::<M>(ok.transaction_update)?)
                }
                ws::ReducerOutcome::OkEmpty => ReducerOutcome::Committed(M::Update::default()),
                ws::ReducerOutcome::Err(error) => {
                    ReducerOutcome::Failed(bsatn::from_slice(&error).map_err(ProtocolError::Decode)?)
                }
                ws::ReducerOutcome::InternalError(error) => ReducerOutcome::InternalError(error.into()),
            },
        },
        ws::ServerMessage::ProcedureResult(result) => Inbound::ProcedureResult {
            request_id: result.request_id,
            timestamp: result.timestamp,
            result: match result.status {
                ws::ProcedureStatus::Returned(bytes) => Ok(bytes),
                ws::ProcedureStatus::InternalError(error) => Err(error),
            },
        },
        ws::ServerMessage::SubscribeBatchApplied(applied) => {
            let mut tables = Vec::new();
            let mut sets = Vec::with_capacity(applied.results.len());
            for result in applied.results {
                let error = match result.outcome {
                    ws::SubscribeSetOutcome::Applied(rows) => {
                        tables.extend(rows.tables);
                        None
                    }
                    ws::SubscribeSetOutcome::Error(error) => Some(error),
                };
                sets.push((result.query_set_id, error));
            }
            let rows = ws::QueryRows { tables: tables.into() };
            Inbound::SubscribeBatchApplied {
                snapshot: parse_query_rows::<M>(rows, true)?,
                sets,
            }
        }
        ws::ServerMessage::OneOffQueryResult(_) => {
            return Err(ProtocolError::Unexpected("OneOffQueryResult"));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uri(options: &ConnectOptions) -> String {
        subscribe_uri(options, None).unwrap().to_string()
    }

    #[test]
    fn subscribe_uri_carries_the_options() {
        let mut options = ConnectOptions::new("https://example.com/prefix/", "chat");
        assert_eq!(
            uri(&options),
            "wss://example.com/prefix/v1/database/chat/subscribe?compression=Brotli"
        );

        options.uri = "http://127.0.0.1:3000".into();
        options.compression = Compression::None;
        options.confirmed_reads = Some(true);
        assert_eq!(
            uri(&options),
            "ws://127.0.0.1:3000/v1/database/chat/subscribe?compression=None&confirmed=true"
        );

        options.confirmed_reads = Some(false);
        assert!(uri(&options).ends_with("&confirmed=false"));
        let with_token = subscribe_uri(&options, Some("abc")).unwrap().to_string();
        assert!(with_token.ends_with("&confirmed=false&token=abc"));
    }

    #[test]
    fn subscribe_uri_carries_the_session() {
        let mut options = ConnectOptions::new("http://127.0.0.1:3000", "chat");
        options.session_id = Some(0xAB);
        assert!(uri(&options).ends_with("&session_id=000000000000000000000000000000ab"));
        assert_ne!(ConnectOptions::random_session_id(), ConnectOptions::random_session_id());
    }

    #[test]
    fn subscribe_uri_refuses_what_it_cannot_use() {
        for bad in ["ftp://example.com", "http://example.com/?x=1", "/no-host"] {
            let options = ConnectOptions::new(bad, "chat");
            assert!(
                matches!(subscribe_uri(&options, None), Err(ConnectError::Uri { .. })),
                "{bad}"
            );
        }
    }
}
