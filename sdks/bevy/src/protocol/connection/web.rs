//! The browser's socket: a `WebSocket`, whose callbacks feed a queue that [`Socket::run`] reads.
//!
//! The browser does the TCP, the TLS and the pings. It does not allow request headers, so a
//! token is traded for one that can ride in the URL; see [`ConnectOptions::token`].

use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_channel::{Receiver, Sender};
use bytes::Bytes;
use futures_util::future::{select, Either};
use js_sys::{Array, Reflect, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    BinaryType, CloseEvent, Headers, MessageEvent, Request, RequestInit, Response, WebSocket, Window, WorkerGlobalScope,
};

use super::{
    encode_payload, protocol_named, subscribe_uri, ClientMessage, CloseReason, ConnectError, ConnectOptions, Protocol,
    Raw, MAX_PAYLOADS_PER_BATCH, OFFERED_PROTOCOLS,
};

/// What went wrong with a websocket, as far as the browser tells.
#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct SocketError(String);

/// What the browser's callbacks report. `Closed` is always the last.
enum Event {
    Open,
    Payload(Bytes),
    Closed { code: u16, reason: String },
}

/// An open websocket, not yet running.
pub struct Socket {
    socket: WebSocket,
    protocol: Protocol,
    events: Receiver<Event>,
    // The browser calls into these for as long as the socket lives.
    _on_open: Closure<dyn FnMut()>,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_close: Closure<dyn FnMut(CloseEvent)>,
}

impl Socket {
    /// Opens the websocket and negotiates the protocol version, within
    /// [`ConnectOptions::connect_timeout`].
    pub async fn connect(options: &ConnectOptions) -> Result<Self, ConnectError> {
        let timeout = options.connect_timeout;
        // Dropping a socket that is still opening closes it.
        match select(pin!(Self::open(options)), pin!(sleep(timeout))).await {
            Either::Left((opened, _)) => opened,
            Either::Right(_) => Err(ConnectError::TimedOut(timeout)),
        }
    }

    async fn open(options: &ConnectOptions) -> Result<Self, ConnectError> {
        let url_token = match &options.token {
            Some(token) => Some(url_token(options, token).await?),
            None => None,
        };
        let uri = subscribe_uri(options, url_token.as_deref())?;

        let protocols: Array = OFFERED_PROTOCOLS.iter().copied().map(JsValue::from).collect();
        let socket = WebSocket::new_with_str_sequence(&uri.to_string(), &protocols)
            .map_err(|error| ConnectError::Handshake(SocketError(describe(&error))))?;
        socket.set_binary_type(BinaryType::Arraybuffer);

        let (tx, events) = async_channel::unbounded();
        let on_open = Closure::<dyn FnMut()>::new({
            let tx = tx.clone();
            move || drop(tx.try_send(Event::Open))
        });
        let on_message = Closure::<dyn FnMut(MessageEvent)>::new({
            let tx = tx.clone();
            move |message: MessageEvent| {
                // Text frames are not part of the protocol.
                if let Ok(buffer) = message.data().dyn_into::<js_sys::ArrayBuffer>() {
                    let payload = Uint8Array::new(&buffer).to_vec();
                    let _ = tx.try_send(Event::Payload(payload.into()));
                }
            }
        });
        // An error is always followed by a close, and says nothing that the close does not.
        let on_close = Closure::<dyn FnMut(CloseEvent)>::new(move |close: CloseEvent| {
            let _ = tx.try_send(Event::Closed {
                code: close.code(),
                reason: close.reason(),
            });
        });
        socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        let mut this = Self {
            socket,
            protocol: Protocol::V2,
            events,
            _on_open: on_open,
            _on_message: on_message,
            _on_close: on_close,
        };
        match this.events.recv().await {
            Ok(Event::Open) => {}
            Ok(Event::Closed { code, reason }) => {
                return Err(ConnectError::Handshake(closed(code, &reason)));
            }
            Ok(Event::Payload(_)) | Err(_) => unreachable!("a websocket opens or closes first"),
        }
        this.protocol = protocol_named(Some(this.socket.protocol().as_bytes()));
        Ok(this)
    }

    /// Returns which framing the server chose in the handshake.
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// Moves messages until the connection ends. Close `outbound` to disconnect.
    ///
    /// Always ends by sending [`Raw::Closed`] to `inbound`.
    pub async fn run(self, outbound: Receiver<ClientMessage>, inbound: Sender<Raw>, sent: Arc<AtomicU64>) {
        let reason = self.run_until_closed(&outbound, &inbound, &sent).await;
        let _ = inbound.send(Raw::Closed(reason)).await;
    }

    async fn run_until_closed(
        &self,
        outbound: &Receiver<ClientMessage>,
        inbound: &Sender<Raw>,
        sent: &AtomicU64,
    ) -> CloseReason {
        let mut payload = Vec::new();
        let mut carried: Option<ClientMessage> = None;
        loop {
            let first = match carried.take() {
                Some(message) => message,
                None => match select(pin!(self.events.recv()), pin!(outbound.recv())).await {
                    Either::Left((Ok(Event::Payload(payload)), _)) => {
                        // Along with whatever else the browser has delivered meanwhile.
                        let mut payloads = vec![payload];
                        let mut ended = None;
                        while payloads.len() < MAX_PAYLOADS_PER_BATCH {
                            match self.events.try_recv() {
                                Ok(Event::Payload(payload)) => payloads.push(payload),
                                Ok(Event::Open) => {}
                                Ok(Event::Closed { code, reason }) => {
                                    ended = Some((code, reason));
                                    break;
                                }
                                Err(_) => break,
                            }
                        }
                        let sent = inbound.send(Raw::Payloads(payloads)).await;
                        if let Some((code, reason)) = ended {
                            return match code {
                                1000 | 1001 => CloseReason::ClosedByServer,
                                code => CloseReason::Socket(closed(code, &reason)),
                            };
                        }
                        if sent.is_err() {
                            return CloseReason::Requested;
                        }
                        continue;
                    }
                    Either::Left((Ok(Event::Open), _)) => continue,
                    // 1000 is a normal closure, 1001 a server going away.
                    Either::Left((Ok(Event::Closed { code: 1000 | 1001, .. }), _)) => {
                        return CloseReason::ClosedByServer;
                    }
                    Either::Left((Ok(Event::Closed { code, reason }), _)) => {
                        return CloseReason::Socket(closed(code, &reason));
                    }
                    Either::Left((Err(_), _)) => unreachable!("`self` holds the senders"),
                    Either::Right((Ok(message), _)) => message,
                    Either::Right((Err(_), _)) => return CloseReason::Requested,
                },
            };
            encode_payload(&mut payload, first, self.protocol, outbound, &mut carried);
            sent.fetch_add(payload.len() as u64, Ordering::Relaxed);
            if let Err(error) = self.socket.send_with_u8_array(&payload) {
                return CloseReason::Socket(SocketError(describe(&error)));
            }
        }
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // The callbacks are about to be freed; the browser must not call them again.
        self.socket.set_onopen(None);
        self.socket.set_onmessage(None);
        self.socket.set_onclose(None);
        // Does nothing to a socket that is already closed.
        let _ = self.socket.close();
    }
}

fn closed(code: u16, reason: &str) -> SocketError {
    SocketError(match (code, reason) {
        // All a browser says about a connection that failed or dropped, by design.
        (1006, _) => "the connection failed or was lost; the browser's console may say why".into(),
        (code, "") => format!("closed with code {code}"),
        (code, reason) => format!("closed with code {code}: {reason}"),
    })
}

/// Waits by the browser's clock. Never done where there is no `setTimeout` to ask.
async fn sleep(duration: Duration) {
    let millis = i32::try_from(duration.as_millis()).unwrap_or(i32::MAX);
    let mut ask = |done: js_sys::Function, _: js_sys::Function| {
        // A window, or a worker.
        let _ = match js_sys::global().dyn_into::<Window>() {
            Ok(window) => window.set_timeout_with_callback_and_timeout_and_arguments_0(&done, millis),
            Err(global) => global
                .unchecked_into::<WorkerGlobalScope>()
                .set_timeout_with_callback_and_timeout_and_arguments_0(&done, millis),
        };
    };
    let _ = JsFuture::from(js_sys::Promise::new(&mut ask)).await;
}

fn describe(error: &JsValue) -> String {
    match error.dyn_ref::<js_sys::Error>() {
        Some(error) => error.message().into(),
        None => error.as_string().unwrap_or_else(|| format!("{error:?}")),
    }
}

/// Trades `token` for a short-lived one that the server accepts as a query parameter.
async fn url_token(options: &ConnectOptions, token: &str) -> Result<String, ConnectError> {
    let failed = |error: JsValue| ConnectError::TokenExchange(describe(&error));

    let ws_uri = subscribe_uri(options, None)?.to_string();
    let prefix = ws_uri
        .split_once("/v1/database/")
        .expect("written by `subscribe_uri`")
        .0;
    // `ws` to `http`, `wss` to `https`.
    let url = format!("http{}/v1/identity/websocket-token", &prefix[2..]);

    let headers = Headers::new().map_err(failed)?;
    headers
        .set("Authorization", &format!("Bearer {token}"))
        .map_err(failed)?;
    let init = RequestInit::new();
    init.set_method("POST");
    init.set_headers(&headers);
    let request = Request::new_with_str_and_init(&url, &init).map_err(failed)?;

    // A window, or a worker.
    let global = js_sys::global();
    let fetching = match global.dyn_into::<Window>() {
        Ok(window) => window.fetch_with_request(&request),
        Err(global) => global
            .unchecked_into::<WorkerGlobalScope>()
            .fetch_with_request(&request),
    };
    let response: Response = JsFuture::from(fetching).await.map_err(failed)?.unchecked_into();
    if !response.ok() {
        return Err(ConnectError::TokenExchange(format!(
            "the server answered {} {}",
            response.status(),
            response.status_text()
        )));
    }
    let body = JsFuture::from(response.json().map_err(failed)?).await.map_err(failed)?;
    Reflect::get(&body, &"token".into())
        .ok()
        .and_then(|token| token.as_string())
        .ok_or_else(|| ConnectError::TokenExchange("the answer holds no `token`".into()))
}
