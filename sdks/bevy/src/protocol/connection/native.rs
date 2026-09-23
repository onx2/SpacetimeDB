//! The native socket: TCP from `async-net`, TLS from `futures-rustls`, websockets from tungstenite.

use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_channel::{Receiver, Sender};
use async_io::Timer;
use async_net::TcpStream;
use async_tungstenite::tungstenite::client::IntoClientRequest;
use async_tungstenite::tungstenite::protocol::WebSocketConfig;
use async_tungstenite::tungstenite::{self, Message as Frame};
use async_tungstenite::{client_async_with_config, WebSocketStream};
use bytes::Bytes;
use futures_util::future::{select, Either};
use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::{FutureExt, StreamExt};
use http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use http::{HeaderValue, Uri};

use super::{
    encode_payload, protocol_named, subscribe_uri, ClientMessage, CloseReason, ConnectError, ConnectOptions, Protocol,
    Raw, MAX_PAYLOADS_PER_BATCH, OFFERED_PROTOCOLS,
};

/// What went wrong with a websocket.
pub type SocketError = Box<tungstenite::Error>;

/// An open websocket, not yet running.
pub struct Socket {
    stream: WebSocketStream<MaybeTls>,
    protocol: Protocol,
    idle_timeout: Duration,
}

impl Socket {
    /// Opens the websocket and negotiates the protocol version, within
    /// [`ConnectOptions::connect_timeout`].
    pub async fn connect(options: &ConnectOptions) -> Result<Self, ConnectError> {
        let timeout = options.connect_timeout;
        match select(pin!(Self::open(options)), Timer::after(timeout)).await {
            Either::Left((opened, _)) => opened,
            Either::Right(_) => Err(ConnectError::TimedOut(timeout)),
        }
    }

    async fn open(options: &ConnectOptions) -> Result<Self, ConnectError> {
        let uri = subscribe_uri(options, None)?;
        let host = host_of(&uri).to_owned();
        let port = uri
            .port_u16()
            .unwrap_or(if uri.scheme_str() == Some("wss") { 443 } else { 80 });

        let mut request = uri.to_string().into_client_request().map_err(handshake)?;
        let headers = request.headers_mut();
        headers.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::try_from(OFFERED_PROTOCOLS.join(", ")).expect("protocol names are ASCII"),
        );
        if let Some(token) = &options.token {
            let bearer = HeaderValue::try_from(format!("Bearer {token}")).map_err(|error| ConnectError::Uri {
                uri: options.uri.clone(),
                reason: format!("token is not a valid header value: {error}"),
            })?;
            headers.insert(AUTHORIZATION, bearer);
        }

        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|source| ConnectError::Tcp {
                host: host.clone(),
                source,
            })?;
        let transport = if uri.scheme_str() == Some("wss") {
            MaybeTls::secure(&host, &options.root_certificates, tcp).await?
        } else {
            MaybeTls::Plain(tcp)
        };
        // No size limits, as in the official SDK: a subscription's snapshot is one message,
        // however large the tables, and the defaults (16 MiB a frame) would end the connection.
        let config = WebSocketConfig::default().max_frame_size(None).max_message_size(None);
        let (stream, response) = client_async_with_config(request, transport, Some(config))
            .await
            .map_err(handshake)?;

        let chosen = response.headers().get(SEC_WEBSOCKET_PROTOCOL);
        let protocol = protocol_named(chosen.map(HeaderValue::as_bytes));
        Ok(Self {
            stream,
            protocol,
            idle_timeout: options.idle_timeout,
        })
    }

    /// Returns which framing the server chose in the handshake.
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// Moves messages until the connection ends. Close `outbound` to disconnect.
    ///
    /// Always ends by sending [`Raw::Closed`] to `inbound`.
    pub async fn run(self, outbound: Receiver<ClientMessage>, inbound: Sender<Raw>, sent: Arc<AtomicU64>) {
        let protocol = self.protocol;
        let (mut sink, mut stream) = self.stream.split();
        // Set by the reader on any frame, pongs included; cleared by the writer's idle check.
        let heard = AtomicBool::new(true);

        let read = async {
            // A frame taken off the socket while gathering a batch, that was not a payload.
            let mut carried = None;
            loop {
                let frame = match carried.take() {
                    Some(frame) => frame,
                    None => stream.next().await,
                };
                heard.store(true, Ordering::Relaxed);
                match frame {
                    Some(Ok(Frame::Binary(payload))) => {
                        // Handing over wakes the parsing task, which costs more than parsing a
                        // small message does. So everything that has already arrived goes over
                        // in one go: a burst of small transactions is one wake, not one each.
                        let mut payloads = vec![payload];
                        while payloads.len() < MAX_PAYLOADS_PER_BATCH {
                            match stream.next().now_or_never() {
                                Some(Some(Ok(Frame::Binary(payload)))) => payloads.push(payload),
                                Some(other) => {
                                    carried = Some(other);
                                    break;
                                }
                                None => break,
                            }
                        }
                        if inbound.send(Raw::Payloads(payloads)).await.is_err() {
                            return CloseReason::Requested;
                        }
                    }
                    // Tungstenite answers pings itself; text frames are not part of the protocol.
                    Some(Ok(Frame::Close(_))) | None => return CloseReason::ClosedByServer,
                    Some(Ok(_)) => {}
                    Some(Err(tungstenite::Error::ConnectionClosed)) => {
                        return CloseReason::ClosedByServer;
                    }
                    Some(Err(error)) => return CloseReason::Socket(Box::new(error)),
                }
            }
        };

        let write = async {
            let mut payload = Vec::new();
            let mut carried: Option<ClientMessage> = None;
            let mut idle_check = Timer::interval(self.idle_timeout);
            let mut awaiting_pong = false;
            loop {
                let first = match carried.take() {
                    Some(message) => message,
                    None => match select(pin!(outbound.recv()), idle_check.next()).await {
                        Either::Left((Ok(message), _)) => message,
                        Either::Left((Err(_), _)) => {
                            let _ = sink.send(Frame::Close(None)).await;
                            return CloseReason::Requested;
                        }
                        Either::Right(_) => {
                            if heard.swap(false, Ordering::Relaxed) {
                                awaiting_pong = false;
                            } else if awaiting_pong {
                                return CloseReason::TimedOut;
                            } else if let Err(error) = sink.send(Frame::Ping(Bytes::new())).await {
                                return CloseReason::Socket(Box::new(error));
                            } else {
                                awaiting_pong = true;
                            }
                            continue;
                        }
                    },
                };
                encode_payload(&mut payload, first, protocol, &outbound, &mut carried);
                // One add per payload, not per message: what it costs is lost in the write.
                sent.fetch_add(payload.len() as u64, Ordering::Relaxed);

                if let Err(error) = sink.send(Frame::Binary(Bytes::copy_from_slice(&payload))).await {
                    return CloseReason::Socket(Box::new(error));
                }
            }
        };

        let reason = match select(Box::pin(read), Box::pin(write)).await {
            Either::Left((reason, _)) | Either::Right((reason, _)) => reason,
        };
        let _ = inbound.send(Raw::Closed(reason)).await;
    }
}

/// A TCP stream, with or without TLS on top.
enum MaybeTls {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Secure(Box<futures_rustls::client::TlsStream<TcpStream>>),
}

impl MaybeTls {
    #[cfg(feature = "tls")]
    async fn secure(host: &str, extra_roots: &[Vec<u8>], tcp: TcpStream) -> Result<Self, ConnectError> {
        use futures_rustls::rustls::pki_types::{CertificateDer, ServerName};
        use futures_rustls::rustls::{ClientConfig, RootCertStore};
        use std::sync::Arc;

        let tls_error = |source| ConnectError::Tls {
            host: host.to_owned(),
            source,
        };
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        for der in extra_roots {
            roots
                .add(CertificateDer::from_slice(der))
                .map_err(|error| tls_error(std::io::Error::new(std::io::ErrorKind::InvalidInput, error)))?;
        }
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let name = ServerName::try_from(host.to_owned())
            .map_err(|error| tls_error(std::io::Error::new(std::io::ErrorKind::InvalidInput, error)))?;
        let stream = futures_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await
            .map_err(tls_error)?;
        Ok(Self::Secure(Box::new(stream)))
    }

    #[cfg(not(feature = "tls"))]
    async fn secure(host: &str, _: &[Vec<u8>], _: TcpStream) -> Result<Self, ConnectError> {
        Err(ConnectError::TlsDisabled(host.to_owned()))
    }
}

impl AsyncRead for MaybeTls {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Self::Secure(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Self::Secure(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Secure(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_close(cx),
            #[cfg(feature = "tls")]
            Self::Secure(stream) => Pin::new(stream).poll_close(cx),
        }
    }
}

fn handshake(error: tungstenite::Error) -> ConnectError {
    ConnectError::Handshake(Box::new(error))
}

/// Returns the host to resolve and to name in TLS. A URI holds an IPv6 address in brackets,
/// which neither of the two takes.
fn host_of(uri: &Uri) -> &str {
    let host = uri.host().expect("checked by `subscribe_uri`");
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    #[test]
    fn an_ipv6_host_loses_its_brackets() {
        let host = |uri: &str| host_of(&uri.parse().unwrap()).to_owned();
        assert_eq!(host("ws://[::1]:3000/v1/database/chat/subscribe"), "::1");
        assert_eq!(host("wss://[2001:db8::7]/v1"), "2001:db8::7");
        assert_eq!(host("ws://127.0.0.1:3000/v1"), "127.0.0.1");
        assert_eq!(host("wss://example.com/v1"), "example.com");
    }

    /// A host that accepts the TCP connection and never answers the handshake.
    #[test]
    fn a_silent_host_is_given_up_on() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut options = ConnectOptions::new(format!("ws://127.0.0.1:{port}"), "chat");
        options.connect_timeout = Duration::from_millis(100);

        let connected = futures_lite::future::block_on(Socket::connect(&options));
        assert!(
            matches!(connected, Err(ConnectError::TimedOut(after)) if after == options.connect_timeout),
            "{:?}",
            connected.err()
        );
    }
}
