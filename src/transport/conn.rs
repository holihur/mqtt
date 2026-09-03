//! Transport connections: unified wrapper over TCP / TLS / WebSocket streams
//! supporting concurrent read+write (like Go's `internal/transport`).

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{watch, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;

pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

type TcpStream = tokio::net::TcpStream;
type TlsStream = tokio_rustls::server::TlsStream<TcpStream>;

type WriteResult = Result<Result<(), std::io::Error>, ()>; // outer: timeout hit

struct WsReadState {
    stream: futures_util::stream::SplitStream<WebSocketStream<TcpStream>>,
    buf: Vec<u8>, // leftover bytes of a multi-frame message
}

struct WsWriteState {
    stream: futures_util::stream::SplitSink<WebSocketStream<TcpStream>, Message>,
}

enum ReaderHalf {
    Tcp(tokio::io::ReadHalf<TcpStream>),
    Tls(tokio::io::ReadHalf<TlsStream>),
    Ws(WsReadState),
}

enum WriterHalf {
    Tcp(tokio::io::WriteHalf<TcpStream>),
    Tls(tokio::io::WriteHalf<TlsStream>),
    Ws(WsWriteState),
}

struct ReaderState {
    half: ReaderHalf,
    buf: Vec<u8>,
}

/// One MQTT connection over TCP / TLS / WS. Shared via `Arc`.
pub struct Conn {
    reader: Mutex<ReaderState>,
    writer: Mutex<WriterHalf>,
    close_tx: watch::Sender<bool>,
    close_rx: watch::Receiver<bool>,
    version: AtomicU8,
    client_id: std::sync::Mutex<String>,
    remote_addr: String,
    closed: AtomicBool,
    max_packet_size: usize,
}

fn timeout_err() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, "write timeout")
}

impl Conn {
    pub fn new_tcp(stream: TcpStream, max_packet_size: usize) -> Arc<Self> {
        let remote = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
        let (r, w) = tokio::io::split(stream);
        Self::build(ReaderHalf::Tcp(r), WriterHalf::Tcp(w), remote, max_packet_size)
    }

    pub fn new_tls(stream: TlsStream, max_packet_size: usize) -> Arc<Self> {
        let remote = stream.get_ref().0.peer_addr().map(|a| a.to_string()).unwrap_or_default();
        let (r, w) = tokio::io::split(stream);
        Self::build(ReaderHalf::Tls(r), WriterHalf::Tls(w), remote, max_packet_size)
    }

    pub fn new_ws(ws: WebSocketStream<TcpStream>, max_packet_size: usize) -> Arc<Self> {
        let (write, read) = ws.split();
        Self::build(
            ReaderHalf::Ws(WsReadState { stream: read, buf: Vec::new() }),
            WriterHalf::Ws(WsWriteState { stream: write }),
            "ws-remote".to_string(),
            max_packet_size,
        )
    }

    fn build(reader: ReaderHalf, writer: WriterHalf, remote: String, max_packet_size: usize) -> Arc<Self> {
        let (close_tx, close_rx) = watch::channel(false);
        Arc::new(Self {
            reader: Mutex::new(ReaderState { half: reader, buf: Vec::with_capacity(4096) }),
            writer: Mutex::new(writer),
            close_tx,
            close_rx,
            version: AtomicU8::new(0),
            client_id: std::sync::Mutex::new(String::new()),
            remote_addr: remote,
            closed: AtomicBool::new(false),
            max_packet_size,
        })
    }

    pub fn set_version(&self, v: u8) {
        self.version.store(v, Ordering::Relaxed);
    }
    pub fn version(&self) -> u8 {
        self.version.load(Ordering::Relaxed)
    }
    pub fn set_client_id(&self, id: &str) {
        *self.client_id.lock().unwrap() = id.to_string();
    }
    pub fn client_id(&self) -> String {
        self.client_id.lock().unwrap().clone()
    }
    pub fn remote_addr(&self) -> String {
        self.remote_addr.clone()
    }
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Read one complete MQTT frame. `timeout` mirrors Go's read deadline
    /// (keepalive × 1.5); `None` = no deadline. Errors when the conn is closed
    /// locally, timed out, or the peer disconnected.
    pub async fn read_frame(&self, timeout: Option<Duration>) -> std::io::Result<Vec<u8>> {
        let deadline = timeout.map(|d| tokio::time::Instant::from_std(Instant::now() + d));
        let mut close_rx = self.close_rx.clone();
        let mut guard = self.reader.lock().await;
        loop {
            // 1. try to split a complete frame from the buffer
            match crate::parser::split_frame(&guard.buf, self.max_packet_size) {
                Ok(Some((frame, leftover))) => {
                    let frame = frame.to_vec();
                    let leftover = leftover.to_vec();
                    guard.buf.clear();
                    guard.buf.extend_from_slice(&leftover);
                    return Ok(frame);
                }
                Ok(None) => {}
                Err(e) => {
                    guard.buf.clear();
                    return Err(e);
                }
            }
            // 2. await: close signal | deadline | more bytes
            let mut tmp = [0u8; 4096];
            let deadline_fut = Self::deadline_sleep(deadline);
            tokio::pin!(deadline_fut);
            tokio::select! {
                biased;
                _ = close_rx.changed() => {
                    return Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "connection closed"));
                }
                _ = &mut deadline_fut => {
                    return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "read deadline exceeded"));
                }
                r = Self::read_more(&mut guard, &mut tmp) => {
                    let n = r?;
                    if n == 0 {
                        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF"));
                    }
                    guard.buf.extend_from_slice(&tmp[..n]);
                }
            }
        }
    }

    async fn deadline_sleep(deadline: Option<tokio::time::Instant>) {
        match deadline {
            Some(d) => tokio::time::sleep_until(d).await,
            None => std::future::pending::<()>().await,
        }
    }

    async fn read_more(guard: &mut ReaderState, tmp: &mut [u8]) -> std::io::Result<usize> {
        match &mut guard.half {
            ReaderHalf::Tcp(r) => r.read(tmp).await,
            ReaderHalf::Tls(r) => r.read(tmp).await,
            ReaderHalf::Ws(ws) => {
                // WS messages may contain multiple MQTT frames; read one
                // message and buffer it.
                loop {
                    let msg = match ws.stream.next().await {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            return Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, e.to_string()))
                        }
                        None => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF")),
                    };
                    match msg {
                        Message::Binary(data) => {
                            ws.buf.extend_from_slice(&data);
                            let n = tmp.len().min(ws.buf.len());
                            tmp[..n].copy_from_slice(&ws.buf[..n]);
                            ws.buf.drain(..n);
                            return Ok(n);
                        }
                        Message::Close(_) => {
                            return Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "ws close"))
                        }
                        Message::Ping(_) | Message::Pong(_) | Message::Text(_) | Message::Frame(_) => {
                            continue; // tungstenite auto-responds to pings
                        }
                    }
                }
            }
        }
    }

    /// Write raw bytes (10s write timeout, like Go's writeTimeout).
    pub async fn write(&self, data: &[u8]) -> std::io::Result<()> {
        let mut guard = self.writer.lock().await;
        // write
        let write_res: WriteResult = match &mut *guard {
            WriterHalf::Tcp(w) => match tokio::time::timeout(WRITE_TIMEOUT, w.write_all(data)).await {
                Ok(r) => Ok(r),
                Err(_) => Err(()),
            },
            WriterHalf::Tls(w) => match tokio::time::timeout(WRITE_TIMEOUT, w.write_all(data)).await {
                Ok(r) => Ok(r),
                Err(_) => Err(()),
            },
            WriterHalf::Ws(w) => {
                match tokio::time::timeout(WRITE_TIMEOUT, w.stream.send(Message::Binary(data.to_vec()))).await {
                    Ok(r) => Ok(r.map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionAborted, e.to_string()))),
                    Err(_) => Err(()),
                }
            }
        };
        match write_res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(()) => return Err(timeout_err()),
        }
        // flush stream-based writers (WS sinks flush eagerly)
        let flush_res: Result<Result<(), std::io::Error>, ()> = match &mut *guard {
            WriterHalf::Tcp(w) => match tokio::time::timeout(WRITE_TIMEOUT, w.flush()).await {
                Ok(r) => Ok(r),
                Err(_) => Err(()),
            },
            WriterHalf::Tls(w) => match tokio::time::timeout(WRITE_TIMEOUT, w.flush()).await {
                Ok(r) => Ok(r),
                Err(_) => Err(()),
            },
            WriterHalf::Ws(_) => return Ok(()),
        };
        match flush_res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(()) => Err(timeout_err()),
        }
    }

    /// Close the connection: signal the reader and shut down the writer.
    pub async fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = self.close_tx.send(true);
        let mut guard = self.writer.lock().await;
        let res: Result<Result<(), std::io::Error>, tokio_tungstenite::tungstenite::Error> =
            match &mut *guard {
                WriterHalf::Tcp(w) => Ok(w.shutdown().await),
                WriterHalf::Tls(w) => Ok(w.shutdown().await),
                WriterHalf::Ws(w) => w.stream.close().await.map(Ok),
            };
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::debug!("conn shutdown: {}", e),
            Err(e) => tracing::debug!("ws close: {}", e),
        }
    }
}
