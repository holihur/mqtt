//! Listeners for TCP (with optional TLS) and WebSocket.
//! Port of `internal/transport/listener.go`:
//! - TCP: TCP_NODELAY + keepalive 3m, 20k connection semaphore
//! - WS: paths `/` and `/mqtt`, subprotocols `mqtt`/`mqttv3.1`, Origin check,
//!   read limit = max packet size
//! - TLS: MinVersion TLS1.2, optional mTLS via CA file

use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{watch, Semaphore};

use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use super::conn::Conn;

pub const MAX_CONCURRENT_CONNS: usize = 20000;

pub struct Listener {
    pub tcp_addr: String,
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
    pub ws_addr: String,
    ws_allow_origins: HashSet<String>,
    ws_allow_all: bool,
    pub max_packet_size: usize,
    /// Actual bound TCP address (for :0 ephemeral ports).
    pub bound_addr: Arc<std::sync::Mutex<Option<String>>>,
}

impl Listener {
    pub fn new(tcp_addr: &str, tls_config: Option<Arc<rustls::ServerConfig>>, ws_addr: &str) -> Self {
        Self {
            tcp_addr: tcp_addr.to_string(),
            tls_config,
            ws_addr: ws_addr.to_string(),
            ws_allow_origins: HashSet::new(),
            ws_allow_all: false,
            max_packet_size: 1 << 20,
            bound_addr: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub fn set_max_packet_size(&mut self, n: usize) {
        if n > 0 {
            self.max_packet_size = n;
        }
    }

    /// Set WS allowed origins. "*" allows all; entries are normalized like Go:
    /// store the full origin, its host[:port], and its bare hostname.
    pub fn set_ws_allow_origins(&mut self, origins: &[String]) {
        let mut m = HashSet::new();
        let mut allow_all = false;
        for o in origins {
            let o = o.trim();
            if o.is_empty() {
                continue;
            }
            if o == "*" {
                allow_all = true;
                continue;
            }
            if let Some(u) = OriginUrl::parse(o) {
                if let Some(host) = &u.host {
                    m.insert(host.clone());
                }
                if let Some(hostname) = &u.hostname {
                    m.insert(hostname.clone());
                }
            }
            m.insert(o.to_string());
        }
        self.ws_allow_origins = m;
        self.ws_allow_all = allow_all;
    }

    /// Run both listeners until `shutdown` fires. `handler` is invoked for
    /// every accepted connection (same as Go's `Listen(ctx, handle)`).
    pub async fn listen<F>(&self, mut shutdown: watch::Receiver<bool>, handler: F) -> io::Result<()>
    where
        F: Fn(Arc<Conn>) + Send + Sync + 'static,
    {
        let has_tcp = !self.tcp_addr.is_empty();
        let has_ws = !self.ws_addr.is_empty();

        if !has_tcp && !has_ws {
            // embedded mode without listeners: wait for shutdown
            let _ = shutdown.changed().await;
            return Ok(());
        }

        let handler = Arc::new(handler);

        if has_ws {
            let addr = self.ws_addr.clone();
            let handler = handler.clone();
            let mut shutdown2 = shutdown.clone();
            let origins = self.ws_allow_origins.clone();
            let allow_all = self.ws_allow_all;
            let max_packet = self.max_packet_size;
            tokio::spawn(async move {
                if let Err(e) = serve_ws(&addr, handler, origins, allow_all, max_packet, &mut shutdown2).await {
                    tracing::warn!("ws server error: {}", e);
                }
            });
        }

        if !has_tcp {
            let _ = shutdown.changed().await;
            return Ok(());
        }

        let ln = tokio::net::TcpListener::bind(&self.tcp_addr)
            .await
            .map_err(|e| io::Error::new(e.kind(), format!("tcp listen {}: {}", self.tcp_addr, e)))?;
        if let Ok(local) = ln.local_addr() {
            *self.bound_addr.lock().unwrap() = Some(local.to_string());
        }
        let acceptor: Option<tokio_rustls::TlsAcceptor> = self.tls_config.clone().map(tokio_rustls::TlsAcceptor::from);

        // shutdown closer
        let mut shutdown_close = shutdown.clone();
        let ln_close = Arc::new(ln);
        {
            let ln = ln_close.clone();
            tokio::spawn(async move {
                let _ = shutdown_close.changed().await;
                // dropping the listener closes it
                drop(ln);
            });
        }

        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNS));
        let max_packet = self.max_packet_size;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    return Ok(());
                }
                accepted = ln_close.accept() => {
                    let (mut stream, _) = match accepted {
                        Ok(x) => x,
                        Err(_) => {
                            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                            continue;
                        }
                    };
                    configure_tcp(&stream);
                    let permit = match sem.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            // over capacity: drop the connection
                            let _ = stream.shutdown().await;
                            continue;
                        }
                    };
                    let handler = handler.clone();
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let _permit = permit; // released when the task ends
                        match acceptor {
                            Some(a) => match a.accept(stream).await {
                                Ok(tls) => handler(Conn::new_tls(tls, max_packet)),
                                Err(e) => tracing::debug!("tls accept failed: {}", e),
                            },
                            None => handler(Conn::new_tcp(stream, max_packet)),
                        }
                    });
                }
            }
        }
    }
}

fn configure_tcp(stream: &TcpStream) {
    use socket2::SockRef;
    let sock = SockRef::from(stream);
    let _ = sock.set_nodelay(true);
    let _ = sock.set_keepalive(true);
    let _ = sock.set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(std::time::Duration::from_secs(180)));
    let _ = sock.set_recv_buffer_size(32 * 1024);
    let _ = sock.set_send_buffer_size(32 * 1024);
}

async fn serve_ws<F>(
    addr: &str,
    handler: Arc<F>,
    origins: HashSet<String>,
    allow_all: bool,
    max_packet_size: usize,
    shutdown: &mut watch::Receiver<bool>,
) -> io::Result<()>
where
    F: Fn(Arc<Conn>) + Send + Sync + 'static,
{
    let ln = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("ws listen {}: {}", addr, e)))?;
    let ln = Arc::new(ln);
    let mut shutdown_close = shutdown.clone();
    {
        let ln = ln.clone();
        tokio::spawn(async move {
            let _ = shutdown_close.changed().await;
            drop(ln);
        });
    }
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNS));
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                return Ok(());
            }
            accepted = ln.accept() => {
                let (mut stream, _) = match accepted {
                    Ok(x) => x,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        continue;
                    }
                };
                let permit = match sem.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        let _ = stream.shutdown().await;
                        continue;
                    }
                };
                let handler = handler.clone();
                let origins = origins.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    configure_tcp(&stream);
                    let host = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
                    let checker = {
                        let origins = origins.clone();
                        move |origin: &str| check_ws_origin(origin, &host, &origins, allow_all)
                    };
                    let cfg = WebSocketConfig {
                        max_message_size: Some(max_packet_size),
                        max_frame_size: Some(max_packet_size),
                        ..Default::default()
                    };
                    let origins_cb = origins.clone();
                    let callback = move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                        let origin = req.headers().get("Origin").and_then(|v| v.to_str().ok()).unwrap_or("");
                        if !check_ws_origin(origin, "", &origins_cb, allow_all) {
                            let mut err = ErrorResponse::new(None);
                            *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::FORBIDDEN;
                            return Err(err);
                        }
                        // negotiate subprotocol: mqtt / mqttv3.1
                        let mut resp = resp;
                        if let Some(proto) = req.headers().get("Sec-WebSocket-Protocol").and_then(|v| v.to_str().ok()) {
                            for candidate in proto.split(',').map(|s| s.trim()) {
                                if candidate == "mqtt" || candidate == "mqttv3.1" {
                                    if let Ok(v) = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(candidate) {
                                        resp.headers_mut().append("Sec-WebSocket-Protocol", v);
                                    }
                                    break;
                                }
                            }
                        }
                        Ok(resp)
                    };
                    match tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(cfg)).await {
                        Ok(ws) => handler(Conn::new_ws(ws, max_packet_size)),
                        Err(e) => tracing::debug!("ws upgrade failed: {}", e),
                    }
                    let _ = checker;
                });
            }
        }
    }
}

fn check_ws_origin(origin: &str, _host: &str, origins: &HashSet<String>, allow_all: bool) -> bool {
    if origin.is_empty() {
        return true;
    }
    if allow_all {
        return true;
    }
    if let Some(u) = OriginUrl::parse(origin) {
        if let Some(oh) = &u.host {
            if origins.contains(oh) {
                return true;
            }
        }
        if let Some(hn) = &u.hostname {
            if origins.contains(hn) {
                return true;
            }
        }
    }
    origins.contains(origin)
}

/// Minimal origin URL parsing (scheme://host[:port]/path).
struct OriginUrl {
    host: Option<String>,    // host[:port]
    hostname: Option<String>, // host
}

impl OriginUrl {
    fn parse(s: &str) -> Option<Self> {
        let rest = s.split_once("://")?.1;
        let end = rest.find(['/']).unwrap_or(rest.len());
        let authority = &rest[..end];
        if authority.is_empty() {
            return None;
        }
        // strip userinfo
        let authority = authority.rsplit('@').next().unwrap_or(authority);
        if authority.starts_with('[') {
            // IPv6
            if let Some(close) = authority.find(']') {
                let hostname = &authority[..=close];
                let host = if authority[close + 1..].starts_with(':') {
                    format!("{}{}", hostname, &authority[close + 1..])
                } else {
                    hostname.to_string()
                };
                return Some(OriginUrl { host: Some(host), hostname: Some(hostname.to_string()) });
            }
            return None;
        }
        match authority.rsplit_once(':') {
            Some((h, _port)) => Some(OriginUrl {
                host: Some(authority.to_string()),
                hostname: Some(h.to_string()),
            }),
            None => Some(OriginUrl { host: Some(authority.to_string()), hostname: Some(authority.to_string()) }),
        }
    }
}

/// Build a rustls ServerConfig from cert/key files with optional mTLS CA.
pub fn load_tls_config(
    cert_file: &str,
    key_file: &str,
    ca_file: &str,
) -> Result<Arc<rustls::ServerConfig>, String> {
    if cert_file.is_empty() || key_file.is_empty() {
        return Err("no cert/key configured".into());
    }
    let certs = load_certs(cert_file)?;
    let key = load_key(key_file)?;
    let builder = rustls::ServerConfig::builder();
    let builder = if !ca_file.is_empty() {
        let ca_pem = std::fs::read(ca_file).map_err(|e| format!("read ca {}: {}", ca_file, e))?;
        let mut rd = std::io::BufReader::new(&ca_pem[..]);
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut rd) {
            let cert = cert.map_err(|e| format!("parse ca {}: {}", ca_file, e))?;
            roots.add(cert).map_err(|e| format!("add ca cert: {}", e))?;
        }
        builder.with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| format!("client cert verifier: {}", e))?,
        )
    } else {
        builder.with_no_client_auth()
    };
    let cfg = builder
        .with_single_cert(certs, key)
        .map_err(|e| format!("tls single cert: {}", e))?;
    Ok(Arc::new(cfg))
}

fn load_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    use rustls::pki_types::CertificateDer;
    let data = std::fs::read(path).map_err(|e| format!("read cert {}: {}", path, e))?;
    let mut rd = std::io::BufReader::new(&data[..]);
    let mut certs = Vec::new();
    for cert in rustls_pemfile::certs(&mut rd) {
        certs.push(CertificateDer::from(cert.map_err(|e| format!("parse certs {}: {}", path, e))?));
    }
    Ok(certs)
}

fn load_key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>, String> {
    let data = std::fs::read(path).map_err(|e| format!("read key {}: {}", path, e))?;
    let mut rd = std::io::BufReader::new(&data[..]);
    match rustls_pemfile::private_key(&mut rd) {
        Ok(Some(k)) => Ok(k),
        Ok(None) => Err("no supported private key found".into()),
        Err(e) => Err(format!("parse key {}: {}", path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_parse() {
        let u = OriginUrl::parse("http://localhost:8080").unwrap();
        assert_eq!(u.host.as_deref(), Some("localhost:8080"));
        assert_eq!(u.hostname.as_deref(), Some("localhost"));
        let u = OriginUrl::parse("https://example.com/x/y").unwrap();
        assert_eq!(u.host.as_deref(), Some("example.com"));
        let u = OriginUrl::parse("http://[::1]:8080").unwrap();
        assert_eq!(u.host.as_deref(), Some("[::1]:8080"));
        assert_eq!(u.hostname.as_deref(), Some("[::1]"));
    }

    #[test]
    fn origin_check() {
        let mut l = Listener::new(":1883", None, "");
        l.set_ws_allow_origins(&["http://localhost:8080".to_string()]);
        assert!(check_ws_origin("http://localhost:8080", "", &l.ws_allow_origins, false));
        assert!(check_ws_origin("", "", &l.ws_allow_origins, false));
        assert!(!check_ws_origin("http://evil.com", "", &l.ws_allow_origins, false));
        l.set_ws_allow_origins(&["*".to_string()]);
        assert!(check_ws_origin("http://evil.com", "", &l.ws_allow_origins, true));
    }
}
