//! 管理 API (Management REST API) — 行为与 Go 版本完全一致。
//! Port of `internal/broker/admin.go`:
//! - Bearer/X-Admin-Token auth (constant-time), loopback fallback when no token
//! - identical endpoints, JSON field names, status codes and error strings
//!
//! GET    /api/v1/info
//! GET    /api/v1/stats
//! GET    /api/v1/health
//! GET    /api/v1/clients
//! GET    /api/v1/clients/{clientID}
//! DELETE /api/v1/clients/{clientID}
//! GET    /api/v1/sessions
//! GET    /api/v1/sessions/{clientID}
//! DELETE /api/v1/sessions/{clientID}
//! GET    /api/v1/subscriptions
//! GET    /api/v1/subscriptions/{clientID}
//! GET    /api/v1/retained?with_payload=true
//! DELETE /api/v1/retained?topic=t | ?all=true
//! POST   /api/v1/publish
//! GET    /api/v1/nodes
//! POST   /api/v1/acl/reload

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use base64::Engine;

use crate::codec::packet_type::DISCONNECT;
use crate::codec::Packet;
use crate::metrics::metrics;
use crate::persistence::Message;
use crate::session::Session;
use crate::topic::SubEntry;
use crate::transport::Conn;

use super::util;
use super::Broker;

// ---------------------------------------------------------------------------
// Response / request structs (JSON field names identical to the Go structs)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ApiError {
    error: String,
}

#[derive(Serialize)]
struct InfoResponse {
    #[serde(rename = "nodeId")]
    node_id: String,
    version: String,
    commit: String,
    date: String,
    #[serde(rename = "uptimeSeconds")]
    uptime_seconds: i64,
    mode: String, // "cluster" | "standalone"
    #[serde(rename = "redisAddr")]
    redis_addr: String,
    #[serde(rename = "adminEnabled")]
    admin_enabled: bool,
    #[serde(rename = "adminTls")]
    admin_tls: bool,
}

#[derive(Serialize)]
struct StatsResponse {
    #[serde(rename = "startedAt")]
    started_at: String,
    #[serde(rename = "uptimeSeconds")]
    uptime_seconds: i64,
    #[serde(rename = "messagesReceived")]
    messages_received: i64,
    #[serde(rename = "messagesSent")]
    messages_sent: i64,
    #[serde(rename = "clientsConnected")]
    clients_connected: i64,
    #[serde(rename = "clientsTotal")]
    clients_total: i64,
    sessions: usize,
    #[serde(rename = "retainedMessages")]
    retained_messages: usize,
    #[serde(rename = "retainedSizeBytes")]
    retained_size_bytes: i64,
    nodes: Vec<String>,
}

#[derive(Serialize)]
struct ClientResponse {
    #[serde(rename = "clientId")]
    client_id: String,
    username: String,
    version: String, // "3.1" | "3.1.1" | "5.0"
    #[serde(rename = "remoteAddr")]
    remote_addr: String,
    #[serde(rename = "keepAlive")]
    keep_alive: u16,
    #[serde(rename = "cleanStart")]
    clean_start: bool,
    #[serde(rename = "sessionExpiry")]
    session_expiry: u32,
    #[serde(rename = "nodeId")]
    node_id: String,
    subscriptions: usize,
    inflight: usize,
    #[serde(rename = "connectedAt")]
    connected_at: String,
}

#[derive(Serialize)]
struct SessionResponse {
    #[serde(rename = "clientId")]
    client_id: String,
    username: String,
    version: String,
    connected: bool,
    #[serde(rename = "cleanStart")]
    clean_start: bool,
    #[serde(rename = "sessionExpiry")]
    session_expiry: u32,
    #[serde(rename = "keepAlive")]
    keep_alive: u16,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "nodeId")]
    node_id: String,
    subscriptions: usize,
    inflight: usize,
}

#[derive(Serialize)]
struct SubscriptionResponse {
    #[serde(rename = "clientId")]
    client_id: String,
    filter: String,
    qos: u8,
    #[serde(rename = "noLocal")]
    no_local: bool,
}

#[derive(Serialize)]
struct RetainedResponse {
    topic: String,
    qos: u8,
    size: usize,
    #[serde(rename = "payloadB64", skip_serializing_if = "String::is_empty")]
    payload_b64: String,
}

#[derive(Deserialize, Default)]
struct PublishRequest {
    #[serde(default)]
    topic: String,
    #[serde(default)]
    payload: String, // UTF-8 text payload
    #[serde(default, rename = "payloadB64")]
    payload_b64: String, // binary payload (base64), takes priority over payload
    #[serde(default)]
    qos: u8,
    #[serde(default)]
    retain: bool,
}

// ---------------------------------------------------------------------------
// HTTP server plumbing
// ---------------------------------------------------------------------------

pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub query: HashMap<String, String>,
    pub headers: HashMap<String, String>, // lowercase keys
    pub body: Vec<u8>,
    pub remote_addr: String,
}

pub struct HttpResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
    pub extra_headers: Vec<(String, String)>,
}

impl HttpResponse {
    fn json(status: u16, v: &impl Serialize) -> Self {
        let mut body = serde_json::to_vec(v).unwrap_or_default();
        body.push(b'\n'); // Go's json.Encoder appends a newline
        HttpResponse { status, content_type: "application/json; charset=utf-8", body, extra_headers: vec![] }
    }

    pub fn text(status: u16, body: &str) -> Self {
        HttpResponse { status, content_type: "text/plain; charset=utf-8", body: body.as_bytes().to_vec(), extra_headers: vec![] }
    }
}

fn status_reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "",
    }
}

/// Read one HTTP request from the stream. Ok(None) = clean EOF.
async fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<HttpRequest>> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut tmp = [0u8; 4096];
    let header_end;
    loop {
        // find header terminator
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            header_end = pos;
            break;
        }
        if buf.len() > 64 * 1024 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "header too large"));
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof in headers"));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let header_str = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = header_str.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let _version = parts.next().unwrap_or("HTTP/1.1").to_string();

    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    // split query
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), parse_query(q)),
        None => (target.clone(), HashMap::new()),
    };

    // body
    let mut body = Vec::new();
    const MAX_BODY: usize = 1 << 20; // 1MB, mirrors io.LimitReader
    let chunked = headers
        .get("transfer-encoding")
        .map(|v| v.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false);
    if chunked {
        let mut rest = buf[header_end + 4..].to_vec();
        loop {
            if let Some((data, consumed)) = decode_chunked(&rest) {
                body.extend_from_slice(&data);
                let _ = consumed;
                break;
            }
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            rest.extend_from_slice(&tmp[..n]);
        }
    } else {
        let content_length: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
        let to_read = content_length.min(MAX_BODY + 1);
        body.extend_from_slice(&buf[header_end + 4..]);
        while body.len() < to_read {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
    }
    body.truncate(MAX_BODY);

    let remote_addr = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    Ok(Some(HttpRequest { method, path, query, headers, body, remote_addr, }))
}

fn decode_chunked(data: &[u8]) -> Option<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut pos = 0;
    loop {
        let line_end = find_subslice(&data[pos..], b"\r\n")? + pos;
        let size_str = String::from_utf8_lossy(&data[pos..line_end]);
        let size = usize::from_str_radix(size_str.trim().split(';').next()?.trim(), 16).ok()?;
        pos = line_end + 2;
        if size == 0 {
            // consume trailing CRLF
            if data.len() >= pos + 2 {
                pos += 2;
            }
            return Some((out, pos));
        }
        if pos + size + 2 > data.len() {
            return None; // incomplete
        }
        out.extend_from_slice(&data[pos..pos + size]);
        pos += size + 2;
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse percent-encoded query string (also decodes '+' as space like Go).
fn parse_query(q: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.entry(percent_decode(k)).or_insert(percent_decode(v));
    }
    out
}

pub(crate) fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                match (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push(h * 16 + l);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

async fn write_response(stream: &mut TcpStream, resp: &HttpResponse, keep_alive: bool) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n",
        resp.status,
        status_reason(resp.status),
        resp.content_type,
        resp.body.len()
    );
    for (k, v) in &resp.extra_headers {
        head.push_str(&format!("{}: {}\r\n", k, v));
    }
    head.push_str(if keep_alive { "Connection: keep-alive\r\n" } else { "Connection: close\r\n" });
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&resp.body).await?;
    stream.flush().await
}

/// Test helper: serve admin HTTP over an existing stream.
pub async fn test_serve_conn(stream: TcpStream, b: Arc<Broker>) -> std::io::Result<()> {
    serve_http_conn(stream, b, false).await
}

/// Serve the admin API (plaintext).
pub async fn serve_admin(addr: &str, b: Arc<Broker>) -> std::io::Result<()> {
    let ln = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = ln.accept().await?;
        let b = b.clone();
        tokio::spawn(async move {
            let _ = serve_http_conn(stream, b, false).await;
        });
    }
}

/// Serve the admin API over TLS.
pub async fn serve_admin_tls(addr: &str, b: Arc<Broker>, tls: Arc<rustls::ServerConfig>) -> std::io::Result<()> {
    let ln = TcpListener::bind(addr).await?;
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    loop {
        let (stream, _) = ln.accept().await?;
        let b = b.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let _ = serve_http_tls(tls_stream, b, true).await;
                }
                Err(e) => tracing::debug!("admin tls accept failed: {}", e),
            }
        });
    }
}

/// Serve the embedded dashboard + /api/v1 on the same port (combined handler).
pub async fn serve_webui(addr: &str, b: Arc<Broker>) -> std::io::Result<()> {
    let ln = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = ln.accept().await?;
        let b = b.clone();
        tokio::spawn(async move {
            let _ = serve_http_conn(stream, b, true).await;
        });
    }
}

async fn serve_http_conn(mut stream: TcpStream, b: Arc<Broker>, webui: bool) -> std::io::Result<()> {
    loop {
        let req = match read_request(&mut stream).await {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(()),
            Err(_) => return Ok(()),
        };
        let keep_alive = !req.headers.get("connection").map(|v| v.to_ascii_lowercase().contains("close")).unwrap_or(false);
        let resp = route(&b, &req, webui).await;
        write_response(&mut stream, &resp, keep_alive).await?;
        if !keep_alive {
            return Ok(());
        }
    }
}

async fn serve_http_tls(mut stream: tokio_rustls::server::TlsStream<TcpStream>, b: Arc<Broker>, webui: bool) -> std::io::Result<()> {
    loop {
        let req = match read_request_tls(&mut stream).await {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(()),
            Err(_) => return Ok(()),
        };
        let keep_alive = !req.headers.get("connection").map(|v| v.to_ascii_lowercase().contains("close")).unwrap_or(false);
        let resp = route(&b, &req, webui).await;
        write_response_tls(&mut stream, &resp, keep_alive).await?;
        if !keep_alive {
            return Ok(());
        }
    }
}

async fn read_request_tls<S: AsyncReadExt + Unpin>(
    stream: &mut S,
) -> std::io::Result<Option<HttpRequest>> {
    // Reuse the TCP logic via a shim: read raw bytes into a buffer manually.
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut tmp = [0u8; 4096];
    let header_end;
    loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            header_end = pos;
            break;
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    parse_from_buffer(buf, header_end, stream).await
}

async fn parse_from_buffer<S: AsyncReadExt + Unpin>(
    buf: Vec<u8>,
    header_end: usize,
    stream: &mut S,
) -> std::io::Result<Option<HttpRequest>> {
    let header_str = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = header_str.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let _version = parts.next().unwrap_or("HTTP/1.1").to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), parse_query(q)),
        None => (target.clone(), HashMap::new()),
    };
    let mut body = Vec::new();
    let content_length: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
    const MAX_BODY: usize = 1 << 20;
    let to_read = content_length.min(MAX_BODY + 1);
    body.extend_from_slice(&buf[header_end + 4..]);
    let mut tmp = [0u8; 4096];
    while body.len() < to_read {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(MAX_BODY);
    Ok(Some(HttpRequest { method, path, query, headers, body, remote_addr: "tls".to_string() }))
}

async fn write_response_tls<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    resp: &HttpResponse,
    keep_alive: bool,
) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n",
        resp.status,
        status_reason(resp.status),
        resp.content_type,
        resp.body.len()
    );
    for (k, v) in &resp.extra_headers {
        head.push_str(&format!("{}: {}\r\n", k, v));
    }
    head.push_str(if keep_alive { "Connection: keep-alive\r\n" } else { "Connection: close\r\n" });
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&resp.body).await?;
    stream.flush().await
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn is_loopback_ip(host: &str) -> bool {
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => false,
    }
}

fn authorized(b: &Broker, req: &HttpRequest) -> bool {
    let token = &b.cfg.admin_token;
    if token.is_empty() {
        // no token configured: only loopback callers
        let host = req.remote_addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(&req.remote_addr);
        let host = host.trim_start_matches('[').trim_end_matches(']');
        return is_loopback_ip(host);
    }
    let mut h: Option<String> = None;
    if let Some(authz) = req.headers.get("authorization") {
        const PREFIX: &str = "Bearer ";
        if authz.len() > PREFIX.len() && authz[..PREFIX.len()].eq_ignore_ascii_case(PREFIX) {
            h = Some(authz[PREFIX.len()..].to_string());
        }
    }
    let h = match h {
        Some(h) => h,
        None => match req.headers.get("x-admin-token") {
            Some(t) if !t.is_empty() => t.clone(),
            _ => return false,
        },
    };
    constant_time_eq(h.as_bytes(), token.as_bytes())
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

async fn route(b: &Arc<Broker>, req: &HttpRequest, webui: bool) -> HttpResponse {
    if webui && !req.path.starts_with("/api/") {
        return crate::webui::serve(&req.path);
    }
    if !authorized(b, req) {
        let mut resp = HttpResponse::json(401, &ApiError { error: "unauthorized".into() });
        resp.extra_headers.push(("WWW-Authenticate".into(), "Bearer realm=\"mqtt-admin\"".into()));
        return resp;
    }
    let segs: Vec<&str> = req.path.trim_end_matches('/').split('/').collect();
    // /api/v1/...
    if segs.len() >= 3 && segs[1] == "api" && segs[2] == "v1" {
        let rest = &segs[3..];
        match (req.method.as_str(), rest) {
            ("GET", ["info"]) => handle_info(b).await,
            ("GET", ["stats"]) => handle_stats(b).await,
            ("GET", ["health"]) => handle_health(b).await,
            ("GET", ["clients"]) => handle_list_clients(b).await,
            ("GET", ["clients", id]) => handle_get_client(b, id).await,
            ("DELETE", ["clients", id]) => handle_kick_client(b, id).await,
            ("GET", ["sessions"]) => handle_list_sessions(b).await,
            ("GET", ["sessions", id]) => handle_get_session(b, id).await,
            ("DELETE", ["sessions", id]) => handle_delete_session(b, id).await,
            ("GET", ["subscriptions"]) => handle_list_subscriptions(b).await,
            ("GET", ["subscriptions", id]) => handle_client_subscriptions(b, id).await,
            ("GET", ["retained"]) => handle_list_retained(b, req).await,
            ("DELETE", ["retained"]) => handle_delete_retained(b, req).await,
            ("POST", ["publish"]) => handle_publish(b, req).await,
            ("GET", ["nodes"]) => handle_nodes(b).await,
            ("POST", ["acl", "reload"]) => handle_acl_reload(b).await,
            (_, _) if rest.is_empty() => HttpResponse::text(404, "404 page not found\n"),
            // method mismatch on a known path -> 405 like Go's ServeMux
            (m, r) if path_known(r) => HttpResponse::text(405, "Method Not Allowed\n").with_method(m),
            _ => HttpResponse::text(404, "404 page not found\n"),
        }
    } else {
        HttpResponse::text(404, "404 page not found\n")
    }
}

fn path_known(rest: &[&str]) -> bool {
    matches!(
        rest,
        ["info"]
            | ["stats"]
            | ["health"]
            | ["clients"]
            | ["clients", _]
            | ["sessions"]
            | ["sessions", _]
            | ["subscriptions"]
            | ["subscriptions", _]
            | ["retained"]
            | ["publish"]
            | ["nodes"]
            | ["acl", "reload"]
    )
}

impl HttpResponse {
    fn with_method(self, _m: &str) -> Self {
        self
    }
}

// ---------------------------------------------------------------------------
// Handlers (identical behavior to the Go implementations)
// ---------------------------------------------------------------------------

async fn handle_info(b: &Arc<Broker>) -> HttpResponse {
    let mode = if b.cluster.lock().unwrap().is_some() { "cluster" } else { "standalone" };
    let started_at = b.started_at.lock().unwrap().clone();
    let uptime = started_at
        .map(|t| t.elapsed().unwrap_or_default().as_secs() as i64)
        .unwrap_or(0);
    let version_info = b.version_info.lock().unwrap().clone();
    let (version, commit, date) = match version_info {
        Some(v) => (v.version, v.commit, v.date),
        None => ("dev".into(), "none".into(), "unknown".into()),
    };
    HttpResponse::json(
        200,
        &InfoResponse {
            node_id: b.node_id(),
            version,
            commit,
            date,
            uptime_seconds: uptime,
            mode: mode.into(),
            redis_addr: b.cfg.redis_addr.clone(),
            admin_enabled: !b.cfg.admin_addr.is_empty(),
            admin_tls: b.cfg.admin_tls,
        },
    )
}

async fn handle_stats(b: &Arc<Broker>) -> HttpResponse {
    let st = b.stats();
    let (retained, size) = match b.store.get_retained_stats().await {
        Ok(s) => (s.total_messages, s.total_size),
        Err(e) => return HttpResponse::json(500, &ApiError { error: e }),
    };
    let mut nodes = vec![b.node_id()];
    let cluster = b.cluster.lock().unwrap().clone();
    if let Some(cluster) = cluster {
        match tokio::time::timeout(Duration::from_secs(2), cluster.nodes()).await {
            Ok(Ok(ns)) => nodes = ns,
            _ => {}
        }
    }
    nodes.sort();
    let started_at = st.started_at;
    let uptime = started_at.elapsed().unwrap_or_default().as_secs() as i64;
    HttpResponse::json(
        200,
        &StatsResponse {
            started_at: util::go_rfc3339(&started_at),
            uptime_seconds: uptime,
            messages_received: st.messages_received,
            messages_sent: st.messages_sent,
            clients_connected: st.clients_connected,
            clients_total: st.clients_total,
            sessions: b.session_count(),
            retained_messages: retained,
            retained_size_bytes: size,
            nodes,
        },
    )
}

async fn handle_health(b: &Arc<Broker>) -> HttpResponse {
    match tokio::time::timeout(Duration::from_secs(2), b.health()).await {
        Ok(Ok(())) => HttpResponse::json(200, &serde_json::json!({"status": "ok"})),
        Ok(Err(e)) => HttpResponse::json(503, &ApiError { error: e }),
        Err(_) => HttpResponse::json(503, &ApiError { error: "context deadline exceeded".into() }),
    }
}

fn client_response_from(client_id: &str, conn: Option<&Arc<Conn>>, sess: Option<&Arc<Session>>) -> ClientResponse {
    let mut ci = ClientResponse {
        client_id: client_id.to_string(),
        username: String::new(),
        version: String::new(),
        remote_addr: String::new(),
        keep_alive: 0,
        clean_start: false,
        session_expiry: 0,
        node_id: String::new(),
        subscriptions: 0,
        inflight: 0,
        connected_at: "0001-01-01T00:00:00Z".into(),
    };
    if let Some(conn) = conn {
        ci.remote_addr = conn.remote_addr();
        ci.version = util::protocol_name(conn.version());
    }
    if let Some(sess) = sess {
        ci.username = sess.username();
        ci.keep_alive = *sess.keep_alive.lock().unwrap();
        ci.clean_start = *sess.clean_start.lock().unwrap();
        ci.session_expiry = *sess.expiry_interval.lock().unwrap();
        ci.node_id = sess.node_id.lock().unwrap().clone();
        ci.subscriptions = sess.subscription_count();
        ci.inflight = sess.inflight_len();
        ci.connected_at = util::go_rfc3339(&sess.created_at.lock().unwrap().clone());
    }
    ci
}

async fn handle_list_clients(b: &Arc<Broker>) -> HttpResponse {
    let conns = b.conns.read().unwrap().clone();
    let sessions = b.sessions.read().unwrap().clone();
    let mut out: Vec<ClientResponse> = Vec::with_capacity(conns.len());
    for (id, conn) in &conns {
        out.push(client_response_from(id, Some(conn), sessions.get(id)));
    }
    out.sort_by(|a, b| a.client_id.cmp(&b.client_id));
    HttpResponse::json(200, &out)
}

async fn handle_get_client(b: &Arc<Broker>, id: &str) -> HttpResponse {
    let (conn, sess) = {
        let conns = b.conns.read().unwrap();
        let sessions = b.sessions.read().unwrap();
        (conns.get(id).cloned(), sessions.get(id).cloned())
    };
    match conn {
        None => HttpResponse::json(
            404,
            &ApiError { error: format!("client {} not connected", util::go_quote(id)) },
        ),
        Some(conn) => HttpResponse::json(200, &client_response_from(id, Some(&conn), sess.as_ref())),
    }
}

async fn handle_list_sessions(b: &Arc<Broker>) -> HttpResponse {
    let sessions = b.sessions.read().unwrap().clone();
    let conns = b.conns.read().unwrap();
    let mut out: Vec<SessionResponse> = Vec::with_capacity(sessions.len());
    for (id, sess) in &sessions {
        out.push(session_response_from(id, sess, conns.contains_key(id)));
    }
    out.sort_by(|a, b| a.client_id.cmp(&b.client_id));
    HttpResponse::json(200, &out)
}

fn session_response_from(client_id: &str, sess: &Arc<Session>, conn_present: bool) -> SessionResponse {
    SessionResponse {
        client_id: client_id.to_string(),
        username: sess.username(),
        version: util::protocol_name(*sess.version.lock().unwrap()),
        connected: sess.is_connected() || conn_present,
        clean_start: *sess.clean_start.lock().unwrap(),
        session_expiry: *sess.expiry_interval.lock().unwrap(),
        keep_alive: *sess.keep_alive.lock().unwrap(),
        created_at: util::go_rfc3339(&sess.created_at.lock().unwrap().clone()),
        node_id: sess.node_id.lock().unwrap().clone(),
        subscriptions: sess.subscription_count(),
        inflight: sess.inflight_len(),
    }
}

async fn handle_get_session(b: &Arc<Broker>, id: &str) -> HttpResponse {
    let (sess, connected) = {
        let sessions = b.sessions.read().unwrap();
        let conns = b.conns.read().unwrap();
        (sessions.get(id).cloned(), conns.contains_key(id))
    };
    match sess {
        None => HttpResponse::json(404, &ApiError { error: format!("session {} not found", util::go_quote(id)) }),
        Some(sess) => HttpResponse::json(200, &session_response_from(id, &sess, connected)),
    }
}

fn sub_response(e: &SubEntry) -> SubscriptionResponse {
    SubscriptionResponse { client_id: e.client_id.clone(), filter: e.filter.clone(), qos: e.qos, no_local: e.no_local }
}

async fn handle_list_subscriptions(b: &Arc<Broker>) -> HttpResponse {
    let mut out: Vec<SubscriptionResponse> = b.trie.subscriptions().iter().map(sub_response).collect();
    out.sort_by(|a, c| {
        a.client_id.cmp(&c.client_id).then_with(|| a.filter.cmp(&c.filter))
    });
    HttpResponse::json(200, &out)
}

async fn handle_client_subscriptions(b: &Arc<Broker>, id: &str) -> HttpResponse {
    let mut out: Vec<SubscriptionResponse> =
        b.trie.subscriptions_for(id).iter().map(sub_response).collect();
    out.sort_by(|a, c| a.filter.cmp(&c.filter));
    HttpResponse::json(200, &out)
}

async fn handle_list_retained(b: &Arc<Broker>, req: &HttpRequest) -> HttpResponse {
    let with_payload = req.query.get("with_payload").map(|v| v == "true").unwrap_or(false);
    let msgs = match b.store.list_retained().await {
        Ok(m) => m,
        Err(e) => return HttpResponse::json(500, &ApiError { error: e }),
    };
    let mut out: Vec<RetainedResponse> = msgs
        .iter()
        .map(|m: &Message| {
            let payload_b64 = if with_payload {
                base64::engine::general_purpose::STANDARD.encode(&m.payload)
            } else {
                String::new()
            };
            RetainedResponse { topic: m.topic.clone(), qos: m.qos, size: m.payload.len(), payload_b64 }
        })
        .collect();
    out.sort_by(|a, c| a.topic.cmp(&c.topic));
    HttpResponse::json(200, &out)
}

async fn handle_nodes(b: &Arc<Broker>) -> HttpResponse {
    let mut nodes = vec![b.node_id()];
    let cluster = b.cluster.lock().unwrap().clone();
    if let Some(cluster) = cluster {
        match tokio::time::timeout(Duration::from_secs(2), cluster.nodes()).await {
            Ok(Ok(ns)) => nodes = ns,
            _ => {}
        }
    }
    nodes.sort();
    let mut obj = std::collections::BTreeMap::new();
    obj.insert("nodes".to_string(), serde_json::json!(nodes));
    HttpResponse::json(200, &serde_json::Value::Object(obj.into_iter().collect()))
}

async fn handle_kick_client(b: &Arc<Broker>, id: &str) -> HttpResponse {
    let conn = b.conns.read().unwrap().get(id).cloned();
    let conn = match conn {
        Some(c) => c,
        None => {
            return HttpResponse::json(
                404,
                &ApiError { error: format!("client {} not connected", util::go_quote(id)) },
            )
        }
    };
    kick_conn(b, &conn).await;
    tracing::info!("admin kick client: client={} by={}", id, conn.remote_addr());
    let mut obj = std::collections::BTreeMap::new();
    obj.insert("ok".to_string(), serde_json::json!(true));
    obj.insert("clientId".to_string(), serde_json::json!(id));
    HttpResponse::json(200, &serde_json::Value::Object(obj.into_iter().collect()))
}

async fn kick_conn(b: &Arc<Broker>, conn: &Arc<Conn>) {
    if conn.version() == crate::codec::PROTOCOL_V5 {
        let disc = Packet {
            ptype: DISCONNECT,
            version: crate::codec::PROTOCOL_V5,
            disc_reason: 0x99, // administrative action
            ..Default::default()
        };
        let _ = b.send_packet(conn, &disc).await;
    }
    conn.close().await;
}

async fn handle_delete_session(b: &Arc<Broker>, id: &str) -> HttpResponse {
    let (conn, sess) = {
        let mut conns = b.conns.write().unwrap();
        let mut sessions = b.sessions.write().unwrap();
        (conns.remove(id), sessions.remove(id))
    };
    if let Some(sess) = &sess {
        sess.set_deleted(true);
        let subs = sess.subscription_filters();
        sess.set_will(None);
        for f in &subs {
            b.trie.remove(f, id);
        }
    }
    if let Some(conn) = &conn {
        kick_conn(b, conn).await;
    }
    if let Err(e) = b.store.delete_session(id).await {
        return HttpResponse::json(500, &ApiError { error: e });
    }
    if let Err(e) = b.store.clear_offline(id).await {
        return HttpResponse::json(500, &ApiError { error: e });
    }
    tracing::info!("admin delete session: client={}", id);
    let mut obj = std::collections::BTreeMap::new();
    obj.insert("ok".to_string(), serde_json::json!(true));
    obj.insert("clientId".to_string(), serde_json::json!(id));
    HttpResponse::json(200, &serde_json::Value::Object(obj.into_iter().collect()))
}

async fn handle_delete_retained(b: &Arc<Broker>, req: &HttpRequest) -> HttpResponse {
    if req.query.get("all").map(|v| v == "true").unwrap_or(false) {
        let msgs = match b.store.list_retained().await {
            Ok(m) => m,
            Err(e) => return HttpResponse::json(500, &ApiError { error: e }),
        };
        let n = msgs.len();
        for m in &msgs {
            if let Err(e) = b.store.delete_retained(&m.topic).await {
                tracing::warn!("admin clear retained failed: topic={} err={}", m.topic, e);
            }
        }
        let mut obj = std::collections::BTreeMap::new();
        obj.insert("ok".to_string(), serde_json::json!(true));
        obj.insert("deleted".to_string(), serde_json::json!(n));
        return HttpResponse::json(200, &serde_json::Value::Object(obj.into_iter().collect()));
    }
    let topic = req.query.get("topic").cloned().unwrap_or_default();
    if topic.is_empty() {
        return HttpResponse::json(
            400,
            &ApiError { error: "missing \"topic\" query param (or use ?all=true)".into() },
        );
    }
    if let Err(e) = b.store.delete_retained(&topic).await {
        return HttpResponse::json(500, &ApiError { error: e });
    }
    let mut obj = std::collections::BTreeMap::new();
    obj.insert("ok".to_string(), serde_json::json!(true));
    obj.insert("topic".to_string(), serde_json::json!(topic));
    HttpResponse::json(200, &serde_json::Value::Object(obj.into_iter().collect()))
}

async fn handle_publish(b: &Arc<Broker>, req: &HttpRequest) -> HttpResponse {
    let body = &req.body;
    let req_obj: PublishRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return HttpResponse::json(400, &ApiError { error: format!("invalid json: {}", e) }),
    };
    if req_obj.topic.is_empty() {
        return HttpResponse::json(400, &ApiError { error: "missing topic".into() });
    }
    if req_obj.qos > 2 {
        return HttpResponse::json(400, &ApiError { error: "qos must be 0..2".into() });
    }
    let payload = if !req_obj.payload_b64.is_empty() {
        match base64::engine::general_purpose::STANDARD.decode(req_obj.payload_b64.as_bytes()) {
            Ok(p) => p,
            Err(e) => return HttpResponse::json(400, &ApiError { error: format!("invalid payloadB64: {}", e) }),
        }
    } else {
        req_obj.payload.clone().into_bytes()
    };
    if let Err(e) = b.publish(&req_obj.topic, &payload, req_obj.qos, req_obj.retain).await {
        return HttpResponse::json(400, &ApiError { error: e });
    }
    tracing::info!(
        "admin publish: topic={} qos={} retain={}",
        req_obj.topic, req_obj.qos, req_obj.retain
    );
    let mut obj = std::collections::BTreeMap::new();
    obj.insert("ok".to_string(), serde_json::json!(true));
    obj.insert("topic".to_string(), serde_json::json!(req_obj.topic));
    HttpResponse::json(200, &serde_json::Value::Object(obj.into_iter().collect()))
}

async fn handle_acl_reload(b: &Arc<Broker>) -> HttpResponse {
    let acls = &b.file_acls;
    if acls.is_empty() {
        return HttpResponse::json(400, &ApiError { error: "no FileACL configured".into() });
    }
    let mut reloaded = 0;
    for facl in acls {
        match facl.reload().await {
            Err(e) => return HttpResponse::json(500, &ApiError { error: e }),
            Ok(true) => reloaded += 1,
            Ok(false) => {}
        }
    }
    tracing::info!("admin acl reload: reloaded={}", reloaded);
    let mut obj = std::collections::BTreeMap::new();
    obj.insert("ok".to_string(), serde_json::json!(true));
    obj.insert("reloaded".to_string(), serde_json::json!(reloaded));
    HttpResponse::json(200, &serde_json::Value::Object(obj.into_iter().collect()))
}

// ---------------------------------------------------------------------------
// Metrics / healthz / readyz server (pprof port)
// ---------------------------------------------------------------------------

pub async fn serve_metrics(addr: &str, b: Arc<Broker>) -> std::io::Result<()> {
    let ln = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = ln.accept().await?;
        let b = b.clone();
        tokio::spawn(async move {
            let _ = serve_metrics_conn(stream, b).await;
        });
    }
}

async fn serve_metrics_conn(mut stream: TcpStream, b: Arc<Broker>) -> std::io::Result<()> {
    loop {
        let req = match read_request(&mut stream).await {
            Ok(Some(r)) => r,
            _ => return Ok(()),
        };
        let keep_alive = !req.headers.get("connection").map(|v| v.to_ascii_lowercase().contains("close")).unwrap_or(false);
        let resp = match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/metrics") => {
                HttpResponse::text(200, &metrics().render()).with_prom()
            }
            ("GET", "/healthz") => HttpResponse::text(200, "ok"),
            ("GET", "/readyz") => {
                let start = std::time::Instant::now();
                let mut err = None;
                if b.cluster.lock().unwrap().is_some() {
                    match tokio::time::timeout(Duration::from_millis(50), b.store.ping()).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => err = Some(format!("redis unavailable: {}", e)),
                        Err(_) => err = Some("redis unavailable: timeout".into()),
                    }
                }
                metrics().redis_latency.observe(start.elapsed().as_secs_f64());
                if err.is_none() {
                    let n = b.client_count();
                    if n > 16000 {
                        err = Some("too many connections".into());
                    }
                }
                match err {
                    Some(e) => HttpResponse::text(503, &format!("{}\n", e)),
                    None => HttpResponse::text(200, "ok"),
                }
            }
            _ => HttpResponse::text(404, "404 page not found\n"),
        };
        write_response(&mut stream, &resp, keep_alive).await?;
        if !keep_alive {
            return Ok(());
        }
    }
}

impl HttpResponse {
    fn with_prom(mut self) -> Self {
        self.content_type = "text/plain; version=0.0.4; charset=utf-8";
        self
    }
}
