//! 管理 API 行为一致性集成测试 — 移植自 Go 版 `internal/broker/admin_test.go`。
//! 验证: 鉴权(401/loopback/X-Admin-Token)、info/stats/health/nodes、
//! publish/retained 管理、clients 列表与踢下线、sessions 持久化与删除、ACL reload。

use std::sync::Arc;
use std::time::Duration;

use mqtt_broker::broker::{Broker, Config};
use mqtt_broker::persistence::MemoryStore;

// ---------------------------------------------------------------------------
// 测试基建
// ---------------------------------------------------------------------------

async fn new_admin_broker(cfg: Config) -> (Arc<Broker>, String) {
    let mut cfg = cfg;
    if cfg.node_id.is_empty() {
        cfg.node_id = "admin-test".into();
    }
    cfg.allow_anonymous = true;
    cfg.max_packet_size = 1 << 20;
    let store = Arc::new(MemoryStore::new());
    let b = Broker::new_with_options(
        cfg,
        Some(store),
        None,
        Some(mqtt_broker::broker::BrokerVersion {
            version: "1.2.3".into(),
            commit: "abc123".into(),
            date: "2026-08-28".into(),
        }),
    )
    .unwrap();
    let addr = start_admin_server(&b).await;
    (b, addr)
}

/// Start an embedded admin server on an ephemeral port (mirrors the
/// httptest.NewServer(adm.handler()) setup in the Go tests).
async fn start_admin_server(b: &Arc<Broker>) -> String {
    let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ln.local_addr().unwrap().to_string();
    let b = b.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match ln.accept().await {
                Ok(x) => x,
                Err(_) => return,
            };
            let b = b.clone();
            tokio::spawn(async move {
                // reuse the admin HTTP plumbing via the public serve helper
                let _ = mqtt_broker::broker::admin::test_serve_conn(stream, b).await;
            });
        }
    });
    addr
}

async fn http_request(
    addr: &str,
    token: &str,
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
) -> (u16, serde_json::Value) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut req = format!("{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n", method, path, addr);
    if let Some(b) = body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", b.to_string().len()));
    }
    if !token.is_empty() {
        req.push_str(&format!("Authorization: Bearer {}\r\n", token));
    }
    req.push_str("\r\n");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(req.as_bytes()).await.unwrap();
    if let Some(b) = body {
        stream.write_all(b.to_string().as_bytes()).await.unwrap();
    }
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = stream.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let text = String::from_utf8_lossy(&buf);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_start = text.find("\r\n\r\n").map(|p| p + 4).unwrap_or(text.len());
    let json: serde_json::Value = serde_json::from_str(text[body_start..].trim())
        .unwrap_or(serde_json::Value::Null);
    (status, json)
}

async fn wait_tcp_addr(b: &Arc<Broker>) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let a = b.addr();
        if !a.is_empty() && a.contains(':') && !a.ends_with(":0") {
            return a;
        }
        assert!(tokio::time::Instant::now() < deadline, "tcp listener never bound");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_clients(b: &Arc<Broker>, want: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if b.client_count() == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("clients: want {}, got {}", want, b.client_count());
}

// ---------------------------------------------------------------------------
// 鉴权
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_admin_auth_required() {
    let (_b, addr) = new_admin_broker(Config { admin_token: "s3cret".into(), ..Default::default() }).await;

    // 无 token → 401
    let (status, _) = http_request(&addr, "", "GET", "/api/v1/info", None).await;
    assert_eq!(status, 401, "no token");
    // 错误 token → 401
    let (status, _) = http_request(&addr, "wrong", "GET", "/api/v1/info", None).await;
    assert_eq!(status, 401, "wrong token");
    // 正确 token → 200
    let (status, _) = http_request(&addr, "s3cret", "GET", "/api/v1/info", None).await;
    assert_eq!(status, 200, "right token");
}

#[tokio::test]
async fn test_admin_loopback_no_token() {
    // 未配置 token: 请求来自 127.0.0.1, 应放行
    let (_b, addr) = new_admin_broker(Config { admin_token: String::new(), ..Default::default() }).await;
    let (status, _) = http_request(&addr, "", "GET", "/api/v1/info", None).await;
    assert_eq!(status, 200, "loopback without token");
}

#[tokio::test]
async fn test_admin_x_token_header() {
    let (_b, addr) = new_admin_broker(Config { admin_token: "s3cret".into(), ..Default::default() }).await;
    // X-Admin-Token via raw request
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = "GET /api/v1/info HTTP/1.1\r\nHost: t\r\nX-Admin-Token: s3cret\r\nConnection: close\r\n\r\n";
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = String::new();
    stream.read_to_string(&mut buf).await.unwrap();
    assert!(buf.starts_with("HTTP/1.1 200"), "X-Admin-Token: got {}", &buf[..15.min(buf.len())]);
}

// ---------------------------------------------------------------------------
// 只读端点
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_admin_info() {
    let (_b, addr) = new_admin_broker(Config {
        admin_token: "s3cret".into(),
        redis_addr: "127.0.0.1:1".into(),
        ..Default::default()
    })
    .await;
    let (status, info) = http_request(&addr, "s3cret", "GET", "/api/v1/info", None).await;
    assert_eq!(status, 200, "info");
    assert_eq!(info["nodeId"], "admin-test", "nodeId");
    assert_eq!(info["version"], "1.2.3", "version");
    assert_eq!(info["mode"], "standalone", "mode");
    assert_eq!(info["redisAddr"], "127.0.0.1:1", "redisAddr");
}

#[tokio::test]
async fn test_admin_nodes_and_health() {
    let (_b, addr) = new_admin_broker(Config { admin_token: "s3cret".into(), ..Default::default() }).await;

    let (status, nodes) = http_request(&addr, "s3cret", "GET", "/api/v1/nodes", None).await;
    assert_eq!(status, 200, "nodes");
    assert_eq!(nodes["nodes"][0], "admin-test", "nodes");
    assert_eq!(nodes["nodes"].as_array().unwrap().len(), 1, "nodes len");

    let (status, _) = http_request(&addr, "s3cret", "GET", "/api/v1/health", None).await;
    assert_eq!(status, 200, "health");
}

// ---------------------------------------------------------------------------
// 发布 + retain 管理
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_admin_publish_and_retained() {
    let (_b, addr) = new_admin_broker(Config { admin_token: "s3cret".into(), ..Default::default() }).await;

    // 发布 retain 消息
    let (status, _) = http_request(
        &addr,
        "s3cret",
        "POST",
        "/api/v1/publish",
        Some(&serde_json::json!({"topic":"admin/t1","payload":"hello","qos":1,"retain":true})),
    )
    .await;
    assert_eq!(status, 200, "publish");

    // stats 应反映消息与 retain
    let (status, st) = http_request(&addr, "s3cret", "GET", "/api/v1/stats", None).await;
    assert_eq!(status, 200, "stats");
    assert!(st["messagesReceived"].as_i64().unwrap() >= 1, "messagesReceived: {}", st);
    assert!(st["retainedMessages"].as_i64().unwrap() >= 1, "retainedMessages: {}", st);
    assert_eq!(st["sessions"].as_i64().unwrap(), 0, "sessions");

    // retain 列表默认不含 payload
    let (status, retained) = http_request(&addr, "s3cret", "GET", "/api/v1/retained", None).await;
    assert_eq!(status, 200, "retained");
    assert_eq!(retained.as_array().unwrap().len(), 1, "retained len");
    assert_eq!(retained[0]["topic"], "admin/t1", "retained topic");
    assert_eq!(retained[0]["qos"], 1, "retained qos");
    assert_eq!(retained[0]["size"], 5, "retained size");
    assert!(retained[0].get("payloadB64").is_none(), "payload should be omitted by default");

    // with_payload=true 返回 base64
    let (_, retained) =
        http_request(&addr, "s3cret", "GET", "/api/v1/retained?with_payload=true", None).await;
    use base64::Engine;
    let expect = base64::engine::general_purpose::STANDARD.encode(b"hello");
    assert_eq!(retained[0]["payloadB64"], expect, "payloadB64");

    // 二进制发布 (payloadB64)
    let b64 = base64::engine::general_purpose::STANDARD.encode([0x00u8, 0xFF, 0x10]);
    let (status, _) = http_request(
        &addr,
        "s3cret",
        "POST",
        "/api/v1/publish",
        Some(&serde_json::json!({"topic":"admin/bin","payloadB64":b64,"retain":true})),
    )
    .await;
    assert_eq!(status, 200, "publish bin");

    // 删除单个 retain
    let (status, resp) =
        http_request(&addr, "s3cret", "DELETE", "/api/v1/retained?topic=admin%2Ft1", None).await;
    assert_eq!(status, 200, "delete retained");
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["topic"], "admin/t1");
    let (_, retained) = http_request(&addr, "s3cret", "GET", "/api/v1/retained", None).await;
    let arr = retained.as_array().unwrap();
    assert_eq!(arr.len(), 1, "after delete");
    assert_eq!(arr[0]["topic"], "admin/bin");

    // 清空全部
    let (status, resp) = http_request(&addr, "s3cret", "DELETE", "/api/v1/retained?all=true", None).await;
    assert_eq!(status, 200, "clear retained");
    assert_eq!(resp["deleted"].as_i64().unwrap(), 1, "deleted count");
    let (_, retained) = http_request(&addr, "s3cret", "GET", "/api/v1/retained", None).await;
    assert_eq!(retained.as_array().unwrap().len(), 0, "after clear");

    // 参数校验
    let (status, _) = http_request(
        &addr,
        "s3cret",
        "POST",
        "/api/v1/publish",
        Some(&serde_json::json!({"qos":3})),
    )
    .await;
    assert_eq!(status, 400, "qos=3");
    let (status, _) = http_request(&addr, "s3cret", "DELETE", "/api/v1/retained", None).await;
    assert_eq!(status, 400, "delete without topic");
    // missing topic
    let (status, err) = http_request(&addr, "s3cret", "POST", "/api/v1/publish", Some(&serde_json::json!({"payload":"x"}))).await;
    assert_eq!(status, 400, "missing topic");
    assert_eq!(err["error"], "missing topic");
    // invalid payloadB64
    let (status, err) = http_request(
        &addr,
        "s3cret",
        "POST",
        "/api/v1/publish",
        Some(&serde_json::json!({"topic":"t","payloadB64":"!!!"})),
    )
    .await;
    assert_eq!(status, 400, "invalid payloadB64");
    assert!(err["error"].as_str().unwrap().starts_with("invalid payloadB64"), "err: {}", err);
}

// ---------------------------------------------------------------------------
// 客户端 / 订阅 / 会话 (需要真实连接)
// ---------------------------------------------------------------------------

/// v3.1.1 CONNECT + optional SUBSCRIBE via raw TCP (mirrors connectTestClient).
async fn connect_test_client(addr: &str, client_id: &str, clean: bool) -> tokio::net::TcpStream {
    use tokio::io::AsyncReadExt;
    let conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    // CONNECT packet
    let mut vh = Vec::new();
    vh.extend_from_slice(&[0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04]); // MQTT, level 4
    let flags = if clean { 0x02u8 } else { 0x00 };
    vh.push(flags);
    vh.extend_from_slice(&30u16.to_be_bytes());
    vh.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    vh.extend_from_slice(client_id.as_bytes());
    let mut frame = vec![0x10]; // CONNECT
    mqtt_broker::codec::append_var_int(&mut frame, vh.len());
    frame.extend_from_slice(&vh);
    use tokio::io::AsyncWriteExt;
    let mut conn = conn;
    conn.write_all(&frame).await.unwrap();
    // read CONNACK
    let mut buf = [0u8; 16];
    let _ = conn.read(&mut buf).await.unwrap(); // CONNACK (variable length)
    conn
}

#[tokio::test]
async fn test_admin_clients_list_kick() {
    let mut cfg = Config { admin_token: "s3cret".into(), node_id: "admin-ln".into(), ..Default::default() };
    // real TCP listener on an ephemeral port
    cfg.tcp_addr = "127.0.0.1:0".into();
    let (b, addr) = new_admin_broker(cfg).await;
    // broker's real TCP listener isn't started by start_admin_server; start it
    let b2 = b.clone();
    tokio::spawn(async move {
        let _ = b2.start().await;
    });
    // wait for the listener; fetch bound port via broker addr
    let tcp_addr = wait_tcp_addr(&b).await;

    let mut conn = connect_test_client(&tcp_addr, "admin-c1", true).await;
    // 订阅 a/b
    let mut vh = vec![0x00, 0x02]; // packetID 2
    vh.extend_from_slice(&[0x00, 0x03, b'a', b'/', b'b', 0x01]);
    let mut frame = vec![0x82];
    mqtt_broker::codec::append_var_int(&mut frame, vh.len());
    frame.extend_from_slice(&vh);
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt};
    conn.write_all(&frame).await.unwrap();
    let mut buf = [0u8; 16];
    let _ = conn.read(&mut buf).await.unwrap(); // SUBACK
    wait_clients(&b, 1).await;
    let _ = addr;

    // 列表
    let (status, clients) = http_request(&addr_ref(&addr), "s3cret", "GET", "/api/v1/clients", None).await;
    assert_eq!(status, 200, "clients");
    let arr = clients.as_array().unwrap();
    assert_eq!(arr.len(), 1, "clients len: {}", clients);
    let c = &arr[0];
    assert_eq!(c["clientId"], "admin-c1", "clientId");
    assert_eq!(c["version"], "3.1.1", "version");
    assert_eq!(c["subscriptions"], 1, "subscriptions");
    assert!(c["remoteAddr"].as_str().unwrap().starts_with("127.0.0.1:"), "remoteAddr");

    // 详情
    let (status, _) = http_request(&addr_ref(&addr), "s3cret", "GET", "/api/v1/clients/admin-c1", None).await;
    assert_eq!(status, 200, "client detail");
    // 不存在 → 404
    let (status, err) = http_request(&addr_ref(&addr), "s3cret", "GET", "/api/v1/clients/nope", None).await;
    assert_eq!(status, 404, "missing client");
    assert_eq!(err["error"], "client \"nope\" not connected", "error text");

    // 订阅列表
    let (status, subs) = http_request(&addr_ref(&addr), "s3cret", "GET", "/api/v1/subscriptions", None).await;
    assert_eq!(status, 200, "subscriptions");
    let sarr = subs.as_array().unwrap();
    assert_eq!(sarr.len(), 1, "subs len");
    assert_eq!(sarr[0]["filter"], "a/b");
    assert_eq!(sarr[0]["clientId"], "admin-c1");
    assert_eq!(sarr[0]["qos"], 1);
    assert_eq!(sarr[0]["noLocal"], false);
    let (status, subs) =
        http_request(&addr_ref(&addr), "s3cret", "GET", "/api/v1/subscriptions/admin-c1", None).await;
    assert_eq!(status, 200);
    assert_eq!(subs.as_array().unwrap().len(), 1);

    // 踢下线
    let (status, resp) =
        http_request(&addr_ref(&addr), "s3cret", "DELETE", "/api/v1/clients/admin-c1", None).await;
    assert_eq!(status, 200, "kick");
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["clientId"], "admin-c1");
    wait_clients(&b, 0).await;
    // 再踢 → 404
    let (status, _) = http_request(&addr_ref(&addr), "s3cret", "DELETE", "/api/v1/clients/admin-c1", None).await;
    assert_eq!(status, 404, "kick missing");
    // clean=true 会话应已删除
    let (status, _) =
        http_request(&addr_ref(&addr), "s3cret", "GET", "/api/v1/sessions/admin-c1", None).await;
    assert_eq!(status, 404, "session after clean kick");
}

fn addr_ref(s: &str) -> String {
    s.to_string()
}

#[tokio::test]
async fn test_admin_sessions_persistent_delete() {
    let cfg = Config {
        admin_token: "s3cret".into(),
        node_id: "admin-ln".into(),
        tcp_addr: "127.0.0.1:0".into(),
        ..Default::default()
    };
    let (b, addr) = new_admin_broker(cfg).await;
    let b2 = b.clone();
    tokio::spawn(async move {
        let _ = b2.start().await;
    });
    let tcp_addr = wait_tcp_addr(&b).await;

    // 持久会话 (clean=false): 连接后断开, 会话保留
    {
        let mut conn = connect_test_client(&tcp_addr, "admin-p1", false).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        use tokio::io::AsyncWriteExt;
        // graceful DISCONNECT (v3: 2 bytes)
        let _ = conn.write_all(&[0xE0, 0x00]).await;
        let _ = conn.shutdown().await;
    }
    // wait for session to persist
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if b.session_count() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let (status, sessions) = http_request(&addr, "s3cret", "GET", "/api/v1/sessions", None).await;
    assert_eq!(status, 200, "sessions");
    let arr = sessions.as_array().unwrap();
    assert_eq!(arr.len(), 1, "sessions len: {}", sessions);
    assert_eq!(arr[0]["clientId"], "admin-p1");
    assert_eq!(arr[0]["connected"], false, "expected offline session");
    assert_eq!(arr[0]["sessionExpiry"], 0xFFFFFFFFu64 as f64, "expiry (never expire)");

    let (status, sess) = http_request(&addr, "s3cret", "GET", "/api/v1/sessions/admin-p1", None).await;
    assert_eq!(status, 200, "session detail");
    assert_eq!(sess["clientId"], "admin-p1");
    assert_eq!(sess["cleanStart"], false);
    assert_eq!(sess["keepAlive"], 30);

    // 删除会话
    let (status, resp) =
        http_request(&addr, "s3cret", "DELETE", "/api/v1/sessions/admin-p1", None).await;
    assert_eq!(status, 200, "delete session");
    assert_eq!(resp["ok"], true);
    let (status, _) = http_request(&addr, "s3cret", "GET", "/api/v1/sessions/admin-p1", None).await;
    assert_eq!(status, 404, "session after delete");
}

#[tokio::test]
async fn test_admin_delete_session_while_connected() {
    let cfg = Config {
        admin_token: "s3cret".into(),
        node_id: "admin-ln".into(),
        tcp_addr: "127.0.0.1:0".into(),
        ..Default::default()
    };
    let (b, addr) = new_admin_broker(cfg).await;
    let b2 = b.clone();
    tokio::spawn(async move {
        let _ = b2.start().await;
    });
    let tcp_addr = wait_tcp_addr(&b).await;

    // 持久会话客户端在线
    let _conn = connect_test_client(&tcp_addr, "admin-p2", false).await;
    wait_clients(&b, 1).await;

    // 删除在线会话: 连接被踢 + 会话被清
    let (status, _) = http_request(&addr, "s3cret", "DELETE", "/api/v1/sessions/admin-p2", None).await;
    assert_eq!(status, 200, "delete session");
    wait_clients(&b, 0).await;
    let (status, _) = http_request(&addr, "s3cret", "GET", "/api/v1/sessions/admin-p2", None).await;
    assert_eq!(status, 404, "session after delete");
    // 断开回调不应把会话写回 store (no resurrection)
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, _) = http_request(&addr, "s3cret", "GET", "/api/v1/sessions/admin-p2", None).await;
    assert_eq!(status, 404, "session resurrected");
}

#[tokio::test]
async fn test_admin_acl_reload_no_acl() {
    let (_b, addr) = new_admin_broker(Config { admin_token: "s3cret".into(), ..Default::default() }).await;
    let (status, err) = http_request(&addr, "s3cret", "POST", "/api/v1/acl/reload", None).await;
    assert_eq!(status, 400, "acl reload without FileACL");
    assert_eq!(err["error"], "no FileACL configured");
}

#[tokio::test]
async fn test_admin_unknown_path_and_method() {
    let (_b, addr) = new_admin_broker(Config { admin_token: "s3cret".into(), ..Default::default() }).await;
    // unknown path → 404
    let (status, _) = http_request(&addr, "s3cret", "GET", "/api/v1/nope", None).await;
    assert_eq!(status, 404, "unknown path");
    // method mismatch → 405
    let (status, _) = http_request(&addr, "s3cret", "DELETE", "/api/v1/info", None).await;
    assert_eq!(status, 405, "method mismatch");
}
