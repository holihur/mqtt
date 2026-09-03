//! Broker lifecycle: start/stop, listeners, cluster wiring, metrics/admin
//! servers, sys ticker, ACL watcher, graceful shutdown.
//! Port of `internal/broker/broker_lifecycle.go`.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::codec::packet_type::*;
use crate::codec::Packet;
use crate::transport::{Conn, Listener};

use super::conn::{handle_raw_conn, restore_pending_retries, restore_pending_wills};
use super::util;
use super::publish;
use super::{metrics, Broker, Config};

impl Broker {
    /// Start the broker (blocking until shutdown). Mirrors `Broker.Start`.
    pub async fn start(self: &Arc<Self>) -> Result<(), String> {
        self.init_start().await?;
        let need_tcp = !self.cfg.tcp_addr.is_empty();
        let need_ws = !self.cfg.ws_addr.is_empty();
        if !need_tcp && !need_ws {
            tracing::info!("broker running in embedded mode without listeners: node={}", self.node_id());
            let mut rx = self.shutdown.subscribe();
            loop {
                if rx.changed().await.is_err() {
                    return Ok(());
                }
                if *rx.borrow() {
                    return Ok(());
                }
            }
        }
        let tls_cfg = self.tls_cfg.lock().unwrap().clone();
        let tls_enabled = tls_cfg.is_some();
        let mut listener = Listener::new(&self.cfg.tcp_addr, tls_cfg, &self.cfg.ws_addr);
        listener.bound_addr = self.listener_addr.clone();
        listener.set_ws_allow_origins(&self.cfg.ws_allow_origins);
        listener.set_max_packet_size(self.cfg.max_packet_size);
        tracing::info!(
            "broker listening: node={} tcp={} ws={} redis={} tls={}",
            self.node_id(), self.cfg.tcp_addr, self.cfg.ws_addr, self.cfg.redis_addr, tls_enabled
        );
        let shutdown_rx = self.shutdown.subscribe();
        let b2 = self.clone();
        let result = listener
            .listen(shutdown_rx, move |conn: Arc<Conn>| {
                let b = b2.clone();
                tokio::spawn(async move {
                    handle_raw_conn(&b, conn).await;
                });
            })
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    /// Start everything (admin API, cluster, background tasks) without
    /// blocking on listeners. Mirrors `StartAsync`/`initStart`.
    pub async fn init_start(self: &Arc<Self>) -> Result<(), String> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err("broker already running".into());
        }
        *self.started_at.lock().unwrap() = Some(util::system_now());

        // node id
        {
            let mut nid = self.node_id.lock().unwrap();
            if nid.is_empty() {
                *nid = self.cfg.node_id.clone();
            }
        }

        // TLS
        if self.cfg.tls_config_from_files() {
            match crate::transport::listener::load_tls_config(
                &self.cfg.tls_cert_file,
                &self.cfg.tls_key_file,
                &self.cfg.tls_ca_file,
            ) {
                Ok(cfg) => {
                    *self.tls_cfg.lock().unwrap() = Some(cfg);
                }
                Err(e) => tracing::warn!("tls config load failed: {}", e),
            }
        }

        // metrics / healthz / readyz server
        if !self.cfg.pprof_addr.is_empty() {
            let addr = self.cfg.pprof_addr.clone();
            let log_addr = addr.clone();
            let b2 = self.clone();
            let task = tokio::spawn(async move {
                if let Err(e) = super::admin::serve_metrics(&addr, b2).await {
                    tracing::warn!("metrics server error: {}", e);
                }
            });
            self.server_tasks.lock().unwrap().push(task);
            tracing::info!("metrics listening: addr={}", log_addr);
        }

        // admin API / webui
        if !self.cfg.admin_addr.is_empty() || !self.cfg.webui_addr.is_empty() {
            let b2 = self.clone();
            if !self.cfg.admin_addr.is_empty() {
                let addr = self.cfg.admin_addr.clone();
                let b3 = b2.clone();
                let task = tokio::spawn(async move {
                    if b3.cfg.admin_tls {
                        let tc = b3.tls_cfg.lock().unwrap().clone();
                        match tc {
                            Some(tc) => {
                                if let Err(e) = super::admin::serve_admin_tls(&addr, b3, tc).await {
                                    tracing::warn!("admin api server error: {}", e);
                                }
                                return;
                            }
                            None => {
                                tracing::warn!(
                                    "admin tls requested but no cert/key configured, serving plaintext: addr={}",
                                    addr
                                );
                            }
                        }
                    }
                    if let Err(e) = super::admin::serve_admin(&addr, b3).await {
                        tracing::warn!("admin api server error: {}", e);
                    }
                });
                self.server_tasks.lock().unwrap().push(task);
                tracing::info!(
                    "admin api listening: addr={} tokenSet={} tls={}",
                    self.cfg.admin_addr, !self.cfg.admin_token.is_empty(), self.cfg.admin_tls
                );
            }
            if !self.cfg.webui_addr.is_empty() {
                let addr = self.cfg.webui_addr.clone();
                let log_addr = addr.clone();
                let b3 = b2.clone();
                let task = tokio::spawn(async move {
                    if let Err(e) = super::admin::serve_webui(&addr, b3).await {
                        tracing::warn!("webui server error: {}", e);
                    }
                });
                self.server_tasks.lock().unwrap().push(task);
                tracing::info!(
                    "webui dashboard listening: addr={} tokenSet={}",
                    log_addr, !self.cfg.admin_token.is_empty()
                );
            }
        }

        // cluster
        self.ensure_cluster().await;

        // background tasks
        {
            let b2 = self.clone();
            tokio::spawn(async move { sys_ticker(b2).await });
        }
        {
            let b2 = self.clone();
            tokio::spawn(async move { limiter_janitor(b2).await });
        }
        if !self.file_acls.is_empty() {
            let b2 = self.clone();
            tokio::spawn(async move { watch_acl(b2).await });
        }
        {
            let b2 = self.clone();
            tokio::spawn(async move { restore_pending_wills(&b2).await });
        }
        {
            let b2 = self.clone();
            tokio::spawn(async move { restore_pending_retries(&b2).await });
        }
        Ok(())
    }

    /// Connect to Redis and start the cluster bus if configured.
    async fn ensure_cluster(self: &Arc<Self>) {
        if self.cfg.redis_addr.is_empty() {
            return;
        }
        let client = match redis::Client::open(format!("redis://{}", self.cfg.redis_addr)) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("redis client open failed, cluster disabled: {}", e);
                return;
            }
        };
        let conn: redis::aio::ConnectionManager = match redis::aio::ConnectionManager::new(client.clone()).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("redis connect failed, cluster disabled: {}", e);
                return;
            }
        };
        // ping with 2s budget
        let mut ping_conn = conn.clone();
        let ping_cmd = redis::cmd("PING");
        let ping = ping_cmd.query_async::<_, ()>(&mut ping_conn);
        if let Err(e) = tokio::time::timeout(Duration::from_secs(2), ping).await {
            tracing::warn!("redis ping failed, cluster disabled: {}", e);
            return;
        }

        let cluster = Arc::new(crate::cluster::Cluster::new(client, conn, &self.node_id(), "mqtt"));
        let (msg_tx, mut msg_rx) = tokio::sync::mpsc::channel::<crate::cluster::ClusterMessage>(4096);
        let (meta_tx, mut meta_rx) = tokio::sync::mpsc::channel::<crate::cluster::ClusterMeta>(4096);
        let handle = match cluster.start(msg_tx, meta_tx).await {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!("cluster start failed: {}", e);
                return;
            }
        };
        *self.cluster_handle.lock().unwrap() = Some(handle);
        *self.cluster.lock().unwrap() = Some(cluster);
        tracing::info!("cluster started: node={}", self.node_id());

        // message dispatch
        let b2 = self.clone();
        tokio::spawn(async move {
            while let Some(msg) = msg_rx.recv().await {
                if msg.topic.is_empty() || msg.topic.starts_with('$') {
                    continue;
                }
                publish::deliver_local(&b2, &msg.topic, &msg.payload, msg.qos, None, &msg.from).await;
            }
        });
        // meta dispatch
        let b3 = self.clone();
        tokio::spawn(async move {
            while let Some(meta) = meta_rx.recv().await {
                b3.on_cluster_meta(&meta);
            }
        });
    }

    /// Stop everything: cancel listeners/servers, remove node key, drain.
    pub async fn stop(self: &Arc<Self>) -> Result<(), String> {
        let was_running = self.running.load(Ordering::SeqCst);
        let _ = self.shutdown.send(true);
        {
            let tasks: Vec<_> = self.server_tasks.lock().unwrap().drain(..).collect();
            for t in tasks {
                t.abort();
            }
        }
        {
            let handle = self.cluster_handle.lock().unwrap().take();
            if let Some(h) = handle {
                h.stop().await;
            }
        }
        self.running.store(false, Ordering::SeqCst);
        if was_running {
            self.shutdown_drain(Duration::from_secs(30)).await?;
        }
        Ok(())
    }

    /// Graceful shutdown: DISCONNECT 0x8B broadcast + inflight drain.
    /// Mirrors `Broker.Shutdown` (30s ctx in main; here parameterized).
    pub async fn shutdown_drain(&self, ctx_timeout: Duration) -> Result<(), String> {
        tracing::info!("shutdown draining: node={}", self.node_id());
        let conns: Vec<Arc<Conn>> = {
            let c = self.conns.read().unwrap();
            c.values().cloned().collect()
        };
        for conn in &conns {
            let mut disc = Packet { ptype: DISCONNECT, version: conn.version(), ..Default::default() };
            if conn.version() == crate::codec::PROTOCOL_V5 {
                disc.disc_reason = 0x8B;
                disc.disc_props = Some(crate::codec::Properties {
                    reason_string: Some("Server shutting down".into()),
                    ..Default::default()
                });
            }
            let _ = self.send_packet(conn, &disc).await;
        }
        let drain = if conns.is_empty() { Duration::from_millis(500) } else { Duration::from_secs(5) };
        tokio::time::sleep(drain.min(ctx_timeout)).await;
        let deadline = tokio::time::Instant::now() + ctx_timeout;
        loop {
            let empty = {
                let sessions = self.sessions.read().unwrap();
                sessions.values().all(|s| s.inflight_count() == 0)
            };
            if empty {
                tracing::info!("shutdown complete: node={}", self.node_id());
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("context deadline exceeded".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Config {
    /// Whether TLS cert/key files are configured.
    pub(crate) fn tls_config_from_files(&self) -> bool {
        !self.tls_cert_file.is_empty()
    }
}

async fn sys_ticker(b: Arc<Broker>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let elapsed = b
            .started_at
            .lock().unwrap()
            .map(|t| t.elapsed().unwrap_or_default().as_secs_f64())
            .unwrap_or(0.0);
        let n = b.client_count() as i64;
        b.route_message("$SYS/broker/uptime", format!("{:.0}", elapsed).as_bytes(), 0, true, None, "sys")
            .await;
        b.route_message("$SYS/broker/clients/connected", format!("{}", n).as_bytes(), 0, true, None, "sys")
            .await;
    }
}

async fn limiter_janitor(b: Arc<Broker>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5 * 60));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let now = std::time::SystemTime::now();
        let mut limiters = b.limiters.lock().unwrap();
        limiters.retain(|_, lim| {
            let last = lim.last_seen.or(lim.window);
            match last {
                Some(l) => now.duration_since(l).unwrap_or_default() <= Duration::from_secs(10 * 60),
                None => false,
            }
        });
    }
}

async fn watch_acl(b: Arc<Broker>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        for facl in &b.file_acls {
            match facl.reload().await {
                Err(e) => tracing::warn!("acl reload failed: {}", e),
                Ok(true) => tracing::info!("acl reloaded: path={}", b.cfg.acl_file),
                Ok(false) => {}
            }
        }
    }
}

// metrics() used indirectly by handlers
#[allow(dead_code)]
fn _keep_metrics() -> &'static crate::metrics::Registry {
    metrics()
}
