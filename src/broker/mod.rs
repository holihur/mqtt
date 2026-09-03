//! Broker core: config, construction, stats, embedded publish, health.
//! Port of `internal/broker/broker.go` + `embedded.go`.

pub mod admin;
mod conn;
mod lifecycle;
pub(crate) mod publish;
pub(crate) mod subscribe;
pub(crate) mod util;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use crate::auth::{self, Authenticator};
use crate::cluster::{Cluster, ClusterHandle, ClusterMeta};
use crate::codec::Packet;
use crate::persistence::{Message, Store};
use crate::session::Session;
use crate::topic::Trie;
use crate::transport::Conn;


// defaults mirroring `DefaultConfig` / `ApplyDefaults`
pub const DEF_MAX_PACKET_SIZE: usize = 1 << 20;
pub const DEF_MAX_CONNECTIONS: usize = 20000;
pub const DEF_MAX_PUBLISH_PER_SEC: i64 = 100;
pub const DEF_MAX_SUBSCRIBE_PER_SEC: i64 = 20;
pub const DEF_MAX_RETAINED_MESSAGES: usize = 10000;
pub const DEF_MAX_RETAINED_SIZE: i64 = 1 << 30;
pub const DEF_MAX_RETAIN_PER_TOPIC: usize = 1000;
pub const DEF_MAX_RETAIN_SIZE_PER_TOPIC: i64 = 100 << 20;
pub const DEF_MAX_INFLIGHT_WINDOW: u16 = 100;
pub const DEF_MAX_SUBSCRIPTIONS_PER_CLIENT: usize = 128;

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub node_id: String,
    pub tcp_addr: String,
    pub ws_addr: String,
    pub redis_addr: String,
    pub pprof_addr: String,
    pub admin_addr: String,
    pub admin_token: String,
    pub admin_tls: bool,
    pub webui_addr: String,
    pub acl_file: String,
    pub jwt_secret: String,
    pub max_packet_size: usize,
    pub allow_anonymous: bool,
    pub tls_cert_file: String,
    pub tls_key_file: String,
    pub tls_ca_file: String,
    pub max_connections: usize,
    pub max_publish_per_sec: i64,
    pub max_subscribe_per_sec: i64,
    pub max_retained_messages: usize,
    pub max_retained_size: i64,
    pub max_retain_per_topic: usize,
    pub max_retain_size_per_topic: i64,
    pub max_inflight_window: u16,
    pub max_subscriptions_per_client: usize,
    /// WAL dir is accepted for flag compatibility; the Rust port persists
    /// via the Store implementations (memory / redis).
    pub wal_dir: String,
    pub ws_allow_origins: Vec<String>,
}

impl Config {
    pub fn apply_defaults(&mut self) {
        if self.max_packet_size == 0 {
            self.max_packet_size = DEF_MAX_PACKET_SIZE;
        }
        if self.max_connections == 0 {
            self.max_connections = DEF_MAX_CONNECTIONS;
        }
        if self.max_publish_per_sec == 0 {
            self.max_publish_per_sec = DEF_MAX_PUBLISH_PER_SEC;
        }
        if self.max_subscribe_per_sec == 0 {
            self.max_subscribe_per_sec = DEF_MAX_SUBSCRIBE_PER_SEC;
        }
        if self.max_retained_messages == 0 {
            self.max_retained_messages = DEF_MAX_RETAINED_MESSAGES;
        }
        if self.max_retained_size == 0 {
            self.max_retained_size = DEF_MAX_RETAINED_SIZE;
        }
        if self.max_retain_per_topic == 0 {
            self.max_retain_per_topic = DEF_MAX_RETAIN_PER_TOPIC;
        }
        if self.max_inflight_window == 0 {
            self.max_inflight_window = DEF_MAX_INFLIGHT_WINDOW;
        }
        if self.max_subscriptions_per_client == 0 {
            self.max_subscriptions_per_client = DEF_MAX_SUBSCRIPTIONS_PER_CLIENT;
        }
        if self.max_retain_size_per_topic == 0 {
            self.max_retain_size_per_topic = DEF_MAX_RETAIN_SIZE_PER_TOPIC;
        }
    }
}

#[derive(Debug, Clone)]
pub struct BrokerStats {
    pub started_at: SystemTime,
    pub messages_received: i64,
    pub messages_sent: i64,
    pub clients_connected: i64,
    pub clients_total: i64,
}

#[derive(Default)]
pub(crate) struct ClientLimiter {
    pub publish_count: i64,
    pub subscribe_count: i64,
    pub window: Option<SystemTime>,
    pub last_seen: Option<SystemTime>,
}

#[derive(Clone)]
pub struct BrokerVersion {
    pub version: String,
    pub commit: String,
    pub date: String,
}

pub(crate) struct SharedSubs {
    pub subs: HashMap<String, HashMap<String, Vec<String>>>, // group -> filter -> clientIDs
    pub idx: HashMap<String, usize>,                         // group -> round-robin index
}

pub struct Broker {
    pub(crate) cfg: Config,
    pub(crate) store: Arc<dyn Store>,
    pub(crate) trie: Arc<Trie>,
    pub(crate) shared: Mutex<SharedSubs>,
    pub(crate) auth: Box<dyn Authenticator>,
    pub(crate) file_acls: Vec<Arc<auth::FileAcl>>,
    pub(crate) node_id: Mutex<String>,
    pub(crate) cluster: Mutex<Option<Arc<Cluster>>>,
    pub(crate) cluster_handle: Mutex<Option<ClusterHandle>>,
    pub(crate) conns: RwLock<HashMap<String, Arc<Conn>>>,
    pub(crate) sessions: RwLock<HashMap<String, Arc<Session>>>,
    pub(crate) started_at: Mutex<Option<SystemTime>>,
    pub(crate) messages_received: AtomicI64,
    pub(crate) messages_sent: AtomicI64,
    pub(crate) clients_total: AtomicI64,
    pub(crate) limiters: Mutex<HashMap<String, ClientLimiter>>,
    pub(crate) remote_tries: RwLock<HashMap<String, Arc<Trie>>>,
    pub(crate) version_info: Mutex<Option<BrokerVersion>>,
    pub(crate) running: AtomicBool,
    pub(crate) shutdown: tokio::sync::watch::Sender<bool>,
    pub(crate) tls_cfg: Mutex<Option<Arc<rustls::ServerConfig>>>,
    pub(crate) server_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub(crate) listener_addr: Arc<std::sync::Mutex<Option<String>>>,
}

impl Broker {
    /// Build a broker (mirrors `NewWithOptions` with defaults applied).
    pub fn new_with_options(
        mut cfg: Config,
        store: Option<Arc<dyn Store>>,
        authenticator: Option<Box<dyn Authenticator>>,
        version: Option<BrokerVersion>,
    ) -> Result<Arc<Self>, String> {
        cfg.apply_defaults();
        if cfg.node_id.is_empty() {
            cfg.node_id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
        }
        let node_id = cfg.node_id.clone();
        let (auth, file_acls) = match authenticator {
            Some(a) => (a, Vec::new()),
            None => {
                let ac = auth::AuthConfig {
                    acl_file: cfg.acl_file.clone(),
                    jwt_secret: cfg.jwt_secret.clone(),
                    allow_anonymous: cfg.allow_anonymous,
                };
                let (a, acls) = auth::build_authenticator(&ac)?;
                (Box::new(a) as Box<dyn Authenticator>, acls)
            }
        };
        let store = match store {
            Some(s) => s,
            None => Arc::new(crate::persistence::MemoryStore::new()),
        };
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Ok(Arc::new(Self {
            cfg,
            store,
            trie: Trie::new(),
            shared: Mutex::new(SharedSubs { subs: HashMap::new(), idx: HashMap::new() }),
            auth,
            file_acls,
            node_id: Mutex::new(node_id),
            cluster: Mutex::new(None),
            cluster_handle: Mutex::new(None),
            conns: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            started_at: Mutex::new(None),
            messages_received: AtomicI64::new(0),
            messages_sent: AtomicI64::new(0),
            clients_total: AtomicI64::new(0),
            limiters: Mutex::new(HashMap::new()),
            remote_tries: RwLock::new(HashMap::new()),
            version_info: Mutex::new(version),
            running: AtomicBool::new(false),
            shutdown: shutdown_tx,
            tls_cfg: Mutex::new(None),
            server_tasks: Mutex::new(Vec::new()),
            listener_addr: Arc::new(std::sync::Mutex::new(None)),
        }))
    }

    pub fn new(
        cfg: Config,
        store: Option<Arc<dyn Store>>,
        authenticator: Option<Box<dyn Authenticator>>,
    ) -> Result<Arc<Self>, String> {
        Self::new_with_options(cfg, store, authenticator, None)
    }

    pub fn node_id(&self) -> String {
        self.node_id.lock().unwrap().clone()
    }

    pub fn client_count(&self) -> usize {
        self.conns.read().unwrap().len()
    }

    pub fn session_count(&self) -> usize {
        self.sessions.read().unwrap().len()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn stats(&self) -> BrokerStats {
        let started_at = self.started_at.lock().unwrap().clone().unwrap_or(SystemTime::UNIX_EPOCH);
        BrokerStats {
            started_at,
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            clients_connected: self.client_count() as i64,
            clients_total: self.clients_total.load(Ordering::Relaxed),
        }
    }

    /// Health check: Redis ping + connection water level (mirrors `Health`).
    pub async fn health(&self) -> Result<(), String> {
        if self.cluster.lock().unwrap().is_some() {
            if let Err(e) = tokio::time::timeout(Duration::from_millis(50), self.store.ping()).await {
                return Err(format!("redis unavailable: {}", e));
            }
        }
        let n = self.client_count();
        if n > 16000 {
            return Err(format!("too many connections: {}", n));
        }
        Ok(())
    }

    /// Embedded publish path: local trie delivery + cluster broadcast +
    /// retain handling (mirrors `Broker.Publish`).
    pub async fn publish(self: &Arc<Self>, topic: &str, payload: &[u8], qos: u8, retain: bool) -> Result<(), String> {
        if topic.is_empty() {
            return Err("topic empty".into());
        }
        if topic.len() > 4096 {
            return Err("topic too long".into());
        }
        if retain {
            if payload.is_empty() {
                if let Err(e) = self.store.delete_retained(topic).await {
                    tracing::warn!("store DeleteRetained failed: {}", e);
                }
            } else {
                if let (true, reason) = self.check_retain_quota(topic, payload).await {
                    tracing::warn!("retain quota exceeded: reason={} topic={} client=embedded", reason, topic);
                    metrics().retain_quota_exceeded.inc(&reason);
                    metrics().packet_dropped.inc("retain_quota");
                    return Err(format!("retain quota exceeded: {}", reason));
                }
                let msg = Message {
                    topic: topic.to_string(),
                    payload: payload.to_vec(),
                    qos,
                    retain: true,
                    ..Default::default()
                };
                if let Err(e) = self.store.save_retained(topic, &msg).await {
                    tracing::warn!("store SaveRetained failed: {}", e);
                }
            }
        }
        self.route_message(topic, payload, qos, retain, None, "embedded").await;
        Ok(())
    }

    pub(crate) async fn check_retain_quota(&self, topic: &str, payload: &[u8]) -> (bool, String) {
        let stats = match self.store.get_retained_stats().await {
            Ok(s) => s,
            Err(e) => {
                // fail closed
                tracing::warn!("GetRetainedStats failed, denying retain write: {}", e);
                return (true, "stats_unavailable".into());
            }
        };
        let new_size = (topic.len() + payload.len() + 10) as i64;
        let (existing_size, exists) = match stats.topic_stats.get(topic) {
            Some(ts) => (ts.size, true),
            None => (0i64, false),
        };
        let total_count_after = stats.total_messages + if exists { 0 } else { 1 };
        let total_size_after = stats.total_size - existing_size + new_size;
        if total_count_after as usize > self.cfg.max_retained_messages {
            return (true, "global_count".into());
        }
        if total_size_after > self.cfg.max_retained_size {
            return (true, "global_size".into());
        }
        if self.cfg.max_retain_per_topic < 1 {
            return (true, "per_topic_count".into());
        }
        if new_size > self.cfg.max_retain_size_per_topic {
            return (true, "per_topic_size".into());
        }
        (false, String::new())
    }

    /// Route a message: cluster broadcast (if remote subscribers) + local delivery.
    pub(crate) async fn route_message(
        self: &Arc<Self>,
        topic_name: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
        props: Option<&crate::codec::Properties>,
        from: &str,
    ) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        metrics().messages_received.inc();
        if self.cfg.max_packet_size > 0 && payload.len() + topic_name.len() > self.cfg.max_packet_size {
            return;
        }
        if self.cluster.lock().unwrap().is_some() && self.has_remote_subscribers(topic_name) {
            let cluster = self.cluster.lock().unwrap().clone();
            if let Some(cluster) = cluster {
                let topic = topic_name.to_string();
                let payload = payload.to_vec();
                tokio::spawn(async move {
                    if let Err(e) = cluster.publish(&topic, &payload, qos, retain).await {
                        tracing::warn!("cluster publish failed: {}", e);
                    }
                });
            }
        }
        publish::deliver_local(self, topic_name, payload, qos, props, from).await;
    }

    pub(crate) fn has_remote_subscribers(&self, topic: &str) -> bool {
        let rt = self.remote_tries.read().unwrap();
        if rt.is_empty() {
            return true;
        }
        rt.values().any(|t| !t.match_topic(topic).is_empty())
    }

    pub(crate) fn add_remote_sub(&self, node_id: &str, filter: &str) {
        let mut rt = self.remote_tries.write().unwrap();
        let trie = rt.entry(node_id.to_string()).or_insert_with(Trie::new);
        trie.add(filter, node_id, 0, false);
    }

    pub(crate) fn remove_remote_sub(&self, node_id: &str, filter: &str) {
        let rt = self.remote_tries.write().unwrap();
        if let Some(trie) = rt.get(node_id) {
            trie.remove(filter, node_id);
        }
    }

    pub(crate) fn on_cluster_meta(&self, meta: &ClusterMeta) {
        match meta.action.as_str() {
            "sub" => {
                self.add_remote_sub(&meta.from, &meta.filter);
                tracing::debug!("remote sub node={} filter={}", meta.from, meta.filter);
            }
            "unsub" => {
                self.remove_remote_sub(&meta.from, &meta.filter);
                tracing::debug!("remote unsub node={} filter={}", meta.from, meta.filter);
            }
            _ => {}
        }
    }

    /// Encode + send a packet to a connection (mirrors `sendPacket`).
    pub(crate) async fn send_packet(&self, conn: &Conn, pkt: &Packet) -> Result<(), String> {
        let data = crate::codec::encode(pkt).map_err(|e| e.to_string())?;
        conn.write(&data).await.map_err(|e| e.to_string())
    }

    pub fn addr(&self) -> String {
        if let Some(a) = self.listener_addr.lock().unwrap().as_ref() {
            return a.clone();
        }
        self.cfg.tcp_addr.clone()
    }
}

pub(crate) fn metrics() -> &'static crate::metrics::Registry {
    crate::metrics::metrics()
}

