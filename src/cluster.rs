//! Inter-broker routing via Redis PubSub. Port of `internal/cluster/cluster.go`.
//!
//! - heartbeat: `SET mqtt:nodes:<id> <unix> EX 15s` every 5s
//! - message bus: `PUBLISH mqtt:cluster` (JSON ClusterMessage)
//! - meta bus: `PUBLISH mqtt:cluster:meta` (JSON ClusterMeta, sub/unsub)

use futures_util::StreamExt;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMessage {
    #[serde(rename = "from")]
    pub from: String,
    #[serde(rename = "topic")]
    pub topic: String,
    #[serde(rename = "payload", with = "crate::session::serde_bytes_b64")]
    pub payload: Vec<u8>,
    #[serde(rename = "qos")]
    pub qos: u8,
    #[serde(rename = "retain")]
    pub retain: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMeta {
    #[serde(rename = "from")]
    pub from: String,
    #[serde(rename = "action")]
    pub action: String, // sub / unsub
    #[serde(rename = "filter")]
    pub filter: String,
}

pub struct Cluster {
    node_id: String,
    client: redis::Client,
    conn: ConnectionManager,
    prefix: String,
}

impl Cluster {
    pub fn new(client: redis::Client, conn: ConnectionManager, node_id: &str, prefix: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            client,
            conn,
            prefix: if prefix.is_empty() { "mqtt".into() } else { prefix.to_string() },
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Start heartbeat + pubsub listener. Returns a shutdown handle.
    pub async fn start(
        &self,
        on_msg: mpsc::Sender<ClusterMessage>,
        on_meta: mpsc::Sender<ClusterMeta>,
    ) -> Result<ClusterHandle, String> {
        let channel = format!("{}:cluster", self.prefix);
        let meta_channel = format!("{}:cluster:meta", self.prefix);

        // dedicated pubsub connection
        let mut pubsub = self.client.get_async_pubsub().await.map_err(|e| e.to_string())?;
        pubsub.subscribe(&channel).await.map_err(|e| e.to_string())?;
        pubsub.subscribe(&meta_channel).await.map_err(|e| e.to_string())?;

        // heartbeat task
        let hb = {
            let conn = self.conn.clone();
            let key = format!("{}:nodes:{}", self.prefix, self.node_id);
            tokio::spawn(async move {
                let mut conn = conn;
                let _: Result<(), _> = redis::cmd("SET")
                    .arg(&key)
                    .arg(crate::auth::now_unix() as i64)
                    .arg("EX")
                    .arg(15usize)
                    .query_async(&mut conn)
                    .await;
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    let _: Result<(), _> = redis::cmd("SET")
                        .arg(&key)
                        .arg(crate::auth::now_unix() as i64)
                        .arg("EX")
                        .arg(15usize)
                        .query_async(&mut conn)
                        .await;
                }
            })
        };

        // pubsub dispatch task
        let node_id = self.node_id.clone();
        let dispatch = tokio::spawn(async move {
            let mut stream = pubsub.into_on_message();
            while let Some(msg) = stream.next().await {
                let ch = msg.get_channel_name().to_string();
                let payload: String = msg.get_payload().unwrap_or_default();
                if ch == meta_channel_ref(&meta_channel) || ch == meta_channel {
                    if let Ok(meta) = serde_json::from_str::<ClusterMeta>(&payload) {
                        if meta.from == node_id {
                            continue;
                        }
                        if on_meta.send(meta).await.is_err() {
                            break;
                        }
                    }
                    continue;
                }
                if let Ok(cm) = serde_json::from_str::<ClusterMessage>(&payload) {
                    if cm.from == node_id {
                        continue;
                    }
                    if on_msg.send(cm).await.is_err() {
                        break;
                    }
                }
            }
        });

        Ok(ClusterHandle { hb: Some(hb), dispatch: Some(dispatch), conn: self.conn.clone(), node_key: format!("{}:nodes:{}", self.prefix, self.node_id) })
    }

    /// Broadcast a message to the cluster.
    pub async fn publish(&self, topic: &str, payload: &[u8], qos: u8, retain: bool) -> Result<(), String> {
        let cm = ClusterMessage {
            from: self.node_id.clone(),
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos,
            retain,
        };
        let data = serde_json::to_string(&cm).map_err(|e| e.to_string())?;
        let mut conn = self.conn.clone();
        let _: () = redis::cmd("PUBLISH")
            .arg(format!("{}:cluster", self.prefix))
            .arg(data)
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Broadcast subscription meta (sub/unsub).
    pub async fn publish_meta(&self, action: &str, filter: &str) -> Result<(), String> {
        let meta = ClusterMeta { from: self.node_id.clone(), action: action.to_string(), filter: filter.to_string() };
        let data = serde_json::to_string(&meta).map_err(|e| e.to_string())?;
        let mut conn = self.conn.clone();
        let _: () = redis::cmd("PUBLISH")
            .arg(format!("{}:cluster:meta", self.prefix))
            .arg(data)
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// List live nodes via SCAN on `mqtt:nodes:*`.
    pub async fn nodes(&self) -> Result<Vec<String>, String> {
        let mut conn = self.conn.clone();
        let pattern = format!("{}:nodes:*", self.prefix);
        let p = format!("{}:nodes:", self.prefix);
        let keys = scan_keys(&mut conn, &pattern).await?;
        Ok(keys.iter().filter_map(|k| k.strip_prefix(&p).map(|s| s.to_string())).collect())
    }
}

fn meta_channel_ref(s: &str) -> String {
    s.to_string()
}

async fn scan_keys(conn: &mut ConnectionManager, pattern: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(100usize)
            .query_async(conn)
            .await
            .map_err(|e| e.to_string())?;
        out.extend(batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(out)
}

/// Handle controlling cluster background tasks.
pub struct ClusterHandle {
    hb: Option<tokio::task::JoinHandle<()>>,
    dispatch: Option<tokio::task::JoinHandle<()>>,
    conn: ConnectionManager,
    node_key: String,
}

impl ClusterHandle {
    /// Stop tasks and remove our node key (TTL 15s would also expire it).
    pub async fn stop(mut self) {
        if let Some(h) = self.hb.take() {
            h.abort();
        }
        if let Some(h) = self.dispatch.take() {
            h.abort();
        }
        let mut conn = self.conn.clone();
        let _: Result<(), _> = redis::cmd("DEL").arg(&self.node_key).query_async(&mut conn).await;
    }
}
