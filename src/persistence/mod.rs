//! Persistence: Store trait + normalized message types.
//! Port of `internal/persistence/store.go`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::session::Session;

/// Normalized message for persistence.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    #[serde(rename = "Topic")]
    pub topic: String,
    #[serde(rename = "Payload")]
    pub payload: Vec<u8>,
    #[serde(rename = "QoS")]
    pub qos: u8,
    #[serde(rename = "Retain")]
    pub retain: bool,
    #[serde(rename = "From", default)]
    pub from: String,
    #[serde(rename = "CreatedAt", default)]
    pub created_at: i64, // unix millis for expiry calc
    #[serde(rename = "ExpiryInterval", default)]
    pub expiry_interval: u32, // 0 means no expiry
}

impl Message {
    pub fn is_expired(&self) -> bool {
        if self.expiry_interval == 0 || self.created_at == 0 {
            return false;
        }
        let expiry_time = self.created_at + (self.expiry_interval as i64) * 1000;
        expiry_time < now_millis()
    }
}

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetainStats {
    #[serde(rename = "TotalMessages")]
    pub total_messages: usize,
    #[serde(rename = "TotalSize")]
    pub total_size: i64,
    #[serde(rename = "TopicStats")]
    pub topic_stats: std::collections::HashMap<String, TopicRetainStats>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicRetainStats {
    #[serde(rename = "Count")]
    pub count: usize,
    #[serde(rename = "Size")]
    pub size: i64,
}

/// Retained size accounting: topic + payload + small overhead (consistent
/// with the Go MemoryStore and RedisStore implementations).
pub fn retained_size(msg: &Message) -> i64 {
    (msg.topic.len() + msg.payload.len() + 10) as i64
}

/// Records a delayed Will waiting to be delivered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWill {
    #[serde(rename = "clientID")]
    pub client_id: String,
    #[serde(rename = "topic")]
    pub topic: String,
    #[serde(rename = "payload")]
    pub payload: Vec<u8>,
    #[serde(rename = "qos")]
    pub qos: u8,
    #[serde(rename = "retain")]
    pub retain: bool,
    #[serde(rename = "deliverAt")]
    pub deliver_at: i64, // unix millis
}

/// Records a QoS retry waiting to be executed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingRetry {
    #[serde(rename = "clientID")]
    pub client_id: String,
    #[serde(rename = "packetID")]
    pub packet_id: u16,
    #[serde(rename = "topic")]
    pub topic: String,
    #[serde(rename = "payload")]
    pub payload: Vec<u8>,
    #[serde(rename = "qos")]
    pub qos: u8,
    #[serde(rename = "nextRetryAt")]
    pub next_retry_at: i64, // unix millis
    #[serde(rename = "retries")]
    pub retries: i32,
    #[serde(rename = "createdAt")]
    pub created_at: i64, // unix millis
}

pub fn retry_key(client_id: &str, packet_id: u16) -> String {
    format!("{}:{}", client_id, packet_id)
}

/// Persistence Store interface (async).
#[async_trait]
pub trait Store: Send + Sync {
    async fn get_session(&self, client_id: &str) -> Result<Option<Session>, String>;
    async fn save_session(&self, s: &Session) -> Result<(), String>;
    async fn delete_session(&self, client_id: &str) -> Result<(), String>;

    async fn get_retained(&self, topic: &str) -> Result<Option<Message>, String>;
    async fn save_retained(&self, topic: &str, msg: &Message) -> Result<(), String>;
    async fn delete_retained(&self, topic: &str) -> Result<(), String>;
    async fn list_retained(&self) -> Result<Vec<Message>, String>;
    async fn get_retained_stats(&self) -> Result<RetainStats, String>;

    async fn enqueue_offline(&self, client_id: &str, msg: &Message) -> Result<(), String>;
    async fn dequeue_offline(&self, client_id: &str) -> Result<Vec<Message>, String>;
    async fn clear_offline(&self, client_id: &str) -> Result<(), String>;

    async fn save_pending_will(&self, w: &PendingWill) -> Result<(), String>;
    async fn delete_pending_will(&self, client_id: &str) -> Result<(), String>;
    async fn list_pending_wills(&self) -> Result<Vec<PendingWill>, String>;

    async fn save_pending_retry(&self, r: &PendingRetry) -> Result<(), String>;
    async fn delete_pending_retry(&self, client_id: &str, packet_id: u16) -> Result<(), String>;
    async fn list_pending_retries(&self) -> Result<Vec<PendingRetry>, String>;

    async fn close(&self) -> Result<(), String>;

    /// Health probe (Redis ping etc.); default = always healthy.
    async fn ping(&self) -> Result<(), String> {
        Ok(())
    }
}

mod memory;
mod redis_store;

pub use memory::MemoryStore;
pub use redis_store::RedisStore;
