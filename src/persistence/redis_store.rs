//! Redis-backed Store. Port of `internal/persistence/redis.go`.
//! Keys: `mqtt:session:<id>`, `mqtt:retain:<topic>`, `mqtt:offline:<id>`,
//! `mqtt:pending-will:<id>`, `mqtt:pending-retry:<id>:<pid>`.

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

use super::*;
use crate::session::Session;

#[derive(Clone)]
pub struct RedisStore {
    conn: ConnectionManager,
    prefix: String,
}

impl RedisStore {
    pub fn new(addr: &str, prefix: &str) -> Result<Self, String> {
        let client = redis::Client::open(format!("redis://{}", addr)).map_err(|e| e.to_string())?;
        // ConnectionManager auto-reconnects; creation itself does not connect.
        // We construct lazily; a ping is performed by the caller at startup.
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| "RedisStore::new must be called within a tokio runtime".to_string())?;
        let conn = rt.block_on(async {
            let cm: ConnectionManager = ConnectionManager::new(client).await.map_err(|e| e.to_string())?;
            Ok::<ConnectionManager, String>(cm)
        })?;
        Ok(Self { conn, prefix: if prefix.is_empty() { "mqtt".into() } else { prefix.to_string() } })
    }

    pub fn with_manager(conn: ConnectionManager, prefix: &str) -> Self {
        Self { conn, prefix: if prefix.is_empty() { "mqtt".into() } else { prefix.to_string() } }
    }

    fn key(&self, parts: &[&str]) -> String {
        let mut s = self.prefix.clone();
        for p in parts {
            s.push(':');
            s.push_str(p);
        }
        s
    }

    fn session_key(&self, id: &str) -> String {
        self.key(&["session", id])
    }
    fn retain_key(&self, topic: &str) -> String {
        self.key(&["retain", topic])
    }
    fn offline_key(&self, id: &str) -> String {
        self.key(&["offline", id])
    }
    fn pending_will_key(&self, id: &str) -> String {
        self.key(&["pending-will", id])
    }
    fn pending_retry_key(&self, id: &str, pid: u16) -> String {
        self.key(&["pending-retry", &format!("{}:{}", id, pid)])
    }
}

#[async_trait]
impl Store for RedisStore {
    async fn get_session(&self, client_id: &str) -> Result<Option<Session>, String> {
        let mut conn = self.conn.clone();
        let data: Option<String> = conn.get(self.session_key(client_id)).await.map_err(|e| e.to_string())?;
        match data {
            None => Ok(None),
            Some(s) => {
                let v: serde_json::Value = serde_json::from_str(&s).map_err(|e| e.to_string())?;
                Ok(Session::from_store_json(&v))
            }
        }
    }

    async fn save_session(&self, s: &Session) -> Result<(), String> {
        let mut conn = self.conn.clone();
        let data = s.to_store_json().to_string();
        let ttl = {
            let expiry = *s.expiry_interval.lock().unwrap();
            if expiry != 0 && expiry != 0xFFFF_FFFF {
                std::time::Duration::from_secs(expiry as u64)
            } else {
                std::time::Duration::ZERO
            }
        };
        if ttl.is_zero() {
            conn.set::<_, _, ()>(self.session_key(&s.client_id), data).await.map_err(|e| e.to_string())
        } else {
            conn.set_ex::<_, _, ()>(self.session_key(&s.client_id), data, ttl.as_secs())
                .await
                .map_err(|e| e.to_string())
        }
    }

    async fn delete_session(&self, client_id: &str) -> Result<(), String> {
        let mut conn = self.conn.clone();
        conn.del::<_, ()>(self.session_key(client_id)).await.map_err(|e| e.to_string())
    }

    async fn get_retained(&self, topic: &str) -> Result<Option<Message>, String> {
        let mut conn = self.conn.clone();
        let data: Option<String> = conn.get(self.retain_key(topic)).await.map_err(|e| e.to_string())?;
        match data {
            None => Ok(None),
            Some(s) => serde_json::from_str(&s).map(Some).map_err(|e| e.to_string()),
        }
    }

    async fn save_retained(&self, topic: &str, msg: &Message) -> Result<(), String> {
        let mut conn = self.conn.clone();
        let data = serde_json::to_string(msg).map_err(|e| e.to_string())?;
        conn.set::<_, _, ()>(self.retain_key(topic), data).await.map_err(|e| e.to_string())
    }

    async fn delete_retained(&self, topic: &str) -> Result<(), String> {
        let mut conn = self.conn.clone();
        conn.del::<_, ()>(self.retain_key(topic)).await.map_err(|e| e.to_string())
    }

    async fn list_retained(&self) -> Result<Vec<Message>, String> {
        let mut conn = self.conn.clone();
        let pattern = self.key(&["retain", "*"]);
        let keys: Vec<String> = scan_keys(&mut conn, &pattern).await?;
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let vals: Vec<Option<String>> = conn.mget(&keys).await.map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for v in vals.into_iter().flatten() {
            if let Ok(m) = serde_json::from_str::<Message>(&v) {
                out.push(m);
            }
        }
        Ok(out)
    }

    async fn get_retained_stats(&self) -> Result<RetainStats, String> {
        let msgs = self.list_retained().await?;
        let mut stats = RetainStats { total_messages: msgs.len(), ..Default::default() };
        let mut total = 0i64;
        for m in &msgs {
            let sz = retained_size(m);
            total += sz;
            stats.topic_stats.insert(m.topic.clone(), TopicRetainStats { count: 1, size: sz });
        }
        stats.total_size = total;
        Ok(stats)
    }

    async fn enqueue_offline(&self, client_id: &str, msg: &Message) -> Result<(), String> {
        let mut conn = self.conn.clone();
        let data = serde_json::to_string(msg).map_err(|e| e.to_string())?;
        let k = self.offline_key(client_id);
        let _: () = conn.rpush(&k, data).await.map_err(|e| e.to_string())?;
        let _: () = conn.ltrim(&k, -1000, -1).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn dequeue_offline(&self, client_id: &str) -> Result<Vec<Message>, String> {
        let mut conn = self.conn.clone();
        let k = self.offline_key(client_id);
        let vals: Vec<String> = conn.lrange(&k, 0, -1).await.map_err(|e| e.to_string())?;
        let _: () = conn.del(&k).await.map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for v in vals {
            if let Ok(m) = serde_json::from_str::<Message>(&v) {
                out.push(m);
            }
        }
        Ok(out)
    }

    async fn clear_offline(&self, client_id: &str) -> Result<(), String> {
        let mut conn = self.conn.clone();
        conn.del::<_, ()>(self.offline_key(client_id)).await.map_err(|e| e.to_string())
    }

    async fn save_pending_will(&self, w: &PendingWill) -> Result<(), String> {
        let mut conn = self.conn.clone();
        let data = serde_json::to_string(w).map_err(|e| e.to_string())?;
        conn.set::<_, _, ()>(self.pending_will_key(&w.client_id), data).await.map_err(|e| e.to_string())
    }

    async fn delete_pending_will(&self, client_id: &str) -> Result<(), String> {
        let mut conn = self.conn.clone();
        conn.del::<_, ()>(self.pending_will_key(client_id)).await.map_err(|e| e.to_string())
    }

    async fn list_pending_wills(&self) -> Result<Vec<PendingWill>, String> {
        let mut conn = self.conn.clone();
        let keys = scan_keys(&mut conn, &self.key(&["pending-will", "*"])).await?;
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let vals: Vec<Option<String>> = conn.mget(&keys).await.map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for v in vals.into_iter().flatten() {
            if let Ok(w) = serde_json::from_str::<PendingWill>(&v) {
                out.push(w);
            }
        }
        Ok(out)
    }

    async fn save_pending_retry(&self, r: &PendingRetry) -> Result<(), String> {
        let mut conn = self.conn.clone();
        let data = serde_json::to_string(r).map_err(|e| e.to_string())?;
        conn.set::<_, _, ()>(self.pending_retry_key(&r.client_id, r.packet_id), data)
            .await
            .map_err(|e| e.to_string())
    }

    async fn delete_pending_retry(&self, client_id: &str, packet_id: u16) -> Result<(), String> {
        let mut conn = self.conn.clone();
        conn.del::<_, ()>(self.pending_retry_key(client_id, packet_id)).await.map_err(|e| e.to_string())
    }

    async fn list_pending_retries(&self) -> Result<Vec<PendingRetry>, String> {
        let mut conn = self.conn.clone();
        let keys = scan_keys(&mut conn, &self.key(&["pending-retry", "*"])).await?;
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let vals: Vec<Option<String>> = conn.mget(&keys).await.map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for v in vals.into_iter().flatten() {
            if let Ok(r) = serde_json::from_str::<PendingRetry>(&v) {
                out.push(r);
            }
        }
        Ok(out)
    }

    async fn close(&self) -> Result<(), String> {
        Ok(())
    }

    async fn ping(&self) -> Result<(), String> {
        let mut conn = self.conn.clone();
        redis::cmd("PING").query_async::<_, ()>(&mut conn).await.map_err(|e| e.to_string())
    }
}

/// SCAN with the given pattern (mirrors Go's SCAN iterator with count 0).
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
