//! In-memory Store. Port of `internal/persistence/memory.go`.

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;

use super::*;
use crate::session::Session;

#[derive(Default)]
pub struct MemoryStore {
    inner: RwLock<MemInner>,
}

#[derive(Default)]
struct MemInner {
    /// Session snapshots serialized to JSON (decoupled from live objects,
    /// mirroring Go's store semantics).
    sessions: HashMap<String, serde_json::Value>,
    retained: HashMap<String, Message>,
    offline: HashMap<String, Vec<Message>>,
    pending_wills: HashMap<String, PendingWill>,
    pending_retries: HashMap<String, PendingRetry>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn get_session(&self, client_id: &str) -> Result<Option<Session>, String> {
        let g = self.inner.read().unwrap();
        Ok(g.sessions.get(client_id).and_then(|v| Session::from_store_json(v)))
    }

    async fn save_session(&self, s: &Session) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        g.sessions.insert(s.client_id.clone(), s.to_store_json());
        Ok(())
    }

    async fn delete_session(&self, client_id: &str) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        g.sessions.remove(client_id);
        Ok(())
    }

    async fn get_retained(&self, topic: &str) -> Result<Option<Message>, String> {
        let g = self.inner.read().unwrap();
        Ok(g.retained.get(topic).cloned())
    }

    async fn save_retained(&self, topic: &str, msg: &Message) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        g.retained.insert(topic.to_string(), msg.clone());
        Ok(())
    }

    async fn delete_retained(&self, topic: &str) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        g.retained.remove(topic);
        Ok(())
    }

    async fn list_retained(&self) -> Result<Vec<Message>, String> {
        let g = self.inner.read().unwrap();
        Ok(g.retained.values().cloned().collect())
    }

    async fn get_retained_stats(&self) -> Result<RetainStats, String> {
        let g = self.inner.read().unwrap();
        let mut stats = RetainStats { total_messages: g.retained.len(), ..Default::default() };
        let mut total = 0i64;
        for (topic, msg) in &g.retained {
            let sz = retained_size(msg);
            total += sz;
            stats.topic_stats.insert(topic.clone(), TopicRetainStats { count: 1, size: sz });
        }
        stats.total_size = total;
        Ok(stats)
    }

    async fn enqueue_offline(&self, client_id: &str, msg: &Message) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        let q = g.offline.entry(client_id.to_string()).or_default();
        q.push(msg.clone());
        // cap 1000 to prevent unbounded growth
        if q.len() > 1000 {
            let drain = q.len() - 1000;
            q.drain(..drain);
        }
        Ok(())
    }

    async fn dequeue_offline(&self, client_id: &str) -> Result<Vec<Message>, String> {
        let mut g = self.inner.write().unwrap();
        Ok(g.offline.remove(client_id).unwrap_or_default())
    }

    async fn clear_offline(&self, client_id: &str) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        g.offline.remove(client_id);
        Ok(())
    }

    async fn save_pending_will(&self, w: &PendingWill) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        g.pending_wills.insert(w.client_id.clone(), w.clone());
        Ok(())
    }

    async fn delete_pending_will(&self, client_id: &str) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        g.pending_wills.remove(client_id);
        Ok(())
    }

    async fn list_pending_wills(&self) -> Result<Vec<PendingWill>, String> {
        let g = self.inner.read().unwrap();
        Ok(g.pending_wills.values().cloned().collect())
    }

    async fn save_pending_retry(&self, r: &PendingRetry) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        g.pending_retries.insert(retry_key(&r.client_id, r.packet_id), r.clone());
        Ok(())
    }

    async fn delete_pending_retry(&self, client_id: &str, packet_id: u16) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        g.pending_retries.remove(&retry_key(client_id, packet_id));
        Ok(())
    }

    async fn list_pending_retries(&self) -> Result<Vec<PendingRetry>, String> {
        let g = self.inner.read().unwrap();
        Ok(g.pending_retries.values().cloned().collect())
    }

    async fn close(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retained_roundtrip() {
        let st = MemoryStore::new();
        st.save_retained("a/b", &Message { topic: "a/b".into(), payload: vec![1], qos: 1, retain: true, ..Default::default() })
            .await
            .unwrap();
        assert_eq!(st.list_retained().await.unwrap().len(), 1);
        let stats = st.get_retained_stats().await.unwrap();
        assert_eq!(stats.total_messages, 1);
        assert_eq!(stats.total_size, 1 + 3 + 10);
        st.delete_retained("a/b").await.unwrap();
        assert!(st.list_retained().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn offline_queue_cap() {
        let st = MemoryStore::new();
        for i in 0..1010 {
            st.enqueue_offline("c1", &Message { topic: format!("t/{}", i), payload: vec![], qos: 0, retain: false, ..Default::default() })
                .await
                .unwrap();
        }
        let msgs = st.dequeue_offline("c1").await.unwrap();
        assert_eq!(msgs.len(), 1000);
        assert_eq!(msgs[0].topic, "t/10"); // oldest trimmed
        assert!(st.dequeue_offline("c1").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn pending_wills_retries() {
        let st = MemoryStore::new();
        st.save_pending_will(&PendingWill { client_id: "c1".into(), topic: "t".into(), payload: vec![], qos: 0, retain: false, deliver_at: 5 })
            .await
            .unwrap();
        assert_eq!(st.list_pending_wills().await.unwrap().len(), 1);
        st.delete_pending_will("c1").await.unwrap();
        assert!(st.list_pending_wills().await.unwrap().is_empty());

        st.save_pending_retry(&PendingRetry { client_id: "c1".into(), packet_id: 2, topic: String::new(), payload: vec![], qos: 0, next_retry_at: 0, retries: 0, created_at: 0 })
            .await
            .unwrap();
        assert_eq!(st.list_pending_retries().await.unwrap().len(), 1);
        st.delete_pending_retry("c1", 2).await.unwrap();
        assert!(st.list_pending_retries().await.unwrap().is_empty());
    }
}
