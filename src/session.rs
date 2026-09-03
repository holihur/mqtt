//! Session state. Port of `internal/session/session.go`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

pub const DEFAULT_RECEIVE_MAXIMUM: u16 = 65535;
pub const DEFAULT_MAX_PACKET_SIZE: u32 = 1 << 20;
pub const MAX_TOPIC_ALIAS: u16 = 100;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    #[serde(default)]
    pub delay_interval: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InflightEntry {
    #[serde(rename = "PacketID")]
    pub packet_id: u16,
    #[serde(rename = "QoS")]
    pub qos: u8,
    #[serde(rename = "Topic")]
    pub topic: String,
    #[serde(rename = "Payload", with = "serde_bytes_b64")]
    pub payload: Vec<u8>,
    #[serde(rename = "State", default)]
    pub state: String, // "qos1-pending", "qos2-publish", "qos2-pubrel"
    #[serde(rename = "CreatedAt", default)]
    pub created_at: i64, // unix millis
    #[serde(rename = "Dup", default)]
    pub dup: bool,
}

/// Helper module: base64 encode/decode of byte payloads for JSON
/// (Go serializes []byte as base64 strings in JSON).
pub mod serde_bytes_b64 {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Default)]
struct SessionState {
    connected: bool,
    will: Option<Will>,
    username: String,
    subscriptions: HashMap<String, u8>, // filter -> QoS
    inflight: HashMap<u16, InflightEntry>,
    next_id: u16,
    free_ids: Vec<u16>,
    alias_to_topic: HashMap<u16, String>,
    topic_to_alias: HashMap<String, u16>,
    deleted: bool,
}

/// Session with interior mutability (like Go's `Mu sync.Mutex` guarded fields).
/// Short critical sections only — never hold across `.await`.
#[derive(Debug)]
pub struct Session {
    pub client_id: String,
    pub version: Mutex<u8>,
    pub clean_start: Mutex<bool>,
    pub expiry_interval: Mutex<u32>, // seconds, 0=expire on disconnect, 0xFFFFFFFF = never expire
    pub keep_alive: Mutex<u16>,
    pub created_at: Mutex<SystemTime>,
    pub node_id: Mutex<String>,
    pub receive_maximum: Mutex<u16>,
    pub maximum_packet_size: Mutex<u32>,
    pub topic_alias_maximum: Mutex<u16>,
    state: Mutex<SessionState>,
}

impl Session {
    pub fn new(client_id: &str, version: u8, clean_start: bool, expiry: u32) -> Self {
        Self {
            client_id: client_id.to_string(),
            version: Mutex::new(version),
            clean_start: Mutex::new(clean_start),
            expiry_interval: Mutex::new(expiry),
            keep_alive: Mutex::new(0),
            created_at: Mutex::new(SystemTime::now()),
            node_id: Mutex::new(String::new()),
            receive_maximum: Mutex::new(DEFAULT_RECEIVE_MAXIMUM),
            maximum_packet_size: Mutex::new(DEFAULT_MAX_PACKET_SIZE),
            topic_alias_maximum: Mutex::new(0),
            state: Mutex::new(SessionState { next_id: 1, ..Default::default() }),
        }
    }

    pub fn next_packet_id(&self) -> u16 {
        let mut st = self.state.lock().unwrap();
        if !st.free_ids.is_empty() {
            let id = st.free_ids.pop().unwrap();
            if !st.inflight.contains_key(&id) {
                return id;
            }
        }
        for _ in 0..65535 {
            let id = st.next_id;
            st.next_id = st.next_id.wrapping_add(1);
            if st.next_id == 0 {
                st.next_id = 1;
            }
            if !st.inflight.contains_key(&id) {
                return id;
            }
        }
        0 // exhausted
    }

    pub fn add_inflight(&self, e: InflightEntry) {
        let mut st = self.state.lock().unwrap();
        st.inflight.insert(e.packet_id, e);
    }

    pub fn set_subscription(&self, filter: &str, qos: u8) {
        let mut st = self.state.lock().unwrap();
        st.subscriptions.insert(filter.to_string(), qos);
    }

    pub fn delete_subscription(&self, filter: &str) {
        let mut st = self.state.lock().unwrap();
        st.subscriptions.remove(filter);
    }

    pub fn get_subscription(&self, filter: &str) -> Option<u8> {
        let st = self.state.lock().unwrap();
        st.subscriptions.get(filter).copied()
    }

    pub fn subscriptions_snapshot(&self) -> HashMap<String, u8> {
        let st = self.state.lock().unwrap();
        st.subscriptions.clone()
    }

    pub fn remove_inflight(&self, id: u16) {
        let mut st = self.state.lock().unwrap();
        st.inflight.remove(&id);
        if st.free_ids.len() < 1024 {
            st.free_ids.push(id);
        }
    }

    pub fn get_inflight(&self, id: u16) -> Option<InflightEntry> {
        let st = self.state.lock().unwrap();
        st.inflight.get(&id).cloned()
    }

    pub fn inflight_count(&self) -> usize {
        let st = self.state.lock().unwrap();
        st.inflight.len()
    }

    pub fn can_send(&self) -> bool {
        let st = self.state.lock().unwrap();
        (st.inflight.len() as u16) < *self.receive_maximum.lock().unwrap()
    }

    // -- convenience field accessors (mirror Go's guarded field access) --

    pub fn is_connected(&self) -> bool {
        self.state.lock().unwrap().connected
    }
    pub fn set_connected(&self, v: bool) {
        self.state.lock().unwrap().connected = v;
    }
    pub fn username(&self) -> String {
        self.state.lock().unwrap().username.clone()
    }
    pub fn set_username(&self, v: &str) {
        self.state.lock().unwrap().username = v.to_string();
    }
    pub fn get_will(&self) -> Option<Will> {
        self.state.lock().unwrap().will.clone()
    }
    pub fn take_will(&self) -> Option<Will> {
        let mut st = self.state.lock().unwrap();
        st.will.take()
    }
    pub fn set_will(&self, w: Option<Will>) {
        self.state.lock().unwrap().will = w;
    }
    pub fn is_deleted(&self) -> bool {
        self.state.lock().unwrap().deleted
    }
    pub fn set_deleted(&self, v: bool) {
        self.state.lock().unwrap().deleted = v;
    }
    pub fn clear_subscriptions_and_inflight(&self) {
        let mut st = self.state.lock().unwrap();
        st.subscriptions.clear();
        st.inflight.clear();
    }
    pub fn subscription_filters(&self) -> Vec<String> {
        let st = self.state.lock().unwrap();
        st.subscriptions.keys().cloned().collect()
    }
    pub fn subscription_count(&self) -> usize {
        let st = self.state.lock().unwrap();
        st.subscriptions.len()
    }
    pub fn inflight_len(&self) -> usize {
        let st = self.state.lock().unwrap();
        st.inflight.len()
    }

    // Topic alias maps (v5)
    pub fn alias_to_topic_insert(&self, alias: u16, topic: &str) {
        let mut st = self.state.lock().unwrap();
        st.alias_to_topic.insert(alias, topic.to_string());
        st.topic_to_alias.insert(topic.to_string(), alias);
    }
    pub fn alias_to_topic_len(&self) -> usize {
        let st = self.state.lock().unwrap();
        st.alias_to_topic.len()
    }
    pub fn alias_to_topic_get(&self, alias: u16) -> Option<String> {
        let st = self.state.lock().unwrap();
        st.alias_to_topic.get(&alias).cloned()
    }

    /// Serialize the session for persistence (mirrors Go's json.Marshal fields).
    pub fn to_store_json(&self) -> serde_json::Value {
        let st = self.state.lock().unwrap();
        serde_json::json!({
            "ClientID": self.client_id,
            "Version": *self.version.lock().unwrap(),
            "CleanStart": *self.clean_start.lock().unwrap(),
            "ExpiryInterval": *self.expiry_interval.lock().unwrap(),
            "KeepAlive": *self.keep_alive.lock().unwrap(),
            "Will": st.will,
            "Username": st.username,
            "CreatedAt": chrono_to_go(*self.created_at.lock().unwrap()),
            "Connected": st.connected,
            "NodeID": *self.node_id.lock().unwrap(),
            "Subscriptions": st.subscriptions,
            "Inflight": st.inflight,
            "NextID": st.next_id,
            "ReceiveMaximum": *self.receive_maximum.lock().unwrap(),
            "MaximumPacketSize": *self.maximum_packet_size.lock().unwrap(),
            "TopicAliasMaximum": *self.topic_alias_maximum.lock().unwrap(),
            "AliasToTopic": st.alias_to_topic,
            "TopicToAlias": st.topic_to_alias,
            "Deleted": st.deleted,
        })
    }

    pub fn from_store_json(v: &serde_json::Value) -> Option<Self> {
        let s = Session::new(
            v.get("ClientID").and_then(|x| x.as_str()).unwrap_or(""),
            v.get("Version").and_then(|x| x.as_u64()).unwrap_or(4) as u8,
            v.get("CleanStart").and_then(|x| x.as_bool()).unwrap_or(false),
            v.get("ExpiryInterval").and_then(|x| x.as_u64()).unwrap_or(0) as u32,
        );
        if let Some(x) = v.get("KeepAlive") {
            *s.keep_alive.lock().unwrap() = x.as_u64().unwrap_or(0) as u16;
        }
        if let Some(x) = v.get("Username") {
            s.state.lock().unwrap().username = x.as_str().unwrap_or("").to_string();
        }
        if let Some(x) = v.get("Connected") {
            s.state.lock().unwrap().connected = x.as_bool().unwrap_or(false);
        }
        if let Some(x) = v.get("NodeID") {
            *s.node_id.lock().unwrap() = x.as_str().unwrap_or("").to_string();
        }
        if let Some(x) = v.get("Will") {
            if let Ok(w) = serde_json::from_value::<Will>(x.clone()) {
                s.state.lock().unwrap().will = Some(w);
            }
        }
        if let Some(x) = v.get("Subscriptions") {
            if let Ok(m) = serde_json::from_value::<HashMap<String, u8>>(x.clone()) {
                s.state.lock().unwrap().subscriptions = m;
            }
        }
        if let Some(x) = v.get("Inflight") {
            if let Ok(m) = serde_json::from_value::<HashMap<u16, InflightEntry>>(x.clone()) {
                s.state.lock().unwrap().inflight = m;
            }
        }
        if let Some(x) = v.get("NextID") {
            s.state.lock().unwrap().next_id = x.as_u64().unwrap_or(1) as u16;
        }
        if let Some(x) = v.get("ReceiveMaximum") {
            *s.receive_maximum.lock().unwrap() = x.as_u64().unwrap_or(DEFAULT_RECEIVE_MAXIMUM as u64) as u16;
        }
        if let Some(x) = v.get("MaximumPacketSize") {
            *s.maximum_packet_size.lock().unwrap() = x.as_u64().unwrap_or(DEFAULT_MAX_PACKET_SIZE as u64) as u32;
        }
        if let Some(x) = v.get("TopicAliasMaximum") {
            *s.topic_alias_maximum.lock().unwrap() = x.as_u64().unwrap_or(0) as u16;
        }
        if let Some(x) = v.get("AliasToTopic") {
            if let Ok(m) = serde_json::from_value::<HashMap<u16, String>>(x.clone()) {
                s.state.lock().unwrap().alias_to_topic = m;
            }
        }
        if let Some(x) = v.get("TopicToAlias") {
            if let Ok(m) = serde_json::from_value::<HashMap<String, u16>>(x.clone()) {
                s.state.lock().unwrap().topic_to_alias = m;
            }
        }
        if let Some(x) = v.get("Deleted") {
            s.state.lock().unwrap().deleted = x.as_bool().unwrap_or(false);
        }
        Some(s)
    }
}

pub fn chrono_to_go(t: SystemTime) -> String {
    crate::broker::util::go_rfc3339(&t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_ids() {
        let s = Session::new("c1", 4, true, 0);
        let a = s.next_packet_id();
        let b = s.next_packet_id();
        assert_eq!(a, 1);
        assert_eq!(b, 2);
        s.add_inflight(InflightEntry { packet_id: a, qos: 1, ..Default::default() });
        s.remove_inflight(a);
        // freed ID is reused
        let c = s.next_packet_id();
        assert_eq!(c, a);
    }

    #[test]
    fn packet_id_wrap() {
        let s = Session::new("c1", 4, true, 0);
        {
            let mut st = s.state.lock().unwrap();
            st.next_id = 65535;
        }
        let a = s.next_packet_id();
        assert_eq!(a, 65535);
        let b = s.next_packet_id();
        assert_eq!(b, 1); // wraps to 1
    }

    #[test]
    fn can_send_receive_max() {
        let s = Session::new("c1", 5, true, 0);
        *s.receive_maximum.lock().unwrap() = 2;
        s.add_inflight(InflightEntry { packet_id: 1, qos: 1, ..Default::default() });
        assert!(s.can_send());
        s.add_inflight(InflightEntry { packet_id: 2, qos: 1, ..Default::default() });
        assert!(!s.can_send());
    }

    #[test]
    fn store_json_roundtrip() {
        let s = Session::new("c2", 5, false, 3600);
        s.set_subscription("a/b", 1);
        s.add_inflight(InflightEntry { packet_id: 3, qos: 1, topic: "t".into(), payload: vec![1, 2], ..Default::default() });
        s.set_username("u1");
        let v = s.to_store_json();
        let s2 = Session::from_store_json(&v).unwrap();
        assert_eq!(s2.client_id, "c2");
        assert_eq!(*s2.expiry_interval.lock().unwrap(), 3600);
        assert_eq!(s2.username(), "u1");
        assert_eq!(s2.get_subscription("a/b"), Some(1));
        assert!(s2.get_inflight(3).is_some());
    }
}
