//! Subscribe / unsubscribe handling + shared subscriptions.
//! Port of `internal/broker/broker_subscribe.go`.

use std::sync::Arc;

use crate::codec::packet_type::*;
use crate::codec::Packet;
use crate::session::Session;
use crate::topic;
use crate::transport::Conn;

use super::{metrics, Broker};

pub(crate) async fn handle_subscribe(b: &Arc<Broker>, conn: &Arc<Conn>, sess: &Arc<Session>, pkt: Packet) {
    let sess_version = *sess.version.lock().unwrap();
    if !super::publish::allow_subscribe(b, &sess.client_id).await {
        metrics().packet_dropped.inc("subscribe_rate");
        let fail = if sess_version == crate::codec::PROTOCOL_V5 { 0x97 } else { 0x80 };
        let codes = vec![fail; pkt.subscriptions.len()];
        let ack = Packet {
            ptype: SUBACK,
            version: conn.version(),
            packet_id: pkt.packet_id,
            suback_codes: codes,
            ..Default::default()
        };
        let _ = b.send_packet(conn, &ack).await;
        return;
    }

    let mut codes: Vec<u8> = Vec::new();
    let mut existing = sess.subscriptions_snapshot();
    let mut active = existing.len();
    // retained scan once per packet
    let retained = match b.store.list_retained().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("list retained failed: {}", e);
            Vec::new()
        }
    };

    for sub in &pkt.subscriptions {
        let already = existing.contains_key(&sub.filter);
        if !already && active >= b.cfg.max_subscriptions_per_client {
            metrics().packet_dropped.inc("subscription_cap");
            codes.push(if sess_version == crate::codec::PROTOCOL_V5 { 0x97 } else { 0x80 });
            continue;
        }
        if !topic::is_valid_filter(&sub.filter) {
            codes.push(0x80);
            continue;
        }
        if is_sys_filter(&sub.filter) {
            metrics().packet_dropped.inc("sys_sub_denied");
            codes.push(if sess_version == crate::codec::PROTOCOL_V5 { 0x87 } else { 0x80 });
            continue;
        }
        match is_shared_filter(&sub.filter) {
            Some((group, real_filter)) => {
                if is_sys_filter(&real_filter) {
                    metrics().packet_dropped.inc("sys_sub_denied");
                    codes.push(if sess_version == crate::codec::PROTOCOL_V5 { 0x87 } else { 0x80 });
                    continue;
                }
                if !topic::is_valid_filter(&real_filter) {
                    codes.push(0x80);
                    continue;
                }
                {
                    let mut shared = b.shared.lock().unwrap();
                    let filters = shared.subs.entry(group.clone()).or_default();
                    let list = filters.entry(real_filter.clone()).or_default();
                    if !list.contains(&sess.client_id) {
                        list.push(sess.client_id.clone());
                    }
                }
                sess.set_subscription(&sub.filter, sub.qos);
                existing.insert(sub.filter.clone(), sub.qos);
                active += 1;
                if let Err(e) = b.store.save_session(sess).await {
                    tracing::warn!("store SaveSession failed: {}", e);
                }
                codes.push(sub.qos);
                let cluster = b.cluster.lock().unwrap().clone();
                if let Some(cluster) = cluster {
                    let _ = cluster.publish_meta("sub", &sub.filter).await;
                }
            }
            None => {
                b.trie.add(&sub.filter, &sess.client_id, sub.qos, sub.no_local);
                sess.set_subscription(&sub.filter, sub.qos);
                existing.insert(sub.filter.clone(), sub.qos);
                active += 1;
                if let Err(e) = b.store.save_session(sess).await {
                    tracing::warn!("store SaveSession failed: {}", e);
                }
                codes.push(sub.qos);
                let cluster = b.cluster.lock().unwrap().clone();
                if let Some(cluster) = cluster {
                    let _ = cluster.publish_meta("sub", &sub.filter).await;
                }
            }
        }

        // deliver retained messages matching this filter
        for m in &retained {
            if m.is_expired() {
                let _ = b.store.delete_retained(&m.topic).await;
                continue;
            }
            if topic::match_filter(&m.topic, &sub.filter) {
                if !b.auth.authorize(&sess.client_id, &m.topic, false) {
                    continue;
                }
                let mut pub_pkt = Packet {
                    ptype: PUBLISH,
                    version: conn.version(),
                    topic: m.topic.clone(),
                    qos: m.qos,
                    payload: m.payload.clone(),
                    retain: true,
                    ..Default::default()
                };
                if sub.qos > 0 && m.qos > 0 {
                    // retain deliver QoS = min(sub QoS, retained QoS)
                    pub_pkt.qos = m.qos.min(sub.qos);
                    if pub_pkt.qos > 0 {
                        pub_pkt.packet_id = sess.next_packet_id();
                    }
                } else {
                    pub_pkt.qos = 0;
                }
                let _ = b.send_packet(conn, &pub_pkt).await;
            }
        }
    }

    let mut ack = Packet {
        ptype: SUBACK,
        version: conn.version(),
        packet_id: pkt.packet_id,
        suback_codes: codes,
        ..Default::default()
    };
    if sess_version == crate::codec::PROTOCOL_V5 {
        ack.suback_props = Some(crate::codec::Properties::default());
    }
    let _ = b.send_packet(conn, &ack).await;
}

pub(crate) async fn handle_unsubscribe(b: &Arc<Broker>, conn: &Arc<Conn>, sess: &Arc<Session>, pkt: Packet) {
    let sess_version = *sess.version.lock().unwrap();
    for t in &pkt.topics {
        match is_shared_filter(t) {
            Some((group, real_filter)) => {
                let mut shared = b.shared.lock().unwrap();
                if let Some(filters) = shared.subs.get_mut(&group) {
                    if let Some(list) = filters.get_mut(&real_filter) {
                        list.retain(|cid| cid != &sess.client_id);
                        if list.is_empty() {
                            filters.remove(&real_filter);
                            if filters.is_empty() {
                                shared.subs.remove(&group);
                            }
                        }
                    }
                }
            }
            None => {
                b.trie.remove(t, &sess.client_id);
            }
        }
        sess.delete_subscription(t);
        let cluster = b.cluster.lock().unwrap().clone();
        if let Some(cluster) = cluster {
            let _ = cluster.publish_meta("unsub", t).await;
        }
    }
    if let Err(e) = b.store.save_session(sess).await {
        tracing::warn!("store SaveSession failed: {}", e);
    }
    let mut ack = Packet {
        ptype: UNSUBACK,
        version: conn.version(),
        packet_id: pkt.packet_id,
        ..Default::default()
    };
    if sess_version == crate::codec::PROTOCOL_V5 {
        ack.unsuback_props = Some(crate::codec::Properties::default());
        ack.unsuback_codes = vec![0u8; pkt.topics.len()]; // 0 = success
    }
    let _ = b.send_packet(conn, &ack).await;
}

/// Parse `$share/<group>/<filter>`. Returns Some((group, realFilter)).
pub(crate) fn is_shared_filter(filter: &str) -> Option<(String, String)> {
    if let Some(rest) = filter.strip_prefix("$share/") {
        match rest.find('/') {
            Some(slash) => {
                let group = &rest[..slash];
                let real_filter = &rest[slash + 1..];
                if group.is_empty() || real_filter.is_empty() {
                    return None;
                }
                Some((group.to_string(), real_filter.to_string()))
            }
            None => None,
        }
    } else {
        None
    }
}

pub(crate) fn is_sys_filter(filter: &str) -> bool {
    filter == "$SYS" || filter.starts_with("$SYS/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_filter_parse() {
        assert_eq!(is_shared_filter("$share/g/a/b"), Some(("g".into(), "a/b".into())));
        assert_eq!(is_shared_filter("$share/g/"), None); // empty real filter is invalid (Go behavior)
        assert_eq!(is_shared_filter("$share/g"), None);
        assert_eq!(is_shared_filter("$share//x"), None);
        assert_eq!(is_shared_filter("a/b"), None);
    }

    #[test]
    fn sys_filter() {
        assert!(is_sys_filter("$SYS"));
        assert!(is_sys_filter("$SYS/broker"));
        assert!(!is_sys_filter("$SYSA"));
        assert!(!is_sys_filter("sys/x"));
    }
}
