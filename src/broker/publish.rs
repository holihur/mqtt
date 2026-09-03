//! Publish path: rate limiting, QoS 1/2 inbound flows, topic alias, retain
//! handling, routing + local delivery, retry scheduling.
//! Port of `internal/broker/broker_publish.go`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::codec::packet_type::*;
use crate::codec::Packet;
use crate::persistence::{now_millis, Message, PendingRetry};
use crate::session::{InflightEntry, Session};
use crate::topic;
use crate::transport::Conn;

use super::util;
use super::{metrics, Broker};

pub(crate) async fn allow_publish(b: &Arc<Broker>, client_id: &str) -> bool {
    let now = SystemTime::now();
    let mut limiters = b.limiters.lock().unwrap();
    let lim = limiters.entry(client_id.to_string()).or_default();
    if lim.window.map(|w| now.duration_since(w).unwrap_or_default() >= Duration::from_secs(1)).unwrap_or(true) {
        lim.window = Some(now);
        lim.publish_count = 0;
    }
    lim.publish_count += 1;
    lim.last_seen = Some(now);
    lim.publish_count <= b.cfg.max_publish_per_sec
}

pub(crate) async fn allow_subscribe(b: &Arc<Broker>, client_id: &str) -> bool {
    let now = SystemTime::now();
    let mut limiters = b.limiters.lock().unwrap();
    let lim = limiters.entry(client_id.to_string()).or_default();
    if lim.window.map(|w| now.duration_since(w).unwrap_or_default() >= Duration::from_secs(1)).unwrap_or(true) {
        lim.window = Some(now);
        lim.subscribe_count = 0;
    }
    lim.subscribe_count += 1;
    lim.last_seen = Some(now);
    lim.subscribe_count <= b.cfg.max_subscribe_per_sec
}

pub(crate) async fn handle_publish(b: &Arc<Broker>, conn: &Arc<Conn>, sess: &Arc<Session>, mut pkt: Packet) {
    let sess_version = *sess.version.lock().unwrap();
    if pkt.topic.starts_with("$SYS/") {
        if pkt.qos == 1 {
            let ack = Packet {
                ptype: PUBACK,
                version: sess_version,
                packet_id: pkt.packet_id,
                reason: 0x87,
                ..Default::default()
            };
            let _ = b.send_packet(conn, &ack).await;
        }
        return;
    }
    if !allow_publish(b, &sess.client_id).await {
        metrics().packet_dropped.inc("publish_rate");
        if pkt.qos == 1 {
            let ack = Packet {
                ptype: PUBACK,
                version: sess_version,
                packet_id: pkt.packet_id,
                reason: 0x97,
                ..Default::default()
            };
            let _ = b.send_packet(conn, &ack).await;
        }
        return;
    }

    // topic alias handling (v5)
    let mut topic_name = pkt.topic.clone();
    if sess_version == crate::codec::PROTOCOL_V5 {
        if let Some(props) = &pkt.pub_props {
            if let Some(alias) = props.topic_alias {
                let max = *sess.topic_alias_maximum.lock().unwrap();
                if alias == 0 || alias > max {
                    if pkt.qos == 1 {
                        let ack = Packet {
                            ptype: PUBACK,
                            version: sess_version,
                            packet_id: pkt.packet_id,
                            reason: 0x94,
                            ..Default::default()
                        };
                        let _ = b.send_packet(conn, &ack).await;
                    }
                    conn.close().await;
                    return;
                }
                if sess.alias_to_topic_len() >= max as usize && sess.alias_to_topic_get(alias).is_none() {
                    if pkt.qos == 1 {
                        let ack = Packet {
                            ptype: PUBACK,
                            version: sess_version,
                            packet_id: pkt.packet_id,
                            reason: 0x94,
                            ..Default::default()
                        };
                        let _ = b.send_packet(conn, &ack).await;
                    }
                    return;
                }
                if !topic_name.is_empty() {
                    sess.alias_to_topic_insert(alias, &topic_name);
                } else {
                    match sess.alias_to_topic_get(alias) {
                        Some(t) => {
                            topic_name = t;
                            pkt.topic = topic_name.clone();
                        }
                        None => {
                            if pkt.qos == 1 {
                                let ack = Packet {
                                    ptype: PUBACK,
                                    version: crate::codec::PROTOCOL_V5,
                                    packet_id: pkt.packet_id,
                                    reason: 0x94,
                                    ..Default::default()
                                };
                                let _ = b.send_packet(conn, &ack).await;
                            }
                            return;
                        }
                    }
                }
            }
        }
    }
    if topic_name.is_empty() {
        return;
    }
    if topic_name.len() > 4096 || pkt.payload.len() > 1 << 20 {
        return;
    }
    let maximum_packet_size = *sess.maximum_packet_size.lock().unwrap();
    if maximum_packet_size > 0 && topic_name.len() + pkt.payload.len() + 10 > maximum_packet_size as usize {
        conn.close().await;
        return;
    }
    if !topic::is_valid_topic(&topic_name) {
        return;
    }

    // QoS2 inbound: PUBREC + store, route after PUBREL
    if pkt.qos == 2 {
        if sess.get_inflight(pkt.packet_id).is_some() {
            let rec = Packet {
                ptype: PUBREC,
                version: conn.version(),
                packet_id: pkt.packet_id,
                ..Default::default()
            };
            let _ = b.send_packet(conn, &rec).await;
            return;
        }
        sess.add_inflight(InflightEntry {
            packet_id: pkt.packet_id,
            qos: 2,
            topic: topic_name.clone(),
            payload: pkt.payload.clone(),
            state: "qos2-publish".into(),
            ..Default::default()
        });
        let rec = Packet {
            ptype: PUBREC,
            version: conn.version(),
            packet_id: pkt.packet_id,
            ..Default::default()
        };
        let _ = b.send_packet(conn, &rec).await;
        return;
    }

    let mut msg_expiry: u32 = 0;
    let mut msg_created_at: i64 = 0;
    let has_expiry_prop = pkt.pub_props.as_ref().map(|p| p.message_expiry_interval.is_some()).unwrap_or(false);
    if has_expiry_prop {
        msg_expiry = pkt.pub_props.as_ref().unwrap().message_expiry_interval.unwrap();
        msg_created_at = now_millis();
        if msg_expiry == 0 {
            metrics().packet_dropped.inc("message_expiry");
        }
    }

    // retain handling with quota and expiry
    if pkt.retain {
        if pkt.payload.is_empty() {
            if let Err(e) = b.store.delete_retained(&topic_name).await {
                tracing::warn!("store DeleteRetained failed: {}", e);
            }
        } else if msg_expiry == 0 && has_expiry_prop {
            // expired for retain, skip store
        } else {
            let (exceeded, reason) = b.check_retain_quota(&topic_name, &pkt.payload).await;
            if exceeded {
                tracing::warn!(
                    "retain quota exceeded: reason={} topic={} client={} payloadSize={}",
                    reason, topic_name, sess.client_id, pkt.payload.len()
                );
                metrics().retain_quota_exceeded.inc(&reason);
                metrics().packet_dropped.inc("retain_quota");
                if pkt.qos == 1 {
                    let ack = Packet {
                        ptype: PUBACK,
                        version: sess_version,
                        packet_id: pkt.packet_id,
                        reason: 0x97,
                        ..Default::default()
                    };
                    let _ = b.send_packet(conn, &ack).await;
                }
                return;
            }
            let msg = Message {
                topic: topic_name.clone(),
                payload: pkt.payload.clone(),
                qos: pkt.qos,
                retain: true,
                created_at: msg_created_at,
                expiry_interval: msg_expiry,
                ..Default::default()
            };
            match b.store.save_retained(&topic_name, &msg).await {
                Err(e) => tracing::warn!("store SaveRetained failed: {}", e),
                Ok(()) if msg_expiry > 0 => {
                    let store = b.store.clone();
                    let t = topic_name.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(msg_expiry as u64)).await;
                        let _ = store.delete_retained(&t).await;
                    });
                }
                Ok(()) => {}
            }
        }
    }

    // ACK for QoS1
    if pkt.qos == 1 {
        let ack = Packet {
            ptype: PUBACK,
            version: conn.version(),
            packet_id: pkt.packet_id,
            reason: if sess_version == crate::codec::PROTOCOL_V5 { 0 } else { 0 },
            ..Default::default()
        };
        let _ = b.send_packet(conn, &ack).await;
    }

    // route locally + cluster
    b.route_message(&topic_name, &pkt.payload, pkt.qos, pkt.retain, pkt.pub_props.as_ref(), &sess.client_id)
        .await;
}

#[derive(Debug)]
struct ShChoice {
    group: String,
    filter: String,
    client: String,
}

pub(crate) async fn deliver_local(
    b: &Arc<Broker>,
    topic_name: &str,
    payload: &[u8],
    qos: u8,
    props: Option<&crate::codec::Properties>,
    from: &str,
) {
    if props.map(|p| p.message_expiry_interval == Some(0)).unwrap_or(false) {
        metrics().packet_dropped.inc("message_expiry");
    }

    // shared subscriptions round-robin
    let mut choices: Vec<ShChoice> = Vec::new();
    {
        let mut shared = b.shared.lock().unwrap();
        // collect matching groups immutably first, then update round-robin idx
        let mut matched: Vec<(String, String, Vec<String>)> = Vec::new();
        for (group, filters) in shared.subs.iter() {
            for (filter, clients) in filters.iter() {
                if clients.is_empty() {
                    continue;
                }
                if !topic::match_filter(topic_name, filter) {
                    continue;
                }
                matched.push((group.clone(), filter.clone(), clients.clone()));
            }
        }
        for (group, filter, clients) in matched {
            let idx = shared.idx.get(&group).copied().unwrap_or(0) % clients.len();
            let chosen = clients[idx].clone();
            shared.idx.insert(group.clone(), (idx + 1) % clients.len());
            if chosen == from {
                continue;
            }
            choices.push(ShChoice { group, filter, client: chosen });
        }
    }
    for ch in choices {
        let (conn, sess) = {
            let conns = b.conns.read().unwrap();
            let sessions = b.sessions.read().unwrap();
            (
                conns.get(&ch.client).cloned(),
                sessions.get(&ch.client).cloned(),
            )
        };
        let (conn, sess) = match (conn, sess) {
            (Some(c), Some(s)) => (c, s),
            (_, sess) => {
                // offline: enqueue if the session persists
                if let Some(sess) = sess {
                    if *sess.expiry_interval.lock().unwrap() != 0 {
                        enqueue_offline(b, &ch.client, topic_name, payload, qos, props).await;
                    }
                }
                continue;
            }
        };
        let stored_qos = sess.get_subscription(&format!("$share/{}/{}", ch.group, ch.filter));
        let q = match stored_qos {
            Some(sq) if sq < qos => sq,
            _ => qos,
        };
        let mut pub_pkt = Packet {
            ptype: PUBLISH,
            version: conn.version(),
            topic: topic_name.to_string(),
            qos: q,
            payload: payload.to_vec(),
            ..Default::default()
        };
        if q > 0 {
            if !sess.can_send() {
                enqueue_offline(b, &ch.client, topic_name, payload, qos, props).await;
                continue;
            }
            let pid = sess.next_packet_id();
            if pid == 0 {
                continue;
            }
            pub_pkt.packet_id = pid;
            sess.add_inflight(InflightEntry {
                packet_id: pid,
                qos: q,
                topic: topic_name.to_string(),
                payload: payload.to_vec(),
                ..Default::default()
            });
            schedule_retry(b, &ch.client, pid, 0).await;
        }
        if conn.version() == crate::codec::PROTOCOL_V5 {
            if let Some(props) = props {
                if !props.subscription_id.is_empty() {
                    pub_pkt.pub_props = Some(crate::codec::Properties {
                        subscription_id: props.subscription_id.clone(),
                        ..Default::default()
                    });
                }
            }
        }
        metrics().messages_sent.inc();
        metrics().inflight.set(sess.inflight_count() as i64);
        let _ = b.send_packet(&conn, &pub_pkt).await;
    }

    // regular trie match
    let subs = b.trie.match_topic(topic_name);
    for sub in subs {
        if sub.client_id == from && sub.no_local {
            continue;
        }
        let (conn, sess) = {
            let conns = b.conns.read().unwrap();
            let sessions = b.sessions.read().unwrap();
            (conns.get(&sub.client_id).cloned(), sessions.get(&sub.client_id).cloned())
        };
        let (conn, sess) = match (conn, sess) {
            (Some(c), Some(s)) => (c, s),
            (_, sess) => {
                if let Some(sess) = sess {
                    if *sess.expiry_interval.lock().unwrap() != 0 {
                        enqueue_offline(b, &sub.client_id, topic_name, payload, qos, props).await;
                    }
                }
                continue;
            }
        };
        // deliver with min QoS (publish QoS and sub QoS)
        let deliver_qos = qos.min(sub.qos);
        let mut pub_pkt = Packet {
            ptype: PUBLISH,
            version: conn.version(),
            topic: topic_name.to_string(),
            qos: deliver_qos,
            payload: payload.to_vec(),
            retain: false,
            ..Default::default()
        };
        if deliver_qos > 0 {
            if !sess.can_send() {
                enqueue_offline(b, &sub.client_id, topic_name, payload, qos, props).await;
                continue;
            }
            let pid = sess.next_packet_id();
            if pid == 0 {
                continue;
            }
            pub_pkt.packet_id = pid;
            sess.add_inflight(InflightEntry {
                packet_id: pid,
                qos: deliver_qos,
                topic: topic_name.to_string(),
                payload: payload.to_vec(),
                ..Default::default()
            });
            schedule_retry(b, &sess.client_id, pid, 0).await;
        }
        if conn.version() == crate::codec::PROTOCOL_V5 {
            if let Some(props) = props {
                if !props.subscription_id.is_empty() {
                    pub_pkt.pub_props = Some(crate::codec::Properties {
                        subscription_id: props.subscription_id.clone(),
                        ..Default::default()
                    });
                }
            }
        }
        metrics().messages_sent.inc();
        metrics().inflight.set(sess.inflight_count() as i64 + 1);
        if let Err(e) = b.send_packet(&conn, &pub_pkt).await {
            tracing::warn!("deliver failed: client={} err={}", sub.client_id, e);
        }
    }
}

async fn enqueue_offline(
    b: &Arc<Broker>,
    client_id: &str,
    topic_name: &str,
    payload: &[u8],
    qos: u8,
    props: Option<&crate::codec::Properties>,
) {
    if props.map(|p| p.message_expiry_interval == Some(0)).unwrap_or(false) {
        return;
    }
    let (expiry, created) = match props.and_then(|p| p.message_expiry_interval) {
        Some(e) => (e, now_millis()),
        None => (0, 0),
    };
    let msg = Message {
        topic: topic_name.to_string(),
        payload: payload.to_vec(),
        qos,
        created_at: created,
        expiry_interval: expiry,
        ..Default::default()
    };
    if let Err(e) = b.store.enqueue_offline(client_id, &msg).await {
        tracing::warn!("store EnqueueOffline failed: {}", e);
    }
}

/// Schedule a QoS retry in 20s (mirrors `scheduleRetry`).
/// Returns a boxed future so the recursive call chain stays `Send`.
pub(crate) fn schedule_retry(
    b: &Arc<Broker>,
    client_id: &str,
    packet_id: u16,
    retries: i32,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(schedule_retry_inner(b.clone(), client_id.to_string(), packet_id, retries))
}

async fn schedule_retry_inner(b: Arc<Broker>, client_id: String, packet_id: u16, retries: i32) {
    if retries >= 5 {
        if let Some(sess) = { b.sessions.read().unwrap().get(&client_id).cloned() } {
            sess.remove_inflight(packet_id);
            metrics().packet_dropped.inc("retry_exceeded");
            tracing::warn!("retry exceeded, dropping inflight: client={} packetID={}", client_id, packet_id);
        }
        let _ = b.store.delete_pending_retry(&client_id, packet_id).await;
        return;
    }
    let (topic, payload, qos) = {
        let sessions = b.sessions.read().unwrap();
        match sessions.get(&client_id).and_then(|s| s.get_inflight(packet_id)) {
            Some(e) => (e.topic, e.payload, e.qos),
            None => (String::new(), Vec::new(), 0u8),
        }
    };
    let next_at = util::now_unix_millis() + 20 * 1000;
    let pr = PendingRetry {
        client_id: client_id.to_string(),
        packet_id,
        topic,
        payload,
        qos,
        next_retry_at: next_at,
        retries,
        created_at: util::now_unix_millis(),
    };
    if let Err(e) = b.store.save_pending_retry(&pr).await {
        tracing::warn!("store SavePendingRetry failed: client={} packetID={} err={}", client_id, packet_id, e);
    }
    let b2 = b.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(20)).await;
        let sess = { b2.sessions.read().unwrap().get(&client_id).cloned() };
        let conn = { b2.conns.read().unwrap().get(&client_id).cloned() };
        if let (Some(sess), Some(conn)) = (sess, conn) {
            if let Some(mut e) = sess.get_inflight(packet_id) {
                e.dup = true;
                let pub_pkt = Packet {
                    ptype: PUBLISH,
                    version: conn.version(),
                    topic: e.topic.clone(),
                    qos: e.qos,
                    payload: e.payload.clone(),
                    packet_id,
                    dup: true,
                    ..Default::default()
                };
                let _ = b2.send_packet(&conn, &pub_pkt).await;
                schedule_retry(&b2, &client_id, packet_id, retries + 1).await;
            } else {
                let _ = b2.store.delete_pending_retry(&client_id, packet_id).await;
            }
        }
    });
}
