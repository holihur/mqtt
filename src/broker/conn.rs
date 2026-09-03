//! Connection handling: CONNECT flow, session establishment, read loop,
//! disconnect handling and will delivery.
//! Port of `internal/broker/broker_conn.go` and the will paths of broker.go.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::codec::{packet_type::*, Packet, Properties};
use crate::persistence::{PendingRetry, PendingWill};
use crate::session::{InflightEntry, Session, Will as SessionWill};
use crate::transport::Conn;

use super::metrics;
use super::util;
use super::Broker;

/// Read deadline for the initial CONNECT frame.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn handle_raw_conn(b: &Arc<Broker>, raw: Arc<Conn>) {
    // first packet must be CONNECT within 10s
    let pkt = match raw.read_frame(Some(CONNECT_TIMEOUT)).await {
        Ok(frame) => match crate::codec::decode(&frame) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!("read CONNECT failed: addr={} err={}", raw.remote_addr(), e);
                raw.close().await;
                return;
            }
        },
        Err(e) => {
            tracing::debug!("read CONNECT failed: addr={} err={}", raw.remote_addr(), e);
            raw.close().await;
            return;
        }
    };
    tracing::info!(
        "client connect attempt: client={} addr={} version={} keepAlive={} clean={}",
        pkt.client_id, raw.remote_addr(), pkt.version, pkt.keep_alive, pkt.connect_flags.clean_session
    );
    if pkt.ptype != CONNECT {
        tracing::warn!("first packet not CONNECT: addr={}", raw.remote_addr());
        raw.close().await;
        return;
    }
    // auth
    if !b.auth.authenticate(&pkt.client_id, &pkt.username, &pkt.password) {
        tracing::info!("auth denied: client={} addr={} username={}", pkt.client_id, raw.remote_addr(), pkt.username);
        metrics().auth_failed.inc();
        metrics().packet_dropped.inc("auth");
        let reason = if pkt.version == crate::codec::PROTOCOL_V5 { 0x86 } else { 0x04 };
        let resp = Packet { ptype: CONNACK, version: pkt.version, reason_code: reason, ..Default::default() };
        let _ = b.send_packet(&raw, &resp).await;
        raw.close().await;
        return;
    }

    let mut client_id = pkt.client_id.clone();
    if client_id.is_empty() {
        if !pkt.connect_flags.clean_session && pkt.version != crate::codec::PROTOCOL_V5 {
            let resp = Packet { ptype: CONNACK, version: pkt.version, reason_code: 0x02, ..Default::default() };
            let _ = b.send_packet(&raw, &resp).await;
            raw.close().await;
            return;
        }
        client_id = format!("auto-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    }
    if client_id.len() > 64 {
        metrics().packet_dropped.inc("clientid_too_long");
        let reason = if pkt.version == crate::codec::PROTOCOL_V5 { 0x85 } else { 0x02 };
        let resp = Packet { ptype: CONNACK, version: pkt.version, reason_code: reason, ..Default::default() };
        let _ = b.send_packet(&raw, &resp).await;
        raw.close().await;
        return;
    }
    if pkt.version != crate::codec::PROTOCOL_V5 && client_id.len() > 23 {
        tracing::warn!("clientID exceeds 23 chars for v3: client={}", client_id);
    }
    raw.set_client_id(&client_id);
    raw.set_version(pkt.version);

    // max connections
    let over_capacity = {
        let conns = b.conns.read().unwrap();
        conns.len() >= b.cfg.max_connections
    };
    if over_capacity {
        metrics().packet_dropped.inc("max_connections");
        let reason = if pkt.version == crate::codec::PROTOCOL_V5 { 0x97 } else { 0x03 };
        let resp = Packet { ptype: CONNACK, version: pkt.version, reason_code: reason, ..Default::default() };
        let _ = b.send_packet(&raw, &resp).await;
        tracing::info!(
            "reject max connections: client={} current={} max={}",
            client_id, b.client_count(), b.cfg.max_connections
        );
        raw.close().await;
        return;
    }

    // session handling
    let (sess, session_existed) = match get_or_create_session(b, &pkt, &client_id).await {
        Ok(x) => x,
        Err(e) => {
            tracing::error!("session error: {}", e);
            raw.close().await;
            return;
        }
    };
    if session_existed {
        let old_username = sess.username();
        // a persistent session stays bound to the username that created it
        if !old_username.is_empty() && pkt.username != old_username {
            metrics().packet_dropped.inc("session_hijack");
            tracing::warn!(
                "session hijack attempt: client={} oldUser={} newUser={} addr={}",
                client_id, old_username, pkt.username, raw.remote_addr()
            );
            let reason = if pkt.version == crate::codec::PROTOCOL_V5 { 0x86 } else { 0x04 };
            let resp = Packet { ptype: CONNACK, version: pkt.version, reason_code: reason, ..Default::default() };
            let _ = b.send_packet(&raw, &resp).await;
            raw.close().await;
            return;
        }
    }

    // update session fields
    {
        *sess.version.lock().unwrap() = pkt.version;
        *sess.keep_alive.lock().unwrap() = pkt.keep_alive;
        sess.set_connected(true);
        *sess.node_id.lock().unwrap() = b.node_id();
        sess.set_username(&pkt.username);
        if pkt.version == crate::codec::PROTOCOL_V5 {
            if let Some(props) = &pkt.properties {
                if let Some(rm) = props.receive_maximum {
                    *sess.receive_maximum.lock().unwrap() = rm.min(b.cfg.max_inflight_window);
                }
                if let Some(mps) = props.maximum_packet_size {
                    *sess.maximum_packet_size.lock().unwrap() = mps;
                }
                if let Some(ta) = props.topic_alias_maximum {
                    *sess.topic_alias_maximum.lock().unwrap() = ta.min(crate::session::MAX_TOPIC_ALIAS);
                }
            }
        }
        let clean = pkt.connect_flags.clean_session;
        let expiry = if pkt.version == crate::codec::PROTOCOL_V5 {
            pkt.properties.as_ref().and_then(|p| p.session_expiry_interval).unwrap_or(0)
        } else if clean {
            0
        } else {
            0xFFFF_FFFF
        };
        *sess.clean_start.lock().unwrap() = clean;
        *sess.expiry_interval.lock().unwrap() = expiry;
        {
            let mut rm = sess.receive_maximum.lock().unwrap();
            *rm = (*rm).min(b.cfg.max_inflight_window);
        }
    }
    b.clients_total.fetch_add(1, Ordering::Relaxed);

    // kick existing connection with same clientID, then register the new one
    let old = { b.conns.read().unwrap().get(&client_id).cloned() };
    if let Some(old) = &old {
        old.close().await;
    }
    {
        b.conns.write().unwrap().insert(client_id.clone(), raw.clone());
        b.sessions.write().unwrap().insert(client_id.clone(), sess.clone());
    }
    if let Err(e) = b.store.save_session(&sess).await {
        tracing::warn!("store SaveSession failed: {}", e);
    }

    // will validation
    if let Some(w) = &pkt.will {
        if w.topic.is_empty() || w.topic.starts_with("$SYS/") {
            // invalid will topic: ignore
        } else if w.payload.len() > b.cfg.max_packet_size
            || w.topic.len() + w.payload.len() > b.cfg.max_packet_size
        {
            tracing::warn!("will payload too large: client={} size={}", client_id, w.payload.len());
            metrics().packet_dropped.inc("will_too_large");
        } else {
            let mut delay = w.delay_interval;
            if delay > 86400 {
                delay = 86400;
            }
            sess.set_will(Some(SessionWill {
                topic: w.topic.clone(),
                payload: w.payload.clone(),
                qos: w.qos,
                retain: w.retain,
                delay_interval: delay,
            }));
        }
    }

    // CONNACK - SessionPresent: true if session existed and clean is false
    let mut session_present = session_existed && !pkt.connect_flags.clean_session;
    if client_id != pkt.client_id {
        session_present = false;
    }
    let mut connack = Packet {
        ptype: CONNACK,
        version: pkt.version,
        session_present,
        reason_code: 0,
        ..Default::default()
    };
    if pkt.version == crate::codec::PROTOCOL_V5 {
        let mut props = Properties::default();
        if pkt.client_id.is_empty() {
            props.assigned_client_id = Some(client_id.clone());
        }
        props.receive_maximum = Some(b.cfg.max_inflight_window);
        props.shared_sub_available = Some(1);
        props.maximum_packet_size = Some(b.cfg.max_packet_size as u32);
        props.topic_alias_maximum = Some(crate::session::MAX_TOPIC_ALIAS);
        connack.conn_properties = Some(props);
    }
    if let Err(e) = b.send_packet(&raw, &connack).await {
        tracing::debug!("send CONNACK failed: {}", e);
        raw.close().await;
        return;
    }
    tracing::info!(
        "client connected: client={} addr={} sessionPresent={} version={} clean={}",
        client_id, raw.remote_addr(), session_present, pkt.version, pkt.connect_flags.clean_session
    );

    // re-add subscriptions to trie
    for (filter, qos) in sess.subscriptions_snapshot() {
        b.trie.add(&filter, &client_id, qos, false);
    }

    // replay offline queue (filter expired + re-check ACL)
    match b.store.dequeue_offline(&client_id).await {
        Err(e) => tracing::warn!("dequeue offline failed: {}", e),
        Ok(offline) => {
            for m in offline {
                if m.is_expired() {
                    metrics().packet_dropped.inc("message_expiry");
                    continue;
                }
                if !b.auth.authorize(&client_id, &m.topic, false) {
                    metrics().packet_dropped.inc("offline_acl");
                    continue;
                }
                let mut pub_pkt = Packet {
                    ptype: PUBLISH,
                    version: pkt.version,
                    topic: m.topic.clone(),
                    qos: m.qos,
                    payload: m.payload.clone(),
                    retain: m.retain,
                    ..Default::default()
                };
                if m.qos > 0 {
                    let pid = sess.next_packet_id();
                    pub_pkt.packet_id = pid;
                    sess.add_inflight(InflightEntry {
                        packet_id: pid,
                        qos: m.qos,
                        topic: m.topic.clone(),
                        payload: m.payload.clone(),
                        ..Default::default()
                    });
                }
                let _ = b.send_packet(&raw, &pub_pkt).await;
            }
        }
    }

    // main read loop
    read_loop(b, &raw, &sess).await;
}

pub(crate) async fn get_or_create_session(
    b: &Arc<Broker>,
    pkt: &Packet,
    client_id: &str,
) -> Result<(Arc<Session>, bool), String> {
    if client_id.is_empty() {
        return Ok((
            Arc::new(Session::new(client_id, pkt.version, pkt.connect_flags.clean_session, 0)),
            false,
        ));
    }
    // in-memory first
    let existing = { b.sessions.read().unwrap().get(client_id).cloned() };
    if let Some(s) = existing {
        if pkt.connect_flags.clean_session {
            s.clear_subscriptions_and_inflight();
            if let Err(e) = b.store.clear_offline(client_id).await {
                tracing::warn!("store ClearOffline failed: {}", e);
            }
        }
        return Ok((s, true));
    }
    // store second
    match b.store.get_session(client_id).await? {
        Some(s) => {
            let s = Arc::new(s);
            if pkt.connect_flags.clean_session {
                s.clear_subscriptions_and_inflight();
                if let Err(e) = b.store.clear_offline(client_id).await {
                    tracing::warn!("store ClearOffline failed: {}", e);
                }
            }
            Ok((s, true))
        }
        None => {
            let expiry = if pkt.version == crate::codec::PROTOCOL_V5 {
                pkt.properties.as_ref().and_then(|p| p.session_expiry_interval).unwrap_or(0)
            } else {
                0
            };
            Ok((
                Arc::new(Session::new(client_id, pkt.version, pkt.connect_flags.clean_session, expiry)),
                false,
            ))
        }
    }
}

async fn read_loop(b: &Arc<Broker>, conn: &Arc<Conn>, sess: &Arc<Session>) {
    loop {
        let keep_alive = *sess.keep_alive.lock().unwrap();
        let timeout = if keep_alive > 0 {
            Some(Duration::from_secs_f64(keep_alive as f64 * 1.5))
        } else {
            None
        };
        let frame = match conn.read_frame(timeout).await {
            Ok(f) => f,
            Err(_) => {
                on_client_disconnect(b, &conn.client_id(), Some(sess), false).await;
                conn.close().await;
                return;
            }
        };
        let pkt = match crate::codec::decode_with_version(&frame, conn.version()) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!("packet decode failed: client={} err={}", conn.client_id(), e);
                on_client_disconnect(b, &conn.client_id(), Some(sess), false).await;
                conn.close().await;
                return;
            }
        };
        match pkt.ptype {
            PUBLISH => {
                tracing::debug!(
                    "publish recv: client={} topic={} qos={} retain={} payloadLen={}",
                    conn.client_id(), pkt.topic, pkt.qos, pkt.retain, pkt.payload.len()
                );
                super::publish::handle_publish(b, conn, sess, pkt).await;
            }
            SUBSCRIBE => {
                tracing::debug!("subscribe recv: client={} packetID={}", conn.client_id(), pkt.packet_id);
                super::subscribe::handle_subscribe(b, conn, sess, pkt).await;
            }
            UNSUBSCRIBE => {
                tracing::debug!("unsubscribe recv: client={} packetID={}", conn.client_id(), pkt.packet_id);
                super::subscribe::handle_unsubscribe(b, conn, sess, pkt).await;
            }
            PUBACK => {
                sess.remove_inflight(pkt.packet_id);
                let _ = b.store.delete_pending_retry(&sess.client_id, pkt.packet_id).await;
            }
            PUBREC => {
                if sess.get_inflight(pkt.packet_id).is_some() {
                    let rel = Packet {
                        ptype: PUBREL,
                        version: conn.version(),
                        packet_id: pkt.packet_id,
                        ..Default::default()
                    };
                    let _ = b.send_packet(conn, &rel).await;
                }
            }
            PUBREL => {
                if let Some(e) = sess.get_inflight(pkt.packet_id) {
                    b.route_message(&e.topic, &e.payload, 2, false, None, &sess.client_id).await;
                    sess.remove_inflight(pkt.packet_id);
                } else {
                    sess.remove_inflight(pkt.packet_id);
                }
                let _ = b.store.delete_pending_retry(&sess.client_id, pkt.packet_id).await;
                let comp = Packet {
                    ptype: PUBCOMP,
                    version: conn.version(),
                    packet_id: pkt.packet_id,
                    ..Default::default()
                };
                let _ = b.send_packet(conn, &comp).await;
            }
            PUBCOMP => {
                sess.remove_inflight(pkt.packet_id);
                let _ = b.store.delete_pending_retry(&sess.client_id, pkt.packet_id).await;
            }
            PINGREQ => {
                let resp = Packet { ptype: PINGRESP, version: conn.version(), ..Default::default() };
                let _ = b.send_packet(conn, &resp).await;
            }
            DISCONNECT => {
                on_client_disconnect(b, &conn.client_id(), Some(sess), true).await;
                conn.close().await;
                return;
            }
            other => {
                tracing::debug!("unhandled packet: type={} client={}", other, conn.client_id());
            }
        }
    }
}

/// Disconnect callback (mirrors `onClientDisconnect`). Idempotent.
pub(crate) async fn on_client_disconnect(
    b: &Arc<Broker>,
    client_id: &str,
    sess: Option<&Arc<Session>>,
    clean: bool,
) {
    b.conns.write().unwrap().remove(client_id);
    b.limiters.lock().unwrap().remove(client_id);
    let sess = match sess {
        None => {
            tracing::info!("client disconnect: client={} clean={}", client_id, clean);
            return;
        }
        Some(s) => s,
    };
    if sess.is_deleted() {
        tracing::info!("client disconnect (session deleted via admin api): client={}", client_id);
        return;
    }
    let node_id = sess.node_id.lock().unwrap().clone();
    tracing::info!("client disconnect: client={} clean={} node={}", client_id, clean, node_id);
    let expiry = *sess.expiry_interval.lock().unwrap();
    let subs = sess.subscription_filters();
    if expiry == 0 {
        for f in &subs {
            b.trie.remove(f, client_id);
        }
        if !clean {
            handle_will(b, sess).await;
        } else {
            sess.set_will(None);
        }
        // clean session: drop from memory + store on disconnect
        b.sessions.write().unwrap().remove(client_id);
        if let Err(e) = b.store.delete_session(client_id).await {
            tracing::warn!("store DeleteSession failed: {}", e);
        }
        return;
    }
    if !clean && sess.get_will().is_some() {
        handle_will(b, sess).await;
    } else if clean {
        sess.set_will(None);
    }
    sess.set_connected(false);
    if let Err(e) = b.store.save_session(sess).await {
        tracing::warn!("store SaveSession failed: {}", e);
    }
}

/// Deliver a will message (delay-capable). Mirrors `handleWill`.
pub(crate) async fn handle_will(b: &Arc<Broker>, sess: &Arc<Session>) {
    let will = match sess.take_will() {
        Some(w) => w,
        None => return,
    };
    let client_id = sess.client_id.clone();
    let SessionWill { topic, payload, qos, retain, delay_interval } = will;
    let mut delay = delay_interval;
    if delay > 86400 {
        delay = 86400;
    }
    if delay > 0 {
        let deliver_at = util::now_unix_millis() + (delay as i64) * 1000;
        let pw = PendingWill {
            client_id: client_id.clone(),
            topic: topic.clone(),
            payload: payload.clone(),
            qos,
            retain,
            deliver_at,
        };
        if let Err(e) = b.store.save_pending_will(&pw).await {
            tracing::warn!("store SavePendingWill failed: client={} err={}", client_id, e);
        }
        let b2 = b.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(delay as u64)).await;
            let _ = b2.store.delete_pending_will(&client_id).await;
            b2.route_message(&topic, &payload, qos, retain, None, &client_id).await;
        });
        return;
    }
    b.route_message(&topic, &payload, qos, retain, None, &client_id).await;
}

/// Restore pending wills at startup (mirrors `restorePendingWills`).
pub(crate) async fn restore_pending_wills(b: &Arc<Broker>) {
    let wills = match b.store.list_pending_wills().await {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("restore pending wills failed: {}", e);
            return;
        }
    };
    let now = util::now_unix_millis();
    let count = wills.len();
    for w in wills {
        let delay = w.deliver_at - now;
        let b2 = b.clone();
        let PendingWill { client_id, topic, payload, qos, retain, .. } = w;
        if delay <= 0 {
            let _ = b2.store.delete_pending_will(&client_id).await;
            b2.route_message(&topic, &payload, qos, retain, None, &client_id).await;
            continue;
        }
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            let _ = b2.store.delete_pending_will(&client_id).await;
            b2.route_message(&topic, &payload, qos, retain, None, &client_id).await;
        });
    }
    if count > 0 {
        tracing::info!("restored pending wills: count={}", count);
    }
}

/// Restore pending QoS retries at startup (mirrors `restorePendingRetries`).
pub(crate) async fn restore_pending_retries(b: &Arc<Broker>) {
    let retries = match b.store.list_pending_retries().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("restore pending retries failed: {}", e);
            return;
        }
    };
    let now = util::now_unix_millis();
    let count = retries.len();
    for r in retries {
        let delay = r.next_retry_at - now;
        let b2 = b.clone();
        let PendingRetry { client_id, packet_id, retries: n, .. } = r;
        if delay <= 0 {
            let sess = {
                let sessions = b2.sessions.read().unwrap();
                sessions.get(&client_id).cloned()
            };
            let conn = {
                let conns = b2.conns.read().unwrap();
                conns.get(&client_id).cloned()
            };
            if let (Some(sess), Some(conn)) = (sess, conn) {
                if let Some(mut e) = sess.get_inflight(packet_id) {
                    e.dup = true;
                    let pub_pkt = Packet {
                        ptype: crate::codec::packet_type::PUBLISH,
                        version: conn.version(),
                        topic: e.topic.clone(),
                        qos: e.qos,
                        payload: e.payload.clone(),
                        packet_id,
                        dup: true,
                        ..Default::default()
                    };
                    let _ = b2.send_packet(&conn, &pub_pkt).await;
                    super::publish::schedule_retry(&b2, &client_id, packet_id, n + 1).await;
                    continue;
                }
            }
            let _ = b2.store.delete_pending_retry(&client_id, packet_id).await;
            continue;
        }
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            let sess = {
                let sessions = b2.sessions.read().unwrap();
                sessions.get(&client_id).cloned()
            };
            let conn = {
                let conns = b2.conns.read().unwrap();
                conns.get(&client_id).cloned()
            };
            if let (Some(sess), Some(conn)) = (sess, conn) {
                if let Some(mut e) = sess.get_inflight(packet_id) {
                    e.dup = true;
                    let pub_pkt = Packet {
                        ptype: crate::codec::packet_type::PUBLISH,
                        version: conn.version(),
                        topic: e.topic.clone(),
                        qos: e.qos,
                        payload: e.payload.clone(),
                        packet_id,
                        dup: true,
                        ..Default::default()
                    };
                    let _ = b2.send_packet(&conn, &pub_pkt).await;
                    super::publish::schedule_retry(&b2, &client_id, packet_id, n + 1).await;
                } else {
                    let _ = b2.store.delete_pending_retry(&client_id, packet_id).await;
                }
            }
        });
    }
    if count > 0 {
        tracing::info!("restored pending retries: count={}", count);
    }
}
