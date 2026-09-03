//! Unified packet representation + encode/decode for MQTT 3.1 / 3.1.1 / 5.0.
//! Faithful port of `internal/codec/packet.go` and `codec.go` including the
//! version-aware decode disambiguation rules.

use super::properties::*;
use super::varint::*;

pub const PROTOCOL_V31: u8 = 3;
pub const PROTOCOL_V311: u8 = 4;
pub const PROTOCOL_V5: u8 = 5;

pub mod packet_type {
    pub const CONNECT: u8 = 1;
    pub const CONNACK: u8 = 2;
    pub const PUBLISH: u8 = 3;
    pub const PUBACK: u8 = 4;
    pub const PUBREC: u8 = 5;
    pub const PUBREL: u8 = 6;
    pub const PUBCOMP: u8 = 7;
    pub const SUBSCRIBE: u8 = 8;
    pub const SUBACK: u8 = 9;
    pub const UNSUBSCRIBE: u8 = 10;
    pub const UNSUBACK: u8 = 11;
    pub const PINGREQ: u8 = 12;
    pub const PINGRESP: u8 = 13;
    pub const DISCONNECT: u8 = 14;
    pub const AUTH: u8 = 15;
}

pub use packet_type::*;

pub const QOS0: u8 = 0;
pub const QOS1: u8 = 1;
pub const QOS2: u8 = 2;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectFlags {
    pub clean_session: bool, // v3: clean session, v5: clean start
    pub will_flag: bool,
    pub will_qos: u8,
    pub will_retain: bool,
    pub password_flag: bool,
    pub username_flag: bool,
}

/// Will message (normalized across versions).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub delay_interval: u32, // v5 only
    pub properties: Option<Properties>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subscription {
    pub filter: String,
    pub qos: u8,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8, // 0,1,2
    pub subscription_id: u32,
}

/// Unified packet representation across v3 and v5.
#[derive(Debug, Clone, Default)]
pub struct Packet {
    pub ptype: u8,
    pub version: u8, // 3, 4, 5 (0 = unknown/generic)
    pub fixed: u8,   // raw fixed header first byte

    // CONNECT
    pub protocol_name: String,
    pub protocol_level: u8,
    pub connect_flags: ConnectFlags,
    pub keep_alive: u16,
    pub client_id: String,
    pub will: Option<Will>,
    pub username: String,
    pub password: Vec<u8>,
    pub properties: Option<Properties>, // v5 CONNECT properties

    // CONNACK
    pub session_present: bool,
    pub reason_code: u8, // v5 reason code, v3 0-5 mapped
    pub conn_properties: Option<Properties>,

    // PUBLISH
    pub topic: String,
    pub packet_id: u16,
    pub qos: u8,
    pub retain: bool,
    pub dup: bool,
    pub payload: Vec<u8>,
    pub pub_props: Option<Properties>,

    // PUBACK/PUBREC/PUBREL/PUBCOMP
    pub reason: u8,
    pub ack_props: Option<Properties>,

    // SUBSCRIBE
    pub subscriptions: Vec<Subscription>,
    pub sub_props: Option<Properties>,
    // SUBACK
    pub suback_codes: Vec<u8>,
    pub suback_props: Option<Properties>,

    // UNSUBSCRIBE
    pub topics: Vec<String>,
    pub unsub_props: Option<Properties>,
    pub unsuback_codes: Vec<u8>,
    pub unsuback_props: Option<Properties>,

    // DISCONNECT
    pub disc_reason: u8,
    pub disc_props: Option<Properties>,

    // AUTH (v5)
    pub auth_reason: u8,
    pub auth_props: Option<Properties>,
}

pub type Result<T> = std::result::Result<T, CodecError>;

fn bool_byte(b: bool) -> u8 {
    if b { 1 } else { 0 }
}

/// Encode packet to wire bytes (fixed header + remaining length + payload).
pub fn encode(p: &Packet) -> Result<Vec<u8>> {
    let (flags, vh_and_payload): (u8, Vec<u8>) = match p.ptype {
        CONNECT => (0, encode_connect(p)),
        CONNACK => (0, encode_connack(p)),
        PUBLISH => {
            let dup = if p.dup { 1 } else { 0 };
            let flags = (dup << 3) | (p.qos << 1) | bool_byte(p.retain);
            (flags, encode_publish(p))
        }
        PUBACK | PUBREC | PUBCOMP => (0, encode_ack(p)),
        PUBREL => (0x02, encode_ack(p)),
        SUBSCRIBE => (0x02, encode_subscribe(p)),
        SUBACK => (0, encode_suback(p)),
        UNSUBSCRIBE => (0x02, encode_unsubscribe(p)),
        UNSUBACK => (0, encode_unsuback(p)),
        PINGREQ | PINGRESP => (0, Vec::new()),
        DISCONNECT => (0, encode_disconnect(p)),
        AUTH => (0, encode_auth(p)),
        _ => return Err(CodecError::MalformedPacket),
    };
    let fixed = (p.ptype << 4) | (flags & 0x0F);
    let mut out = Vec::with_capacity(1 + var_int_len(vh_and_payload.len()) + vh_and_payload.len());
    out.push(fixed);
    append_var_int(&mut out, vh_and_payload.len());
    out.extend_from_slice(&vh_and_payload);
    Ok(out)
}

/// Decode one complete frame with heuristic version detection (only used for
/// the initial CONNECT).
pub fn decode(frame: &[u8]) -> Result<Packet> {
    decode_versioned(frame, 0)
}

/// Version-aware decode used once the client protocol version is known.
pub fn decode_with_version(frame: &[u8], version: u8) -> Result<Packet> {
    decode_versioned(frame, version)
}

fn decode_versioned(frame: &[u8], version: u8) -> Result<Packet> {
    if frame.len() < 2 {
        return Err(CodecError::MalformedPacket);
    }
    let fixed = frame[0];
    let ptype = fixed >> 4;
    let flags = fixed & 0x0F;
    let (rl, n) = decode_var_int(&frame[1..])?;
    if 1 + n + rl != frame.len() {
        return Err(CodecError::MalformedPacket);
    }
    let payload = &frame[1 + n..];
    let mut p = Packet { ptype, fixed, version, ..Default::default() };
    match ptype {
        CONNECT => decode_connect(&mut p, payload)?,
        CONNACK => decode_connack(&mut p, payload)?,
        PUBLISH => {
            p.qos = (flags >> 1) & 0x03;
            p.dup = (flags >> 3) & 0x01 == 1;
            p.retain = flags & 0x01 == 1;
            if p.qos > 2 {
                return Err(CodecError::InvalidQoS);
            }
            decode_publish(&mut p, payload)?;
        }
        PUBACK | PUBREC | PUBREL | PUBCOMP => decode_ack(&mut p, payload)?,
        SUBSCRIBE => {
            if flags != 0x02 {
                return Err(CodecError::ProtocolViolation);
            }
            decode_subscribe(&mut p, payload)?;
        }
        SUBACK => decode_suback(&mut p, payload)?,
        UNSUBSCRIBE => {
            if flags != 0x02 {
                return Err(CodecError::ProtocolViolation);
            }
            decode_unsubscribe(&mut p, payload)?;
        }
        UNSUBACK => decode_unsuback(&mut p, payload)?,
        PINGREQ | PINGRESP => {
            if !payload.is_empty() {
                return Err(CodecError::MalformedPacket);
            }
        }
        DISCONNECT => decode_disconnect(&mut p, payload)?,
        AUTH => decode_auth(&mut p, payload)?,
        _ => return Err(CodecError::MalformedPacket),
    }
    Ok(p)
}

// ---- CONNECT ----

fn encode_connect(p: &Packet) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut proto = p.protocol_name.clone();
    if proto.is_empty() {
        proto = if p.version == PROTOCOL_V31 { "MQIsdp".into() } else { "MQTT".into() };
    }
    buf.extend_from_slice(&encode_string(&proto));
    buf.push(p.protocol_level);
    let cf = &p.connect_flags;
    let mut flags: u8 = 0;
    if cf.clean_session {
        flags |= 0x02;
    }
    if cf.will_flag {
        flags |= 0x04;
        flags |= (cf.will_qos & 0x03) << 3;
        if cf.will_retain {
            flags |= 0x20;
        }
    }
    if cf.password_flag {
        flags |= 0x40;
    }
    if cf.username_flag {
        flags |= 0x80;
    }
    buf.push(flags);
    buf.extend_from_slice(&p.keep_alive.to_be_bytes());
    if p.version == PROTOCOL_V5 {
        buf.extend_from_slice(&encode_properties(p.properties.as_ref()));
    }
    buf.extend_from_slice(&encode_string(&p.client_id));
    if cf.will_flag {
        if let Some(w) = &p.will {
            if p.version == PROTOCOL_V5 {
                buf.extend_from_slice(&encode_will_properties(Some(w.delay_interval), w.properties.as_ref()));
                buf.extend_from_slice(&encode_string(&w.topic));
                buf.extend_from_slice(&encode_binary(&w.payload));
            } else {
                buf.extend_from_slice(&encode_string(&w.topic));
                buf.extend_from_slice(&encode_binary(&w.payload));
            }
        }
    }
    if cf.username_flag {
        buf.extend_from_slice(&encode_string(&p.username));
    }
    if cf.password_flag {
        buf.extend_from_slice(&encode_binary(&p.password));
    }
    buf
}

fn decode_connect(p: &mut Packet, b: &[u8]) -> Result<()> {
    if b.len() < 10 {
        return Err(CodecError::MalformedPacket);
    }
    let (proto, mut pos) = decode_string(b, 0)?;
    if proto != "MQTT" && proto != "MQIsdp" {
        return Err(CodecError::MalformedPacket);
    }
    p.protocol_name = proto;
    if pos >= b.len() {
        return Err(CodecError::MalformedPacket);
    }
    let level = b[pos];
    pos += 1;
    p.protocol_level = level;
    p.version = match level {
        3 => PROTOCOL_V31,
        4 => PROTOCOL_V311,
        5 => PROTOCOL_V5,
        _ => return Err(CodecError::UnsupportedProtocol),
    };
    if pos >= b.len() {
        return Err(CodecError::MalformedPacket);
    }
    let flags = b[pos];
    pos += 1;
    p.connect_flags = ConnectFlags {
        clean_session: flags & 0x02 != 0,
        will_flag: flags & 0x04 != 0,
        will_qos: (flags >> 3) & 0x03,
        will_retain: flags & 0x20 != 0,
        password_flag: flags & 0x40 != 0,
        username_flag: flags & 0x80 != 0,
    };
    if pos + 2 > b.len() {
        return Err(CodecError::MalformedPacket);
    }
    p.keep_alive = decode_u16(&b[pos..]);
    pos += 2;
    if p.version == PROTOCOL_V5 {
        let (props, np) = decode_properties(b, pos)?;
        p.properties = Some(props);
        pos = np;
    }
    let (cid, np) = decode_string(b, pos)?;
    p.client_id = cid;
    pos = np;
    if p.connect_flags.will_flag {
        let mut w = Will {
            qos: p.connect_flags.will_qos,
            retain: p.connect_flags.will_retain,
            ..Default::default()
        };
        if p.version == PROTOCOL_V5 {
            let (mut wprops, np2) = decode_properties(b, pos)?;
            if let Some(d) = wprops.will_delay_interval.take() {
                w.delay_interval = d;
            }
            w.properties = Some(wprops);
            pos = np2;
        }
        let (topic, np2) = decode_string(b, pos)?;
        w.topic = topic;
        pos = np2;
        let (pay, np3) = decode_binary(b, pos)?;
        w.payload = pay;
        pos = np3;
        p.will = Some(w);
    }
    if p.connect_flags.username_flag {
        let (u, np) = decode_string(b, pos)?;
        p.username = u;
        pos = np;
    }
    if p.connect_flags.password_flag {
        let (pw, _) = decode_binary(b, pos)?;
        p.password = pw;
    }
    Ok(())
}

// ---- CONNACK ----

fn encode_connack(p: &Packet) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(bool_byte(p.session_present));
    buf.push(p.reason_code);
    if p.version == PROTOCOL_V5 {
        if p.conn_properties.is_none() {
            buf.extend_from_slice(&encode_var_int(0));
        } else {
            buf.extend_from_slice(&encode_properties(p.conn_properties.as_ref()));
        }
    }
    buf
}

fn decode_connack(p: &mut Packet, b: &[u8]) -> Result<()> {
    if b.len() < 2 {
        return Err(CodecError::MalformedPacket);
    }
    p.session_present = b[0] & 0x01 == 1;
    p.reason_code = b[1];
    if b.len() > 2 {
        p.version = PROTOCOL_V5;
        let (props, _) = decode_properties(b, 2)?;
        p.conn_properties = Some(props);
    } else if p.reason_code <= 5 {
        p.version = PROTOCOL_V311;
    } else {
        p.version = PROTOCOL_V5;
    }
    Ok(())
}

// ---- PUBLISH ----

fn encode_publish(p: &Packet) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&encode_string(&p.topic));
    if p.qos > 0 {
        buf.extend_from_slice(&p.packet_id.to_be_bytes());
    }
    if p.version == PROTOCOL_V5 {
        buf.extend_from_slice(&encode_properties(p.pub_props.as_ref()));
    }
    buf.extend_from_slice(&p.payload);
    buf
}

fn decode_publish(p: &mut Packet, b: &[u8]) -> Result<()> {
    let (topic, mut pos) = decode_string(b, 0)?;
    p.topic = topic;
    if p.qos > 0 {
        if pos + 2 > b.len() {
            return Err(CodecError::MalformedPacket);
        }
        p.packet_id = decode_u16(&b[pos..]);
        pos += 2;
    }
    // Version-aware property handling, mirroring the Go logic:
    //  - v5: properties are mandatory and precede the payload
    //  - v3: the remainder is entirely payload
    //  - generic (version 0): only attempt a v5 parse when QoS > 0, and fall
    //    back to all-payload when the block is not well-formed.
    if pos < b.len() {
        let saved = pos;
        if p.version == PROTOCOL_V5 || (p.version == 0 && p.qos > 0) {
            match decode_properties(b, pos) {
                Ok((props, np)) => {
                    p.pub_props = Some(props);
                    pos = np;
                }
                Err(CodecError::MalformedPacket) if p.version == 0 => {
                    // v3 PUBLISH: the rest is payload.
                    p.payload = b[saved..].to_vec();
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }
        p.payload = b[pos..].to_vec();
    } else {
        p.payload = Vec::new();
    }
    Ok(())
}

// ---- ACK (PUBACK etc) ----

fn encode_ack(p: &Packet) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&p.packet_id.to_be_bytes());
    if p.version == PROTOCOL_V5 {
        if p.reason != 0 || p.ack_props.is_some() {
            buf.push(p.reason);
            if p.ack_props.is_none() {
                buf.extend_from_slice(&encode_var_int(0));
            } else {
                buf.extend_from_slice(&encode_properties(p.ack_props.as_ref()));
            }
        }
    }
    buf
}

fn decode_ack(p: &mut Packet, b: &[u8]) -> Result<()> {
    if b.len() < 2 {
        return Err(CodecError::MalformedPacket);
    }
    p.packet_id = decode_u16(b);
    if b.len() == 2 {
        p.reason = 0;
        return Ok(());
    }
    if b.len() >= 3 {
        p.reason = b[2];
        if b.len() > 3 {
            let (props, _) = decode_properties(b, 3)?;
            p.ack_props = Some(props);
            p.version = PROTOCOL_V5;
        } else {
            p.version = PROTOCOL_V5;
        }
    }
    Ok(())
}

// ---- SUBSCRIBE ----

fn encode_subscribe(p: &Packet) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&p.packet_id.to_be_bytes());
    if p.version == PROTOCOL_V5 {
        buf.extend_from_slice(&encode_properties(p.sub_props.as_ref()));
    }
    for s in &p.subscriptions {
        buf.extend_from_slice(&encode_string(&s.filter));
        let mut opts = s.qos & 0x03;
        if p.version == PROTOCOL_V5 {
            if s.no_local {
                opts |= 1 << 2;
            }
            if s.retain_as_published {
                opts |= 1 << 3;
            }
            opts |= (s.retain_handling & 0x03) << 4;
        }
        buf.push(opts);
    }
    buf
}

fn decode_subscribe(p: &mut Packet, b: &[u8]) -> Result<()> {
    if b.len() < 2 {
        return Err(CodecError::MalformedPacket);
    }
    p.packet_id = decode_u16(b);
    if p.version == PROTOCOL_V5 {
        let (props, np) = decode_properties(b, 2)?;
        let subs = try_parse_subscribe_payload(b, np, true).ok_or(CodecError::MalformedPacket)?;
        p.sub_props = Some(props);
        p.subscriptions = subs;
        return Ok(());
    }
    // v3 (or generic): generic attempts a v5 parse first and falls back to v3.
    let start = 2;
    if p.version == 0 {
        if let Ok((props, np)) = decode_properties(b, 2) {
            if let Some(subs) = try_parse_subscribe_payload(b, np, true) {
                p.sub_props = Some(props);
                p.subscriptions = subs;
                return Ok(());
            }
        }
    }
    let subs = try_parse_subscribe_payload(b, start, false).ok_or(CodecError::MalformedPacket)?;
    p.subscriptions = subs;
    p.version = PROTOCOL_V311;
    Ok(())
}

fn try_parse_subscribe_payload(b: &[u8], pos: usize, is_v5: bool) -> Option<Vec<Subscription>> {
    let mut subs = Vec::new();
    let mut pos = pos;
    while pos < b.len() {
        let (filter, np) = decode_string(b, pos).ok()?;
        pos = np;
        if pos >= b.len() {
            return None;
        }
        let opts = b[pos];
        pos += 1;
        let mut sub = Subscription { filter, qos: opts & 0x03, ..Default::default() };
        if is_v5 {
            sub.no_local = opts & 0x04 != 0;
            sub.retain_as_published = opts & 0x08 != 0;
            sub.retain_handling = (opts >> 4) & 0x03;
        }
        subs.push(sub);
    }
    if pos != b.len() || subs.is_empty() {
        return None;
    }
    Some(subs)
}

// ---- SUBACK ----

fn encode_suback(p: &Packet) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&p.packet_id.to_be_bytes());
    if p.version == PROTOCOL_V5 {
        buf.extend_from_slice(&encode_properties(p.suback_props.as_ref()));
    }
    buf.extend_from_slice(&p.suback_codes);
    buf
}

fn decode_suback(p: &mut Packet, b: &[u8]) -> Result<()> {
    if b.len() < 2 {
        return Err(CodecError::MalformedPacket);
    }
    p.packet_id = decode_u16(b);
    let pos = 2;
    if p.version == PROTOCOL_V5 {
        let (props, np) = decode_properties(b, pos)?;
        p.suback_props = Some(props);
        p.suback_codes = b[np..].to_vec();
        return Ok(());
    }
    if p.version == 0 && b.len() > pos {
        if let Ok((props, np)) = decode_properties(b, pos) {
            if np <= b.len() {
                let non_empty = !props.user.is_empty() || props.reason_string.is_some() || np > pos + 1;
                if non_empty {
                    p.suback_props = Some(props);
                    p.suback_codes = b[np..].to_vec();
                    p.version = PROTOCOL_V5;
                    return Ok(());
                }
                // props length == 0: ambiguous. If every remaining byte is a
                // valid v3 code, treat as v3 (leading 0x00 is a reason code).
                let remaining = &b[np..];
                let is_v3 = remaining.iter().all(|&c| c == 0x00 || c == 0x01 || c == 0x02 || c == 0x80);
                if is_v3 {
                    p.suback_codes = b[pos..].to_vec();
                    p.version = PROTOCOL_V311;
                    return Ok(());
                }
                p.suback_props = Some(props);
                p.suback_codes = b[np..].to_vec();
                p.version = PROTOCOL_V5;
                return Ok(());
            }
        }
    }
    // v3: all remaining bytes are reason codes.
    p.suback_codes = b[pos..].to_vec();
    p.version = PROTOCOL_V311;
    Ok(())
}

// ---- UNSUBSCRIBE ----

fn encode_unsubscribe(p: &Packet) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&p.packet_id.to_be_bytes());
    if p.version == PROTOCOL_V5 {
        buf.extend_from_slice(&encode_properties(p.unsub_props.as_ref()));
    }
    for t in &p.topics {
        buf.extend_from_slice(&encode_string(t));
    }
    buf
}

fn decode_unsubscribe(p: &mut Packet, b: &[u8]) -> Result<()> {
    if b.len() < 2 {
        return Err(CodecError::MalformedPacket);
    }
    p.packet_id = decode_u16(b);
    if p.version == PROTOCOL_V5 {
        let (props, np) = decode_properties(b, 2)?;
        let topics = try_parse_unsub_payload(b, np).ok_or(CodecError::MalformedPacket)?;
        p.unsub_props = Some(props);
        p.topics = topics;
        return Ok(());
    }
    let start = 2;
    if p.version == 0 {
        if let Ok((props, np)) = decode_properties(b, 2) {
            if let Some(topics) = try_parse_unsub_payload(b, np) {
                p.unsub_props = Some(props);
                p.topics = topics;
                return Ok(());
            }
        }
    }
    let topics = try_parse_unsub_payload(b, start).ok_or(CodecError::MalformedPacket)?;
    p.topics = topics;
    p.version = PROTOCOL_V311;
    Ok(())
}

fn try_parse_unsub_payload(b: &[u8], pos: usize) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut pos = pos;
    while pos < b.len() {
        let (t, np) = decode_string(b, pos).ok()?;
        pos = np;
        out.push(t);
    }
    if pos != b.len() || out.is_empty() {
        return None;
    }
    Some(out)
}

fn encode_unsuback(p: &Packet) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&p.packet_id.to_be_bytes());
    if p.version == PROTOCOL_V5 {
        buf.extend_from_slice(&encode_properties(p.unsub_props.is_some().then(|| ()).map(|_| p.unsuback_props.as_ref()).unwrap_or(p.unsuback_props.as_ref())));
        for c in &p.unsuback_codes {
            buf.push(*c);
        }
    } else if !p.unsuback_codes.is_empty() {
        buf.extend_from_slice(&p.unsuback_codes);
    }
    buf
}

fn decode_unsuback(p: &mut Packet, b: &[u8]) -> Result<()> {
    if b.len() < 2 {
        return Err(CodecError::MalformedPacket);
    }
    p.packet_id = decode_u16(b);
    if b.len() > 2 {
        if p.version == PROTOCOL_V5 {
            let (props, np) = decode_properties(b, 2)?;
            p.unsuback_props = Some(props);
            p.unsuback_codes = b[np..].to_vec();
            return Ok(());
        }
        if let Ok((props, np)) = decode_properties(b, 2) {
            if np <= b.len() {
                p.unsuback_props = Some(props);
                p.version = PROTOCOL_V5;
                p.unsuback_codes = b[np..].to_vec();
                return Ok(());
            }
        }
        p.unsuback_codes = b[2..].to_vec();
    }
    Ok(())
}

// ---- DISCONNECT ----

fn encode_disconnect(p: &Packet) -> Vec<u8> {
    if p.version != PROTOCOL_V5 {
        return Vec::new();
    }
    let mut buf = Vec::new();
    buf.push(p.disc_reason);
    buf.extend_from_slice(&encode_properties(p.disc_props.as_ref()));
    buf
}

fn decode_disconnect(p: &mut Packet, b: &[u8]) -> Result<()> {
    if b.is_empty() {
        p.disc_reason = 0;
        return Ok(());
    }
    p.disc_reason = b[0];
    p.version = PROTOCOL_V5;
    if b.len() > 1 {
        let (props, _) = decode_properties(b, 1)?;
        p.disc_props = Some(props);
    }
    Ok(())
}

// ---- AUTH ----

fn encode_auth(p: &Packet) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(p.auth_reason);
    buf.extend_from_slice(&encode_properties(p.auth_props.as_ref()));
    buf
}

fn decode_auth(p: &mut Packet, b: &[u8]) -> Result<()> {
    if b.is_empty() {
        return Err(CodecError::MalformedPacket);
    }
    p.auth_reason = b[0];
    p.version = PROTOCOL_V5;
    if b.len() > 1 {
        let (props, _) = decode_properties(b, 1)?;
        p.auth_props = Some(props);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_v311_roundtrip() {
        let p = Packet {
            ptype: CONNECT,
            version: PROTOCOL_V311,
            protocol_name: "MQTT".into(),
            protocol_level: 4,
            connect_flags: ConnectFlags { clean_session: true, ..Default::default() },
            keep_alive: 30,
            client_id: "c1".into(),
            ..Default::default()
        };
        let enc = encode(&p).unwrap();
        let d = decode(&enc).unwrap();
        assert_eq!(d.ptype, CONNECT);
        assert_eq!(d.version, PROTOCOL_V311);
        assert_eq!(d.client_id, "c1");
        assert_eq!(d.keep_alive, 30);
        assert!(d.connect_flags.clean_session);
    }

    #[test]
    fn connect_v5_with_will_and_props() {
        let p = Packet {
            ptype: CONNECT,
            version: PROTOCOL_V5,
            protocol_name: "MQTT".into(),
            protocol_level: 5,
            connect_flags: ConnectFlags {
                clean_session: true,
                will_flag: true,
                will_qos: 1,
                username_flag: true,
                password_flag: true,
                ..Default::default()
            },
            keep_alive: 60,
            client_id: "v5c".into(),
            properties: Some(Properties {
                session_expiry_interval: Some(120),
                receive_maximum: Some(10),
                ..Default::default()
            }),
            will: Some(Will {
                topic: "w/t".into(),
                payload: b"bye".to_vec(),
                qos: 1,
                delay_interval: 5,
                ..Default::default()
            }),
            username: "user".into(),
            password: b"pass".to_vec(),
            ..Default::default()
        };
        let enc = encode(&p).unwrap();
        let d = decode(&enc).unwrap();
        assert_eq!(d.version, PROTOCOL_V5);
        assert_eq!(d.client_id, "v5c");
        assert_eq!(d.properties.as_ref().unwrap().session_expiry_interval, Some(120));
        assert_eq!(d.properties.as_ref().unwrap().receive_maximum, Some(10));
        let w = d.will.unwrap();
        assert_eq!(w.topic, "w/t");
        assert_eq!(w.payload, b"bye");
        assert_eq!(w.qos, 1);
        assert_eq!(w.delay_interval, 5);
        assert_eq!(d.username, "user");
        assert_eq!(d.password, b"pass");
    }

    #[test]
    fn publish_v311_payload_starting_like_properties() {
        // v3 QoS1 publish whose payload begins with 0x05 (a plausible props len)
        let mut vh = Vec::new();
        vh.extend_from_slice(&encode_string("a/b"));
        vh.extend_from_slice(&1u16.to_be_bytes());
        let mut payload = vec![0x05, 0xAA, 0xBB];
        vh.extend_from_slice(&payload);
        let rl = vh.len();
        let mut frame = vec![(PUBLISH << 4) | 0x02];
        append_var_int(&mut frame, rl);
        frame.extend_from_slice(&vh);

        // generic decode: QoS>0 attempts v5 parse; payload 0x05... decodes as
        // properties block 5 bytes, leaving 0xBB — wait: it must NOT succeed as
        // well-formed v5, so it falls back to all-payload (matches Go logic).
        let d = decode(&frame).unwrap();
        assert_eq!(d.qos, 1);
        assert!(d.pub_props.is_none() || !d.payload.is_empty());

        // version-aware decode: v3.1.1 — remainder is entirely payload.
        let d2 = decode_with_version(&frame, PROTOCOL_V311).unwrap();
        payload = vec![0x05, 0xAA, 0xBB];
        assert_eq!(d2.payload, payload);
        assert!(d2.pub_props.is_none());
    }

    #[test]
    fn publish_v5_with_topic_alias() {
        let mut vh = Vec::new();
        vh.extend_from_slice(&encode_string(""));
        vh.extend_from_slice(&7u16.to_be_bytes());
        let props = Properties { topic_alias: Some(3), ..Default::default() };
        vh.extend_from_slice(&encode_properties(Some(&props)));
        vh.extend_from_slice(b"data");
        let rl = vh.len();
        let mut frame = vec![(PUBLISH << 4) | 0x02];
        append_var_int(&mut frame, rl);
        frame.extend_from_slice(&vh);
        let d = decode_with_version(&frame, PROTOCOL_V5).unwrap();
        assert_eq!(d.topic, "");
        assert_eq!(d.packet_id, 7);
        assert_eq!(d.pub_props.unwrap().topic_alias, Some(3));
        assert_eq!(d.payload, b"data");
    }

    #[test]
    fn subscribe_v311_and_v5() {
        let p = Packet {
            ptype: SUBSCRIBE,
            version: PROTOCOL_V311,
            packet_id: 10,
            subscriptions: vec![Subscription { filter: "a/#".into(), qos: 1, ..Default::default() }],
            ..Default::default()
        };
        let enc = encode(&p).unwrap();
        let d = decode_with_version(&enc, PROTOCOL_V311).unwrap();
        assert_eq!(d.packet_id, 10);
        assert_eq!(d.subscriptions[0].filter, "a/#");
        assert_eq!(d.subscriptions[0].qos, 1);

        let p5 = Packet {
            ptype: SUBSCRIBE,
            version: PROTOCOL_V5,
            packet_id: 11,
            sub_props: Some(Properties::default()),
            subscriptions: vec![Subscription {
                filter: "x/+".into(),
                qos: 2,
                no_local: true,
                retain_as_published: true,
                retain_handling: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        let enc5 = encode(&p5).unwrap();
        let d5 = decode_with_version(&enc5, PROTOCOL_V5).unwrap();
        let s = &d5.subscriptions[0];
        assert_eq!(s.filter, "x/+");
        assert_eq!(s.qos, 2);
        assert!(s.no_local);
        assert!(s.retain_as_published);
        assert_eq!(s.retain_handling, 1);
    }

    #[test]
    fn subscribe_flags_violation() {
        // fixed header flags != 0x02 -> protocol violation
        let frame = vec![(SUBSCRIBE << 4) | 0x00, 0x02, 0x00, 0x01];
        assert!(matches!(decode(&frame), Err(CodecError::ProtocolViolation)));
    }

    #[test]
    fn suback_v3_first_code_zero() {
        // v3 SUBACK: packetID(0x0002) + codes [0x00, 0x01]; leading 0x00 must be a code.
        let frame = vec![(SUBACK << 4), 0x04, 0x00, 0x02, 0x00, 0x01];
        let d = decode_with_version(&frame, PROTOCOL_V311).unwrap();
        assert_eq!(d.version, PROTOCOL_V311);
        assert_eq!(d.suback_codes, vec![0x00, 0x01]);
    }

    #[test]
    fn disconnect_v5_reason() {
        let p = Packet {
            ptype: DISCONNECT,
            version: PROTOCOL_V5,
            disc_reason: 0x8B,
            disc_props: Some(Properties { reason_string: Some("Server shutting down".into()), ..Default::default() }),
            ..Default::default()
        };
        let enc = encode(&p).unwrap();
        let d = decode_with_version(&enc, PROTOCOL_V5).unwrap();
        assert_eq!(d.disc_reason, 0x8B);
        assert_eq!(d.disc_props.unwrap().reason_string.unwrap(), "Server shutting down");
        // v3 DISCONNECT encodes to empty body
        let p3 = Packet { ptype: DISCONNECT, version: PROTOCOL_V311, ..Default::default() };
        assert_eq!(encode(&p3).unwrap(), vec![0xE0, 0x00]);
    }

    #[test]
    fn pingreq_pingresp() {
        let enc = encode(&Packet { ptype: PINGREQ, version: PROTOCOL_V311, ..Default::default() }).unwrap();
        assert_eq!(enc, vec![0xC0, 0x00]);
        let d = decode(&enc).unwrap();
        assert_eq!(d.ptype, PINGREQ);
        let enc = encode(&Packet { ptype: PINGRESP, version: PROTOCOL_V5, ..Default::default() }).unwrap();
        assert_eq!(enc, vec![0xD0, 0x00]);
    }

    #[test]
    fn ack_v5_minimal_and_full() {
        // v5 PUBACK with reason 0 and no props -> 2 bytes
        let p = Packet { ptype: PUBACK, version: PROTOCOL_V5, packet_id: 5, ..Default::default() };
        let enc = encode(&p).unwrap();
        assert_eq!(enc, vec![(PUBACK << 4), 0x02, 0x00, 0x05]);
        // with reason
        let p = Packet { ptype: PUBACK, version: PROTOCOL_V5, packet_id: 5, reason: 0x97, ..Default::default() };
        let enc = encode(&p).unwrap();
        assert_eq!(enc, vec![(PUBACK << 4), 0x04, 0x00, 0x05, 0x97, 0x00]);
        let d = decode_with_version(&enc, PROTOCOL_V5).unwrap();
        assert_eq!(d.reason, 0x97);
    }

    #[test]
    fn pubrel_fixed_flags() {
        let p = Packet { ptype: PUBREL, version: PROTOCOL_V311, packet_id: 9, ..Default::default() };
        let enc = encode(&p).unwrap();
        assert_eq!(enc, vec![(PUBREL << 4) | 0x02, 0x02, 0x00, 0x09]);
    }

    #[test]
    fn malformed_varint() {
        // truncated frame
        assert!(decode(&[0xC0]).is_err());
        // length mismatch
        assert!(decode(&[0xC0, 0x05, 0x00]).is_err());
    }
}
