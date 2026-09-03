//! MQTT 5.0 properties encode/decode. Faithful port of `internal/codec/properties.go`.
//! Unknown property IDs are rejected (fail closed).

use super::varint::*;

pub mod prop_ids {
    pub const PAYLOAD_FORMAT_INDICATOR: u8 = 0x01;
    pub const MESSAGE_EXPIRY_INTERVAL: u8 = 0x02;
    pub const CONTENT_TYPE: u8 = 0x03;
    pub const RESPONSE_TOPIC: u8 = 0x08;
    pub const CORRELATION_DATA: u8 = 0x09;
    pub const SUBSCRIPTION_ID: u8 = 0x0B;
    pub const SESSION_EXPIRY_INTERVAL: u8 = 0x11;
    pub const ASSIGNED_CLIENT_ID: u8 = 0x12;
    pub const SERVER_KEEP_ALIVE: u8 = 0x13;
    pub const AUTH_METHOD: u8 = 0x15;
    pub const AUTH_DATA: u8 = 0x16;
    pub const REQUEST_PROBLEM_INFO: u8 = 0x17;
    pub const WILL_DELAY_INTERVAL: u8 = 0x18;
    pub const REQUEST_RESPONSE_INFO: u8 = 0x19;
    pub const RESPONSE_INFO: u8 = 0x1A;
    pub const SERVER_REFERENCE: u8 = 0x1C;
    pub const REASON_STRING: u8 = 0x1F;
    pub const RECEIVE_MAXIMUM: u8 = 0x21;
    pub const TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
    pub const TOPIC_ALIAS: u8 = 0x23;
    pub const MAXIMUM_QOS: u8 = 0x24;
    pub const RETAIN_AVAILABLE: u8 = 0x25;
    pub const USER_PROPERTY: u8 = 0x26;
    pub const MAXIMUM_PACKET_SIZE: u8 = 0x27;
    pub const WILDCARD_SUB_AVAILABLE: u8 = 0x28;
    pub const SUB_ID_AVAILABLE: u8 = 0x29;
    pub const SHARED_SUB_AVAILABLE: u8 = 0x2A;
}

use prop_ids::*;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserProperty {
    pub key: String,
    pub val: String,
}

/// Properties for MQTT5 (subset + generic handling). `None` = absent, like Go's
/// nil pointers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Properties {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
    pub request_response_info: Option<u8>,
    pub request_problem_info: Option<u8>,
    pub user: Vec<UserProperty>,
    pub auth_method: Option<String>,
    pub auth_data: Vec<u8>,
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub will_delay_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<String>,
    pub correlation_data: Vec<u8>,
    pub subscription_id: Vec<u32>,
    pub content_type: Option<String>,
    pub server_keep_alive: Option<u16>,
    pub assigned_client_id: Option<String>,
    pub reason_string: Option<String>,
    pub wildcard_sub_available: Option<u8>,
    pub sub_id_available: Option<u8>,
    pub shared_sub_available: Option<u8>,
    pub retain_available: Option<u8>,
    pub maximum_qos: Option<u8>,
    pub server_reference: Option<String>,
}

impl Properties {
    pub fn is_empty_block(&self) -> bool {
        *self == Properties::default()
    }
}

fn decode_bounded_string(src: &[u8], pos: usize, end: usize) -> Result<(String, usize), CodecError> {
    let (s, np) = decode_string(src, pos)?;
    if np > end {
        return Err(CodecError::MalformedPacket);
    }
    Ok((s, np))
}

fn decode_bounded_binary(src: &[u8], pos: usize, end: usize) -> Result<(Vec<u8>, usize), CodecError> {
    let (b, np) = decode_binary(src, pos)?;
    if np > end {
        return Err(CodecError::MalformedPacket);
    }
    Ok((b, np))
}

/// Decode a properties block starting at `pos`. Returns (props, end position).
/// Like Go: empty/absent block yields default props and unchanged position.
pub fn decode_properties(src: &[u8], pos: usize) -> Result<(Properties, usize), CodecError> {
    if pos >= src.len() {
        return Ok((Properties::default(), pos));
    }
    let (val, n) = decode_var_int(&src[pos..])?;
    let end = pos + n + val;
    if end > src.len() {
        return Err(CodecError::MalformedPacket);
    }
    let mut p = Properties::default();
    let mut i = pos + n;
    while i < end {
        if i >= end {
            return Err(CodecError::MalformedPacket);
        }
        let id = src[i];
        i += 1;
        match id {
            PAYLOAD_FORMAT_INDICATOR => {
                if i >= end {
                    return Err(CodecError::MalformedPacket);
                }
                p.payload_format_indicator = Some(src[i]);
                i += 1;
            }
            MESSAGE_EXPIRY_INTERVAL => {
                if i + 4 > end {
                    return Err(CodecError::MalformedPacket);
                }
                p.message_expiry_interval = Some(decode_u32(&src[i..]));
                i += 4;
            }
            CONTENT_TYPE => {
                let (s, np) = decode_bounded_string(src, i, end)?;
                i = np;
                p.content_type = Some(s);
            }
            RESPONSE_TOPIC => {
                let (s, np) = decode_bounded_string(src, i, end)?;
                i = np;
                p.response_topic = Some(s);
            }
            CORRELATION_DATA => {
                let (b, np) = decode_bounded_binary(src, i, end)?;
                i = np;
                p.correlation_data = b;
            }
            SUBSCRIPTION_ID => {
                if i >= end {
                    return Err(CodecError::MalformedPacket);
                }
                let (v, n2) = decode_var_int(&src[i..end])?;
                i += n2;
                p.subscription_id.push(v as u32);
            }
            SESSION_EXPIRY_INTERVAL => {
                if i + 4 > end {
                    return Err(CodecError::MalformedPacket);
                }
                p.session_expiry_interval = Some(decode_u32(&src[i..]));
                i += 4;
            }
            WILL_DELAY_INTERVAL => {
                if i + 4 > end {
                    return Err(CodecError::MalformedPacket);
                }
                p.will_delay_interval = Some(decode_u32(&src[i..]));
                i += 4;
            }
            ASSIGNED_CLIENT_ID => {
                let (s, np) = decode_string(src, i)?;
                i = np;
                p.assigned_client_id = Some(s);
            }
            SERVER_KEEP_ALIVE => {
                if i + 2 > end {
                    return Err(CodecError::MalformedPacket);
                }
                p.server_keep_alive = Some(decode_u16(&src[i..]));
                i += 2;
            }
            AUTH_METHOD => {
                let (s, np) = decode_bounded_string(src, i, end)?;
                i = np;
                p.auth_method = Some(s);
            }
            AUTH_DATA => {
                let (b, np) = decode_bounded_binary(src, i, end)?;
                i = np;
                p.auth_data = b;
            }
            REQUEST_PROBLEM_INFO => {
                if i >= end {
                    return Err(CodecError::MalformedPacket);
                }
                p.request_problem_info = Some(src[i]);
                i += 1;
            }
            REQUEST_RESPONSE_INFO => {
                if i >= end {
                    return Err(CodecError::MalformedPacket);
                }
                p.request_response_info = Some(src[i]);
                i += 1;
            }
            RESPONSE_INFO => {
                let (_, np) = decode_bounded_string(src, i, end)?;
                i = np;
            }
            SERVER_REFERENCE => {
                let (s, np) = decode_bounded_string(src, i, end)?;
                i = np;
                p.server_reference = Some(s);
            }
            REASON_STRING => {
                let (s, np) = decode_bounded_string(src, i, end)?;
                i = np;
                p.reason_string = Some(s);
            }
            RECEIVE_MAXIMUM => {
                if i + 2 > end {
                    return Err(CodecError::MalformedPacket);
                }
                p.receive_maximum = Some(decode_u16(&src[i..]));
                i += 2;
            }
            TOPIC_ALIAS_MAXIMUM => {
                if i + 2 > end {
                    return Err(CodecError::MalformedPacket);
                }
                p.topic_alias_maximum = Some(decode_u16(&src[i..]));
                i += 2;
            }
            TOPIC_ALIAS => {
                if i + 2 > end {
                    return Err(CodecError::MalformedPacket);
                }
                p.topic_alias = Some(decode_u16(&src[i..]));
                i += 2;
            }
            MAXIMUM_QOS => {
                if i >= end {
                    return Err(CodecError::MalformedPacket);
                }
                p.maximum_qos = Some(src[i]);
                i += 1;
            }
            RETAIN_AVAILABLE => {
                if i >= end {
                    return Err(CodecError::MalformedPacket);
                }
                p.retain_available = Some(src[i]);
                i += 1;
            }
            USER_PROPERTY => {
                if p.user.len() >= 10 {
                    return Err(CodecError::TooManyUserProperties);
                }
                let (k, np) = decode_bounded_string(src, i, end)?;
                if k.len() > 256 || k.is_empty() {
                    return Err(CodecError::MalformedPacket);
                }
                let (v, np2) = decode_bounded_string(src, np, end)?;
                if v.len() > 1024 {
                    return Err(CodecError::MalformedPacket);
                }
                i = np2;
                p.user.push(UserProperty { key: k, val: v });
            }
            MAXIMUM_PACKET_SIZE => {
                if i + 4 > end {
                    return Err(CodecError::MalformedPacket);
                }
                p.maximum_packet_size = Some(decode_u32(&src[i..]));
                i += 4;
            }
            WILDCARD_SUB_AVAILABLE => {
                if i >= end {
                    return Err(CodecError::MalformedPacket);
                }
                p.wildcard_sub_available = Some(src[i]);
                i += 1;
            }
            SUB_ID_AVAILABLE => {
                if i >= end {
                    return Err(CodecError::MalformedPacket);
                }
                p.sub_id_available = Some(src[i]);
                i += 1;
            }
            SHARED_SUB_AVAILABLE => {
                if i >= end {
                    return Err(CodecError::MalformedPacket);
                }
                p.shared_sub_available = Some(src[i]);
                i += 1;
            }
            _ => {
                // Unknown property ID: value length unknowable, fail closed.
                return Err(CodecError::UnknownProperty);
            }
        }
    }
    Ok((p, end))
}

/// Encode properties into a wire block (varint length + body).
pub fn encode_properties(p: Option<&Properties>) -> Vec<u8> {
    let p = match p {
        None => return encode_var_int(0),
        Some(p) => p,
    };
    let mut body: Vec<u8> = Vec::new();

    macro_rules! append_u16 {
        ($id:expr, $v:expr) => {{
            body.push($id);
            body.extend_from_slice(&($v).to_be_bytes());
        }};
    }
    macro_rules! append_u32 {
        ($id:expr, $v:expr) => {{
            body.push($id);
            body.extend_from_slice(&($v).to_be_bytes());
        }};
    }
    macro_rules! append_byte {
        ($id:expr, $v:expr) => {{
            body.push($id);
            body.push($v);
        }};
    }
    macro_rules! append_str {
        ($id:expr, $s:expr) => {{
            body.push($id);
            body.extend_from_slice(&encode_string($s));
        }};
    }
    macro_rules! append_bin {
        ($id:expr, $b:expr) => {{
            body.push($id);
            body.extend_from_slice(&encode_binary($b));
        }};
    }

    if let Some(v) = p.payload_format_indicator {
        append_byte!(PAYLOAD_FORMAT_INDICATOR, v);
    }
    if let Some(v) = p.message_expiry_interval {
        append_u32!(MESSAGE_EXPIRY_INTERVAL, v);
    }
    if let Some(v) = &p.content_type {
        append_str!(CONTENT_TYPE, v);
    }
    if let Some(v) = &p.response_topic {
        append_str!(RESPONSE_TOPIC, v);
    }
    if !p.correlation_data.is_empty() {
        append_bin!(CORRELATION_DATA, &p.correlation_data);
    }
    for sid in &p.subscription_id {
        body.push(SUBSCRIPTION_ID);
        append_var_int(&mut body, *sid as usize);
    }
    if let Some(v) = p.session_expiry_interval {
        append_u32!(SESSION_EXPIRY_INTERVAL, v);
    }
    if let Some(v) = &p.assigned_client_id {
        append_str!(ASSIGNED_CLIENT_ID, v);
    }
    if let Some(v) = p.server_keep_alive {
        append_u16!(SERVER_KEEP_ALIVE, v);
    }
    if let Some(v) = &p.auth_method {
        append_str!(AUTH_METHOD, v);
    }
    if !p.auth_data.is_empty() {
        append_bin!(AUTH_DATA, &p.auth_data);
    }
    if let Some(v) = p.request_problem_info {
        append_byte!(REQUEST_PROBLEM_INFO, v);
    }
    if let Some(v) = p.request_response_info {
        append_byte!(REQUEST_RESPONSE_INFO, v);
    }
    if let Some(v) = &p.server_reference {
        append_str!(SERVER_REFERENCE, v);
    }
    if let Some(v) = &p.reason_string {
        append_str!(REASON_STRING, v);
    }
    if let Some(v) = p.receive_maximum {
        append_u16!(RECEIVE_MAXIMUM, v);
    }
    if let Some(v) = p.topic_alias_maximum {
        append_u16!(TOPIC_ALIAS_MAXIMUM, v);
    }
    if let Some(v) = p.topic_alias {
        append_u16!(TOPIC_ALIAS, v);
    }
    if let Some(v) = p.maximum_qos {
        append_byte!(MAXIMUM_QOS, v);
    }
    if let Some(v) = p.retain_available {
        append_byte!(RETAIN_AVAILABLE, v);
    }
    for up in &p.user {
        body.push(USER_PROPERTY);
        body.extend_from_slice(&encode_string(&up.key));
        body.extend_from_slice(&encode_string(&up.val));
    }
    if let Some(v) = p.maximum_packet_size {
        append_u32!(MAXIMUM_PACKET_SIZE, v);
    }
    if let Some(v) = p.wildcard_sub_available {
        append_byte!(WILDCARD_SUB_AVAILABLE, v);
    }
    if let Some(v) = p.sub_id_available {
        append_byte!(SUB_ID_AVAILABLE, v);
    }
    if let Some(v) = p.shared_sub_available {
        append_byte!(SHARED_SUB_AVAILABLE, v);
    }
    let mut out = encode_var_int(body.len());
    out.extend_from_slice(&body);
    out
}

/// Encode will-specific properties (WillDelayInterval handled separately).
pub fn encode_will_properties(delay: Option<u32>, props: Option<&Properties>) -> Vec<u8> {
    let has_user = props.map(|p| !p.user.is_empty()).unwrap_or(false);
    if delay.is_none() && !has_user && props.is_none() {
        return encode_var_int(0);
    }
    let mut body: Vec<u8> = Vec::new();
    if let Some(d) = delay {
        body.push(WILL_DELAY_INTERVAL);
        body.extend_from_slice(&d.to_be_bytes());
    }
    if let Some(props) = props {
        for up in &props.user {
            body.push(USER_PROPERTY);
            body.extend_from_slice(&encode_string(&up.key));
            body.extend_from_slice(&encode_string(&up.val));
        }
        if let Some(v) = props.payload_format_indicator {
            body.push(PAYLOAD_FORMAT_INDICATOR);
            body.push(v);
        }
        if let Some(v) = props.message_expiry_interval {
            body.push(MESSAGE_EXPIRY_INTERVAL);
            body.extend_from_slice(&v.to_be_bytes());
        }
        if let Some(v) = &props.content_type {
            body.push(CONTENT_TYPE);
            body.extend_from_slice(&encode_string(v));
        }
        if let Some(v) = &props.response_topic {
            body.push(RESPONSE_TOPIC);
            body.extend_from_slice(&encode_string(v));
        }
        if !props.correlation_data.is_empty() {
            body.push(CORRELATION_DATA);
            body.extend_from_slice(&encode_binary(&props.correlation_data));
        }
    }
    let mut out = encode_var_int(body.len());
    out.extend_from_slice(&body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_properties() {
        let enc = encode_properties(None);
        assert_eq!(enc, vec![0]);
        let enc2 = encode_properties(Some(&Properties::default()));
        assert_eq!(enc2, vec![0]);
        // empty block: length varint 0
        let (p, pos) = decode_properties(&[0x00], 0).unwrap();
        assert_eq!(pos, 1);
        assert!(p.is_empty_block());
        // 0xAA = varint continuation with no following byte -> malformed
        assert!(decode_properties(&[0xAA], 0).is_err());
    }

    #[test]
    fn user_property_roundtrip() {
        let props = Properties {
            user: vec![UserProperty { key: "k".into(), val: "v".into() }],
            ..Default::default()
        };
        let enc = encode_properties(Some(&props));
        let (p, _) = decode_properties(&enc, 0).unwrap();
        assert_eq!(p.user.len(), 1);
        assert_eq!(p.user[0].key, "k");
        assert_eq!(p.user[0].val, "v");
    }

    #[test]
    fn too_many_user_properties() {
        let mut src = vec![0x00]; // placeholder, real block built manually
        src.clear();
        for _ in 0..11 {
            src.push(USER_PROPERTY);
            src.extend_from_slice(&encode_string("a"));
            src.extend_from_slice(&encode_string("b"));
        }
        let mut block = encode_var_int(src.len());
        block.extend_from_slice(&src);
        let r = decode_properties(&block, 0);
        assert!(matches!(r, Err(CodecError::TooManyUserProperties)));
    }

    #[test]
    fn unknown_property_rejected() {
        let block = [0x01, 0xEE];
        let r = decode_properties(&block, 0);
        assert!(matches!(r, Err(CodecError::UnknownProperty)));
    }

    #[test]
    fn session_expiry_roundtrip() {
        let props = Properties { session_expiry_interval: Some(300), ..Default::default() };
        let enc = encode_properties(Some(&props));
        let (p, _) = decode_properties(&enc, 0).unwrap();
        assert_eq!(p.session_expiry_interval, Some(300));
    }
}
