//! MQTT variable byte integer + UTF-8 string + binary helpers.
//! Mirrors `internal/codec/varint.go` and the frame-level helpers of
//! `internal/parser/frame.go` (max remaining length 256MB).

pub const MAX_REMAINING_LENGTH: usize = 268_435_455; // 256MB

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    MalformedPacket,
    MalformedRemainingLength,
    PacketTooLarge,
    VarIntOverflow,
    IncompletePacket,
    TooManyUserProperties,
    UnknownProperty,
    UnsupportedProtocol,
    InvalidQoS,
    ProtocolViolation,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            CodecError::MalformedPacket => "malformed packet",
            CodecError::MalformedRemainingLength => "malformed remaining length",
            CodecError::PacketTooLarge => "packet too large",
            CodecError::VarIntOverflow => "varint overflow",
            CodecError::IncompletePacket => "incomplete packet",
            CodecError::TooManyUserProperties => "too many user properties",
            CodecError::UnknownProperty => "unknown property",
            CodecError::UnsupportedProtocol => "unsupported protocol version",
            CodecError::InvalidQoS => "invalid QoS",
            CodecError::ProtocolViolation => "protocol violation",
        };
        f.write_str(s)
    }
}

impl std::error::Error for CodecError {}

/// Append an MQTT varint (variable byte integer) to `dst`.
pub fn append_var_int(dst: &mut Vec<u8>, n: usize) {
    let mut n = n;
    loop {
        let mut b = (n & 0x7F) as u8;
        n >>= 7;
        if n > 0 {
            b |= 0x80;
        }
        dst.push(b);
        if n == 0 {
            break;
        }
    }
}

pub fn encode_var_int(n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    append_var_int(&mut out, n);
    out
}

pub fn var_int_len(n: usize) -> usize {
    let mut n = n;
    let mut l = 1;
    while n >= 128 {
        n >>= 7;
        l += 1;
    }
    l
}

/// Decode varint. Returns (value, bytes consumed).
/// Mirrors Go: 4 continuation bytes => ErrVarIntOverflow, short input => ErrMalformedPacket.
pub fn decode_var_int(src: &[u8]) -> Result<(usize, usize), CodecError> {
    let mut val: usize = 0;
    let mut shift: u32 = 0;
    for i in 0..src.len().min(4) {
        let b = src[i];
        val |= ((b & 0x7F) as usize) << shift;
        shift += 7;
        if b & 0x80 == 0 {
            return Ok((val, i + 1));
        }
        if i == 3 {
            return Err(CodecError::VarIntOverflow);
        }
    }
    Err(CodecError::MalformedPacket)
}

pub fn decode_u16(b: &[u8]) -> u16 {
    ((b[0] as u16) << 8) | b[1] as u16
}

pub fn decode_u32(b: &[u8]) -> u32 {
    ((b[0] as u32) << 24) | ((b[1] as u32) << 16) | ((b[2] as u32) << 8) | b[3] as u32
}

pub fn encode_string(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(2 + b.len());
    out.push((b.len() >> 8) as u8);
    out.push((b.len() & 0xFF) as u8);
    out.extend_from_slice(b);
    out
}

/// Decode a MQTT UTF-8 string at `pos`. Returns (string, new position).
pub fn decode_string(src: &[u8], pos: usize) -> Result<(String, usize), CodecError> {
    if pos + 2 > src.len() {
        return Err(CodecError::MalformedPacket);
    }
    let l = ((src[pos] as usize) << 8) | src[pos + 1] as usize;
    if pos + 2 + l > src.len() {
        return Err(CodecError::MalformedPacket);
    }
    let s = String::from_utf8_lossy(&src[pos + 2..pos + 2 + l]).into_owned();
    Ok((s, pos + 2 + l))
}

pub fn encode_binary(b: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + b.len());
    out.push((b.len() >> 8) as u8);
    out.push((b.len() & 0xFF) as u8);
    out.extend_from_slice(b);
    out
}

pub fn decode_binary(src: &[u8], pos: usize) -> Result<(Vec<u8>, usize), CodecError> {
    if pos + 2 > src.len() {
        return Err(CodecError::MalformedPacket);
    }
    let l = ((src[pos] as usize) << 8) | src[pos + 1] as usize;
    if pos + 2 + l > src.len() {
        return Err(CodecError::MalformedPacket);
    }
    Ok((src[pos + 2..pos + 2 + l].to_vec(), pos + 2 + l))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for n in [0usize, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455] {
            let enc = encode_var_int(n);
            let (val, used) = decode_var_int(&enc).unwrap();
            assert_eq!(val, n);
            assert_eq!(used, enc.len());
        }
    }

    #[test]
    fn varint_overflow() {
        let enc = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        // Go behavior: 4 bytes with continuation on the 4th => overflow
        let r = decode_var_int(&enc);
        assert!(matches!(r, Err(CodecError::VarIntOverflow)));
    }

    #[test]
    fn varint_len() {
        assert_eq!(var_int_len(0), 1);
        assert_eq!(var_int_len(127), 1);
        assert_eq!(var_int_len(128), 2);
        assert_eq!(var_int_len(16383), 2);
        assert_eq!(var_int_len(16384), 3);
        assert_eq!(var_int_len(268435455), 4);
    }

    #[test]
    fn string_roundtrip() {
        let enc = encode_string("hello");
        let (s, pos) = decode_string(&enc, 0).unwrap();
        assert_eq!(s, "hello");
        assert_eq!(pos, enc.len());
        assert!(decode_string(&enc[..3], 0).is_err());
    }

    #[test]
    fn binary_roundtrip() {
        let data = vec![0u8, 0xFF, 0x10];
        let enc = encode_binary(&data);
        let (b, pos) = decode_binary(&enc, 0).unwrap();
        assert_eq!(b, data);
        assert_eq!(pos, enc.len());
    }
}
