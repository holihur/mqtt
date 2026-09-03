//! Broker utility helpers: Go-compatible formatting (RFC3339, %q quoting,
//! protocol name mapping) and shared context helpers.

use std::time::SystemTime;

/// Format a SystemTime like Go's time.Time JSON marshaling (RFC3339Nano with
/// trailing fractional zeros removed, `Z` for UTC).
pub fn go_rfc3339(t: &SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = (*t).into();
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = dt.timestamp_subsec_nanos();
    if nanos == 0 {
        return format!("{}Z", base);
    }
    let mut frac = format!("{:09}", nanos);
    while frac.ends_with('0') {
        frac.pop();
    }
    format!("{}.{}Z", base, frac)
}

/// Emulate Go's `%q` quoting for simple strings (double quotes, escapes for
/// backslash, quote, and common control characters).
pub fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Map MQTT protocol level to a readable version name (mirrors `protocolName`).
pub fn protocol_name(v: u8) -> String {
    match v {
        crate::codec::PROTOCOL_V31 => "3.1".to_string(),
        crate::codec::PROTOCOL_V311 => "3.1.1".to_string(),
        crate::codec::PROTOCOL_V5 => "5.0".to_string(),
        other => format!("unknown({})", other),
    }
}

pub fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn system_now() -> SystemTime {
    SystemTime::now()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn rfc3339_format() {
        let t = SystemTime::UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789);
        assert_eq!(go_rfc3339(&t), "2023-11-14T22:13:20.123456789Z");
        let t = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
        assert_eq!(go_rfc3339(&t), "2023-11-14T22:13:20Z");
        let t = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_500);
        assert_eq!(go_rfc3339(&t), "2023-11-14T22:13:20.5Z");
    }

    #[test]
    fn quote_format() {
        assert_eq!(go_quote("abc"), "\"abc\"");
        assert_eq!(go_quote("a\"b"), "\"a\\\"b\"");
        assert_eq!(go_quote("a\nb"), "\"a\\nb\"");
    }

    #[test]
    fn protocol_names() {
        assert_eq!(protocol_name(3), "3.1");
        assert_eq!(protocol_name(4), "3.1.1");
        assert_eq!(protocol_name(5), "5.0");
        assert_eq!(protocol_name(9), "unknown(9)");
    }
}
