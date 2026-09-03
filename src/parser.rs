//! Streaming frame parser. Port of `internal/parser/frame.go`.

use std::io;

use crate::codec::{decode_var_int, CodecError, MAX_REMAINING_LENGTH};

pub fn err_incomplete() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "incomplete packet")
}

pub fn err_malformed() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "malformed remaining length")
}

pub fn err_too_large() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "packet too large")
}

/// Try to extract one complete MQTT frame from `buf`.
/// Returns Ok(Some((frame, leftover))) | Ok(None) (incomplete) | Err.
pub fn split_frame(buf: &[u8], max_packet_size: usize) -> Result<Option<(&[u8], &[u8])>, io::Error> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let (val, n) = match decode_var_int(&buf[1..]) {
        Ok(v) => v,
        Err(CodecError::MalformedPacket) => return Ok(None),
        Err(_) => return Err(err_malformed()),
    };
    if val > MAX_REMAINING_LENGTH {
        return Err(err_malformed());
    }
    if max_packet_size > 0 && val + 1 + n > max_packet_size {
        return Err(err_too_large());
    }
    let total = 1 + n + val;
    if buf.len() < total {
        return Ok(None);
    }
    Ok(Some((&buf[..total], &buf[total..])))
}

/// Streaming reader yielding complete frames. Reads from a byte-sink closure
/// so it can sit on top of TCP or WebSocket.
pub struct FrameReader<F> {
    buf: Vec<u8>,
    max_packet_size: usize,
    read: F,
}

impl<F> FrameReader<F>
where
    F: FnMut(&mut [u8]) -> io::Result<usize>,
{
    pub fn new(max_packet_size: usize, read: F) -> Self {
        Self { buf: Vec::with_capacity(4096), max_packet_size, read }
    }

    /// Block until one complete frame is available or error.
    pub async fn read_frame(&mut self) -> io::Result<Vec<u8>>
    where
        F: Unpin,
    {
        loop {
            match split_frame(&self.buf, self.max_packet_size) {
                Ok(Some((frame, leftover))) => {
                    let frame = frame.to_vec();
                    let leftover = leftover.to_vec();
                    self.buf.clear();
                    self.buf.extend_from_slice(&leftover);
                    return Ok(frame);
                }
                Ok(None) => {}
                Err(e) => {
                    self.buf.clear();
                    return Err(e);
                }
            }
            let mut tmp = [0u8; 4096];
            let n = (self.read)(&mut tmp)?;
            if n == 0 {
                if self.buf.is_empty() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
                }
                return Err(err_incomplete());
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_basic() {
        let buf = [0xC0u8, 0x00, 0xEE];
        let (frame, leftover) = split_frame(&buf, 1024).unwrap().unwrap();
        assert_eq!(frame, &[0xC0, 0x00]);
        assert_eq!(leftover, &[0xEE]);
    }

    #[test]
    fn split_incomplete() {
        let buf = [0x30u8, 0x05, 0x00];
        assert!(split_frame(&buf, 1024).unwrap().is_none());
    }

    #[test]
    fn split_too_large() {
        // remaining length 300 > max 10
        let buf = [0x30u8, 0xAC, 0x02, 0x00];
        assert!(split_frame(&buf, 10).is_err());
    }

    #[test]
    fn split_malformed_varint() {
        let buf = [0x30u8, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        assert!(split_frame(&buf, 1024).is_err());
    }
}
