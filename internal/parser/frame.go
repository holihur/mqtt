package parser

import (
	"errors"
	"io"
)

var (
	ErrMalformedRemainingLength = errors.New("malformed remaining length")
	ErrPacketTooLarge           = errors.New("packet too large")
	ErrIncompletePacket         = errors.New("incomplete packet")
)

const MaxRemainingLength = 268435455 // 256MB

// DecodeVarInt decodes MQTT Variable Byte Integer (1-4 bytes).
// Returns value, bytes consumed, error.
func DecodeVarInt(src []byte) (int, int, error) {
	var val int
	var shift uint
	for i := 0; i < len(src) && i < 4; i++ {
		b := src[i]
		val |= int(b&0x7F) << shift
		shift += 7
		if b&0x80 == 0 {
			return val, i + 1, nil
		}
		if i == 3 {
			return 0, 0, ErrMalformedRemainingLength
		}
	}
	return 0, 0, ErrIncompletePacket
}

// EncodeVarInt encodes int to MQTT Variable Byte Integer.
func EncodeVarInt(n int) []byte {
	if n < 0 {
		n = 0
	}
	var out []byte
	for {
		b := byte(n & 0x7F)
		n >>= 7
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			break
		}
	}
	return out
}

// SplitFrame tries to extract one complete MQTT frame from buf.
// Returns frame (including fixed header + remaining length + payload), leftover, error.
// If incomplete, returns nil, buf, ErrIncompletePacket.
func SplitFrame(buf []byte, maxPacketSize int) ([]byte, []byte, error) {
	if len(buf) < 2 {
		return nil, buf, ErrIncompletePacket
	}
	// fixed header is 1 byte, then remaining length varint
	// find varint end
	var val int
	var n int
	var err error
	// try decode varint starting at offset 1
	val, n, err = DecodeVarInt(buf[1:])
	if err != nil {
		if errors.Is(err, ErrIncompletePacket) {
			// need more bytes for varint; check if buf has enough
			// if buf length < 5 (1+4) and varint continues, it's incomplete
			return nil, buf, ErrIncompletePacket
		}
		return nil, buf, err
	}
	if val < 0 || val > MaxRemainingLength {
		return nil, buf, ErrMalformedRemainingLength
	}
	if maxPacketSize > 0 && val+1+n > maxPacketSize {
		return nil, buf, ErrPacketTooLarge
	}
	total := 1 + n + val
	if len(buf) < total {
		return nil, buf, ErrIncompletePacket
	}
	return buf[:total], buf[total:], nil
}

// Reader is a streaming parser that reads from io.Reader and yields frames.
type Reader struct {
	src           io.Reader
	buf           []byte
	tmp           [4096]byte
	maxPacketSize int
}

func NewReader(src io.Reader, maxPacketSize int) *Reader {
	return &Reader{src: src, buf: make([]byte, 0, 4096), maxPacketSize: maxPacketSize}
}

// ReadFrame blocks until one complete frame is available or error.
func (r *Reader) ReadFrame() ([]byte, error) {
	for {
		frame, leftover, err := SplitFrame(r.buf, r.maxPacketSize)
		if err == nil {
			cp := make([]byte, len(frame))
			copy(cp, frame)
			n := copy(r.buf, leftover)
			r.buf = r.buf[:n]
			return cp, nil
		}
		if err != nil && !errors.Is(err, ErrIncompletePacket) {
			return nil, err
		}
		n, readErr := r.src.Read(r.tmp[:])
		if n > 0 {
			r.buf = append(r.buf, r.tmp[:n]...)
		}
		if readErr != nil {
			if readErr == io.EOF {
				if len(r.buf) == 0 {
					return nil, io.EOF
				}
				return nil, ErrIncompletePacket
			}
			return nil, readErr
		}
	}
}
