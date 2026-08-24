package codec

import "errors"

var ErrVarIntOverflow = errors.New("varint overflow")

func encodeVarInt(n int) []byte {
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

func decodeVarInt(src []byte) (int, int, error) {
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
			return 0, 0, ErrVarIntOverflow
		}
	}
	return 0, 0, ErrMalformedPacket
}

func encodeUint16(v uint16) []byte { return []byte{byte(v >> 8), byte(v)} }
func decodeUint16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }

func encodeUint32(v uint32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}
func decodeUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func encodeString(s string) []byte {
	b := []byte(s)
	return append(append([]byte{byte(len(b) >> 8), byte(len(b))}, b...), []byte{}...)
}

func decodeString(src []byte, pos int) (string, int, error) {
	if pos+2 > len(src) {
		return "", 0, ErrMalformedPacket
	}
	l := int(src[pos])<<8 | int(src[pos+1])
	if pos+2+l > len(src) {
		return "", 0, ErrMalformedPacket
	}
	return string(src[pos+2 : pos+2+l]), pos + 2 + l, nil
}

func encodeBinary(b []byte) []byte {
	return append([]byte{byte(len(b) >> 8), byte(len(b))}, b...)
}
func decodeBinary(src []byte, pos int) ([]byte, int, error) {
	if pos+2 > len(src) {
		return nil, 0, ErrMalformedPacket
	}
	l := int(src[pos])<<8 | int(src[pos+1])
	if pos+2+l > len(src) {
		return nil, 0, ErrMalformedPacket
	}
	cp := make([]byte, l)
	copy(cp, src[pos+2:pos+2+l])
	return cp, pos + 2 + l, nil
}
