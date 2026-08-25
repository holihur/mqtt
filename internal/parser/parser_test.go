package parser

import (
	"bytes"
	"io"
	"testing"
)

func TestDecodeVarInt(t *testing.T) {
	cases := []struct {
		enc []byte
		val int
		n   int
		ok  bool
	}{
		{[]byte{0x00}, 0, 1, true},
		{[]byte{0x7F}, 127, 1, true},
		{[]byte{0x80, 0x01}, 128, 2, true},
		{[]byte{0xFF, 0xFF, 0xFF, 0x7F}, MaxRemainingLength, 4, true},
		{[]byte{0x80}, 0, 0, false},                   // incomplete
		{[]byte{0xFF, 0xFF, 0xFF, 0x80}, 0, 0, false}, // malformed continuation
	}
	for _, tc := range cases {
		val, n, err := DecodeVarInt(tc.enc)
		if tc.ok && err != nil {
			t.Errorf("DecodeVarInt %v should ok got err %v", tc.enc, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("DecodeVarInt %v should err", tc.enc)
		}
		if tc.ok && (val != tc.val || n != tc.n) {
			t.Errorf("DecodeVarInt %v got %d,%d want %d,%d", tc.enc, val, n, tc.val, tc.n)
		}
	}
}

func TestDecodeVarIntIncomplete(t *testing.T) {
	_, _, err := DecodeVarInt([]byte{})
	if err == nil {
		t.Fatalf("empty should err")
	}
	_, _, err = DecodeVarInt([]byte{0x80, 0x80})
	if err == nil {
		t.Fatalf("incomplete varint should err")
	}
}

func TestEncodeDecodeVarIntRoundTrip(t *testing.T) {
	vals := []int{0, 1, 127, 128, 16383, 16384, 2097151, MaxRemainingLength}
	for _, v := range vals {
		enc := EncodeVarInt(v)
		dec, n, err := DecodeVarInt(enc)
		if err != nil || dec != v || n != len(enc) {
			t.Fatalf("roundtrip %d failed enc %v dec %d err %v", v, enc, dec, err)
		}
	}
	// negative encodes as 0
	enc := EncodeVarInt(-5)
	dec, _, _ := DecodeVarInt(enc)
	if dec != 0 {
		t.Fatalf("negative should be 0")
	}
}

func TestEncodeVarIntNeg(t *testing.T) {
	enc := EncodeVarInt(-1)
	if len(enc) != 1 || enc[0] != 0 {
		t.Fatalf("neg encode wrong")
	}
}

func TestSplitFrameBasic(t *testing.T) {
	// PINGREQ: 0xC0 0x00
	buf := []byte{0xC0, 0x00}
	frame, leftover, err := SplitFrame(buf, 1024)
	if err != nil || len(frame) != 2 || len(leftover) != 0 {
		t.Fatalf("split basic failed %v %v", err, frame)
	}
	// incomplete: len <2
	_, _, err = SplitFrame([]byte{0xC0}, 1024)
	if err != ErrIncompletePacket {
		t.Fatalf("should incomplete")
	}
	// extra bytes: should return first frame and leftover
	buf2 := []byte{0xC0, 0x00, 0xC0, 0x00}
	f, left, err := SplitFrame(buf2, 1024)
	if err != nil || len(f) != 2 || len(left) != 2 {
		t.Fatalf("split with leftover failed")
	}
}

func TestSplitFrameRemainingLength(t *testing.T) {
	// PUBLISH with remaining length 5, payload "hello" style: fixed header 0x30, remaining 5, payload bytes
	// Build frame: 0x30 0x05 'h','e','l','l','o'
	buf := []byte{0x30, 0x05, 'h', 'e', 'l', 'l', 'o'}
	f, left, err := SplitFrame(buf, 1024)
	if err != nil || len(f) != 7 || len(left) != 0 {
		t.Fatalf("remaining length frame failed")
	}
	// incomplete payload
	buf = []byte{0x30, 0x05, 'h', 'e'}
	_, _, err = SplitFrame(buf, 1024)
	if err != ErrIncompletePacket {
		t.Fatalf("should incomplete payload")
	}
}

func TestSplitFrameVarIntOverflow(t *testing.T) {
	// malformed remaining length: continuation bit stuck
	buf := []byte{0x30, 0xFF, 0xFF, 0xFF, 0x80}
	_, _, err := SplitFrame(buf, 1024)
	if err != ErrMalformedRemainingLength {
		t.Fatalf("should malformed remaining length got %v", err)
	}
}

func TestSplitFramePacketTooLarge(t *testing.T) {
	// remaining length 10 but maxPacketSize 5 -> too large
	buf := []byte{0x30, 0x0A, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	_, _, err := SplitFrame(buf, 5)
	if err != ErrPacketTooLarge {
		t.Fatalf("should too large got %v", err)
	}
}

func TestSplitFrameTooLargeZero(t *testing.T) {
	buf := []byte{0x30, 0x0A, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	// maxPacketSize 0 means unlimited, should be incomplete if not enough bytes but not too large
	_, _, err := SplitFrame(buf, 0)
	if err == ErrPacketTooLarge {
		t.Fatalf("0 max should not trigger too large")
	}
}

func TestSplitFrameIncompleteVarInt(t *testing.T) {
	// buf len 2 but varint incomplete (needs more bytes)
	buf := []byte{0x30, 0x80}
	_, _, err := SplitFrame(buf, 1024)
	if err != ErrIncompletePacket {
		t.Fatalf("incomplete varint should be incomplete packet got %v", err)
	}
}

func TestReaderReadFrame(t *testing.T) {
	// Use bytes.Reader as src
	data := []byte{0xC0, 0x00, 0xC0, 0x00, 0x30, 0x02, 'h', 'i'}
	src := bytes.NewReader(data)
	r := NewReader(src, 1024)
	f1, err := r.ReadFrame()
	if err != nil || len(f1) != 2 || f1[0] != 0xC0 {
		t.Fatalf("read frame1 failed %v %v", err, f1)
	}
	f2, err := r.ReadFrame()
	if err != nil || f2[0] != 0xC0 {
		t.Fatalf("read frame2 failed")
	}
	f3, err := r.ReadFrame()
	if err != nil || f3[0] != 0x30 {
		t.Fatalf("read frame3 failed")
	}
	_, err = r.ReadFrame()
	if err != io.EOF {
		t.Fatalf("should EOF got %v", err)
	}
}

func TestReaderIncomplete(t *testing.T) {
	// src provides incomplete frame then EOF
	src := bytes.NewReader([]byte{0x30, 0x05, 'h'})
	r := NewReader(src, 1024)
	_, err := r.ReadFrame()
	if err != ErrIncompletePacket {
		t.Fatalf("should incomplete got %v", err)
	}
}

func TestReaderPacketTooLarge(t *testing.T) {
	src := bytes.NewReader([]byte{0x30, 0x0A, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	r := NewReader(src, 5)
	_, err := r.ReadFrame()
	if err != ErrPacketTooLarge {
		t.Fatalf("should too large got %v", err)
	}
}

func TestSplitFrameMaxRemaining(t *testing.T) {
	// Max remain 256MB encoded as 0xFF 0xFF 0xFF 0x7F
	enc := EncodeVarInt(MaxRemainingLength)
	if len(enc) != 4 {
		t.Fatalf("max enc len 4 got %d", len(enc))
	}
	dec, n, err := DecodeVarInt(enc)
	if err != nil || dec != MaxRemainingLength || n != 4 {
		t.Fatalf("max decode failed")
	}
}

func TestReaderChunked(t *testing.T) {
	// simulate chunked reads: use custom reader that returns 1 byte at a time
	pr, pw := io.Pipe()
	go func() {
		pw.Write([]byte{0xC0})
		pw.Write([]byte{0x00})
		pw.Write([]byte{0xC0, 0x00})
		pw.Close()
	}()
	r := NewReader(pr, 1024)
	f, err := r.ReadFrame()
	if err != nil || f[0] != 0xC0 {
		t.Fatalf("chunked read failed")
	}
	f, err = r.ReadFrame()
	if err != nil {
		t.Fatalf("second chunked %v", err)
	}
	_ = f
}

func TestDecodeVarIntMaxAndOverflow(t *testing.T) {
	// overflow beyond 4 bytes with continuation
	_, _, err := DecodeVarInt([]byte{0x80, 0x80, 0x80, 0x80, 0x01})
	if err != ErrMalformedRemainingLength {
		// DecodeVarInt only looks at 4 bytes, so 5th byte not seen; but our implementation returns malformed on i==3
		if err == nil {
			t.Fatalf("should error on 5 byte varint")
		}
	}
	// exactly 4 bytes valid
	_, n, err := DecodeVarInt([]byte{0xFF, 0xFF, 0xFF, 0x7F})
	if err != nil || n != 4 {
		t.Fatalf("4 byte max should succeed")
	}
}
