package codec

import "testing"

func FuzzDecode(f *testing.F) {
	seeds := [][]byte{
		{0x10, 0x0e, 0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x04, 0x02, 0x00, 0x3c, 0x00, 0x03, 0x61, 0x62, 0x63},
		{0x30, 0x07, 0x00, 0x03, 0x61, 0x2f, 0x62, 0x68, 0x69},
		{0x82, 0x0c, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x05, 0x61, 0x2f, 0x62, 0x00},
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		pkt, err := Decode(data)
		if err != nil {
			return
		}
		enc, err := Encode(pkt)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		pkt2, err := Decode(enc)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if pkt.Type != pkt2.Type {
			t.Fatalf("type mismatch %d vs %d", pkt.Type, pkt2.Type)
		}
	})
}

func FuzzProperties(f *testing.F) {
	f.Add([]byte{0x00})
	f.Add([]byte{0x02, 0x11, 0x00, 0x00, 0x0e, 0x10})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = decodeProperties(data, 0)
	})
}
