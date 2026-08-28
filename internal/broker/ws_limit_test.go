package broker

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// Regression: the WebSocket connection must enforce a read limit so a frame
// declaring a huge payload is aborted at the header instead of being buffered
// into memory while the attacker trickles bytes (OOM vector).
func TestWSOversizedFrameRejected(t *testing.T) {
	wsAddr := "127.0.0.1:12212"
	b := New(Config{NodeID: "ws-limit", WSAddr: wsAddr, MaxPacketSize: 1024, AllowAnonymous: true}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	time.Sleep(300 * time.Millisecond)

	// raw TCP WebSocket client so we can send a deliberately partial frame
	conn, err := net.Dial("tcp", wsAddr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	req := "GET /mqtt HTTP/1.1\r\nHost: " + wsAddr + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("handshake write: %v", err)
	}
	br := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := br.ReadString('\n'); err != nil { // status line
		t.Fatalf("handshake read: %v", err)
	}
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	want := base64.StdEncoding.EncodeToString(h[:])
	found := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("handshake headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
		if len(line) > 24 && line[:24] == "Sec-WebSocket-Accept: "+"" && line[:24] != "" {
			// loose check below
		}
		if len(line) >= len("Sec-WebSocket-Accept: ") && line[:len("Sec-WebSocket-Accept: ")] == "Sec-WebSocket-Accept: " {
			got := line[len("Sec-WebSocket-Accept: "):]
			got = got[:len(got)-2]
			if got == want {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("websocket handshake failed")
	}

	// masked binary frame header declaring 10MB payload, then only 100 bytes sent
	header := []byte{0x82, 0xFF} // FIN + binary + MASK + len127
	var ext [8]byte
	binary.BigEndian.PutUint64(ext[:], 10*1024*1024)
	header = append(header, ext[:]...)
	mask := []byte{0x11, 0x22, 0x33, 0x44}
	header = append(header, mask...)
	payload := make([]byte, 100)
	for i := range payload {
		payload[i] = mask[i%4]
	}
	header = append(header, payload...)
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(header); err != nil {
		t.Fatalf("partial frame write: %v", err)
	}

	// server must abort the connection at the frame header, not wait for 10MB:
	// either a close frame (status 1009 = message too big) arrives, or the TCP
	// connection is torn down. A read timeout means the server is still
	// waiting for the remaining ~10MB — no read limit enforced.
	conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	buf := make([]byte, 16)
	n, err := conn.Read(buf)
	if osTimeout(err) {
		t.Fatal("server kept the connection open waiting for a 10MB frame — no WS read limit enforced")
	}
	if err == nil && n >= 2 && buf[0] == 0x88 {
		status := binary.BigEndian.Uint16(buf[2:min(n, 4)])
		if status == 1009 {
			return // aborted with "message too big" — correct
		}
		t.Fatalf("unexpected close status %d", status)
	}
	if err != nil {
		return // connection torn down — also correct
	}
	t.Fatalf("unexpected data instead of close: %x", buf[:n])
}

func osTimeout(err error) bool {
	type timeout interface{ Timeout() bool }
	te, ok := err.(timeout)
	return ok && te.Timeout()
}
