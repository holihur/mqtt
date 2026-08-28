package transport

import (
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
)

// Regression: WritePacket must not block forever on a stalled peer.
// A slow/stopped client must not pin the publishing goroutine indefinitely.
func TestWritePacketStalledPeerTimesOut(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	c := NewConn(client, 1024*1024)
	c.writeTimeout = 100 * time.Millisecond
	p := &codec.Packet{Type: codec.TypePUBLISH, Topic: "t", Payload: make([]byte, 4096)}
	done := make(chan error, 1)
	go func() { done <- c.WritePacket(p) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("write to stalled peer should time out with error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WritePacket blocked forever on stalled peer — no write deadline")
	}
}
