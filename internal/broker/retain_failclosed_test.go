package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

// statsErrStore makes GetRetainedStats fail (simulating Redis/pebble trouble).
type statsErrStore struct {
	*persistence.MemoryStore
}

func (s *statsErrStore) GetRetainedStats(_ context.Context) (persistence.RetainStats, error) {
	return persistence.RetainStats{}, context.DeadlineExceeded
}

// Regression: when retain-quota stats cannot be read, the write must be
// REJECTED (fail-closed). Allowing it lets an attacker fill the disk while
// the stats backend is erroring.
func TestRetainQuotaStatsErrorFailsClosed(t *testing.T) {
	inner := persistence.NewMemoryStore()
	store := &statsErrStore{MemoryStore: inner}
	b, err := NewWithOptions(Config{NodeID: "quota-fail", TCPAddr: "127.0.0.1:12214", AllowAnonymous: true}, WithStore(store))
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// publisher sends a retained message
	pub, _ := net.Dial("tcp", "127.0.0.1:12214")
	c := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "quota-pub"}
	d, _ := codec.Encode(c)
	pub.Write(d)
	buf := make([]byte, 1024)
	pub.SetReadDeadline(time.Now().Add(1 * time.Second))
	pub.Read(buf)
	p := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "quota/x", QoS: 0, Retain: true, Payload: []byte("keep")}
	d, _ = codec.Encode(p)
	pub.Write(d)
	time.Sleep(200 * time.Millisecond)
	pub.Close()

	// a fresh subscriber must NOT receive the retained message
	sub, _ := net.Dial("tcp", "127.0.0.1:12214")
	d, _ = codec.Encode(c)
	sub.Write(d)
	sub.SetReadDeadline(time.Now().Add(1 * time.Second))
	sub.Read(buf)
	sp := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "quota/x", QoS: 0}}}
	d, _ = codec.Encode(sp)
	sub.Write(d)
	// read SUBACK first
	sub.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, err := sub.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("no SUBACK: %v", err)
	}
	ack, _ := codec.Decode(buf[:n])
	if ack == nil || ack.Type != codec.TypeSUBACK {
		t.Fatalf("expected SUBACK, got %+v", ack)
	}
	// then check no retained PUBLISH follows
	sub.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, err = sub.Read(buf)
	if err == nil && n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypePUBLISH {
			t.Fatal("retained message stored despite quota stats being unreadable (fail-open)")
		}
	}
	sub.Close()
}
