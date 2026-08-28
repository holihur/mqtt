package broker

import (
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
)

// Regression: total subscriptions per client must be capped, otherwise a
// single client can inflate the topic trie without bound (memory DoS) with
// many filters inside one SUBSCRIBE packet.
func TestSubscribeTotalCapped(t *testing.T) {
	b := New(Config{NodeID: "sub-cap", TCPAddr: "127.0.0.1:12215", AllowAnonymous: true, MaxSubscriptionsPerClient: 10}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	conn, _ := net.Dial("tcp", "127.0.0.1:12215")
	c := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-cap-client"}
	d, _ := codec.Encode(c)
	conn.Write(d)
	buf := make([]byte, 8192)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	conn.Read(buf)

	// single SUBSCRIBE with 20 filters while cap is 10
	subs := make([]codec.Subscription, 20)
	for i := range subs {
		subs[i] = codec.Subscription{Filter: "cap/" + string(rune('a'+i)) + "/x", QoS: 0}
	}
	sp := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: subs}
	d, _ = codec.Encode(sp)
	conn.Write(d)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, err := conn.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("no SUBACK: %v", err)
	}
	ack, _ := codec.Decode(buf[:n])
	if ack == nil || ack.Type != codec.TypeSUBACK {
		t.Fatalf("expected SUBACK, got %+v", ack)
	}
	granted := 0
	for _, code := range ack.SubackCodes {
		if code < 0x80 {
			granted++
		}
	}
	if granted > 10 {
		t.Fatalf("subscription cap not enforced: %d granted (cap 10)", granted)
	}
	conn.Close()
}
