package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
	"mqtt/internal/session"
)

func TestClientIDLengthLimit(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "test-cid", TCPAddr: "127.0.0.1:13190", AllowAnonymous: true, WalDir: "-"}
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// too long clientID >64
	conn, _ := net.Dial("tcp", "127.0.0.1:13190")
	longID := ""
	for i := 0; i < 70; i++ {
		longID += "a"
	}
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: longID}
	data, _ := codec.Encode(p)
	conn.Write(data)
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	pkt, _ := codec.Decode(buf[:n])
	if pkt == nil || pkt.Type != codec.TypeCONNACK || pkt.ReasonCode != 0x02 {
		t.Fatalf("expected CONNACK 0x02 for long clientID got %+v", pkt)
	}
	conn.Close()

	// normal should pass
	conn2, _ := net.Dial("tcp", "127.0.0.1:13190")
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "short"}
	data, _ = codec.Encode(p2)
	conn2.Write(data)
	n, _ = conn2.Read(buf)
	pkt, _ = codec.Decode(buf[:n])
	if pkt.ReasonCode != 0 {
		t.Fatalf("short clientID should succeed got %+v", pkt)
	}
	conn2.Close()
}

func TestWillPayloadSizeLimit(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "test-will", TCPAddr: "127.0.0.1:13191", AllowAnonymous: true, MaxPacketSize: 500, WalDir: "-"}
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// Will with $SYS should be dropped
	conn, _ := net.Dial("tcp", "127.0.0.1:13191")
	p := &codec.Packet{
		Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4,
		ConnectFlags: codec.ConnectFlags{CleanSession: true, WillFlag: true}, ClientID: "will-test-sys",
		Will: &codec.Will{Topic: "$SYS/bad", Payload: []byte("x")},
	}
	data, _ := codec.Encode(p)
	conn.Write(data)
	buf := make([]byte, 2048)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	pkt, _ := codec.Decode(buf[:n])
	if pkt == nil || pkt.ReasonCode != 0 {
		t.Fatalf("SYS will should still connect got %+v", pkt)
	}
	b.mu.RLock()
	sess := b.sessions["will-test-sys"]
	b.mu.RUnlock()
	if sess != nil && sess.Will != nil {
		t.Fatalf("SYS will should be dropped")
	}
	conn.Close()

	// large Will payload exceeding MaxPacketSize should be dropped (or CONN rejected)
	conn2, _ := net.Dial("tcp", "127.0.0.1:13191")
	largePayload := make([]byte, 600)
	for i := range largePayload {
		largePayload[i] = 'x'
	}
	p2 := &codec.Packet{
		Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4,
		ConnectFlags: codec.ConnectFlags{CleanSession: true, WillFlag: true}, ClientID: "will-test-large",
		Will: &codec.Will{Topic: "t/will", Payload: largePayload},
	}
	data, _ = codec.Encode(p2)
	conn2.Write(data)
	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ = conn2.Read(buf)
	if n == 0 {
		// packet too large, server closed without CONNACK - also acceptable (rejected)
		conn2.Close()
		return
	}
	pkt, _ = codec.Decode(buf[:n])
	if pkt != nil && pkt.ReasonCode != 0 {
		// CONNACK with failure is also acceptable for oversize
	}
	b.mu.RLock()
	sess2 := b.sessions["will-test-large"]
	b.mu.RUnlock()
	if sess2 != nil && sess2.Will != nil {
		t.Fatalf("large will should be dropped")
	}
	conn2.Close()
}

func TestSysSubDenied(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "test-sys", TCPAddr: "127.0.0.1:13192", WSAddr: "127.0.0.1:13193", AllowAnonymous: true, WalDir: "-"}
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	conn, _ := net.Dial("tcp", "127.0.0.1:13192")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sys-sub"}
	data, _ := codec.Encode(p)
	conn.Write(data)
	buf := make([]byte, 1024)
	conn.Read(buf)

	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "$SYS/#", QoS: 0}}}
	data, _ = codec.Encode(sub)
	conn.Write(data)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	pkt, _ := codec.Decode(buf[:n])
	if pkt.Type != codec.TypeSUBACK || len(pkt.SubackCodes) == 0 || pkt.SubackCodes[0] != 0x80 {
		t.Fatalf("SYS sub should be denied got %+v", pkt)
	}
	conn.Close()
}

func TestSessionHijackDenied(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "test-hijack", TCPAddr: "127.0.0.1:13194", AllowAnonymous: true, WalDir: "-"}
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// first client with username alice
	c1, _ := net.Dial("tcp", "127.0.0.1:13194")
	p1 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true, UsernameFlag: true}, ClientID: "hijack-id", Username: "alice"}
	data, _ := codec.Encode(p1)
	c1.Write(data)
	buf := make([]byte, 1024)
	c1.Read(buf)

	// second client same ID different username bob should be denied
	c2, _ := net.Dial("tcp", "127.0.0.1:13194")
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true, UsernameFlag: true}, ClientID: "hijack-id", Username: "bob"}
	data, _ = codec.Encode(p2)
	c2.Write(data)
	c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := c2.Read(buf)
	pkt, _ := codec.Decode(buf[:n])
	if pkt == nil || pkt.Type != codec.TypeCONNACK || pkt.ReasonCode == 0 {
		t.Fatalf("hijack should be denied got %+v", pkt)
	}
	c1.Close()
	c2.Close()
}

func TestRedisSessionTTL(t *testing.T) {
	store, err := persistence.NewRedisStore("127.0.0.1:6379", "test-ttl")
	if err != nil {
		t.Skip("redis not available")
	}
	defer store.Close()
	ctx := context.Background()
	realSess := newSessionWithExpiry("ttl-test", 10)
	if err := store.SaveSession(ctx, realSess); err != nil {
		t.Fatalf("SaveSession failed %v", err)
	}
	ttl, err := store.Client().TTL(ctx, "test-ttl:session:ttl-test").Result()
	if err != nil {
		t.Fatalf("TTL failed %v", err)
	}
	if ttl < 5*time.Second || ttl > 15*time.Second {
		t.Fatalf("TTL should be ~10s got %v", ttl)
	}
	realSess2 := newSessionWithExpiry("ttl-never", 0xFFFFFFFF)
	if err := store.SaveSession(ctx, realSess2); err != nil {
		t.Fatalf("SaveSession never failed %v", err)
	}
	ttl2, _ := store.Client().TTL(ctx, "test-ttl:session:ttl-never").Result()
	if ttl2 != -1 {
		t.Fatalf("never expire should have no TTL got %v", ttl2)
	}
}

func newSessionWithExpiry(clientID string, expiry uint32) *session.Session {
	s := session.NewSession(clientID, 4, false, expiry)
	s.ExpiryInterval = expiry
	return s
}
