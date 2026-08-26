package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

func TestMessageExpiryRetainNotStored(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "expiry-retain", TCPAddr: "127.0.0.1:13200", AllowAnonymous: true, WalDir: "-"}
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	conn, _ := net.Dial("tcp", "127.0.0.1:13200")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-expiry", Properties: &codec.Properties{}}
	data, _ := codec.Encode(p)
	conn.Write(data)
	buf := make([]byte, 2048)
	conn.Read(buf)

	expiry := uint32(0)
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "expiry/retain", QoS: 0, Payload: []byte("hello"), Retain: true, PubProps: &codec.Properties{MessageExpiryInterval: &expiry}}
	data, _ = codec.Encode(pub)
	conn.Write(data)
	time.Sleep(100 * time.Millisecond)

	// retained should not be stored
	if m, _ := store.GetRetained(context.Background(), "expiry/retain"); m != nil {
		t.Fatalf("expiry 0 retain should not be stored")
	}
	conn.Close()
}

func TestMessageExpiryRetainWithTTL(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "expiry-ttl", TCPAddr: "127.0.0.1:13201", AllowAnonymous: true, WalDir: "-"}
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	conn, _ := net.Dial("tcp", "127.0.0.1:13201")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-expiry2"}
	data, _ := codec.Encode(p)
	conn.Write(data)
	buf := make([]byte, 2048)
	conn.Read(buf)

	expiry := uint32(1)
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "expiry/retain2", QoS: 0, Payload: []byte("hello2"), Retain: true, PubProps: &codec.Properties{MessageExpiryInterval: &expiry}}
	data, _ = codec.Encode(pub)
	conn.Write(data)
	time.Sleep(100 * time.Millisecond)

	if m, _ := store.GetRetained(context.Background(), "expiry/retain2"); m == nil {
		t.Fatalf("retain should be stored initially")
	}
	time.Sleep(1200 * time.Millisecond)
	if m, _ := store.GetRetained(context.Background(), "expiry/retain2"); m != nil {
		// may still be stored but should be considered expired for delivery
		// check via ListRetained filtering
		list, _ := store.ListRetained(context.Background())
		for _, msg := range list {
			if msg.Topic == "expiry/retain2" && msg.IsExpired() {
				// should be expired
				return
			}
		}
		// if not expired yet, wait a bit more
		time.Sleep(500 * time.Millisecond)
		if m2, _ := store.GetRetained(context.Background(), "expiry/retain2"); m2 != nil && !m2.IsExpired() {
			t.Fatalf("retain should be expired after 1s")
		}
	}
	conn.Close()
}

func TestMessageExpiryOfflineNotEnqueued(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "expiry-offline", TCPAddr: "127.0.0.1:13202", AllowAnonymous: true, WalDir: "-"}
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// create offline session with expiry
	subConn, _ := net.Dial("tcp", "127.0.0.1:13202")
	expiry := uint32(60)
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: false}, ClientID: "offline-expiry", Properties: &codec.Properties{SessionExpiryInterval: &expiry}}
	data, _ := codec.Encode(p)
	subConn.Write(data)
	buf := make([]byte, 2048)
	subConn.Read(buf)
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV5, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "expiry/offline", QoS: 1}}}
	data, _ = codec.Encode(sub)
	subConn.Write(data)
	subConn.Read(buf)
	subConn.Close()
	time.Sleep(200 * time.Millisecond)

	// publish with expiry 0 to offline client
	pubConn, _ := net.Dial("tcp", "127.0.0.1:13202")
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-offline-expiry"}
	data, _ = codec.Encode(p2)
	pubConn.Write(data)
	pubConn.Read(buf)
	expiry0 := uint32(0)
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "expiry/offline", QoS: 1, Payload: []byte("offline-msg"), PubProps: &codec.Properties{MessageExpiryInterval: &expiry0}}
	data, _ = codec.Encode(pub)
	pubConn.Write(data)
	pubConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pubConn.Read(buf)
	pubConn.Close()
	time.Sleep(200 * time.Millisecond)

	// offline queue should be empty (not enqueued due to expiry 0)
	msgs, _ := store.DequeueOffline(context.Background(), "offline-expiry")
	if len(msgs) != 0 {
		t.Fatalf("expiry 0 should not be enqueued offline got %d", len(msgs))
	}
}
