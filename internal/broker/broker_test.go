package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

func startTestBroker(t *testing.T, tcpAddr string) *Broker {
	t.Helper()
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "test-node", TCPAddr: tcpAddr, WSAddr: "", RedisAddr: ""}
	b := New(cfg, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); time.Sleep(100 * time.Millisecond) })
	go func() {
		if err := b.Start(ctx); err != nil {
			// ctx cancel is expected
		}
	}()
	// wait for listen
	for i := 0; i < 20; i++ {
		conn, err := net.DialTimeout("tcp", tcpAddr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return b
}

func TestConnectV311(t *testing.T) {
	b := startTestBroker(t, "127.0.0.1:18880")
	_ = b
	conn, err := net.Dial("tcp", "127.0.0.1:18880")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "c1"}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatal(err)
	}
	// read connack
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	if n == 0 {
		t.Fatal("no connack")
	}
	// use parser frame split then decode
	frame := buf[:n]
	pkt, err := codec.Decode(frame)
	if err != nil {
		t.Fatalf("decode connack: %v frame %x", err, frame[:n])
	}
	if pkt.Type != codec.TypeCONNACK || pkt.ReasonCode != 0 {
		t.Fatalf("expected connack 0, got %+v", pkt)
	}
}

func TestConnectV5(t *testing.T) {
	b := startTestBroker(t, "127.0.0.1:18881")
	_ = b
	conn, _ := net.Dial("tcp", "127.0.0.1:18881")
	defer conn.Close()
	exp := uint32(60)
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 30, ClientID: "v5c1", Properties: &codec.Properties{SessionExpiryInterval: &exp}}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Write(data)
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	pkt, err := codec.Decode(buf[:n])
	if err != nil {
		t.Fatalf("v5 decode: %v", err)
	}
	if pkt.Type != codec.TypeCONNACK || pkt.ReasonCode != 0 {
		t.Fatalf("v5 connack failed %+v", pkt)
	}
}

func TestPublishSubscribeQoS0(t *testing.T) {
	b := startTestBroker(t, "127.0.0.1:18882")
	_ = b
	// subscriber
	subConn, _ := net.Dial("tcp", "127.0.0.1:18882")
	defer subConn.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "sub1"}
	data, _ := codec.Encode(p)
	subConn.Write(data)
	buf := make([]byte, 1024)
	subConn.SetDeadline(time.Now().Add(2 * time.Second))
	subConn.Read(buf)
	// subscribe
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "test/#", QoS: 0}}}
	data, _ = codec.Encode(sub)
	subConn.Write(data)
	n, _ := subConn.Read(buf)
	pkt, _ := codec.Decode(buf[:n])
	if pkt.Type != codec.TypeSUBACK {
		t.Fatalf("expected suback got %d", pkt.Type)
	}
	// publisher
	pubConn, _ := net.Dial("tcp", "127.0.0.1:18882")
	defer pubConn.Close()
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "pub1"}
	data, _ = codec.Encode(p2)
	pubConn.Write(data)
	pubConn.SetDeadline(time.Now().Add(2 * time.Second))
	pubConn.Read(buf)
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "test/hello", QoS: 0, Payload: []byte("hi")}
	data, _ = codec.Encode(pub)
	pubConn.Write(data)
	// subscriber should receive
	subConn.SetDeadline(time.Now().Add(2 * time.Second))
	n, err := subConn.Read(buf)
	if err != nil {
		t.Fatalf("sub read: %v", err)
	}
	pkt, err = codec.Decode(buf[:n])
	if err != nil {
		t.Fatalf("decode publish: %v", err)
	}
	if pkt.Type != codec.TypePUBLISH || string(pkt.Payload) != "hi" {
		t.Fatalf("unexpected publish %+v", pkt)
	}
}

func TestClusterViaRedis(t *testing.T) {
	// requires redis running, skip if not available
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6379", 200*time.Millisecond)
	if err != nil {
		t.Skip("redis not available")
	}
	conn.Close()
	store := persistence.NewMemoryStore()
	// two brokers sharing redis
	cfg1 := Config{NodeID: "n1", TCPAddr: "127.0.0.1:18883", RedisAddr: "127.0.0.1:6379"}
	cfg2 := Config{NodeID: "n2", TCPAddr: "127.0.0.1:18884", RedisAddr: "127.0.0.1:6379"}
	b1 := New(cfg1, store, nil)
	b2 := New(cfg2, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b1.Start(ctx)
	go b2.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	// sub on b1, pub on b2
	subConn, _ := net.Dial("tcp", "127.0.0.1:18883")
	defer subConn.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "csub"}
	data, _ := codec.Encode(p)
	subConn.Write(data)
	buf := make([]byte, 2048)
	subConn.SetDeadline(time.Now().Add(2 * time.Second))
	subConn.Read(buf)
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "cluster/test", QoS: 0}}}
	data, _ = codec.Encode(sub)
	subConn.Write(data)
	subConn.Read(buf)
	pubConn, _ := net.Dial("tcp", "127.0.0.1:18884")
	defer pubConn.Close()
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "cpub"}
	data, _ = codec.Encode(p2)
	pubConn.Write(data)
	pubConn.Read(buf)
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "cluster/test", QoS: 0, Payload: []byte("cluster-hi")}
	data, _ = codec.Encode(pub)
	pubConn.Write(data)
	subConn.SetDeadline(time.Now().Add(3 * time.Second))
	n, err := subConn.Read(buf)
	if err != nil {
		t.Fatalf("cluster deliver failed: %v", err)
	}
	pkt, _ := codec.Decode(buf[:n])
	if string(pkt.Payload) != "cluster-hi" {
		t.Fatalf("cluster payload mismatch %s", pkt.Payload)
	}
}
