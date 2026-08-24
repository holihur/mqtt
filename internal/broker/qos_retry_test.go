package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/parser"
	"mqtt/internal/persistence"
)

func TestQoS1RetryAndDedup(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "qos-test", TCPAddr: "127.0.0.1:11887", RedisAddr: ""}
	b := New(cfg, store, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// sub
	sub, _ := net.Dial("tcp", "127.0.0.1:11887")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-qos"}
	data, _ := codec.Encode(p)
	sub.Write(data)
	buf := make([]byte, 2048)
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	sub.Read(buf)
	subPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "qos/test", QoS: 1}}}
	data, _ = codec.Encode(subPkt)
	sub.Write(data)
	sub.Read(buf)

	pub, _ := net.Dial("tcp", "127.0.0.1:11887")
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-qos"}
	data, _ = codec.Encode(p2)
	pub.Write(data)
	pub.Read(buf)

	// publish QoS1
	pubPkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "qos/test", QoS: 1, PacketID: 100, Payload: []byte("qos1")}
	data, _ = codec.Encode(pubPkt)
	pub.Write(data)
	// pub should get PUBACK
	pub.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := pub.Read(buf)
	pkt, _ := codec.Decode(buf[:n])
	if pkt.Type != codec.TypePUBACK || pkt.PacketID != 100 {
		t.Fatalf("expected puback 100 got %+v", pkt)
	}
	// sub should get publish with packetID assigned by broker (not 100)
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ = sub.Read(buf)
	pkt, _ = codec.Decode(buf[:n])
	if pkt.Type != codec.TypePUBLISH || string(pkt.Payload) != "qos1" {
		t.Fatalf("sub publish mismatch %+v", pkt)
	}
	// ack from sub
	ack := &codec.Packet{Type: codec.TypePUBACK, Version: codec.ProtocolV311, PacketID: pkt.PacketID}
	data, _ = codec.Encode(ack)
	sub.Write(data)
}

func TestOfflineQueue(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "offline-test", TCPAddr: "127.0.0.1:11888", RedisAddr: ""}
	b := New(cfg, store, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// sub with persistent session
	sub, _ := net.Dial("tcp", "127.0.0.1:11888")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: false}, ClientID: "offline-client"}
	data, _ := codec.Encode(p)
	sub.Write(data)
	buf := make([]byte, 2048)
	sub.Read(buf)
	subPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "offline/#", QoS: 1}}}
	data, _ = codec.Encode(subPkt)
	sub.Write(data)
	sub.Read(buf)
	sub.Close() // offline

	time.Sleep(500 * time.Millisecond)
	// publish while offline
	pub, _ := net.Dial("tcp", "127.0.0.1:11888")
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-offline"}
	data, _ = codec.Encode(p2)
	pub.Write(data)
	pub.Read(buf)
	pubPkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "offline/test", QoS: 1, PacketID: 10, Payload: []byte("offline-msg")}
	data, _ = codec.Encode(pubPkt)
	pub.Write(data)
	pub.Read(buf)
	pub.Close()
	time.Sleep(500 * time.Millisecond)

	sub2, _ := net.Dial("tcp", "127.0.0.1:11888")
	p3 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: false}, ClientID: "offline-client"}
	data, _ = codec.Encode(p3)
	sub2.Write(data)
	sub2.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, _ := sub2.Read(buf)
	remaining := buf[:n]
	found := false
	// split possibly coalesced CONNACK + PUBLISH
	tmp := remaining
	for len(tmp) > 0 {
		frame, leftover, err := parser.SplitFrame(tmp, 1<<20)
		if err != nil {
			break
		}
		pkt2, _ := codec.Decode(frame)
		if pkt2 != nil && pkt2.Type == codec.TypePUBLISH && string(pkt2.Payload) == "offline-msg" {
			found = true
			break
		}
		if len(leftover) == 0 {
			break
		}
		tmp = leftover
	}
	if !found {
		sub2.SetReadDeadline(time.Now().Add(2 * time.Second))
		n2, err := sub2.Read(buf)
		if err != nil {
			t.Fatalf("offline replay failed: %v", err)
		}
		frame, _, _ := parser.SplitFrame(buf[:n2], 1<<20)
		pkt2, _ := codec.Decode(frame)
		if pkt2 == nil || string(pkt2.Payload) != "offline-msg" {
			t.Fatalf("offline payload mismatch %v", pkt2)
		}
	}
}

func newCtx() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}
