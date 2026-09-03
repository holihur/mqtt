package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/parser"
	"mqtt/internal/persistence"
	"mqtt/internal/session"
	"mqtt/internal/transport"
)

func TestQoS1RetryAndDedup(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "qos-test", TCPAddr: "127.0.0.1:11887", RedisAddr: "", AllowAnonymous: true}
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
	cfg := Config{NodeID: "offline-test", TCPAddr: "127.0.0.1:11888", RedisAddr: "", AllowAnonymous: true}
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

// TestRetryLoopFiresDueEntries 覆盖 retryLoop ticker 触发在内存到期项的分支：
// 到期后应发送 Dup PUBLISH 并安排下一次重试 (持久化 Retries+1)。
func TestRetryLoopFiresDueEntries(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "retryloop", AllowAnonymous: true, MaxPublishPerSec: 1 << 30, MaxSubscribePerSec: 1 << 30}
	b, err := NewWithOptions(cfg, WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := b.StartAsync(ctx); err != nil {
		t.Fatal(err)
	}
	// 在线订阅者: net.Pipe 写端给 broker，读端 drain 并解析 PUBLISH
	c1, c2 := net.Pipe()
	cc := transport.NewConn(c2, 1<<20)
	cc.SetVersion(codec.ProtocolV311)
	cc.SetClientID("rt-sub")
	sess := session.NewSession("rt-sub", codec.ProtocolV311, true, 0)
	b.mu.Lock()
	b.conns["rt-sub"] = cc
	b.sessions["rt-sub"] = sess
	b.mu.Unlock()
	packetID := uint16(7)
	sess.AddInflight(&session.InflightEntry{PacketID: packetID, QoS: 1, Topic: "rt/t", Payload: []byte("x")})

	type pubMsg struct {
		dup      bool
		packetID uint16
	}
	pubs := make(chan pubMsg, 4)
	go func() {
		var leftover []byte
		rbuf := make([]byte, 4096)
		for {
			n, err := c1.Read(rbuf)
			if err != nil {
				return
			}
			buf := append(leftover, rbuf[:n]...)
			for {
				frame, rest, err := parser.SplitFrame(buf, 1<<20)
				if err != nil {
					break
				}
				pkt, derr := codec.Decode(frame)
				if derr == nil && pkt.Type == codec.TypePUBLISH {
					pubs <- pubMsg{dup: pkt.Dup, packetID: pkt.PacketID}
				}
				if len(rest) == 0 {
					buf = nil
					break
				}
				buf = rest
			}
			leftover = buf
		}
	}()

	// 到期项: nextAt 在过去，retryLoop 下一个 tick (≤200ms) 应触发
	b.armRetry("rt-sub", packetID, 0, time.Now().UnixMilli()-1000)

	select {
	case pm := <-pubs:
		if !pm.dup || pm.packetID != packetID {
			t.Fatalf("expected Dup PUBLISH pid=%d, got dup=%v pid=%d", packetID, pm.dup, pm.packetID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retryLoop did not fire due entry within 2s")
	}

	// 重试后应持久化 Retries=1 并安排在将来 (20s)
	time.Sleep(100 * time.Millisecond)
	list, err := store.ListPendingRetries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var found *persistence.PendingRetry
	for i := range list {
		if list[i].PacketID == packetID {
			found = list[i]
		}
	}
	if found == nil {
		t.Fatalf("pending retry not persisted after retryLoop fire")
	}
	if found.Retries != 1 || found.NextRetryAt <= time.Now().UnixMilli() {
		t.Fatalf("pending retry state wrong: %+v", found)
	}
	// ACK 取消: 内存队列与持久化记录都应清除
	b.cancelRetry("rt-sub", packetID)
	b.retryMu.Lock()
	_, stillArmed := b.retryQueue["rt-sub"][packetID]
	b.retryMu.Unlock()
	if stillArmed {
		t.Fatal("entry still armed after cancelRetry")
	}
	list, _ = store.ListPendingRetries(context.Background())
	for _, r := range list {
		if r.PacketID == packetID {
			t.Fatalf("pending retry still in store after cancelRetry")
		}
	}
	_ = c1.Close()
	_ = c2.Close()
}
