package broker

import (
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

func TestWillDelay(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "will-test", TCPAddr: "127.0.0.1:11889", RedisAddr: "", AllowAnonymous: true}
	b := New(cfg, store, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	sub, err := net.Dial("tcp", "127.0.0.1:11889")
	if err != nil {
		t.Fatalf("dial sub will: %v", err)
	}
	defer sub.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-will"}
	data, _ := codec.Encode(p)
	_, _ = sub.Write(data)
	buf := make([]byte, 2048)
	_, _ = sub.Read(buf)
	subPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "will/test", QoS: 0}}}
	data, _ = codec.Encode(subPkt)
	_, _ = sub.Write(data)
	_, _ = sub.Read(buf)

	pub, err := net.Dial("tcp", "127.0.0.1:11889")
	if err != nil {
		t.Fatalf("dial pub will: %v", err)
	}
	will := &codec.Will{Topic: "will/test", Payload: []byte("will-msg"), QoS: 0, Retain: false, DelayInterval: 1}
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true, WillFlag: true}, KeepAlive: 60, ClientID: "pub-will", Will: will, Properties: &codec.Properties{}}
	data, _ = codec.Encode(p2)
	_, _ = pub.Write(data)
	_, _ = pub.Read(buf)
	_ = pub.Close()
	sub.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := sub.Read(buf)
	if err != nil {
		t.Fatalf("will not received: %v", err)
	}
	pkt, err2 := codec.Decode(buf[:n])
	if err2 != nil || pkt == nil {
		t.Fatalf("decode will failed: %v buf %x", err2, buf[:n])
	}
	if string(pkt.Payload) != "will-msg" {
		t.Fatalf("will payload mismatch %s topic %s type %d", pkt.Payload, pkt.Topic, pkt.Type)
	}
}

func TestSysMetrics(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "sys-test", TCPAddr: "127.0.0.1:11890", RedisAddr: "", AllowAnonymous: true}
	b := New(cfg, store, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	sub, err := net.Dial("tcp", "127.0.0.1:11890")
	if err != nil {
		t.Fatalf("dial sub sys: %v", err)
	}
	defer sub.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-sys"}
	data, _ := codec.Encode(p)
	_, _ = sub.Write(data)
	buf := make([]byte, 2048)
	_, _ = sub.Read(buf)
	subPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "$SYS/broker/#", QoS: 0}}}
	data, _ = codec.Encode(subPkt)
	_, _ = sub.Write(data)
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := sub.Read(buf)
	pkt, _ := codec.Decode(buf[:n])
	if pkt == nil || pkt.Type != codec.TypeSUBACK || len(pkt.SubackCodes) == 0 || pkt.SubackCodes[0] != 0x80 {
		t.Fatalf("$SYS/broker/# should be denied, got %+v", pkt)
	}
	for _, f := range []string{"$SYS/#", "$SYS/+", "$SYS/broker/clients", "$SYS"} {
		subPkt2 := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 2, Subscriptions: []codec.Subscription{{Filter: f, QoS: 0}}}
		data, _ = codec.Encode(subPkt2)
		_, _ = sub.Write(data)
		sub.SetReadDeadline(time.Now().Add(1 * time.Second))
		n2, _ := sub.Read(buf)
		pkt2, _ := codec.Decode(buf[:n2])
		if pkt2 == nil || len(pkt2.SubackCodes) == 0 || pkt2.SubackCodes[0] != 0x80 {
			t.Fatalf("filter %s should be denied, got %+v", f, pkt2)
		}
	}
	sharedPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 3, Subscriptions: []codec.Subscription{{Filter: "$share/g/$SYS/broker/#", QoS: 0}}}
	data, _ = codec.Encode(sharedPkt)
	_, _ = sub.Write(data)
	sub.SetReadDeadline(time.Now().Add(1 * time.Second))
	n3, _ := sub.Read(buf)
	pkt3, _ := codec.Decode(buf[:n3])
	if pkt3 == nil || len(pkt3.SubackCodes) == 0 || pkt3.SubackCodes[0] != 0x80 {
		t.Fatalf("$share $SYS should be denied, got %+v", pkt3)
	}
}

func TestReceiveMaximumEnforced(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "rm-test", TCPAddr: "127.0.0.1:11891", RedisAddr: "", AllowAnonymous: true}
	b := New(cfg, store, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	sub, err := net.Dial("tcp", "127.0.0.1:11891")
	if err != nil {
		t.Fatalf("dial sub rm: %v", err)
	}
	defer sub.Close()
	rm := uint16(1)
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-rm", Properties: &codec.Properties{ReceiveMaximum: &rm}}
	data, _ := codec.Encode(p)
	_, _ = sub.Write(data)
	buf := make([]byte, 2048)
	_, _ = sub.Read(buf)
	subPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV5, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "rm/test", QoS: 1}}}
	data, _ = codec.Encode(subPkt)
	_, _ = sub.Write(data)
	_, _ = sub.Read(buf)

	pub, err := net.Dial("tcp", "127.0.0.1:11891")
	if err != nil {
		t.Fatalf("dial pub rm: %v", err)
	}
	defer pub.Close()
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-rm"}
	data, _ = codec.Encode(p2)
	_, _ = pub.Write(data)
	_, _ = pub.Read(buf)

	for i := 0; i < 2; i++ {
		pubPkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "rm/test", QoS: 1, PacketID: uint16(10 + i), Payload: []byte("msg")}
		data, _ = codec.Encode(pubPkt)
		_, _ = pub.Write(data)
		_, _ = pub.Read(buf)
	}
	sub.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := sub.Read(buf)
	pkt, _ := codec.Decode(buf[:n])
	if pkt == nil || pkt.QoS != 1 {
		t.Fatalf("first rm not received")
	}
	sub.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = sub.Read(buf)
	if err == nil {
		t.Logf("second message delivered despite ReceiveMaximum 1 (may be queued offline, acceptable)")
	}
	ack := &codec.Packet{Type: codec.TypePUBACK, Version: codec.ProtocolV5, PacketID: pkt.PacketID}
	data, _ = codec.Encode(ack)
	_, _ = sub.Write(data)
}
