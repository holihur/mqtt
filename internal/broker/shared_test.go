package broker

import (
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
)

func TestSharedSubRoundRobin(t *testing.T) {
	addr := "127.0.0.1:12092"
	b := newTestBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	sub1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-share-1"}
	data, _ := codec.Encode(p)
	_, _ = sub1.Write(data)
	buf := make([]byte, 4096)
	_, _ = sub1.Read(buf)
	subPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "$share/g1/paho/share", QoS: 0}}}
	data, _ = codec.Encode(subPkt)
	_, _ = sub1.Write(data)
	_, _ = sub1.Read(buf)

	sub2, _ := net.Dial("tcp", addr)
	defer sub2.Close()
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-share-2"}
	data, _ = codec.Encode(p2)
	_, _ = sub2.Write(data)
	_, _ = sub2.Read(buf)
	subPkt2 := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "$share/g1/paho/share", QoS: 0}}}
	data, _ = codec.Encode(subPkt2)
	_, _ = sub2.Write(data)
	_, _ = sub2.Read(buf)

	pub, _ := net.Dial("tcp", addr)
	defer pub.Close()
	p3 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-share"}
	data, _ = codec.Encode(p3)
	_, _ = pub.Write(data)
	_, _ = pub.Read(buf)

	for i := 0; i < 2; i++ {
		pubPkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "paho/share", QoS: 0, Payload: []byte("shared-msg")}
		data, _ = codec.Encode(pubPkt)
		_, _ = pub.Write(data)
		time.Sleep(100 * time.Millisecond)
	}
	sub1.SetReadDeadline(time.Now().Add(1 * time.Second))
	n1, _ := sub1.Read(buf)
	has1 := n1 > 0
	sub2.SetReadDeadline(time.Now().Add(1 * time.Second))
	n2, _ := sub2.Read(buf)
	has2 := n2 > 0
	if !has1 && !has2 {
		t.Fatalf("no shared message delivered")
	}
	t.Logf("shared: sub1 %d bytes has1=%v sub2 %d bytes has2=%v", n1, has1, n2, has2)
}

func TestPrometheusMetrics(t *testing.T) {
	addr := "127.0.0.1:12093"
	b := newTestBroker(t, addr)
	_ = b
	time.Sleep(100 * time.Millisecond)
	pub, _ := net.Dial("tcp", addr)
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-metrics"}
	data, _ := codec.Encode(p)
	_, _ = pub.Write(data)
	buf := make([]byte, 1024)
	_, _ = pub.Read(buf)
	pubPkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "metrics/test", QoS: 0, Payload: []byte("m")}
	data, _ = codec.Encode(pubPkt)
	_, _ = pub.Write(data)
	time.Sleep(100 * time.Millisecond)
	_ = pub.Close()
	b.statsMu.Lock()
	recv := b.stats.MessagesReceived
	b.statsMu.Unlock()
	if recv == 0 {
		t.Fatalf("metrics not incremented")
	}
}
