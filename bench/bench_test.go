package bench

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/broker"
	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

func Benchmark10kClients(b *testing.B) {
	store := persistence.NewMemoryStore()
	cfg := broker.Config{NodeID: "bench", TCPAddr: "127.0.0.1:11885", WSAddr: "", RedisAddr: ""}
	br := broker.New(cfg, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go br.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, _ := net.Dial("tcp", "127.0.0.1:11885")
		p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "bench-" + string(rune(i))}
		data, _ := codec.Encode(p)
		conn.Write(data)
		buf := make([]byte, 64)
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		conn.Read(buf)
		conn.Close()
	}
}

func BenchmarkPublishThroughput(b *testing.B) {
	store := persistence.NewMemoryStore()
	cfg := broker.Config{NodeID: "bench-pub", TCPAddr: "127.0.0.1:11886", RedisAddr: ""}
	br := broker.New(cfg, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go br.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	// subscriber
	sub, _ := net.Dial("tcp", "127.0.0.1:11886")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-bench"}
	data, _ := codec.Encode(p)
	sub.Write(data)
	buf := make([]byte, 1024)
	sub.Read(buf)
	subPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "bench/#", QoS: 0}}}
	data, _ = codec.Encode(subPkt)
	sub.Write(data)
	sub.Read(buf)

	pub, _ := net.Dial("tcp", "127.0.0.1:11886")
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-bench"}
	data, _ = codec.Encode(p2)
	pub.Write(data)
	pub.Read(buf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pubPkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "bench/test", QoS: 0, Payload: []byte("payload")}
		data, _ = codec.Encode(pubPkt)
		pub.Write(data)
	}
}
