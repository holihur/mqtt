package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"

	"go.uber.org/goleak"
)

func TestGoleakBrokerStartStop(t *testing.T) {
	cfg := Config{NodeID: "goleak-1", TCPAddr: "127.0.0.1:18890", AllowAnonymous: true}
	b := New(cfg, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Start(ctx) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	time.Sleep(200 * time.Millisecond)
	goleak.VerifyNone(t)
}

func TestGoleakNoLeakAfterPublish(t *testing.T) {
	cfg := Config{NodeID: "goleak-2", TCPAddr: "127.0.0.1:18891", AllowAnonymous: true}
	b := New(cfg, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Start(ctx) }()
	time.Sleep(200 * time.Millisecond)
	conn, err := net.Dial("tcp", "127.0.0.1:18891")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "goleak-pub"}
	data, _ := codec.Encode(p)
	_, _ = conn.Write(data)
	buf := make([]byte, 1024)
	_, _ = conn.Read(buf)
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "goleak/test", QoS: 0, Payload: []byte("hi")}
	data, _ = codec.Encode(pub)
	_, _ = conn.Write(data)
	time.Sleep(100 * time.Millisecond)
	_ = conn.Close()
	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(200 * time.Millisecond)
	goleak.VerifyNone(t)
}
