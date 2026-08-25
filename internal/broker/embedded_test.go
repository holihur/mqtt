package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

func TestEmbeddedNoListener(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "emb-test-1", TCPAddr: "", WSAddr: "", AllowAnonymous: true}
	b, err := NewWithOptions(cfg, WithStore(store))
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.StartAsync(ctx); err != nil {
		t.Fatalf("StartAsync: %v", err)
	}
	if !b.IsEmbedded() {
		t.Fatalf("expected embedded")
	}
	if !b.IsRunning() {
		t.Fatalf("expected running")
	}
	if err := b.Publish(ctx, "test/embedded", []byte("hi"), 0, false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// health should pass without redis
	if err := b.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}
	// HandleConn via pipe should not panic
	c1, c2 := net.Pipe()
	b.HandleConn(c2)
	time.Sleep(50 * time.Millisecond)
	_ = c1.Close()
	time.Sleep(50 * time.Millisecond)
	if err := b.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if b.IsRunning() {
		t.Fatalf("expected stopped")
	}
}

func TestEmbeddedCustomListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	store := persistence.NewMemoryStore()
	b, err := NewWithOptions(Config{NodeID: "emb-test-2", WSAddr: "", AllowAnonymous: true}, WithStore(store), WithCustomListener(ln))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.StartAsync(ctx); err != nil {
		t.Fatalf("StartAsync: %v", err)
	}
	// wait for listen
	time.Sleep(100 * time.Millisecond)
	if b.Addr() != addr {
		t.Fatalf("addr mismatch %s vs %s", b.Addr(), addr)
	}
	// connect via MQTT
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "emb-c1"}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	if n == 0 {
		t.Fatal("no connack")
	}
	pkt, err := codec.Decode(buf[:n])
	if err != nil {
		t.Fatalf("decode connack: %v", err)
	}
	if pkt.Type != codec.TypeCONNACK || pkt.ReasonCode != 0 {
		t.Fatalf("connack failed %+v", pkt)
	}
	conn.Close()
	time.Sleep(100 * time.Millisecond)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer stopCancel()
	_ = b.Stop(stopCtx)
}

func TestBrokerPublishAPI(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "emb-test-3", TCPAddr: "", WSAddr: "", AllowAnonymous: true}
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx := context.Background()
	_ = b.Publish(ctx, "sys/test", []byte("payload"), 1, false)
	stats := b.Stats()
	if stats.MessagesReceived == 0 {
		t.Fatalf("expected messages received incremented")
	}
}

func TestWithOptions(t *testing.T) {
	store := persistence.NewMemoryStore()
	b, err := NewWithOptions(DefaultConfig(), WithStore(store), WithTCPAddr(":0"), WithWSAddr(""), WithAllowAnonymous(true))
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.TCPAddr != ":0" {
		t.Fatalf("WithTCPAddr not applied")
	}
	if b.cfg.WSAddr != "" {
		t.Fatalf("WithWSAddr not applied")
	}
	if b.store != store {
		t.Fatalf("WithStore not applied")
	}
}
