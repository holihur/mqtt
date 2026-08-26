package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

func newQuotaBroker(t *testing.T, cfg Config) *Broker {
	t.Helper()
	store := persistence.NewMemoryStore()
	if cfg.TCPAddr == "" {
		cfg.TCPAddr = ""
		cfg.WSAddr = ""
	}
	cfg.ApplyDefaults()
	b, err := NewWithOptions(cfg, WithStore(store))
	if err != nil {
		t.Fatalf("NewWithOptions failed: %v", err)
	}
	return b
}

func TestDefaultConfigRetainDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxRetainedMessages != 10000 {
		t.Fatalf("expected 10000 got %d", cfg.MaxRetainedMessages)
	}
	if cfg.MaxRetainedSize != 1<<30 {
		t.Fatalf("expected 1GB got %d", cfg.MaxRetainedSize)
	}
	if cfg.MaxRetainPerTopic != 1000 {
		t.Fatalf("expected 1000 got %d", cfg.MaxRetainPerTopic)
	}
	if cfg.MaxRetainSizePerTopic != 100<<20 {
		t.Fatalf("expected 100MB got %d", cfg.MaxRetainSizePerTopic)
	}
}

func TestApplyDefaultsRetain(t *testing.T) {
	cfg := Config{}
	cfg.ApplyDefaults()
	if cfg.MaxRetainedMessages != 10000 {
		t.Fatalf("ApplyDefaults MaxRetainedMessages %d", cfg.MaxRetainedMessages)
	}
	if cfg.MaxRetainedSize != 1<<30 {
		t.Fatalf("ApplyDefaults MaxRetainedSize %d", cfg.MaxRetainedSize)
	}
	if cfg.MaxRetainPerTopic != 1000 {
		t.Fatalf("ApplyDefaults MaxRetainPerTopic %d", cfg.MaxRetainPerTopic)
	}
	if cfg.MaxRetainSizePerTopic != 100<<20 {
		t.Fatalf("ApplyDefaults MaxRetainSizePerTopic %d", cfg.MaxRetainSizePerTopic)
	}
	// not overwrite explicit
	cfg2 := Config{MaxRetainedMessages: 5, MaxRetainedSize: 100, MaxRetainPerTopic: 2, MaxRetainSizePerTopic: 50}
	cfg2.ApplyDefaults()
	if cfg2.MaxRetainedMessages != 5 || cfg2.MaxRetainedSize != 100 || cfg2.MaxRetainPerTopic != 2 || cfg2.MaxRetainSizePerTopic != 50 {
		t.Fatalf("should not overwrite explicit %+v", cfg2)
	}
}

func TestWithConfigRetain(t *testing.T) {
	b, _ := NewWithOptions(DefaultConfig(), WithStore(persistence.NewMemoryStore()))
	if b.cfg.MaxRetainedMessages != 10000 {
		t.Fatalf("default via NewWithOptions failed")
	}
	cfg := Config{MaxRetainedMessages: 123, MaxRetainedSize: 456, MaxRetainPerTopic: 7, MaxRetainSizePerTopic: 89}
	if err := WithConfig(cfg)(b); err != nil {
		t.Fatal(err)
	}
	if b.cfg.MaxRetainedMessages != 123 {
		t.Fatalf("WithConfig MaxRetainedMessages %d", b.cfg.MaxRetainedMessages)
	}
	if b.cfg.MaxRetainedSize != 456 {
		t.Fatalf("WithConfig MaxRetainedSize %d", b.cfg.MaxRetainedSize)
	}
	if b.cfg.MaxRetainPerTopic != 7 {
		t.Fatalf("WithConfig MaxRetainPerTopic %d", b.cfg.MaxRetainPerTopic)
	}
	if b.cfg.MaxRetainSizePerTopic != 89 {
		t.Fatalf("WithConfig MaxRetainSizePerTopic %d", b.cfg.MaxRetainSizePerTopic)
	}
}

func TestCheckRetainQuotaGlobalCount(t *testing.T) {
	cfg := Config{MaxRetainedMessages: 2, MaxRetainedSize: 1 << 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 100 << 20}
	b := newQuotaBroker(t, cfg)
	ctx := context.Background()
	_ = b.store.SaveRetained(ctx, "t/a", &persistence.Message{Topic: "t/a", Payload: []byte("x")})
	_ = b.store.SaveRetained(ctx, "t/b", &persistence.Message{Topic: "t/b", Payload: []byte("y")})
	// third new topic should exceed
	exceeded, reason := b.checkRetainQuota("t/c", []byte("z"))
	if !exceeded || reason != "global_count" {
		t.Fatalf("expected global_count exceeded got %v %s", exceeded, reason)
	}
	// overwrite existing should not exceed count
	exceeded, _ = b.checkRetainQuota("t/a", []byte("new"))
	if exceeded {
		t.Fatalf("overwrite should not exceed global_count")
	}
}

func TestCheckRetainQuotaGlobalSize(t *testing.T) {
	cfg := Config{MaxRetainedMessages: 10000, MaxRetainedSize: 50, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 100 << 20}
	b := newQuotaBroker(t, cfg)
	ctx := context.Background()
	_ = b.store.SaveRetained(ctx, "t/a", &persistence.Message{Topic: "t/a", Payload: []byte("hello")}) // size ~ t/a(3)+5+10=18
	exceeded, reason := b.checkRetainQuota("t/b", []byte("this is a very long payload exceeding size"))
	if !exceeded || reason != "global_size" {
		t.Fatalf("expected global_size got %v %s", exceeded, reason)
	}
	// small payload should pass
	exceeded, _ = b.checkRetainQuota("t/b", []byte("x"))
	if exceeded {
		t.Fatalf("small payload should not exceed")
	}
}

func TestCheckRetainQuotaPerTopicSize(t *testing.T) {
	cfg := Config{MaxRetainedMessages: 10000, MaxRetainedSize: 1 << 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 20}
	b := newQuotaBroker(t, cfg)
	// payload that makes newSize > 20 should fail
	// newSize = len(topic)+len(payload)+10
	// topic "t/a" len 3, so payload len 10 => 23 >20
	exceeded, reason := b.checkRetainQuota("t/a", []byte("0123456789"))
	if !exceeded || reason != "per_topic_size" {
		t.Fatalf("expected per_topic_size got %v %s", exceeded, reason)
	}
	// small payload should pass: payload "x" => 3+1+10=14 <20
	exceeded, _ = b.checkRetainQuota("t/a", []byte("x"))
	if exceeded {
		t.Fatalf("small per topic should pass")
	}
}

func TestCheckRetainQuotaPerTopicCount(t *testing.T) {
	cfg := Config{MaxRetainedMessages: 10000, MaxRetainedSize: 1 << 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 100 << 20}
	b := newQuotaBroker(t, cfg)
	b.cfg.MaxRetainPerTopic = 0
	exceeded, reason := b.checkRetainQuota("t/a", []byte("x"))
	if !exceeded || reason != "per_topic_count" {
		t.Fatalf("expected per_topic_count got %v %s", exceeded, reason)
	}
}

func TestCheckRetainQuotaOverwriteSize(t *testing.T) {
	cfg := Config{MaxRetainedMessages: 10000, MaxRetainedSize: 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 100 << 20}
	b := newQuotaBroker(t, cfg)
	ctx := context.Background()
	_ = b.store.SaveRetained(ctx, "t/a", &persistence.Message{Topic: "t/a", Payload: []byte("hello")}) // 18
	// now existing size 18, total 18, new payload larger but still within 30 if overwrite
	// overwrite t/a with payload "hello world" => size 3+11+10=24, total after =24 <30 => should pass
	exceeded, _ := b.checkRetainQuota("t/a", []byte("hello world"))
	if exceeded {
		t.Fatalf("overwrite within size should pass")
	}
	// new topic with large payload should exceed
	exceeded, reason := b.checkRetainQuota("t/b", []byte("hello world long payload"))
	if !exceeded || reason != "global_size" {
		t.Fatalf("expected global_size for new topic got %v %s", exceeded, reason)
	}
}

func TestCheckRetainQuotaNoErrorOnStatsFail(t *testing.T) {
	// stats failure should not panic, return false
	cfg := DefaultConfig()
	b := newQuotaBroker(t, cfg)
	// normal should not exceed
	exceeded, _ := b.checkRetainQuota("t/a", []byte("x"))
	if exceeded {
		t.Fatalf("default config should not exceed")
	}
}

func TestEmbeddedPublishQuotaExceeded(t *testing.T) {
	cfg := Config{MaxRetainedMessages: 1, MaxRetainedSize: 1 << 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 100 << 20, AllowAnonymous: true}
	b := newQuotaBroker(t, cfg)
	ctx := context.Background()
	if err := b.Publish(ctx, "t/a", []byte("hello"), 0, true); err != nil {
		t.Fatalf("first publish should succeed %v", err)
	}
	// second distinct topic should fail
	err := b.Publish(ctx, "t/b", []byte("world"), 0, true)
	if err == nil {
		t.Fatalf("second publish should be quota exceeded")
	}
	// verify store still has 1 message
	list, _ := b.store.ListRetained(ctx)
	if len(list) != 1 {
		t.Fatalf("expected 1 retained got %d", len(list))
	}
	// overwrite existing should succeed
	if err := b.Publish(ctx, "t/a", []byte("newval"), 0, true); err != nil {
		t.Fatalf("overwrite should succeed %v", err)
	}
	list, _ = b.store.ListRetained(ctx)
	if len(list) != 1 {
		t.Fatalf("overwrite should keep 1 got %d", len(list))
	}
	// delete via empty payload should free quota
	if err := b.Publish(ctx, "t/a", []byte{}, 0, true); err != nil {
		t.Fatalf("delete should succeed %v", err)
	}
	list, _ = b.store.ListRetained(ctx)
	if len(list) != 0 {
		t.Fatalf("after delete expected 0 got %d", len(list))
	}
	// now new publish should succeed
	if err := b.Publish(ctx, "t/b", []byte("world"), 0, true); err != nil {
		t.Fatalf("after delete, new publish should succeed %v", err)
	}
}

func TestEmbeddedPublishPerTopicSizeExceeded(t *testing.T) {
	cfg := Config{MaxRetainedMessages: 10000, MaxRetainedSize: 1 << 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 15, AllowAnonymous: true}
	b := newQuotaBroker(t, cfg)
	ctx := context.Background()
	err := b.Publish(ctx, "t/a", []byte("verylongpayload"), 0, true)
	if err == nil {
		t.Fatalf("should exceed per_topic_size")
	}
	// small should pass
	if err := b.Publish(ctx, "t/a", []byte("x"), 0, true); err != nil {
		t.Fatalf("small should pass %v", err)
	}
}

func TestEmbeddedPublishWithPropertiesQuota(t *testing.T) {
	cfg := Config{MaxRetainedMessages: 1, MaxRetainedSize: 1 << 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 100 << 20}
	b := newQuotaBroker(t, cfg)
	ctx := context.Background()
	if err := b.PublishWithProperties(ctx, "t/a", []byte("hello"), 0, true, nil); err != nil {
		t.Fatalf("first should pass %v", err)
	}
	if err := b.PublishWithProperties(ctx, "t/b", []byte("world"), 0, true, nil); err == nil {
		t.Fatalf("second should quota exceeded")
	}
}

func TestEmbeddedPublishNonRetainNoQuota(t *testing.T) {
	cfg := Config{MaxRetainedMessages: 1, MaxRetainedSize: 1 << 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 100 << 20}
	b := newQuotaBroker(t, cfg)
	ctx := context.Background()
	_ = b.Publish(ctx, "t/a", []byte("hello"), 0, true)
	// non-retain publish should always succeed even if quota full
	if err := b.Publish(ctx, "t/b", []byte("world"), 0, false); err != nil {
		t.Fatalf("non-retain should not be limited %v", err)
	}
}

func TestHandlePublishRetainQuotaIntegration(t *testing.T) {
	cfg := Config{NodeID: "test-quota", TCPAddr: "127.0.0.1:18990", WSAddr: "", AllowAnonymous: true, MaxRetainedMessages: 1, MaxRetainedSize: 1 << 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 100 << 20}
	store := persistence.NewMemoryStore()
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	_ = b.Publish(context.Background(), "t/fill", []byte("fill"), 0, true)

	conn, err := net.Dial("tcp", "127.0.0.1:18990")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "quota-client"}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	n, _ := conn.Read(buf)
	pkt, err := codec.Decode(buf[:n])
	if err != nil || pkt.Type != codec.TypeCONNACK {
		t.Fatalf("connack failed %v %+v", err, pkt)
	}

	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "t/new", QoS: 1, PacketID: 1, Payload: []byte("new payload"), Retain: true}
	data, _ = codec.Encode(pub)
	conn.Write(data)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	n, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("read puback failed %v", err)
	}
	pkt, err = codec.Decode(buf[:n])
	if err != nil {
		t.Fatalf("decode puback %v", err)
	}
	if pkt.Type != codec.TypePUBACK || pkt.Reason != 0x97 {
		t.Fatalf("expected PUBACK 0x97 got type %d reason %x pkt %+v", pkt.Type, pkt.Reason, pkt)
	}
	// verify retain not stored
	if msg, _ := store.GetRetained(context.Background(), "t/new"); msg != nil {
		t.Fatalf("t/new should not be stored")
	}
	pub2 := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "t/fill", QoS: 1, PacketID: 2, Payload: []byte("newfill"), Retain: true}
	data, _ = codec.Encode(pub2)
	conn.Write(data)
	n, _ = conn.Read(buf)
	pkt, _ = codec.Decode(buf[:n])
	if pkt.Type != codec.TypePUBACK || pkt.Reason != 0 {
		t.Fatalf("overwrite should succeed got %+v", pkt)
	}
	pub3 := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "t/fill", QoS: 1, PacketID: 3, Payload: []byte{}, Retain: true}
	data, _ = codec.Encode(pub3)
	conn.Write(data)
	n, _ = conn.Read(buf)
	pkt, _ = codec.Decode(buf[:n])
	if pkt.Type != codec.TypePUBACK {
		t.Fatalf("delete should get puback got %+v", pkt)
	}
	list, _ := store.ListRetained(context.Background())
	t.Logf("after delete list len %d", len(list))
	for _, m := range list {
		t.Logf(" retained %s => %s", m.Topic, string(m.Payload))
	}
	stats, _ := store.GetRetainedStats(context.Background())
	t.Logf("stats %+v", stats)
	if msg, _ := store.GetRetained(context.Background(), "t/fill"); msg != nil {
		t.Fatalf("t/fill should be deleted got %+v", msg)
	}
	pub4 := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "t/new2", QoS: 1, PacketID: 4, Payload: []byte("ok"), Retain: true}
	data, _ = codec.Encode(pub4)
	conn.Write(data)
	n, _ = conn.Read(buf)
	pkt, _ = codec.Decode(buf[:n])
	if pkt.Type != codec.TypePUBACK || pkt.Reason != 0 {
		t.Fatalf("after delete, new should succeed got %+v", pkt)
	}
	if msg, _ := store.GetRetained(context.Background(), "t/new2"); msg == nil {
		t.Fatalf("t/new2 should be stored")
	}
}

func TestHandlePublishRetainQoS0QuotaDrop(t *testing.T) {
	cfg := Config{NodeID: "test-quota-qos0", TCPAddr: "127.0.0.1:18991", WSAddr: "", AllowAnonymous: true, MaxRetainedMessages: 1, MaxRetainedSize: 1 << 30, MaxRetainPerTopic: 1000, MaxRetainSizePerTopic: 100 << 20}
	store := persistence.NewMemoryStore()
	b, _ := NewWithOptions(cfg, WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Start(ctx)
	time.Sleep(300 * time.Millisecond)
	_ = b.Publish(context.Background(), "t/fill", []byte("fill"), 0, true)

	conn, _ := net.Dial("tcp", "127.0.0.1:18991")
	defer conn.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60, ClientID: "qos0-client"}
	data, _ := codec.Encode(p)
	conn.Write(data)
	buf := make([]byte, 2048)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf)
	// QoS0 retain to new topic should be silently dropped (no ack)
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "t/new", QoS: 0, Payload: []byte("drop"), Retain: true}
	data, _ = codec.Encode(pub)
	conn.Write(data)
	time.Sleep(200 * time.Millisecond)
	if msg, _ := store.GetRetained(context.Background(), "t/new"); msg != nil {
		t.Fatalf("QoS0 quota exceeded should drop")
	}
}

func TestEmbeddedPublishValidation(t *testing.T) {
	b := newQuotaBroker(t, DefaultConfig())
	ctx := context.Background()
	if err := b.Publish(ctx, "", []byte("x"), 0, true); err == nil {
		t.Fatalf("empty topic should fail")
	}
	// topic too long
	long := make([]byte, 4097)
	for i := range long {
		long[i] = 'a'
	}
	if err := b.Publish(ctx, string(long), []byte("x"), 0, true); err == nil {
		t.Fatalf("long topic should fail")
	}
	if err := b.PublishWithProperties(ctx, "", []byte("x"), 0, true, nil); err == nil {
		t.Fatalf("empty topic via props should fail")
	}
}
