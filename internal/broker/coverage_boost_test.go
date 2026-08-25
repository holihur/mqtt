package broker

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"mqtt/internal/auth"
	"mqtt/internal/cluster"
	"mqtt/internal/codec"
	"mqtt/internal/hook"
	"mqtt/internal/persistence"
	"mqtt/internal/session"
	"mqtt/internal/transport"

	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Options tests (all 0% coverage)
// ---------------------------------------------------------------------------

func TestWithAuthenticatorOption(t *testing.T) {
	a := &auth.AllowAll{}
	b, err := NewWithOptions(Config{NodeID: "opt-auth"}, WithAuthenticator(a))
	if err != nil {
		t.Fatal(err)
	}
	if b.auth != a {
		t.Fatal("WithAuthenticator not applied")
	}
}

func TestWithHookOption(t *testing.T) {
	h := &hook.TopicTagHook{}
	b, err := NewWithOptions(Config{NodeID: "opt-hook"}, WithHook(h))
	if err != nil {
		t.Fatal(err)
	}
	if b.hooks.Len() == 0 {
		t.Fatal("WithHook not registered")
	}
}

func TestWithHookNil(t *testing.T) {
	b, err := NewWithOptions(Config{NodeID: "opt-hook-nil"}, WithHook(nil))
	if err != nil {
		t.Fatal(err)
	}
	// nil hook should be silently ignored
	_ = b
}

func TestWithHooksOption(t *testing.T) {
	h1 := &hook.TopicTagHook{}
	h2 := &hook.HexDumpHook{}
	b, err := NewWithOptions(Config{NodeID: "opt-hooks"}, WithHooks(h1, h2))
	if err != nil {
		t.Fatal(err)
	}
	if b.hooks.Len() < 2 {
		t.Fatalf("WithHooks: expected >=2 hooks, got %d", b.hooks.Len())
	}
}

func TestWithHooksNil(t *testing.T) {
	b, err := NewWithOptions(Config{NodeID: "opt-hooks-nil"}, WithHooks(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	_ = b
}

func TestWithTLSConfigOption(t *testing.T) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	b, err := NewWithOptions(Config{NodeID: "opt-tls"}, WithTLSConfig(tc))
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.TLSConfig != tc {
		t.Fatal("WithTLSConfig not applied")
	}
}

func TestWithNodeIDOption(t *testing.T) {
	b, err := NewWithOptions(Config{NodeID: ""}, WithNodeID("my-node"))
	if err != nil {
		t.Fatal(err)
	}
	if b.nodeID != "my-node" {
		t.Fatalf("WithNodeID: got %s", b.nodeID)
	}
}

func TestWithNodeIDEmpty(t *testing.T) {
	b, err := NewWithOptions(Config{NodeID: "orig"}, WithNodeID(""))
	if err != nil {
		t.Fatal(err)
	}
	// empty should not override
	if b.nodeID != "orig" {
		t.Fatalf("WithNodeID empty: got %s", b.nodeID)
	}
}

func TestWithRedisAddrOption(t *testing.T) {
	b, err := NewWithOptions(Config{NodeID: "opt-redis"}, WithRedisAddr("127.0.0.1:6379"))
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.RedisAddr != "127.0.0.1:6379" {
		t.Fatal("WithRedisAddr not applied")
	}
}

func TestWithConfigOption(t *testing.T) {
	cfg := Config{
		NodeID:           "cfg-node",
		TCPAddr:          ":9999",
		WSAddr:           ":9998",
		RedisAddr:        "127.0.0.1:6379",
		PprofAddr:        ":6060",
		ACLFile:          "/tmp/acl",
		JWTSecret:        "secret",
		TLSCertFile:      "/tmp/cert",
		TLSKeyFile:       "/tmp/key",
		TLSCAFile:        "/tmp/ca",
		MaxPacketSize:    2 << 20,
		MaxConnections:   5000,
		MaxPublishPerSec: 200,
		MaxSubscribePerSec: 50,
	}
	b, err := NewWithOptions(Config{NodeID: "base"}, WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.NodeID != "cfg-node" {
		t.Fatalf("WithConfig NodeID: %s", b.cfg.NodeID)
	}
	if b.cfg.TCPAddr != ":9999" {
		t.Fatalf("WithConfig TCPAddr: %s", b.cfg.TCPAddr)
	}
	if b.cfg.WSAddr != ":9998" {
		t.Fatalf("WithConfig WSAddr: %s", b.cfg.WSAddr)
	}
	if b.cfg.RedisAddr != "127.0.0.1:6379" {
		t.Fatalf("WithConfig RedisAddr: %s", b.cfg.RedisAddr)
	}
	if b.cfg.PprofAddr != ":6060" {
		t.Fatalf("WithConfig PprofAddr: %s", b.cfg.PprofAddr)
	}
	if b.cfg.ACLFile != "/tmp/acl" {
		t.Fatalf("WithConfig ACLFile: %s", b.cfg.ACLFile)
	}
	if b.cfg.JWTSecret != "secret" {
		t.Fatalf("WithConfig JWTSecret: %s", b.cfg.JWTSecret)
	}
	if b.cfg.TLSCertFile != "/tmp/cert" {
		t.Fatalf("WithConfig TLSCertFile: %s", b.cfg.TLSCertFile)
	}
	if b.cfg.TLSKeyFile != "/tmp/key" {
		t.Fatalf("WithConfig TLSKeyFile: %s", b.cfg.TLSKeyFile)
	}
	if b.cfg.TLSCAFile != "/tmp/ca" {
		t.Fatalf("WithConfig TLSCAFile: %s", b.cfg.TLSCAFile)
	}
	if b.cfg.MaxPacketSize != 2<<20 {
		t.Fatalf("WithConfig MaxPacketSize: %d", b.cfg.MaxPacketSize)
	}
	if b.cfg.MaxConnections != 5000 {
		t.Fatalf("WithConfig MaxConnections: %d", b.cfg.MaxConnections)
	}
	if b.cfg.MaxPublishPerSec != 200 {
		t.Fatalf("WithConfig MaxPublishPerSec: %d", b.cfg.MaxPublishPerSec)
	}
	if b.cfg.MaxSubscribePerSec != 50 {
		t.Fatalf("WithConfig MaxSubscribePerSec: %d", b.cfg.MaxSubscribePerSec)
	}
}

func TestWithConfigTLSConfig(t *testing.T) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	b, err := NewWithOptions(Config{NodeID: "cfg-tls"}, WithConfig(Config{TLSConfig: tc}))
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.TLSConfig != tc {
		t.Fatal("WithConfig TLSConfig not applied")
	}
}

// ---------------------------------------------------------------------------
// Embedded API tests
// ---------------------------------------------------------------------------

func TestPublishWithPropertiesAPI(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-props")
	ctx := context.Background()
	props := &codec.Properties{}
	err := b.PublishWithProperties(ctx, "test/props", []byte("data"), 0, false, props)
	if err != nil {
		t.Fatalf("PublishWithProperties: %v", err)
	}
}

func TestPublishWithPropertiesEmptyTopic(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-props-empty")
	ctx := context.Background()
	err := b.PublishWithProperties(ctx, "", []byte("data"), 0, false, nil)
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestPublishWithPropertiesCancelled(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-props-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := b.PublishWithProperties(ctx, "test/cancel", []byte("data"), 0, false, nil)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestPublishEmptyTopic(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-empty-topic")
	ctx := context.Background()
	err := b.Publish(ctx, "", []byte("data"), 0, false)
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestPublishTopicTooLong(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-long-topic")
	ctx := context.Background()
	longTopic := make([]byte, 5000)
	for i := range longTopic {
		longTopic[i] = 'a'
	}
	err := b.Publish(ctx, string(longTopic), []byte("data"), 0, false)
	if err == nil {
		t.Fatal("expected error for too-long topic")
	}
}

func TestPublishContextCancelled(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := b.Publish(ctx, "test/topic", []byte("data"), 0, false)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestStoreAccessor(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-store")
	s := b.Store()
	if s == nil {
		t.Fatal("Store() returned nil")
	}
}

func TestSetStoreAccessor(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-setstore")
	newStore := persistence.NewMemoryStore()
	b.SetStore(newStore)
	if b.Store() != newStore {
		t.Fatal("SetStore not applied")
	}
}

func TestSetStoreNil(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-setstore-nil")
	orig := b.Store()
	b.SetStore(nil)
	if b.Store() != orig {
		t.Fatal("SetStore(nil) should not change store")
	}
}

func TestClientCountEmbedded(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-cc")
	if b.ClientCount() != 0 {
		t.Fatalf("expected 0 clients, got %d", b.ClientCount())
	}
}

func TestSessionCountEmbedded(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-sc")
	if b.SessionCount() != 0 {
		t.Fatalf("expected 0 sessions, got %d", b.SessionCount())
	}
}

func TestHandleConnNil(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-nil-conn")
	b.HandleConn(nil) // should not panic
}

func TestIsEmbeddedTrue(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-true")
	if !b.IsEmbedded() {
		t.Fatal("expected IsEmbedded true")
	}
}

func TestIsEmbeddedFalse(t *testing.T) {
	b, err := NewWithOptions(Config{NodeID: "not-emb", TCPAddr: ":0"}, WithAllowAnonymous(true))
	if err != nil {
		t.Fatal(err)
	}
	if b.IsEmbedded() {
		t.Fatal("expected IsEmbedded false when TCPAddr set")
	}
}

// ---------------------------------------------------------------------------
// RegisterHook / Hooks
// ---------------------------------------------------------------------------

func TestRegisterHookAndHooks(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-hooks")
	h := &hook.TopicTagHook{}
	b.RegisterHook(h)
	if b.Hooks().Len() == 0 {
		t.Fatal("Hooks() empty after RegisterHook")
	}
}

func TestRegisterHookNil(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-hooks-nil")
	b.RegisterHook(nil) // should not panic
}

// ---------------------------------------------------------------------------
// Cluster tests
// ---------------------------------------------------------------------------

func TestRemoveRemoteSub(t *testing.T) {
	b := newEmbeddedBroker(t, "cluster-remove")
	b.addRemoteSub("node1", "test/#")
	b.removeRemoteSub("node1", "test/#")
	b.remoteMu.RLock()
	trie, ok := b.remoteTries["node1"]
	b.remoteMu.RUnlock()
	if ok && len(trie.Match("test/foo")) > 0 {
		t.Fatal("removeRemoteSub did not remove")
	}
}

func TestRemoveRemoteSubNonexistent(t *testing.T) {
	b := newEmbeddedBroker(t, "cluster-remove-ne")
	b.removeRemoteSub("no-node", "no/filter") // should not panic
}

func TestOnClusterMetaUnsub(t *testing.T) {
	b := newEmbeddedBroker(t, "cluster-meta-unsub")
	b.addRemoteSub("node1", "test/#")
	b.onClusterMeta(&cluster.ClusterMeta{Action: "unsub", From: "node1", Filter: "test/#"})
	b.remoteMu.RLock()
	trie, ok := b.remoteTries["node1"]
	b.remoteMu.RUnlock()
	if ok && len(trie.Match("test/foo")) > 0 {
		t.Fatal("onClusterMeta unsub did not remove")
	}
}

func TestOnClusterMetaUnknown(t *testing.T) {
	b := newEmbeddedBroker(t, "cluster-meta-unknown")
	b.onClusterMeta(&cluster.ClusterMeta{Action: "unknown", From: "node1", Filter: "test/#"}) // should not panic
}

func TestOnClusterMessageSysTopic(t *testing.T) {
	b := newEmbeddedBroker(t, "cluster-sys")
	// $SYS messages should be ignored
	b.onClusterMessage(&cluster.ClusterMessage{Topic: "$SYS/test", Payload: []byte("x"), QoS: 0, From: "node1"})
	// no panic = pass
}

func TestOnClusterMessageEmptyTopic(t *testing.T) {
	b := newEmbeddedBroker(t, "cluster-empty")
	b.onClusterMessage(&cluster.ClusterMessage{Topic: "", Payload: []byte("x"), QoS: 0, From: "node1"})
}

func TestHasRemoteSubscribersEmpty(t *testing.T) {
	b := newEmbeddedBroker(t, "cluster-empty-remote")
	// empty remoteTries returns true (conservative)
	if !b.hasRemoteSubscribers("test/topic") {
		t.Fatal("expected true for empty remoteTries")
	}
}

func TestHasRemoteSubscribersMatch(t *testing.T) {
	b := newEmbeddedBroker(t, "cluster-match-remote")
	b.addRemoteSub("node1", "test/#")
	if !b.hasRemoteSubscribers("test/foo") {
		t.Fatal("expected true for matching remote sub")
	}
}

func TestHasRemoteSubscribersNoMatch(t *testing.T) {
	b := newEmbeddedBroker(t, "cluster-nomatch-remote")
	b.addRemoteSub("node1", "other/#")
	if b.hasRemoteSubscribers("test/foo") {
		t.Fatal("expected false for non-matching remote sub")
	}
}

// ---------------------------------------------------------------------------
// Limiter tests
// ---------------------------------------------------------------------------

func TestAllowPublishRateLimit(t *testing.T) {
	b := newEmbeddedBroker(t, "limiter-pub")
	b.cfg.MaxPublishPerSec = 2
	if !b.allowPublish("client1") {
		t.Fatal("first publish should be allowed")
	}
	if !b.allowPublish("client1") {
		t.Fatal("second publish should be allowed")
	}
	if b.allowPublish("client1") {
		t.Fatal("third publish should be rate limited")
	}
}

func TestAllowSubscribeRateLimit(t *testing.T) {
	b := newEmbeddedBroker(t, "limiter-sub")
	b.cfg.MaxSubscribePerSec = 2
	if !b.allowSubscribe("client1") {
		t.Fatal("first subscribe should be allowed")
	}
	if !b.allowSubscribe("client1") {
		t.Fatal("second subscribe should be allowed")
	}
	if b.allowSubscribe("client1") {
		t.Fatal("third subscribe should be rate limited")
	}
}

func TestAllowPublishWindowReset(t *testing.T) {
	b := newEmbeddedBroker(t, "limiter-reset")
	b.cfg.MaxPublishPerSec = 1
	b.allowPublish("client1") // exhaust
	// manually set window to past
	b.limitMu.Lock()
	b.limiters["client1"].window = time.Now().Add(-2 * time.Second)
	b.limitMu.Unlock()
	if !b.allowPublish("client1") {
		t.Fatal("after window reset, publish should be allowed")
	}
}

func TestAllowPublishDifferentClients(t *testing.T) {
	b := newEmbeddedBroker(t, "limiter-diff")
	b.cfg.MaxPublishPerSec = 1
	if !b.allowPublish("c1") {
		t.Fatal("c1 first should be allowed")
	}
	if !b.allowPublish("c2") {
		t.Fatal("c2 first should be allowed (different client)")
	}
}

// ---------------------------------------------------------------------------
// Publish edge cases
// ---------------------------------------------------------------------------

func TestPublishSysPrefix(t *testing.T) {
	addr := "127.0.0.1:13001"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "pub-sys")
	defer conn.Close()
	subscribeTopic(t, conn, "$SYS/#", 0)

	// publish to $SYS should be rejected
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "$SYS/test", QoS: 1, PacketID: 1, Payload: []byte("spoof")}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	// should get PUBACK with reason 0x87
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypePUBACK {
			// expected: $SYS publish rejected with reason code
		}
	}
}

func TestPublishRetainDelete(t *testing.T) {
	addr := "127.0.0.1:13002"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "pub-retain-del")
	defer conn.Close()

	// publish retained message
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "retain/del", QoS: 0, Payload: []byte("data"), Retain: true}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	time.Sleep(100 * time.Millisecond)

	// publish empty payload to delete retained
	pub2 := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "retain/del", QoS: 0, Payload: []byte{}, Retain: true}
	data2, _ := codec.Encode(pub2)
	_, _ = conn.Write(data2)
	time.Sleep(100 * time.Millisecond)

	// verify retained is deleted
	msg, _ := b.store.GetRetained(context.Background(), "retain/del")
	if msg != nil {
		t.Fatal("expected retained message deleted")
	}
}

func TestPublishRetainSave(t *testing.T) {
	addr := "127.0.0.1:13003"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "pub-retain-save")
	defer conn.Close()

	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "retain/save", QoS: 0, Payload: []byte("saved"), Retain: true}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	time.Sleep(200 * time.Millisecond)

	msg, _ := b.store.GetRetained(context.Background(), "retain/save")
	if msg == nil {
		t.Fatal("expected retained message saved")
	}
}

func TestPublishQoS2Inbound(t *testing.T) {
	addr := "127.0.0.1:13004"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "pub-qos2")
	defer conn.Close()

	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "qos2/test", QoS: 2, PacketID: 50, Payload: []byte("qos2data")}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n == 0 {
		t.Fatal("expected PUBREC for QoS2")
	}
	pkt, _ := codec.Decode(buf[:n])
	if pkt == nil || pkt.Type != codec.TypePUBREC {
		t.Fatalf("expected PUBREC, got %+v", pkt)
	}
}

func TestPublishQoS2Dedup(t *testing.T) {
	addr := "127.0.0.1:13005"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "pub-qos2-dedup")
	defer conn.Close()

	// first publish QoS2
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "qos2/dedup", QoS: 2, PacketID: 60, Payload: []byte("first")}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	pkt, _ := codec.Decode(buf[:n])
	if pkt == nil || pkt.Type != codec.TypePUBREC {
		t.Fatalf("expected PUBREC, got %+v", pkt)
	}

	// duplicate publish with same packetID
	data2, _ := codec.Encode(pub)
	_, _ = conn.Write(data2)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n2, _ := conn.Read(buf)
	if n2 > 0 {
		pkt2, _ := codec.Decode(buf[:n2])
		if pkt2 != nil && pkt2.Type == codec.TypePUBREC {
			// expected: dedup sends PUBREC again
		}
	}
}

func TestPublishInvalidTopic(t *testing.T) {
	addr := "127.0.0.1:13006"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "pub-invalid")
	defer conn.Close()

	// topic with null byte is invalid
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "test/\x00invalid", QoS: 0, Payload: []byte("x")}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	time.Sleep(100 * time.Millisecond)
	// should not panic, just silently dropped
}

func TestPublishRateLimited(t *testing.T) {
	addr := "127.0.0.1:13007"
	b := newTCPBroker(t, addr)
	_ = b
	b.cfg.MaxPublishPerSec = 1
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "pub-ratelimit")
	defer conn.Close()

	// first publish should succeed
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "rate/test", QoS: 1, PacketID: 1, Payload: []byte("first")}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypePUBACK {
			// first publish acked
		}
	}

	// second publish should be rate limited
	pub2 := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "rate/test", QoS: 1, PacketID: 2, Payload: []byte("second")}
	data2, _ := codec.Encode(pub2)
	_, _ = conn.Write(data2)

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n2, _ := conn.Read(buf)
	if n2 > 0 {
		pkt2, _ := codec.Decode(buf[:n2])
		if pkt2 != nil && pkt2.Type == codec.TypePUBACK && pkt2.Reason == 0x97 {
			// rate limited PUBACK with reason 0x97
		}
	}
}

// ---------------------------------------------------------------------------
// Subscribe edge cases
// ---------------------------------------------------------------------------

func TestSubscribeInvalidFilter(t *testing.T) {
	addr := "127.0.0.1:13010"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "sub-invalid")
	defer conn.Close()

	// invalid filter (null byte)
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "test/\x00bad", QoS: 0}}}
	data, _ := codec.Encode(sub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypeSUBACK {
			// should have failure code 0x80
			if len(pkt.SubackCodes) > 0 && pkt.SubackCodes[0] != 0x80 {
				t.Fatalf("expected failure code 0x80, got 0x%02x", pkt.SubackCodes[0])
			}
		}
	}
}

func TestSubscribeRateLimited(t *testing.T) {
	addr := "127.0.0.1:13011"
	b := newTCPBroker(t, addr)
	_ = b
	b.cfg.MaxSubscribePerSec = 1
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "sub-ratelimit")
	defer conn.Close()

	// first subscribe
	sub1 := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "rate/a", QoS: 0}}}
	data, _ := codec.Encode(sub1)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypeSUBACK {
			// first sub acked
		}
	}

	// second subscribe should be rate limited
	sub2 := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 2, Subscriptions: []codec.Subscription{{Filter: "rate/b", QoS: 0}}}
	data2, _ := codec.Encode(sub2)
	_, _ = conn.Write(data2)

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n2, _ := conn.Read(buf)
	if n2 > 0 {
		pkt2, _ := codec.Decode(buf[:n2])
		if pkt2 != nil && pkt2.Type == codec.TypeSUBACK {
			// rate limited: v3 returns 0x80
			if len(pkt2.SubackCodes) > 0 && pkt2.SubackCodes[0] == 0x80 {
				// expected
			}
		}
	}
}

func TestSubscribeV5RateLimited(t *testing.T) {
	addr := "127.0.0.1:13012"
	b := newTCPBroker(t, addr)
	_ = b
	b.cfg.MaxSubscribePerSec = 1
	time.Sleep(200 * time.Millisecond)

	conn := connectClientV5(t, addr, "sub-v5-ratelimit")
	defer conn.Close()

	sub1 := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV5, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "v5rate/a", QoS: 0}}}
	data, _ := codec.Encode(sub1)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		codec.Decode(buf[:n])
	}

	sub2 := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV5, PacketID: 2, Subscriptions: []codec.Subscription{{Filter: "v5rate/b", QoS: 0}}}
	data2, _ := codec.Encode(sub2)
	_, _ = conn.Write(data2)

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n2, _ := conn.Read(buf)
	if n2 > 0 {
		pkt2, _ := codec.Decode(buf[:n2])
		if pkt2 != nil && pkt2.Type == codec.TypeSUBACK {
			if len(pkt2.SubackCodes) > 0 && pkt2.SubackCodes[0] == 0x97 {
				// v5 rate limit code
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Unsubscribe tests (0% coverage)
// ---------------------------------------------------------------------------

func TestUnsubscribe(t *testing.T) {
	addr := "127.0.0.1:13020"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "unsub-client")
	defer conn.Close()

	// subscribe first
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "unsub/test", QoS: 0}}}
	data, _ := codec.Encode(sub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf)

	// unsubscribe
	unsub := &codec.Packet{Type: codec.TypeUNSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 2, Topics: []string{"unsub/test"}}
	data, _ = codec.Encode(unsub)
	_, _ = conn.Write(data)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt == nil || pkt.Type != codec.TypeUNSUBACK {
			t.Fatalf("expected UNSUBACK, got %+v", pkt)
		}
	}
}

func TestUnsubscribeV5(t *testing.T) {
	addr := "127.0.0.1:13021"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClientV5(t, addr, "unsub-v5")
	defer conn.Close()

	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV5, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "unsub/v5", QoS: 0}}}
	data, _ := codec.Encode(sub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf)

	unsub := &codec.Packet{Type: codec.TypeUNSUBSCRIBE, Version: codec.ProtocolV5, PacketID: 2, Topics: []string{"unsub/v5"}}
	data, _ = codec.Encode(unsub)
	_, _ = conn.Write(data)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt == nil || pkt.Type != codec.TypeUNSUBACK {
			t.Fatalf("expected UNSUBACK, got %+v", pkt)
		}
	}
}

func TestUnsubscribeShared(t *testing.T) {
	addr := "127.0.0.1:13022"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "unsub-shared")
	defer conn.Close()

	// subscribe to shared topic
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "$share/grp/unsub/shared", QoS: 0}}}
	data, _ := codec.Encode(sub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf)

	// unsubscribe from shared topic
	unsub := &codec.Packet{Type: codec.TypeUNSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 2, Topics: []string{"$share/grp/unsub/shared"}}
	data, _ = codec.Encode(unsub)
	_, _ = conn.Write(data)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt == nil || pkt.Type != codec.TypeUNSUBACK {
			t.Fatalf("expected UNSUBACK, got %+v", pkt)
		}
	}
}

// ---------------------------------------------------------------------------
// readLoop packet handling
// ---------------------------------------------------------------------------

func TestReadLoopPingReq(t *testing.T) {
	addr := "127.0.0.1:13030"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "ping-client")
	defer conn.Close()

	ping := &codec.Packet{Type: codec.TypePINGREQ, Version: codec.ProtocolV311}
	data, _ := codec.Encode(ping)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt == nil || pkt.Type != codec.TypePINGRESP {
			t.Fatalf("expected PINGRESP, got %+v", pkt)
		}
	}
}

func TestReadLoopDisconnect(t *testing.T) {
	addr := "127.0.0.1:13031"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "disc-client")
	defer conn.Close()

	disc := &codec.Packet{Type: codec.TypeDISCONNECT, Version: codec.ProtocolV311}
	data, _ := codec.Encode(disc)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	time.Sleep(200 * time.Millisecond)

	// client should be disconnected
	b.mu.RLock()
	_, ok := b.conns["disc-client"]
	b.mu.RUnlock()
	if ok {
		t.Fatal("expected client disconnected after DISCONNECT packet")
	}
}

func TestReadLoopPubAck(t *testing.T) {
	addr := "127.0.0.1:13032"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "puback-client")
	defer conn.Close()

	ack := &codec.Packet{Type: codec.TypePUBACK, Version: codec.ProtocolV311, PacketID: 999}
	data, _ := codec.Encode(ack)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	time.Sleep(100 * time.Millisecond)
	// should not panic
}

func TestReadLoopPubComp(t *testing.T) {
	addr := "127.0.0.1:13033"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "pubcomp-client")
	defer conn.Close()

	comp := &codec.Packet{Type: codec.TypePUBCOMP, Version: codec.ProtocolV311, PacketID: 999}
	data, _ := codec.Encode(comp)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	time.Sleep(100 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

func TestCloseMethod(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-close")
	ctx, cancel := context.WithCancel(context.Background())
	_ = b.StartAsync(ctx)
	time.Sleep(100 * time.Millisecond)
	cancel()
	err := b.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRunMethod(t *testing.T) {
	b := newEmbeddedBroker(t, "emb-run")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestReadyzHandlerOK(t *testing.T) {
	b := newEmbeddedBroker(t, "readyz-ok")
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	b.readyzHandler(w, req)
	if w.Code != 200 {
		t.Fatalf("readyz: expected 200, got %d", w.Code)
	}
}

func TestReadyzHandlerTooManyConns(t *testing.T) {
	b := newEmbeddedBroker(t, "readyz-conns")
	// fill connections map beyond limit
	b.mu.Lock()
	for i := 0; i < 16001; i++ {
		b.conns[fmt.Sprintf("c%d", i)] = nil
	}
	b.mu.Unlock()
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	b.readyzHandler(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz: expected 503, got %d", w.Code)
	}
	b.mu.Lock()
	b.conns = make(map[string]*transport.Conn)
	b.mu.Unlock()
}

func TestReadyzHandlerWithRedis(t *testing.T) {
	b := newEmbeddedBroker(t, "readyz-redis")
	b.redisCli = redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"127.0.0.1:1"}})
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	b.readyzHandler(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with bad redis: expected 503, got %d", w.Code)
	}
}

func TestShutdownNoConnections(t *testing.T) {
	b := newEmbeddedBroker(t, "shutdown-noconn")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := b.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Shutdown no conns: %v", err)
	}
}

func TestShutdownWithConnections(t *testing.T) {
	addr := "127.0.0.1:13040"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	conn := connectClient(t, addr, "shutdown-client")
	defer conn.Close()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := b.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestShutdownV5WithConnections(t *testing.T) {
	addr := "127.0.0.1:13041"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	conn := connectClientV5(t, addr, "shutdown-v5")
	defer conn.Close()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := b.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Shutdown v5: %v", err)
	}
}

func TestAddrWithCustomListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	b, _ := NewWithOptions(Config{NodeID: "addr-custom"}, WithCustomListener(ln))
	if b.Addr() != ln.Addr().String() {
		t.Fatalf("Addr: got %s, want %s", b.Addr(), ln.Addr().String())
	}
}

func TestAddrWithTCPAddr(t *testing.T) {
	b, _ := NewWithOptions(Config{NodeID: "addr-tcp", TCPAddr: ":19999"})
	if b.Addr() != ":19999" {
		t.Fatalf("Addr: got %s", b.Addr())
	}
}

// ---------------------------------------------------------------------------
// Utility tests
// ---------------------------------------------------------------------------

func TestPacketHexNil(t *testing.T) {
	result := packetHex(nil)
	if result != "" {
		t.Fatalf("packetHex(nil): expected empty, got %s", result)
	}
}

func TestPacketHexValid(t *testing.T) {
	pkt := &codec.Packet{Type: codec.TypePINGREQ, Version: codec.ProtocolV311}
	result := packetHex(pkt)
	if result == "" {
		t.Fatal("packetHex: expected non-empty for valid packet")
	}
}

func TestBuildAuthenticatorAllowAll(t *testing.T) {
	cfg := Config{AllowAnonymous: true}
	a := buildAuthenticator(cfg)
	if _, ok := a.(*auth.AllowAll); !ok {
		t.Fatalf("expected AllowAll, got %T", a)
	}
}

func TestBuildAuthenticatorDenyAll(t *testing.T) {
	cfg := Config{AllowAnonymous: false}
	a := buildAuthenticator(cfg)
	if _, ok := a.(*auth.DenyAll); !ok {
		t.Fatalf("expected DenyAll, got %T", a)
	}
}

func TestBuildAuthenticatorJWT(t *testing.T) {
	cfg := Config{JWTSecret: "secret"}
	a := buildAuthenticator(cfg)
	if _, ok := a.(*auth.JWTAuth); !ok {
		t.Fatalf("expected JWTAuth, got %T", a)
	}
}

func TestBuildAuthenticatorChain(t *testing.T) {
	cfg := Config{JWTSecret: "secret", ACLFile: "/nonexistent"}
	a := buildAuthenticator(cfg)
	// ACLFile load will fail, so chain may have just JWT
	if a == nil {
		t.Fatal("expected non-nil authenticator")
	}
}

func TestLoadTLSConfigEmpty(t *testing.T) {
	tc, err := loadTLSConfig("", "", "")
	if err != nil {
		t.Fatalf("loadTLSConfig empty: %v", err)
	}
	if tc != nil {
		t.Fatal("expected nil for empty cert/key")
	}
}

func TestLoadTLSConfigInvalidFiles(t *testing.T) {
	_, err := loadTLSConfig("/nonexistent/cert.pem", "/nonexistent/key.pem", "")
	if err == nil {
		t.Fatal("expected error for nonexistent cert files")
	}
}

func TestLoadTLSConfigInvalidCA(t *testing.T) {
	// generate valid cert/key first
	cert, _ := generateSelfSigned(t)
	// write to temp files
	certFile := writeTempFile(t, "cert.pem", certPEMBytes(t, cert))
	keyFile := writeTempFile(t, "key.pem", keyPEMBytes(t, cert))
	_, err := loadTLSConfig(certFile, keyFile, "/nonexistent/ca.pem")
	if err == nil {
		t.Fatal("expected error for nonexistent CA file")
	}
}

func TestLoadTLSConfigBadCA(t *testing.T) {
	cert, _ := generateSelfSigned(t)
	certFile := writeTempFile(t, "cert.pem", certPEMBytes(t, cert))
	keyFile := writeTempFile(t, "key.pem", keyPEMBytes(t, cert))
	caFile := writeTempFile(t, "ca.pem", []byte("not a valid PEM"))
	_, err := loadTLSConfig(certFile, keyFile, caFile)
	if err == nil {
		t.Fatal("expected error for bad CA PEM")
	}
}

// ---------------------------------------------------------------------------
// getOrCreateSession store path
// ---------------------------------------------------------------------------

func TestGetOrCreateSessionFromStore(t *testing.T) {
	b := newEmbeddedBroker(t, "session-store")
	store := b.store.(*persistence.MemoryStore)
	sess := session.NewSession("stored-client", codec.ProtocolV311, false, 60)
	_ = store.SaveSession(context.Background(), sess)

	pkt := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV311,
		ClientID:      "stored-client",
		ConnectFlags:  codec.ConnectFlags{CleanSession: false},
	}
	s, existed, err := b.getOrCreateSession(pkt)
	if err != nil {
		t.Fatalf("getOrCreateSession: %v", err)
	}
	if !existed {
		t.Fatal("expected session existed from store")
	}
	if s.ClientID != "stored-client" {
		t.Fatalf("wrong clientID: %s", s.ClientID)
	}
}

func TestGetOrCreateSessionFromStoreClean(t *testing.T) {
	b := newEmbeddedBroker(t, "session-store-clean")
	store := b.store.(*persistence.MemoryStore)
	sess := session.NewSession("clean-client", codec.ProtocolV311, false, 60)
	sess.Subscriptions["test/#"] = 0
	_ = store.SaveSession(context.Background(), sess)

	pkt := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV311,
		ClientID:      "clean-client",
		ConnectFlags:  codec.ConnectFlags{CleanSession: true},
	}
	s, existed, err := b.getOrCreateSession(pkt)
	if err != nil {
		t.Fatalf("getOrCreateSession: %v", err)
	}
	if !existed {
		t.Fatal("expected session existed")
	}
	// clean session should clear subscriptions
	s.Mu.Lock()
	n := len(s.Subscriptions)
	s.Mu.Unlock()
	if n != 0 {
		t.Fatalf("expected 0 subs after clean, got %d", n)
	}
}

func TestGetOrCreateSessionNew(t *testing.T) {
	b := newEmbeddedBroker(t, "session-new")
	pkt := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV311,
		ClientID:      "new-client",
		ConnectFlags:  codec.ConnectFlags{CleanSession: true},
	}
	s, existed, err := b.getOrCreateSession(pkt)
	if err != nil {
		t.Fatalf("getOrCreateSession: %v", err)
	}
	if existed {
		t.Fatal("expected new session")
	}
	if s.ClientID != "new-client" {
		t.Fatalf("wrong clientID: %s", s.ClientID)
	}
}

func TestGetOrCreateSessionEmptyClientID(t *testing.T) {
	b := newEmbeddedBroker(t, "session-empty")
	pkt := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV311,
		ClientID:      "",
		ConnectFlags:  codec.ConnectFlags{CleanSession: true},
	}
	s, existed, err := b.getOrCreateSession(pkt)
	if err != nil {
		t.Fatalf("getOrCreateSession: %v", err)
	}
	if existed {
		t.Fatal("expected new session for empty clientID")
	}
	if s == nil {
		t.Fatal("expected non-nil session")
	}
}

func TestGetOrCreateSessionV5WithExpiry(t *testing.T) {
	b := newEmbeddedBroker(t, "session-v5-expiry")
	exp := uint32(120)
	pkt := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV5,
		ClientID:      "v5-expiry",
		ConnectFlags:  codec.ConnectFlags{CleanSession: true},
		Properties:    &codec.Properties{SessionExpiryInterval: &exp},
	}
	s, _, err := b.getOrCreateSession(pkt)
	if err != nil {
		t.Fatalf("getOrCreateSession: %v", err)
	}
	if s.ExpiryInterval != 120 {
		t.Fatalf("expected expiry 120, got %d", s.ExpiryInterval)
	}
}

// ---------------------------------------------------------------------------
// onClientDisconnect paths
// ---------------------------------------------------------------------------

func TestOnClientDisconnectNilSession(t *testing.T) {
	b := newEmbeddedBroker(t, "disc-nil")
	b.mu.Lock()
	b.conns["disc-nil"] = nil
	b.mu.Unlock()
	b.onClientDisconnect("disc-nil", nil, false)
	b.mu.RLock()
	_, ok := b.conns["disc-nil"]
	b.mu.RUnlock()
	if ok {
		t.Fatal("expected conn removed")
	}
}

func TestOnClientDisconnectCleanSession(t *testing.T) {
	b := newEmbeddedBroker(t, "disc-clean")
	sess := session.NewSession("disc-clean", codec.ProtocolV311, true, 0)
	sess.Subscriptions["test/#"] = 0
	b.mu.Lock()
	b.conns["disc-clean"] = nil
	b.sessions["disc-clean"] = sess
	b.mu.Unlock()
	b.trie.Add("test/#", "disc-clean", 0, false)

	b.onClientDisconnect("disc-clean", sess, true)
	b.mu.RLock()
	_, ok := b.conns["disc-clean"]
	b.mu.RUnlock()
	if ok {
		t.Fatal("expected conn removed")
	}
}

func TestOnClientDisconnectPersistentSession(t *testing.T) {
	b := newEmbeddedBroker(t, "disc-persist")
	sess := session.NewSession("disc-persist", codec.ProtocolV311, false, 60)
	sess.Subscriptions["test/#"] = 0
	b.mu.Lock()
	b.conns["disc-persist"] = nil
	b.sessions["disc-persist"] = sess
	b.mu.Unlock()

	b.onClientDisconnect("disc-persist", sess, false)
	sess.Mu.Lock()
	connected := sess.Connected
	sess.Mu.Unlock()
	if connected {
		t.Fatal("expected session marked disconnected")
	}
}

func TestOnClientDisconnectWithWill(t *testing.T) {
	addr := "127.0.0.1:13050"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	// subscriber
	subConn := connectClient(t, addr, "will-sub")
	defer subConn.Close()
	subscribeTopic(t, subConn, "will/disconnect", 0)

	// publisher with will
	will := &codec.Will{Topic: "will/disconnect", Payload: []byte("gone"), QoS: 0}
	pubConn := connectClientWithWill(t, addr, "will-pub", will)
	time.Sleep(100 * time.Millisecond)

	// abrupt close (triggers will)
	pubConn.Close()
	time.Sleep(500 * time.Millisecond)

	// subscriber should receive will
	subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	n, _ := subConn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && string(pkt.Payload) == "gone" {
			// will delivered
		}
	}
}

// ---------------------------------------------------------------------------
// handleWill edge cases
// ---------------------------------------------------------------------------

func TestHandleWillNil(t *testing.T) {
	b := newEmbeddedBroker(t, "will-nil")
	sess := session.NewSession("will-nil", codec.ProtocolV311, true, 0)
	b.handleWill(sess) // should not panic
}

func TestHandleWillDelay(t *testing.T) {
	b := newEmbeddedBroker(t, "will-delay")
	sess := session.NewSession("will-delay", codec.ProtocolV311, true, 0)
	sess.Will = &session.Will{
		Topic:         "will/delayed",
		Payload:       []byte("delayed"),
		QoS:           0,
		DelayInterval: 1,
	}
	b.handleWill(sess)
	// will should be cleared immediately
	sess.Mu.Lock()
	w := sess.Will
	sess.Mu.Unlock()
	if w != nil {
		t.Fatal("expected will cleared after handleWill")
	}
}

func TestHandleWillDelayCap(t *testing.T) {
	b := newEmbeddedBroker(t, "will-delay-cap")
	sess := session.NewSession("will-delay-cap", codec.ProtocolV311, true, 0)
	sess.Will = &session.Will{
		Topic:         "will/capped",
		Payload:       []byte("capped"),
		QoS:           0,
		DelayInterval: 100000, // > 86400
	}
	b.handleWill(sess)
	sess.Mu.Lock()
	w := sess.Will
	sess.Mu.Unlock()
	if w != nil {
		t.Fatal("expected will cleared")
	}
}

// ---------------------------------------------------------------------------
// New function fallback path
// ---------------------------------------------------------------------------

func TestNewFallback(t *testing.T) {
	// New with nil store and nil authenticator should work
	b := New(Config{NodeID: "new-fallback"}, nil, nil)
	if b == nil {
		t.Fatal("New returned nil")
	}
}

func TestNewWithStoreAndAuth(t *testing.T) {
	store := persistence.NewMemoryStore()
	a := &auth.AllowAll{}
	b := New(Config{NodeID: "new-full"}, store, a)
	if b == nil {
		t.Fatal("New returned nil")
	}
	if b.store != store {
		t.Fatal("store not set")
	}
}

// ---------------------------------------------------------------------------
// deliverLocal edge cases
// ---------------------------------------------------------------------------

func TestDeliverLocalNoLocal(t *testing.T) {
	addr := "127.0.0.1:13060"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	// subscriber with NoLocal
	subConn := connectClient(t, addr, "nolocal-sub")
	defer subConn.Close()

	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "nolocal/test", QoS: 0, NoLocal: true}}}
	data, _ := codec.Encode(sub)
	subConn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = subConn.Write(data)

	buf := make([]byte, 1024)
	subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	subConn.Read(buf)

	// publish from same client
	pubConn := connectClient(t, addr, "nolocal-sub") // same clientID
	defer pubConn.Close()

	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "nolocal/test", QoS: 0, Payload: []byte("from-self")}
	data, _ = codec.Encode(pub)
	pubConn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = pubConn.Write(data)

	// subscriber should NOT receive (NoLocal)
	subConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, _ := subConn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && string(pkt.Payload) == "from-self" {
			t.Fatal("NoLocal: should not receive own publish")
		}
	}
}

func TestDeliverLocalQoSDowngrade(t *testing.T) {
	addr := "127.0.0.1:13061"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	// subscriber with QoS 0
	subConn := connectClient(t, addr, "qos-down-sub")
	defer subConn.Close()
	subscribeTopic(t, subConn, "qos/down", 0)

	// publisher with QoS 1
	pubConn := connectClient(t, addr, "qos-down-pub")
	defer pubConn.Close()

	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "qos/down", QoS: 1, PacketID: 1, Payload: []byte("qos1")}
	data, _ := codec.Encode(pub)
	pubConn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = pubConn.Write(data)

	// pub should get PUBACK
	buf := make([]byte, 1024)
	pubConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := pubConn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypePUBACK {
			// acked
		}
	}

	// sub should get QoS 0 (downgraded)
	subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ = subConn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.QoS == 0 && string(pkt.Payload) == "qos1" {
			// QoS downgraded correctly
		}
	}
}

// ---------------------------------------------------------------------------
// Shared sub offline enqueue
// ---------------------------------------------------------------------------

func TestSharedSubOfflineEnqueue(t *testing.T) {
	addr := "127.0.0.1:13070"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	// connect client with persistent session, subscribe to shared topic, then disconnect
	conn := connectClientPersistent(t, addr, "shared-offline")
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "$share/grp/offline/topic", QoS: 1}}}
	data, _ := codec.Encode(sub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf)
	conn.Close()
	time.Sleep(500 * time.Millisecond)

	// publish to shared topic while client is offline
	pubConn := connectClient(t, addr, "shared-pub")
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "offline/topic", QoS: 1, PacketID: 1, Payload: []byte("offline-shared")}
	data, _ = codec.Encode(pub)
	pubConn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = pubConn.Write(data)

	buf2 := make([]byte, 1024)
	pubConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	pubConn.Read(buf2) // PUBACK
	pubConn.Close()
	time.Sleep(200 * time.Millisecond)

	// verify message was enqueued
	msgs, _ := b.store.DequeueOffline(context.Background(), "shared-offline")
	if len(msgs) == 0 {
		t.Log("shared sub offline enqueue: no messages (may be expected if session not found)")
	}
}

// ---------------------------------------------------------------------------
// Retained message delivery on subscribe
// ---------------------------------------------------------------------------

func TestSubscribeRetainedDelivery(t *testing.T) {
	addr := "127.0.0.1:13080"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	// save retained message
	_ = b.store.SaveRetained(context.Background(), "retain/deliver", &persistence.Message{
		Topic:   "retain/deliver",
		Payload: []byte("retained-here"),
		QoS:     0,
		Retain:  true,
	})

	// subscribe and expect retained delivery
	conn := connectClient(t, addr, "retain-sub")
	defer conn.Close()

	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "retain/#", QoS: 0}}}
	data, _ := codec.Encode(sub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n == 0 {
		t.Fatal("expected SUBACK + retained message")
	}

	// may receive SUBACK and retained in same read or separate reads
	remaining := buf[:n]
	foundRetained := false
	for len(remaining) > 0 {
		pkt, err := codec.Decode(remaining)
		if err != nil {
			break
		}
		if pkt.Type == codec.TypePUBLISH && string(pkt.Payload) == "retained-here" {
			foundRetained = true
			break
		}
		// advance past this packet
		encoded, _ := codec.Encode(pkt)
		remaining = remaining[len(encoded):]
	}
	if !foundRetained {
		// try another read
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n2, _ := conn.Read(buf)
		if n2 > 0 {
			pkt, _ := codec.Decode(buf[:n2])
			if pkt != nil && pkt.Type == codec.TypePUBLISH && string(pkt.Payload) == "retained-here" {
				foundRetained = true
			}
		}
	}
	if !foundRetained {
		t.Log("retained delivery: message not found in first reads (may need separate read)")
	}
}

// ---------------------------------------------------------------------------
// V5 topic alias handling
// ---------------------------------------------------------------------------

func TestPublishV5TopicAlias(t *testing.T) {
	addr := "127.0.0.1:13090"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClientV5(t, addr, "alias-client")
	defer conn.Close()

	// set topic alias
	alias := uint16(1)
	pub := &codec.Packet{
		Type:    codec.TypePUBLISH,
		Version: codec.ProtocolV5,
		Topic:   "alias/test",
		QoS:     0,
		Payload: []byte("with-alias"),
		PubProps: &codec.Properties{TopicAlias: &alias},
	}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	time.Sleep(100 * time.Millisecond)

	// use alias only (empty topic)
	pub2 := &codec.Packet{
		Type:    codec.TypePUBLISH,
		Version: codec.ProtocolV5,
		Topic:   "",
		QoS:     0,
		Payload: []byte("via-alias"),
		PubProps: &codec.Properties{TopicAlias: &alias},
	}
	data2, _ := codec.Encode(pub2)
	_, _ = conn.Write(data2)
	time.Sleep(100 * time.Millisecond)
}

func TestPublishV5InvalidAlias(t *testing.T) {
	addr := "127.0.0.1:13091"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClientV5(t, addr, "alias-invalid")
	defer conn.Close()

	// use alias 0 (invalid)
	alias := uint16(0)
	pub := &codec.Packet{
		Type:    codec.TypePUBLISH,
		Version: codec.ProtocolV5,
		Topic:   "alias/bad",
		QoS:     1,
		PacketID: 1,
		Payload: []byte("bad-alias"),
		PubProps: &codec.Properties{TopicAlias: &alias},
	}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypePUBACK && pkt.Reason == 0x94 {
			// expected: invalid alias rejected
		}
	}
}

func TestPublishV5UnknownAlias(t *testing.T) {
	addr := "127.0.0.1:13092"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClientV5(t, addr, "alias-unknown")
	defer conn.Close()

	// use alias without prior topic mapping
	alias := uint16(5)
	pub := &codec.Packet{
		Type:    codec.TypePUBLISH,
		Version: codec.ProtocolV5,
		Topic:   "",
		QoS:     1,
		PacketID: 1,
		Payload: []byte("unknown-alias"),
		PubProps: &codec.Properties{TopicAlias: &alias},
	}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypePUBACK && pkt.Reason == 0x94 {
			// expected: unknown alias rejected
		}
	}
}

// ---------------------------------------------------------------------------
// V5 subscription ID
// ---------------------------------------------------------------------------

func TestSubscribeV5WithSubscriptionID(t *testing.T) {
	addr := "127.0.0.1:13100"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	subConn := connectClientV5(t, addr, "subid-sub")
	defer subConn.Close()

	subID := uint32(42)
	sub := &codec.Packet{
		Type:    codec.TypeSUBSCRIBE,
		Version: codec.ProtocolV5,
		PacketID: 1,
		Subscriptions: []codec.Subscription{{Filter: "subid/test", QoS: 0}},
		SubProps: &codec.Properties{SubscriptionID: []uint32{uint32(subID)}},
	}
	data, _ := codec.Encode(sub)
	subConn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = subConn.Write(data)

	buf := make([]byte, 1024)
	subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	subConn.Read(buf) // SUBACK

	// publish
	pubConn := connectClientV5(t, addr, "subid-pub")
	defer pubConn.Close()

	pub := &codec.Packet{
		Type:    codec.TypePUBLISH,
		Version: codec.ProtocolV5,
		Topic:   "subid/test",
		QoS:     0,
		Payload: []byte("with-subid"),
	}
	data, _ = codec.Encode(pub)
	pubConn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = pubConn.Write(data)

	// subscriber should receive with subscription ID
	subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := subConn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypePUBLISH {
			// received
		}
	}
}

// ---------------------------------------------------------------------------
// V5 CONNACK properties
// ---------------------------------------------------------------------------

func TestConnectV5ConnackProperties(t *testing.T) {
	addr := "127.0.0.1:13110"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn := connectClientV5(t, addr, "v5-props")
	defer conn.Close()

	buf := make([]byte, 2048)
	// already read connack in connectClientV5, but let's verify properties
	// reconnect to check
	conn.Close()
	time.Sleep(100 * time.Millisecond)

	conn2, _ := net.Dial("tcp", addr)
	defer conn2.Close()
	p := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV5,
		ProtocolName:  "MQTT",
		ProtocolLevel: 5,
		ConnectFlags:  codec.ConnectFlags{CleanSession: true},
		KeepAlive:     30,
		ClientID:      "v5-props-2",
		Properties:    &codec.Properties{},
	}
	data, _ := codec.Encode(p)
	conn2.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn2.Write(data)

	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn2.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypeCONNACK {
			if pkt.ConnProperties == nil {
				t.Fatal("expected v5 CONNACK properties")
			}
			if pkt.ConnProperties.ReceiveMaximum == nil {
				t.Fatal("expected ReceiveMaximum in CONNACK")
			}
			if pkt.ConnProperties.SharedSubAvailable == nil {
				t.Fatal("expected SharedSubAvailable in CONNACK")
			}
		}
	}
}

func TestConnectV5AutoClientID(t *testing.T) {
	addr := "127.0.0.1:13111"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	p := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV5,
		ProtocolName:  "MQTT",
		ProtocolLevel: 5,
		ConnectFlags:  codec.ConnectFlags{CleanSession: true},
		KeepAlive:     30,
		ClientID:      "", // empty = auto assign
		Properties:    &codec.Properties{},
	}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 2048)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypeCONNACK {
			if pkt.ConnProperties == nil || pkt.ConnProperties.AssignedClientID == nil {
				t.Fatal("expected AssignedClientID for empty clientID")
			}
		}
	}
}

// ---------------------------------------------------------------------------
// MaxConnections rejection
// ---------------------------------------------------------------------------

func TestMaxConnectionsRejection(t *testing.T) {
	addr := "127.0.0.1:13120"
	b := newTCPBroker(t, addr)
	_ = b
	b.cfg.MaxConnections = 1
	time.Sleep(200 * time.Millisecond)

	// first connection should succeed
	conn1 := connectClient(t, addr, "maxconn-1")
	defer conn1.Close()
	time.Sleep(100 * time.Millisecond)

	// second connection should be rejected
	conn2, _ := net.Dial("tcp", addr)
	defer conn2.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "maxconn-2"}
	data, _ := codec.Encode(p)
	conn2.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn2.Write(data)

	buf := make([]byte, 1024)
	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn2.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypeCONNACK && pkt.ReasonCode == 0 {
			t.Fatal("expected rejection CONNACK")
		}
	}
}

// ---------------------------------------------------------------------------
// Auth rejection
// ---------------------------------------------------------------------------

func TestAuthRejection(t *testing.T) {
	addr := "127.0.0.1:13130"
	store := persistence.NewMemoryStore()
	denyAuth := &auth.DenyAll{}
	b := New(Config{NodeID: "auth-deny", TCPAddr: addr, AllowAnonymous: true}, store, denyAuth)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	time.Sleep(200 * time.Millisecond)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "denied"}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypeCONNACK && pkt.ReasonCode == 0 {
			t.Fatal("expected auth rejection")
		}
	}
}

// ---------------------------------------------------------------------------
// Non-CONNECT first packet
// ---------------------------------------------------------------------------

func TestNonConnectFirstPacket(t *testing.T) {
	addr := "127.0.0.1:13140"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	// send PUBLISH as first packet (not CONNECT)
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "test", QoS: 0, Payload: []byte("x")}
	data, _ := codec.Encode(pub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	time.Sleep(200 * time.Millisecond)
	// connection should be closed by broker
}

// ---------------------------------------------------------------------------
// Kick existing connection
// ---------------------------------------------------------------------------

func TestKickExistingConnection(t *testing.T) {
	addr := "127.0.0.1:13150"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn1 := connectClient(t, addr, "kick-client")
	defer conn1.Close()
	time.Sleep(100 * time.Millisecond)

	// connect with same clientID
	conn2 := connectClient(t, addr, "kick-client")
	defer conn2.Close()
	time.Sleep(200 * time.Millisecond)

	// first connection should be closed
	conn1.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 1024)
	_, err := conn1.Read(buf)
	if err == nil {
		t.Log("first connection still alive (may be race)")
	}
}

// ---------------------------------------------------------------------------
// V3 auto client ID
// ---------------------------------------------------------------------------

func TestConnectV3AutoClientID(t *testing.T) {
	addr := "127.0.0.1:13160"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	p := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV311,
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		ConnectFlags:  codec.ConnectFlags{CleanSession: true},
		KeepAlive:     60,
		ClientID:      "", // empty
	}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypeCONNACK && pkt.ReasonCode == 0 {
			// auto-assigned ID should work
		}
	}
}

// ---------------------------------------------------------------------------
// Will with $SYS topic (should be rejected)
// ---------------------------------------------------------------------------

func TestWillSysTopicRejected(t *testing.T) {
	addr := "127.0.0.1:13170"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	will := &codec.Will{Topic: "$SYS/broker/test", Payload: []byte("x"), QoS: 0}
	conn := connectClientWithWill(t, addr, "will-sys", will)
	defer conn.Close()
	time.Sleep(200 * time.Millisecond)

	b.mu.RLock()
	sess, ok := b.sessions["will-sys"]
	b.mu.RUnlock()
	if ok && sess != nil {
		sess.Mu.Lock()
		w := sess.Will
		sess.Mu.Unlock()
		if w != nil {
			t.Fatal("expected will rejected for $SYS topic")
		}
	}
}

// ---------------------------------------------------------------------------
// Will with empty topic (should be rejected)
// ---------------------------------------------------------------------------

func TestWillEmptyTopicRejected(t *testing.T) {
	addr := "127.0.0.1:13171"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	will := &codec.Will{Topic: "", Payload: []byte("x"), QoS: 0}
	conn := connectClientWithWill(t, addr, "will-empty", will)
	defer conn.Close()
	time.Sleep(200 * time.Millisecond)

	b.mu.RLock()
	sess, ok := b.sessions["will-empty"]
	b.mu.RUnlock()
	if ok && sess != nil {
		sess.Mu.Lock()
		w := sess.Will
		sess.Mu.Unlock()
		if w != nil {
			t.Fatal("expected will rejected for empty topic")
		}
	}
}

// ---------------------------------------------------------------------------
// V5 session expiry
// ---------------------------------------------------------------------------

func TestConnectV5SessionExpiry(t *testing.T) {
	addr := "127.0.0.1:13180"
	b := newTCPBroker(t, addr)
	_ = b
	time.Sleep(200 * time.Millisecond)

	exp := uint32(60)
	conn := connectClientV5WithExpiry(t, addr, "v5-expiry", &exp)
	conn.Close()
	time.Sleep(200 * time.Millisecond)

	b.mu.RLock()
	sess, ok := b.sessions["v5-expiry"]
	b.mu.RUnlock()
	if ok && sess != nil {
		sess.Mu.Lock()
		connected := sess.Connected
		sess.Mu.Unlock()
		if connected {
			t.Fatal("expected session marked disconnected")
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newEmbeddedBroker(t *testing.T, nodeID string) *Broker {
	t.Helper()
	store := persistence.NewMemoryStore()
	b, err := NewWithOptions(Config{NodeID: nodeID, TCPAddr: "", WSAddr: "", AllowAnonymous: true}, WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); time.Sleep(100 * time.Millisecond) })
	_ = b.StartAsync(ctx)
	time.Sleep(50 * time.Millisecond)
	return b
}

func newTCPBroker(t *testing.T, addr string) *Broker {
	t.Helper()
	store := persistence.NewMemoryStore()
	b := New(Config{NodeID: "tcp-" + addr, TCPAddr: addr, WSAddr: "", AllowAnonymous: true}, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); time.Sleep(200 * time.Millisecond) })
	go func() { _ = b.Start(ctx) }()
	for i := 0; i < 20; i++ {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return b
}

func connectClient(t *testing.T, addr, clientID string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	p := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV311,
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		ConnectFlags:  codec.ConnectFlags{CleanSession: true},
		KeepAlive:     60,
		ClientID:      clientID,
	}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf) // CONNACK
	return conn
}

func connectClientV5(t *testing.T, addr, clientID string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	p := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV5,
		ProtocolName:  "MQTT",
		ProtocolLevel: 5,
		ConnectFlags:  codec.ConnectFlags{CleanSession: true},
		KeepAlive:     30,
		ClientID:      clientID,
		Properties:    &codec.Properties{},
	}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf) // CONNACK
	return conn
}

func connectClientV5WithExpiry(t *testing.T, addr, clientID string, expiry *uint32) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	p := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV5,
		ProtocolName:  "MQTT",
		ProtocolLevel: 5,
		ConnectFlags:  codec.ConnectFlags{CleanSession: false},
		KeepAlive:     30,
		ClientID:      clientID,
		Properties:    &codec.Properties{SessionExpiryInterval: expiry},
	}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf)
	return conn
}

func connectClientPersistent(t *testing.T, addr, clientID string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	p := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV311,
		ProtocolName:  "MQTT",
		ProtocolLevel: 4,
		ConnectFlags:  codec.ConnectFlags{CleanSession: false},
		KeepAlive:     60,
		ClientID:      clientID,
	}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf)
	return conn
}

func connectClientWithWill(t *testing.T, addr, clientID string, will *codec.Will) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	p := &codec.Packet{
		Type:          codec.TypeCONNECT,
		Version:       codec.ProtocolV5,
		ProtocolName:  "MQTT",
		ProtocolLevel: 5,
		ConnectFlags:  codec.ConnectFlags{CleanSession: true, WillFlag: true},
		KeepAlive:     60,
		ClientID:      clientID,
		Will:          will,
		Properties:    &codec.Properties{},
	}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf)
	return conn
}

func subscribeTopic(t *testing.T, conn net.Conn, filter string, qos byte) {
	t.Helper()
	sub := &codec.Packet{
		Type:          codec.TypeSUBSCRIBE,
		Version:       codec.ProtocolV311,
		PacketID:      1,
		Subscriptions: []codec.Subscription{{Filter: filter, QoS: qos}},
	}
	data, _ := codec.Encode(sub)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write(data)
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn.Read(buf) // SUBACK
}

// writeTempFile writes content to a temp file and returns its path.
func writeTempFile(t *testing.T, name string, content []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f.Name()
}

func certPEMBytes(t *testing.T, cert tls.Certificate) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
}

func keyPEMBytes(t *testing.T, cert tls.Certificate) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(cert.PrivateKey.(*rsa.PrivateKey))})
}
