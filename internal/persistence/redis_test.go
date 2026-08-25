package persistence

import (
	"context"
	"fmt"
	"testing"
	"time"

	"mqtt/internal/session"

	"github.com/redis/go-redis/v9"
)

func newTestRedisStore(t *testing.T) *RedisStore {
	t.Helper()
	cli := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"127.0.0.1:6379"}})
	if err := cli.Ping(context.Background()).Err(); err != nil {
		t.Skip("redis not available")
	}
	prefix := fmt.Sprintf("test:mqtt:%d", time.Now().UnixNano())
	rs := NewRedisStoreWithClient(cli, prefix)
	t.Cleanup(func() {
		// flush prefix keys
		ctx := context.Background()
		keys, _ := cli.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			_ = cli.Del(ctx, keys...).Err()
		}
		_ = cli.Close()
	})
	return rs
}

func TestRedisSession(t *testing.T) {
	rs := newTestRedisStore(t)
	ctx := context.Background()
	s := session.NewSession("redis-c1", 4, true, 0)
	s.Subscriptions["a/b"] = 1
	if err := rs.SaveSession(ctx, s); err != nil {
		t.Fatal(err)
	}
	got, err := rs.GetSession(ctx, "redis-c1")
	if err != nil || got == nil || got.ClientID != "redis-c1" {
		t.Fatalf("get session failed %v %v", got, err)
	}
	if got.Subscriptions["a/b"] != 1 {
		t.Fatalf("subs not persisted")
	}
	if err := rs.DeleteSession(ctx, "redis-c1"); err != nil {
		t.Fatal(err)
	}
	got, _ = rs.GetSession(ctx, "redis-c1")
	if got != nil {
		t.Fatalf("should deleted")
	}
	// get non-existent
	got, err = rs.GetSession(ctx, "nope")
	if err != nil || got != nil {
		t.Fatalf("nonexistent should nil")
	}
}

func TestRedisSessionNilMaps(t *testing.T) {
	rs := newTestRedisStore(t)
	ctx := context.Background()
	// save session with nil maps, ensure Get handles nil init
	s := &session.Session{ClientID: "nil-maps", Subscriptions: nil, Inflight: nil}
	if err := rs.SaveSession(ctx, s); err != nil {
		t.Fatal(err)
	}
	got, err := rs.GetSession(ctx, "nil-maps")
	if err != nil || got == nil {
		t.Fatal(err)
	}
	if got.Subscriptions == nil || got.Inflight == nil {
		t.Fatalf("nil maps should be initialized")
	}
}

func TestRedisRetained(t *testing.T) {
	rs := newTestRedisStore(t)
	ctx := context.Background()
	msg := &Message{Topic: "t/a", Payload: []byte("hello"), QoS: 1, Retain: true}
	if err := rs.SaveRetained(ctx, "t/a", msg); err != nil {
		t.Fatal(err)
	}
	got, _ := rs.GetRetained(ctx, "t/a")
	if string(got.Payload) != "hello" {
		t.Fatalf("retained mismatch")
	}
	list, _ := rs.ListRetained(ctx)
	if len(list) != 1 {
		t.Fatalf("list 1 got %d", len(list))
	}
	if err := rs.DeleteRetained(ctx, "t/a"); err != nil {
		t.Fatal(err)
	}
	got, _ = rs.GetRetained(ctx, "t/a")
	if got != nil {
		t.Fatalf("should deleted")
	}
	// list after delete empty
	list, _ = rs.ListRetained(ctx)
	if len(list) != 0 {
		t.Fatalf("list empty after delete")
	}
}

func TestRedisListRetainedMultiple(t *testing.T) {
	rs := newTestRedisStore(t)
	ctx := context.Background()
	_ = rs.SaveRetained(ctx, "t/a", &Message{Topic: "t/a", Payload: []byte("1")})
	_ = rs.SaveRetained(ctx, "t/b", &Message{Topic: "t/b", Payload: []byte("2")})
	list, _ := rs.ListRetained(ctx)
	if len(list) != 2 {
		t.Fatalf("list 2 got %d", len(list))
	}
}

func TestRedisOfflineQueue(t *testing.T) {
	rs := newTestRedisStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_ = rs.EnqueueOffline(ctx, "c1", &Message{Topic: "t", Payload: []byte("m")})
	}
	msgs, _ := rs.DequeueOffline(ctx, "c1")
	if len(msgs) != 3 {
		t.Fatalf("expected 3 got %d", len(msgs))
	}
	msgs, _ = rs.DequeueOffline(ctx, "c1")
	if len(msgs) != 0 {
		t.Fatalf("should empty")
	}
	_ = rs.EnqueueOffline(ctx, "c2", &Message{Topic: "t"})
	_ = rs.ClearOffline(ctx, "c2")
	msgs, _ = rs.DequeueOffline(ctx, "c2")
	if len(msgs) != 0 {
		t.Fatalf("clear failed")
	}
}

func TestRedisOfflineCap(t *testing.T) {
	rs := newTestRedisStore(t)
	ctx := context.Background()
	for i := 0; i < 1100; i++ {
		_ = rs.EnqueueOffline(ctx, "c-cap", &Message{Topic: "t", Payload: []byte{byte(i)}})
	}
	msgs, _ := rs.DequeueOffline(ctx, "c-cap")
	if len(msgs) != 1000 {
		t.Fatalf("cap 1000 got %d", len(msgs))
	}
}

func TestRedisNewStore(t *testing.T) {
	_, err := NewRedisStore("127.0.0.1:0", "mqtt")
	if err == nil {
		t.Fatalf("should fail with bad addr")
	}
	rs, err := NewRedisStore("127.0.0.1:6379", "")
	if err != nil {
		t.Fatalf("should succeed %v", err)
	}
	if rs.prefix != "mqtt" {
		t.Fatalf("default prefix")
	}
	_ = rs.Close()
}

func TestRedisStoreClientAndKey(t *testing.T) {
	rs := newTestRedisStore(t)
	if rs.Client() == nil {
		t.Fatalf("client nil")
	}
	if rs.key("a", "b") != rs.prefix+":a:b" {
		t.Fatalf("key wrong")
	}
	// clientIDKey helper
	if clientIDKey("x") != "x" {
		t.Fatalf("clientIDKey")
	}
}
