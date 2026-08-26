package persistence

import (
	"context"
	"testing"

	"mqtt/internal/session"
)

func newTestPebbleStore(t *testing.T) *PebbleStore {
	t.Helper()
	dir := t.TempDir()
	s, err := NewPebbleStore(dir, "test")
	if err != nil {
		t.Fatalf("NewPebbleStore failed: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPebbleSession(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	s := session.NewSession("c1", 4, true, 0)
	s.Subscriptions["a/b"] = 1
	if err := store.SaveSession(ctx, s); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetSession(ctx, "c1")
	if err != nil || got == nil || got.ClientID != "c1" {
		t.Fatalf("get session failed %v %v", got, err)
	}
	if err := store.DeleteSession(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetSession(ctx, "c1")
	if got != nil {
		t.Fatalf("should be deleted")
	}
}

func TestPebbleRetained(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	msg := &Message{Topic: "t/a", Payload: []byte("hello"), QoS: 1, Retain: true}
	if err := store.SaveRetained(ctx, "t/a", msg); err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetRetained(ctx, "t/a")
	if string(got.Payload) != "hello" {
		t.Fatalf("retained mismatch")
	}
	list, _ := store.ListRetained(ctx)
	if len(list) != 1 {
		t.Fatalf("list retained 1 got %d", len(list))
	}
	stats, _ := store.GetRetainedStats(ctx)
	if stats.TotalMessages != 1 {
		t.Fatalf("stats 1 got %d", stats.TotalMessages)
	}
	if err := store.DeleteRetained(ctx, "t/a"); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetRetained(ctx, "t/a")
	if got != nil {
		t.Fatalf("should deleted")
	}
}

func TestPebbleOffline(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_ = store.EnqueueOffline(ctx, "c1", &Message{Topic: "t", Payload: []byte("m")})
	}
	msgs, _ := store.DequeueOffline(ctx, "c1")
	if len(msgs) != 3 {
		t.Fatalf("expected 3 got %d", len(msgs))
	}
	msgs, _ = store.DequeueOffline(ctx, "c1")
	if len(msgs) != 0 {
		t.Fatalf("should empty")
	}
}

func TestPebbleFallbackStore(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	primary, _ := NewPebbleStore(dir1, "p1")
	fallback, _ := NewPebbleStore(dir2, "p2")
	t.Cleanup(func() { _ = primary.Close(); _ = fallback.Close() })
	fb := NewFallbackStore(primary, fallback)
	ctx := context.Background()
	_ = fb.SaveRetained(ctx, "t/a", &Message{Topic: "t/a", Payload: []byte("x")})
	got, _ := fb.GetRetained(ctx, "t/a")
	if got == nil || string(got.Payload) != "x" {
		t.Fatalf("fallback get failed")
	}
	list, _ := fb.ListRetained(ctx)
	if len(list) != 1 {
		t.Fatalf("fallback list %d", len(list))
	}
}

func TestPebbleImplementsStore(t *testing.T) {
	var _ Store = (*PebbleStore)(nil)
	var _ Store = (*FallbackStore)(nil)
}
