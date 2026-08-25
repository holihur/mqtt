package persistence

import (
	"context"
	"testing"

	"mqtt/internal/session"
)

func TestMemorySession(t *testing.T) {
	store := NewMemoryStore()
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
	// get non-existent returns nil,nil
	got, err = store.GetSession(ctx, "nope")
	if err != nil || got != nil {
		t.Fatalf("nonexistent should be nil")
	}
}

func TestMemoryRetained(t *testing.T) {
	store := NewMemoryStore()
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
		t.Fatalf("list retained 1")
	}
	if err := store.DeleteRetained(ctx, "t/a"); err != nil {
		t.Fatal(err)
	}
	got, _ = store.GetRetained(ctx, "t/a")
	if got != nil {
		t.Fatalf("should deleted")
	}
	list, _ = store.ListRetained(ctx)
	if len(list) != 0 {
		t.Fatalf("list should empty")
	}
	// delete non-existent not error
	_ = store.DeleteRetained(ctx, "nope")
}

func TestMemoryOfflineQueue(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	// enqueue and dequeue
	for i := 0; i < 3; i++ {
		_ = store.EnqueueOffline(ctx, "c1", &Message{Topic: "t", Payload: []byte("m")})
	}
	msgs, _ := store.DequeueOffline(ctx, "c1")
	if len(msgs) != 3 {
		t.Fatalf("expected 3 got %d", len(msgs))
	}
	// after dequeue, second dequeue empty
	msgs, _ = store.DequeueOffline(ctx, "c1")
	if len(msgs) != 0 {
		t.Fatalf("should empty after dequeue")
	}
	// clear offline
	_ = store.EnqueueOffline(ctx, "c2", &Message{Topic: "t"})
	_ = store.ClearOffline(ctx, "c2")
	msgs, _ = store.DequeueOffline(ctx, "c2")
	if len(msgs) != 0 {
		t.Fatalf("clear failed")
	}
}

func TestMemoryOfflineCap(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	// enqueue 1100, cap at 1000
	for i := 0; i < 1100; i++ {
		_ = store.EnqueueOffline(ctx, "c1", &Message{Topic: "t", Payload: []byte{byte(i)}})
	}
	msgs, _ := store.DequeueOffline(ctx, "c1")
	if len(msgs) != 1000 {
		t.Fatalf("cap 1000 expected got %d", len(msgs))
	}
}

func TestMemoryClose(t *testing.T) {
	store := NewMemoryStore()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryConcurrent(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			s := session.NewSession("c-conc", 4, true, 0)
			_ = store.SaveSession(ctx, s)
			_, _ = store.GetSession(ctx, "c-conc")
		}
		done <- true
	}()
	go func() {
		for i := 0; i < 100; i++ {
			_ = store.SaveRetained(ctx, "t/a", &Message{Topic: "t/a", Payload: []byte("x")})
			_, _ = store.GetRetained(ctx, "t/a")
		}
		done <- true
	}()
	<-done
	<-done
}
