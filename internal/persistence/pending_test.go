package persistence

import (
	"context"
	"testing"
)

func TestPendingWillMemory(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	pw := &PendingWill{ClientID: "c1", Topic: "will/t", Payload: []byte("hello"), QoS: 1, Retain: false, DeliverAt: 1234567890}
	if err := store.SavePendingWill(ctx, pw); err != nil {
		t.Fatal(err)
	}
	list, _ := store.ListPendingWills(ctx)
	if len(list) != 1 || list[0].ClientID != "c1" || string(list[0].Payload) != "hello" {
		t.Fatalf("list mismatch %+v", list)
	}
	// overwrite same clientID
	pw2 := &PendingWill{ClientID: "c1", Topic: "will/t2", Payload: []byte("bye"), QoS: 0, DeliverAt: 1234567900}
	_ = store.SavePendingWill(ctx, pw2)
	list, _ = store.ListPendingWills(ctx)
	if len(list) != 1 || list[0].Topic != "will/t2" {
		t.Fatalf("overwrite failed %+v", list)
	}
	if err := store.DeletePendingWill(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	list, _ = store.ListPendingWills(ctx)
	if len(list) != 0 {
		t.Fatalf("should empty after delete")
	}
	_ = store.DeletePendingWill(ctx, "nope")
}

func TestPendingRetryMemory(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	pr := &PendingRetry{ClientID: "c1", PacketID: 10, Topic: "a/b", Payload: []byte("p"), QoS: 1, NextRetryAt: 999, Retries: 0}
	if err := store.SavePendingRetry(ctx, pr); err != nil {
		t.Fatal(err)
	}
	pr2 := &PendingRetry{ClientID: "c1", PacketID: 11, Topic: "a/b", Payload: []byte("p2"), QoS: 1, NextRetryAt: 1000, Retries: 1}
	_ = store.SavePendingRetry(ctx, pr2)
	list, _ := store.ListPendingRetries(ctx)
	if len(list) != 2 {
		t.Fatalf("expected 2 got %d", len(list))
	}
	if err := store.DeletePendingRetry(ctx, "c1", 10); err != nil {
		t.Fatal(err)
	}
	list, _ = store.ListPendingRetries(ctx)
	if len(list) != 1 || list[0].PacketID != 11 {
		t.Fatalf("delete failed %+v", list)
	}
	_ = store.DeletePendingRetry(ctx, "c1", 99)
	list, _ = store.ListPendingRetries(ctx)
	if len(list) != 1 {
		t.Fatalf("should still 1")
	}
	_ = store.DeletePendingRetry(ctx, "c1", 11)
	list, _ = store.ListPendingRetries(ctx)
	if len(list) != 0 {
		t.Fatalf("should empty")
	}
}

func TestPendingPebble(t *testing.T) {
	dir := t.TempDir()
	store, err := NewPebbleStore(dir, "test")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	pw := &PendingWill{ClientID: "c-pebble", Topic: "will/x", Payload: []byte("pebble-will"), QoS: 0, DeliverAt: 111}
	if err := store.SavePendingWill(ctx, pw); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	store2, err := NewPebbleStore(dir, "test")
	if err != nil {
		t.Fatal(err)
	}
	list, _ := store2.ListPendingWills(ctx)
	if len(list) != 1 || string(list[0].Payload) != "pebble-will" {
		t.Fatalf("pebble will persist failed %+v", list)
	}
	_ = store2.DeletePendingWill(ctx, "c-pebble")
	list, _ = store2.ListPendingWills(ctx)
	if len(list) != 0 {
		t.Fatalf("pebble delete failed")
	}

	pr := &PendingRetry{ClientID: "c-pebble", PacketID: 5, Topic: "a/b", Payload: []byte("retry"), QoS: 1, NextRetryAt: 222, Retries: 0}
	_ = store2.SavePendingRetry(ctx, pr)
	_ = store2.Close()
	store3, _ := NewPebbleStore(dir, "test")
	defer store3.Close()
	list2, _ := store3.ListPendingRetries(ctx)
	if len(list2) != 1 || list2[0].PacketID != 5 {
		t.Fatalf("pebble retry persist failed %+v", list2)
	}
	_ = store3.DeletePendingRetry(ctx, "c-pebble", 5)
	list2, _ = store3.ListPendingRetries(ctx)
	if len(list2) != 0 {
		t.Fatalf("pebble retry delete failed")
	}
}

func TestPendingFallback(t *testing.T) {
	primary := NewMemoryStore()
	fallback := NewMemoryStore()
	fs := NewFallbackStore(primary, fallback)
	ctx := context.Background()
	pw := &PendingWill{ClientID: "fb", Topic: "fb/t", Payload: []byte("fb"), DeliverAt: 1}
	_ = fs.SavePendingWill(ctx, pw)
	list, _ := fs.ListPendingWills(ctx)
	if len(list) != 1 {
		t.Fatalf("fallback will failed")
	}
	pr := &PendingRetry{ClientID: "fb", PacketID: 1, Topic: "fb/t", NextRetryAt: 1}
	_ = fs.SavePendingRetry(ctx, pr)
	list2, _ := fs.ListPendingRetries(ctx)
	if len(list2) != 1 {
		t.Fatalf("fallback retry failed")
	}
	_ = fs.DeletePendingWill(ctx, "fb")
	_ = fs.DeletePendingRetry(ctx, "fb", 1)
	list, _ = fs.ListPendingWills(ctx)
	if len(list) != 0 {
		t.Fatalf("fallback delete will failed")
	}
}
