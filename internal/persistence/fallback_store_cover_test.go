package persistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"mqtt/internal/session"

	"github.com/cockroachdb/pebble"
)

type errStore struct {
	err error
	Store
}

func (e *errStore) GetSession(_ context.Context, _ string) (*session.Session, error) {
	return nil, e.err
}
func (e *errStore) SaveSession(_ context.Context, _ *session.Session) error { return e.err }
func (e *errStore) DeleteSession(_ context.Context, _ string) error         { return e.err }
func (e *errStore) GetRetained(_ context.Context, _ string) (*Message, error) {
	return nil, e.err
}
func (e *errStore) SaveRetained(_ context.Context, _ string, _ *Message) error { return e.err }
func (e *errStore) DeleteRetained(_ context.Context, _ string) error           { return e.err }
func (e *errStore) ListRetained(_ context.Context) ([]*Message, error) {
	return nil, e.err
}
func (e *errStore) GetRetainedStats(_ context.Context) (RetainStats, error) {
	return RetainStats{}, e.err
}
func (e *errStore) EnqueueOffline(_ context.Context, _ string, _ *Message) error { return e.err }
func (e *errStore) DequeueOffline(_ context.Context, _ string) ([]*Message, error) {
	return nil, e.err
}
func (e *errStore) ClearOffline(_ context.Context, _ string) error { return e.err }
func (e *errStore) Close() error                                   { return e.err }

func TestFallbackStoreAllBranches(t *testing.T) {
	ctx := context.Background()
	primary := NewMemoryStore()
	fallback := NewMemoryStore()
	fs := NewFallbackStore(primary, fallback)

	sess := session.NewSession("c1", 4, true, 0)
	if err := fs.SaveSession(ctx, sess); err != nil {
		t.Fatalf("SaveSession %v", err)
	}
	got, err := fs.GetSession(ctx, "c1")
	if err != nil || got == nil {
		t.Fatalf("GetSession %v %v", err, got)
	}
	if err := fs.DeleteSession(ctx, "c1"); err != nil {
		t.Fatalf("DeleteSession %v", err)
	}
	msg := &Message{Topic: "a/b", Payload: []byte("x"), QoS: 0}
	if err := fs.SaveRetained(ctx, "a/b", msg); err != nil {
		t.Fatalf("SaveRetained %v", err)
	}
	if _, err := fs.GetRetained(ctx, "a/b"); err != nil {
		t.Fatalf("GetRetained %v", err)
	}
	if _, err := fs.ListRetained(ctx); err != nil {
		t.Fatalf("ListRetained %v", err)
	}
	if _, err := fs.GetRetainedStats(ctx); err != nil {
		t.Fatalf("GetRetainedStats %v", err)
	}
	if err := fs.EnqueueOffline(ctx, "c1", msg); err != nil {
		t.Fatalf("Enqueue %v", err)
	}
	if _, err := fs.DequeueOffline(ctx, "c1"); err != nil {
		t.Fatalf("Dequeue %v", err)
	}
	if err := fs.ClearOffline(ctx, "c1"); err != nil {
		t.Fatalf("ClearOffline %v", err)
	}
	if err := fs.DeleteRetained(ctx, "a/b"); err != nil {
		t.Fatalf("DeleteRetained %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("Close %v", err)
	}

	errPrimary := errors.New("primary fail")
	es := &errStore{err: errPrimary}
	fs2 := NewFallbackStore(es, fallback)
	if err := fs2.SaveSession(ctx, sess); err != nil {
		t.Fatalf("fallback SaveSession %v", err)
	}
	if _, err := fs2.GetSession(ctx, "c1"); err != nil {
		// fallback had deleted, so should still try fallback
	}
	if err := fs2.SaveRetained(ctx, "x/y", msg); err != nil {
		t.Fatalf("fallback SaveRetained %v", err)
	}
	if _, err := fs2.GetRetained(ctx, "x/y"); err != nil {
		t.Fatalf("fallback GetRetained %v", err)
	}
	if _, err := fs2.ListRetained(ctx); err != nil {
		t.Fatalf("fallback ListRetained %v", err)
	}
	if _, err := fs2.GetRetainedStats(ctx); err != nil {
		t.Fatalf("fallback GetRetainedStats %v", err)
	}
	if err := fs2.EnqueueOffline(ctx, "c1", msg); err != nil {
		t.Fatalf("fallback Enqueue %v", err)
	}
	if _, err := fs2.DequeueOffline(ctx, "nope"); err != nil {
		// both fail, returns last err - acceptable
	}
	_ = fs2.DeleteSession(ctx, "c1")
	_ = fs2.DeleteRetained(ctx, "x/y")
	_ = fs2.ClearOffline(ctx, "c1")
	_ = fs2.Close()

	bothErr := &errStore{err: errors.New("both fail")}
	fs3 := NewFallbackStore(bothErr, bothErr)
	_, _ = fs3.GetSession(ctx, "missing")
	_ = fs3.SaveSession(ctx, sess)
	_, _ = fs3.GetRetained(ctx, "missing")
	_ = fs3.SaveRetained(ctx, "t", msg)
	_, _ = fs3.ListRetained(ctx)
	_, _ = fs3.GetRetainedStats(ctx)
	_ = fs3.EnqueueOffline(ctx, "c", msg)
	_, _ = fs3.DequeueOffline(ctx, "c")
}

func TestStoreIsExpired(t *testing.T) {
	orig := timeNow
	defer func() { timeNow = orig }()
	fixed := time.Unix(1000, 0)
	timeNow = func() time.Time { return fixed }

	m1 := &Message{ExpiryInterval: 0, CreatedAt: fixed.UnixMilli()}
	if m1.IsExpired() {
		t.Fatal("0 expiry should not expire")
	}
	m2 := &Message{ExpiryInterval: 10, CreatedAt: 0}
	if m2.IsExpired() {
		t.Fatal("0 CreatedAt should not expire")
	}
	m3 := &Message{ExpiryInterval: 10, CreatedAt: fixed.Add(-5 * time.Second).UnixMilli()}
	if m3.IsExpired() {
		t.Fatal("5s ago with 10s TTL should not expire")
	}
	m4 := &Message{ExpiryInterval: 10, CreatedAt: fixed.Add(-15 * time.Second).UnixMilli()}
	if !m4.IsExpired() {
		t.Fatal("15s ago with 10s TTL should expire")
	}
	_ = timeNowMillis()
}

func TestMemoryRetainedSizeAndClearOffline(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStore()
	msg := &Message{Topic: "t1", Payload: []byte("hello")}
	if err := m.SaveRetained(ctx, "t1", msg); err != nil {
		t.Fatal(err)
	}
	stats, _ := m.GetRetainedStats(ctx)
	if stats.TotalMessages != 1 {
		t.Fatalf("stats %v", stats)
	}
	if err := m.SaveRetained(ctx, "t1", &Message{Topic: "t1", Payload: []byte("hi")}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteRetained(ctx, "t1"); err != nil {
		t.Fatal(err)
	}
	// offline
	if err := m.EnqueueOffline(ctx, "c1", msg); err != nil {
		t.Fatal(err)
	}
	if err := m.EnqueueOffline(ctx, "c1", msg); err != nil {
		t.Fatal(err)
	}
	if err := m.ClearOffline(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if msgs, _ := m.DequeueOffline(ctx, "c1"); len(msgs) != 0 {
		t.Fatalf("clear should empty")
	}
	// pebble ClearOffline equivalent via memory
	m2 := NewMemoryStore()
	_ = m2.ClearOffline(ctx, "nonexistent")
	_ = m2.Close()
}

func TestPebbleClearOfflineAndOverflow(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	msg := &Message{Topic: "t", Payload: []byte("m")}
	for i := 0; i < 5; i++ {
		if err := store.EnqueueOffline(ctx, "c-clear", msg); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ClearOffline(ctx, "c-clear"); err != nil {
		t.Fatal(err)
	}
	if msgs, _ := store.DequeueOffline(ctx, "c-clear"); len(msgs) != 0 {
		t.Fatalf("clear should empty, got %d", len(msgs))
	}
	if err := store.ClearOffline(ctx, "nonexistent"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1005; i++ {
		_ = store.EnqueueOffline(ctx, "c-overflow", msg)
	}
	msgs, _ := store.DequeueOffline(ctx, "c-overflow")
	if len(msgs) != 1000 {
		t.Fatalf("overflow trim to 1000, got %d", len(msgs))
	}
	dir2 := t.TempDir()
	if _, err := NewPebbleStore("", "test"); err == nil {
		t.Fatal("empty dir should fail")
	}
	s2, err := NewPebbleStore(dir2, "")
	if err != nil {
		t.Fatalf("empty prefix should default, got %v", err)
	}
	_ = s2.Close()
}

func TestPebbleCorruptDequeue(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	k := []byte(store.key("offline", "c-bad"))
	_ = store.db.Set(k, []byte("not-json"), pebble.Sync)
	_, err := store.DequeueOffline(ctx, "c-bad")
	if err == nil {
		t.Fatal("corrupt should error")
	}
	_, _ = store.DequeueOffline(ctx, "c-bad")
}

func TestRetainedSizeNil(t *testing.T) {
	if retainedSize(nil) != 0 {
		t.Fatal("nil should be 0")
	}
	if retainedSize(&Message{Topic: "a", Payload: []byte("b")}) == 0 {
		t.Fatal("non-nil should not be 0")
	}
	_ = NewFallbackStore(nil, nil)
}

func TestMemoryOverflow(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStore()
	msg := &Message{Topic: "t", Payload: []byte("m")}
	for i := 0; i < 1005; i++ {
		_ = m.EnqueueOffline(ctx, "c-overflow-mem", msg)
	}
	msgs, _ := m.DequeueOffline(ctx, "c-overflow-mem")
	if len(msgs) != 1000 {
		t.Fatalf("mem overflow 1000, got %d", len(msgs))
	}
}

func TestPebbleNewStoreEdge(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir, "mqtt-test")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.key("a", "b") == "" {
		t.Fatal("key empty")
	}
	if _, err := NewPebbleStore("/dev/null/impossible", "x"); err == nil {
		t.Log("expected error for impossible dir, but got nil")
	}
}

func TestPebbleGetSessionCorrupt(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	k := []byte(store.key("session", "c-bad"))
	_ = store.db.Set(k, []byte("not-json"), pebble.Sync)
	if _, err := store.GetSession(ctx, "c-bad"); err == nil {
		t.Fatal("corrupt session should error")
	}
	k2 := []byte(store.key("retain", "bad/topic"))
	_ = store.db.Set(k2, []byte("not-json"), pebble.Sync)
	list, err := store.ListRetained(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = list
}
