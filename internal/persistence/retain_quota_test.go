package persistence

import (
	"context"
	"testing"
)

func TestGetRetainedStatsEmpty(t *testing.T) {
	store := NewMemoryStore()
	stats, err := store.GetRetainedStats(context.Background())
	if err != nil {
		t.Fatalf("GetRetainedStats failed: %v", err)
	}
	if stats.TotalMessages != 0 {
		t.Fatalf("expected 0 messages got %d", stats.TotalMessages)
	}
	if stats.TotalSize != 0 {
		t.Fatalf("expected 0 size got %d", stats.TotalSize)
	}
	if len(stats.TopicStats) != 0 {
		t.Fatalf("expected 0 topic stats got %d", len(stats.TopicStats))
	}
}

func TestGetRetainedStatsSingle(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	msg := &Message{Topic: "t/a", Payload: []byte("hello"), QoS: 1, Retain: true}
	if err := store.SaveRetained(ctx, "t/a", msg); err != nil {
		t.Fatal(err)
	}
	stats, err := store.GetRetainedStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalMessages != 1 {
		t.Fatalf("expected 1 got %d", stats.TotalMessages)
	}
	expectedSize := int64(len("t/a") + len("hello") + 10)
	if stats.TotalSize != expectedSize {
		t.Fatalf("size mismatch expected %d got %d", expectedSize, stats.TotalSize)
	}
	ts, ok := stats.TopicStats["t/a"]
	if !ok {
		t.Fatalf("topic t/a not found")
	}
	if ts.Count != 1 || ts.Size != expectedSize {
		t.Fatalf("topic stats mismatch %+v", ts)
	}
}

func TestGetRetainedStatsMultiple(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		topic := string(rune('a'+i)) + "/b"
		if err := store.SaveRetained(ctx, topic, &Message{Topic: topic, Payload: []byte("payload")}); err != nil {
			t.Fatal(err)
		}
	}
	stats, _ := store.GetRetainedStats(ctx)
	if stats.TotalMessages != 5 {
		t.Fatalf("expected 5 got %d", stats.TotalMessages)
	}
	if len(stats.TopicStats) != 5 {
		t.Fatalf("expected 5 topic stats got %d", len(stats.TopicStats))
	}
	var sum int64
	for _, v := range stats.TopicStats {
		sum += v.Size
	}
	if sum != stats.TotalSize {
		t.Fatalf("sum %d != total %d", sum, stats.TotalSize)
	}
}

func TestGetRetainedStatsOverwrite(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.SaveRetained(ctx, "t/a", &Message{Topic: "t/a", Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	stats1, _ := store.GetRetainedStats(ctx)
	if err := store.SaveRetained(ctx, "t/a", &Message{Topic: "t/a", Payload: []byte("hello world longer")}); err != nil {
		t.Fatal(err)
	}
	stats2, _ := store.GetRetainedStats(ctx)
	if stats2.TotalMessages != 1 {
		t.Fatalf("overwrite should keep 1 message got %d", stats2.TotalMessages)
	}
	if stats2.TotalSize == stats1.TotalSize {
		t.Fatalf("size should change after overwrite %d vs %d", stats1.TotalSize, stats2.TotalSize)
	}
	expected := int64(len("t/a") + len("hello world longer") + 10)
	if stats2.TotalSize != expected {
		t.Fatalf("expected %d got %d", expected, stats2.TotalSize)
	}
}

func TestGetRetainedStatsAfterDelete(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	_ = store.SaveRetained(ctx, "t/a", &Message{Topic: "t/a", Payload: []byte("x")})
	_ = store.SaveRetained(ctx, "t/b", &Message{Topic: "t/b", Payload: []byte("yy")})
	_ = store.DeleteRetained(ctx, "t/a")
	stats, _ := store.GetRetainedStats(ctx)
	if stats.TotalMessages != 1 {
		t.Fatalf("expected 1 after delete got %d", stats.TotalMessages)
	}
	if _, ok := stats.TopicStats["t/a"]; ok {
		t.Fatalf("t/a should be deleted")
	}
	if _, ok := stats.TopicStats["t/b"]; !ok {
		t.Fatalf("t/b should remain")
	}
}

func TestGetRetainedStatsDeleteNonExist(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	_ = store.SaveRetained(ctx, "t/a", &Message{Topic: "t/a", Payload: []byte("x")})
	_ = store.DeleteRetained(ctx, "nonexistent")
	stats, _ := store.GetRetainedStats(ctx)
	if stats.TotalMessages != 1 {
		t.Fatalf("expected 1 got %d", stats.TotalMessages)
	}
}

func TestGetRetainedStatsSizeConsistency(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	topics := []string{"a", "a/b", "a/b/c"}
	payloads := [][]byte{[]byte(""), []byte("x"), []byte("hello world")}
	for i, topic := range topics {
		_ = store.SaveRetained(ctx, topic, &Message{Topic: topic, Payload: payloads[i]})
	}
	stats, _ := store.GetRetainedStats(ctx)
	if stats.TotalMessages != 3 {
		t.Fatalf("expected 3 got %d", stats.TotalMessages)
	}
	for topic, ts := range stats.TopicStats {
		found := false
		for _, tp := range topics {
			if tp == topic {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("unexpected topic %s", topic)
		}
		if ts.Count != 1 {
			t.Fatalf("count should be 1 got %d", ts.Count)
		}
		if ts.Size <= 0 {
			t.Fatalf("size should be positive")
		}
	}
}

func TestRedisStoreGetRetainedStats(t *testing.T) {
	store, err := NewRedisStore("127.0.0.1:6379", "test-retain-stats")
	if err != nil {
		t.Skip("redis not available")
	}
	defer store.Close()
	ctx := context.Background()
	// clean
	_ = store.cli.FlushDB(ctx).Err()
	defer store.cli.FlushDB(ctx).Err()

	if err := store.SaveRetained(ctx, "t/a", &Message{Topic: "t/a", Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveRetained(ctx, "t/b", &Message{Topic: "t/b", Payload: []byte("world")}); err != nil {
		t.Fatal(err)
	}
	stats, err := store.GetRetainedStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalMessages != 2 {
		t.Fatalf("expected 2 got %d", stats.TotalMessages)
	}
	if len(stats.TopicStats) != 2 {
		t.Fatalf("expected 2 topics got %d", len(stats.TopicStats))
	}
	// overwrite
	_ = store.SaveRetained(ctx, "t/a", &Message{Topic: "t/a", Payload: []byte("longer payload")})
	stats2, _ := store.GetRetainedStats(ctx)
	if stats2.TotalMessages != 2 {
		t.Fatalf("overwrite should keep 2 got %d", stats2.TotalMessages)
	}
	if stats2.TotalSize == stats.TotalSize {
		t.Fatalf("size should change after overwrite")
	}
	// delete
	_ = store.DeleteRetained(ctx, "t/a")
	stats3, _ := store.GetRetainedStats(ctx)
	if stats3.TotalMessages != 1 {
		t.Fatalf("expected 1 after delete got %d", stats3.TotalMessages)
	}
}
