package hook

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func TestNewMongoMessagePersisterHookNilClient(t *testing.T) {
	if _, err := NewMongoMessagePersisterHook(nil, MongoMessagePersisterConfig{}); err == nil {
		t.Fatal("should fail with nil client")
	}
}

func TestMongoMessagePersisterDefaultsAndBatch(t *testing.T) {
	var got []*persistedMessage
	h := newMongoMessagePersisterHook(func(_ context.Context, batch []*persistedMessage) error {
		got = append(got, batch...)
		return nil
	}, MongoMessagePersisterConfig{FlushInterval: time.Hour, BatchSize: 2})
	if h.batchSize != 2 || h.flushInterval != time.Hour || cap(h.queue) != DefaultPersisterQueueCap {
		t.Fatalf("config not applied: %+v", h)
	}
	payload := []byte("AAA")
	if err := h.OnPublish("c1", "sensor/a/temp", payload, 1, false); err != nil {
		t.Fatalf("OnPublish should not error: %v", err)
	}
	payload[0] = 'X' // broker reuses the slice; the queued copy must survive
	_ = h.OnPublish("c1", "sensor/a/temp", []byte("BBB"), 0, false)
	_ = h.Close()
	if len(got) != 2 {
		t.Fatalf("inserted %d docs, want 2", len(got))
	}
	if string(got[0].payload) != "AAA" {
		t.Fatalf("payload was not copied: %q", got[0].payload)
	}
	if st := h.Stats(); st.Enqueued != 2 || st.Flushed != 2 || st.Dropped != 0 || st.InsertErrors != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestMongoMessagePersisterFilterAndSkipRetain(t *testing.T) {
	var got []*persistedMessage
	h := newMongoMessagePersisterHook(func(_ context.Context, batch []*persistedMessage) error {
		got = append(got, batch...)
		return nil
	}, MongoMessagePersisterConfig{
		FlushInterval: time.Hour,
		TopicFilters:  []string{"sensor/+/temp"},
		SkipRetain:    true,
	})
	_ = h.OnPublish("c1", "sensor/a/temp", []byte("1"), 0, false)     // match
	_ = h.OnPublish("c1", "sensor/a/temp", []byte("2"), 0, true)      // retain skipped
	_ = h.OnPublish("c1", "sensor/a/humidity", []byte("3"), 0, false) // filter miss
	_ = h.Close()
	if len(got) != 1 || string(got[0].payload) != "1" {
		t.Fatalf("unexpected docs: %+v", got)
	}
	if st := h.Stats(); st.Enqueued != 1 || st.Flushed != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestMongoMessagePersisterInsertError(t *testing.T) {
	h := newMongoMessagePersisterHook(func(_ context.Context, _ []*persistedMessage) error {
		return errors.New("mongo down")
	}, MongoMessagePersisterConfig{FlushInterval: time.Hour})
	_ = h.OnPublish("c1", "a", []byte("1"), 0, false)
	_ = h.OnPublish("c1", "b", []byte("2"), 0, false)
	_ = h.Close()
	if st := h.Stats(); st.InsertErrors != 2 || st.Flushed != 0 {
		t.Fatalf("stats = %+v, want insertErrors=2", st)
	}
}

func TestMongoMessagePersisterQueueFull(t *testing.T) {
	h := &MongoMessagePersisterHook{
		queue:        make(chan *persistedMessage, 1),
		dropPolicy:   DropPolicyDrop,
		blockTimeout: DefaultPersisterBlockTO,
	}
	h.queue <- &persistedMessage{}
	if err := h.OnPublish("c1", "a/b", []byte("x"), 0, false); err != nil {
		t.Fatalf("full queue must not deny publish: %v", err)
	}
	if h.Stats().Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", h.Stats().Dropped)
	}
}

func TestMongoMessagePersisterClosedNoop(t *testing.T) {
	h := &MongoMessagePersisterHook{}
	h.closed.Store(true)
	if err := h.OnPublish("c1", "a/b", []byte("x"), 0, false); err != nil {
		t.Fatalf("closed hook must return nil: %v", err)
	}
	if h.Stats().Enqueued != 0 {
		t.Fatal("closed hook must not enqueue")
	}
}

func TestMongoMessageDocBSONShape(t *testing.T) {
	doc := mongoMessageDoc{
		ClientID:  "c1",
		Topic:     "a/b",
		Payload:   []byte("x"),
		QoS:       1,
		Retain:    true,
		NodeID:    "n1",
		CreatedAt: 123,
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var decoded bson.M
	if err := bson.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"client_id", "topic", "payload", "qos", "retain", "node_id", "created_at"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("BSON missing %q: %v", key, decoded)
		}
	}
}
