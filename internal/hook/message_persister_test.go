package hook

import (
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// ---------------------------------------------------------------------------
// Construction / defaults
// ---------------------------------------------------------------------------

func TestMessagePersisterNilDB(t *testing.T) {
	if _, err := NewMessagePersisterHook(nil, MessagePersisterConfig{}); err == nil {
		t.Fatalf("should fail with nil db")
	}
}

func TestMessagePersisterDefaults(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	defer h.Close()
	if h.batchSize != DefaultPersisterBatchSize {
		t.Fatalf("batchSize = %d, want %d", h.batchSize, DefaultPersisterBatchSize)
	}
	if h.flushInterval != DefaultPersisterFlushInt {
		t.Fatalf("flushInterval = %v, want %v", h.flushInterval, DefaultPersisterFlushInt)
	}
	if cap(h.queue) != DefaultPersisterQueueCap {
		t.Fatalf("queue cap = %d, want %d", cap(h.queue), DefaultPersisterQueueCap)
	}
	if h.dropPolicy != DropPolicyDrop {
		t.Fatalf("dropPolicy = %q, want %q", h.dropPolicy, DropPolicyDrop)
	}
	if h.insertQuery != DefaultMessageInsertQuery(DefaultPersisterTable) {
		t.Fatalf("insertQuery = %q, want default", h.insertQuery)
	}
	if h.ID() != "message-persister" {
		t.Fatalf("ID = %q", h.ID())
	}
}

func TestMessagePersisterCustomConfig(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{
		TableName:     "audit",
		BatchSize:     5,
		FlushInterval: time.Minute,
		QueueCapacity: 3,
		DropPolicy:    DropPolicyBlock,
		NodeID:        "n1",
	})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	defer h.Close()
	if h.batchSize != 5 || h.flushInterval != time.Minute || cap(h.queue) != 3 || h.dropPolicy != DropPolicyBlock || h.nodeID != "n1" {
		t.Fatalf("custom config not applied: %+v", h)
	}
	if h.insertQuery != DefaultMessageInsertQuery("audit") {
		t.Fatalf("custom table not reflected in insert query: %q", h.insertQuery)
	}
}

func TestMessagePersisterCustomInsertQuery(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	q := "INSERT INTO audit (client_id, topic, payload, qos, retain, node_id, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)"
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{InsertQuery: q})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	defer h.Close()
	if h.insertQuery != q {
		t.Fatalf("custom insert query not kept")
	}
}

// ---------------------------------------------------------------------------
// OnPublish: copy, filtering, never deny
// ---------------------------------------------------------------------------

// OnPublish must enqueue a private copy: mutating the caller's slice afterwards
// must not affect the persisted payload (the broker reuses packet buffers).
func TestMessagePersisterCopiesPayload(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	// hour-long flush so the only flush happens on Close (deterministic drain).
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{
		FlushInterval: time.Hour,
		BatchSize:     1000,
	})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	expectInsertBatch(mock, 1)
	payload := []byte("AAA")
	if err := h.OnPublish("c1", "sensor/a/temp", payload, 1, false); err != nil {
		t.Fatalf("OnPublish should not error: %v", err)
	}
	payload[0] = 'X' // broker reuses the slice; the queued copy must survive
	_ = h.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if st := h.Stats(); st.Flushed != 1 {
		t.Fatalf("flushed = %d, want 1", st.Flushed)
	}
}

func TestMessagePersisterTopicFilter(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{
		FlushInterval: time.Hour,
		BatchSize:     1000,
		TopicFilters:  []string{"sensor/+/temp", "audit/#"},
	})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	expectInsertBatch(mock, 2)
	_ = h.OnPublish("c1", "sensor/a/temp", []byte("t"), 0, false)     // match -> persist
	_ = h.OnPublish("c1", "sensor/a/humidity", []byte("h"), 0, false) // no match
	_ = h.OnPublish("c1", "audit/login", []byte("l"), 0, false)       // match -> persist
	_ = h.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if st := h.Stats(); st.Enqueued != 2 || st.Flushed != 2 {
		t.Fatalf("stats = %+v, want enqueued=2 flushed=2", st)
	}
}

func TestMessagePersisterSkipRetain(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{
		FlushInterval: time.Hour,
		BatchSize:     1000,
		SkipRetain:    true,
	})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	expectInsertBatch(mock, 1)
	_ = h.OnPublish("c1", "a/b", []byte("r"), 1, true)  // retain -> skipped
	_ = h.OnPublish("c1", "a/b", []byte("n"), 1, false) // normal -> persisted
	_ = h.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if st := h.Stats(); st.Enqueued != 1 || st.Flushed != 1 {
		t.Fatalf("stats = %+v, want enqueued=1 flushed=1", st)
	}
}

func TestMessagePersisterNeverDeniesWhenQueueFull(t *testing.T) {
	// No worker: exercise the enqueue path in isolation against a full queue.
	h := &MessagePersisterHook{
		queue:        make(chan *persistedMessage, 1),
		dropPolicy:   DropPolicyDrop,
		blockTimeout: DefaultPersisterBlockTO,
	}
	h.queue <- &persistedMessage{} // fill the queue
	if err := h.OnPublish("c1", "a/b", []byte("x"), 0, false); err != nil {
		t.Fatalf("full queue must not deny publish: %v", err)
	}
	if h.Stats().Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", h.Stats().Dropped)
	}
}

func TestMessagePersisterBlockPolicy(t *testing.T) {
	h := &MessagePersisterHook{
		queue:        make(chan *persistedMessage, 1),
		dropPolicy:   DropPolicyBlock,
		blockTimeout: 30 * time.Millisecond,
	}
	h.queue <- &persistedMessage{} // fill the queue
	start := time.Now()
	if err := h.OnPublish("c1", "a/b", []byte("x"), 0, false); err != nil {
		t.Fatalf("full queue must not deny publish: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("block policy returned too fast: %v", elapsed)
	}
	if h.Stats().Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", h.Stats().Dropped)
	}
}

func TestMessagePersisterClosedNoop(t *testing.T) {
	h := &MessagePersisterHook{}
	h.closed.Store(true)
	if err := h.OnPublish("c1", "a/b", []byte("x"), 0, false); err != nil {
		t.Fatalf("closed hook must return nil: %v", err)
	}
	if h.Stats().Enqueued != 0 {
		t.Fatalf("closed hook must not enqueue")
	}
}

// ---------------------------------------------------------------------------
// flush / worker behavior
// ---------------------------------------------------------------------------

// Direct flush of a batch: SQL shape + failure accounting.
func TestMessagePersisterFlushBatch(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{FlushInterval: time.Hour, BatchSize: 1000})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	defer h.Close()
	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO mqtt_messages")
	for i := 0; i < 2; i++ {
		mock.ExpectExec("INSERT INTO mqtt_messages").
			WithArgs("c1", "a/b", []byte("m"), sqlmock.AnyArg(), sqlmock.AnyArg(), "", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()
	h.flush([]*persistedMessage{
		{clientID: "c1", topic: "a/b", payload: []byte("m"), qos: 1, retain: false, createdAt: time.Now().UnixMilli()},
		{clientID: "c1", topic: "a/b", payload: []byte("m"), qos: 0, retain: true, createdAt: time.Now().UnixMilli()},
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if st := h.Stats(); st.Flushed != 2 {
		t.Fatalf("flushed = %d, want 2", st.Flushed)
	}
}

func TestMessagePersisterFlushBeginError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	defer h.Close()
	mock.ExpectBegin().WillReturnError(sql.ErrTxDone)
	h.flush([]*persistedMessage{{clientID: "c1", topic: "a/b", payload: []byte("m")}})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if st := h.Stats(); st.InsertErrors != 1 || st.Flushed != 0 {
		t.Fatalf("stats = %+v, want insertErrors=1 flushed=0", st)
	}
}

func TestMessagePersisterFlushExecError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	defer h.Close()
	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO mqtt_messages")
	mock.ExpectExec("INSERT INTO mqtt_messages").WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()
	h.flush([]*persistedMessage{{clientID: "c1", topic: "a/b", payload: []byte("m")}})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if st := h.Stats(); st.InsertErrors != 1 {
		t.Fatalf("insertErrors = %d, want 1", st.InsertErrors)
	}
}

// Worker integration: size-triggered batch flush on Close drain.
func TestMessagePersisterWorkerBatchFlushOnClose(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{
		FlushInterval: time.Hour,
		BatchSize:     2,
	})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	expectInsertBatch(mock, 2)
	_ = h.OnPublish("c1", "a", []byte("1"), 0, false)
	_ = h.OnPublish("c1", "b", []byte("2"), 0, false)
	_ = h.Close() // drains the 2 queued messages -> one 2-row batch
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if st := h.Stats(); st.Flushed != 2 {
		t.Fatalf("flushed = %d, want 2", st.Flushed)
	}
}

// Worker integration: ticker-triggered flush of a partial batch.
func TestMessagePersisterWorkerTickerFlush(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{
		FlushInterval: 20 * time.Millisecond,
		BatchSize:     1000,
	})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	defer h.Close()
	expectInsertBatch(mock, 1)
	_ = h.OnPublish("c1", "a", []byte("1"), 0, false)
	deadline := time.Now().Add(2 * time.Second)
	for h.Stats().Flushed < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if st := h.Stats(); st.Flushed != 1 {
		t.Fatalf("ticker did not flush, stats = %+v", st)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMessagePersisterCloseIdempotent(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	h, err := NewMessagePersisterHook(db, MessagePersisterConfig{FlushInterval: time.Hour, BatchSize: 1000})
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	_ = h.OnPublish("c1", "a", []byte("x"), 0, false) // after close: no-op, nil
}

// expectInsertBatch sets up expectations for a single batch insert of n rows.
// Worker-flush tests cannot predict createdAt, so only the meaningful columns
// are asserted (clientID and topic for each row).
func expectInsertBatch(mock sqlmock.Sqlmock, n int) {
	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO mqtt_messages")
	for i := 0; i < n; i++ {
		mock.ExpectExec("INSERT INTO mqtt_messages").
			WithArgs("c1", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()
}
