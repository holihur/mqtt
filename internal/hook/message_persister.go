package hook

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Message persistence for SQL query (issue #5).
//
// The broker only persists retain messages, session metadata and offline
// queues; regular PUBLISHes live in memory and are lost on restart. This hook
// captures every PUBLISH that passes the broker's pre-routing checks and writes
// it to a SQL database (PostgreSQL / MySQL / SQLite / ...) in background
// batches, giving an audit trail that can be queried with SQL or OctoSQL.
//
// Design rules:
//   - OnPublish NEVER denies or blocks message routing. It only enqueues a
//     private copy of the payload (the broker owns the original slice — see the
//     "must not retain slice" contract on Manager.ExecPublish) and returns nil.
//   - Writes happen in a single worker goroutine. A full queue either drops the
//     message immediately (DropPolicy "drop", default) or waits up to
//     BlockTimeout ("block") before dropping.
//   - The hook knows nothing about MQTT v5 MessageExpiryInterval (OnPublish does
//     not carry it). Retention/TTL of historical rows is a SQL-side concern.

// MessagePersisterConfig configures MessagePersisterHook.
type MessagePersisterConfig struct {
	// TableName is used to build the default insert query when InsertQuery is
	// empty. Default "mqtt_messages".
	TableName string
	// InsertQuery overrides the default insert statement. Placeholders must
	// match the driver dialect (? for SQLite/MySQL, $1..$n for PostgreSQL).
	// Column order: client_id, topic, payload, qos, retain, node_id, created_at.
	InsertQuery string
	// BatchSize is the number of messages accumulated before a batch insert.
	// Default 1000.
	BatchSize int
	// FlushInterval is the maximum time a partial batch waits before being
	// flushed. Default 5s.
	FlushInterval time.Duration
	// QueueCapacity is the size of the in-memory queue in front of the DB
	// worker. Default 10000.
	QueueCapacity int
	// DropPolicy controls behavior when the queue is full: "drop" (default)
	// drops the message immediately without blocking the publish path; "block"
	// waits up to BlockTimeout then drops.
	DropPolicy string
	// BlockTimeout is used when DropPolicy is "block". Default 100ms.
	BlockTimeout time.Duration
	// BatchTimeout bounds each batch transaction. Default 30s.
	BatchTimeout time.Duration
	// NodeID is recorded in the node_id column (may be empty).
	NodeID string
	// TopicFilters, when non-empty, restricts persistence to topics matching
	// any filter (wildcards supported). Empty means persist everything.
	TopicFilters []string
	// SkipRetain skips retain messages (the broker already stores the latest
	// retain per topic). Default false.
	SkipRetain bool
}

// Defaults for MessagePersisterConfig.
const (
	DefaultPersisterTable     = "mqtt_messages"
	DefaultPersisterBatchSize = 1000
	DefaultPersisterQueueCap  = 10000
	DefaultPersisterFlushInt  = 5 * time.Second
	DefaultPersisterBlockTO   = 100 * time.Millisecond
	DefaultPersisterBatchTO   = 30 * time.Second

	// DropPolicyDrop drops the message as soon as the queue is full.
	DropPolicyDrop = "drop"
	// DropPolicyBlock waits up to BlockTimeout for queue space before dropping.
	DropPolicyBlock = "block"
)

// persistedMessage is a single message queued for the DB worker.
// payload is a private copy: the hook must not retain the slice owned by the
// broker's publish path.
type persistedMessage struct {
	clientID  string
	topic     string
	payload   []byte
	qos       byte
	retain    bool
	createdAt int64 // unix millis
}

// PersisterStats exposes counters for observability (Prometheus/日志 均可拉取)。
type PersisterStats struct {
	Enqueued     int64
	Flushed      int64
	Dropped      int64
	InsertErrors int64
}

// MessagePersisterHook captures PUBLISH events and writes them to a SQL
// database in background batches. It never denies or blocks message routing.
//
//	h, err := hook.NewMessagePersisterHook(db, hook.MessagePersisterConfig{})
//	if err != nil { ... }
//	b.RegisterHook(h)
//	defer h.Close()
type MessagePersisterHook struct {
	BaseHook
	db            *sql.DB
	insertQuery   string
	batchSize     int
	flushInterval time.Duration
	dropPolicy    string
	blockTimeout  time.Duration
	batchTimeout  time.Duration
	nodeID        string
	topicFilters  []string
	skipRetain    bool

	queue  chan *persistedMessage
	done   chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
	closed atomic.Bool

	enqueued     atomic.Int64
	flushed      atomic.Int64
	dropped      atomic.Int64
	insertErrors atomic.Int64
}

// NewMessagePersisterHook validates config, applies defaults and starts the
// background worker. The caller owns db and must call Close on shutdown.
func NewMessagePersisterHook(db *sql.DB, cfg MessagePersisterConfig) (*MessagePersisterHook, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	table := cfg.TableName
	if table == "" {
		table = DefaultPersisterTable
	}
	insertQuery := cfg.InsertQuery
	if insertQuery == "" {
		insertQuery = DefaultMessageInsertQuery(table)
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultPersisterBatchSize
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = DefaultPersisterFlushInt
	}
	queueCap := cfg.QueueCapacity
	if queueCap <= 0 {
		queueCap = DefaultPersisterQueueCap
	}
	dropPolicy := cfg.DropPolicy
	if dropPolicy != DropPolicyBlock {
		dropPolicy = DropPolicyDrop
	}
	blockTimeout := cfg.BlockTimeout
	if blockTimeout <= 0 {
		blockTimeout = DefaultPersisterBlockTO
	}
	batchTimeout := cfg.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = DefaultPersisterBatchTO
	}
	if err := db.Ping(); err != nil {
		slog.Warn("msg persister db ping failed", "err", err)
	}
	h := &MessagePersisterHook{
		db:            db,
		insertQuery:   insertQuery,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		dropPolicy:    dropPolicy,
		blockTimeout:  blockTimeout,
		batchTimeout:  batchTimeout,
		nodeID:        cfg.NodeID,
		topicFilters:  cfg.TopicFilters,
		skipRetain:    cfg.SkipRetain,
		queue:         make(chan *persistedMessage, queueCap),
		done:          make(chan struct{}),
	}
	h.wg.Add(1)
	go h.worker()
	slog.Info("msg persister started", "table", table, "batchSize", batchSize, "flushInterval", flushInterval, "queueCapacity", queueCap, "dropPolicy", dropPolicy)
	return h, nil
}

// DefaultMessageInsertQuery returns the ?-placeholder INSERT used by SQLite and
// MySQL. PostgreSQL callers pass a $1..$n variant with the same column order
// (client_id, topic, payload, qos, retain, node_id, created_at).
func DefaultMessageInsertQuery(table string) string {
	return "INSERT INTO " + table + " (client_id, topic, payload, qos, retain, node_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
}

func (h *MessagePersisterHook) ID() string { return "message-persister" }

// OnPublish enqueues a private copy of the message for background persistence.
// It always returns nil: persistence must never deny or delay routing.
func (h *MessagePersisterHook) OnPublish(clientID, topic string, payload []byte, qos byte, retain bool) error {
	if h.closed.Load() {
		return nil
	}
	if h.skipRetain && retain {
		return nil
	}
	if len(h.topicFilters) > 0 && !matchesAnyFilter(topic, h.topicFilters) {
		return nil
	}
	msg := &persistedMessage{
		clientID:  clientID,
		topic:     topic,
		payload:   append([]byte(nil), payload...), // private copy
		qos:       qos,
		retain:    retain,
		createdAt: time.Now().UnixMilli(),
	}
	select {
	case h.queue <- msg:
		h.enqueued.Add(1)
		return nil
	default:
	}
	if h.dropPolicy == DropPolicyBlock {
		select {
		case h.queue <- msg:
			h.enqueued.Add(1)
			return nil
		case <-time.After(h.blockTimeout):
		}
	}
	h.dropped.Add(1)
	slog.Warn("msg persister queue full, dropping message", "client", clientID, "topic", topic, "policy", h.dropPolicy)
	return nil
}

func matchesAnyFilter(topic string, filters []string) bool {
	for _, f := range filters {
		if Match(f, topic) {
			return true
		}
	}
	return false
}

// worker batches messages and flushes them to the DB on size or time
// threshold. On Close it drains the queue before exiting.
func (h *MessagePersisterHook) worker() {
	defer h.wg.Done()
	ticker := time.NewTicker(h.flushInterval)
	defer ticker.Stop()
	var batch []*persistedMessage
	flushBatch := func() {
		if len(batch) > 0 {
			h.flush(batch)
			batch = nil
		}
	}
	for {
		select {
		case msg, ok := <-h.queue:
			if !ok {
				return
			}
			batch = append(batch, msg)
			if len(batch) >= h.batchSize {
				h.flush(batch)
				batch = nil
			}
		case <-ticker.C:
			flushBatch()
		case <-h.done:
			// Drain whatever is queued, then exit.
			for {
				select {
				case msg, ok := <-h.queue:
					if !ok {
						return
					}
					batch = append(batch, msg)
					if len(batch) >= h.batchSize {
						h.flush(batch)
						batch = nil
					}
				default:
					flushBatch()
					return
				}
			}
		}
	}
}

// flush writes one batch in a single transaction. A failed batch is dropped
// (at-most-once delivery) and counted in InsertErrors.
func (h *MessagePersisterHook) flush(batch []*persistedMessage) {
	if len(batch) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.batchTimeout)
	defer cancel()
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		h.insertErrors.Add(int64(len(batch)))
		slog.Warn("msg persister begin tx failed", "err", err, "batch", len(batch))
		return
	}
	stmt, err := tx.PrepareContext(ctx, h.insertQuery)
	if err != nil {
		_ = tx.Rollback()
		h.insertErrors.Add(int64(len(batch)))
		slog.Warn("msg persister prepare failed", "err", err, "batch", len(batch))
		return
	}
	for _, m := range batch {
		if _, err := stmt.ExecContext(ctx, m.clientID, m.topic, m.payload, m.qos, m.retain, h.nodeID, m.createdAt); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			h.insertErrors.Add(int64(len(batch)))
			slog.Warn("msg persister insert failed", "err", err, "batch", len(batch))
			return
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		h.insertErrors.Add(int64(len(batch)))
		slog.Warn("msg persister commit failed", "err", err, "batch", len(batch))
		return
	}
	h.flushed.Add(int64(len(batch)))
}

// Close stops the worker, flushes any queued messages and returns. It is safe
// to call multiple times.
func (h *MessagePersisterHook) Close() error {
	h.once.Do(func() {
		h.closed.Store(true)
		close(h.done)
		h.wg.Wait()
	})
	return nil
}

// Stats returns cumulative counters.
func (h *MessagePersisterHook) Stats() PersisterStats {
	return PersisterStats{
		Enqueued:     h.enqueued.Load(),
		Flushed:      h.flushed.Load(),
		Dropped:      h.dropped.Load(),
		InsertErrors: h.insertErrors.Load(),
	}
}
