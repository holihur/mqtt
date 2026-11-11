package hook

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Message persistence for MongoDB.
//
// The hook mirrors MessagePersisterHook: every PUBLISH accepted by the
// broker's pre-routing checks is copied into an in-memory queue and written to
// MongoDB by a single background worker in batches. Persistence never denies or
// blocks message routing.
//
// Rows/documents use the same field names as the SQL persister so the same
// dashboards and ad-hoc queries work across backends:
//
//	client_id, topic, payload, qos, retain, node_id, created_at
//
// created_at is unix milliseconds.

// MongoMessagePersisterConfig configures MongoMessagePersisterHook.
type MongoMessagePersisterConfig struct {
	// Database is the MongoDB database. Default "mqtt".
	Database string
	// Collection is the target collection. Default "mqtt_messages".
	Collection string
	// BatchSize is the number of messages accumulated before a batch insert.
	// Default 1000.
	BatchSize int
	// FlushInterval is the maximum time a partial batch waits before being
	// flushed. Default 5s.
	FlushInterval time.Duration
	// QueueCapacity is the size of the in-memory queue in front of the writer.
	// Default 10000.
	QueueCapacity int
	// DropPolicy controls behavior when the queue is full: "drop" (default)
	// drops the message immediately without blocking the publish path; "block"
	// waits up to BlockTimeout then drops.
	DropPolicy string
	// BlockTimeout is used when DropPolicy is "block". Default 100ms.
	BlockTimeout time.Duration
	// BatchTimeout bounds each batch insert. Default 30s.
	BatchTimeout time.Duration
	// NodeID is recorded in the node_id field (may be empty).
	NodeID string
	// TopicFilters, when non-empty, restricts persistence to topics matching
	// any filter (wildcards supported). Empty means persist everything.
	TopicFilters []string
	// SkipRetain skips retain messages (the broker already stores the latest
	// retain per topic). Default false.
	SkipRetain bool
	// DisableIndexes skips best-effort creation of the topic/client/time
	// indexes on startup. Index creation failures are logged but never abort
	// startup.
	DisableIndexes bool
}

// Defaults for MongoMessagePersisterConfig.
const (
	DefaultMongoDatabase   = "mqtt"
	DefaultMongoCollection = DefaultPersisterTable
)

// mongoMessageDoc is the BSON representation of a persisted message.
type mongoMessageDoc struct {
	ClientID  string `bson:"client_id"`
	Topic     string `bson:"topic"`
	Payload   []byte `bson:"payload"`
	QoS       byte   `bson:"qos"`
	Retain    bool   `bson:"retain"`
	NodeID    string `bson:"node_id"`
	CreatedAt int64  `bson:"created_at"`
}

// mongoInsertFunc is the test seam used by the worker.
type mongoInsertFunc func(ctx context.Context, batch []*persistedMessage) error

// MongoMessagePersisterHook captures PUBLISH events and writes them to
// MongoDB in background batches. It never denies or blocks message routing.
//
//	client, _ := mongo.Connect(ctx, options.Client().ApplyURI(uri))
//	h, err := hook.NewMongoMessagePersisterHook(client, hook.MongoMessagePersisterConfig{})
//	if err != nil { ... }
//	b.RegisterHook(h)
//	defer h.Close()
type MongoMessagePersisterHook struct {
	BaseHook

	insertFn      mongoInsertFunc
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

// NewMongoMessagePersisterHook validates config, ensures indexes (best effort)
// and starts the background worker. The caller owns client and must call Close
// on shutdown; disconnecting the client remains the caller's responsibility.
func NewMongoMessagePersisterHook(client *mongo.Client, cfg MongoMessagePersisterConfig) (*MongoMessagePersisterHook, error) {
	if client == nil {
		return nil, errors.New("mongo client is nil")
	}
	database := cfg.Database
	if database == "" {
		database = DefaultMongoDatabase
	}
	collection := cfg.Collection
	if collection == "" {
		collection = DefaultMongoCollection
	}
	coll := client.Database(database).Collection(collection)
	nodeID := cfg.NodeID

	h := newMongoMessagePersisterHook(func(ctx context.Context, batch []*persistedMessage) error {
		docs := make([]any, 0, len(batch))
		for _, m := range batch {
			docs = append(docs, mongoMessageDoc{
				ClientID:  m.clientID,
				Topic:     m.topic,
				Payload:   m.payload,
				QoS:       m.qos,
				Retain:    m.retain,
				NodeID:    nodeID,
				CreatedAt: m.createdAt,
			})
		}
		if len(docs) == 0 {
			return nil
		}
		_, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
		return err
	}, cfg)

	if !cfg.DisableIndexes {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ensureMongoIndexes(ctx, coll); err != nil {
			slog.Warn("mongo message persister index creation failed", "database", database, "collection", collection, "err", err)
		}
	}
	slog.Info("mongo message persister started",
		"database", database,
		"collection", collection,
		"batchSize", h.batchSize,
		"flushInterval", h.flushInterval,
		"queueCapacity", cap(h.queue),
		"dropPolicy", h.dropPolicy,
	)
	return h, nil
}

// newMongoMessagePersisterHook applies defaults and starts the worker. It is
// separated from NewMongoMessagePersisterHook so tests can inject a fake
// insert function without a live MongoDB.
func newMongoMessagePersisterHook(insertFn mongoInsertFunc, cfg MongoMessagePersisterConfig) *MongoMessagePersisterHook {
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
	h := &MongoMessagePersisterHook{
		insertFn:      insertFn,
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
	return h
}

// ensureMongoIndexes creates the same lookup indexes used by the SQL schema.
func ensureMongoIndexes(ctx context.Context, coll *mongo.Collection) error {
	_, err := coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "topic", Value: 1}}},
		{Keys: bson.D{{Key: "client_id", Value: 1}, {Key: "created_at", Value: 1}}},
		{Keys: bson.D{{Key: "created_at", Value: 1}}},
	})
	return err
}

func (h *MongoMessagePersisterHook) ID() string { return "mongo-message-persister" }

// OnPublish enqueues a private copy of the message for background persistence.
// It always returns nil: persistence must never deny or delay routing.
func (h *MongoMessagePersisterHook) OnPublish(clientID, topic string, payload []byte, qos byte, retain bool) error {
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
	slog.Warn("mongo message persister queue full, dropping message", "client", clientID, "topic", topic, "policy", h.dropPolicy)
	return nil
}

// worker batches messages and flushes them to MongoDB on size or time
// threshold. On Close it drains the queue before exiting.
func (h *MongoMessagePersisterHook) worker() {
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

// flush writes one batch. A failed batch is dropped (at-most-once delivery)
// and counted in InsertErrors.
func (h *MongoMessagePersisterHook) flush(batch []*persistedMessage) {
	if len(batch) == 0 {
		return
	}
	if h.insertFn == nil {
		h.insertErrors.Add(int64(len(batch)))
		slog.Warn("mongo message persister has no insert function", "batch", len(batch))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.batchTimeout)
	defer cancel()
	if err := h.insertFn(ctx, batch); err != nil {
		h.insertErrors.Add(int64(len(batch)))
		slog.Warn("mongo message persister insert failed", "err", err, "batch", len(batch))
		return
	}
	h.flushed.Add(int64(len(batch)))
}

// Close stops the worker, flushes any queued messages and returns. It is safe
// to call multiple times.
func (h *MongoMessagePersisterHook) Close() error {
	h.once.Do(func() {
		h.closed.Store(true)
		close(h.done)
		h.wg.Wait()
	})
	return nil
}

// Stats returns cumulative counters.
func (h *MongoMessagePersisterHook) Stats() PersisterStats {
	return PersisterStats{
		Enqueued:     h.enqueued.Load(),
		Flushed:      h.flushed.Load(),
		Dropped:      h.dropped.Load(),
		InsertErrors: h.insertErrors.Load(),
	}
}
