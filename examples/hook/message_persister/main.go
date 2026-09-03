package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	"mqtt/internal/broker"
	"mqtt/internal/hook"
	"mqtt/internal/persistence"

	_ "modernc.org/sqlite"
)

// Message persistence demo (issue #5): every PUBLISH is written to SQL in
// background batches; query the table directly or via OctoSQL.
//
// Production: point the same hook at PostgreSQL, e.g.
//
//	db, _ := sql.Open("postgres", "postgres://user:pass@localhost/mqtt?sslmode=disable")
//	// Postgres insert uses $1..$n placeholders:
//	//   hook.MessagePersisterConfig{InsertQuery: "INSERT INTO mqtt_messages (client_id, topic, payload, qos, retain, node_id, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)"}
//
// SQLite DDL used below:
//
//	CREATE TABLE mqtt_messages (
//	    id INTEGER PRIMARY KEY AUTOINCREMENT,
//	    client_id TEXT,
//	    topic TEXT NOT NULL,
//	    payload BLOB,
//	    qos INTEGER,
//	    retain INTEGER,
//	    node_id TEXT,
//	    created_at INTEGER NOT NULL,   -- unix millis
//	    message_expiry INTEGER DEFAULT 0
//	);
//	CREATE INDEX idx_topic ON mqtt_messages (topic);
//	CREATE INDEX idx_client_time ON mqtt_messages (client_id, created_at);
//	CREATE INDEX idx_created ON mqtt_messages (created_at);
func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// In-memory SQLite for demo; replace with a real DSN in production.
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		slog.Error("open db failed", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	if _, err := db.Exec(`
CREATE TABLE mqtt_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT,
    topic TEXT NOT NULL,
    payload BLOB,
    qos INTEGER,
    retain INTEGER,
    node_id TEXT,
    created_at INTEGER NOT NULL,
    message_expiry INTEGER DEFAULT 0
);
CREATE INDEX idx_topic ON mqtt_messages (topic);
CREATE INDEX idx_created ON mqtt_messages (created_at);
`); err != nil {
		slog.Error("create schema failed", "err", err)
		os.Exit(1)
	}

	ph, err := hook.NewMessagePersisterHook(db, hook.MessagePersisterConfig{
		BatchSize:     2, // flush every 2 messages (or 1s, whichever first)
		FlushInterval: time.Second,
		NodeID:        "demo-1",
	})
	if err != nil {
		slog.Error("new persister hook failed", "err", err)
		os.Exit(1)
	}
	defer ph.Close()

	store := persistence.NewMemoryStore()
	b := broker.New(broker.Config{TCPAddr: "127.0.0.1:0", AllowAnonymous: true}, store, nil)
	b.RegisterHook(ph)

	m := b.Hooks()
	simulate(m, "sensor-a", "sensor/a/temp", "21.5", 1)
	simulate(m, "sensor-a", "sensor/a/humidity", "58", 0)
	simulate(m, "device-x", "tenant/t42/state", "online", 1)

	// Wait for the worker to flush (batch of 2 -> one insert, then ticker -> one insert).
	time.Sleep(1500 * time.Millisecond)
	ph.Close()

	rows, err := db.Query(`SELECT client_id, topic, payload, qos, node_id, created_at FROM mqtt_messages ORDER BY id`)
	if err != nil {
		slog.Error("query failed", "err", err)
		os.Exit(1)
	}
	defer rows.Close()
	fmt.Println("persisted messages:")
	for rows.Next() {
		var clientID, topic, nodeID string
		var payload []byte
		var qos int
		var createdAt int64
		if err := rows.Scan(&clientID, &topic, &payload, &qos, &nodeID, &createdAt); err != nil {
			slog.Error("scan failed", "err", err)
			os.Exit(1)
		}
		fmt.Printf("  %-10s %-20s qos=%d node=%s ts=%d payload=%q\n", clientID, topic, qos, nodeID, createdAt, string(payload))
	}
	fmt.Printf("hook stats: enqueued=%d flushed=%d dropped=%d errors=%d\n",
		ph.Stats().Enqueued, ph.Stats().Flushed, ph.Stats().Dropped, ph.Stats().InsertErrors)
	fmt.Println("message_persister example done")
}

func simulate(m *hook.Manager, clientID, topic, payload string, qos byte) {
	err := m.ExecPublish(clientID, topic, []byte(payload), qos, false)
	slog.Info("publish", "client", clientID, "topic", topic, "qos", qos, "hookErr", err)
}
