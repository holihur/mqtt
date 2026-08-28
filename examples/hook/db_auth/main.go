package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"mqtt/internal/broker"
	"mqtt/internal/hook"
	"mqtt/internal/persistence"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/crypto/bcrypt"
)

func mustHash(pw string) string {
	h, _ := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(h)
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// In-memory SQLite for demo; replace with postgres/mysql DSN in production:
	//  postgres:  sql.Open("postgres", "postgres://user:pass@localhost/mqtt?sslmode=disable")
	//  mysql:     sql.Open("mysql", "user:pass@tcp(localhost:3306)/mqtt")
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		slog.Error("open db failed", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	schema := `
	CREATE TABLE users (username VARCHAR(256) PRIMARY KEY, password_hash VARCHAR(255), status VARCHAR(20) DEFAULT 'active');
	CREATE TABLE acl (username VARCHAR(256), topic_pattern VARCHAR(512));
	`
	if _, err := db.Exec(schema); err != nil {
		slog.Error("create schema failed", "err", err)
		os.Exit(1)
	}
	aliceHash := mustHash("s3cr3t")
	if _, err := db.Exec(`INSERT INTO users (username, password_hash, status) VALUES (?, ?, ?)`, "alice", aliceHash, "active"); err != nil {
		slog.Error("insert user failed", "err", err)
		os.Exit(1)
	}
	if _, err := db.Exec(`INSERT INTO users (username, password_hash, status) VALUES (?, ?, ?)`, "bob", mustHash("123456"), "disabled"); err != nil {
		slog.Error("insert bob failed", "err", err)
		os.Exit(1)
	}
	if _, err := db.Exec(`INSERT INTO acl (username, topic_pattern) VALUES (?, ?), (?, ?)`, "alice", "tenant/t42/#", "alice", "sensor/+/temp"); err != nil {
		slog.Error("insert acl failed", "err", err)
		os.Exit(1)
	}

	dbHook, err := hook.NewDBAuthHook(db, hook.DBAuthConfig{
		UsersQuery: "SELECT password_hash, status FROM users WHERE username = ?",
		ACLQuery:   "SELECT topic_pattern FROM acl WHERE username = ?",
		Hasher:     hook.BcryptHasher{},
	})
	if err != nil {
		slog.Error("new db hook failed", "err", err)
		os.Exit(1)
	}

	store := persistence.NewMemoryStore()
	b := broker.New(broker.Config{TCPAddr: "127.0.0.1:0", AllowAnonymous: true}, store, nil)
	b.RegisterHook(dbHook)
	slog.Info("broker hooks", "ids", hookIDs(b))

	m := b.Hooks()
	test(m, "t42-dev", "alice", "s3cr3t", "tenant/t42/data", true)
	test(m, "t42-dev", "alice", "wrong", "tenant/t42/data", false)
	test(m, "t42-dev", "bob", "123456", "tenant/t42/data", false)
	test(m, "t42-dev", "alice", "s3cr3t", "tenant/t99/data", false)
	test(m, "t42-dev", "alice", "s3cr3t", "sensor/a/temp", true)
	test(m, "t42-dev", "alice", "s3cr3t", "sensor/a/humidity", false)
	test(m, "unknown", "unknown", "nopass", "any/topic", false)
	fmt.Println("db_auth example done")
}

func test(m *hook.Manager, clientID, username, password, topic string, shouldPass bool) {
	errAuth := m.ExecAuth(clientID, username, []byte(password))
	errPub := m.ExecPublish(clientID, topic, []byte("hello"), 0, false)
	ok := errAuth == nil && errPub == nil
	status := "PASS"
	if ok != shouldPass {
		status = "FAIL"
	}
	slog.Info("test", "client", clientID, "user", username, "topic", topic, "authErr", errAuth, "pubErr", errPub, "expectedPass", shouldPass, "result", status)
}

func hookIDs(b *broker.Broker) []string {
	var ids []string
	for _, h := range b.Hooks().Hooks() {
		ids = append(ids, h.ID())
	}
	return ids
}
