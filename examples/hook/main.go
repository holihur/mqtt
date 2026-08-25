package main

import (
	"log/slog"
	"os"

	"mqtt/internal/broker"
	"mqtt/internal/hook"
	"mqtt/internal/persistence"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	store := persistence.NewMemoryStore()
	b := broker.New(broker.Config{
		TCPAddr:        "127.0.0.1:1884",
		AllowAnonymous: true,
	}, store, nil)

	b.RegisterHook(hook.AuthHook{})
	b.RegisterHook(hook.TenantIsolationHook{})
	b.RegisterHook(hook.EncTopicValidationHook{})
	b.RegisterHook(hook.TopicTagHook{})
	b.RegisterHook(hook.HexDumpHook{})

	slog.Info("hooks registered", "count", b.Hooks().Len())
	for _, h := range b.Hooks().Hooks() {
		slog.Info("hook loaded", "id", h.ID())
	}

	_ = b

	// Manual demo of hook logic without starting broker
	demoHooks(b.Hooks())
}

func demoHooks(m *hook.Manager) {
	cases := []struct {
		clientID string
		topic    string
		payload  []byte
	}{
		{"t42-abc123", "tenant/t42/enc/data", []byte("short")},
		{"t42-abc123", "tenant/t42/enc/data", []byte("1234567890123456-ok")},
		{"t42-abc123", "tenant/t99/enc/data", []byte("1234567890123456-ok")},
		{"internal-dev-1", "internal/metrics", []byte("hello")},
	}
	for i, c := range cases {
		err := m.ExecPublish(c.clientID, c.topic, c.payload, 1, false)
		slog.Info("demo publish", "i", i, "client", c.clientID, "topic", c.topic, "err", err)
	}
	_ = m.ExecAuth("t42-abc123", "blocked", []byte("any"))
	_ = m.ExecAuth("t42-abc123", "alice", []byte("ok"))
	slog.Info("demo auth blocked", "err", m.ExecAuth("t42-abc123", "blocked", nil))
	slog.Info("demo auth ok", "err", m.ExecAuth("t42-abc123", "alice", nil))
	_ = m.ExecConnect("t42-newclient")
	m.ExecDisconnect("t42-newclient", true)
	_ = m.ExecUnsubscribe("t42-abc123", "tenant/t42/enc/data")
}
