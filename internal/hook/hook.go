package hook

import (
	"errors"
	"sync"

	"mqtt/internal/codec"
	topict "mqtt/internal/topic"
)

// ErrDenied is returned by a Hook to deny the operation.
// Broker maps it to MQTT reason codes (0x87 Not authorized, 0x80 failure, etc.).
var ErrDenied = errors.New("hook denied")

// Hook is the extension point for topic-based custom logic.
// Implement only the methods you care about; embed BaseHook for defaults.
type Hook interface {
	ID() string
	OnAuth(clientID, username string, password []byte) error
	OnConnect(clientID string) error
	OnPublish(clientID, topic string, payload []byte, qos byte, retain bool) error
	OnSubscribe(clientID, filter string, qos byte) error
	OnUnsubscribe(clientID, filter string) error
	OnDisconnect(clientID string, clean bool)
	OnPacket(dir, clientID string, pkt *codec.Packet, hex string)
}

// PacketHexSink 由真正需要消费原始包 hex dump 的 hook 实现（如审计/调试）。
// broker 只在存在这类 hook（或 debug 日志开启）时才计算 hex，避免每次收/发
// 包都做一次额外 Encode + hex 格式化。
type PacketHexSink interface {
	PacketHexNeeded() bool
}

// BaseHook provides no-op defaults so implementors can override selectively.
type BaseHook struct{}

func (BaseHook) ID() string                                         { return "base" }
func (BaseHook) OnAuth(string, string, []byte) error                { return nil }
func (BaseHook) OnConnect(string) error                             { return nil }
func (BaseHook) OnPublish(string, string, []byte, byte, bool) error { return nil }
func (BaseHook) OnSubscribe(string, string, byte) error             { return nil }
func (BaseHook) OnUnsubscribe(string, string) error                 { return nil }
func (BaseHook) OnDisconnect(string, bool)                          {}
func (BaseHook) OnPacket(string, string, *codec.Packet, string)     {}

// Manager holds ordered hooks and dispatches calls.
// It is safe for concurrent Register and Exec.
type Manager struct {
	mu    sync.RWMutex
	hooks []Hook
}

// NewManager creates an empty Manager.
func NewManager() *Manager { return &Manager{} }

// Register appends a Hook. If a hook with same ID exists it is replaced.
func (m *Manager) Register(h Hook) {
	if h == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range m.hooks {
		if e.ID() == h.ID() {
			m.hooks[i] = h
			return
		}
	}
	m.hooks = append(m.hooks, h)
}

// Hooks returns a snapshot of registered hooks.
func (m *Manager) Hooks() []Hook {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Hook, len(m.hooks))
	copy(out, m.hooks)
	return out
}

// Len returns number of hooks.
func (m *Manager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.hooks)
}

// PacketHexNeeded reports whether any registered hook consumes the raw packet
// hex dump.  If false, the broker can skip hex encoding entirely on the hot
// path (e.g. the always-present auth adapter does not need it).
func (m *Manager) PacketHexNeeded() bool {
	if m == nil {
		return false
	}
	for _, h := range m.Hooks() {
		if sink, ok := h.(PacketHexSink); ok && sink.PacketHexNeeded() {
			return true
		}
	}
	return false
}

// ExecAuth calls all hooks for authentication. First error denies auth.
func (m *Manager) ExecAuth(clientID, username string, password []byte) error {
	if m == nil {
		return nil
	}
	for _, h := range m.Hooks() {
		if err := h.OnAuth(clientID, username, password); err != nil {
			return err
		}
	}
	return nil
}

// ExecConnect calls all hooks, returns first non-nil error.
func (m *Manager) ExecConnect(clientID string) error {
	if m == nil {
		return nil
	}
	for _, h := range m.Hooks() {
		if err := h.OnConnect(clientID); err != nil {
			return err
		}
	}
	return nil
}

// ExecPublish calls all hooks sequentially. Payload is not copied; hooks must not retain slice.
func (m *Manager) ExecPublish(clientID, topic string, payload []byte, qos byte, retain bool) error {
	if m == nil {
		return nil
	}
	for _, h := range m.Hooks() {
		if err := h.OnPublish(clientID, topic, payload, qos, retain); err != nil {
			return err
		}
	}
	return nil
}

// ExecSubscribe calls all hooks.
func (m *Manager) ExecSubscribe(clientID, filter string, qos byte) error {
	if m == nil {
		return nil
	}
	for _, h := range m.Hooks() {
		if err := h.OnSubscribe(clientID, filter, qos); err != nil {
			return err
		}
	}
	return nil
}

// ExecUnsubscribe calls all hooks.
func (m *Manager) ExecUnsubscribe(clientID, filter string) error {
	if m == nil {
		return nil
	}
	for _, h := range m.Hooks() {
		if err := h.OnUnsubscribe(clientID, filter); err != nil {
			return err
		}
	}
	return nil
}

// ExecDisconnect notifies all hooks (errors ignored).
func (m *Manager) ExecDisconnect(clientID string, clean bool) {
	if m == nil {
		return
	}
	for _, h := range m.Hooks() {
		h.OnDisconnect(clientID, clean)
	}
}

// ExecPacket dispatches hex dump to all hooks. hex is pre-encoded packet hex (may be truncated).
func (m *Manager) ExecPacket(dir, clientID string, pkt *codec.Packet, hex string) {
	if m == nil {
		return
	}
	for _, h := range m.Hooks() {
		h.OnPacket(dir, clientID, pkt, hex)
	}
}

// Match reports whether topic matches filter using the same Trie semantics as broker.
// Useful inside hook implementations to avoid reimplementing wildcard logic.
func Match(filter, tp string) bool {
	tr := topict.NewTrie()
	tr.Add(filter, "hook-match", 0, false)
	return len(tr.Match(tp)) > 0
}
