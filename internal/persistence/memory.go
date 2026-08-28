package persistence

import (
	"context"
	"fmt"
	"mqtt/internal/session"
	"sync"
)

type MemoryStore struct {
	mu             sync.RWMutex
	sessions       map[string]*session.Session
	retained       map[string]*Message
	offline        map[string][]*Message
	pendingWills   map[string]*PendingWill
	pendingRetries map[string]*PendingRetry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sessions:       make(map[string]*session.Session),
		retained:       make(map[string]*Message),
		offline:        make(map[string][]*Message),
		pendingWills:   make(map[string]*PendingWill),
		pendingRetries: make(map[string]*PendingRetry),
	}
}

func (m *MemoryStore) GetSession(_ context.Context, clientID string) (*session.Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[clientID]
	if !ok {
		return nil, nil
	}
	return s, nil
}
func (m *MemoryStore) SaveSession(_ context.Context, s *session.Session) error {
	m.mu.Lock()
	m.sessions[s.ClientID] = s
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) DeleteSession(_ context.Context, clientID string) error {
	m.mu.Lock()
	delete(m.sessions, clientID)
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) GetRetained(_ context.Context, topic string) (*Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	msg, ok := m.retained[topic]
	if !ok {
		return nil, nil
	}
	return msg, nil
}
func (m *MemoryStore) SaveRetained(_ context.Context, topic string, msg *Message) error {
	m.mu.Lock()
	m.retained[topic] = msg
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) DeleteRetained(_ context.Context, topic string) error {
	m.mu.Lock()
	delete(m.retained, topic)
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) ListRetained(_ context.Context) ([]*Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Message, 0, len(m.retained))
	for _, v := range m.retained {
		out = append(out, v)
	}
	return out, nil
}

func retainedSize(msg *Message) int64 {
	if msg == nil {
		return 0
	}
	// topic + payload + small overhead; keep consistent with Redis counting
	return int64(len(msg.Topic) + len(msg.Payload) + 10)
}

func (m *MemoryStore) GetRetainedStats(_ context.Context) (RetainStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stats := RetainStats{
		TotalMessages: len(m.retained),
		TopicStats:    make(map[string]TopicRetainStats, len(m.retained)),
	}
	var total int64
	for topic, msg := range m.retained {
		sz := retainedSize(msg)
		total += sz
		stats.TopicStats[topic] = TopicRetainStats{Count: 1, Size: sz}
	}
	stats.TotalSize = total
	return stats, nil
}
func (m *MemoryStore) EnqueueOffline(_ context.Context, clientID string, msg *Message) error {
	m.mu.Lock()
	m.offline[clientID] = append(m.offline[clientID], msg)
	// cap 1000 to prevent unbounded growth
	if len(m.offline[clientID]) > 1000 {
		m.offline[clientID] = m.offline[clientID][len(m.offline[clientID])-1000:]
	}
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) DequeueOffline(_ context.Context, clientID string) ([]*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := m.offline[clientID]
	delete(m.offline, clientID)
	return msgs, nil
}
func (m *MemoryStore) ClearOffline(_ context.Context, clientID string) error {
	m.mu.Lock()
	delete(m.offline, clientID)
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) SavePendingWill(_ context.Context, w *PendingWill) error {
	m.mu.Lock()
	m.pendingWills[w.ClientID] = w
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) DeletePendingWill(_ context.Context, clientID string) error {
	m.mu.Lock()
	delete(m.pendingWills, clientID)
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) ListPendingWills(_ context.Context) ([]*PendingWill, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*PendingWill, 0, len(m.pendingWills))
	for _, v := range m.pendingWills {
		out = append(out, v)
	}
	return out, nil
}
func (m *MemoryStore) SavePendingRetry(_ context.Context, r *PendingRetry) error {
	m.mu.Lock()
	key := retryKey(r.ClientID, r.PacketID)
	m.pendingRetries[key] = r
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) DeletePendingRetry(_ context.Context, clientID string, packetID uint16) error {
	m.mu.Lock()
	delete(m.pendingRetries, retryKey(clientID, packetID))
	m.mu.Unlock()
	return nil
}
func (m *MemoryStore) ListPendingRetries(_ context.Context) ([]*PendingRetry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*PendingRetry, 0, len(m.pendingRetries))
	for _, v := range m.pendingRetries {
		out = append(out, v)
	}
	return out, nil
}

func retryKey(clientID string, packetID uint16) string {
	return fmt.Sprintf("%s:%d", clientID, packetID)
}

func (m *MemoryStore) Close() error { return nil }
