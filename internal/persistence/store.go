package persistence

import (
	"context"
	"time"

	"mqtt/internal/session"
)

// Message for persistence (normalized)
type Message struct {
	Topic          string
	Payload        []byte
	QoS            byte
	Retain         bool
	From           string
	CreatedAt      int64  // unix millis for expiry calc
	ExpiryInterval uint32 // 0 means no expiry
}

func (m *Message) IsExpired() bool {
	if m.ExpiryInterval == 0 {
		return false
	}
	if m.CreatedAt == 0 {
		return false
	}
	expiryTime := m.CreatedAt + int64(m.ExpiryInterval)*1000
	return expiryTime < timeNowMillis()
}

func timeNowMillis() int64 {
	return timeNow().UnixMilli()
}

var timeNow = func() time.Time { return time.Now() }

type RetainStats struct {
	TotalMessages int
	TotalSize     int64
	TopicStats    map[string]TopicRetainStats
}

type TopicRetainStats struct {
	Count int
	Size  int64
}

type Store interface {
	GetSession(ctx context.Context, clientID string) (*session.Session, error)
	SaveSession(ctx context.Context, s *session.Session) error
	DeleteSession(ctx context.Context, clientID string) error

	GetRetained(ctx context.Context, topic string) (*Message, error)
	SaveRetained(ctx context.Context, topic string, msg *Message) error
	DeleteRetained(ctx context.Context, topic string) error
	ListRetained(ctx context.Context) ([]*Message, error)
	GetRetainedStats(ctx context.Context) (RetainStats, error)

	EnqueueOffline(ctx context.Context, clientID string, msg *Message) error
	DequeueOffline(ctx context.Context, clientID string) ([]*Message, error)
	ClearOffline(ctx context.Context, clientID string) error

	Close() error
}
