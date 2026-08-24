package persistence

import (
	"context"
	"mqtt/internal/session"
)

// Message for persistence (normalized)
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	From    string
}

type Store interface {
	GetSession(ctx context.Context, clientID string) (*session.Session, error)
	SaveSession(ctx context.Context, s *session.Session) error
	DeleteSession(ctx context.Context, clientID string) error

	GetRetained(ctx context.Context, topic string) (*Message, error)
	SaveRetained(ctx context.Context, topic string, msg *Message) error
	DeleteRetained(ctx context.Context, topic string) error
	ListRetained(ctx context.Context) ([]*Message, error)

	EnqueueOffline(ctx context.Context, clientID string, msg *Message) error
	DequeueOffline(ctx context.Context, clientID string) ([]*Message, error)
	ClearOffline(ctx context.Context, clientID string) error

	Close() error
}
