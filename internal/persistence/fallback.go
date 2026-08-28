package persistence

import (
	"context"

	"mqtt/internal/session"
)

type FallbackStore struct {
	primary  Store
	fallback Store
}

func NewFallbackStore(primary Store, fallback Store) *FallbackStore {
	return &FallbackStore{primary: primary, fallback: fallback}
}

func (f *FallbackStore) GetSession(ctx context.Context, clientID string) (*session.Session, error) {
	s, err := f.primary.GetSession(ctx, clientID)
	if err == nil && s != nil {
		return s, nil
	}
	if s2, err2 := f.fallback.GetSession(ctx, clientID); err2 == nil && s2 != nil {
		return s2, nil
	}
	return s, err
}

func (f *FallbackStore) SaveSession(ctx context.Context, s *session.Session) error {
	if err := f.primary.SaveSession(ctx, s); err == nil {
		_ = f.fallback.SaveSession(ctx, s)
		return nil
	}
	return f.fallback.SaveSession(ctx, s)
}

func (f *FallbackStore) DeleteSession(ctx context.Context, clientID string) error {
	_ = f.primary.DeleteSession(ctx, clientID)
	_ = f.fallback.DeleteSession(ctx, clientID)
	return nil
}

func (f *FallbackStore) GetRetained(ctx context.Context, topic string) (*Message, error) {
	m, err := f.primary.GetRetained(ctx, topic)
	if err == nil && m != nil {
		return m, nil
	}
	return f.fallback.GetRetained(ctx, topic)
}

func (f *FallbackStore) SaveRetained(ctx context.Context, topic string, msg *Message) error {
	if err := f.primary.SaveRetained(ctx, topic, msg); err == nil {
		_ = f.fallback.SaveRetained(ctx, topic, msg)
		return nil
	}
	return f.fallback.SaveRetained(ctx, topic, msg)
}

func (f *FallbackStore) DeleteRetained(ctx context.Context, topic string) error {
	_ = f.primary.DeleteRetained(ctx, topic)
	_ = f.fallback.DeleteRetained(ctx, topic)
	return nil
}

func (f *FallbackStore) ListRetained(ctx context.Context) ([]*Message, error) {
	list, err := f.primary.ListRetained(ctx)
	if err == nil && len(list) > 0 {
		return list, nil
	}
	return f.fallback.ListRetained(ctx)
}

func (f *FallbackStore) GetRetainedStats(ctx context.Context) (RetainStats, error) {
	stats, err := f.primary.GetRetainedStats(ctx)
	if err == nil {
		return stats, nil
	}
	return f.fallback.GetRetainedStats(ctx)
}

func (f *FallbackStore) EnqueueOffline(ctx context.Context, clientID string, msg *Message) error {
	if err := f.primary.EnqueueOffline(ctx, clientID, msg); err == nil {
		_ = f.fallback.EnqueueOffline(ctx, clientID, msg)
		return nil
	}
	return f.fallback.EnqueueOffline(ctx, clientID, msg)
}

func (f *FallbackStore) DequeueOffline(ctx context.Context, clientID string) ([]*Message, error) {
	msgs, err := f.primary.DequeueOffline(ctx, clientID)
	if err == nil && len(msgs) > 0 {
		return msgs, nil
	}
	return f.fallback.DequeueOffline(ctx, clientID)
}

func (f *FallbackStore) ClearOffline(ctx context.Context, clientID string) error {
	_ = f.primary.ClearOffline(ctx, clientID)
	_ = f.fallback.ClearOffline(ctx, clientID)
	return nil
}

func (f *FallbackStore) SavePendingWill(ctx context.Context, w *PendingWill) error {
	if err := f.primary.SavePendingWill(ctx, w); err == nil {
		_ = f.fallback.SavePendingWill(ctx, w)
		return nil
	}
	return f.fallback.SavePendingWill(ctx, w)
}
func (f *FallbackStore) DeletePendingWill(ctx context.Context, clientID string) error {
	_ = f.primary.DeletePendingWill(ctx, clientID)
	_ = f.fallback.DeletePendingWill(ctx, clientID)
	return nil
}
func (f *FallbackStore) ListPendingWills(ctx context.Context) ([]*PendingWill, error) {
	list, err := f.primary.ListPendingWills(ctx)
	if err == nil && len(list) > 0 {
		return list, nil
	}
	return f.fallback.ListPendingWills(ctx)
}
func (f *FallbackStore) SavePendingRetry(ctx context.Context, r *PendingRetry) error {
	if err := f.primary.SavePendingRetry(ctx, r); err == nil {
		_ = f.fallback.SavePendingRetry(ctx, r)
		return nil
	}
	return f.fallback.SavePendingRetry(ctx, r)
}
func (f *FallbackStore) DeletePendingRetry(ctx context.Context, clientID string, packetID uint16) error {
	_ = f.primary.DeletePendingRetry(ctx, clientID, packetID)
	_ = f.fallback.DeletePendingRetry(ctx, clientID, packetID)
	return nil
}
func (f *FallbackStore) ListPendingRetries(ctx context.Context) ([]*PendingRetry, error) {
	list, err := f.primary.ListPendingRetries(ctx)
	if err == nil && len(list) > 0 {
		return list, nil
	}
	return f.fallback.ListPendingRetries(ctx)
}

func (f *FallbackStore) Close() error {
	_ = f.primary.Close()
	_ = f.fallback.Close()
	return nil
}

var _ Store = (*FallbackStore)(nil)
