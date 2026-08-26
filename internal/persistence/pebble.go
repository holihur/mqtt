package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"mqtt/internal/session"

	"github.com/cockroachdb/pebble"
)

type PebbleStore struct {
	db     *pebble.DB
	prefix string
}

func NewPebbleStore(dir string, prefix string) (*PebbleStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("pebble dir empty")
	}
	if prefix == "" {
		prefix = "mqtt"
	}
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStore{db: db, prefix: prefix}, nil
}

func (p *PebbleStore) key(parts ...string) string {
	s := p.prefix
	for _, part := range parts {
		s += ":" + part
	}
	return s
}

func (p *PebbleStore) GetSession(_ context.Context, clientID string) (*session.Session, error) {
	v, closer, err := p.db.Get([]byte(p.key("session", clientID)))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var s session.Session
	if err := json.Unmarshal(v, &s); err != nil {
		return nil, err
	}
	if s.Subscriptions == nil {
		s.Subscriptions = make(map[string]byte)
	}
	if s.Inflight == nil {
		s.Inflight = make(map[uint16]*session.InflightEntry)
	}
	return &s, nil
}

func (p *PebbleStore) SaveSession(_ context.Context, s *session.Session) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return p.db.Set([]byte(p.key("session", s.ClientID)), data, pebble.Sync)
}

func (p *PebbleStore) DeleteSession(_ context.Context, clientID string) error {
	err := p.db.Delete([]byte(p.key("session", clientID)), pebble.Sync)
	if err == pebble.ErrNotFound {
		return nil
	}
	return err
}

func (p *PebbleStore) GetRetained(_ context.Context, topic string) (*Message, error) {
	v, closer, err := p.db.Get([]byte(p.key("retain", topic)))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var m Message
	if err := json.Unmarshal(v, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (p *PebbleStore) SaveRetained(_ context.Context, topic string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return p.db.Set([]byte(p.key("retain", topic)), data, pebble.Sync)
}

func (p *PebbleStore) DeleteRetained(_ context.Context, topic string) error {
	err := p.db.Delete([]byte(p.key("retain", topic)), pebble.Sync)
	if err == pebble.ErrNotFound {
		return nil
	}
	return err
}

func (p *PebbleStore) ListRetained(_ context.Context) ([]*Message, error) {
	prefix := p.key("retain", "")
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*Message
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if !strings.HasPrefix(string(k), prefix) {
			continue
		}
		v := iter.Value()
		var m Message
		if err := json.Unmarshal(v, &m); err != nil {
			continue
		}
		out = append(out, &m)
	}
	return out, nil
}

func (p *PebbleStore) GetRetainedStats(ctx context.Context) (RetainStats, error) {
	msgs, err := p.ListRetained(ctx)
	if err != nil {
		return RetainStats{}, err
	}
	stats := RetainStats{
		TotalMessages: len(msgs),
		TopicStats:    make(map[string]TopicRetainStats, len(msgs)),
	}
	var total int64
	for _, m := range msgs {
		sz := retainedSize(m)
		total += sz
		stats.TopicStats[m.Topic] = TopicRetainStats{Count: 1, Size: sz}
	}
	stats.TotalSize = total
	return stats, nil
}

func (p *PebbleStore) EnqueueOffline(_ context.Context, clientID string, msg *Message) error {
	k := []byte(p.key("offline", clientID))
	v, closer, err := p.db.Get(k)
	var list []*Message
	if err == nil {
		_ = json.Unmarshal(v, &list)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return err
	}
	list = append(list, msg)
	if len(list) > 1000 {
		list = list[len(list)-1000:]
	}
	data, err := json.Marshal(list)
	if err != nil {
		return err
	}
	return p.db.Set(k, data, pebble.Sync)
}

func (p *PebbleStore) DequeueOffline(_ context.Context, clientID string) ([]*Message, error) {
	k := []byte(p.key("offline", clientID))
	v, closer, err := p.db.Get(k)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var list []*Message
	if err := json.Unmarshal(v, &list); err != nil {
		closer.Close()
		_ = p.db.Delete(k, pebble.Sync)
		return nil, err
	}
	closer.Close()
	_ = p.db.Delete(k, pebble.Sync)
	return list, nil
}

func (p *PebbleStore) ClearOffline(_ context.Context, clientID string) error {
	err := p.db.Delete([]byte(p.key("offline", clientID)), pebble.Sync)
	if err == pebble.ErrNotFound {
		return nil
	}
	return err
}

func (p *PebbleStore) Close() error {
	return p.db.Close()
}

var _ Store = (*PebbleStore)(nil)
var _ Store = (*MemoryStore)(nil)
var _ Store = (*RedisStore)(nil)
