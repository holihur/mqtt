package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt/internal/session"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	cli    redis.UniversalClient
	prefix string
}

func NewRedisStore(addr string, prefix string) (*RedisStore, error) {
	cli := redis.NewClient(&redis.Options{Addr: addr})
	if err := cli.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}
	if prefix == "" {
		prefix = "mqtt"
	}
	return &RedisStore{cli: cli, prefix: prefix}, nil
}

func NewRedisStoreWithClient(cli redis.UniversalClient, prefix string) *RedisStore {
	if prefix == "" {
		prefix = "mqtt"
	}
	return &RedisStore{cli: cli, prefix: prefix}
}

func (r *RedisStore) key(parts ...string) string {
	s := r.prefix
	for _, p := range parts {
		s += ":" + p
	}
	return s
}

// session stored as JSON

func (r *RedisStore) GetSession(ctx context.Context, clientID string) (*session.Session, error) {
	data, err := r.cli.Get(ctx, r.key("session", clientID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s session.Session
	if err := json.Unmarshal(data, &s); err != nil {
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
func (r *RedisStore) SaveSession(ctx context.Context, s *session.Session) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	// Use expiry interval if set and not max
	// For simplicity, store without TTL; expiry handled by manager
	return r.cli.Set(ctx, r.key("session", clientIDKey(s.ClientID)), data, 0).Err()
}
func clientIDKey(id string) string { return id }

func (r *RedisStore) DeleteSession(ctx context.Context, clientID string) error {
	return r.cli.Del(ctx, r.key("session", clientID)).Err()
}

func (r *RedisStore) GetRetained(ctx context.Context, topic string) (*Message, error) {
	data, err := r.cli.Get(ctx, r.key("retain", topic)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
func (r *RedisStore) SaveRetained(ctx context.Context, topic string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return r.cli.Set(ctx, r.key("retain", topic), data, 0).Err()
}
func (r *RedisStore) DeleteRetained(ctx context.Context, topic string) error {
	return r.cli.Del(ctx, r.key("retain", topic)).Err()
}
func (r *RedisStore) ListRetained(ctx context.Context) ([]*Message, error) {
	var keys []string
	iter := r.cli.Scan(ctx, 0, r.key("retain", "*"), 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if len(keys) == 0 {
		return nil, nil
	}
	vals, err := r.cli.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]*Message, 0, len(vals))
	for _, v := range vals {
		s, ok := v.(string)
		if !ok || s == "" {
			continue
		}
		var m Message
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			continue
		}
		out = append(out, &m)
	}
	return out, nil
}
func (r *RedisStore) EnqueueOffline(ctx context.Context, clientID string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	k := r.key("offline", clientID)
	pipe := r.cli.Pipeline()
	pipe.RPush(ctx, k, data)
	pipe.LTrim(ctx, k, -1000, -1)
	_, err = pipe.Exec(ctx)
	return err
}
func (r *RedisStore) DequeueOffline(ctx context.Context, clientID string) ([]*Message, error) {
	k := r.key("offline", clientID)
	vals, err := r.cli.LRange(ctx, k, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	_ = r.cli.Del(ctx, k).Err()
	var out []*Message
	for _, v := range vals {
		var m Message
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			fmt.Printf("unmarshal offline msg: %v\n", err)
			continue
		}
		out = append(out, &m)
	}
	return out, nil
}
func (r *RedisStore) ClearOffline(ctx context.Context, clientID string) error {
	return r.cli.Del(ctx, r.key("offline", clientID)).Err()
}
func (r *RedisStore) Close() error                  { return r.cli.Close() }
func (r *RedisStore) Client() redis.UniversalClient { return r.cli }
