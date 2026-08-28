package hook

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/topic"

	"golang.org/x/crypto/bcrypt"
)

type PasswordHasher interface {
	Verify(password []byte, hash string) bool
}

type BcryptHasher struct{}

func (BcryptHasher) Verify(password []byte, hash string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), password) == nil
}

type PlainHasher struct{}

func (PlainHasher) Verify(password []byte, hash string) bool {
	return string(password) == hash
}

type DBAuthConfig struct {
	UsersQuery   string
	ACLQuery     string
	QueryTimeout time.Duration
	Hasher       PasswordHasher
}

type DBAuthHook struct {
	db           *sql.DB
	usersQuery   string
	aclQuery     string
	hasher       PasswordHasher
	queryTimeout time.Duration

	mu      sync.RWMutex
	userMap map[string]string
}

func NewDBAuthHook(db *sql.DB, cfg DBAuthConfig) (*DBAuthHook, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	if cfg.UsersQuery == "" {
		return nil, errors.New("UsersQuery required")
	}
	hasher := cfg.Hasher
	if hasher == nil {
		hasher = BcryptHasher{}
	}
	timeout := cfg.QueryTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if err := db.Ping(); err != nil {
		slog.Warn("db ping failed", "err", err)
	}
	return &DBAuthHook{
		db:           db,
		usersQuery:   cfg.UsersQuery,
		aclQuery:     cfg.ACLQuery,
		hasher:       hasher,
		queryTimeout: timeout,
		userMap:      make(map[string]string),
	}, nil
}

func (h *DBAuthHook) ID() string { return "db-auth" }

func (h *DBAuthHook) OnAuth(clientID, username string, password []byte) error {
	if username == "" {
		// anonymous access must be an explicit deployment decision (broker-level
		// AllowAnonymous), never a silent bypass of the DB credential check
		return ErrDenied
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.queryTimeout)
	defer cancel()
	rows, err := h.db.QueryContext(ctx, h.usersQuery, username)
	if err != nil {
		slog.Warn("db auth query failed", "username", username, "err", err)
		return ErrDenied
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			slog.Warn("db auth rows error", "err", err)
			return ErrDenied
		}
		slog.Info("db auth user not found", "username", username, "client", clientID)
		return ErrDenied
	}
	cols, _ := rows.Columns()
	var hash string
	var status sql.NullString
	if len(cols) >= 2 {
		if err := rows.Scan(&hash, &status); err != nil {
			slog.Warn("db auth scan failed", "err", err)
			return ErrDenied
		}
		if status.Valid && status.String != "" && status.String != "active" {
			slog.Info("db auth user disabled", "username", username, "status", status.String)
			return ErrDenied
		}
	} else {
		if err := rows.Scan(&hash); err != nil {
			slog.Warn("db auth scan failed", "err", err)
			return ErrDenied
		}
	}
	if !h.hasher.Verify(password, hash) {
		slog.Info("db auth password mismatch", "username", username, "client", clientID)
		return ErrDenied
	}
	h.mu.Lock()
	h.userMap[clientID] = username
	h.mu.Unlock()
	slog.Info("db auth success", "username", username, "client", clientID)
	return nil
}

func (h *DBAuthHook) OnSubscribe(clientID, filter string, _ byte) error {
	return h.checkACL(clientID, filter)
}

func (h *DBAuthHook) OnPublish(clientID, topicName string, _ []byte, _ byte, _ bool) error {
	return h.checkACL(clientID, topicName)
}

func (h *DBAuthHook) checkACL(clientID, requested string) error {
	if h.aclQuery == "" {
		return nil
	}
	if requested == "" {
		return nil
	}
	h.mu.RLock()
	username, ok := h.userMap[clientID]
	h.mu.RUnlock()
	if !ok {
		username = clientID
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.queryTimeout)
	defer cancel()
	rows, err := h.db.QueryContext(ctx, h.aclQuery, username)
	if err != nil {
		slog.Warn("db acl query failed", "client", clientID, "requested", requested, "err", err)
		return ErrDenied
	}
	defer rows.Close()
	var patterns []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			continue
		}
		patterns = append(patterns, p)
	}
	if err := rows.Err(); err != nil {
		slog.Warn("db acl rows error", "err", err)
		return ErrDenied
	}
	if len(patterns) == 0 {
		slog.Info("db acl no patterns, deny", "client", clientID, "requested", requested)
		return ErrDenied
	}
	for _, pat := range patterns {
		if pat == requested || topic.MatchFilter(requested, pat) {
			return nil
		}
		if Match(pat, requested) {
			return nil
		}
	}
	slog.Info("db acl denied", "client", clientID, "requested", requested, "patterns", patterns)
	return ErrDenied
}

func (h *DBAuthHook) OnConnect(_ string) error               { return nil }
func (h *DBAuthHook) OnUnsubscribe(_ string, _ string) error { return nil }
func (h *DBAuthHook) OnDisconnect(clientID string, _ bool) {
	h.mu.Lock()
	delete(h.userMap, clientID)
	h.mu.Unlock()
}
func (h *DBAuthHook) OnPacket(_ string, _ string, _ *codec.Packet, _ string) {}
