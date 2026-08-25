package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"mqtt/internal/broker"
	"mqtt/internal/hook"
	"mqtt/internal/persistence"
)

// ---------------------------------------------------------------------------
// 方式1：实现 auth.Authenticator 接口（兼容老代码），再用 AuthAdapter 桥进 Hook
// 适合：已有 DB/JWT 校验逻辑，直接复用
// ---------------------------------------------------------------------------

type DBAuthenticator struct {
	users map[string]string // username -> password
}

func (d *DBAuthenticator) Authenticate(clientID, username string, password []byte) bool {
	if username == "" {
		return false
	}
	want, ok := d.users[username]
	if !ok {
		return false
	}
	return want == string(password)
}

func (d *DBAuthenticator) Authorize(clientID, topic string, isPublish bool) bool {
	// 示例：租户隔离，clientID 形如 t42-xxxx 只允许 tenant/t42/#
	tenant := ""
	if idx := strings.Index(clientID, "-"); idx > 0 {
		tenant = clientID[:idx]
	}
	if tenant == "" {
		return true // 内部设备放行
	}
	if strings.HasPrefix(topic, "$SYS/") {
		return true
	}
	return strings.HasPrefix(topic, "tenant/"+tenant+"/")
}

// ---------------------------------------------------------------------------
// 方式2：直接实现 hook.Hook 的 OnAuth/OnSubscribe（更灵活）
// 适合：新写逻辑，需要返回详细错误、记录审计、或做异步校验
// ---------------------------------------------------------------------------

type HookAuthenticator struct {
	hook.BaseHook
	blocked map[string]bool
}

func (h HookAuthenticator) ID() string { return "my-hook-auth" }

func (h HookAuthenticator) OnAuth(clientID, username string, password []byte) error {
	if h.blocked[username] {
		return fmt.Errorf("%w: user %s is blocked", hook.ErrDenied, username)
	}
	if username == "alice" && string(password) != "s3cr3t" {
		return fmt.Errorf("%w: bad password", hook.ErrDenied)
	}
	slog.Info("hook auth ok", "client", clientID, "username", username)
	return nil
}

func (h HookAuthenticator) OnSubscribe(clientID, filter string, qos byte) error {
	if strings.HasPrefix(filter, "tenant/secret/#") {
		return fmt.Errorf("%w: filter %s is forbidden", hook.ErrDenied, filter)
	}
	return nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	store := persistence.NewMemoryStore()

	// 使用方式1：DBAuthenticator + AuthAdapter
	dbAuth := &DBAuthenticator{
		users: map[string]string{"alice": "s3cr3t", "bob": "123456"},
	}
	b1 := broker.New(broker.Config{TCPAddr: "127.0.0.1:0", AllowAnonymous: false}, store, dbAuth)
	// broker.New 已自动将 dbAuth 包装成 hook id="auth" 注册，此时无需手动注册
	slog.Info("b1 hooks", "count", b1.Hooks().Len(), "ids", hookIDs(b1))

	// 覆盖示例：如果你想手动控制顺序，可传 nil 再手动注册
	b2 := broker.New(broker.Config{TCPAddr: "127.0.0.1:0", AllowAnonymous: true}, store, nil)
	// 手动注册多个 Auth Hook，顺序决定优先级：先 DB 再 Hook
	b2.RegisterHook(hook.NewAuthAdapter(dbAuth))
	b2.RegisterHook(HookAuthenticator{blocked: map[string]bool{"evil": true}})
	b2.RegisterHook(hook.TenantIsolationHook{})
	b2.RegisterHook(hook.HexDumpHook{})
	slog.Info("b2 hooks", "count", b2.Hooks().Len(), "ids", hookIDs(b2))

	// 演示：直接调 Manager 验证效果
	m := b2.Hooks()
	testAuth(m, "t42-abc", "alice", "s3cr3t", "tenant/t42/data")
	testAuth(m, "t42-abc", "alice", "wrong", "tenant/t42/data")
	testAuth(m, "t42-abc", "evil", "any", "tenant/t42/data")
	testAuth(m, "t42-abc", "alice", "s3cr3t", "tenant/t99/data") // 租户越权
	testAuth(m, "t42-abc", "alice", "s3cr3t", "tenant/secret/data")
}

func testAuth(m *hook.Manager, clientID, username, password, topic string) {
	errAuth := m.ExecAuth(clientID, username, []byte(password))
	errPub := m.ExecPublish(clientID, topic, []byte("hello"), 0, false)
	errSub := m.ExecSubscribe(clientID, topic, 0)
	slog.Info("test",
		"client", clientID, "user", username, "topic", topic,
		"authErr", errAuth, "pubErr", errPub, "subErr", errSub,
	)
}

func hookIDs(b *broker.Broker) []string {
	var ids []string
	for _, h := range b.Hooks().Hooks() {
		ids = append(ids, h.ID())
	}
	return ids
}
