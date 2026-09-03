package broker

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"log/slog"
	"mqtt/internal/auth"
	"mqtt/internal/cluster"
	"mqtt/internal/codec"
	"mqtt/internal/hook"
	"mqtt/internal/persistence"
	"mqtt/internal/session"
	"mqtt/internal/topic"
	"mqtt/internal/transport"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	mqttMessagesReceived = promauto.NewCounter(prometheus.CounterOpts{Name: "mqtt_messages_received_total", Help: "Total MQTT messages received"})
	mqttMessagesSent     = promauto.NewCounter(prometheus.CounterOpts{Name: "mqtt_messages_sent_total", Help: "Total MQTT messages sent"})
	//nolint:unused
	mqttClientsConnected    = promauto.NewGauge(prometheus.GaugeOpts{Name: "mqtt_clients_connected", Help: "Current connected clients"})
	mqttInflight            = promauto.NewGauge(prometheus.GaugeOpts{Name: "mqtt_inflight_messages", Help: "Current inflight messages"})
	mqttAuthFailed          = promauto.NewCounter(prometheus.CounterOpts{Name: "mqtt_auth_failed_total", Help: "Total auth failures"})
	mqttPacketDropped       = promauto.NewCounterVec(prometheus.CounterOpts{Name: "mqtt_packet_dropped_total", Help: "Total dropped packets"}, []string{"reason"})
	mqttRedisLatency        = promauto.NewHistogram(prometheus.HistogramOpts{Name: "mqtt_redis_latency_seconds", Help: "Redis operation latency", Buckets: prometheus.DefBuckets})
	mqttRetainQuotaExceeded = promauto.NewCounterVec(prometheus.CounterOpts{Name: "mqtt_retain_quota_exceeded_total", Help: "Total retain quota exceeded"}, []string{"reason"})
)

type Config struct {
	NodeID                    string
	TCPAddr                   string
	WSAddr                    string
	RedisAddr                 string
	PprofAddr                 string
	AdminAddr                 string // 管理 API 监听地址, 空则禁用
	AdminToken                string // 管理 API Bearer token, 空则仅允许 loopback
	AdminTLS                  bool   // 管理 API 是否走 TLS (复用 -tls-cert/-tls-key)
	WebUIAddr                 string // dashboard 监听地址 (嵌入前端 + /api/v1), 空则禁用
	ACLFile                   string
	JWTSecret                 string
	MaxPacketSize             int
	AllowAnonymous            bool
	TLSCertFile               string
	TLSKeyFile                string
	TLSCAFile                 string
	TLSConfig                 *tls.Config
	MaxConnections            int
	MaxPublishPerSec          int
	MaxSubscribePerSec        int
	MaxRetainedMessages       int
	MaxRetainedSize           int64
	MaxRetainPerTopic         int
	MaxRetainSizePerTopic     int64
	MaxInflightWindow         int // server-side cap on concurrent unacked QoS1/2 deliveries per client
	MaxSubscriptionsPerClient int // cap on total active subscriptions per client (trie memory bound)
	WalDir                    string
	WsAllowOrigins            []string
}

type BrokerStats struct {
	StartedAt        time.Time
	MessagesReceived int64
	MessagesSent     int64
	ClientsConnected int64
	ClientsTotal     int64
}

type clientLimiter struct {
	mu             sync.Mutex
	publishCount   int
	subscribeCount int
	window         time.Time
	lastSeen       time.Time
}

type Broker struct {
	cfg        Config
	store      persistence.Store
	trie       topic.Trie
	sharedMu   sync.Mutex
	sharedSubs map[string]map[string][]string // group -> filter -> []clientID
	sharedIdx  map[string]int                 // group -> next index for round-robin
	auth       auth.Authenticator
	nodeID     string
	cluster    *cluster.Cluster
	redisCli   redis.UniversalClient

	mu       sync.RWMutex
	conns    map[string]*transport.Conn // clientID -> conn
	sessions map[string]*session.Session

	stats    BrokerStats
	listener *transport.Listener

	limitMu  sync.Mutex
	limiters map[string]*clientLimiter

	// QoS1/QoS2 重试调度: 单 broker ticker 驱动在内存到期队列，替代每条消息
	// 一个 time.AfterFunc。持久化 (SavePendingRetry/DeletePendingRetry) 语义不变。
	retryMu    sync.Mutex
	retryQueue map[string]map[uint16]*retryEntry // clientID -> packetID -> entry

	// 延迟遗嘱 (DelayInterval>0) 的取消句柄: clientID -> timer。
	// 同一 client 重连 (会话恢复/重建) 时按 MQTT5 需丢弃未投递的延迟遗嘱。
	willMu     sync.Mutex
	willTimers map[string]*time.Timer

	remoteMu    sync.RWMutex
	remoteTries map[string]topic.Trie // nodeID -> trie of remote subs

	hooks *hook.Manager

	// 管理 API 版本信息 (由 WithVersion 注入, 独立运行时来自 cmd/broker ldflags)
	versionInfo brokerVersion

	// lifecycle: 支持独立与嵌入式双模式
	customListener net.Listener
	runMu          sync.Mutex
	running        bool
	cancel         context.CancelFunc
	metricsSrv     *http.Server
	adminSrv       *http.Server
	webuiSrv       *http.Server
}

// brokerVersion 承载构建期版本信息, 用于管理 API /api/v1/info。
type brokerVersion struct {
	version string
	commit  string
	date    string
}

func NewWithOptions(cfg Config, opts ...Option) (*Broker, error) {
	cfg.ApplyDefaults()
	if cfg.NodeID == "" {
		cfg.NodeID = uuid.NewString()[:8]
	}
	b := &Broker{
		cfg:         cfg,
		trie:        topic.NewTrie(),
		sharedSubs:  make(map[string]map[string][]string),
		sharedIdx:   make(map[string]int),
		nodeID:      cfg.NodeID,
		conns:       make(map[string]*transport.Conn),
		sessions:    make(map[string]*session.Session),
		limiters:    make(map[string]*clientLimiter),
		retryQueue:  make(map[string]map[uint16]*retryEntry),
		willTimers:  make(map[string]*time.Timer),
		remoteTries: make(map[string]topic.Trie),
		hooks:       hook.NewManager(),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(b); err != nil {
			return nil, err
		}
	}
	if b.auth == nil {
		a, err := buildAuthenticator(b.cfg)
		if err != nil {
			return nil, err
		}
		b.auth = a
	}
	if b.store == nil {
		if b.cfg.WalDir != "" && b.cfg.WalDir != "-" {
			if ps, err := persistence.NewPebbleStore(b.cfg.WalDir, "mqtt"); err == nil {
				b.store = ps
			} else {
				slog.Warn("pebble WAL open failed, fallback to memory", "dir", b.cfg.WalDir, "err", err)
				b.store = persistence.NewMemoryStore()
			}
		} else {
			b.store = persistence.NewMemoryStore()
		}
	}
	if b.nodeID == "" {
		b.nodeID = b.cfg.NodeID
	}
	if aa := hook.NewAuthAdapter(b.auth); aa != nil {
		b.hooks.Register(aa)
	}
	return b, nil
}

func New(cfg Config, store persistence.Store, authenticator auth.Authenticator) *Broker {
	b, err := NewWithOptions(cfg, WithStore(store), func(br *Broker) error {
		if authenticator != nil {
			br.auth = authenticator
		}
		return nil
	})
	if err != nil {
		// fallback: should not happen, but keep compat
		slog.Warn("NewWithOptions failed, fallback", "err", err)
		cfg.ApplyDefaults()
		if cfg.NodeID == "" {
			cfg.NodeID = uuid.NewString()[:8]
		}
		if authenticator == nil {
			if a, err := buildAuthenticator(cfg); err != nil {
				slog.Error("build authenticator failed, denying all auth", "err", err)
				authenticator = &auth.DenyAll{}
			} else {
				authenticator = a
			}
		}
		if store == nil {
			if cfg.WalDir != "" && cfg.WalDir != "-" {
				if ps, err := persistence.NewPebbleStore(cfg.WalDir, "mqtt"); err == nil {
					store = ps
				} else {
					slog.Warn("pebble WAL open failed, fallback to memory", "err", err)
					store = persistence.NewMemoryStore()
				}
			} else {
				store = persistence.NewMemoryStore()
			}
		}
		b = &Broker{
			cfg:         cfg,
			store:       store,
			trie:        topic.NewTrie(),
			sharedSubs:  make(map[string]map[string][]string),
			sharedIdx:   make(map[string]int),
			auth:        authenticator,
			nodeID:      cfg.NodeID,
			conns:       make(map[string]*transport.Conn),
			sessions:    make(map[string]*session.Session),
			limiters:    make(map[string]*clientLimiter),
			retryQueue:  make(map[string]map[uint16]*retryEntry),
			willTimers:  make(map[string]*time.Timer),
			remoteTries: make(map[string]topic.Trie),
			hooks:       hook.NewManager(),
		}
		if aa := hook.NewAuthAdapter(authenticator); aa != nil {
			b.hooks.Register(aa)
		}
	}
	// 保持对老代码的兼容：若 RedisAddr 非空且尚未建立 cluster，延迟到 Start 再建；
	// 此处不做网络 IO，避免嵌入式构造阻塞。
	return b
}

func (b *Broker) RegisterHook(h hook.Hook) { b.hooks.Register(h) }

func (b *Broker) Hooks() *hook.Manager { return b.hooks }

func (b *Broker) onClientDisconnect(clientID string, sess *session.Session, clean bool) {
	b.hooks.ExecDisconnect(clientID, clean)
	b.mu.Lock()
	delete(b.conns, clientID)
	b.mu.Unlock()
	b.removeLimiter(clientID)
	if sess == nil {
		slog.Info("client disconnect", "client", clientID, "clean", clean)
		return
	}
	// 管理 API 显式删除会话后, 不再把会话写回 store / 触发 will。
	sess.Mu.Lock()
	deleted := sess.Deleted
	sess.Mu.Unlock()
	if deleted {
		slog.Info("client disconnect (session deleted via admin api)", "client", clientID)
		return
	}
	sess.Mu.Lock()
	nodeID := sess.NodeID
	sess.Mu.Unlock()
	slog.Info("client disconnect", "client", clientID, "clean", clean, "node", nodeID)
	sess.Mu.Lock()
	expiry := sess.ExpiryInterval
	subs := make([]string, 0, len(sess.Subscriptions))
	for f := range sess.Subscriptions {
		subs = append(subs, f)
	}
	sess.Mu.Unlock()
	if expiry == 0 {
		for _, f := range subs {
			b.trie.Remove(f, clientID)
		}
		if !clean {
			b.handleWill(sess)
		} else {
			sess.Mu.Lock()
			sess.Will = nil
			sess.Mu.Unlock()
		}
		// clean 会话断开即废弃: 从 store 与内存会话表一并移除,
		// 避免残留条目被 /sessions 列出、或让后续持久重连误判 SessionPresent。
		b.mu.Lock()
		delete(b.sessions, clientID)
		b.mu.Unlock()
		if err := b.store.DeleteSession(bgCtx(), clientID); err != nil {
			slog.Warn("store DeleteSession failed", "err", err)
		}
		return
	}
	if !clean && sess.Will != nil {
		b.handleWill(sess)
	} else if clean {
		sess.Mu.Lock()
		sess.Will = nil
		sess.Mu.Unlock()
	}
	sess.Mu.Lock()
	sess.Connected = false
	// 记录离线时刻: 供持久会话 (有限 ExpiryInterval) 的过期清理使用
	sess.OfflineSince = time.Now()
	sess.Mu.Unlock()
	if err := b.store.SaveSession(bgCtx(), sess); err != nil {
		slog.Warn("store SaveSession failed", "err", err)
	}
	if expiry == 0 && clean {
		for _, f := range subs {
			b.trie.Remove(f, clientID)
		}
	}
}

func (b *Broker) handleWill(sess *session.Session) {
	sess.Mu.Lock()
	if sess.Will == nil {
		sess.Mu.Unlock()
		return
	}
	w := sess.Will
	clientID := sess.ClientID
	sess.Will = nil
	sess.Mu.Unlock()
	if err := b.hooks.ExecPublish(clientID, w.Topic, w.Payload, w.QoS, w.Retain); err != nil {
		return
	}
	if w.DelayInterval > 86400 {
		w.DelayInterval = 86400
	}
	if w.DelayInterval > 0 {
		deliverAt := time.Now().UnixMilli() + int64(w.DelayInterval)*1000
		pw := &persistence.PendingWill{
			ClientID:  clientID,
			Topic:     w.Topic,
			Payload:   w.Payload,
			QoS:       w.QoS,
			Retain:    w.Retain,
			DeliverAt: deliverAt,
		}
		if err := b.store.SavePendingWill(bgCtx(), pw); err != nil {
			slog.Warn("store SavePendingWill failed", "client", clientID, "err", err)
		}
		topic := w.Topic
		payload := w.Payload
		q := w.QoS
		ret := w.Retain
		b.armWillTimer(clientID, time.Duration(w.DelayInterval)*time.Second, func() {
			_ = b.store.DeletePendingWill(bgCtx(), clientID)
			b.routeMessage(topic, payload, q, ret, nil, clientID)
		})
		return
	}
	b.routeMessage(w.Topic, w.Payload, w.QoS, w.Retain, nil, clientID)
}

// armWillTimer 注册延迟遗嘱投递 timer 并记录句柄，供同 client 重连时取消。
func (b *Broker) armWillTimer(clientID string, delay time.Duration, do func()) {
	timer := time.AfterFunc(delay, func() {
		b.willMu.Lock()
		delete(b.willTimers, clientID)
		b.willMu.Unlock()
		do()
	})
	b.willMu.Lock()
	if old := b.willTimers[clientID]; old != nil {
		old.Stop()
	}
	b.willTimers[clientID] = timer
	b.willMu.Unlock()
}

// cancelPendingWill 取消并删除某 client 尚未投递的延迟遗嘱
// (MQTT5: 延迟期间会话恢复时遗嘱必须被丢弃)。
func (b *Broker) cancelPendingWill(clientID string) {
	b.willMu.Lock()
	if t := b.willTimers[clientID]; t != nil {
		t.Stop()
		delete(b.willTimers, clientID)
	}
	b.willMu.Unlock()
	_ = b.store.DeletePendingWill(bgCtx(), clientID)
}

func (b *Broker) restorePendingWills() {
	wills, err := b.store.ListPendingWills(bgCtx())
	if err != nil {
		slog.Warn("restore pending wills failed", "err", err)
		return
	}
	now := time.Now().UnixMilli()
	for _, w := range wills {
		ww := w
		delay := ww.DeliverAt - now
		if delay <= 0 {
			_ = b.store.DeletePendingWill(bgCtx(), ww.ClientID)
			b.routeMessage(ww.Topic, ww.Payload, ww.QoS, ww.Retain, nil, ww.ClientID)
			continue
		}
		b.armWillTimer(ww.ClientID, time.Duration(delay)*time.Millisecond, func() {
			_ = b.store.DeletePendingWill(bgCtx(), ww.ClientID)
			b.routeMessage(ww.Topic, ww.Payload, ww.QoS, ww.Retain, nil, ww.ClientID)
		})
	}
	if len(wills) > 0 {
		slog.Info("restored pending wills", "count", len(wills))
	}
}

func (b *Broker) restorePendingRetries() {
	retries, err := b.store.ListPendingRetries(bgCtx())
	if err != nil {
		slog.Warn("restore pending retries failed", "err", err)
		return
	}
	now := time.Now().UnixMilli()
	for _, r := range retries {
		rr := r
		delay := rr.NextRetryAt - now
		if delay <= 0 {
			// 已到期的: 客户端在线且有对应 inflight 则立即 Dup 重投并安排下一次；
			// 否则删除失效的持久化记录。
			b.mu.RLock()
			sess, ok1 := b.sessions[rr.ClientID]
			conn, ok2 := b.conns[rr.ClientID]
			b.mu.RUnlock()
			if ok1 && ok2 {
				if e, ok := sess.GetInflight(rr.PacketID); ok {
					e.Dup = true
					pub := &codec.Packet{Type: codec.TypePUBLISH, Version: conn.Version(), Topic: e.Topic, QoS: e.QoS, Payload: e.Payload, PacketID: rr.PacketID, Dup: true}
					_ = b.sendPacket(conn, pub)
					b.scheduleRetry(rr.ClientID, rr.PacketID, rr.Retries+1)
					continue
				}
			}
			_ = b.store.DeletePendingRetry(bgCtx(), rr.ClientID, rr.PacketID)
			continue
		}
		// 未到期: 只入内存调度队列，由 retryLoop 在到期时触发。
		b.armRetry(rr.ClientID, rr.PacketID, rr.Retries, rr.NextRetryAt)
	}
	if len(retries) > 0 {
		slog.Info("restored pending retries", "count", len(retries))
	}
}

func (b *Broker) removeLimiter(clientID string) {
	b.limitMu.Lock()
	delete(b.limiters, clientID)
	b.limitMu.Unlock()
}

// LimiterCount returns number of entries in limiters map (for metrics/tests).
func (b *Broker) LimiterCount() int {
	b.limitMu.Lock()
	defer b.limitMu.Unlock()
	return len(b.limiters)
}

// cleanupLimiters removes limiters idle longer than idle duration.
// Returns number of removed entries. Exposed for testing.
func (b *Broker) cleanupLimiters(idle time.Duration) int {
	now := time.Now()
	b.limitMu.Lock()
	defer b.limitMu.Unlock()
	removed := 0
	for id, lim := range b.limiters {
		lim.mu.Lock()
		last := lim.lastSeen
		if last.IsZero() {
			last = lim.window
		}
		idleTime := now.Sub(last)
		lim.mu.Unlock()
		if idleTime > idle {
			delete(b.limiters, id)
			removed++
		}
	}
	return removed
}

func (b *Broker) limiterJanitor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.cleanupLimiters(10 * time.Minute)
		}
	}
}

//nolint:unused

// ---------------------------------------------------------------------------
// 会话过期清理 (P0-4)
// ---------------------------------------------------------------------------

// sessionJanitor 周期扫描离线持久会话，清理已超过 ExpiryInterval 的会话及其
// 订阅/离线队列/挂起重试。
func (b *Broker) sessionJanitor(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.sweepExpiredSessions()
		}
	}
}

// sweepExpiredSessions 删除所有已离线且超过 ExpiryInterval 的会话。
func (b *Broker) sweepExpiredSessions() {
	now := time.Now()
	b.mu.RLock()
	var expired []string
	for id, sess := range b.sessions {
		if sess == nil {
			continue
		}
		if _, online := b.conns[id]; online {
			continue
		}
		sess.Mu.Lock()
		expiry := sess.ExpiryInterval
		off := sess.OfflineSince
		sess.Mu.Unlock()
		if expiry == 0 || expiry == 0xFFFFFFFF || off.IsZero() {
			continue
		}
		if now.After(off.Add(time.Duration(expiry) * time.Second)) {
			expired = append(expired, id)
		}
	}
	b.mu.RUnlock()
	for _, id := range expired {
		b.expireOfflineSession(id)
	}
	if len(expired) > 0 {
		slog.Info("session expiry sweep", "expired", len(expired))
	}
}

// expireOfflineSession 清理一个过期的离线持久会话。
func (b *Broker) expireOfflineSession(clientID string) {
	b.mu.RLock()
	sess := b.sessions[clientID]
	b.mu.RUnlock()
	if sess == nil {
		return
	}
	// 取消其挂起的 inflight 重试 (在内存 + store)
	var ids []uint16
	sess.Mu.Lock()
	for pid := range sess.Inflight {
		ids = append(ids, pid)
	}
	sess.Mu.Unlock()
	for _, pid := range ids {
		b.cancelRetry(clientID, pid)
	}
	if err := b.deleteSession(bgCtx(), clientID); err != nil {
		slog.Warn("session expiry delete failed", "client", clientID, "err", err)
	}
}
