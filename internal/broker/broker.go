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
	mqttMessagesReceived      = promauto.NewCounter(prometheus.CounterOpts{Name: "mqtt_messages_received_total", Help: "Total MQTT messages received"})
	mqttMessagesSent          = promauto.NewCounter(prometheus.CounterOpts{Name: "mqtt_messages_sent_total", Help: "Total MQTT messages sent"})
	//nolint:unused
	mqttClientsConnected      = promauto.NewGauge(prometheus.GaugeOpts{Name: "mqtt_clients_connected", Help: "Current connected clients"})
	mqttInflight              = promauto.NewGauge(prometheus.GaugeOpts{Name: "mqtt_inflight_messages", Help: "Current inflight messages"})
	mqttAuthFailed            = promauto.NewCounter(prometheus.CounterOpts{Name: "mqtt_auth_failed_total", Help: "Total auth failures"})
	mqttPacketDropped         = promauto.NewCounterVec(prometheus.CounterOpts{Name: "mqtt_packet_dropped_total", Help: "Total dropped packets"}, []string{"reason"})
	mqttRedisLatency          = promauto.NewHistogram(prometheus.HistogramOpts{Name: "mqtt_redis_latency_seconds", Help: "Redis operation latency", Buckets: prometheus.DefBuckets})
	mqttRetainQuotaExceeded   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "mqtt_retain_quota_exceeded_total", Help: "Total retain quota exceeded"}, []string{"reason"})
)

type Config struct {
	NodeID                string
	TCPAddr               string
	WSAddr                string
	RedisAddr             string
	PprofAddr             string
	ACLFile               string
	JWTSecret             string
	MaxPacketSize         int
	AllowAnonymous        bool
	TLSCertFile           string
	TLSKeyFile            string
	TLSCAFile             string
	TLSConfig             *tls.Config
	MaxConnections        int
	MaxPublishPerSec      int
	MaxSubscribePerSec    int
	MaxRetainedMessages   int
	MaxRetainedSize       int64
	MaxRetainPerTopic     int
	MaxRetainSizePerTopic int64
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
}

type Broker struct {
	cfg        Config
	store      persistence.Store
	trie       *topic.Trie
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

	remoteMu    sync.RWMutex
	remoteTries map[string]*topic.Trie // nodeID -> trie of remote subs

	hooks *hook.Manager

	// lifecycle: 支持独立与嵌入式双模式
	customListener net.Listener
	runMu          sync.Mutex
	running        bool
	cancel         context.CancelFunc
	metricsSrv     *http.Server
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
		remoteTries: make(map[string]*topic.Trie),
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
		b.auth = buildAuthenticator(b.cfg)
	}
	if b.store == nil {
		b.store = persistence.NewMemoryStore()
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
			authenticator = buildAuthenticator(cfg)
		}
		if store == nil {
			store = persistence.NewMemoryStore()
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
			remoteTries: make(map[string]*topic.Trie),
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
	if sess == nil {
		slog.Info("client disconnect", "client", clientID, "clean", clean)
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
	// delay
	if w.DelayInterval > 0 {
		time.AfterFunc(time.Duration(w.DelayInterval)*time.Second, func() {
			b.routeMessage(w.Topic, w.Payload, w.QoS, w.Retain, nil, clientID)
		})
		return
	}
	b.routeMessage(w.Topic, w.Payload, w.QoS, w.Retain, nil, clientID)
}

//nolint:unused
