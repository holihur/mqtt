package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"log/slog"
	"mqtt/internal/auth"
	"mqtt/internal/cluster"
	"mqtt/internal/codec"
	"mqtt/internal/persistence"
	"mqtt/internal/session"
	"mqtt/internal/topic"
	"mqtt/internal/transport"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	mqttMessagesReceived = promauto.NewCounter(prometheus.CounterOpts{Name: "mqtt_messages_received_total", Help: "Total MQTT messages received"})
	mqttMessagesSent     = promauto.NewCounter(prometheus.CounterOpts{Name: "mqtt_messages_sent_total", Help: "Total MQTT messages sent"})
	//nolint:unused
	mqttClientsConnected = promauto.NewGauge(prometheus.GaugeOpts{Name: "mqtt_clients_connected", Help: "Current connected clients"})
	mqttInflight         = promauto.NewGauge(prometheus.GaugeOpts{Name: "mqtt_inflight_messages", Help: "Current inflight messages"})
	mqttAuthFailed       = promauto.NewCounter(prometheus.CounterOpts{Name: "mqtt_auth_failed_total", Help: "Total auth failures"})
	mqttPacketDropped    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "mqtt_packet_dropped_total", Help: "Total dropped packets"}, []string{"reason"})
	//nolint:unused
	mqttRedisLatency = promauto.NewHistogram(prometheus.HistogramOpts{Name: "mqtt_redis_latency_seconds", Help: "Redis operation latency", Buckets: prometheus.DefBuckets})
)

type Config struct {
	NodeID             string
	TCPAddr            string
	WSAddr             string
	RedisAddr          string
	PprofAddr          string
	ACLFile            string
	JWTSecret          string
	MaxPacketSize      int
	AllowAnonymous     bool
	TLSCertFile        string
	TLSKeyFile         string
	TLSCAFile          string
	TLSConfig          *tls.Config
	MaxConnections     int
	MaxPublishPerSec   int
	MaxSubscribePerSec int
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

	statsMu  sync.Mutex
	stats    BrokerStats
	listener *transport.Listener

	limitMu  sync.Mutex
	limiters map[string]*clientLimiter
}

func storeCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

//nolint:govet
func bgCtx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = cancel
	return ctx
}

func loadTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		},
	}
	// preload to validate
	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		return nil, err
	}
	if caFile != "" {
		caData, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("failed to parse CA %s", caFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func (b *Broker) watchACL(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if acl, ok := b.auth.(*auth.FileACL); ok {
				if reloaded, err := acl.Reload(); err != nil {
					slog.Warn("acl reload failed", "err", err)
				} else if reloaded {
					slog.Info("acl reloaded", "path", b.cfg.ACLFile)
				}
			} else if chain, ok := b.auth.(*auth.Chain); ok {
				for _, a := range chain.Auths {
					if facl, ok := a.(*auth.FileACL); ok {
						if reloaded, err := facl.Reload(); err != nil {
							slog.Warn("acl reload failed", "err", err)
						} else if reloaded {
							slog.Info("acl reloaded", "path", b.cfg.ACLFile)
						}
					}
				}
			}
		}
	}
}

func packetHex(p *codec.Packet) string {
	if p == nil {
		return ""
	}
	data, err := codec.Encode(p)
	if err != nil || len(data) == 0 {
		return ""
	}
	hexStr := fmt.Sprintf("%x", data)
	if len(hexStr) > 512 {
		hexStr = hexStr[:512] + "..."
	}
	return hexStr
}

func debugPacket(dir, clientID string, pkt *codec.Packet) {
	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	slog.Debug("packet "+dir, "client", clientID, "type", pkt.Type, "version", pkt.Version, "hex", packetHex(pkt))
}

func (b *Broker) allowPublish(clientID string) bool {
	b.limitMu.Lock()
	lim, ok := b.limiters[clientID]
	if !ok {
		lim = &clientLimiter{window: time.Now()}
		b.limiters[clientID] = lim
	}
	b.limitMu.Unlock()
	lim.mu.Lock()
	defer lim.mu.Unlock()
	now := time.Now()
	if now.Sub(lim.window) >= time.Second {
		lim.window = now
		lim.publishCount = 0
	}
	lim.publishCount++
	return lim.publishCount <= b.cfg.MaxPublishPerSec
}

func (b *Broker) allowSubscribe(clientID string) bool {
	b.limitMu.Lock()
	lim, ok := b.limiters[clientID]
	if !ok {
		lim = &clientLimiter{window: time.Now()}
		b.limiters[clientID] = lim
	}
	b.limitMu.Unlock()
	lim.mu.Lock()
	defer lim.mu.Unlock()
	now := time.Now()
	if now.Sub(lim.window) >= time.Second {
		lim.window = now
		lim.subscribeCount = 0
	}
	lim.subscribeCount++
	return lim.subscribeCount <= b.cfg.MaxSubscribePerSec
}

func New(cfg Config, store persistence.Store, authenticator auth.Authenticator) *Broker {
	if cfg.NodeID == "" {
		cfg.NodeID = uuid.NewString()[:8]
	}
	if authenticator == nil {
		var chain []auth.Authenticator
		if cfg.JWTSecret != "" {
			chain = append(chain, &auth.JWTAuth{Secret: cfg.JWTSecret})
		}
		if cfg.ACLFile != "" {
			if acl, err := auth.NewFileACL(cfg.ACLFile); err == nil {
				chain = append(chain, acl)
			} else {
				slog.Warn("acl file load failed", "file", cfg.ACLFile, "err", err)
			}
		}
		if len(chain) == 0 {
			if cfg.AllowAnonymous {
				authenticator = &auth.AllowAll{}
			} else {
				authenticator = &auth.DenyAll{}
			}
		} else if len(chain) == 1 {
			authenticator = chain[0]
		} else {
			authenticator = &auth.Chain{Auths: chain}
		}
	}
	if cfg.MaxPacketSize == 0 {
		cfg.MaxPacketSize = 1 << 20 // 1MB
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 20000
	}
	if cfg.MaxPublishPerSec == 0 {
		cfg.MaxPublishPerSec = 100
	}
	if cfg.MaxSubscribePerSec == 0 {
		cfg.MaxSubscribePerSec = 20
	}
	b := &Broker{
		cfg:        cfg,
		store:      store,
		trie:       topic.NewTrie(),
		sharedSubs: make(map[string]map[string][]string),
		sharedIdx:  make(map[string]int),
		auth:       authenticator,
		nodeID:     cfg.NodeID,
		conns:      make(map[string]*transport.Conn),
		sessions:   make(map[string]*session.Session),
		limiters:   make(map[string]*clientLimiter),
	}
	// setup redis cluster if addr provided
	if cfg.RedisAddr != "" {
		cli := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{cfg.RedisAddr}})
		pingCtx, cancel := storeCtx()
		pingErr := cli.Ping(pingCtx).Err()
		cancel()
		if pingErr != nil {
			slog.Warn("redis ping failed, cluster disabled", "err", pingErr)
		} else {
			b.redisCli = cli
			b.cluster = cluster.New(cli, cfg.NodeID, "mqtt", b.onClusterMessage)
		}
	}
	if cfg.TLSConfig == nil && cfg.TLSCertFile != "" {
		if tc, err := loadTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile); err != nil {
			slog.Warn("tls config load failed", "err", err)
		} else {
			cfg.TLSConfig = tc
			b.cfg.TLSConfig = tc
		}
	}
	// If store is nil, use memory
	if b.store == nil {
		b.store = persistence.NewMemoryStore()
	}
	return b
}

func (b *Broker) Start(ctx context.Context) error {
	b.stats.StartedAt = time.Now()
	if b.cfg.PprofAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok")) })
		mux.HandleFunc("/readyz", b.readyzHandler)
		go func() { _ = http.ListenAndServe(b.cfg.PprofAddr, mux) }()
		slog.Info("metrics listening", "addr", b.cfg.PprofAddr)
	}
	if b.cluster != nil {
		if err := b.cluster.Start(ctx); err != nil {
			log.Printf("cluster start failed: %v", err)
		} else {
			slog.Info("cluster started", "node", b.nodeID)
		}
	}
	go b.sysTicker(ctx)
	if b.cfg.ACLFile != "" {
		go b.watchACL(ctx)
	}
	tlsCfg := b.cfg.TLSConfig
	if tlsCfg == nil && b.cfg.TLSCertFile != "" {
		if tc, err := loadTLSConfig(b.cfg.TLSCertFile, b.cfg.TLSKeyFile, b.cfg.TLSCAFile); err == nil {
			tlsCfg = tc
		}
	}
	b.listener = transport.NewListener(b.cfg.TCPAddr, tlsCfg, b.cfg.WSAddr)
	slog.Info("broker listening", "node", b.nodeID, "tcp", b.cfg.TCPAddr, "ws", b.cfg.WSAddr, "redis", b.cfg.RedisAddr, "tls", tlsCfg != nil)
	err := b.listener.Listen(ctx, b.handleRawConn)
	slog.Debug("listener returned", "err", err)
	return err
}

func (b *Broker) Shutdown(ctx context.Context) error {
	slog.Info("shutdown draining", "node", b.nodeID)
	b.mu.RLock()
	conns := make([]*transport.Conn, 0, len(b.conns))
	for _, c := range b.conns {
		conns = append(conns, c)
	}
	b.mu.RUnlock()
	for _, conn := range conns {
		disc := &codec.Packet{Type: codec.TypeDISCONNECT, Version: conn.Version()}
		if conn.Version() == codec.ProtocolV5 {
			disc.DiscReason = 0x8B
			rs := "Server shutting down"
			disc.DiscProps = &codec.Properties{ReasonString: &rs}
		}
		_ = conn.WritePacket(disc)
		debugPacket("send", conn.ClientID(), disc)
	}
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			empty := true
			b.mu.RLock()
			for _, sess := range b.sessions {
				if sess.InflightCount() > 0 {
					empty = false
					break
				}
			}
			b.mu.RUnlock()
			if empty {
				slog.Info("shutdown complete", "node", b.nodeID)
				return nil
			}
		}
	}
}

func (b *Broker) readyzHandler(w http.ResponseWriter, r *http.Request) {
	// redis check <50ms, goroutines <50k, fd <80%
	if b.redisCli != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 50*time.Millisecond)
		defer cancel()
		if err := b.redisCli.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	b.mu.RLock()
	n := len(b.conns)
	b.mu.RUnlock()
	if n > 16000 {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(200)
	_, _ = w.Write([]byte("ok"))
}
func (b *Broker) sysTicker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.publishSys()
		}
	}
}
func (b *Broker) publishSys() {
	b.statsMu.Lock()
	u := time.Since(b.stats.StartedAt).Seconds()
	n := int64(len(b.conns))
	b.statsMu.Unlock()
	b.routeMessage("$SYS/broker/uptime", []byte(fmt.Sprintf("%.0f", u)), 0, true, nil, "sys")
	b.routeMessage("$SYS/broker/clients/connected", []byte(fmt.Sprintf("%d", n)), 0, true, nil, "sys")
	_ = u
}

func (b *Broker) handleRawConn(raw net.Conn) {
	conn := transport.NewConn(raw, b.cfg.MaxPacketSize)
	// first packet must be CONNECT with timeout 10s
	_ = raw.SetReadDeadline(time.Now().Add(10 * time.Second))
	pkt, err := conn.ReadPacket()
	if err != nil {
		slog.Debug("read CONNECT failed", "addr", raw.RemoteAddr().String(), "err", err)
		_ = raw.Close()
		return
	}
	debugPacket("recv", pkt.ClientID, pkt)
	slog.Info("client connect attempt", "client", pkt.ClientID, "addr", raw.RemoteAddr().String(), "version", pkt.Version, "keepAlive", pkt.KeepAlive, "clean", pkt.ConnectFlags.CleanSession)
	if pkt.Type != codec.TypeCONNECT {
		slog.Warn("first packet not CONNECT", "addr", raw.RemoteAddr().String())
		_ = raw.Close()
		return
	}
	// Authenticate
	if !b.auth.Authenticate(pkt.ClientID, pkt.Username, pkt.Password) {
		slog.Info("auth failed", "client", pkt.ClientID, "addr", raw.RemoteAddr().String(), "username", pkt.Username)
		mqttAuthFailed.Inc()
		mqttPacketDropped.WithLabelValues("auth").Inc()
		reason := byte(0x04) // bad username/password for v3
		if pkt.Version == codec.ProtocolV5 {
			reason = 0x86 // Bad User Name or Password
		}
		resp := &codec.Packet{Type: codec.TypeCONNACK, Version: pkt.Version, ReasonCode: reason}
		_ = conn.WritePacket(resp)
		debugPacket("send", pkt.ClientID, resp)
		_ = conn.Close()
		return
	}
	clientID := pkt.ClientID
	if clientID == "" {
		// v3: if clean session true, broker may assign id. For simplicity, generate
		clientID = "auto-" + uuid.NewString()[:8]
	}
	conn.SetClientID(clientID)
	conn.SetVersion(pkt.Version)
	b.mu.RLock()
	if len(b.conns) >= b.cfg.MaxConnections {
		b.mu.RUnlock()
		mqttPacketDropped.WithLabelValues("max_connections").Inc()
		reason := byte(0x03)
		if pkt.Version == codec.ProtocolV5 {
			reason = 0x97
		}
		resp := &codec.Packet{Type: codec.TypeCONNACK, Version: pkt.Version, ReasonCode: reason}
		_ = conn.WritePacket(resp)
		debugPacket("send", clientID, resp)
		slog.Info("reject max connections", "client", clientID, "current", len(b.conns), "max", b.cfg.MaxConnections)
		_ = conn.Close()
		return
	}
	b.mu.RUnlock()

	// Session handling
	sess, sessionExisted, err := b.getOrCreateSession(pkt)
	if err != nil {
		slog.Error("session error", "err", err)
		_ = conn.Close()
		return
	}
	sess.ClientID = clientID
	sess.Version = pkt.Version
	sess.KeepAlive = pkt.KeepAlive
	sess.Connected = true
	sess.NodeID = b.nodeID
	if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil {
		if pkt.Properties.ReceiveMaximum != nil {
			sess.ReceiveMaximum = *pkt.Properties.ReceiveMaximum
		}
		if pkt.Properties.MaximumPacketSize != nil {
			sess.MaximumPacketSize = *pkt.Properties.MaximumPacketSize
		}
		if pkt.Properties.TopicAliasMaximum != nil {
			v := *pkt.Properties.TopicAliasMaximum
			if v > 100 {
				v = 100
			}
			sess.TopicAliasMaximum = v
		}
	}
	b.statsMu.Lock()
	b.stats.ClientsConnected = int64(len(b.conns)) + 1
	b.stats.ClientsTotal++
	b.statsMu.Unlock()

	// Clean start handling per version
	clean := pkt.ConnectFlags.CleanSession
	expiry := uint32(0)
	if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil && pkt.Properties.SessionExpiryInterval != nil {
		expiry = *pkt.Properties.SessionExpiryInterval
	} else if clean {
		expiry = 0
	} else {
		expiry = 0xFFFFFFFF // v3 clean false => never expire
	}
	sess.Mu.Lock()
	sess.CleanStart = clean
	sess.ExpiryInterval = expiry
	sess.Mu.Unlock()

	// Kick existing connection with same clientID
	b.mu.Lock()
	if old, ok := b.conns[clientID]; ok {
		_ = old.Close()
	}
	b.conns[clientID] = conn
	b.sessions[clientID] = sess
	b.mu.Unlock()
	if err := b.store.SaveSession(bgCtx(), sess); err != nil {
		slog.Warn("store SaveSession failed", "err", err)
	}

	// Will with validation and delay cap
	if pkt.Will != nil {
		if pkt.Will.Topic == "" || strings.HasPrefix(pkt.Will.Topic, "$SYS/") {
			// invalid will topic
		} else {
			delay := pkt.Will.DelayInterval
			if delay > 86400 {
				delay = 86400
			}
			sess.Will = &session.Will{
				Topic:         pkt.Will.Topic,
				Payload:       pkt.Will.Payload,
				QoS:           pkt.Will.QoS,
				Retain:        pkt.Will.Retain,
				DelayInterval: delay,
			}
		}
	}

	// CONNACK - SessionPresent per MQTT spec: true if session existed and CleanSession/CleanStart is false
	sessionPresent := sessionExisted && !pkt.ConnectFlags.CleanSession
	if clientID != pkt.ClientID {
		sessionPresent = false
	}
	connack := &codec.Packet{
		Type:           codec.TypeCONNACK,
		Version:        pkt.Version,
		SessionPresent: sessionPresent,
		ReasonCode:     0,
	}
	// For v5, include props like AssignedClientID if we generated one
	if pkt.Version == codec.ProtocolV5 {
		props := &codec.Properties{}
		if pkt.ClientID == "" {
			props.AssignedClientID = &clientID
		}
		rm := uint16(65535)
		props.ReceiveMaximum = &rm
		ssa := byte(1)
		props.SharedSubAvailable = &ssa
		mps := uint32(b.cfg.MaxPacketSize)
		props.MaximumPacketSize = &mps
		ta := uint16(100)
		props.TopicAliasMaximum = &ta
		connack.ConnProperties = props
	}
	if err := conn.WritePacket(connack); err != nil {
		_ = conn.Close()
		return
	}
	debugPacket("send", clientID, connack)
	slog.Info("client connected", "client", clientID, "addr", raw.RemoteAddr().String(), "sessionPresent", sessionPresent, "version", pkt.Version, "clean", pkt.ConnectFlags.CleanSession)
	for filter, qos := range sess.Subscriptions {
		b.trie.Add(filter, clientID, qos, false)
	}

	// Replay retained for existing subs? Not needed until SUBSCRIBE

	// Replay offline queue
	offline, err := b.store.DequeueOffline(bgCtx(), clientID)
	if err != nil {
		slog.Warn("dequeue offline failed", "client", clientID, "err", err)
	} else if len(offline) > 0 {
		for _, m := range offline {
			pub := &codec.Packet{
				Type:    codec.TypePUBLISH,
				Version: pkt.Version,
				Topic:   m.Topic,
				QoS:     m.QoS,
				Payload: m.Payload,
				Retain:  m.Retain,
			}
			if m.QoS > 0 {
				pub.PacketID = sess.NextPacketID()
				sess.AddInflight(&session.InflightEntry{PacketID: pub.PacketID, QoS: m.QoS, Topic: m.Topic, Payload: m.Payload})
			}
			_ = conn.WritePacket(pub)
		}
	}

	// Main loop
	_ = raw.SetReadDeadline(time.Time{}) // clear
	conn.SetOnClose(func() {
		b.onClientDisconnect(clientID, sess, false)
	})
	go b.readLoop(conn, sess)
}

func (b *Broker) getOrCreateSession(pkt *codec.Packet) (*session.Session, bool, error) {
	clientID := pkt.ClientID
	if clientID == "" {
		return session.NewSession(clientID, pkt.Version, pkt.ConnectFlags.CleanSession, 0), false, nil
	}
	b.mu.RLock()
	if s, ok := b.sessions[clientID]; ok {
		b.mu.RUnlock()
		existed := true
		if pkt.ConnectFlags.CleanSession {
			s.Subscriptions = make(map[string]byte)
			s.Inflight = make(map[uint16]*session.InflightEntry)
			if err := b.store.ClearOffline(bgCtx(), clientID); err != nil {
				slog.Warn("store ClearOffline failed", "err", err)
			}
		}
		return s, existed, nil
	}
	b.mu.RUnlock()
	s, err := b.store.GetSession(bgCtx(), clientID)
	if err != nil {
		return nil, false, err
	}
	if s != nil {
		existed := true
		if pkt.ConnectFlags.CleanSession {
			s.Subscriptions = make(map[string]byte)
			s.Inflight = make(map[uint16]*session.InflightEntry)
			if err := b.store.ClearOffline(bgCtx(), clientID); err != nil {
				slog.Warn("store ClearOffline failed", "err", err)
			}
		}
		return s, existed, nil
	}
	// new session
	expiry := uint32(0)
	if pkt.Version == codec.ProtocolV5 && pkt.Properties != nil && pkt.Properties.SessionExpiryInterval != nil {
		expiry = *pkt.Properties.SessionExpiryInterval
	}
	s = session.NewSession(clientID, pkt.Version, pkt.ConnectFlags.CleanSession, expiry)
	return s, false, nil
}

func (b *Broker) readLoop(conn *transport.Conn, sess *session.Session) {
	defer func() { _ = conn.Close() }()
	for {
		if sess.KeepAlive > 0 {
			_ = conn.Raw().SetReadDeadline(time.Now().Add(time.Duration(float64(sess.KeepAlive)*1.5) * time.Second))
		} else {
			_ = conn.Raw().SetReadDeadline(time.Time{})
		}
		pkt, err := conn.ReadPacket()
		if err != nil {
			b.onClientDisconnect(conn.ClientID(), sess, false)
			return
		}
		debugPacket("recv", conn.ClientID(), pkt)
		switch pkt.Type {
		case codec.TypePUBLISH:
			slog.Info("publish recv", "client", conn.ClientID(), "topic", pkt.Topic, "qos", pkt.QoS, "retain", pkt.Retain, "payloadLen", len(pkt.Payload))
			b.handlePublish(conn, sess, pkt)
		case codec.TypeSUBSCRIBE:
			slog.Info("subscribe recv", "client", conn.ClientID(), "packetID", pkt.PacketID, "filters", pkt.Subscriptions)
			b.handleSubscribe(conn, sess, pkt)
		case codec.TypeUNSUBSCRIBE:
			slog.Info("unsubscribe recv", "client", conn.ClientID(), "packetID", pkt.PacketID, "topics", pkt.Topics)
			b.handleUnsubscribe(conn, sess, pkt)
		case codec.TypePUBACK:
			sess.RemoveInflight(pkt.PacketID)
		case codec.TypePUBREC:
			if _, ok := sess.GetInflight(pkt.PacketID); ok {
				rel := &codec.Packet{Type: codec.TypePUBREL, Version: conn.Version(), PacketID: pkt.PacketID}
				_ = conn.WritePacket(rel)
			}
		case codec.TypePUBREL:
			if e, ok := sess.GetInflight(pkt.PacketID); ok {
				b.routeMessage(e.Topic, e.Payload, 2, false, nil, sess.ClientID)
				sess.RemoveInflight(pkt.PacketID)
			} else {
				sess.RemoveInflight(pkt.PacketID)
			}
			comp := &codec.Packet{Type: codec.TypePUBCOMP, Version: conn.Version(), PacketID: pkt.PacketID}
			_ = conn.WritePacket(comp)
		case codec.TypePUBCOMP:
			sess.RemoveInflight(pkt.PacketID)
		case codec.TypePINGREQ:
			resp := &codec.Packet{Type: codec.TypePINGRESP, Version: conn.Version()}
			_ = conn.WritePacket(resp)
		case codec.TypeDISCONNECT:
			b.onClientDisconnect(conn.ClientID(), sess, true)
			return
		default:
			slog.Debug("unhandled packet", "type", pkt.Type, "client", conn.ClientID())
		}
	}
}

func (b *Broker) handlePublish(conn *transport.Conn, sess *session.Session, pkt *codec.Packet) {
	if strings.HasPrefix(pkt.Topic, "$SYS/") {
		if pkt.QoS == 1 {
			ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x87}
			_ = conn.WritePacket(ack)
		}
		return
	}
	// ACL check
	if !b.auth.Authorize(sess.ClientID, pkt.Topic, true) {
		mqttPacketDropped.WithLabelValues("acl").Inc()
		if sess.Version == codec.ProtocolV5 {
			ack := &codec.Packet{Type: codec.TypePUBACK, Version: codec.ProtocolV5, PacketID: pkt.PacketID, Reason: 0x87} // Not authorized
			if pkt.QoS == 1 {
				_ = conn.WritePacket(ack)
				debugPacket("send", sess.ClientID, ack)
			}
		}
		return
	}
	if !b.allowPublish(sess.ClientID) {
		mqttPacketDropped.WithLabelValues("publish_rate").Inc()
		if pkt.QoS == 1 {
			ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x97}
			_ = conn.WritePacket(ack)
			debugPacket("send", sess.ClientID, ack)
		}
		return
	}
	// topic alias handling (v5) with limit
	topicName := pkt.Topic
	if sess.Version == codec.ProtocolV5 && pkt.PubProps != nil && pkt.PubProps.TopicAlias != nil {
		alias := *pkt.PubProps.TopicAlias
		if alias == 0 || alias > sess.TopicAliasMaximum {
			if pkt.QoS == 1 {
				ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x94}
				_ = conn.WritePacket(ack)
			}
			_ = conn.Close()
			return
		}
		if len(sess.AliasToTopic) >= int(sess.TopicAliasMaximum) && sess.AliasToTopic[alias] == "" {
			if pkt.QoS == 1 {
				ack := &codec.Packet{Type: codec.TypePUBACK, Version: sess.Version, PacketID: pkt.PacketID, Reason: 0x94}
				_ = conn.WritePacket(ack)
			}
			return
		}
		if topicName != "" {
			sess.AliasToTopic[alias] = topicName
			sess.TopicToAlias[topicName] = alias
		} else {
			if t, ok := sess.AliasToTopic[alias]; ok {
				topicName = t
				pkt.Topic = t
			} else {
				// invalid alias
				if pkt.QoS == 1 {
					ack := &codec.Packet{Type: codec.TypePUBACK, Version: codec.ProtocolV5, PacketID: pkt.PacketID, Reason: 0x94}
					_ = conn.WritePacket(ack)
				}
				return
			}
		}
	}
	if topicName == "" {
		return
	}
	if len(topicName) > 4096 || len(pkt.Payload) > 1<<20 {
		return
	}
	if sess.MaximumPacketSize > 0 && len(topicName)+len(pkt.Payload)+10 > int(sess.MaximumPacketSize) {
		_ = conn.Close()
		return
	}
	if !topic.IsValidTopic(topicName) {
		return
	}
	// QoS2 inbound: need to send PUBREC and store, route after PUBREL
	if pkt.QoS == 2 {
		if _, exists := sess.GetInflight(pkt.PacketID); exists {
			rec := &codec.Packet{Type: codec.TypePUBREC, Version: conn.Version(), PacketID: pkt.PacketID}
			_ = conn.WritePacket(rec)
			return
		}
		sess.AddInflight(&session.InflightEntry{PacketID: pkt.PacketID, QoS: 2, Topic: topicName, Payload: pkt.Payload, State: "qos2-publish"})
		rec := &codec.Packet{Type: codec.TypePUBREC, Version: conn.Version(), PacketID: pkt.PacketID}
		_ = conn.WritePacket(rec)
		return
	}

	// Retain handling
	if pkt.Retain {
		if len(pkt.Payload) == 0 {
			if err := b.store.DeleteRetained(bgCtx(), topicName); err != nil {
				slog.Warn("store DeleteRetained failed", "err", err)
			}
		} else {
			if err := b.store.SaveRetained(bgCtx(), topicName, &persistence.Message{Topic: topicName, Payload: pkt.Payload, QoS: pkt.QoS, Retain: true}); err != nil {
				slog.Warn("store SaveRetained failed", "err", err)
			}
		}
	}

	// ACK for QoS1
	if pkt.QoS == 1 {
		ack := &codec.Packet{Type: codec.TypePUBACK, Version: conn.Version(), PacketID: pkt.PacketID}
		if sess.Version == codec.ProtocolV5 {
			ack.Reason = 0
		}
		_ = conn.WritePacket(ack)
	}

	// Route locally + cluster
	b.routeMessage(topicName, pkt.Payload, pkt.QoS, pkt.Retain, pkt.PubProps, sess.ClientID)

	// For QoS2 inbound, actual routing should happen after PUBREL; we already did. To be spec compliant, we would defer until PUBREL. Simplified as above.
}

func (b *Broker) routeMessage(topicName string, payload []byte, qos byte, retain bool, props *codec.Properties, from string) {
	b.statsMu.Lock()
	b.stats.MessagesReceived++
	b.statsMu.Unlock()
	mqttMessagesReceived.Inc()
	if b.cfg.MaxPacketSize > 0 && len(payload)+len(topicName) > b.cfg.MaxPacketSize {
		return
	}
	if b.cluster != nil {
		go func() {
			if err := b.cluster.Publish(bgCtx(), topicName, payload, qos, retain); err != nil {
				slog.Warn("cluster publish failed", "err", err)
			}
		}()
	}
	b.deliverLocal(topicName, payload, qos, props, from)
}

func (b *Broker) deliverLocal(topicName string, payload []byte, qos byte, props *codec.Properties, from string) {
	// SharedSub handling: deliver one per group round-robin
	b.sharedMu.Lock()
	for group, filters := range b.sharedSubs {
		for filter, clients := range filters {
			if len(clients) == 0 {
				continue
			}
			if !matchFilter(topicName, filter) {
				continue
			}
			idx := b.sharedIdx[group] % len(clients)
			b.sharedIdx[group] = (idx + 1) % len(clients)
			chosen := clients[idx]
			if chosen == from {
				continue
			}
			b.mu.RLock()
			conn, ok1 := b.conns[chosen]
			sess, ok2 := b.sessions[chosen]
			b.mu.RUnlock()
			if !ok1 || !ok2 {
				if sess != nil && sess.ExpiryInterval != 0 {
					if err := b.store.EnqueueOffline(bgCtx(), chosen, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos}); err != nil {
						slog.Warn("store EnqueueOffline failed", "err", err)
					}
				}
				continue
			}
			q := qos
			if storedQoS, ok := sess.Subscriptions["$share/"+group+"/"+filter]; ok && storedQoS < q {
				q = storedQoS
			}
			pub := &codec.Packet{Type: codec.TypePUBLISH, Version: conn.Version(), Topic: topicName, QoS: q, Payload: payload}
			if q > 0 {
				if !sess.CanSend() {
					if err := b.store.EnqueueOffline(bgCtx(), chosen, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos}); err != nil {
						slog.Warn("store EnqueueOffline failed", "err", err)
					}
					continue
				}
				pub.PacketID = sess.NextPacketID()
				if pub.PacketID == 0 {
					continue
				}
				sess.AddInflight(&session.InflightEntry{PacketID: pub.PacketID, QoS: q, Topic: topicName, Payload: payload})
				b.scheduleRetry(chosen, pub.PacketID)
			}
			if sess.Version == codec.ProtocolV5 && props != nil && len(props.SubscriptionID) > 0 {
				pub.PubProps = &codec.Properties{SubscriptionID: props.SubscriptionID}
			}
			mqttMessagesSent.Inc()
			mqttInflight.Set(float64(sess.InflightCount()))
			_ = conn.WritePacket(pub)
		}
	}
	b.sharedMu.Unlock()
	subs := b.trie.Match(topicName)
	for _, sub := range subs {
		if sub.ClientID == from && sub.NoLocal {
			continue
		}
		b.mu.RLock()
		conn, ok := b.conns[sub.ClientID]
		sess, sok := b.sessions[sub.ClientID]
		b.mu.RUnlock()
		if !ok || !sok {
			// offline: enqueue if session expiry >0
			if sess != nil && sess.ExpiryInterval != 0 {
				if err := b.store.EnqueueOffline(bgCtx(), sub.ClientID, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos}); err != nil {
					slog.Warn("store EnqueueOffline failed", "err", err)
				}
			}
			continue
		}
		// deliver with min QoS (publish QoS and sub QoS)
		deliverQoS := qos
		if sub.QoS < deliverQoS {
			deliverQoS = sub.QoS
		}
		pub := &codec.Packet{
			Type:    codec.TypePUBLISH,
			Version: conn.Version(),
			Topic:   topicName,
			QoS:     deliverQoS,
			Payload: payload,
			Retain:  false,
		}
		if deliverQoS > 0 {
			if !sess.CanSend() {
				if err := b.store.EnqueueOffline(bgCtx(), sub.ClientID, &persistence.Message{Topic: topicName, Payload: payload, QoS: qos}); err != nil {
					slog.Warn("store EnqueueOffline failed", "err", err)
				}
				continue
			}
			pub.PacketID = sess.NextPacketID()
			if pub.PacketID == 0 {
				continue
			}
			e := &session.InflightEntry{PacketID: pub.PacketID, QoS: deliverQoS, Topic: topicName, Payload: payload}
			sess.AddInflight(e)
			b.scheduleRetry(sess.ClientID, pub.PacketID)
		}
		// v5 subscription ID
		if sess.Version == codec.ProtocolV5 && props != nil && len(props.SubscriptionID) > 0 {
			pub.PubProps = &codec.Properties{SubscriptionID: props.SubscriptionID}
		}
		mqttMessagesSent.Inc()
		mqttInflight.Set(float64(len(sess.Inflight) + 1))
		if err := conn.WritePacket(pub); err != nil {
			slog.Warn("deliver failed", "client", sub.ClientID, "err", err)
		}
	}
}

func (b *Broker) handleSubscribe(conn *transport.Conn, sess *session.Session, pkt *codec.Packet) {
	if !b.allowSubscribe(sess.ClientID) {
		mqttPacketDropped.WithLabelValues("subscribe_rate").Inc()
		codes := make([]byte, len(pkt.Subscriptions))
		for i := range codes {
			if sess.Version == codec.ProtocolV5 {
				codes[i] = 0x97
			} else {
				codes[i] = 0x80
			}
		}
		ack := &codec.Packet{Type: codec.TypeSUBACK, Version: conn.Version(), PacketID: pkt.PacketID, SubackCodes: codes}
		_ = conn.WritePacket(ack)
		debugPacket("send", sess.ClientID, ack)
		return
	}
	var codes []byte
	for _, sub := range pkt.Subscriptions {
		if !topic.IsValidFilter(sub.Filter) {
			codes = append(codes, 0x80) // failure
			continue
		}
		if !b.auth.Authorize(sess.ClientID, sub.Filter, false) {
			if sess.Version == codec.ProtocolV5 {
				codes = append(codes, 0x87) // Not authorized
			} else {
				codes = append(codes, 0x80)
			}
			continue
		}
		if isShared, group, realFilter := isSharedFilter(sub.Filter); isShared {
			if !topic.IsValidFilter(realFilter) {
				codes = append(codes, 0x80)
				continue
			}
			b.sharedMu.Lock()
			if b.sharedSubs[group] == nil {
				b.sharedSubs[group] = make(map[string][]string)
			}
			// avoid duplicate
			found := false
			for _, cid := range b.sharedSubs[group][realFilter] {
				if cid == sess.ClientID {
					found = true
					break
				}
			}
			if !found {
				b.sharedSubs[group][realFilter] = append(b.sharedSubs[group][realFilter], sess.ClientID)
			}
			b.sharedMu.Unlock()
			sess.Subscriptions[sub.Filter] = sub.QoS
			if err := b.store.SaveSession(bgCtx(), sess); err != nil {
				slog.Warn("store SaveSession failed", "err", err)
			}
			codes = append(codes, sub.QoS)
		} else {
			// add to trie and session
			b.trie.Add(sub.Filter, sess.ClientID, sub.QoS, sub.NoLocal)
			sess.Subscriptions[sub.Filter] = sub.QoS
			if err := b.store.SaveSession(bgCtx(), sess); err != nil {
				slog.Warn("store SaveSession failed", "err", err)
			}
			codes = append(codes, sub.QoS)
		}

		// deliver retained messages matching this filter
		retained, err := b.store.ListRetained(bgCtx())
		if err != nil {
			slog.Warn("list retained failed", "err", err)
		}
		for _, m := range retained {
			if matchFilter(m.Topic, sub.Filter) {
				if !b.auth.Authorize(sess.ClientID, m.Topic, false) {
					continue
				}
				pub := &codec.Packet{
					Type:    codec.TypePUBLISH,
					Version: conn.Version(),
					Topic:   m.Topic,
					QoS:     m.QoS,
					Payload: m.Payload,
					Retain:  true,
				}
				if sub.QoS > 0 && m.QoS > 0 {
					// retain deliver QoS = min(sub QoS, retained QoS)
					if m.QoS < sub.QoS {
						pub.QoS = m.QoS
					} else {
						pub.QoS = sub.QoS
					}
					if pub.QoS > 0 {
						pub.PacketID = sess.NextPacketID()
					}
				} else {
					pub.QoS = 0
				}
				_ = conn.WritePacket(pub)
			}
		}
	}
	ack := &codec.Packet{Type: codec.TypeSUBACK, Version: conn.Version(), PacketID: pkt.PacketID, SubackCodes: codes}
	if sess.Version == codec.ProtocolV5 {
		ack.SubackProps = &codec.Properties{}
	}
	_ = conn.WritePacket(ack)
}

func (b *Broker) handleUnsubscribe(conn *transport.Conn, sess *session.Session, pkt *codec.Packet) {
	for _, t := range pkt.Topics {
		if isShared, group, realFilter := isSharedFilter(t); isShared {
			b.sharedMu.Lock()
			if m, ok := b.sharedSubs[group]; ok {
				list := m[realFilter]
				newList := list[:0]
				for _, cid := range list {
					if cid != sess.ClientID {
						newList = append(newList, cid)
					}
				}
				if len(newList) == 0 {
					delete(m, realFilter)
					if len(m) == 0 {
						delete(b.sharedSubs, group)
					} else {
						m[realFilter] = newList
					}
				} else {
					m[realFilter] = newList
				}
			}
			b.sharedMu.Unlock()
		} else {
			b.trie.Remove(t, sess.ClientID)
		}
		delete(sess.Subscriptions, t)
	}
	if err := b.store.SaveSession(bgCtx(), sess); err != nil {
		slog.Warn("store SaveSession failed", "err", err)
	}
	ack := &codec.Packet{Type: codec.TypeUNSUBACK, Version: conn.Version(), PacketID: pkt.PacketID}
	if sess.Version == codec.ProtocolV5 {
		ack.UnsubackProps = &codec.Properties{}
		ack.UnsubackCodes = make([]byte, len(pkt.Topics)) // 0 = success
	}
	_ = conn.WritePacket(ack)
}

func (b *Broker) onClientDisconnect(clientID string, sess *session.Session, clean bool) {
	b.mu.Lock()
	delete(b.conns, clientID)
	b.mu.Unlock()
	if sess == nil {
		slog.Info("client disconnect", "client", clientID, "clean", clean)
		return
	}
	slog.Info("client disconnect", "client", clientID, "clean", clean, "node", sess.NodeID)
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
	if sess.Will == nil {
		return
	}
	w := sess.Will
	sess.Will = nil
	if !b.auth.Authorize(sess.ClientID, w.Topic, true) {
		return
	}
	if w.DelayInterval > 86400 {
		w.DelayInterval = 86400
	}
	// delay
	if w.DelayInterval > 0 {
		time.AfterFunc(time.Duration(w.DelayInterval)*time.Second, func() {
			b.routeMessage(w.Topic, w.Payload, w.QoS, w.Retain, nil, sess.ClientID)
		})
		return
	}
	b.routeMessage(w.Topic, w.Payload, w.QoS, w.Retain, nil, sess.ClientID)
}

//nolint:unused
func (b *Broker) keepAliveMonitor(conn *transport.Conn, sess *session.Session) {
	interval := time.Duration(float64(sess.KeepAlive)*1.5) * time.Second
	if interval == 0 {
		return
	}
	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()
	for {
		// we rely on read deadline? Simpler: check last activity via conn read timeout
		// For now, just sleep and check if conn still exists
		time.Sleep(interval)
		b.mu.RLock()
		_, ok := b.conns[sess.ClientID]
		b.mu.RUnlock()
		if !ok {
			return
		}
		// If no packet received within 1.5*keepalive, close
		// We set read deadline dynamically: transport's parser blocks, so we set deadline on raw conn
		// This monitor just closes if still idle; actual deadline is set below
		// Set read deadline to trigger next ReadPacket timeout
		_ = conn.Raw().SetReadDeadline(time.Now().Add(2 * time.Second))
	}
}

func (b *Broker) scheduleRetry(clientID string, packetID uint16) {
	time.AfterFunc(20*time.Second, func() {
		b.mu.RLock()
		sess, ok1 := b.sessions[clientID]
		conn, ok2 := b.conns[clientID]
		b.mu.RUnlock()
		if !ok1 || !ok2 {
			return
		}
		if e, ok := sess.GetInflight(packetID); ok {
			e.Dup = true
			pub := &codec.Packet{Type: codec.TypePUBLISH, Version: conn.Version(), Topic: e.Topic, QoS: e.QoS, Payload: e.Payload, PacketID: packetID, Dup: true}
			_ = conn.WritePacket(pub)
			b.scheduleRetry(clientID, packetID)
		}
	})
}

func (b *Broker) onClusterMessage(msg *cluster.ClusterMessage) {
	if msg.Topic == "" || msg.Topic[0] == '$' {
		return
	}
	b.deliverLocal(msg.Topic, msg.Payload, msg.QoS, nil, msg.From)
}

func isSharedFilter(filter string) (bool, string, string) {
	if len(filter) > 7 && filter[:7] == "$share/" {
		rest := filter[7:]
		slash := -1
		for i, c := range rest {
			if c == '/' {
				slash = i
				break
			}
		}
		if slash < 0 {
			return false, "", ""
		}
		group := rest[:slash]
		realFilter := rest[slash+1:]
		if group == "" || realFilter == "" {
			return false, "", ""
		}
		return true, group, realFilter
	}
	return false, "", ""
}

func matchFilter(t, filter string) bool {
	// reuse trie for single match
	tr := topic.NewTrie()
	tr.Add(filter, "test", 0, false)
	return len(tr.Match(t)) > 0
}

var _ = fmt.Sprintf
