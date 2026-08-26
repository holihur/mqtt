package broker

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"runtime"
	"time"

	"log/slog"
	"mqtt/internal/auth"
	"mqtt/internal/cluster"
	"mqtt/internal/codec"
	"mqtt/internal/hook"
	"mqtt/internal/persistence"
	"mqtt/internal/transport"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

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

func (b *Broker) ensureCluster() {
	if b.cluster != nil || b.cfg.RedisAddr == "" {
		return
	}
	cli := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{b.cfg.RedisAddr}})
	pingCtx, cancel := storeCtx()
	pingErr := cli.Ping(pingCtx).Err()
	cancel()
	if pingErr != nil {
		slog.Warn("redis ping failed, cluster disabled", "err", pingErr)
		return
	}
	b.redisCli = cli
	b.cluster = cluster.New(cli, b.nodeID, "mqtt", b.onClusterMessage)
	b.cluster.SetOnMeta(b.onClusterMeta)
}

func (b *Broker) ensureTLS() *tls.Config {
	if b.cfg.TLSConfig != nil {
		return b.cfg.TLSConfig
	}
	if b.cfg.TLSCertFile == "" {
		return nil
	}
	if tc, err := loadTLSConfig(b.cfg.TLSCertFile, b.cfg.TLSKeyFile, b.cfg.TLSCAFile); err != nil {
		slog.Warn("tls config load failed", "err", err)
		return nil
	} else {
		b.cfg.TLSConfig = tc
		return tc
	}
}

func (b *Broker) initStart(ctx context.Context) (context.Context, error) {
	b.runMu.Lock()
	if b.running {
		b.runMu.Unlock()
		return nil, fmt.Errorf("broker already running")
	}
	b.running = true
	b.runMu.Unlock()

	if b.store == nil {
		b.store = persistence.NewMemoryStore()
	}
	if b.auth == nil {
		b.auth = buildAuthenticator(b.cfg)
		if aa := hook.NewAuthAdapter(b.auth); aa != nil {
			b.hooks.Register(aa)
		}
	}
	b.cfg.ApplyDefaults()
	if b.nodeID == "" {
		b.nodeID = b.cfg.NodeID
		if b.nodeID == "" {
			b.nodeID = uuid.NewString()[:8]
			b.cfg.NodeID = b.nodeID
		}
	}
	b.stats.StartedAt = time.Now()
	b.ensureCluster()
	b.ensureTLS()

	runCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel

	if b.cfg.PprofAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok")) })
		mux.HandleFunc("/readyz", b.readyzHandler)
		srv := &http.Server{Addr: b.cfg.PprofAddr, Handler: mux}
		b.metricsSrv = srv
		go func() {
			_ = srv.ListenAndServe()
		}()
		slog.Info("metrics listening", "addr", b.cfg.PprofAddr)
		go func() {
			<-runCtx.Done()
			shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer shutCancel()
			_ = srv.Shutdown(shutCtx)
		}()
	}
	if b.cluster != nil {
		if err := b.cluster.Start(runCtx); err != nil {
			log.Printf("cluster start failed: %v", err)
		} else {
			slog.Info("cluster started", "node", b.nodeID)
		}
	}
	go b.sysTicker(runCtx)
	if b.cfg.ACLFile != "" {
		go b.watchACL(runCtx)
	}
	return runCtx, nil
}

func (b *Broker) Start(ctx context.Context) error {
	runCtx, err := b.initStart(ctx)
	if err != nil {
		return err
	}
	defer func() {
		b.runMu.Lock()
		b.running = false
		b.runMu.Unlock()
	}()

	needTCP := b.cfg.TCPAddr != "" || b.customListener != nil
	needWS := b.cfg.WSAddr != ""
	if !needTCP && !needWS {
		slog.Info("broker running in embedded mode without listeners", "node", b.nodeID)
		<-runCtx.Done()
		return nil
	}
	tlsCfg := b.cfg.TLSConfig
	b.listener = transport.NewListener(b.cfg.TCPAddr, tlsCfg, b.cfg.WSAddr)
	b.listener.SetWsAllowOrigins(b.cfg.WsAllowOrigins)
	if b.customListener != nil {
		b.listener.SetCustomListener(b.customListener)
	}
	slog.Info("broker listening", "node", b.nodeID, "tcp", b.cfg.TCPAddr, "ws", b.cfg.WSAddr, "redis", b.cfg.RedisAddr, "tls", tlsCfg != nil)
	err = b.listener.Listen(runCtx, b.handleRawConn)
	slog.Debug("listener returned", "err", err)
	return err
}

func (b *Broker) Run(ctx context.Context) error { return b.Start(ctx) }

func (b *Broker) StartAsync(ctx context.Context) error {
	runCtx, err := b.initStart(ctx)
	if err != nil {
		return err
	}
	needTCP := b.cfg.TCPAddr != "" || b.customListener != nil
	needWS := b.cfg.WSAddr != ""
	if !needTCP && !needWS {
		slog.Info("broker running in embedded mode without listeners (async)", "node", b.nodeID)
		return nil
	}
	tlsCfg := b.cfg.TLSConfig
	b.listener = transport.NewListener(b.cfg.TCPAddr, tlsCfg, b.cfg.WSAddr)
	b.listener.SetWsAllowOrigins(b.cfg.WsAllowOrigins)
	if b.customListener != nil {
		b.listener.SetCustomListener(b.customListener)
	}
	slog.Info("broker listening (async)", "node", b.nodeID, "tcp", b.cfg.TCPAddr, "ws", b.cfg.WSAddr, "redis", b.cfg.RedisAddr, "tls", tlsCfg != nil)
	ready := make(chan error, 1)
	go func() {
		err := b.listener.Listen(runCtx, b.handleRawConn)
		slog.Debug("listener returned (async)", "err", err)
		ready <- err
		b.runMu.Lock()
		b.running = false
		b.runMu.Unlock()
	}()
	select {
	case err := <-ready:
		return err
	case <-time.After(200 * time.Millisecond):
		return nil
	case <-runCtx.Done():
		return runCtx.Err()
	}
}

func (b *Broker) Stop(ctx context.Context) error {
	b.runMu.Lock()
	wasRunning := b.running
	b.runMu.Unlock()
	if b.cancel != nil {
		b.cancel()
	}
	if b.listener != nil {
		_ = b.listener.Close()
	}
	if b.metricsSrv != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.metricsSrv.Shutdown(shutCtx)
	}
	if b.cluster != nil {
		b.cluster.Stop()
	}
	var err error
	if wasRunning {
		err = b.Shutdown(ctx)
	}
	b.runMu.Lock()
	b.running = false
	b.runMu.Unlock()
	return err
}

func (b *Broker) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return b.Stop(ctx)
}

func (b *Broker) IsRunning() bool {
	b.runMu.Lock()
	defer b.runMu.Unlock()
	return b.running
}

func (b *Broker) Addr() string {
	if b.listener != nil {
		return b.listener.Addr()
	}
	if b.customListener != nil {
		return b.customListener.Addr().String()
	}
	return b.cfg.TCPAddr
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
		_ = b.sendPacket(conn, disc)
		b.debugPacket("send", conn.ClientID(), disc)
	}
	drain := 5 * time.Second
	if len(conns) == 0 {
		drain = 500 * time.Millisecond
	}
	select {
	case <-time.After(drain):
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
	start := time.Now()
	defer func() { mqttRedisLatency.Observe(time.Since(start).Seconds()) }()
	if b.redisCli != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 50*time.Millisecond)
		defer cancel()
		if err := b.redisCli.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	if runtime.NumGoroutine() > 50000 {
		http.Error(w, "too many goroutines", http.StatusServiceUnavailable)
		return
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
	u := time.Since(b.stats.StartedAt).Seconds()
	n := int64(len(b.conns))
	b.routeMessage("$SYS/broker/uptime", []byte(fmt.Sprintf("%.0f", u)), 0, true, nil, "sys")
	b.routeMessage("$SYS/broker/clients/connected", []byte(fmt.Sprintf("%d", n)), 0, true, nil, "sys")
	_ = u
}
