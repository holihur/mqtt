package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mqtt/internal/broker"
	"mqtt/internal/logger"
	"mqtt/internal/persistence"

	"github.com/redis/go-redis/v9"
)

func splitAddrs(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func main() {
	var (
		tcpAddr        = flag.String("tcp", ":1883", "TCP listen addr")
		wsAddr         = flag.String("ws", ":8083", "WebSocket listen addr (empty to disable)")
		redisAddr      = flag.String("redis", "127.0.0.1:6379", "Redis addr (comma-separated for cluster, empty to disable)")
		pprofAddr      = flag.String("pprof", "", "pprof listen addr (empty to disable, e.g. :6060)")
		aclFile        = flag.String("acl", "", "ACL file path (empty to disable)")
		jwtSecret      = flag.String("jwt-secret", "", "JWT HMAC secret (empty to disable)")
		allowAnonymous = flag.String("allow-anonymous", "false", "allow anonymous (true/false)")
		logLevel       = flag.String("log-level", "info", "log level: debug/info/warn/error (lower more verbose)")
		nodeID         = flag.String("node", "", "Node ID (auto if empty)")
		tlsCert        = flag.String("tls-cert", "", "TLS cert file (empty to disable TLS)")
		tlsKey         = flag.String("tls-key", "", "TLS key file")
		tlsCA          = flag.String("tls-ca", "", "TLS CA file for mTLS (empty to disable client auth)")
	)
	flag.Parse()
	logger.Init(*logLevel)
	slog.Info("starting", "log_level", *logLevel)

	var store persistence.Store
	var redisCli redis.UniversalClient
	if *redisAddr != "" {
		addrs := []string{*redisAddr}
		if len(*redisAddr) > 0 {
			// support comma-separated for cluster/sentinel
			addrs = splitAddrs(*redisAddr)
		}
		cli := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: addrs})
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		pingErr := cli.Ping(pingCtx).Err()
		cancel()
		if pingErr != nil {
			slog.Warn("redis unavailable, falling back to memory", "addr", *redisAddr, "err", pingErr)
			store = persistence.NewMemoryStore()
		} else {
			store = persistence.NewRedisStoreWithClient(cli, "mqtt")
			redisCli = cli
			slog.Info("using redis store", "addr", *redisAddr)
		}
	} else {
		store = persistence.NewMemoryStore()
	}
	if redisCli != nil {
		defer func() { _ = redisCli.Close() }()
	}

	allowAnon := *allowAnonymous == "true"
	cfg := broker.Config{
		NodeID:         *nodeID,
		TCPAddr:        *tcpAddr,
		WSAddr:         *wsAddr,
		RedisAddr:      *redisAddr,
		PprofAddr:      *pprofAddr,
		ACLFile:        *aclFile,
		JWTSecret:      *jwtSecret,
		AllowAnonymous: allowAnon,
		TLSCertFile:    *tlsCert,
		TLSKeyFile:     *tlsKey,
		TLSCAFile:      *tlsCA,
	}
	b := broker.New(cfg, store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		slog.Info("shutting down")
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutCancel()
		if err := b.Shutdown(shutCtx); err != nil {
			slog.Warn("shutdown", "err", err)
		}
		cancel()
		sig2 := make(chan os.Signal, 1)
		signal.Notify(sig2, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sig2
			slog.Warn("force exit")
			os.Exit(1)
		}()
	}()

	if err := b.Start(ctx); err != nil {
		slog.Error("broker error", "err", err)
		os.Exit(1)
	}
}
