package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mqtt/internal/broker"
	"mqtt/internal/hook"
	"mqtt/internal/logger"
	"mqtt/internal/persistence"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/redis/go-redis/v9"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
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
		tcpAddr               = flag.String("tcp", ":1883", "TCP listen addr")
		wsAddr                = flag.String("ws", ":8083", "WebSocket listen addr (empty to disable)")
		redisAddr             = flag.String("redis", "127.0.0.1:6379", "Redis addr (comma-separated for cluster, empty to disable)")
		pprofAddr             = flag.String("pprof", "", "pprof listen addr (empty to disable, e.g. :6060)")
		aclFile               = flag.String("acl", "", "ACL file path (empty to disable)")
		jwtSecret             = flag.String("jwt-secret", "", "JWT HMAC secret (empty to disable)")
		allowAnonymous        = flag.String("allow-anonymous", "false", "allow anonymous (true/false)")
		logLevel              = flag.String("log-level", "info", "log level: debug/info/warn/error (lower more verbose)")
		nodeID                = flag.String("node", "", "Node ID (auto if empty)")
		tlsCert               = flag.String("tls-cert", "", "TLS cert file (empty to disable TLS)")
		tlsKey                = flag.String("tls-key", "", "TLS key file")
		tlsCA                 = flag.String("tls-ca", "", "TLS CA file for mTLS (empty to disable client auth)")
		maxRetainMessages     = flag.Int("max-retain-messages", 10000, "max retained messages globally")
		maxRetainSize         = flag.Int64("max-retain-size", 1<<30, "max total retained size in bytes")
		maxRetainPerTopic     = flag.Int("max-retain-per-topic", 1000, "max retained messages per topic")
		maxRetainSizePerTopic = flag.Int64("max-retain-size-per-topic", 100<<20, "max retained size per topic in bytes")
		walDir                = flag.String("wal-dir", "./data/wal", "WAL dir (pebble), \"-\" to disable; any Store impl can be injected via WithStore")
		walEnabled            = flag.Bool("wal", true, "enable WAL (default true, uses Store interface, pebble is one impl)")
		wsAllowOrigins        = flag.String("ws-allow-origins", "", "WS allowed origins, comma separated or \"*\" for all; empty means same-origin + empty Origin only")
		showVersion           = flag.Bool("version", false, "print version and exit")
		msgPersistDSN         = flag.String("msg-persist-dsn", "", "SQL DSN for message persistence (empty to disable); postgres://... for PostgreSQL, a file path for SQLite")
		msgPersistTable       = flag.String("msg-persist-table", "mqtt_messages", "message persistence table name")
		msgPersistBatchSize   = flag.Int("msg-persist-batch-size", 1000, "messages per batch insert")
		msgPersistFlushInt    = flag.Duration("msg-persist-flush-interval", 5*time.Second, "max time between batch flushes")
		msgPersistQueueCap    = flag.Int("msg-persist-queue-capacity", 10000, "in-memory queue capacity before the DB worker")
		msgPersistDropPolicy  = flag.String("msg-persist-drop-policy", "drop", "queue-full policy: drop (non-blocking) or block (wait up to block-timeout)")
		msgPersistBlockTO     = flag.Duration("msg-persist-block-timeout", 100*time.Millisecond, "max wait when queue is full and drop-policy=block")
		msgPersistNodeID      = flag.String("msg-persist-node-id", "", "node id recorded in persisted rows (defaults to -node)")
		msgPersistSkipRetain  = flag.Bool("msg-persist-skip-retain", false, "skip persist retain messages")
	)
	flag.Parse()
	if *showVersion {
		fmt.Printf("mqtt broker %s commit %s date %s\n", version, commit, date)
		os.Exit(0)
	}
	logger.Init(*logLevel)
	slog.Info("starting", "mode", "standalone", "version", version, "commit", commit, "date", date, "log_level", *logLevel)

	var store persistence.Store
	var walStore persistence.Store
	if *walEnabled && *walDir != "" && *walDir != "-" {
		if ps, err := persistence.NewPebbleStore(*walDir, "mqtt"); err == nil {
			walStore = ps
			slog.Info("using pebble WAL", "dir", *walDir)
		} else {
			slog.Warn("pebble WAL open failed, fallback", "dir", *walDir, "err", err)
		}
	}
	if *redisAddr != "" {
		addrs := splitAddrs(*redisAddr)
		cli := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: addrs})
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		pingErr := cli.Ping(pingCtx).Err()
		cancel()
		if pingErr != nil {
			slog.Warn("redis unavailable", "addr", *redisAddr, "err", pingErr)
			if walStore != nil {
				store = walStore
				slog.Info("fallback to pebble WAL")
			} else {
				store = persistence.NewMemoryStore()
				slog.Info("fallback to memory")
			}
			_ = cli.Close()
		} else {
			redisStore := persistence.NewRedisStoreWithClient(cli, "mqtt")
			slog.Info("using redis store", "addr", *redisAddr)
			if walStore != nil {
				store = persistence.NewFallbackStore(redisStore, walStore)
				slog.Info("using fallback store: redis primary + pebble WAL (both implement persistence.Store)")
			} else {
				store = redisStore
			}
		}
	} else {
		if walStore != nil {
			store = walStore
		} else {
			store = persistence.NewMemoryStore()
		}
	}
	defer func() { _ = store.Close() }()

	// 可选的消息持久化 hook: 把全部 PUBLISH 异步批量写入 SQL (issue #5)。
	// 只写不查: 查询走 SQL/OctoSQL 直连数据库。
	var msgPersistHook *hook.MessagePersisterHook
	var msgPersistDB *sql.DB
	if *msgPersistDSN != "" {
		driver, dsn := "sqlite3", *msgPersistDSN
		if strings.HasPrefix(*msgPersistDSN, "postgres://") || strings.HasPrefix(*msgPersistDSN, "postgresql://") {
			driver, dsn = "postgres", *msgPersistDSN
		}
		pdb, err := sql.Open(driver, dsn)
		if err != nil {
			slog.Error("msg persister open failed", "dsn", *msgPersistDSN, "err", err)
			os.Exit(1)
		}
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
		pingErr := pdb.PingContext(pingCtx)
		pingCancel()
		if pingErr != nil {
			slog.Error("msg persister ping failed", "dsn", *msgPersistDSN, "err", pingErr)
			_ = pdb.Close()
			os.Exit(1)
		}
		persistNode := *msgPersistNodeID
		if persistNode == "" {
			persistNode = *nodeID
		}
		ph, err := hook.NewMessagePersisterHook(pdb, hook.MessagePersisterConfig{
			TableName:     *msgPersistTable,
			BatchSize:     *msgPersistBatchSize,
			FlushInterval: *msgPersistFlushInt,
			QueueCapacity: *msgPersistQueueCap,
			DropPolicy:    *msgPersistDropPolicy,
			BlockTimeout:  *msgPersistBlockTO,
			NodeID:        persistNode,
			SkipRetain:    *msgPersistSkipRetain,
		})
		if err != nil {
			slog.Error("msg persister init failed", "err", err)
			_ = pdb.Close()
			os.Exit(1)
		}
		msgPersistHook, msgPersistDB = ph, pdb
		slog.Info("message persistence enabled", "driver", driver, "table", *msgPersistTable, "batchSize", *msgPersistBatchSize, "flushInterval", *msgPersistFlushInt)
	}
	if msgPersistHook != nil {
		defer func() {
			_ = msgPersistHook.Close()
			_ = msgPersistDB.Close()
		}()
	}

	allowAnon := *allowAnonymous == "true"
	walDirCfg := ""
	if *walEnabled && *walDir != "" && *walDir != "-" {
		walDirCfg = *walDir
	}
	wsOrigins := splitAddrs(*wsAllowOrigins)
	cfg := broker.Config{
		NodeID:                *nodeID,
		TCPAddr:               *tcpAddr,
		WSAddr:                *wsAddr,
		RedisAddr:             *redisAddr,
		PprofAddr:             *pprofAddr,
		ACLFile:               *aclFile,
		JWTSecret:             *jwtSecret,
		AllowAnonymous:        allowAnon,
		TLSCertFile:           *tlsCert,
		TLSKeyFile:            *tlsKey,
		TLSCAFile:             *tlsCA,
		MaxRetainedMessages:   *maxRetainMessages,
		MaxRetainedSize:       *maxRetainSize,
		MaxRetainPerTopic:     *maxRetainPerTopic,
		MaxRetainSizePerTopic: *maxRetainSizePerTopic,
		WalDir:                walDirCfg,
		WsAllowOrigins:        wsOrigins,
	}
	opts := []broker.Option{broker.WithStore(store)}
	if msgPersistHook != nil {
		opts = append(opts, broker.WithHook(msgPersistHook))
	}
	b, err := broker.NewWithOptions(cfg, opts...)
	if err != nil {
		slog.Error("broker init failed", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		slog.Info("shutting down (standalone)")
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutCancel()
		if err := b.Stop(shutCtx); err != nil {
			slog.Warn("stop", "err", err)
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
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		for range ch {
			slog.Info("sighup reload", "acl", *aclFile)
		}
	}()

	// 独立运行：阻塞式 Start，由信号触发 Stop
	if err := b.Start(ctx); err != nil {
		slog.Error("broker error", "err", err)
		os.Exit(1)
	}
}
