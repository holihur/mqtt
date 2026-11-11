package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mqtt/internal/broker"
	"mqtt/internal/hook"
	"mqtt/internal/logger"
	"mqtt/internal/persistence"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	_ "modernc.org/sqlite"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// isMongoDSN reports whether the persistence DSN targets MongoDB.
func isMongoDSN(dsn string) bool {
	return strings.HasPrefix(dsn, "mongodb://") || strings.HasPrefix(dsn, "mongodb+srv://")
}

// resolveSQLDSN maps the user-facing DSN to a registered database/sql driver.
// Native go-sql-driver DSNs (user:pass@tcp(host:3306)/db) and mysql:// URLs are
// both accepted; anything else falls back to SQLite for backwards compatibility.
func resolveSQLDSN(raw string) (driver, dsn string) {
	switch {
	case strings.HasPrefix(raw, "postgres://"), strings.HasPrefix(raw, "postgresql://"):
		return "postgres", raw
	case strings.HasPrefix(raw, "mysql://"), strings.HasPrefix(raw, "mariadb://"):
		return "mysql", mysqlDSNFromURL(raw)
	case strings.HasPrefix(raw, "mysql:"):
		return "mysql", strings.TrimPrefix(raw, "mysql:")
	case strings.Contains(raw, "@tcp("), strings.Contains(raw, "@unix("):
		return "mysql", raw
	default:
		return "sqlite", raw
	}
}

// mysqlDSNFromURL converts mysql://user:pass@host:3306/db?params into the
// native go-sql-driver DSN user:pass@tcp(host:3306)/db?params. The port defaults
// to 3306 when omitted.
func mysqlDSNFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "3306"
	}
	auth := ""
	if u.User != nil {
		auth = u.User.Username()
		if password, ok := u.User.Password(); ok {
			auth += ":" + password
		}
		auth += "@"
	}
	dsn := auth + "tcp(" + net.JoinHostPort(host, port) + ")/" + strings.TrimPrefix(u.Path, "/")
	if u.RawQuery != "" {
		dsn += "?" + u.RawQuery
	}
	return dsn
}

// mongoDatabaseFromDSN extracts the database from a MongoDB URI, falling back
// to the default when the URI has no path component.
func mongoDatabaseFromDSN(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		db := strings.TrimPrefix(u.Path, "/")
		if i := strings.IndexByte(db, '/'); i >= 0 {
			db = db[:i]
		}
		if db != "" {
			if decoded, err := url.PathUnescape(db); err == nil {
				return decoded
			}
			return db
		}
	}
	return hook.DefaultMongoDatabase
}

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
		adminAPIAddr          = flag.String("admin-api", "", "management API listen addr (empty to disable, e.g. :6061)")
		adminAPIToken         = flag.String("admin-api-token", "", "management API bearer token (empty: loopback only)")
		adminAPITLS           = flag.Bool("admin-api-tls", false, "serve management API over TLS (uses -tls-cert/-tls-key)")
		mcpEnabled            = flag.Bool("mcp", false, "enable MCP server at /mcp on the admin/webui listener (use -mcp=true form)")
		mcpToken              = flag.String("mcp-token", "", "MCP auth key (empty: loopback only; independent of admin token)")
		httpPublishAddr       = flag.String("http-publish", "", "standalone HTTP publish API listen addr (empty to disable, e.g. :6062)")
		httpPublishToken      = flag.String("http-publish-token", "", "HTTP publish API bearer token (empty: loopback only)")
		webuiAddr             = flag.String("webui", "", "web dashboard listen addr (empty to disable, e.g. :8080); serves embedded UI + /api/v1")
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
		msgPersistDSN         = flag.String("msg-persist-dsn", "", "SQL/MongoDB DSN for message persistence (empty to disable); postgres://... / mysql://... / mongodb://... / SQLite file path")
		msgPersistTable       = flag.String("msg-persist-table", "mqtt_messages", "message persistence table name (SQL) or collection name (MongoDB)")
		msgPersistMongoDB     = flag.String("msg-persist-mongodb-database", "", "MongoDB database for message persistence (default: URI path or \"mqtt\")")
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
	slog.Info("starting", "mode", "standalone", "version", version, "commit", commit, "date", date, "log_level", *logLevel, "admin_api", *adminAPIAddr, "webui", *webuiAddr, "admin_token_set", *adminAPIToken != "")

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

	// 可选的消息持久化 hook: 把全部 PUBLISH 异步批量写入 SQL/MongoDB (issue #5)。
	// 只写不查: 查询走 SQL/OctoSQL 或 MongoDB 直连数据库。
	var msgPersistHook hook.Hook
	var closeMsgPersist func()
	if *msgPersistDSN != "" {
		persistNode := *msgPersistNodeID
		if persistNode == "" {
			persistNode = *nodeID
		}

		if isMongoDSN(*msgPersistDSN) {
			database := *msgPersistMongoDB
			if database == "" {
				database = mongoDatabaseFromDSN(*msgPersistDSN)
			}
			connectCtx, connectCancel := context.WithTimeout(context.Background(), 5*time.Second)
			client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(*msgPersistDSN).SetConnectTimeout(5*time.Second))
			if err != nil {
				connectCancel()
				slog.Error("mongo msg persister connect failed", "err", err)
				os.Exit(1)
			}
			if err := client.Ping(connectCtx, nil); err != nil {
				connectCancel()
				_ = client.Disconnect(context.Background())
				slog.Error("mongo msg persister ping failed", "database", database, "err", err)
				os.Exit(1)
			}
			connectCancel()

			mh, err := hook.NewMongoMessagePersisterHook(client, hook.MongoMessagePersisterConfig{
				Database:      database,
				Collection:    *msgPersistTable,
				BatchSize:     *msgPersistBatchSize,
				FlushInterval: *msgPersistFlushInt,
				QueueCapacity: *msgPersistQueueCap,
				DropPolicy:    *msgPersistDropPolicy,
				BlockTimeout:  *msgPersistBlockTO,
				NodeID:        persistNode,
				SkipRetain:    *msgPersistSkipRetain,
			})
			if err != nil {
				_ = client.Disconnect(context.Background())
				slog.Error("mongo msg persister init failed", "err", err)
				os.Exit(1)
			}
			msgPersistHook = mh
			closeMsgPersist = func() {
				_ = mh.Close()
				disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer disconnectCancel()
				_ = client.Disconnect(disconnectCtx)
			}
			slog.Info("message persistence enabled", "driver", "mongodb", "database", database, "collection", *msgPersistTable, "batchSize", *msgPersistBatchSize, "flushInterval", *msgPersistFlushInt)
		} else {
			driver, dsn := resolveSQLDSN(*msgPersistDSN)
			pdb, err := sql.Open(driver, dsn)
			if err != nil {
				slog.Error("msg persister open failed", "driver", driver, "err", err)
				os.Exit(1)
			}
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
			pingErr := pdb.PingContext(pingCtx)
			pingCancel()
			if pingErr != nil {
				slog.Error("msg persister ping failed", "driver", driver, "err", pingErr)
				_ = pdb.Close()
				os.Exit(1)
			}
			ph, err := hook.NewMessagePersisterHook(pdb, hook.MessagePersisterConfig{
				TableName:        *msgPersistTable,
				BatchSize:        *msgPersistBatchSize,
				BatchInsert:      true,
				PlaceholderStyle: driver,
				FlushInterval:    *msgPersistFlushInt,
				QueueCapacity:    *msgPersistQueueCap,
				DropPolicy:       *msgPersistDropPolicy,
				BlockTimeout:     *msgPersistBlockTO,
				NodeID:           persistNode,
				SkipRetain:       *msgPersistSkipRetain,
			})
			if err != nil {
				slog.Error("msg persister init failed", "err", err)
				_ = pdb.Close()
				os.Exit(1)
			}
			msgPersistHook = ph
			closeMsgPersist = func() {
				_ = ph.Close()
				_ = pdb.Close()
			}
			slog.Info("message persistence enabled", "driver", driver, "table", *msgPersistTable, "batchSize", *msgPersistBatchSize, "flushInterval", *msgPersistFlushInt)
		}
	}
	if closeMsgPersist != nil {
		defer closeMsgPersist()
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
		AdminAddr:             *adminAPIAddr,
		AdminToken:            *adminAPIToken,
		AdminTLS:              *adminAPITLS,
		MCPEnabled:            *mcpEnabled,
		MCPToken:              *mcpToken,
		HTTPPublishAddr:       *httpPublishAddr,
		HTTPPublishToken:      *httpPublishToken,
		WebUIAddr:             *webuiAddr,
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
	opts := []broker.Option{broker.WithStore(store), broker.WithVersion(version, commit, date)}
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
