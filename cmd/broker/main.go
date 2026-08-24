package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"mqtt/internal/broker"
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
		nodeID         = flag.String("node", "", "Node ID (auto if empty)")
	)
	flag.Parse()

	var store persistence.Store
	var redisCli redis.UniversalClient
	if *redisAddr != "" {
		addrs := []string{*redisAddr}
		if len(*redisAddr) > 0 {
			// support comma-separated for cluster/sentinel
			addrs = splitAddrs(*redisAddr)
		}
		cli := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: addrs})
		if err := cli.Ping(context.Background()).Err(); err != nil {
			log.Printf("redis unavailable %s: %v, falling back to memory store", *redisAddr, err)
			store = persistence.NewMemoryStore()
		} else {
			store = persistence.NewRedisStoreWithClient(cli, "mqtt")
			redisCli = cli
			log.Printf("using redis store at %s", *redisAddr)
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
	}
	b := broker.New(cfg, store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// signal handling
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		log.Println("shutting down...")
		cancel()
	}()

	if err := b.Start(ctx); err != nil {
		log.Fatalf("broker error: %v", err)
	}
}
