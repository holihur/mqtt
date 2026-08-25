package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"time"

	"mqtt/internal/broker"
	"mqtt/internal/hook"
	"mqtt/internal/persistence"
)

type auditHook struct{ hook.BaseHook }

func (a auditHook) ID() string { return "audit" }
func (a auditHook) OnPublish(clientID, topic string, payload []byte, qos byte, retain bool) error {
	slog.Info("hook publish", "client", clientID, "topic", topic, "qos", qos)
	return nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// 嵌入式模式 1: 纯内存，无网络监听，通过 HandleConn + Publish API 交互
	b1, _ := broker.NewWithOptions(broker.Config{
		NodeID:         "embedded-mem",
		TCPAddr:        "", // 禁用 TCP
		WSAddr:         "", // 禁用 WS
		AllowAnonymous: true,
	}, broker.WithStore(persistence.NewMemoryStore()), broker.WithHook(auditHook{}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// StartAsync 适合嵌入式：非阻塞，适合在已有进程中后台运行
	if err := b1.StartAsync(ctx); err != nil {
		slog.Error("b1 start failed", "err", err)
		os.Exit(1)
	}
	slog.Info("b1 started", "isEmbedded", b1.IsEmbedded(), "isRunning", b1.IsRunning())

	// 嵌入式直接发布（无客户端连接时仅走 retain/cluster，本例演示 API 可用）
	_ = b1.Publish(ctx, "tenant/demo/hello", []byte("embedded payload"), 0, false)
	slog.Info("b1 publish done", "stats", b1.Stats())

	// 通过 net.Pipe 模拟客户端连接，无需真实端口
	c1, c2 := net.Pipe()
	b1.HandleConn(c2) // broker 侧持有 c2
	// c1 可用于按 MQTT 协议读写，这里仅演示注入成功
	time.Sleep(100 * time.Millisecond)
	_ = c1.Close()
	slog.Info("b1 pipe demo done", "clients", b1.ClientCount())

	// 嵌入式模式 2: 带网络监听，复用已有 net.Listener（例如与 HTTP 服务同端口或动态端口）
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	slog.Info("custom listener", "addr", ln.Addr().String())

	b2, _ := broker.NewWithOptions(broker.Config{
		NodeID:         "embedded-net",
		AllowAnonymous: true,
		WSAddr:         "", // 本例仅演示 TCP
	}, broker.WithStore(persistence.NewMemoryStore()), broker.WithCustomListener(ln))

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := b2.StartAsync(ctx2); err != nil {
		slog.Error("b2 start failed", "err", err)
		os.Exit(1)
	}
	slog.Info("b2 started with custom listener", "addr", b2.Addr(), "stats", b2.Stats())

	// 演示优雅停止
	time.Sleep(200 * time.Millisecond)
	if err := b1.Stop(ctx); err != nil {
		slog.Warn("b1 stop", "err", err)
	}
	if err := b2.Stop(ctx2); err != nil {
		slog.Warn("b2 stop", "err", err)
	}
	slog.Info("embedded demo finished")

	// 嵌入式模式 3: 同进程多实例对比（独立运行则通过 cmd/broker 另起进程）
	// standalone: go run ./cmd/broker -tcp :1883 -redis :6379
	// embedded:  broker.NewWithOptions(...).StartAsync(ctx)  // 如上
}
