package broker

// 白盒核心路径基准: handlePublish → 速率/配额检查 → routeMessage → trie 匹配
// → 投递 (QoS0 fan-out)。使用 net.Pipe 而非 TCP，消除内核/协议栈噪音，稳定测量
// broker 自身 CPU 与分配开销。
//
// 订阅者数量可用参数调整:
//
//	go test -bench=BenchmarkCoreDeliverFanOut -run=^$ -benchmem \
//	    -args -core.subs=32 ./internal/broker
//
// 说明: net.Pipe 的写端是同步握手，绝对 ns 值受其影响；allocs/op 与 B/op 是
// 可靠指标，用于对比投递路径的分配优化。
import (
	"context"
	"flag"
	"io"
	"net"
	"testing"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
	"mqtt/internal/session"
	"mqtt/internal/transport"
)

var benchCoreSubs = flag.Int("core.subs", 1, "number of subscribers for BenchmarkCoreDeliverFanOut")

// newCoreBenchBroker 启动嵌入式 broker 并注册 nSubs 个 v3.1.1 订阅者 (bench/#, QoS0),
// 每个订阅者由 drain goroutine 持续消费 net.Pipe 写端。
func newCoreBenchBroker(b *testing.B, nSubs int) *Broker {
	b.Helper()
	store := persistence.NewMemoryStore()
	cfg := Config{
		NodeID:             "core-bench",
		AllowAnonymous:     true,
		MaxPublishPerSec:   1 << 30,
		MaxSubscribePerSec: 1 << 30,
	}
	br, err := NewWithOptions(cfg, WithStore(store))
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	if err := br.StartAsync(ctx); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < nSubs; i++ {
		c1, c2 := net.Pipe()
		cc := transport.NewConn(c2, 1<<20)
		id := "core-sub"
		if nSubs > 1 {
			id = "core-sub-" + string(rune('a'+i))
		}
		cc.SetVersion(codec.ProtocolV311)
		cc.SetClientID(id)
		sess := session.NewSession(id, codec.ProtocolV311, true, 0)
		br.mu.Lock()
		br.conns[id] = cc
		br.sessions[id] = sess
		br.mu.Unlock()
		br.trie.Add("bench/#", id, 0, false)
		go func(r io.Reader) {
			buf := make([]byte, 32768)
			for {
				if _, err := r.Read(buf); err != nil {
					return
				}
			}
		}(c1)
	}
	return br
}

// BenchmarkCoreDeliverFanOut 测量 1 条 QoS0 消息发布 → 投递到 N 个订阅者的核心路径。
func BenchmarkCoreDeliverFanOut(b *testing.B) {
	nSubs := *benchCoreSubs
	if nSubs < 1 {
		nSubs = 1
	}
	br := newCoreBenchBroker(b, nSubs)
	// 发布端: 独立 pipe conn + session (无真实 TCP)
	pc1, pc2 := net.Pipe()
	pconn := transport.NewConn(pc2, 1<<20)
	pconn.SetVersion(codec.ProtocolV311)
	pconn.SetClientID("core-pub")
	go func(r io.Reader) {
		buf := make([]byte, 32768)
		for {
			if _, err := r.Read(buf); err != nil {
				return
			}
		}
	}(pc1)
	psess := session.NewSession("core-pub", codec.ProtocolV311, true, 0)

	payload := []byte("hello-benchmark-payload")
	pkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "bench/topic", QoS: 0, Payload: payload}
	b.ReportMetric(float64(nSubs), "subscribers")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.handlePublish(pconn, psess, pkt)
	}
	b.StopTimer()
	_ = pc1.Close()
	_ = pc2.Close()
	_ = pconn.Close()
}
