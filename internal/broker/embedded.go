package broker

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

// Publish 供嵌入式调用方直接发布消息，无需网络往返。
// 等价于外部客户端的 PUBLISH，会走本地 Trie 投递 + 集群广播（若启用）。
func (b *Broker) Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	if topic == "" {
		return fmt.Errorf("topic empty")
	}
	if len(topic) > 4096 {
		return fmt.Errorf("topic too long")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	b.routeMessage(topic, payload, qos, retain, nil, "embedded")
	return nil
}

// PublishWithProperties 支持 v5 属性的嵌入式发布（可选）。
func (b *Broker) PublishWithProperties(ctx context.Context, topic string, payload []byte, qos byte, retain bool, props *codec.Properties) error {
	if topic == "" {
		return fmt.Errorf("topic empty")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	b.routeMessage(topic, payload, qos, retain, props, "embedded")
	return nil
}

// HandleConn 供嵌入式调用方注入已建立的 net.Conn（例如内存管道 net.Pipe 或自定义传输）。
// 与网络监听走同一条 handleRawConn 路径，复用所有认证、会话、订阅逻辑。
func (b *Broker) HandleConn(conn net.Conn) {
	if conn == nil {
		return
	}
	go b.handleRawConn(conn)
}

func (b *Broker) Stats() BrokerStats {
	b.mu.RLock()
	n := int64(len(b.conns))
	b.mu.RUnlock()
	return BrokerStats{
		StartedAt:        b.stats.StartedAt,
		MessagesReceived: atomic.LoadInt64(&b.stats.MessagesReceived),
		MessagesSent:     atomic.LoadInt64(&b.stats.MessagesSent),
		ClientsConnected: n,
		ClientsTotal:     atomic.LoadInt64(&b.stats.ClientsTotal),
	}
}

// Health 供嵌入式健康检查：检查 Redis、连接数、goroutine 阈值。
func (b *Broker) Health(ctx context.Context) error {
	if b.redisCli != nil {
		if err := b.redisCli.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis unavailable: %w", err)
		}
	}
	b.mu.RLock()
	n := len(b.conns)
	b.mu.RUnlock()
	if n > 16000 {
		return fmt.Errorf("too many connections: %d", n)
	}
	return nil
}

// Store 暴露持久化接口，供嵌入式定制。
func (b *Broker) Store() persistence.Store { return b.store }

// SetStore 允许运行时替换 Store（仅在未 Start 时调用更安全）。
func (b *Broker) SetStore(s persistence.Store) {
	if s != nil {
		b.store = s
	}
}

// ClientCount 返回在线连接数。
func (b *Broker) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.conns)
}

// SessionCount 返回会话数。
func (b *Broker) SessionCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.sessions)
}

// IsEmbedded 判断是否以嵌入式（无监听）模式运行。
func (b *Broker) IsEmbedded() bool {
	return b.cfg.TCPAddr == "" && b.cfg.WSAddr == "" && b.customListener == nil
}
