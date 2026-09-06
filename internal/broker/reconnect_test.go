package broker

import (
	"testing"
	"time"
)

// TestReconnectSameClientIDKicksOld 覆盖同一 clientID 在旧连接仍存活时重连的
// 路径：旧的 handleRawConn 曾在持有 b.mu 时调用 old.Close()，而 Close 会同步
// 触发 onClientDisconnect 再次获取 b.mu，导致死锁。此测试确保该路径不阻塞，
// 且新的连接成为 conns 中的唯一条目。
func TestReconnectSameClientIDKicksOld(t *testing.T) {
	addr := "127.0.0.1:13159"
	_ = newTCPBroker(t, addr)
	time.Sleep(200 * time.Millisecond)

	conn1 := connectClient(t, addr, "reconnect-kick")
	time.Sleep(100 * time.Millisecond)

	// 旧连接仍存活时重连，旧连接应被踢掉且不阻塞。
	conn2 := connectClient(t, addr, "reconnect-kick")
	time.Sleep(200 * time.Millisecond)

	// conn1 已被服务端关闭：写入应失败（或读返回 EOF）。
	_ = conn1.SetDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	if _, err := conn1.Read(buf); err == nil {
		t.Fatal("expected old connection to be closed by server")
	}
	_ = conn2.Close()
}
