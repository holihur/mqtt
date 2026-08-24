package bench

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"mqtt/internal/broker"
	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

func TestC100kGrad(t *testing.T) {
	if testing.Short() {
		t.Skip("grad only on next/c100k -- use go test -run TestC100kGrad -count=1 -v ./bench")
	}
	levels := []int{10000, 25000, 50000, 100000}
	store := persistence.NewMemoryStore()
	cfg := broker.Config{NodeID: "c100k-grad", TCPAddr: "127.0.0.1:11888", WSAddr: "", RedisAddr: "", AllowAnonymous: true}
	br := broker.New(cfg, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go br.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	for _, n := range levels {
		t.Run(fmt.Sprintf("%d", n), func(t *testing.T) {
			conns := make([]net.Conn, 0, n)
			start := time.Now()
			ok := 0
			for i := 0; i < n; i++ {
				c, err := net.DialTimeout("tcp", "127.0.0.1:11888", 2*time.Second)
				if err != nil {
					continue
				}
				p := &codec.Packet{
					Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4,
					ConnectFlags: codec.ConnectFlags{CleanSession: true}, KeepAlive: 60,
					ClientID: fmt.Sprintf("grad-%d-%d", n, i),
				}
				data, _ := codec.Encode(p)
				c.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if _, err := c.Write(data); err != nil {
					c.Close()
					continue
				}
				buf := make([]byte, 128)
				c.SetReadDeadline(time.Now().Add(2 * time.Second))
				if _, err := c.Read(buf); err != nil {
					c.Close()
					continue
				}
				c.SetReadDeadline(time.Time{})
				conns = append(conns, c)
				ok++
			}
			elapsed := time.Since(start)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			t.Logf("grad %d: ok=%d/%d elapsed=%s heap=%dMB goroutines=%d per-conn=%.1fKB", n, ok, n, elapsed, m.Alloc/1024/1024, runtime.NumGoroutine(), float64(m.Alloc)/float64(ok+1)/1024)
			if ok < n*8/10 {
				t.Fatalf("grad %d failed ok %d/%d <80%%", n, ok, n)
			}
			for _, c := range conns {
				_ = c.Close()
			}
			time.Sleep(500 * time.Millisecond)
			runtime.GC()
		})
	}
}
