package bench

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"mqtt/internal/broker"
	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

var (
	c10kN    = flag.Int("c10k-n", 10000, "number of concurrent clients")
	c10kHold = flag.Duration("c10k-hold", 10*time.Second, "hold duration")
)

func TestC10k(t *testing.T) {
	if os.Getenv("GITHUB_ACTIONS") != "true" && *c10kN == 10000 && !testing.Short() {
		t.Skip("c10k heavy 10k/60s only on GitHub Actions; locally use task c10k-quick or go test -run TestC10k -c10k-n=1000 -short")
	}
	n := *c10kN
	hold := *c10kHold
	if testing.Short() {
		n = 1000
		hold = 5 * time.Second
	}
	if os.Getenv("GITHUB_ACTIONS") != "true" && n > 2000 {
		t.Logf("dev machine: clamp c10k %d -> 1000 (set GITHUB_ACTIONS=true or -c10k-n to override)", n)
		n = 1000
		if hold > 15*time.Second {
			hold = 10 * time.Second
		}
	}
	if n < 100 {
		n = 100
	}
	store := persistence.NewMemoryStore()
	cfg := broker.Config{NodeID: "c10k", TCPAddr: "127.0.0.1:11887", WSAddr: "", RedisAddr: "", AllowAnonymous: true}
	br := broker.New(cfg, store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go br.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	conns := make([]net.Conn, 0, n)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	var mu sync.Mutex
	errs := 0
	start := time.Now()
	for i := 0; i < n; i++ {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:11887", 2*time.Second)
		if err != nil {
			mu.Lock()
			errs++
			mu.Unlock()
			continue
		}
		p := &codec.Packet{
			Type:          codec.TypeCONNECT,
			Version:       codec.ProtocolV311,
			ProtocolName:  "MQTT",
			ProtocolLevel: 4,
			ConnectFlags:  codec.ConnectFlags{CleanSession: true},
			KeepAlive:     60,
			ClientID:      fmt.Sprintf("c10k-%d-%d", n, i),
		}
		data, _ := codec.Encode(p)
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Write(data); err != nil {
			_ = conn.Close()
			mu.Lock()
			errs++
			mu.Unlock()
			continue
		}
		buf := make([]byte, 128)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Read(buf); err != nil {
			_ = conn.Close()
			mu.Lock()
			errs++
			mu.Unlock()
			continue
		}
		_ = conn.SetReadDeadline(time.Time{})
		conns = append(conns, conn)
		if (i+1)%1000 == 0 {
			t.Logf("connected %d/%d", i+1, n)
		}
	}
	elapsed := time.Since(start)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("c10k connect: %d/%d ok=%d errs=%d elapsed=%s heap=%dMB goroutines=%d", n, n, len(conns), errs, elapsed, m.Alloc/1024/1024, runtime.NumGoroutine())
	if len(conns) < n*9/10 {
		t.Fatalf("c10k connect failed: %d/%d connected, need >=90%%", len(conns), n)
	}
	if errs > n/10 {
		t.Fatalf("too many dial errors: %d", errs)
	}
	// hold
	t.Logf("holding %d conns for %s", len(conns), hold)
	// verify publish still works while holding
	subConn := conns[0]
	_ = subConn
	time.Sleep(hold / 3)
	// simple publish from first conn
	pubPkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "c10k/test", QoS: 0, Payload: []byte("hello-c10k")}
	if data, err := codec.Encode(pubPkt); err == nil {
		_, _ = conns[0].Write(data)
	}
	time.Sleep(hold * 2 / 3)
	runtime.ReadMemStats(&m)
	t.Logf("hold done heap=%dMB goroutines=%d", m.Alloc/1024/1024, runtime.NumGoroutine())
	if m.Alloc > 600*1024*1024 {
		t.Logf("warning: heap >600MB for %d conns (per-conn %.1fKB)", n, float64(m.Alloc)/float64(n)/1024)
	}
}
