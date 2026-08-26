package broker

import (
	"sync"
	"testing"
	"time"
)

func TestIsSysFilter(t *testing.T) {
	cases := []struct {
		f    string
		want bool
	}{
		{"$SYS/#", true},
		{"$SYS/broker/uptime", true},
		{"$SYS/broker/#", true},
		{"$SYS/+", true},
		{"$SYS/broker/clients", true},
		{"$SYS", true},
		{"$SYS/", true},
		{"$share/g/$SYS/broker/#", false},
		{"a/b", false},
		{"$SYSfoo", false},
		{"#", false},
		{"$share/g/a/b", false},
		{"", false},
		{"sys/broker", false},
	}
	for _, tc := range cases {
		if got := isSysFilter(tc.f); got != tc.want {
			t.Errorf("isSysFilter(%q)=%v want %v", tc.f, got, tc.want)
		}
	}
}

func TestLimiterLeakFix(t *testing.T) {
	b := &Broker{
		cfg:      Config{MaxPublishPerSec: 100, MaxSubscribePerSec: 20},
		limiters: make(map[string]*clientLimiter),
	}
	b.cfg.ApplyDefaults()
	if b.LimiterCount() != 0 {
		t.Fatalf("initial count 0, got %d", b.LimiterCount())
	}
	for i := 0; i < 100; i++ {
		id := string(rune('a'+i%26)) + string(rune('0'+i%10)) + "-test"
		b.allowPublish(id)
		b.allowSubscribe(id)
	}
	if b.LimiterCount() != 100 {
		t.Fatalf("expected 100 limiters, got %d", b.LimiterCount())
	}
	for i := 0; i < 50; i++ {
		id := string(rune('a'+i%26)) + string(rune('0'+i%10)) + "-test"
		b.removeLimiter(id)
	}
	if b.LimiterCount() != 50 {
		t.Fatalf("after remove 50, got %d", b.LimiterCount())
	}
	b.limitMu.Lock()
	for _, lim := range b.limiters {
		lim.mu.Lock()
		lim.lastSeen = time.Now().Add(-20 * time.Minute)
		lim.window = time.Now().Add(-20 * time.Minute)
		lim.mu.Unlock()
	}
	b.limitMu.Unlock()
	removed := b.cleanupLimiters(10 * time.Minute)
	if removed != 50 {
		t.Fatalf("cleanup should remove 50, got %d", removed)
	}
	if b.LimiterCount() != 0 {
		t.Fatalf("after cleanup 0, got %d", b.LimiterCount())
	}
}

func TestLimiterCleanupIdleSkipRecent(t *testing.T) {
	b := &Broker{
		cfg:      Config{MaxPublishPerSec: 100, MaxSubscribePerSec: 20},
		limiters: make(map[string]*clientLimiter),
	}
	b.cfg.ApplyDefaults()
	b.allowPublish("recent-client")
	b.allowSubscribe("recent-client")
	b.limitMu.Lock()
	if lim, ok := b.limiters["recent-client"]; ok {
		lim.mu.Lock()
		lim.lastSeen = time.Now()
		lim.mu.Unlock()
	}
	oldID := "old-client"
	b.limitMu.Unlock()
	b.allowPublish(oldID)
	b.limitMu.Lock()
	if lim, ok := b.limiters[oldID]; ok {
		lim.mu.Lock()
		lim.lastSeen = time.Now().Add(-20 * time.Minute)
		lim.window = time.Now().Add(-20 * time.Minute)
		lim.mu.Unlock()
	}
	b.limitMu.Unlock()
	removed := b.cleanupLimiters(10 * time.Minute)
	if removed != 1 {
		t.Fatalf("should remove 1 old, got %d", removed)
	}
	if b.LimiterCount() != 1 {
		t.Fatalf("should keep 1 recent, got %d", b.LimiterCount())
	}
	if _, ok := b.limiters["recent-client"]; !ok {
		t.Fatalf("recent should remain")
	}
}

func TestLimiterOnDisconnectRemoves(t *testing.T) {
	b := &Broker{
		cfg:      Config{MaxPublishPerSec: 100, MaxSubscribePerSec: 20},
		limiters: make(map[string]*clientLimiter),
	}
	b.cfg.ApplyDefaults()
	b.allowPublish("to-remove")
	if b.LimiterCount() != 1 {
		t.Fatalf("should have 1")
	}
	b.removeLimiter("to-remove")
	if b.LimiterCount() != 0 {
		t.Fatalf("removeLimiter failed")
	}
	b.removeLimiter("nonexistent")
	if b.LimiterCount() != 0 {
		t.Fatalf("remove nonexistent should stay 0")
	}
}

func TestLimiterConcurrent(t *testing.T) {
	b := &Broker{
		cfg:      Config{MaxPublishPerSec: 1000, MaxSubscribePerSec: 1000},
		limiters: make(map[string]*clientLimiter),
	}
	b.cfg.ApplyDefaults()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := "concurrent-client"
			b.allowPublish(id)
			b.allowSubscribe(id)
		}(i)
	}
	wg.Wait()
	if b.LimiterCount() != 1 {
		t.Fatalf("concurrent should have 1 limiter, got %d", b.LimiterCount())
	}
	removed := b.cleanupLimiters(0)
	if removed != 1 {
		t.Fatalf("cleanup 0 should remove, got %d", removed)
	}
}

func TestAllowPublishWindowResetLimiter(t *testing.T) {
	b := &Broker{
		cfg:      Config{MaxPublishPerSec: 2, MaxSubscribePerSec: 2},
		limiters: make(map[string]*clientLimiter),
	}
	b.cfg.ApplyDefaults()
	b.cfg.MaxPublishPerSec = 2
	id := "win-client"
	if !b.allowPublish(id) {
		t.Fatal("first should pass")
	}
	if !b.allowPublish(id) {
		t.Fatal("second should pass")
	}
	if b.allowPublish(id) {
		t.Fatal("third should be limited")
	}
	b.limitMu.Lock()
	lim := b.limiters[id]
	lim.mu.Lock()
	lim.window = time.Now().Add(-2 * time.Second)
	lim.mu.Unlock()
	b.limitMu.Unlock()
	if !b.allowPublish(id) {
		t.Fatal("after window reset should pass")
	}
}

func TestCleanupLimitersZeroLastSeen(t *testing.T) {
	b := &Broker{
		cfg:      Config{MaxPublishPerSec: 100, MaxSubscribePerSec: 20},
		limiters: make(map[string]*clientLimiter),
	}
	b.cfg.ApplyDefaults()
	b.limitMu.Lock()
	b.limiters["legacy"] = &clientLimiter{window: time.Now().Add(-20 * time.Minute)}
	b.limitMu.Unlock()
	removed := b.cleanupLimiters(10 * time.Minute)
	if removed != 1 {
		t.Fatalf("legacy zero lastSeen should be cleaned via window, got %d", removed)
	}
}
