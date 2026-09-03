package topic

// 前缀树两种实现 (默认层级 trie vs -tags flat_trie) 的 A/B 基准：
//
//	go test -bench=. -run=^$ ./internal/topic
//	go test -bench=. -run=^$ -tags flat_trie ./internal/topic
//
// 订阅量可用 -args -trie.subs=N 调整 (默认 10000)。

import (
	"flag"
	"fmt"
	"testing"
)

var benchSubs = flag.Int("trie.subs", 10000, "number of subscriptions to register")

// benchFill 注册 N 条混合订阅 (精确 + /+ + /# 通配)。
func benchFill(b *testing.B, tr Trie, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		fl := fmt.Sprintf("sensors/area%d/+/temp", i%50)
		if i%7 == 0 {
			fl = fmt.Sprintf("sensors/area%d/#", i%50)
		} else if i%11 == 0 {
			fl = fmt.Sprintf("devices/%d/status", i)
		}
		tr.Add(fl, fmt.Sprintf("client-%d", i), byte(i%3), false)
	}
}

func BenchmarkTrieAdd(b *testing.B) {
	n := *benchSubs
	tr := NewTrie()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fl := fmt.Sprintf("bench/topic/%d", i%n)
		tr.Add(fl, "c", 0, false)
	}
}

func BenchmarkTrieMatchExact(b *testing.B) {
	n := *benchSubs
	tr := NewTrie()
	benchFill(b, tr, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tr.Match("sensors/area3/room5/temp")
	}
}

func BenchmarkTrieMatchFanOut(b *testing.B) {
	n := *benchSubs
	tr := NewTrie()
	benchFill(b, tr, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tr.Match(fmt.Sprintf("sensors/area%d/room/temp", i%50))
	}
}

func BenchmarkTrieRemove(b *testing.B) {
	n := *benchSubs
	tr := NewTrie()
	benchFill(b, tr, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fl := fmt.Sprintf("devices/%d/status", i%n)
		tr.Remove(fl, fmt.Sprintf("client-%d", i%n))
	}
}
