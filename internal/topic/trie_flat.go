//go:build flat_trie

package topic

// 备用实现 (build tag: flat_trie): 不建树，用 map[clientID#filter] 扁平存放，
// Match 时全量扫描 + 无状态 MatchFilter。订阅量小、精确匹配多时可能更省内存；
// 与默认实现共享同一 Trie 接口与 MQTT 语义，用于 A/B 压测。

import (
	"strings"
	"sync"
)

type flatTrie struct {
	mu sync.RWMutex
	m  map[string]*SubEntry // key: clientID#filter
}

// NewTrie 由 build tag 选择实现 (本文件: newFlatTrie)。
func NewTrie() Trie { return newFlatTrie() }

func newFlatTrie() Trie {
	return &flatTrie{m: make(map[string]*SubEntry)}
}

func (t *flatTrie) Add(filter, clientID string, qos byte, noLocal bool) {
	t.mu.Lock()
	t.m[subKey(clientID, filter)] = &SubEntry{ClientID: clientID, Filter: filter, QoS: qos, NoLocal: noLocal}
	t.mu.Unlock()
}

func (t *flatTrie) Remove(filter, clientID string) {
	t.mu.Lock()
	delete(t.m, subKey(clientID, filter))
	t.mu.Unlock()
}

// Match returns all subscribers whose filter matches topic.
func (t *flatTrie) Match(topic string) []*SubEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var result []*SubEntry
	sysTopic := strings.HasPrefix(topic, "$")
	for _, s := range t.m {
		if sysTopic && (s.Filter == "#" || strings.HasPrefix(s.Filter, "+") || strings.HasPrefix(s.Filter, "#")) {
			continue
		}
		if MatchFilter(topic, s.Filter) {
			result = append(result, entryCopy(s))
		}
	}
	return result
}

// Subscriptions returns all subscription entries (for management API).
func (t *flatTrie) Subscriptions() []*SubEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*SubEntry, 0, len(t.m))
	for _, s := range t.m {
		out = append(out, entryCopy(s))
	}
	return out
}

// SubscriptionsFor returns all subscription entries of a given client (for management API).
func (t *flatTrie) SubscriptionsFor(clientID string) []*SubEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []*SubEntry
	for _, s := range t.m {
		if s.ClientID == clientID {
			out = append(out, entryCopy(s))
		}
	}
	return out
}
