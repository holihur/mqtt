//go:build !flat_trie && !radix_trie

package topic

// 默认实现: 按 topic 层级的共享前缀树。订阅量越大、匹配越省 (fan-out 时单次
// Match 复杂度 O(主题层数 + 命中分支)，与总订阅量无关)。

import (
	"strings"
	"sync"
)

type levelTrie struct {
	mu   sync.RWMutex
	root *lnode
}

type lnode struct {
	children map[string]*lnode
	subs     map[string]*SubEntry // key: clientID#filter (unique)
}

// NewTrie 由 build tag 选择实现 (本文件: newLevelTrie)。
func NewTrie() Trie { return newLevelTrie() }

func newLevelTrie() Trie {
	return &levelTrie{root: &lnode{children: make(map[string]*lnode), subs: make(map[string]*SubEntry)}}
}

func (t *levelTrie) Add(filter, clientID string, qos byte, noLocal bool) {
	levels := strings.Split(filter, "/")
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.root
	for _, lv := range levels {
		if n.children[lv] == nil {
			n.children[lv] = &lnode{children: make(map[string]*lnode), subs: make(map[string]*SubEntry)}
		}
		n = n.children[lv]
	}
	key := subKey(clientID, filter)
	n.subs[key] = &SubEntry{ClientID: clientID, Filter: filter, QoS: qos, NoLocal: noLocal}
}

func (t *levelTrie) Remove(filter, clientID string) {
	levels := strings.Split(filter, "/")
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.root
	var stack []*lnode
	stack = append(stack, n)
	for _, lv := range levels {
		c := n.children[lv]
		if c == nil {
			return
		}
		n = c
		stack = append(stack, n)
	}
	key := subKey(clientID, filter)
	delete(n.subs, key)
	// prune empty branches
	for i := len(levels); i >= 0; i-- {
		cur := stack[i]
		if len(cur.subs) == 0 && len(cur.children) == 0 && i > 0 {
			parent := stack[i-1]
			delete(parent.children, levels[i-1])
		} else {
			break
		}
	}
}

// Match returns all subscribers whose filter matches topic.
func (t *levelTrie) Match(topic string) []*SubEntry {
	levels := strings.Split(topic, "/")
	t.mu.RLock()
	defer t.mu.RUnlock()
	var result []*SubEntry
	type frame struct {
		n   *lnode
		idx int
	}
	stack := []frame{{t.root, 0}}
	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n, idx := f.n, f.idx
		if n == nil {
			continue
		}
		if idx == len(levels) {
			for _, s := range n.subs {
				result = append(result, entryCopy(s))
			}
			if child, ok := n.children["#"]; ok {
				for _, s := range child.subs {
					result = append(result, entryCopy(s))
				}
			}
			continue
		}
		if child, ok := n.children["#"]; ok {
			for _, s := range child.subs {
				result = append(result, entryCopy(s))
			}
		}
		if child, ok := n.children["+"]; ok {
			stack = append(stack, frame{child, idx + 1})
		}
		if child, ok := n.children[levels[idx]]; ok {
			stack = append(stack, frame{child, idx + 1})
		}
	}
	// Filter $SYS violation: 主题以 "$" 开头时滤除首层通配符/# 的订阅
	return filterSysSubs(topic, result)
}

// Subscriptions returns all subscription entries across the trie (for management API).
func (t *levelTrie) Subscriptions() []*SubEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []*SubEntry
	var walk func(n *lnode)
	walk = func(n *lnode) {
		for _, s := range n.subs {
			out = append(out, entryCopy(s))
		}
		for _, c := range n.children {
			walk(c)
		}
	}
	walk(t.root)
	return out
}

// SubscriptionsFor returns all subscription entries of a given client (for management API).
func (t *levelTrie) SubscriptionsFor(clientID string) []*SubEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []*SubEntry
	var walk func(n *lnode)
	walk = func(n *lnode) {
		for _, s := range n.subs {
			if s.ClientID == clientID {
				out = append(out, entryCopy(s))
			}
		}
		for _, c := range n.children {
			walk(c)
		}
	}
	walk(t.root)
	return out
}
