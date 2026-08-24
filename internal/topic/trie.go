package topic

import (
	"strings"
	"sync"
)

// Trie for topic filter matching. Supports + and # wildcards only in filters (not topic names).
// Topic names must not contain +/#, but we don't enforce strictly for publish.

type Trie struct {
	mu   sync.RWMutex
	root *node
}

type node struct {
	children map[string]*node
	subs     map[string]*subEntry // key: clientID#filter (unique)
	// for shared subs later?
}

type subEntry struct {
	ClientID string
	Filter   string
	QoS      byte
	NoLocal  bool
}

func NewTrie() *Trie {
	return &Trie{root: &node{children: make(map[string]*node), subs: make(map[string]*subEntry)}}
}

func (t *Trie) Add(filter, clientID string, qos byte, noLocal bool) {
	levels := strings.Split(filter, "/")
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.root
	for _, lv := range levels {
		if n.children[lv] == nil {
			n.children[lv] = &node{children: make(map[string]*node), subs: make(map[string]*subEntry)}
		}
		n = n.children[lv]
	}
	key := clientID + "#" + filter
	n.subs[key] = &subEntry{ClientID: clientID, Filter: filter, QoS: qos, NoLocal: noLocal}
}

func (t *Trie) Remove(filter, clientID string) {
	levels := strings.Split(filter, "/")
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.root
	var stack []*node
	stack = append(stack, n)
	for _, lv := range levels {
		c := n.children[lv]
		if c == nil {
			return
		}
		n = c
		stack = append(stack, n)
	}
	key := clientID + "#" + filter
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
func (t *Trie) Match(topic string) []*subEntry {
	levels := strings.Split(topic, "/")
	t.mu.RLock()
	defer t.mu.RUnlock()
	var result []*subEntry
	type frame struct {
		n   *node
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
				result = append(result, s)
			}
			if child, ok := n.children["#"]; ok {
				for _, s := range child.subs {
					result = append(result, s)
				}
			}
			continue
		}
		if child, ok := n.children["#"]; ok {
			for _, s := range child.subs {
				result = append(result, s)
			}
		}
		if child, ok := n.children["+"]; ok {
			stack = append(stack, frame{child, idx + 1})
		}
		if child, ok := n.children[levels[idx]]; ok {
			stack = append(stack, frame{child, idx + 1})
		}
	}
	// Filter $SYS violation: if topic starts with "$", remove subs whose filter is "#" or starts with "+"
	if strings.HasPrefix(topic, "$") {
		filtered := result[:0]
		for _, s := range result {
			if s.Filter == "#" || strings.HasPrefix(s.Filter, "+") || strings.HasPrefix(s.Filter, "#") {
				continue
			}
			// also filter "+/#" etc already not matched because topic first level is $SYS, + would have matched but we skip
			filtered = append(filtered, s)
		}
		result = filtered
	}
	return result
}

// IsValidFilter validates filter per MQTT spec.
func IsValidFilter(f string) bool {
	if f == "" {
		return false
	}
	levels := strings.Split(f, "/")
	for i, lv := range levels {
		if lv == "" {
			// empty level allowed? MQTT allows empty? e.g., "a//b" is valid but we allow
			continue
		}
		if lv == "#" {
			if i != len(levels)-1 {
				return false
			}
		} else if strings.Contains(lv, "#") || strings.Contains(lv, "+") {
			// wildcard must occupy entire level
			if lv != "+" && lv != "#" {
				return false
			}
		}
	}
	return true
}

func IsValidTopic(topic string) bool {
	if topic == "" {
		return false
	}
	if strings.Contains(topic, "+") || strings.Contains(topic, "#") {
		return false
	}
	return true
}
