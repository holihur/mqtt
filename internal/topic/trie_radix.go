//go:build radix_trie

package topic

// 备用实现 (build tag: radix_trie): 压缩/Radix 前缀树。
//
// 与层级树不同，连续的字面层级被合并成一条边的 label（可含 '/'），通配符
// + / # 各自占独立单层边。不变量：同一节点的子边首 token 互不相同，因此
// 插入只需处理单条候选边，按共同前缀分裂 (radix split)。语义与 Trie 接口一致。
//
//	A/B: go test -bench=. -run=^$ ./internal/topic          # 层级
//	     go test -bench=. -run=^$ -tags flat_trie  ./internal/topic
//	     go test -bench=. -run=^$ -tags radix_trie ./internal/topic

import (
	"strings"
	"sync"
)

type radixTrie struct {
	mu   sync.RWMutex
	root *rnode
}

// rnode: edges 按 label 存；byFirst 索引首 token → 边 (不变量: 每首 token 至多
// 一条边)。节点可同时持有终止订阅 subs 与子边。
type rnode struct {
	subs    map[string]*SubEntry
	edges   map[string]*redge
	byFirst map[string]*redge
}

type redge struct {
	label string
	lvls  []string // label 的 token 数组 (纯字面 run，或单个 "+"/"#")
	child *rnode
}

func newRadixTrie() Trie { return &radixTrie{root: newRNode()} }

// NewTrie 由 build tag 选择实现 (本文件: radix 压缩树)。
func NewTrie() Trie { return newRadixTrie() }

func newRNode() *rnode {
	return &rnode{subs: make(map[string]*SubEntry), edges: make(map[string]*redge), byFirst: make(map[string]*redge)}
}

func splitLvls(label string) []string { return strings.Split(label, "/") }

func (n *rnode) addEdge(lvls []string, child *rnode) {
	label := strings.Join(lvls, "/")
	e := &redge{label: label, lvls: lvls, child: child}
	n.edges[label] = e
	n.byFirst[lvls[0]] = e
}

func (n *rnode) delEdge(label, first string) {
	delete(n.edges, label)
	delete(n.byFirst, first)
}

func (t *radixTrie) Add(filter, clientID string, qos byte, noLocal bool) {
	entry := &SubEntry{ClientID: clientID, Filter: filter, QoS: qos, NoLocal: noLocal}
	t.mu.Lock()
	insertUnits(t.root, splitLvls(filter), entry)
	t.mu.Unlock()
}

// insertUnits 按单元插入: 连续字面层构成一个 run，可能与已有边共享前缀从而被
// 分裂；'+'/'#' 独立单 token 边。
func insertUnits(n *rnode, toks []string, entry *SubEntry) {
	if len(toks) == 0 {
		n.subs[subKey(entry.ClientID, entry.Filter)] = entry
		return
	}
	// 通配符 token: 独立单 token 边
	if toks[0] == "+" || toks[0] == "#" {
		if e, ok := n.byFirst[toks[0]]; ok {
			insertUnits(e.child, toks[1:], entry)
		} else {
			child := newRNode()
			n.addEdge(toks[:1], child)
			insertUnits(child, toks[1:], entry)
		}
		return
	}
	// 字面 run
	run := 0
	for run < len(toks) && toks[run] != "+" && toks[run] != "#" {
		run++
	}
	runToks := toks[:run]
	// 首 token 相同的既有边 (不变量保证至多一条)
	if e := n.byFirst[runToks[0]]; e != nil {
		p := commonPrefix(runToks, e.lvls)
		if p == len(e.lvls) {
			// 边被 run 完全消费: 下沉继续 (剩余 token 进入子节点)
			insertUnits(e.child, toks[len(e.lvls):], entry)
			return
		}
		// 边比共享前缀长: 在 p 处分裂
		n.delEdge(e.label, e.lvls[0])
		mid := newRNode()
		mid.addEdge(e.lvls[p:], e.child)
		if p < len(runToks) {
			// 前缀边 (p) 挂回本节点，run 剩余部分继续在 mid 插入
			n.addEdge(runToks[:p], mid)
			insertUnits(mid, toks[p:], entry)
		} else {
			// run 恰好等于共享前缀长度 (run 比边短/终止于此)
			n.addEdge(runToks, mid)
			insertUnits(mid, toks[run:], entry)
		}
		return
	}
	// 无共享边: 新建整段 run 边；run 后还有剩余则继续下沉
	child := newRNode()
	n.addEdge(runToks, child)
	insertUnits(child, toks[run:], entry)
}

func commonPrefix(a, b []string) int {
	p := 0
	for p < len(a) && p < len(b) && a[p] == b[p] {
		p++
	}
	return p
}

// Match returns all subscribers whose filter matches topic.
func (t *radixTrie) Match(topic string) []*SubEntry {
	lvls := splitLvls(topic)
	t.mu.RLock()
	defer t.mu.RUnlock()
	var result []*SubEntry
	matchLevels(t.root, lvls, 0, &result)
	return filterSysSubs(topic, result)
}

func matchLevels(n *rnode, lvls []string, idx int, out *[]*SubEntry) {
	// '#' 边: 匹配任意剩余层 (含零层)，filter "…/#"
	if e, ok := n.byFirst["#"]; ok {
		for _, s := range e.child.subs {
			*out = append(*out, entryCopy(s))
		}
	}
	if idx == len(lvls) {
		for _, s := range n.subs {
			*out = append(*out, entryCopy(s))
		}
		return
	}
	// '+' 边: 消费恰好一层
	if e, ok := n.byFirst["+"]; ok {
		matchLevels(e.child, lvls, idx+1, out)
	}
	// 字面边: 整段比对 (主题同位置的字面 token 需全等)
	rem := lvls[idx:]
	for _, e := range n.edges {
		if e.lvls[0] == "+" || e.lvls[0] == "#" {
			continue
		}
		if len(e.lvls) <= len(rem) && lvlsEqual(rem[:len(e.lvls)], e.lvls) {
			matchLevels(e.child, lvls, idx+len(e.lvls), out)
		}
	}
}

func lvlsEqual(a, b []string) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (t *radixTrie) Remove(filter, clientID string) {
	t.mu.Lock()
	removeUnits(t.root, splitLvls(filter), subKey(clientID, filter))
	t.mu.Unlock()
}

// removeUnits 沿插入时的结构删除订阅并剪除空枝。
func removeUnits(n *rnode, toks []string, key string) {
	if len(toks) == 0 {
		delete(n.subs, key)
		return
	}
	if toks[0] == "+" || toks[0] == "#" {
		e := n.byFirst[toks[0]]
		if e == nil {
			return
		}
		removeUnits(e.child, toks[1:], key)
		if len(e.child.subs) == 0 && len(e.child.edges) == 0 {
			n.delEdge(e.label, e.lvls[0])
		}
		return
	}
	run := 0
	for run < len(toks) && toks[run] != "+" && toks[run] != "#" {
		run++
	}
	e := n.byFirst[toks[0]]
	if e == nil {
		return
	}
	p := commonPrefix(toks[:run], e.lvls)
	if p == 0 || p != len(e.lvls) {
		// 该边与待删 filter 不完全同构 (结构分裂点不同): 过滤器中不可能出现
		// 于真实结构，直接返回。
		return
	}
	removeUnits(e.child, toks[len(e.lvls):], key)
	if len(e.child.subs) == 0 && len(e.child.edges) == 0 {
		n.delEdge(e.label, e.lvls[0])
	}
}

// Subscriptions returns all subscription entries (for management API).
func (t *radixTrie) Subscriptions() []*SubEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []*SubEntry
	var walk func(n *rnode)
	walk = func(n *rnode) {
		for _, s := range n.subs {
			out = append(out, entryCopy(s))
		}
		for _, e := range n.edges {
			walk(e.child)
		}
	}
	walk(t.root)
	return out
}

// SubscriptionsFor returns all subscription entries of a given client (for management API).
func (t *radixTrie) SubscriptionsFor(clientID string) []*SubEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []*SubEntry
	var walk func(n *rnode)
	walk = func(n *rnode) {
		for _, s := range n.subs {
			if s.ClientID == clientID {
				out = append(out, entryCopy(s))
			}
		}
		for _, e := range n.edges {
			walk(e.child)
		}
	}
	walk(t.root)
	return out
}
