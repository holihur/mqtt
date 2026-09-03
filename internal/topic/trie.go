// Package topic 提供 MQTT 主题订阅注册表（前缀树）及其匹配语义。
//
// # 实现可切换（build tags）
//
// 默认编译使用按 topic 层级的前缀树实现（trie_level.go）。可用
//
//	-tags flat_trie
//
// 切换为基于 filter 全量扫描的扁平实现（trie_flat.go），便于 A/B 压测两种
// 结构在订阅量、fan-out 下的差异：
//
//	go test -bench=. ./internal/topic            # 默认 (层级 trie)
//	go test -bench=. -tags flat_trie ./internal/topic
//
// 两个实现满足同一个 Trie 接口与同一套 MQTT 匹配语义（+/#、$ 前缀规则）。
package topic

import (
	"strings"
)

// SubEntry 是单个订阅条目。
type SubEntry struct {
	ClientID string
	Filter   string
	QoS      byte
	NoLocal  bool
}

// Trie 是订阅注册表接口：按 filter 注册客户端，并回答某具体主题会命中哪些
// 订阅。实现必须自身线程安全。
type Trie interface {
	// Add 注册/更新客户端对 filter 的订阅 (QoS/noLocal 覆盖旧值)。
	Add(filter, clientID string, qos byte, noLocal bool)
	// Remove 移除客户端对 filter 的订阅。
	Remove(filter, clientID string)
	// Match 返回订阅 filter 匹配给定具体主题 (含 +/# 与 $ 规则) 的全部条目。
	Match(topic string) []*SubEntry
	// Subscriptions 返回全部订阅条目 (管理 API 用)。
	Subscriptions() []*SubEntry
	// SubscriptionsFor 返回某客户端的全部订阅条目 (管理 API 用)。
	SubscriptionsFor(clientID string) []*SubEntry
}

// NewTrie 由 build tag 选择实现：
//   - 默认 (无 tag): 按层前缀树 trie_level.go
//   - -tags flat_trie: 扁平实现 trie_flat.go
//
// (实现分别位于 trie_level.go / trie_flat.go，受对应 build tag 约束。)

// ---------------------------------------------------------------------------
// 无状态校验/匹配工具 (两个实现共用)
// ---------------------------------------------------------------------------

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

// MatchFilter reports whether topic matches filter without allocating a Trie.
// Stateless O(levels) check for hot paths (shared subs, retained replay).
func MatchFilter(topic, filter string) bool {
	if filter == "#" {
		// # matches everything except $SYS
		if strings.HasPrefix(topic, "$") {
			return false
		}
		return true
	}
	if filter == topic {
		return true
	}
	tLevels := strings.Split(topic, "/")
	fLevels := strings.Split(filter, "/")
	for i, f := range fLevels {
		if f == "#" {
			return i == len(fLevels)-1
		}
		if i >= len(tLevels) {
			return false
		}
		if f == "+" {
			continue
		}
		if f != tLevels[i] {
			return false
		}
	}
	return len(tLevels) == len(fLevels)
}

// subKey 构造 map key：同一 client 对同一 filter 至多一条。
func subKey(clientID, filter string) string { return clientID + "#" + filter }

// entryCopy 返回浅拷贝，避免外部持有内部指针被并发修改。
func entryCopy(s *SubEntry) *SubEntry {
	if s == nil {
		return nil
	}
	c := *s
	return &c
}

// filterSysSubs 应用 MQTT 的 $ 前缀规则：主题以 "$" 开头时，滤除 filter 为
// "#" 或首层为通配符 (+/#) 的订阅。三个实现共用，保证语义一致。
func filterSysSubs(topic string, result []*SubEntry) []*SubEntry {
	if !strings.HasPrefix(topic, "$") || len(result) == 0 {
		return result
	}
	filtered := result[:0]
	for _, s := range result {
		if s.Filter == "#" || strings.HasPrefix(s.Filter, "+") || strings.HasPrefix(s.Filter, "#") {
			continue
		}
		filtered = append(filtered, s)
	}
	return filtered
}
