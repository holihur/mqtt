package topic

// 差分测试: 对当前编译的实现做随机化 Add/Remove，并把每次 Match/Subscriptions
// 的结果与参照 registry 逐一比对。该测试对三种实现 (默认层级树 / flat_trie /
// radix_trie) 各自编译运行，防止语义漂移。

import (
	"math/rand"
	"strings"
	"testing"
)

var diffTokens = []string{"a", "b", "$SYS", "x", "y", "sensors", "home", "room1", "room2", "temp"}

func randFilter(r *rand.Rand) string {
	depth := r.Intn(4) + 1
	var lv []string
	for i := 0; i < depth; i++ {
		lv = append(lv, diffTokens[r.Intn(len(diffTokens))])
	}
	// 插入通配: 某些位置变成 + 或末尾变成 #
	w := r.Intn(4)
	if w == 1 && len(lv) > 1 {
		lv[r.Intn(len(lv)-1)] = "+"
	} else if w == 2 {
		lv[len(lv)-1] = "#"
	}
	f := strings.Join(lv, "/")
	if !IsValidFilter(f) {
		return "a/b"
	}
	return f
}

type diffEntry struct {
	clientID string
	filter   string
	qos      byte
	noLocal  bool
}

func TestDifferentialRandomOps(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	tr := NewTrie()
	reg := make(map[string]*diffEntry) // key clientID#filter
	var hist []string
	record := func(s string) {
		hist = append(hist, s)
		if len(hist) > 40 {
			hist = hist[1:]
		}
	}

	checkAll := func() {
		// Match 与暴力结果一致
		for _, topic := range []string{"a/b/c", "a", "b", "sensors/room1/temp", "x/y", "$SYS/a/b", "a//c", "home/room1", "temp", "a/b/#x"} {
			var want []string // "clientID|filter"
			for _, e := range reg {
				if strings.HasPrefix(topic, "$") && (e.filter == "#" || strings.HasPrefix(e.filter, "+") || strings.HasPrefix(e.filter, "#")) {
					continue
				}
				if MatchFilter(topic, e.filter) {
					want = append(want, e.clientID+"|"+e.filter)
				}
			}
			got := tr.Match(topic)
			if len(got) != len(want) {
				t.Fatalf("topic %q: got %d entries, brute %d", topic, len(got), len(want))
			}
			matched := map[string]bool{}
			for _, g := range got {
				matched[g.ClientID+"|"+g.Filter] = true
			}
			for _, w := range want {
				if !matched[w] {
					t.Fatalf("topic %q: missing %q in %+v", topic, w, got)
				}
			}
		}
		// Subscriptions 与 registry 一致
		if len(tr.Subscriptions()) != len(reg) {
			gotList := tr.Subscriptions()
			treeKeys := []string{}
			for _, g := range gotList {
				treeKeys = append(treeKeys, g.ClientID+"#"+g.Filter)
			}
			regKeys := []string{}
			for k := range reg {
				regKeys = append(regKeys, k)
			}
			t.Fatalf("Subscriptions %d != registry %d\nhist=%v\ntree=%v\nreg =%v", len(gotList), len(reg), hist, treeKeys, regKeys)
		}
		if len(tr.Subscriptions()) == len(reg) {
			// per-entry check
			seen2 := map[string]bool{}
			for _, e := range tr.Subscriptions() {
				seen2[e.ClientID+"#"+e.Filter] = true
			}
			for k := range reg {
				if !seen2[k] {
					t.Fatalf("missing entry %q; tree=%v", k, tr.Subscriptions())
				}
			}
		}
		if len(tr.SubscriptionsFor("c0")) != countFor(reg, "c0") {
			t.Fatalf("SubscriptionsFor(c0) mismatch")
		}
	}

	for step := 0; step < 3000; step++ {
		op := r.Intn(100)
		switch {
		case op < 55: // Add
			f := randFilter(r)
			cid := "c" + string(rune('a'+r.Intn(3)))
			key := cid + "#" + f
			tr.Add(f, cid, byte(r.Intn(3)), r.Intn(2) == 0)
			reg[key] = &diffEntry{clientID: cid, filter: f, qos: byte(r.Intn(3)), noLocal: r.Intn(2) == 0}
			record("ADD " + key)
		case op < 80: // Remove (随机移除一条真实存在的订阅)
			if len(reg) > 0 {
				keys := make([]string, 0, len(reg))
				for k := range reg {
					keys = append(keys, k)
				}
				k := keys[r.Intn(len(keys))]
				cid := reg[k].clientID
				f := reg[k].filter
				tr.Remove(f, cid)
				delete(reg, k)
				record("DEL " + k)
			}
		default:
			checkAll()
		}
	}
	checkAll()
}

func countFor(reg map[string]*diffEntry, cid string) int {
	n := 0
	for _, e := range reg {
		if e.clientID == cid {
			n++
		}
	}
	return n
}

// debugDump prints tree contents for debugging.
func debugDump(t *testing.T, tr Trie, label string) {
	t.Logf("%s:", label)
	for _, s := range tr.Subscriptions() {
		t.Logf("  %s#%s q%d nolocal=%v", s.ClientID, s.Filter, s.QoS, s.NoLocal)
	}
}
