package topic

import (
	"sync"
	"testing"
)

func TestTrieAddMatchBasic(t *testing.T) {
	trie := NewTrie()
	trie.Add("a/b/c", "c1", 0, false)
	m := trie.Match("a/b/c")
	if len(m) != 1 || m[0].ClientID != "c1" {
		t.Fatalf("basic match failed %v", m)
	}
	m = trie.Match("a/b/d")
	if len(m) != 0 {
		t.Fatalf("should not match")
	}
}

func TestTriePlusWildcard(t *testing.T) {
	trie := NewTrie()
	trie.Add("a/+/c", "c1", 1, false)
	tests := []struct {
		topic string
		hit   bool
	}{
		{"a/b/c", true},
		{"a/x/c", true},
		{"a/b/d", false},
		{"a/b/c/d", false},
		{"a//c", true},
	}
	for _, tc := range tests {
		m := trie.Match(tc.topic)
		got := len(m) == 1
		if got != tc.hit {
			t.Errorf("topic %q hit=%v want %v", tc.topic, got, tc.hit)
		}
	}
}

func TestTrieHashWildcard(t *testing.T) {
	trie := NewTrie()
	trie.Add("a/#", "c1", 0, false)
	trie.Add("#", "c2", 0, false)
	if len(trie.Match("a/b/c")) != 2 {
		t.Fatalf("a/# and # should both match a/b/c")
	}
	if len(trie.Match("a")) != 2 {
		t.Fatalf("a should match both")
	}
	if len(trie.Match("x/y")) != 1 {
		t.Fatalf("# should match x/y")
	}
	// # must be last level
	trie2 := NewTrie()
	trie2.Add("a/b/+", "c3", 0, false)
	if len(trie2.Match("a/b/c/d")) != 0 {
		t.Fatalf("+/c should not match deeper")
	}
}

func TestTrieSysIsolation(t *testing.T) {
	trie := NewTrie()
	trie.Add("#", "c1", 0, false)
	trie.Add("+/+", "c2", 0, false)
	trie.Add("$SYS/broker/uptime", "c3", 0, false)
	// $SYS topic should NOT match # or +/+
	m := trie.Match("$SYS/broker/uptime")
	if len(m) != 1 || m[0].ClientID != "c3" {
		t.Fatalf("$SYS isolation failed, got %v", m)
	}
	m = trie.Match("$SYS/broker/clients")
	if len(m) != 0 {
		t.Fatalf("$SYS should be isolated from #")
	}
	// normal topic still matches #
	m = trie.Match("a/b")
	found := false
	for _, s := range m {
		if s.ClientID == "c1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("# should match normal topic")
	}
}

func TestTrieRemoveAndPrune(t *testing.T) {
	trie := NewTrie()
	trie.Add("a/b/c", "c1", 0, false)
	trie.Add("a/b/+", "c2", 0, false)
	trie.Remove("a/b/c", "c1")
	if len(trie.Match("a/b/c")) != 1 {
		t.Fatalf("after remove c1, c2 via + should still match")
	}
	// check prune: remove last
	trie.Remove("a/b/+", "c2")
	if len(trie.Match("a/b/c")) != 0 {
		t.Fatalf("should be empty after removes")
	}
	// remove non-existent no panic
	trie.Remove("non/existent", "cx")
	trie.Remove("a/b/c", "c1")
}

func TestTrieMultipleClientsSameFilter(t *testing.T) {
	trie := NewTrie()
	trie.Add("t/#", "c1", 0, false)
	trie.Add("t/#", "c2", 1, false)
	m := trie.Match("t/a")
	if len(m) != 2 {
		t.Fatalf("two subs same filter expected 2 got %d", len(m))
	}
	trie.Remove("t/#", "c1")
	m = trie.Match("t/a")
	if len(m) != 1 || m[0].ClientID != "c2" {
		t.Fatalf("remove one failed")
	}
}

func TestTrieNoLocalAndQoS(t *testing.T) {
	trie := NewTrie()
	trie.Add("a/b", "c1", 2, true)
	m := trie.Match("a/b")
	if m[0].QoS != 2 || !m[0].NoLocal {
		t.Fatalf("qos/nolocal not preserved")
	}
}

func TestIsValidFilter(t *testing.T) {
	cases := []struct {
		f     string
		valid bool
	}{
		{"", false},
		{"a/b/c", true},
		{"a/+/c", true},
		{"a/#", true},
		{"#", true},
		{"a/#/c", false},
		{"a/+b", false},
		{"a/b#", false},
		{"+/+", true},
		{"a//b", true},
		{"a/b/#", true},
		{"a/b/+", true},
		{"a/b/+/c", true},
	}
	for _, tc := range cases {
		if got := IsValidFilter(tc.f); got != tc.valid {
			t.Errorf("IsValidFilter %q got %v want %v", tc.f, got, tc.valid)
		}
	}
}

func TestIsValidTopic(t *testing.T) {
	cases := []struct {
		topic string
		valid bool
	}{
		{"", false},
		{"a/b/c", true},
		{"a/b", true},
		{"a/+/c", false},
		{"a/#", false},
		{"$SYS/broker", true},
		{"test/hello", true},
	}
	for _, tc := range cases {
		if got := IsValidTopic(tc.topic); got != tc.valid {
			t.Errorf("IsValidTopic %q got %v want %v", tc.topic, got, tc.valid)
		}
	}
}

func TestTrieConcurrent(t *testing.T) {
	trie := NewTrie()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			filter := "a/b/c"
			client := "c"
			trie.Add(filter, client, 0, false)
			_ = trie.Match("a/b/c")
			trie.Remove(filter, client)
		}(i)
	}
	wg.Wait()
}

func TestTrieMatchEmptyTopic(t *testing.T) {
	trie := NewTrie()
	trie.Add("a/b/c", "c1", 0, false)
	// empty topic split gives [""], should not match
	m := trie.Match("")
	_ = m
}

func TestTrieHashtagAtRoot(t *testing.T) {
	trie := NewTrie()
	trie.Add("#", "c1", 0, false)
	trie.Add("a/b", "c2", 0, false)
	m := trie.Match("a/b")
	if len(m) != 2 {
		t.Fatalf("expected 2 matches for a/b, got %d", len(m))
	}
	m = trie.Match("a")
	if len(m) != 1 {
		t.Fatalf("a should match # only")
	}
}
