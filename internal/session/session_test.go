package session

import (
	"sync"
	"testing"
)

func TestNewSession(t *testing.T) {
	s := NewSession("client1", 4, true, 0)
	if s.ClientID != "client1" {
		t.Fatalf("ClientID = %q", s.ClientID)
	}
	if s.Version != 4 {
		t.Fatalf("Version = %d", s.Version)
	}
	if !s.CleanStart {
		t.Fatal("CleanStart should be true")
	}
	if s.ExpiryInterval != 0 {
		t.Fatalf("ExpiryInterval = %d", s.ExpiryInterval)
	}
	if s.ReceiveMaximum != 65535 {
		t.Fatalf("ReceiveMaximum = %d", s.ReceiveMaximum)
	}
	if s.MaximumPacketSize != 1<<20 {
		t.Fatalf("MaximumPacketSize = %d", s.MaximumPacketSize)
	}
	if s.NextID != 1 {
		t.Fatalf("NextID = %d", s.NextID)
	}
	if s.Subscriptions == nil || len(s.Subscriptions) != 0 {
		t.Fatal("Subscriptions should be empty map")
	}
	if s.Inflight == nil || len(s.Inflight) != 0 {
		t.Fatal("Inflight should be empty map")
	}
	if s.AliasToTopic == nil || s.TopicToAlias == nil {
		t.Fatal("Alias maps should be initialized")
	}
	if s.CreatedAt.IsZero() {
		t.Fatal("CreatedAt should be set")
	}
}

func TestNextPacketID_Basic(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	id1 := s.NextPacketID()
	if id1 != 1 {
		t.Fatalf("first ID = %d, want 1", id1)
	}
	id2 := s.NextPacketID()
	if id2 != 2 {
		t.Fatalf("second ID = %d, want 2", id2)
	}
}

func TestNextPacketID_SkipsInflight(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	// pre-occupy ID 1
	s.AddInflight(&InflightEntry{PacketID: 1})
	id := s.NextPacketID()
	if id != 2 {
		t.Fatalf("skipped inflight: got %d, want 2", id)
	}
}

func TestNextPacketID_RecyclesFreeIDs(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	s.AddInflight(&InflightEntry{PacketID: 1})
	s.RemoveInflight(1) // puts 1 back in freeIDs
	id := s.NextPacketID()
	if id != 1 {
		t.Fatalf("recycled ID = %d, want 1", id)
	}
}

func TestNextPacketID_FreeIDInflightCollision(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	// Put ID 5 in freeIDs manually
	s.Mu.Lock()
	s.freeIDs = []uint16{5}
	s.Mu.Unlock()
	// Also occupy ID 5 in inflight
	s.AddInflight(&InflightEntry{PacketID: 5})
	id := s.NextPacketID()
	// freeID 5 is in inflight, so it falls through to sequential scan
	if id == 0 {
		t.Fatal("got exhausted (0)")
	}
	if id == 5 {
		t.Fatal("should not return inflight ID")
	}
}

func TestNextPacketID_WrapAround(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	s.Mu.Lock()
	s.NextID = 65535
	s.Mu.Unlock()
	id1 := s.NextPacketID()
	if id1 != 65535 {
		t.Fatalf("ID = %d, want 65535", id1)
	}
	id2 := s.NextPacketID()
	if id2 != 1 {
		t.Fatalf("wrapped ID = %d, want 1", id2)
	}
}

func TestAddInflight(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	e := &InflightEntry{PacketID: 10, QoS: 1, Topic: "t", State: "qos1-pending"}
	s.AddInflight(e)
	got, ok := s.GetInflight(10)
	if !ok || got != e {
		t.Fatal("GetInflight failed")
	}
}

func TestRemoveInflight(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	s.AddInflight(&InflightEntry{PacketID: 10})
	s.RemoveInflight(10)
	_, ok := s.GetInflight(10)
	if ok {
		t.Fatal("should be removed")
	}
}

func TestRemoveInflight_FreeIDCap(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	// fill freeIDs to 1024
	s.Mu.Lock()
	for i := uint16(0); i < 1024; i++ {
		s.freeIDs = append(s.freeIDs, i)
	}
	s.Mu.Unlock()
	s.RemoveInflight(9999) // should not append since cap reached
	s.Mu.Lock()
	for _, id := range s.freeIDs {
		if id == 9999 {
			s.Mu.Unlock()
			t.Fatal("should not have appended 9999")
		}
	}
	s.Mu.Unlock()
}

func TestInflightCount(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	if s.InflightCount() != 0 {
		t.Fatalf("count = %d, want 0", s.InflightCount())
	}
	s.AddInflight(&InflightEntry{PacketID: 1})
	s.AddInflight(&InflightEntry{PacketID: 2})
	if s.InflightCount() != 2 {
		t.Fatalf("count = %d, want 2", s.InflightCount())
	}
}

func TestCanSend(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	if !s.CanSend() {
		t.Fatal("should be able to send")
	}
	s.ReceiveMaximum = 1
	s.AddInflight(&InflightEntry{PacketID: 1})
	if s.CanSend() {
		t.Fatal("should not be able to send at limit")
	}
}

func TestConcurrentNextPacketID(t *testing.T) {
	s := NewSession("c", 4, true, 0)
	var wg sync.WaitGroup
	seen := make(map[uint16]bool)
	var mu sync.Mutex
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := s.NextPacketID()
			mu.Lock()
			if seen[id] {
				t.Errorf("duplicate ID %d", id)
			}
			seen[id] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != 100 {
		t.Fatalf("got %d unique IDs, want 100", len(seen))
	}
}

func TestWillStruct(t *testing.T) {
	w := &Will{Topic: "t", Payload: []byte("p"), QoS: 1, Retain: true, DelayInterval: 5}
	if w.Topic != "t" || w.QoS != 1 || !w.Retain || w.DelayInterval != 5 {
		t.Fatalf("Will fields mismatch: %+v", w)
	}
}

func TestInflightEntryStruct(t *testing.T) {
	e := &InflightEntry{
		PacketID: 42, QoS: 2, Topic: "t", Payload: []byte("p"),
		State: "qos2-publish", Dup: true,
	}
	if e.PacketID != 42 || e.QoS != 2 || e.State != "qos2-publish" || !e.Dup {
		t.Fatalf("InflightEntry fields mismatch: %+v", e)
	}
}
