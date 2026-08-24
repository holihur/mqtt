package session

import (
	"sync"
	"time"
)

type Session struct {
	ClientID       string
	Version        byte
	CleanStart     bool
	ExpiryInterval uint32 // seconds, 0=expire on disconnect, 0xFFFFFFFF = never expire (v3 clean=false mapping)
	KeepAlive      uint16
	Will           *Will
	Username       string

	CreatedAt time.Time
	Connected bool
	NodeID    string // which broker node holds the connection

	Subscriptions map[string]byte // filter -> QoS

	// Inflight for QoS1/2
	Mu       sync.Mutex
	Inflight map[uint16]*InflightEntry
	NextID   uint16
	freeIDs  []uint16

	ReceiveMaximum    uint16
	MaximumPacketSize uint32
	TopicAliasMaximum uint16

	// Topic Alias maps (v5)
	AliasToTopic map[uint16]string
	TopicToAlias map[string]uint16
}

type Will struct {
	Topic         string
	Payload       []byte
	QoS           byte
	Retain        bool
	DelayInterval uint32
}

type InflightEntry struct {
	PacketID  uint16
	QoS       byte
	Topic     string
	Payload   []byte
	State     string // "qos1-pending", "qos2-publish", "qos2-pubrel"
	CreatedAt time.Time
	Dup       bool
}

func NewSession(clientID string, version byte, cleanStart bool, expiry uint32) *Session {
	return &Session{
		ReceiveMaximum:    65535,
		MaximumPacketSize: 1 << 20,
		TopicAliasMaximum: 0,
		ClientID:          clientID,
		Version:           version,
		CleanStart:        cleanStart,
		ExpiryInterval:    expiry,
		Subscriptions:     make(map[string]byte),
		Inflight:          make(map[uint16]*InflightEntry),
		AliasToTopic:      make(map[uint16]string),
		TopicToAlias:      make(map[string]uint16),
		CreatedAt:         time.Now(),
		NextID:            1,
	}
}

func (s *Session) NextPacketID() uint16 {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if len(s.freeIDs) > 0 {
		n := len(s.freeIDs) - 1
		id := s.freeIDs[n]
		s.freeIDs = s.freeIDs[:n]
		if _, exists := s.Inflight[id]; !exists {
			return id
		}
	}
	for i := 0; i < 65535; i++ {
		id := s.NextID
		s.NextID++
		if s.NextID == 0 {
			s.NextID = 1
		}
		if _, exists := s.Inflight[id]; !exists {
			return id
		}
	}
	return 0 // exhausted
}

func (s *Session) AddInflight(e *InflightEntry) {
	s.Mu.Lock()
	s.Inflight[e.PacketID] = e
	s.Mu.Unlock()
}
func (s *Session) RemoveInflight(id uint16) {
	s.Mu.Lock()
	delete(s.Inflight, id)
	if len(s.freeIDs) < 1024 {
		s.freeIDs = append(s.freeIDs, id)
	}
	s.Mu.Unlock()
}
func (s *Session) GetInflight(id uint16) (*InflightEntry, bool) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	e, ok := s.Inflight[id]
	return e, ok
}
func (s *Session) InflightCount() int { s.Mu.Lock(); defer s.Mu.Unlock(); return len(s.Inflight) }
func (s *Session) CanSend() bool {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return uint16(len(s.Inflight)) < s.ReceiveMaximum
}
