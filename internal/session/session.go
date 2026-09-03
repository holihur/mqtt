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

	// OfflineSince 记录离线开始时间 (持久会话, 有限 ExpiryInterval 时用于过期清理)
	OfflineSince time.Time

	Subscriptions map[string]byte // filter -> QoS

	// SubOpts 每订阅的 v5 选项位 (NoLocal=1, RAP=2)。缺失 key 视为 0。
	// 与 Subscriptions 同 key，供重连重建与投递时读取。
	SubOpts map[string]uint8

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

	// Deleted 标记会话已被管理 API 显式删除：断开回调检测到后不再把会话写回 store。
	Deleted bool
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
		SubOpts:           make(map[string]uint8),
	}
}

// SubOpt 位定义 (SubOpts 值)。
const (
	SubOptNoLocal uint8 = 1 << iota // v5 No Local
	SubOptRAP                       // Retain As Published
)

// SetSubscriptionOpts 记录订阅的 QoS 与 v5 选项。
func (s *Session) SetSubscriptionOpts(filter string, qos byte, noLocal, rap bool) {
	s.Mu.Lock()
	s.Subscriptions[filter] = qos
	if noLocal || rap {
		if s.SubOpts == nil {
			s.SubOpts = make(map[string]uint8)
		}
		s.SubOpts[filter] = subOptsBits(noLocal, rap)
	} else if s.SubOpts != nil {
		delete(s.SubOpts, filter)
	}
	s.Mu.Unlock()
}

// SubOptsFor 返回某订阅的 v5 选项 (nil/缺失时均为 false)。
func (s *Session) SubOptsFor(filter string) (noLocal, rap bool) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.SubOpts == nil {
		return false, false
	}
	return subOptsRead(s.SubOpts[filter])
}

func subOptsBits(noLocal, rap bool) uint8 {
	var v uint8
	if noLocal {
		v |= SubOptNoLocal
	}
	if rap {
		v |= SubOptRAP
	}
	return v
}

func subOptsRead(v uint8) (noLocal, rap bool) {
	return v&SubOptNoLocal != 0, v&SubOptRAP != 0
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

// Subscriptions map is accessed from the subscriber's readLoop (writes) and
// publisher goroutines (reads) concurrently — always go through these helpers.

func (s *Session) SetSubscription(filter string, qos byte) {
	s.Mu.Lock()
	s.Subscriptions[filter] = qos
	s.Mu.Unlock()
}

func (s *Session) DeleteSubscription(filter string) {
	s.Mu.Lock()
	delete(s.Subscriptions, filter)
	if s.SubOpts != nil {
		delete(s.SubOpts, filter)
	}
	s.Mu.Unlock()
}

func (s *Session) GetSubscription(filter string) (byte, bool) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	q, ok := s.Subscriptions[filter]
	return q, ok
}

func (s *Session) SubscriptionsSnapshot() map[string]byte {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	out := make(map[string]byte, len(s.Subscriptions))
	for f, q := range s.Subscriptions {
		out[f] = q
	}
	return out
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
