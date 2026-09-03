package broker

// P2 conformance: MQTT 5 语义回归 (对齐 Paho/Eclipse 场景), 覆盖 P0 修复项:
//  1. 转发属性 (Request/Response + User/Content/PayloadFormat) 透传给 v5 订阅者
//  2. Retain Handling 0/1/2 补发语义
//  3. Retain As Published (RAP) 转发保留位 / v3 清位
//  4. 延迟遗嘱: 会话恢复时丢弃
//  5. 会话过期清理 (sweeper)
//
// 全部走真实 TCP wire。

import (
	"context"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/parser"
	"mqtt/internal/persistence"
	"mqtt/internal/session"
)

// mqttClient 最小 wire 客户端。
type mqttClient struct {
	t       *testing.T
	c       net.Conn
	pr      *parser.Reader
	ver     byte
	pending []*codec.Packet // 读取时顺带收到的非预期 PUBLISH (如先于 SUBACK 的 retain)
}

func dialMqtt(t *testing.T, addr, id string, version byte, clean bool, will *codec.Will, props *codec.Properties) *mqttClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	pkt := &codec.Packet{Type: codec.TypeCONNECT, Version: version, ProtocolName: "MQTT",
		ConnectFlags: codec.ConnectFlags{CleanSession: clean, WillFlag: will != nil}, KeepAlive: 60,
		ClientID: id, Will: will, Properties: props}
	if version == codec.ProtocolV5 {
		pkt.ProtocolLevel = 5
	} else {
		pkt.ProtocolLevel = 4
	}
	if will != nil {
		pkt.ConnectFlags.WillQoS = will.QoS
		pkt.ConnectFlags.WillRetain = will.Retain
	}
	data, err := codec.Encode(pkt)
	if err != nil {
		t.Fatalf("encode CONNECT: %v", err)
	}
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	mc := &mqttClient{t: t, c: conn, pr: parser.NewReader(conn, 1<<20), ver: version}
	frame, err := mc.nextFrame(2 * time.Second)
	if err != nil {
		t.Fatalf("read CONNACK: %v", err)
	}
	if _, err := codec.DecodeWithVersion(frame, version); err != nil {
		t.Fatalf("decode CONNACK: %v", err)
	}
	return mc
}

func (m *mqttClient) send(pkt *codec.Packet) {
	m.t.Helper()
	data, err := codec.Encode(pkt)
	if err != nil {
		m.t.Fatalf("encode: %v", err)
	}
	if _, err := m.c.Write(data); err != nil {
		m.t.Fatalf("write: %v", err)
	}
}

func (m *mqttClient) nextFrame(d time.Duration) ([]byte, error) {
	_ = m.c.SetReadDeadline(time.Now().Add(d))
	frame, err := m.pr.ReadFrame()
	return frame, err
}

func (m *mqttClient) nextPacket(d time.Duration) (*codec.Packet, error) {
	frame, err := m.nextFrame(d)
	if err != nil {
		return nil, err
	}
	return codec.DecodeWithVersion(frame, m.ver)
}

// expectPublish 返回下一条 PUBLISH (优先消费缓存，再读线)。
func (m *mqttClient) expectPublish(d time.Duration) *codec.Packet {
	m.t.Helper()
	if len(m.pending) > 0 {
		p := m.pending[0]
		m.pending = m.pending[1:]
		return p
	}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		remain := time.Until(deadline)
		if remain < 5*time.Millisecond {
			break
		}
		p, err := m.nextPacket(remain)
		if err != nil {
			break
		}
		if p.Type == codec.TypePUBLISH {
			return p
		}
	}
	m.t.Fatalf("no PUBLISH within %v", d)
	return nil
}

func (m *mqttClient) subscribe(filters []codec.Subscription) {
	m.t.Helper()
	pkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: m.ver, PacketID: 1, Subscriptions: filters}
	m.send(pkt)
	// 读到 SUBACK 为止；顺带的 retain 补发先缓存 (服务端可能在 SUBACK 前/后发送)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p, err := m.nextPacket(time.Until(deadline))
		if err != nil {
			m.t.Fatalf("read SUBACK: %v", err)
		}
		if p.Type == codec.TypeSUBACK {
			return
		}
		if p.Type == codec.TypePUBLISH {
			m.pending = append(m.pending, p)
		}
	}
	m.t.Fatal("no SUBACK within 2s")
}

func u32(v uint32) *uint32  { return &v }
func u16(v uint16) *uint16  { return &v }
func u8(v byte) *byte       { return &v }
func strp(s string) *string { return &s }

func TestConformanceV5ForwardPublishProps(t *testing.T) {
	b := startTestBroker(t, "127.0.0.1:19971")
	_ = b
	addr := "127.0.0.1:19971"

	subV5 := dialMqtt(t, addr, "prop-sub5", codec.ProtocolV5, true, nil, nil)
	subV5.subscribe([]codec.Subscription{{Filter: "props/#", QoS: 0}})
	subV3 := dialMqtt(t, addr, "prop-sub3", codec.ProtocolV311, true, nil, nil)
	subV3.subscribe([]codec.Subscription{{Filter: "props/#", QoS: 0}})

	pub := dialMqtt(t, addr, "prop-pub", codec.ProtocolV5, true, nil, nil)
	props := &codec.Properties{
		ResponseTopic:          strp("reply/topic"),
		CorrelationData:        []byte{1, 2, 3},
		ContentType:            strp("application/json"),
		PayloadFormatIndicator: u8(1),
		User:                   []codec.UserProperty{{Key: "tk", Val: "tv"}},
	}
	pub.send(&codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "props/a", QoS: 0, Payload: []byte("hi"), PubProps: props})

	// v5 订阅者应收到转发属性
	p := subV5.expectPublish(2 * time.Second)
	if p.PubProps == nil || p.PubProps.ResponseTopic == nil || *p.PubProps.ResponseTopic != "reply/topic" {
		t.Fatalf("v5 sub missing ResponseTopic: %+v", p.PubProps)
	}
	if len(p.PubProps.CorrelationData) != 3 || p.PubProps.CorrelationData[2] != 3 {
		t.Fatalf("correlation data not forwarded: %v", p.PubProps.CorrelationData)
	}
	if p.PubProps.ContentType == nil || *p.PubProps.ContentType != "application/json" {
		t.Fatalf("content type not forwarded")
	}
	if p.PubProps.PayloadFormatIndicator == nil || *p.PubProps.PayloadFormatIndicator != 1 {
		t.Fatalf("payload format not forwarded")
	}
	if len(p.PubProps.User) != 1 || p.PubProps.User[0].Key != "tk" || p.PubProps.User[0].Val != "tv" {
		t.Fatalf("user property not forwarded: %+v", p.PubProps.User)
	}
	if p.Topic != "props/a" || string(p.Payload) != "hi" {
		t.Fatalf("bad payload/topic")
	}
	// v3 订阅者: 无属性, 载荷完整
	p3 := subV3.expectPublish(2 * time.Second)
	if string(p3.Payload) != "hi" {
		t.Fatalf("v3 payload mismatch: %q", p3.Payload)
	}
}

func TestConformanceV5RetainHandling(t *testing.T) {
	b := startTestBroker(t, "127.0.0.1:19972")
	_ = b
	addr := "127.0.0.1:19972"

	// 造一条 retain
	seed := dialMqtt(t, addr, "rh-seed", codec.ProtocolV5, true, nil, nil)
	seed.send(&codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "rh/a", QoS: 0, Payload: []byte("R1"), Retain: true})
	time.Sleep(100 * time.Millisecond)

	// RH=2: 从不补发
	rh2 := dialMqtt(t, addr, "rh2", codec.ProtocolV5, true, nil, nil)
	rh2.subscribe([]codec.Subscription{{Filter: "rh/a", QoS: 0, RetainHandling: 2}})
	_ = rh2.c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := rh2.nextFrame(300 * time.Millisecond); err == nil {
		t.Fatal("RH=2 must not send retained")
	}

	// RH=0: 首次即补发
	rh0 := dialMqtt(t, addr, "rh0", codec.ProtocolV5, true, nil, nil)
	rh0.subscribe([]codec.Subscription{{Filter: "rh/a", QoS: 0, RetainHandling: 0}})
	p := rh0.expectPublish(1 * time.Second)
	if string(p.Payload) != "R1" || !p.Retain {
		t.Fatalf("RH=0 first sub: expected retained R1, got %+v", p)
	}

	// RH=1: 已存在订阅时重订 → 不补发; 首次订阅 (新 filter) → 补发
	rh1 := dialMqtt(t, addr, "rh1", codec.ProtocolV5, true, nil, nil)
	rh1.subscribe([]codec.Subscription{{Filter: "rh/a", QoS: 0, RetainHandling: 1}}) // 首次 → 补发
	p = rh1.expectPublish(1 * time.Second)
	if string(p.Payload) != "R1" {
		t.Fatalf("RH=1 first sub should send retained")
	}
	rh1.subscribe([]codec.Subscription{{Filter: "rh/a", QoS: 0, RetainHandling: 1}}) // 重订 → 不发
	_ = rh1.c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := rh1.nextFrame(300 * time.Millisecond); err == nil {
		t.Fatal("RH=1 resubscribe must not resend retained")
	}
	// RH=1 新 filter → 补发
	rh1.send(&codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "rh/b", QoS: 0, Payload: []byte("R2"), Retain: true})
	time.Sleep(100 * time.Millisecond)
	rh1.subscribe([]codec.Subscription{{Filter: "rh/b", QoS: 0, RetainHandling: 1}})
	p = rh1.expectPublish(1 * time.Second)
	if string(p.Payload) != "R2" {
		t.Fatalf("RH=1 new filter should send retained")
	}
}

func TestConformanceV5RAPForward(t *testing.T) {
	b := startTestBroker(t, "127.0.0.1:19973")
	_ = b
	addr := "127.0.0.1:19973"

	rap := dialMqtt(t, addr, "rap-on", codec.ProtocolV5, true, nil, nil)
	rap.subscribe([]codec.Subscription{{Filter: "rap/#", QoS: 0, RetainAsPublished: true}})
	norap := dialMqtt(t, addr, "rap-off", codec.ProtocolV5, true, nil, nil)
	norap.subscribe([]codec.Subscription{{Filter: "rap/#", QoS: 0}})
	v3 := dialMqtt(t, addr, "rap-v3", codec.ProtocolV311, true, nil, nil)
	v3.subscribe([]codec.Subscription{{Filter: "rap/#", QoS: 0}})

	pub := dialMqtt(t, addr, "rap-pub", codec.ProtocolV5, true, nil, nil)
	pub.send(&codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "rap/x", QoS: 0, Payload: []byte("m"), Retain: true})

	p1 := rap.expectPublish(2 * time.Second)
	if !p1.Retain {
		t.Fatal("RAP=1 subscriber should receive Retain=1")
	}
	p2 := norap.expectPublish(2 * time.Second)
	if p2.Retain {
		t.Fatal("RAP=0 subscriber should receive Retain=0")
	}
	p3 := v3.expectPublish(2 * time.Second)
	if p3.Retain {
		t.Fatal("v3 subscriber must receive Retain=0")
	}
}

func TestConformanceV5WillDelayCancelledOnReconnect(t *testing.T) {
	b := startTestBroker(t, "127.0.0.1:19974")
	addr := "127.0.0.1:19974"

	sub := dialMqtt(t, addr, "wc-sub", codec.ProtocolV5, true, nil, nil)
	sub.subscribe([]codec.Subscription{{Filter: "will/delay", QoS: 0}})

	// 持久客户端带延迟遗嘱 (3s) 后异常断开
	will := &codec.Will{Topic: "will/delay", Payload: []byte("delayed-will"), QoS: 0, DelayInterval: 3}
	wc := dialMqtt(t, addr, "wc", codec.ProtocolV5, false, will, nil)
	_ = wc.c.Close()

	time.Sleep(300 * time.Millisecond)
	// 确认已持久化待投递遗嘱
	if list, err := b.store.ListPendingWills(context.Background()); err != nil || len(list) != 1 {
		t.Fatalf("expected 1 pending will, got %d err=%v", len(list), err)
	}
	// 同 clientID 会话恢复 → 遗嘱应被丢弃
	re := dialMqtt(t, addr, "wc", codec.ProtocolV5, false, nil, nil)
	defer re.c.Close()
	time.Sleep(200 * time.Millisecond)
	if list, err := b.store.ListPendingWills(context.Background()); err != nil || len(list) != 0 {
		t.Fatalf("pending will should be cancelled after reconnect, got %d err=%v", len(list), err)
	}
	_ = sub.c.SetReadDeadline(time.Now().Add(800 * time.Millisecond))
	if _, err := sub.nextFrame(800 * time.Millisecond); err == nil {
		t.Fatal("cancelled delayed will must not be delivered")
	}
}

func TestConformanceSessionExpirySweep(t *testing.T) {
	store := persistence.NewMemoryStore()
	cfg := Config{NodeID: "sweep", AllowAnonymous: true, MaxPublishPerSec: 1 << 30}
	b, err := NewWithOptions(cfg, WithStore(store))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := b.StartAsync(ctx); err != nil {
		t.Fatal(err)
	}
	// 注入一个已过期 (1s 会话, 离线 2s) 的持久会话
	ses := session.NewSession("exp-1", codec.ProtocolV5, false, 1)
	ses.SetSubscription("a/b", 0)
	ses.Mu.Lock()
	ses.Connected = false
	ses.OfflineSince = time.Now().Add(-2 * time.Second)
	ses.Mu.Unlock()
	b.mu.Lock()
	b.sessions["exp-1"] = ses
	b.mu.Unlock()
	b.trie.Add("a/b", "exp-1", 0, false)
	if err := store.SaveSession(ctx, ses); err != nil {
		t.Fatal(err)
	}

	b.sweepExpiredSessions()

	b.mu.RLock()
	_, still := b.sessions["exp-1"]
	b.mu.RUnlock()
	if still {
		t.Fatal("expired session should be swept from memory")
	}
	if got, err := store.GetSession(ctx, "exp-1"); err != nil || got != nil {
		t.Fatalf("expired session should be removed from store: got=%v err=%v", got, err)
	}
	// 订阅也应从 trie 移除
	if len(b.trie.Match("a/b")) != 0 {
		t.Fatal("subscriptions of expired session should be removed from trie")
	}
}
