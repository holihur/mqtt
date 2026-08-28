package broker

import (
	"net"
	"net/http"
	"testing"
	"time"

	"mqtt/internal/auth"
	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

func TestSecurityDefaultDenyAnonymous(t *testing.T) {
	b := New(Config{NodeID: "sec-deny", TCPAddr: "127.0.0.1:12180", AllowAnonymous: false}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	conn, _ := net.Dial("tcp", "127.0.0.1:12180")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "attacker"}
	data, _ := codec.Encode(p)
	_, _ = conn.Write(data)
	buf := make([]byte, 64)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.ReasonCode == 0 {
			t.Fatalf("anonymous should be denied, got CONNACK 0")
		}
	}
	_ = conn.Close()
}

func TestSecurityWillACL(t *testing.T) {
	// ACL denies will topic, will should not be delivered
	// create acl that denies will topic
	_ = auth.NewFileACL // ensure import used
	store := persistence.NewMemoryStore()
	allow := &auth.SimpleAuth{Users: map[string]string{"u": "p"}, ACL: map[string][]string{"victim": {"allowed/#"}}}
	b := New(Config{NodeID: "sec-will", TCPAddr: "127.0.0.1:12181", AllowAnonymous: true}, store, allow)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	// sub to will topic
	sub, _ := net.Dial("tcp", "127.0.0.1:12181")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-will-acl"}
	data, _ := codec.Encode(p)
	_, _ = sub.Write(data)
	buf := make([]byte, 2048)
	_, _ = sub.Read(buf)
	subPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "forbidden/will", QoS: 0}}}
	data, _ = codec.Encode(subPkt)
	_, _ = sub.Write(data)
	_, _ = sub.Read(buf)
	// publisher with will to forbidden topic
	pub, _ := net.Dial("tcp", "127.0.0.1:12181")
	will := &codec.Will{Topic: "forbidden/will", Payload: []byte("x"), QoS: 0}
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true, WillFlag: true}, KeepAlive: 60, ClientID: "victim", Username: "u", Password: []byte("p"), Will: will, Properties: &codec.Properties{}}
	data, _ = codec.Encode(p2)
	_, _ = pub.Write(data)
	_, _ = pub.Read(buf)
	_ = pub.Close()
	time.Sleep(1200 * time.Millisecond)
	sub.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, _ := sub.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypePUBLISH && string(pkt.Payload) == "x" {
			t.Fatalf("will ACL bypass: forbidden will delivered")
		}
	}
}

func TestSecuritySysPrefixReserved(t *testing.T) {
	b := New(Config{NodeID: "sec-sys", TCPAddr: "127.0.0.1:12182", AllowAnonymous: true}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	sub, _ := net.Dial("tcp", "127.0.0.1:12182")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "sub-sys-reserved"}
	data, _ := codec.Encode(p)
	_, _ = sub.Write(data)
	buf := make([]byte, 2048)
	_, _ = sub.Read(buf)
	subPkt := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "$SYS/broker/uptime", QoS: 0}}}
	data, _ = codec.Encode(subPkt)
	_, _ = sub.Write(data)
	_, _ = sub.Read(buf)
	pub, _ := net.Dial("tcp", "127.0.0.1:12182")
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "pub-sys"}
	data, _ = codec.Encode(p2)
	_, _ = pub.Write(data)
	_, _ = pub.Read(buf)
	pubPkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "$SYS/broker/uptime", QoS: 0, Payload: []byte("spoof")}
	data, _ = codec.Encode(pubPkt)
	_, _ = pub.Write(data)
	time.Sleep(200 * time.Millisecond)
	sub.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, _ := sub.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && string(pkt.Payload) == "spoof" {
			t.Fatalf("$SYS spoof delivered")
		}
	}
}

func TestSecurityTopicAliasLimit(t *testing.T) {
	b := New(Config{NodeID: "sec-alias", TCPAddr: "127.0.0.1:12183", AllowAnonymous: true}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	conn, _ := net.Dial("tcp", "127.0.0.1:12183")
	rm := uint16(1)
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "alias-limit", Properties: &codec.Properties{TopicAliasMaximum: &rm}}
	data, _ := codec.Encode(p)
	_, _ = conn.Write(data)
	buf := make([]byte, 2048)
	_, _ = conn.Read(buf)
	// try alias 2 > maximum 1 should be rejected
	alias := uint16(2)
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "a/b", QoS: 1, PacketID: 1, Payload: []byte("x"), PubProps: &codec.Properties{TopicAlias: &alias}}
	data, _ = codec.Encode(pub)
	_, _ = conn.Write(data)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, _ := conn.Read(buf)
	if n > 0 {
		pkt, _ := codec.Decode(buf[:n])
		if pkt != nil && pkt.Type == codec.TypePUBACK && pkt.Reason == 0 {
			t.Fatalf("alias beyond maximum should be rejected")
		}
	}
}

func TestSecurityWillDelayCap(t *testing.T) {
	b := New(Config{NodeID: "sec-delay", TCPAddr: "127.0.0.1:12184", AllowAnonymous: true}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	conn, _ := net.Dial("tcp", "127.0.0.1:12184")
	will := &codec.Will{Topic: "will/delay", Payload: []byte("x"), DelayInterval: 0xFFFFFFFF}
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5, ConnectFlags: codec.ConnectFlags{CleanSession: true, WillFlag: true}, ClientID: "will-delay", Will: will, Properties: &codec.Properties{}}
	data, _ := codec.Encode(p)
	_, _ = conn.Write(data)
	buf := make([]byte, 2048)
	_, _ = conn.Read(buf)
	// check broker capped delay to 86400
	b.mu.RLock()
	sess, ok := b.sessions["will-delay"]
	b.mu.RUnlock()
	if !ok || sess.Will == nil || sess.Will.DelayInterval > 86400 {
		t.Fatalf("will delay not capped: %v", sess)
	}
}

func TestSecurityUnknownProperty(t *testing.T) {
	// craft raw packet with unknown property 0xFF
	raw := []byte{0x10, 0x0e, 0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x05, 0x02, 0x00, 0x3c, 0x02, 0xff, 0x00, 0x00, 0x03, 0x61, 0x62, 0x63}
	_, err := codec.Decode(raw)
	if err == nil {
		t.Fatalf("unknown property should be rejected")
	}
}

func TestSecurityUserPropertyLimit(t *testing.T) {
	props := &codec.Properties{}
	for i := 0; i < 11; i++ {
		props.User = append(props.User, codec.UserProperty{Key: "k", Val: "v"})
	}
	pkt := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV5, Topic: "a/b", QoS: 0, Payload: []byte("x"), PubProps: props}
	data, _ := codec.Encode(pkt)
	// This is a v5 packet, so enforce the limit through the version-aware path
	// the broker actually uses (generic Decode cannot know a QoS0 PUBLISH is v5,
	// so it must not speculate on properties to avoid corrupting v3 payloads).
	_, err := codec.DecodeWithVersion(data, codec.ProtocolV5)
	if err == nil {
		t.Fatalf("user property limit should be enforced")
	}
}

func TestSecurityPprofDisabledByDefault(t *testing.T) {
	// pprof should not listen when PprofAddr empty
	b := New(Config{NodeID: "sec-pprof", TCPAddr: "127.0.0.1:12185", PprofAddr: "", AllowAnonymous: true}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	_, err := http.Get("http://127.0.0.1:12185/debug/pprof/")
	if err == nil {
		t.Fatalf("pprof should be disabled")
	}
}

func TestSecurityWSCSRF(t *testing.T) {
	// WS CheckOrigin should deny browser Origin
	// We test via direct HTTP request with Origin header
	b := New(Config{NodeID: "sec-ws", TCPAddr: "127.0.0.1:12186", WSAddr: ":12187", AllowAnonymous: true}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	client := &http.Client{}
	req, _ := http.NewRequest("GET", "http://127.0.0.1:12187/", nil)
	req.Header.Set("Origin", "http://evil.com")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-WebSocket-Version", "13")
	resp, err := client.Do(req)
	if err == nil && resp.StatusCode == 101 {
		t.Fatalf("WS should deny cross-origin")
	}
}

// Regression: a persistent session must stay bound to the username that
// created it. Once the original connection is gone, a DIFFERENT username must
// not be able to reconnect with the same clientID and inherit the session
// (subscriptions + offline queue replay).
func TestSecuritySessionTakeoverRejected(t *testing.T) {
	a := &auth.SimpleAuth{Users: map[string]string{"alice": "pw1", "mallory": "pw2"}}
	b := New(Config{NodeID: "sec-takeover", TCPAddr: "127.0.0.1:12210", AllowAnonymous: false}, persistence.NewMemoryStore(), a)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	readConnack := func(conn net.Conn) *codec.Packet {
		buf := make([]byte, 256)
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, _ := conn.Read(buf)
		if n == 0 {
			return nil
		}
		pkt, _ := codec.Decode(buf[:n])
		return pkt
	}

	// victim: persistent session (clean=false)
	victim, _ := net.Dial("tcp", "127.0.0.1:12210")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4,
		ConnectFlags: codec.ConnectFlags{CleanSession: false, UsernameFlag: true, PasswordFlag: true}, ClientID: "victim", Username: "alice", Password: []byte("pw1")}
	data, _ := codec.Encode(p)
	victim.Write(data)
	if ack := readConnack(victim); ack == nil || ack.ReasonCode != 0 {
		t.Fatalf("victim connect should succeed, got %+v", ack)
	}
	// victim subscribes, then goes offline
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1,
		Subscriptions: []codec.Subscription{{Filter: "secret/#", QoS: 1}}}
	data, _ = codec.Encode(sub)
	victim.Write(data)
	victim.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 256)
	victim.Read(buf)
	victim.Close()
	time.Sleep(300 * time.Millisecond)

	// attacker: same clientID, different username, old connection offline
	attacker, _ := net.Dial("tcp", "127.0.0.1:12210")
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4,
		ConnectFlags: codec.ConnectFlags{CleanSession: false, UsernameFlag: true, PasswordFlag: true}, ClientID: "victim", Username: "mallory", Password: []byte("pw2")}
	data, _ = codec.Encode(p2)
	attacker.Write(data)
	ack := readConnack(attacker)
	if ack != nil && ack.ReasonCode == 0 {
		t.Fatalf("session takeover with different username must be rejected, got CONNACK 0")
	}
	attacker.Close()
}

// Regression: the broker must enforce its OWN inflight window instead of
// adopting the client-declared ReceiveMaximum (65535), otherwise a QoS1
// client that never PUBACKs pins unbounded payloads in memory.
func TestSecurityServerEnforcedReceiveMaximum(t *testing.T) {
	b := New(Config{NodeID: "sec-rm", TCPAddr: "127.0.0.1:12213", AllowAnonymous: true}, nil, nil)
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	conn, _ := net.Dial("tcp", "127.0.0.1:12213")
	rm := uint16(65535)
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV5, ProtocolName: "MQTT", ProtocolLevel: 5,
		ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "rm-client", Properties: &codec.Properties{ReceiveMaximum: &rm}}
	data, _ := codec.Encode(p)
	conn.Write(data)
	buf := make([]byte, 512)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, _ := conn.Read(buf)
	ack, _ := codec.Decode(buf[:n])
	if ack == nil || ack.Type != codec.TypeCONNACK {
		t.Fatalf("expected CONNACK, got %+v", ack)
	}
	if ack.ConnProperties == nil || ack.ConnProperties.ReceiveMaximum == nil {
		t.Fatal("CONNACK missing ReceiveMaximum")
	}
	if got := *ack.ConnProperties.ReceiveMaximum; got > 1000 {
		t.Fatalf("server must cap inflight window itself, advertised ReceiveMaximum = %d", got)
	}
	conn.Close()
}
