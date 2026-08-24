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
	_, err := codec.Decode(data)
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
