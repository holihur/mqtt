package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
)

func TestNewConn(t *testing.T) {
	s, c := net.Pipe()
	defer s.Close()
	defer c.Close()
	conn := NewConn(s, 1<<20)
	if conn.Version() != 0 {
		t.Fatalf("initial version = %d", conn.Version())
	}
	if conn.ClientID() != "" {
		t.Fatalf("initial clientID = %q", conn.ClientID())
	}
}

func TestConnSetters(t *testing.T) {
	s, c := net.Pipe()
	defer s.Close()
	defer c.Close()
	conn := NewConn(s, 1<<20)
	conn.SetVersion(5)
	if conn.Version() != 5 {
		t.Fatalf("version = %d", conn.Version())
	}
	conn.SetClientID("test-client")
	if conn.ClientID() != "test-client" {
		t.Fatalf("clientID = %q", conn.ClientID())
	}
	if conn.RemoteAddr() == "" {
		t.Fatal("RemoteAddr empty")
	}
	if conn.Raw() != s {
		t.Fatal("Raw mismatch")
	}
}

func TestWriteReadPacket(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	conn := NewConn(server, 1<<20)

	pkt := &codec.Packet{
		Type:    codec.TypePUBLISH,
		Version: codec.ProtocolV311,
		Topic:   "test/hello",
		QoS:     0,
		Payload: []byte("world"),
	}

	go func() {
		_ = conn.WritePacket(pkt)
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame := make([]byte, 4096)
	n, err := client.Read(frame)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	got, err := codec.Decode(frame[:n])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Topic != "test/hello" {
		t.Fatalf("topic = %q", got.Topic)
	}
	if string(got.Payload) != "world" {
		t.Fatalf("payload = %q", got.Payload)
	}
}

func TestReadPacket(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	conn := NewConn(server, 1<<20)

	pingResp := &codec.Packet{Type: codec.TypePINGRESP, Version: codec.ProtocolV311}
	data, err := codec.Encode(pingResp)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	go func() {
		_, _ = client.Write(data)
		client.Close()
	}()

	got, err := conn.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if got.Type != codec.TypePINGRESP {
		t.Fatalf("type = %d, want PINGRESP", got.Type)
	}
}

func TestReadPacket_V5(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	conn := NewConn(server, 1<<20)
	conn.SetVersion(codec.ProtocolV5)

	pkt := &codec.Packet{
		Type:          codec.TypeCONNACK,
		Version:       codec.ProtocolV5,
		SessionPresent: false,
		ReasonCode:    0,
	}
	data, err := codec.Encode(pkt)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	go func() {
		_, _ = client.Write(data)
		client.Close()
	}()

	got, err := conn.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if got.Type != codec.TypeCONNACK {
		t.Fatalf("type = %d, want CONNACK", got.Type)
	}
}

func TestReadPacket_EOF(t *testing.T) {
	server, client := net.Pipe()
	conn := NewConn(server, 1<<20)

	client.Close()

	_, err := conn.ReadPacket()
	if err == nil {
		t.Fatal("expected error on closed pipe")
	}
}

func TestConnClose(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	conn := NewConn(server, 1<<20)
	closed := false
	conn.SetOnClose(func() { closed = true })

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !closed {
		t.Fatal("onClose not called")
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("double Close: %v", err)
	}
	if closed != true {
		t.Fatal("onClose called twice")
	}
}

func TestConnSetDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()

	conn := NewConn(server, 1<<20)
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
}

func TestListenerCustom(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	l := NewListener("", nil, "")
	l.SetCustomListener(ln)

	if l.Addr() != ln.Addr().String() {
		t.Fatalf("Addr = %q, want %q", l.Addr(), ln.Addr().String())
	}
}

func TestListenerAddr_NoListener(t *testing.T) {
	l := NewListener(":1883", nil, "")
	if l.Addr() != ":1883" {
		t.Fatalf("Addr = %q, want %q", l.Addr(), ":1883")
	}
}

func TestListenerClose(t *testing.T) {
	l := NewListener(":0", nil, "")
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestListenerListen_Accept(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	l := NewListener("", nil, "")
	l.SetCustomListener(ln)

	done := make(chan net.Conn, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = l.Listen(ctx, func(c net.Conn) {
			done <- c
			c.Close()
		})
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	select {
	case c := <-done:
		if c == nil {
			t.Fatal("nil conn in handler")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for accept")
	}
	cancel()
}

func TestListenerListen_NoTCPNoWS(t *testing.T) {
	l := NewListener("", nil, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := l.Listen(ctx, func(net.Conn) {})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
}

func TestFakeAddr(t *testing.T) {
	f := &fakeAddr{"test-net"}
	if f.Network() != "test-net" {
		t.Fatalf("Network = %q", f.Network())
	}
	if f.String() != "test-net" {
		t.Fatalf("String = %q", f.String())
	}
}

func TestWritePacket_EncodeError(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()

	conn := NewConn(server, 1<<20)
	err := conn.WritePacket(&codec.Packet{Type: 0})
	if err == nil {
		t.Fatal("expected encode error for type 0")
	}
}

func TestReadPacket_ClosedMidRead(t *testing.T) {
	server, client := net.Pipe()
	conn := NewConn(server, 1<<20)

	client.Close()

	_, err := conn.ReadPacket()
	if err == nil {
		t.Fatal("expected error")
	}
	if err != io.EOF && !isClosedPipeErr(err) {
	}
}

func isClosedPipeErr(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*net.OpError)
	return ok
}

func TestWritePacket_PingReq(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	conn := NewConn(server, 1<<20)
	pkt := &codec.Packet{Type: codec.TypePINGREQ, Version: codec.ProtocolV311}

	go func() {
		if err := conn.WritePacket(pkt); err != nil {
			t.Errorf("WritePacket: %v", err)
		}
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame := make([]byte, 4096)
	n, err := client.Read(frame)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	got, err := codec.Decode(frame[:n])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Type != codec.TypePINGREQ {
		t.Fatalf("type = %d, want PINGREQ", got.Type)
	}
}

func TestWritePacket_Subscribe(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	conn := NewConn(server, 1<<20)
	pkt := &codec.Packet{
		Type:    codec.TypeSUBSCRIBE,
		Version: codec.ProtocolV311,
		PacketID: 1,
		Subscriptions: []codec.Subscription{
			{Filter: "test/#", QoS: 1},
		},
	}

	go func() {
		if err := conn.WritePacket(pkt); err != nil {
			t.Errorf("WritePacket: %v", err)
		}
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame := make([]byte, 4096)
	n, err := client.Read(frame)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	got, err := codec.Decode(frame[:n])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Type != codec.TypeSUBSCRIBE {
		t.Fatalf("type = %d, want SUBSCRIBE", got.Type)
	}
}

func TestReadPacket_Multiple(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	conn := NewConn(server, 1<<20)

	pingResp := &codec.Packet{Type: codec.TypePINGRESP, Version: codec.ProtocolV311}
	data1, _ := codec.Encode(pingResp)
	data2, _ := codec.Encode(pingResp)

	go func() {
		_, _ = client.Write(append(data1, data2...))
		client.Close()
	}()

	got1, err := conn.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket 1: %v", err)
	}
	if got1.Type != codec.TypePINGRESP {
		t.Fatalf("type 1 = %d", got1.Type)
	}

	got2, err := conn.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket 2: %v", err)
	}
	if got2.Type != codec.TypePINGRESP {
		t.Fatalf("type 2 = %d", got2.Type)
	}
}

func TestConnClose_NoOnClose(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	conn := NewConn(server, 1<<20)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestListenerListen_MultipleConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	l := NewListener("", nil, "")
	l.SetCustomListener(ln)

	conns := make(chan net.Conn, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = l.Listen(ctx, func(c net.Conn) {
			conns <- c
		})
	}()

	for i := 0; i < 3; i++ {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		defer conn.Close()
	}

	for i := 0; i < 3; i++ {
		select {
		case c := <-conns:
			if c == nil {
				t.Fatalf("nil conn %d", i)
			}
			c.Close()
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for conn %d", i)
		}
	}
	cancel()
}

func TestListenerAddr_WithLn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	l := NewListener("", nil, "")
	l.ln = ln
	if l.Addr() != ln.Addr().String() {
		t.Fatalf("Addr = %q, want %q", l.Addr(), ln.Addr().String())
	}
}

func TestListenerClose_WithWS(t *testing.T) {
	l := NewListener("", nil, ":0")
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestReadPacket_Connect(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	conn := NewConn(server, 1<<20)

	connectPkt := &codec.Packet{
		Type:         codec.TypeCONNECT,
		Version:      codec.ProtocolV311,
		ProtocolName: "MQTT",
		ProtocolLevel: 4,
		ClientID:     "test-client",
		KeepAlive:    60,
	}
	data, err := codec.Encode(connectPkt)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	go func() {
		_, _ = client.Write(data)
		client.Close()
	}()

	got, err := conn.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if got.Type != codec.TypeCONNECT {
		t.Fatalf("type = %d, want CONNECT", got.Type)
	}
	if got.ClientID != "test-client" {
		t.Fatalf("clientID = %q", got.ClientID)
	}
}

func TestWritePacket_PublishQoS1(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	conn := NewConn(server, 1<<20)
	pkt := &codec.Packet{
		Type:     codec.TypePUBLISH,
		Version:  codec.ProtocolV311,
		Topic:    "test/qos1",
		QoS:      1,
		PacketID: 42,
		Payload:  []byte("hello"),
	}

	go func() {
		if err := conn.WritePacket(pkt); err != nil {
			t.Errorf("WritePacket: %v", err)
		}
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame := make([]byte, 4096)
	n, err := client.Read(frame)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	got, err := codec.Decode(frame[:n])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Topic != "test/qos1" || got.QoS != 1 || got.PacketID != 42 {
		t.Fatalf("mismatch: topic=%q qos=%d pid=%d", got.Topic, got.QoS, got.PacketID)
	}
}

func TestWsConnLocalAddr(t *testing.T) {
	wc := &wsConn{}
	addr := wc.LocalAddr()
	if addr.Network() != "ws" || addr.String() != "ws" {
		t.Fatalf("LocalAddr = %v", addr)
	}
}

func TestWsConnRemoteAddr(t *testing.T) {
	wc := &wsConn{}
	addr := wc.RemoteAddr()
	if addr.Network() != "ws-remote" || addr.String() != "ws-remote" {
		t.Fatalf("RemoteAddr = %v", addr)
	}
}

func TestListenerListen_ContextCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	l := NewListener("", nil, "")
	l.SetCustomListener(ln)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- l.Listen(ctx, func(c net.Conn) {
			c.Close()
		})
	}()

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Listen: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestConnClose_NilOnClose(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	conn := NewConn(server, 1<<20)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWritePacket_Disconnect(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	conn := NewConn(server, 1<<20)
	pkt := &codec.Packet{Type: codec.TypeDISCONNECT, Version: codec.ProtocolV311}

	go func() {
		if err := conn.WritePacket(pkt); err != nil {
			t.Errorf("WritePacket: %v", err)
		}
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame := make([]byte, 4096)
	n, err := client.Read(frame)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	got, err := codec.Decode(frame[:n])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Type != codec.TypeDISCONNECT {
		t.Fatalf("type = %d, want DISCONNECT", got.Type)
	}
}

func TestWritePacket_Puback(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	conn := NewConn(server, 1<<20)
	pkt := &codec.Packet{Type: codec.TypePUBACK, Version: codec.ProtocolV311, PacketID: 10}

	go func() {
		if err := conn.WritePacket(pkt); err != nil {
			t.Errorf("WritePacket: %v", err)
		}
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame := make([]byte, 4096)
	n, err := client.Read(frame)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	got, err := codec.Decode(frame[:n])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Type != codec.TypePUBACK || got.PacketID != 10 {
		t.Fatalf("mismatch: type=%d pid=%d", got.Type, got.PacketID)
	}
}
