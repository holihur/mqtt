package broker

import (
	"bytes"
	"log/slog"
	"os"
	"testing"
	"time"

	"mqtt/internal/codec"
	"net"
)

func TestACLReloadInfoLog(t *testing.T) {
	// capture slog
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	orig := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(orig)

	// create temp ACL file
	f, err := os.CreateTemp("", "acl-*.conf")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	// initial allow all
	if _, err := f.WriteString("topic allow/# readwrite\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	addr := "127.0.0.1:12200"
	b := New(Config{NodeID: "acl-reload", TCPAddr: addr, ACLFile: f.Name(), AllowAnonymous: true}, nil, nil)
	ctx, cancel := newTestCtx()
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	time.Sleep(300 * time.Millisecond)

	// first publish should succeed (allow)
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "acl-client"}
	data, _ := codec.Encode(p)
	conn1.Write(data)
	buf2 := make([]byte, 1024)
	conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn1.Read(buf2)
	// subscribe to allow topic
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1, Subscriptions: []codec.Subscription{{Filter: "allow/#", QoS: 0}}}
	data, _ = codec.Encode(sub)
	conn1.Write(data)
	conn1.Read(buf2)
	// publish to allow/test
	pub := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "allow/test", QoS: 0, Payload: []byte("hello")}
	data, _ = codec.Encode(pub)
	conn1.Write(data)
	time.Sleep(100 * time.Millisecond)
	// should not be dropped

	// now change ACL to deny allow/# and allow only other/#
	time.Sleep(100 * time.Millisecond)
	if err := os.WriteFile(f.Name(), []byte("topic other/# readwrite\n"), 0644); err != nil {
		t.Fatal(err)
	}
	// wait for watchACL ticker 5s + buffer
	time.Sleep(6 * time.Second)
	// check log contains acl reloaded
	logStr := buf.String()
	if !bytes.Contains([]byte(logStr), []byte("acl reloaded")) {
		t.Fatalf("expected acl reloaded info log, got %q", logStr)
	}
	// now publish to allow/test should be dropped (not authorized)
	// publish again
	data, _ = codec.Encode(pub)
	conn1.Write(data)
	time.Sleep(200 * time.Millisecond)
	// we can't easily verify drop without subscriber, but at least ensure no panic
	// try new connection and publish
	conn2, _ := net.Dial("tcp", addr)
	p2 := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "acl-client2"}
	data, _ = codec.Encode(p2)
	conn2.Write(data)
	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	conn2.Read(buf2)
	pub2 := &codec.Packet{Type: codec.TypePUBLISH, Version: codec.ProtocolV311, Topic: "allow/test", QoS: 0, Payload: []byte("hello2")}
	data, _ = codec.Encode(pub2)
	conn2.Write(data)
	time.Sleep(100 * time.Millisecond)
	conn1.Close()
	conn2.Close()
}
