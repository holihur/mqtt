package broker

import (
	"net"
	"os"
	"testing"
	"time"

	"mqtt/internal/auth"
	"mqtt/internal/codec"
)

// Regression: an ACL file provides AUTHORIZATION, never authentication.
// FileACL.Authenticate must be false so it can never act as a credential
// checker (previously it embedded AllowAll and let everyone through).
func TestFileACLNeverAuthenticates(t *testing.T) {
	tmp, _ := os.CreateTemp("", "acl")
	tmp.WriteString("client sensor-1 topic secret/# read\n")
	tmp.Close()
	defer os.Remove(tmp.Name())
	acl, err := auth.NewFileACL(tmp.Name())
	if err != nil {
		t.Fatalf("NewFileACL: %v", err)
	}
	if acl.Authenticate("any", "any", []byte("any")) {
		t.Fatal("FileACL must never authenticate")
	}
}

// Regression: an ACL file that cannot be loaded must fail broker construction
// instead of silently degrading to an open broker.
func TestBuildAuthenticatorACLLoadFailureFailsClosed(t *testing.T) {
	if _, err := buildAuthenticator(Config{ACLFile: "/nonexistent/acl"}); err == nil {
		t.Fatal("unusable ACL file must return an error")
	}
	_, err := NewWithOptions(Config{NodeID: "acl-fail", ACLFile: "/nonexistent/acl", AllowAnonymous: true})
	if err == nil {
		t.Fatal("NewWithOptions must propagate ACL load failure")
	}
}

// Regression: ACL file alone must NOT count as authentication. Without an
// explicit AllowAnonymous opt-in, authentication must fail closed.
func TestBuildAuthenticatorACLOnlyRequiresAnonymousOptIn(t *testing.T) {
	tmp, _ := os.CreateTemp("", "acl")
	tmp.WriteString("client sensor-1 topic secret/# read\n")
	tmp.Close()
	defer os.Remove(tmp.Name())

	// no anonymous opt-in -> nobody authenticates
	a, err := buildAuthenticator(Config{ACLFile: tmp.Name(), AllowAnonymous: false})
	if err != nil {
		t.Fatalf("buildAuthenticator: %v", err)
	}
	if a.Authenticate("sensor-1", "u", []byte("p")) {
		t.Fatal("ACL-only config without AllowAnonymous must not authenticate anyone")
	}

	// explicit anonymous opt-in -> open auth, but topic ACL still enforced
	a2, err := buildAuthenticator(Config{ACLFile: tmp.Name(), AllowAnonymous: true})
	if err != nil {
		t.Fatalf("buildAuthenticator: %v", err)
	}
	if !a2.Authenticate("x", "u", []byte("p")) {
		t.Fatal("anonymous opt-in should authenticate")
	}
	if !a2.Authorize("sensor-1", "secret/x", false) {
		t.Fatal("allowed topic should pass ACL")
	}
	if a2.Authorize("sensor-1", "other/x", false) {
		t.Fatal("topic outside ACL rules must be denied")
	}
	if a2.Authorize("sensor-1", "secret/x", true) {
		t.Fatal("read-only rule must deny publish")
	}
}

// End-to-end: broker started with only -acl must reject CONNECTs.
func TestSecurityACLOnlyBrokerRejectsConnect(t *testing.T) {
	tmp, _ := os.CreateTemp("", "acl")
	tmp.WriteString("client sensor-1 topic secret/# read\n")
	tmp.Close()
	defer os.Remove(tmp.Name())
	b, err := NewWithOptions(Config{NodeID: "acl-e2e", TCPAddr: "127.0.0.1:12216", ACLFile: tmp.Name(), AllowAnonymous: false, WSAddr: ""})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	ctx, cancel := newCtx()
	defer cancel()
	go b.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	conn, _ := net.Dial("tcp", "127.0.0.1:12216")
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "attacker", Username: "anyone", Password: []byte("guessed")}
	d, _ := codec.Encode(p)
	conn.Write(d)
	buf := make([]byte, 256)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, _ := conn.Read(buf)
	ack, _ := codec.Decode(buf[:n])
	if ack == nil || ack.ReasonCode == 0 {
		t.Fatalf("ACL-only broker must reject unauthenticated CONNECT, got %+v", ack)
	}
	conn.Close()
}
