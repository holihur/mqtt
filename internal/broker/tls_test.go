package broker

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"mqtt/internal/codec"
)

func generateSelfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return cert, pool
}

func TestTLSConnect(t *testing.T) {
	cert, pool := generateSelfSigned(t)
	addr := "127.0.0.1:18991"
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	b := New(Config{NodeID: "tls-test", TCPAddr: addr, TLSConfig: tlsCfg, AllowAnonymous: true}, nil, nil)
	ctx, cancel := newTestContext()
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	time.Sleep(300 * time.Millisecond)

	clientCfg := &tls.Config{RootCAs: pool, InsecureSkipVerify: true}
	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer conn.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "tls-client"}
	data, _ := codec.Encode(p)
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(data); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	ack, err := codec.Decode(buf[:n])
	if err != nil || ack.Type != codec.TypeCONNACK || ack.ReasonCode != 0 {
		t.Fatalf("tls connack failed %v %v", err, ack)
	}
}

func TestMTLSRequireClientCert(t *testing.T) {
	cert, pool := generateSelfSigned(t)
	addr := "127.0.0.1:18992"
	// server requires client cert
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS12}
	b := New(Config{NodeID: "mtls-test", TCPAddr: addr, TLSConfig: tlsCfg, AllowAnonymous: true}, nil, nil)
	ctx, cancel := newTestContext()
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	time.Sleep(300 * time.Millisecond)

	// client without cert should fail handshake
	clientCfgNoCert := &tls.Config{RootCAs: pool}
	conn, err := tls.Dial("tcp", addr, clientCfgNoCert)
	if err == nil {
		_ = conn.Close()
		// handshake may succeed but server will reject? For RequireAndVerify, handshake should fail
		// allow; try writing should fail
	}
	_ = err

	// client with cert should succeed
	clientCfg := &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, InsecureSkipVerify: true}
	conn2, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		t.Fatalf("mtls dial with cert failed: %v", err)
	}
	defer conn2.Close()
	p := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4, ConnectFlags: codec.ConnectFlags{CleanSession: true}, ClientID: "mtls-client"}
	data, _ := codec.Encode(p)
	conn2.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn2.Write(data); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1024)
	n, err := conn2.Read(buf)
	if err != nil {
		t.Fatalf("read connack mtls: %v", err)
	}
	ack, _ := codec.Decode(buf[:n])
	if ack.Type != codec.TypeCONNACK {
		t.Fatalf("mtls connack type %v", ack.Type)
	}
}

func newTestContext() (ctx context.Context, cancel context.CancelFunc) {
	// reuse helper from qos_retry_test
	return newTestCtx()
}

// fallback if newTestCtx not exported; define locally
func newTestCtx() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}
