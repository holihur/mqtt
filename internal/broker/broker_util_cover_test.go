package broker

import (
	"context"
	"os"
	"testing"

	"mqtt/internal/auth"
)

func TestBuildAuthenticator(t *testing.T) {
	cfg := Config{AllowAnonymous: true}
	a, err := buildAuthenticator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.(*auth.AllowAll); !ok {
		t.Fatalf("allow anonymous should be AllowAll")
	}
	cfg2 := Config{AllowAnonymous: false}
	a2, err2 := buildAuthenticator(cfg2)
	if err2 != nil {
		t.Fatal(err2)
	}
	if _, ok := a2.(*auth.DenyAll); !ok {
		t.Fatalf("deny should be DenyAll")
	}
	cfg3 := Config{JWTSecret: "s3cr3t"}
	a3, err3 := buildAuthenticator(cfg3)
	if err3 != nil {
		t.Fatal(err3)
	}
	if a3 == nil {
		t.Fatal("jwt should build")
	}
	cfg4 := Config{JWTSecret: "s", ACLFile: "/tmp/nonexistent_acl_12345"}
	if _, err4 := buildAuthenticator(cfg4); err4 == nil {
		t.Fatal("bad acl file must fail construction (fail-closed)")
	}
	tmp, _ := os.CreateTemp("", "acl")
	tmp.WriteString("topic test read\n")
	tmp.Close()
	defer os.Remove(tmp.Name())
	cfg5 := Config{ACLFile: tmp.Name(), AllowAnonymous: true}
	a5, err5 := buildAuthenticator(cfg5)
	if err5 != nil {
		t.Fatalf("acl should build: %v", err5)
	}
	if a5 == nil {
		t.Fatal("acl should build")
	}
	cfg6 := Config{JWTSecret: "s", ACLFile: tmp.Name()}
	a6, err6 := buildAuthenticator(cfg6)
	if err6 != nil {
		t.Fatalf("jwt+acl should build: %v", err6)
	}
	if _, ok := a6.(*auth.Chain); !ok {
		t.Fatalf("jwt+acl should be Chain")
	}
	c, cancel := storeCtx()
	defer cancel()
	_ = c
	_ = bgCtx()
}

func TestLoadTLSConfig(t *testing.T) {
	cfg, err := loadTLSConfig("", "", "")
	if err != nil || cfg != nil {
		t.Fatalf("empty should nil %v %v", cfg, err)
	}
	cfg, err = loadTLSConfig("nonexistent.crt", "nonexistent.key", "")
	if err == nil {
		t.Fatal("nonexistent should error")
	}
	_ = cfg
}

func TestBrokerLimiterJanitorStart(t *testing.T) {
	b := &Broker{
		cfg:      Config{MaxPublishPerSec: 10, MaxSubscribePerSec: 5},
		limiters: make(map[string]*clientLimiter),
	}
	b.cfg.ApplyDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b.limiterJanitor(ctx)
}
