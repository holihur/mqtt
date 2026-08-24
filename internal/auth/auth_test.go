package auth

import (
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestJWTAuth(t *testing.T) {
	secret := "test-secret-123"
	j := &JWTAuth{Secret: secret}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"client_id": "c1", "exp": time.Now().Add(time.Hour).Unix()})
	signed, _ := token.SignedString([]byte(secret))
	if !j.Authenticate("c1", "any", []byte(signed)) {
		t.Fatalf("valid jwt should pass")
	}
	if j.Authenticate("c2", "any", []byte(signed)) {
		t.Fatalf("client_id mismatch should fail")
	}
	token2 := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"client_id": "c1", "exp": time.Now().Add(-time.Hour).Unix()})
	signed2, _ := token2.SignedString([]byte(secret))
	if j.Authenticate("c1", "any", []byte(signed2)) {
		t.Fatalf("expired should fail")
	}
	if j.Authenticate("c1", "any", []byte("not-a-jwt")) {
		t.Fatalf("invalid should fail")
	}
}

func TestFileACL(t *testing.T) {
	content := `
client client1 topic a/b read
client client1 topic a/# readwrite
`
	tmp, _ := os.CreateTemp("", "acl")
	_, _ = tmp.WriteString(content)
	_ = tmp.Close()
	defer os.Remove(tmp.Name())
	acl, err := NewFileACL(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !acl.Authorize("client1", "a/b", false) {
		t.Fatalf("client1 a/b should allow")
	}
	if acl.Authorize("client1", "x/y", false) {
		t.Fatalf("client1 x/y should deny")
	}
	if !acl.Authorize("client1", "a/c/d", true) {
		t.Fatalf("client1 a/# write should allow")
	}
	if acl.Authorize("other", "a/b", false) {
		t.Fatalf("other should deny")
	}
	// no rules file should allow all
	acl2 := &FileACL{}
	if !acl2.Authorize("any", "any", false) {
		t.Fatalf("empty acl should allow")
	}
}
