package transport

import (
	"net/http"
	"testing"
)

func TestCheckOrigin_EmptyAllowed(t *testing.T) {
	l := NewListener(":0", nil, ":0")
	req, _ := http.NewRequest("GET", "http://example.com/mqtt", nil)
	req.Host = "example.com"
	req.Header.Set("Origin", "")
	if !l.checkOrigin(req) {
		t.Fatalf("empty Origin should be allowed")
	}
}

func TestCheckOrigin_SameHostAllowed(t *testing.T) {
	l := NewListener(":0", nil, ":0")
	req, _ := http.NewRequest("GET", "http://example.com/mqtt", nil)
	req.Host = "example.com"
	req.Header.Set("Origin", "http://example.com")
	if !l.checkOrigin(req) {
		t.Fatalf("same host should be allowed")
	}
}

func TestCheckOrigin_CrossOriginDenied(t *testing.T) {
	l := NewListener(":0", nil, ":0")
	req, _ := http.NewRequest("GET", "http://example.com/mqtt", nil)
	req.Host = "example.com"
	req.Header.Set("Origin", "http://evil.com")
	if l.checkOrigin(req) {
		t.Fatalf("cross origin should be denied without whitelist")
	}
}

func TestCheckOrigin_WhitelistAllowed(t *testing.T) {
	l := NewListener(":0", nil, ":0")
	l.SetWsAllowOrigins([]string{"http://allowed.com", "https://app.example.com"})
	req, _ := http.NewRequest("GET", "http://example.com/mqtt", nil)
	req.Host = "example.com"
	req.Header.Set("Origin", "http://allowed.com")
	if !l.checkOrigin(req) {
		t.Fatalf("whitelisted origin should be allowed")
	}
	req.Header.Set("Origin", "http://evil.com")
	if l.checkOrigin(req) {
		t.Fatalf("non-whitelisted should be denied")
	}
}

func TestCheckOrigin_AllowAll(t *testing.T) {
	l := NewListener(":0", nil, ":0")
	l.SetWsAllowOrigins([]string{"*"})
	req, _ := http.NewRequest("GET", "http://example.com/mqtt", nil)
	req.Host = "example.com"
	req.Header.Set("Origin", "http://any.com")
	if !l.checkOrigin(req) {
		t.Fatalf("allow all should allow any origin")
	}
}
