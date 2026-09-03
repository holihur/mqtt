package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mqtt/internal/persistence"
)

// TestWebUICombinedHandler 验证 -webui 端口的 combinedHandler：
// 静态 dashboard 公开可访问、SPA 回退、/api/ 仍遵循 Bearer 鉴权。
func TestWebUICombinedHandler(t *testing.T) {
	b, err := NewWithOptions(Config{
		NodeID:          "webui-test",
		TCPAddr:         "",
		WSAddr:          "",
		AllowAnonymous:  true,
		MaxPacketSize:   1 << 20,
		AdminToken:      "s3cret",
	}, WithStore(persistence.NewMemoryStore()), WithVersion("1.2.3", "abc123", "2026-08-28"))
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.StartAsync(ctx); err != nil {
		t.Fatalf("StartAsync: %v", err)
	}

	adm := b.newAdminServer()
	srv := httptest.NewServer(adm.combinedHandler())
	defer srv.Close()

	// 静态 dashboard 无需鉴权，返回 index.html
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("GET / content-type = %q, want text/html", ct)
	}

	// SPA 回退：不存在的路径也返回 index.html (200)
	resp2, err := http.Get(srv.URL + "/some/client/route")
	if err != nil {
		t.Fatalf("GET /some/client/route: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET /some/client/route status = %d, want 200", resp2.StatusCode)
	}

	// /api/ 仍需鉴权
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/info", nil)
	resp3, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/v1/info (no token): %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /api/v1/info (no token) = %d, want 401", resp3.StatusCode)
	}

	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/info", nil)
	req2.Header.Set("Authorization", "Bearer s3cret")
	resp4, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("GET /api/v1/info (token): %v", err)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/info (token) = %d, want 200", resp4.StatusCode)
	}
}
