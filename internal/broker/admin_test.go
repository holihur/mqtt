package broker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/persistence"
)

// newAdminAPI 创建嵌入式 broker + 管理 API 测试服务器 (httptest, loopback)。
// cfg.AdminToken 为空时验证 loopback 免 token 策略。
func newAdminAPI(t *testing.T, cfg Config) (*Broker, *httptest.Server) {
	t.Helper()
	if cfg.NodeID == "" {
		cfg.NodeID = "admin-test"
	}
	cfg.TCPAddr = ""
	cfg.WSAddr = ""
	cfg.AllowAnonymous = true
	cfg.MaxPacketSize = 1 << 20
	store := persistence.NewMemoryStore()
	b, err := NewWithOptions(cfg, WithStore(store), WithVersion("1.2.3", "abc123", "2026-08-28"))
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := b.StartAsync(ctx); err != nil {
		t.Fatalf("StartAsync: %v", err)
	}
	adm := b.newAdminServer()
	srv := httptest.NewServer(adm.handler())
	t.Cleanup(srv.Close)
	return b, srv
}

func apiDo(t *testing.T, srv *httptest.Server, token, method, path string, body any) (*http.Response, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rd = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, srv.URL+path, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, b
}

// connectTestClient 通过 TCP 建立真实 MQTT 连接 (v3.1.1)。
func connectTestClient(t *testing.T, addr, clientID string, clean bool) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	pkt := &codec.Packet{Type: codec.TypeCONNECT, Version: codec.ProtocolV311, ProtocolName: "MQTT", ProtocolLevel: 4,
		ConnectFlags: codec.ConnectFlags{CleanSession: clean}, KeepAlive: 30, ClientID: clientID}
	data, err := codec.Encode(pkt)
	if err != nil {
		t.Fatalf("encode CONNECT: %v", err)
	}
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	buf := make([]byte, 4096)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("read CONNACK: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	return conn
}

func waitClients(t *testing.T, b *Broker, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if b.ClientCount() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("clients: want %d, got %d", want, b.ClientCount())
}

// ---------------------------------------------------------------------------
// 鉴权
// ---------------------------------------------------------------------------

func TestAdminAuthRequired(t *testing.T) {
	_, srv := newAdminAPI(t, Config{AdminToken: "s3cret"})

	// 无 token → 401
	resp, _ := apiDo(t, srv, "", http.MethodGet, "/api/v1/info", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: want 401, got %d", resp.StatusCode)
	}
	// 错误 token → 401
	resp, _ = apiDo(t, srv, "wrong", http.MethodGet, "/api/v1/info", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token: want 401, got %d", resp.StatusCode)
	}
	// 正确 token → 200
	resp, _ = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/info", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("right token: want 200, got %d", resp.StatusCode)
	}
}

func TestAdminLoopbackNoToken(t *testing.T) {
	// 未配置 token: httptest 请求来自 127.0.0.1, 应放行
	_, srv := newAdminAPI(t, Config{})
	resp, _ := apiDo(t, srv, "", http.MethodGet, "/api/v1/info", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("loopback without token: want 200, got %d", resp.StatusCode)
	}
}

func TestAdminXTokenHeader(t *testing.T) {
	_, srv := newAdminAPI(t, Config{AdminToken: "s3cret"})
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/info", nil)
	req.Header.Set("X-Admin-Token", "s3cret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("X-Admin-Token: want 200, got %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 只读端点
// ---------------------------------------------------------------------------

func TestAdminInfo(t *testing.T) {
	// RedisAddr 指向不可达地址: 集群自动禁用 (mode=standalone), 但仍回显 redisAddr
	_, srv := newAdminAPI(t, Config{AdminToken: "s3cret", RedisAddr: "127.0.0.1:1"})
	resp, body := apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/info", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("info: want 200, got %d: %s", resp.StatusCode, body)
	}
	var info map[string]any
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("info decode: %v", err)
	}
	if info["nodeId"] != "admin-test" {
		t.Fatalf("nodeId: got %v", info["nodeId"])
	}
	if info["version"] != "1.2.3" {
		t.Fatalf("version: got %v", info["version"])
	}
	if info["mode"] != "standalone" {
		t.Fatalf("mode: got %v", info["mode"])
	}
	if info["redisAddr"] != "127.0.0.1:1" {
		t.Fatalf("redisAddr: got %v", info["redisAddr"])
	}
}

// TestAdminLifecycleStartStop 走真实 initStart 接线: 配置 AdminAddr 后由 broker 拉起/关闭服务。
func TestAdminLifecycleStartStop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	_ = ln.Close()

	b, err := NewWithOptions(Config{NodeID: "admin-life", TCPAddr: "", WSAddr: "", AllowAnonymous: true, AdminAddr: addr, AdminToken: "s3cret", MaxPacketSize: 1 << 20},
		WithStore(persistence.NewMemoryStore()), WithVersion("1.2.3", "abc123", "2026-08-28"))
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.StartAsync(ctx); err != nil {
		t.Fatalf("StartAsync: %v", err)
	}

	// 等待真实管理服务器就绪
	var resp *http.Response
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/api/v1/info", nil)
		req.Header.Set("Authorization", "Bearer s3cret")
		r, err := http.DefaultClient.Do(req)
		if err == nil {
			resp = r
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if resp == nil {
		t.Fatalf("admin server never came up on %s", addr)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("info via real server: %d %s", resp.StatusCode, body)
	}
	var info map[string]any
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("info decode: %v", err)
	}
	if info["adminEnabled"] != true {
		t.Fatalf("adminEnabled via real server: got %v", info["adminEnabled"])
	}

	// 停止后端口应释放
	if err := b.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	_, err = http.Get("http://" + addr + "/api/v1/info")
	if err == nil {
		t.Fatalf("admin server still serving after Stop")
	}
}

func TestAdminNodesAndHealth(t *testing.T) {
	_, srv := newAdminAPI(t, Config{AdminToken: "s3cret"})

	resp, body := apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/nodes", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("nodes: %d %s", resp.StatusCode, body)
	}
	var nodes struct {
		Nodes []string `json:"nodes"`
	}
	if err := json.Unmarshal(body, &nodes); err != nil {
		t.Fatalf("nodes decode: %v", err)
	}
	if len(nodes.Nodes) != 1 || nodes.Nodes[0] != "admin-test" {
		t.Fatalf("nodes: got %v", nodes.Nodes)
	}

	resp, _ = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health: %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 发布 + retain 管理
// ---------------------------------------------------------------------------

func TestAdminPublishAndRetained(t *testing.T) {
	_, srv := newAdminAPI(t, Config{AdminToken: "s3cret"})

	// 发布 retain 消息
	resp, body := apiDo(t, srv, "s3cret", http.MethodPost, "/api/v1/publish", publishRequest{
		Topic: "admin/t1", Payload: "hello", QoS: 1, Retain: true,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("publish: %d %s", resp.StatusCode, body)
	}

	// stats 应反映消息与 retain
	resp, body = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/stats", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats: %d %s", resp.StatusCode, body)
	}
	var st statsResponse
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatalf("stats decode: %v", err)
	}
	if st.MessagesReceived < 1 {
		t.Fatalf("messagesReceived: got %d", st.MessagesReceived)
	}
	if st.RetainedMessages < 1 {
		t.Fatalf("retainedMessages: got %d", st.RetainedMessages)
	}
	if st.Sessions != 0 {
		t.Fatalf("sessions: got %d", st.Sessions)
	}

	// retain 列表默认不含 payload
	resp, body = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/retained", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retained: %d %s", resp.StatusCode, body)
	}
	var retained []retainedResponse
	if err := json.Unmarshal(body, &retained); err != nil {
		t.Fatalf("retained decode: %v", err)
	}
	if len(retained) != 1 || retained[0].Topic != "admin/t1" {
		t.Fatalf("retained: got %+v", retained)
	}
	if retained[0].PayloadB64 != "" {
		t.Fatalf("payload should be omitted by default")
	}

	// with_payload=true 返回 base64
	resp, body = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/retained?with_payload=true", nil)
	if err := json.Unmarshal(body, &retained); err != nil {
		t.Fatalf("retained decode: %v", err)
	}
	if retained[0].PayloadB64 != base64.StdEncoding.EncodeToString([]byte("hello")) {
		t.Fatalf("payloadB64: got %q", retained[0].PayloadB64)
	}

	// 二进制发布 (payloadB64)
	resp, _ = apiDo(t, srv, "s3cret", http.MethodPost, "/api/v1/publish", publishRequest{
		Topic: "admin/bin", PayloadB64: base64.StdEncoding.EncodeToString([]byte{0x00, 0xFF, 0x10}), Retain: true,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("publish bin: %d", resp.StatusCode)
	}

	// 删除单个 retain
	resp, _ = apiDo(t, srv, "s3cret", http.MethodDelete, "/api/v1/retained?topic=admin%2Ft1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete retained: %d", resp.StatusCode)
	}
	resp, body = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/retained", nil)
	if err := json.Unmarshal(body, &retained); err != nil {
		t.Fatalf("retained decode: %v", err)
	}
	if len(retained) != 1 || retained[0].Topic != "admin/bin" {
		t.Fatalf("after delete: got %+v", retained)
	}

	// 清空全部
	resp, _ = apiDo(t, srv, "s3cret", http.MethodDelete, "/api/v1/retained?all=true", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clear retained: %d", resp.StatusCode)
	}
	resp, body = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/retained", nil)
	if err := json.Unmarshal(body, &retained); err != nil {
		t.Fatalf("retained decode: %v", err)
	}
	if len(retained) != 0 {
		t.Fatalf("after clear: got %+v", retained)
	}

	// 参数校验
	resp, _ = apiDo(t, srv, "s3cret", http.MethodPost, "/api/v1/publish", publishRequest{QoS: 3})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("qos=3: want 400, got %d", resp.StatusCode)
	}
	resp, _ = apiDo(t, srv, "s3cret", http.MethodDelete, "/api/v1/retained", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("delete without topic: want 400, got %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 客户端 / 订阅 / 会话 (需要真实连接)
// ---------------------------------------------------------------------------

func newAdminListenerBroker(t *testing.T) (*Broker, *httptest.Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	b, err := NewWithOptions(Config{NodeID: "admin-ln", AllowAnonymous: true, AdminToken: "s3cret", MaxPacketSize: 1 << 20},
		WithStore(persistence.NewMemoryStore()), WithCustomListener(ln), WithVersion("1.2.3", "abc123", "2026-08-28"))
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := b.StartAsync(ctx); err != nil {
		t.Fatalf("StartAsync: %v", err)
	}
	adm := b.newAdminServer()
	srv := httptest.NewServer(adm.handler())
	t.Cleanup(srv.Close)
	return b, srv, addr
}

func TestAdminClientsListKick(t *testing.T) {
	b, srv, addr := newAdminListenerBroker(t)

	conn := connectTestClient(t, addr, "admin-c1", true)
	defer conn.Close()

	// 订阅 a/b
	sub := &codec.Packet{Type: codec.TypeSUBSCRIBE, Version: codec.ProtocolV311, PacketID: 1,
		Subscriptions: []codec.Subscription{{Filter: "a/b", QoS: 1}}}
	data, _ := codec.Encode(sub)
	if _, err := conn.Write(data); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("read SUBACK: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	waitClients(t, b, 1)

	// 列表
	resp, body := apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/clients", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clients: %d %s", resp.StatusCode, body)
	}
	var clients []clientResponse
	if err := json.Unmarshal(body, &clients); err != nil {
		t.Fatalf("clients decode: %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("clients: want 1, got %+v", clients)
	}
	c := clients[0]
	if c.ClientID != "admin-c1" || c.Version != "3.1.1" || c.Subscriptions != 1 {
		t.Fatalf("client detail: %+v", c)
	}
	if !strings.HasPrefix(c.RemoteAddr, "127.0.0.1:") {
		t.Fatalf("remoteAddr: %q", c.RemoteAddr)
	}

	// 详情
	resp, body = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/clients/admin-c1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("client detail: %d %s", resp.StatusCode, body)
	}
	// 不存在 → 404
	resp, _ = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/clients/nope", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing client: want 404, got %d", resp.StatusCode)
	}

	// 订阅列表
	resp, body = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/subscriptions", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("subscriptions: %d %s", resp.StatusCode, body)
	}
	var subs []subscriptionResponse
	if err := json.Unmarshal(body, &subs); err != nil {
		t.Fatalf("subs decode: %v", err)
	}
	if len(subs) != 1 || subs[0].Filter != "a/b" || subs[0].ClientID != "admin-c1" {
		t.Fatalf("subscriptions: %+v", subs)
	}
	resp, body = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/subscriptions/admin-c1", nil)
	if err := json.Unmarshal(body, &subs); err != nil {
		t.Fatalf("subs decode: %v", err)
	}
	if len(subs) != 1 || subs[0].Filter != "a/b" {
		t.Fatalf("client subscriptions: %+v", subs)
	}

	// 踢下线
	resp, body = apiDo(t, srv, "s3cret", http.MethodDelete, "/api/v1/clients/admin-c1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("kick: %d %s", resp.StatusCode, body)
	}
	waitClients(t, b, 0)
	// 再踢 → 404
	resp, _ = apiDo(t, srv, "s3cret", http.MethodDelete, "/api/v1/clients/admin-c1", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("kick missing: want 404, got %d", resp.StatusCode)
	}
	// clean=true 会话应已删除
	resp, _ = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/sessions/admin-c1", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("session after clean kick: want 404, got %d", resp.StatusCode)
	}
}

func TestAdminSessionsPersistentDelete(t *testing.T) {
	b, srv, addr := newAdminListenerBroker(t)

	// 持久会话 (clean=false): 断开后会话保留
	conn := connectTestClient(t, addr, "admin-p1", false)
	_ = conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		sess, ok := b.sessions["admin-p1"]
		b.mu.RUnlock()
		if ok && sess != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	resp, body := apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/sessions", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sessions: %d %s", resp.StatusCode, body)
	}
	var sessions []sessionResponse
	if err := json.Unmarshal(body, &sessions); err != nil {
		t.Fatalf("sessions decode: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ClientID != "admin-p1" {
		t.Fatalf("sessions: %+v", sessions)
	}
	if sessions[0].Connected {
		t.Fatalf("expected offline session, got connected")
	}
	if sessions[0].Expiry != 0xFFFFFFFF {
		t.Fatalf("expiry: got %d", sessions[0].Expiry)
	}

	resp, body = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/sessions/admin-p1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("session detail: %d %s", resp.StatusCode, body)
	}

	// 删除会话
	resp, body = apiDo(t, srv, "s3cret", http.MethodDelete, "/api/v1/sessions/admin-p1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete session: %d %s", resp.StatusCode, body)
	}
	resp, _ = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/sessions/admin-p1", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("session after delete: want 404, got %d", resp.StatusCode)
	}
}

func TestAdminDeleteSessionWhileConnected(t *testing.T) {
	b, srv, addr := newAdminListenerBroker(t)

	// 持久会话客户端在线
	conn := connectTestClient(t, addr, "admin-p2", false)
	defer conn.Close()
	waitClients(t, b, 1)

	// 删除在线会话: 连接被踢 + 会话被清
	resp, body := apiDo(t, srv, "s3cret", http.MethodDelete, "/api/v1/sessions/admin-p2", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete session: %d %s", resp.StatusCode, body)
	}
	waitClients(t, b, 0)
	resp, _ = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/sessions/admin-p2", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("session after delete: want 404, got %d", resp.StatusCode)
	}
	// 断开回调不应把会话写回 store
	time.Sleep(100 * time.Millisecond)
	resp, _ = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/sessions/admin-p2", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("session resurrected: want 404, got %d", resp.StatusCode)
	}
}

func TestAdminACLReloadNoACL(t *testing.T) {
	_, srv := newAdminAPI(t, Config{AdminToken: "s3cret"})
	resp, _ := apiDo(t, srv, "s3cret", http.MethodPost, "/api/v1/acl/reload", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("acl reload without FileACL: want 400, got %d", resp.StatusCode)
	}
}

func TestAdminSubscriptionsMatch(t *testing.T) {
	b, srv := newAdminAPI(t, Config{AdminToken: "s3cret"})

	b.trie.Add("sensors/+/temp", "c1", 0, false)
	b.trie.Add("sensors/#", "c2", 1, false)
	b.trie.Add("other/thing", "c3", 0, false)
	b.trie.Add("#", "c4", 2, false)

	// 匹配具体主题: c1 (+)、c2 (#)、c4 (根 #) 应命中，c3 不命中
	resp, body := apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/subscriptions/match?topic=sensors/room1/temp", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("match: %d %s", resp.StatusCode, body)
	}
	var subs []subscriptionResponse
	if err := json.Unmarshal(body, &subs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := map[string]string{}
	for _, s := range subs {
		got[s.ClientID] = s.Filter
	}
	if len(got) != 3 {
		t.Fatalf("match: want 3 clients, got %+v", subs)
	}
	if got["c1"] != "sensors/+/temp" || got["c2"] != "sensors/#" || got["c4"] != "#" {
		t.Fatalf("match result: %+v", subs)
	}
	if _, ok := got["c3"]; ok {
		t.Fatalf("c3 should not match, got %+v", subs)
	}

	// 缺失 topic → 400
	resp, _ = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/subscriptions/match", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing topic: want 400, got %d", resp.StatusCode)
	}

	// 主题含通配符 → 400
	resp, _ = apiDo(t, srv, "s3cret", http.MethodGet, "/api/v1/subscriptions/match?topic=sensors/%2B/temp", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("wildcard topic: want 400, got %d", resp.StatusCode)
	}
}
