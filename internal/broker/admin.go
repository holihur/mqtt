package broker

// 管理 API: 面向运维的 HTTP REST 接口 (JSON)。
//
// 通过 Config.AdminAddr (-admin-api) 启用，Config.AdminToken (-admin-api-token)
// 提供 Bearer 鉴权；未配置 token 时仅允许 loopback 访问，避免无鉴权暴露在公网。
// 与 /metrics、/debug/pprof (PprofAddr) 分离，独立监听端口，便于单独做网络隔离。
//
// 端点一览 (前缀 /api/v1):
//
//	GET    /api/v1/info            节点与版本信息
//	GET    /api/v1/stats           broker 统计 (消息数/连接数/会话/retain 配额/节点)
//	GET    /api/v1/health          健康检查 (redis ping + 资源水位)
//	GET    /api/v1/clients         在线客户端列表
//	GET    /api/v1/clients/{id}    单个客户端详情
//	DELETE /api/v1/clients/{id}    踢下线 (v5 发 DISCONNECT 0x99 administrative action)
//	GET    /api/v1/sessions        本节点会话列表 (含离线持久会话)
//	GET    /api/v1/sessions/{id}   单个会话详情
//	DELETE /api/v1/sessions/{id}   删除会话 (踢下线 + 清 store + 清订阅)
//	GET    /api/v1/subscriptions   全部订阅
//	GET    /api/v1/subscriptions/{id}  某客户端订阅
//	GET    /api/v1/retained?with_payload=true   retain 列表 (默认不含 payload)
//	DELETE /api/v1/retained?topic=t 或 ?all=true   删除 retain
//	POST   /api/v1/publish         发布消息 {topic, payload|payloadB64, qos, retain}
//	GET    /api/v1/nodes           集群节点列表
//	POST   /api/v1/acl/reload      热加载 FileACL

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"mqtt/internal/codec"
	"mqtt/internal/session"
	"mqtt/internal/transport"
)

// ---------------------------------------------------------------------------
// 响应/请求结构体
// ---------------------------------------------------------------------------

type apiError struct {
	Error string `json:"error"`
}

type infoResponse struct {
	NodeID   string `json:"nodeId"`
	Version  string `json:"version"`
	Commit   string `json:"commit"`
	Date     string `json:"date"`
	Uptime   int64  `json:"uptimeSeconds"`
	Mode     string `json:"mode"` // "cluster" | "standalone"
	Redis    string `json:"redisAddr"`
	Admin    bool   `json:"adminEnabled"`
	AdminTLS bool   `json:"adminTls"`
}

type statsResponse struct {
	StartedAt        time.Time `json:"startedAt"`
	Uptime           int64     `json:"uptimeSeconds"`
	MessagesReceived int64     `json:"messagesReceived"`
	MessagesSent     int64     `json:"messagesSent"`
	ClientsConnected int64     `json:"clientsConnected"`
	ClientsTotal     int64     `json:"clientsTotal"`
	Sessions         int       `json:"sessions"`
	RetainedMessages int       `json:"retainedMessages"`
	RetainedSize     int64     `json:"retainedSizeBytes"`
	Nodes            []string  `json:"nodes"`
}

type clientResponse struct {
	ClientID      string    `json:"clientId"`
	Username      string    `json:"username"`
	Version       string    `json:"version"` // "3.1" | "3.1.1" | "5.0"
	RemoteAddr    string    `json:"remoteAddr"`
	KeepAlive     uint16    `json:"keepAlive"`
	CleanStart    bool      `json:"cleanStart"`
	Expiry        uint32    `json:"sessionExpiry"`
	NodeID        string    `json:"nodeId"`
	Subscriptions int       `json:"subscriptions"`
	Inflight      int       `json:"inflight"`
	ConnectedAt   time.Time `json:"connectedAt"`
}

type sessionResponse struct {
	ClientID      string    `json:"clientId"`
	Username      string    `json:"username"`
	Version       string    `json:"version"`
	Connected     bool      `json:"connected"`
	CleanStart    bool      `json:"cleanStart"`
	Expiry        uint32    `json:"sessionExpiry"`
	KeepAlive     uint16    `json:"keepAlive"`
	CreatedAt     time.Time `json:"createdAt"`
	NodeID        string    `json:"nodeId"`
	Subscriptions int       `json:"subscriptions"`
	Inflight      int       `json:"inflight"`
}

type subscriptionResponse struct {
	ClientID string `json:"clientId"`
	Filter   string `json:"filter"`
	QoS      byte   `json:"qos"`
	NoLocal  bool   `json:"noLocal"`
}

type retainedResponse struct {
	Topic      string `json:"topic"`
	QoS        byte   `json:"qos"`
	Size       int    `json:"size"`
	PayloadB64 string `json:"payloadB64,omitempty"`
}

type publishRequest struct {
	Topic      string `json:"topic"`
	Payload    string `json:"payload"`    // UTF-8 文本负载
	PayloadB64 string `json:"payloadB64"` // 二进制负载 (base64), 优先于 payload
	QoS        byte   `json:"qos"`
	Retain     bool   `json:"retain"`
}

// ---------------------------------------------------------------------------
// adminServer
// ---------------------------------------------------------------------------

type adminServer struct {
	b       *Broker
	token   string
	version string
	commit  string
	date    string
}

func (b *Broker) newAdminServer() *adminServer {
	return &adminServer{
		b:       b,
		token:   b.cfg.AdminToken,
		version: b.versionInfo.version,
		commit:  b.versionInfo.commit,
		date:    b.versionInfo.date,
	}
}

// handler 返回带鉴权中间件的完整路由。
func (s *adminServer) handler() http.Handler {
	return s.authMiddleware(s.mux())
}

func (s *adminServer) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/info", s.handleInfo)
	mux.HandleFunc("GET /api/v1/stats", s.handleStats)
	mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	mux.HandleFunc("GET /api/v1/clients", s.handleListClients)
	mux.HandleFunc("GET /api/v1/clients/{clientID}", s.handleGetClient)
	mux.HandleFunc("DELETE /api/v1/clients/{clientID}", s.handleKickClient)
	mux.HandleFunc("GET /api/v1/sessions", s.handleListSessions)
	mux.HandleFunc("GET /api/v1/sessions/{clientID}", s.handleGetSession)
	mux.HandleFunc("DELETE /api/v1/sessions/{clientID}", s.handleDeleteSession)
	mux.HandleFunc("GET /api/v1/subscriptions", s.handleListSubscriptions)
	mux.HandleFunc("GET /api/v1/subscriptions/{clientID}", s.handleClientSubscriptions)
	mux.HandleFunc("GET /api/v1/retained", s.handleListRetained)
	mux.HandleFunc("DELETE /api/v1/retained", s.handleDeleteRetained)
	mux.HandleFunc("POST /api/v1/publish", s.handlePublish)
	mux.HandleFunc("GET /api/v1/nodes", s.handleNodes)
	mux.HandleFunc("POST /api/v1/acl/reload", s.handleACLReload)
	return mux
}

// authMiddleware: Bearer token 常量时间比对；未配置 token 时仅允许 loopback。
func (s *adminServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="mqtt-admin"`)
			s.writeJSON(w, http.StatusUnauthorized, apiError{Error: "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *adminServer) authorized(r *http.Request) bool {
	if s.token == "" {
		// 未配置 token: 仅允许 loopback 调用者
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		ip := net.ParseIP(host)
		return ip != nil && ip.IsLoopback()
	}
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) > len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		h = h[len(prefix):]
	} else {
		// 兼容 X-Admin-Token 头
		h = r.Header.Get("X-Admin-Token")
		if h == "" {
			return false
		}
	}
	return subtle.ConstantTimeCompare([]byte(h), []byte(s.token)) == 1
}

func (s *adminServer) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// 读取型 Handler
// ---------------------------------------------------------------------------

func (s *adminServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	b := s.b
	mode := "standalone"
	if b.cluster != nil {
		mode = "cluster"
	}
	uptime := int64(0)
	if !b.stats.StartedAt.IsZero() {
		uptime = int64(time.Since(b.stats.StartedAt).Seconds())
	}
	s.writeJSON(w, http.StatusOK, infoResponse{
		NodeID:   b.nodeID,
		Version:  s.version,
		Commit:   s.commit,
		Date:     s.date,
		Uptime:   uptime,
		Mode:     mode,
		Redis:    b.cfg.RedisAddr,
		Admin:    b.cfg.AdminAddr != "",
		AdminTLS: b.cfg.AdminTLS,
	})
}

func (s *adminServer) handleStats(w http.ResponseWriter, r *http.Request) {
	b := s.b
	st := b.Stats()
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	retained, size, err := b.retainedStats(ctx)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}
	nodes := []string{b.nodeID}
	if b.cluster != nil {
		if ns, err := b.cluster.Nodes(ctx); err == nil {
			nodes = ns
		}
	}
	sort.Strings(nodes)
	s.writeJSON(w, http.StatusOK, statsResponse{
		StartedAt:        st.StartedAt,
		Uptime:           int64(time.Since(st.StartedAt).Seconds()),
		MessagesReceived: st.MessagesReceived,
		MessagesSent:     st.MessagesSent,
		ClientsConnected: st.ClientsConnected,
		ClientsTotal:     st.ClientsTotal,
		Sessions:         b.SessionCount(),
		RetainedMessages: retained,
		RetainedSize:     size,
		Nodes:            nodes,
	})
}

func (s *adminServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.b.Health(ctx); err != nil {
		s.writeJSON(w, http.StatusServiceUnavailable, apiError{Error: err.Error()})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *adminServer) handleListClients(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.b.clientInfos())
}

func (s *adminServer) handleGetClient(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("clientID")
	ci, ok := s.b.clientInfo(id)
	if !ok {
		s.writeJSON(w, http.StatusNotFound, apiError{Error: fmt.Sprintf("client %q not connected", id)})
		return
	}
	s.writeJSON(w, http.StatusOK, ci)
}

func (s *adminServer) handleListSessions(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.b.sessionInfos())
}

func (s *adminServer) handleGetSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("clientID")
	si, ok := s.b.sessionInfo(id)
	if !ok {
		s.writeJSON(w, http.StatusNotFound, apiError{Error: fmt.Sprintf("session %q not found", id)})
		return
	}
	s.writeJSON(w, http.StatusOK, si)
}

func (s *adminServer) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	entries := s.b.trie.Subscriptions()
	out := make([]subscriptionResponse, 0, len(entries))
	for _, e := range entries {
		out = append(out, subscriptionResponse{ClientID: e.ClientID, Filter: e.Filter, QoS: e.QoS, NoLocal: e.NoLocal})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ClientID == out[j].ClientID {
			return out[i].Filter < out[j].Filter
		}
		return out[i].ClientID < out[j].ClientID
	})
	s.writeJSON(w, http.StatusOK, out)
}

func (s *adminServer) handleClientSubscriptions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("clientID")
	entries := s.b.trie.SubscriptionsFor(id)
	out := make([]subscriptionResponse, 0, len(entries))
	for _, e := range entries {
		out = append(out, subscriptionResponse{ClientID: e.ClientID, Filter: e.Filter, QoS: e.QoS, NoLocal: e.NoLocal})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Filter < out[j].Filter })
	s.writeJSON(w, http.StatusOK, out)
}

func (s *adminServer) handleListRetained(w http.ResponseWriter, r *http.Request) {
	withPayload := r.URL.Query().Get("with_payload") == "true"
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	msgs, err := s.b.store.ListRetained(ctx)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}
	out := make([]retainedResponse, 0, len(msgs))
	for _, m := range msgs {
		rr := retainedResponse{Topic: m.Topic, QoS: m.QoS, Size: len(m.Payload)}
		if withPayload {
			rr.PayloadB64 = base64.StdEncoding.EncodeToString(m.Payload)
		}
		out = append(out, rr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	s.writeJSON(w, http.StatusOK, out)
}

func (s *adminServer) handleNodes(w http.ResponseWriter, r *http.Request) {
	b := s.b
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	nodes := []string{b.nodeID}
	if b.cluster != nil {
		if ns, err := b.cluster.Nodes(ctx); err == nil {
			nodes = ns
		}
	}
	sort.Strings(nodes)
	s.writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}

// ---------------------------------------------------------------------------
// 写操作 Handler
// ---------------------------------------------------------------------------

// handleKickClient 踢掉在线客户端 (DELETE /api/v1/clients/{id})。
// 仅断开连接，会话按正常断开语义处理 (expiry>0 的持久会话保留)。
func (s *adminServer) handleKickClient(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("clientID")
	if err := s.b.kickClient(id); err != nil {
		s.writeJSON(w, http.StatusNotFound, apiError{Error: err.Error()})
		return
	}
	slog.Info("admin kick client", "client", id, "by", r.RemoteAddr)
	s.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "clientId": id})
}

// handleDeleteSession 删除会话 (DELETE /api/v1/sessions/{id})：
// 断开在线连接、清订阅、清 store 与离线队列。
func (s *adminServer) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("clientID")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.b.deleteSession(ctx, id); err != nil {
		s.writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}
	slog.Info("admin delete session", "client", id, "by", r.RemoteAddr)
	s.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "clientId": id})
}

func (s *adminServer) handleDeleteRetained(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if q.Get("all") == "true" {
		msgs, err := s.b.store.ListRetained(ctx)
		if err != nil {
			s.writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
			return
		}
		for _, m := range msgs {
			if err := s.b.store.DeleteRetained(ctx, m.Topic); err != nil {
				slog.Warn("admin clear retained failed", "topic", m.Topic, "err", err)
			}
		}
		s.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": len(msgs)})
		return
	}
	topic := q.Get("topic")
	if topic == "" {
		s.writeJSON(w, http.StatusBadRequest, apiError{Error: "missing \"topic\" query param (or use ?all=true)"})
		return
	}
	if err := s.b.store.DeleteRetained(ctx, topic); err != nil {
		s.writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "topic": topic})
}

// handlePublish 通过管理 API 直接发布 (POST /api/v1/publish)。
// 复用 b.Publish (嵌入式发布路径)：本地 Trie 投递 + 集群广播 + retain 落库。
func (s *adminServer) handlePublish(w http.ResponseWriter, r *http.Request) {
	var req publishRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiError{Error: "invalid json: " + err.Error()})
		return
	}
	if req.Topic == "" {
		s.writeJSON(w, http.StatusBadRequest, apiError{Error: "missing topic"})
		return
	}
	if req.QoS > 2 {
		s.writeJSON(w, http.StatusBadRequest, apiError{Error: "qos must be 0..2"})
		return
	}
	var payload []byte
	if req.PayloadB64 != "" {
		p, err := base64.StdEncoding.DecodeString(req.PayloadB64)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiError{Error: "invalid payloadB64: " + err.Error()})
			return
		}
		payload = p
	} else {
		payload = []byte(req.Payload)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.b.Publish(ctx, req.Topic, payload, req.QoS, req.Retain); err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiError{Error: err.Error()})
		return
	}
	slog.Info("admin publish", "topic", req.Topic, "qos", req.QoS, "retain", req.Retain, "by", r.RemoteAddr)
	s.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "topic": req.Topic})
}

func (s *adminServer) handleACLReload(w http.ResponseWriter, r *http.Request) {
	b := s.b
	acls := b.findFileACLs()
	if len(acls) == 0 {
		s.writeJSON(w, http.StatusBadRequest, apiError{Error: "no FileACL configured"})
		return
	}
	reloaded := 0
	for _, facl := range acls {
		ok, err := facl.Reload()
		if err != nil {
			s.writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
			return
		}
		if ok {
			reloaded++
		}
	}
	slog.Info("admin acl reload", "reloaded", reloaded, "by", r.RemoteAddr)
	s.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "reloaded": reloaded})
}

// ---------------------------------------------------------------------------
// Broker 数据快照辅助 (供管理 API 与测试复用)
// ---------------------------------------------------------------------------

// retainedStats 返回 retain 总条数与总大小。
func (b *Broker) retainedStats(ctx context.Context) (int, int64, error) {
	st, err := b.store.GetRetainedStats(ctx)
	if err != nil {
		return 0, 0, err
	}
	return st.TotalMessages, st.TotalSize, nil
}

// clientInfos 返回在线客户端快照 (按 clientID 排序)。
func (b *Broker) clientInfos() []clientResponse {
	b.mu.RLock()
	conns := make(map[string]*transport.Conn, len(b.conns))
	for k, v := range b.conns {
		conns[k] = v
	}
	sessions := make(map[string]*session.Session, len(b.sessions))
	for k, v := range b.sessions {
		sessions[k] = v
	}
	b.mu.RUnlock()

	out := make([]clientResponse, 0, len(conns))
	for id, conn := range conns {
		ci := clientResponse{
			ClientID:   id,
			RemoteAddr: conn.RemoteAddr(),
			Version:    protocolName(conn.Version()),
		}
		if sess, ok := sessions[id]; ok && sess != nil {
			sess.Mu.Lock()
			ci.Username = sess.Username
			ci.KeepAlive = sess.KeepAlive
			ci.CleanStart = sess.CleanStart
			ci.Expiry = sess.ExpiryInterval
			ci.NodeID = sess.NodeID
			ci.Subscriptions = len(sess.Subscriptions)
			ci.Inflight = len(sess.Inflight)
			ci.ConnectedAt = sess.CreatedAt
			sess.Mu.Unlock()
		}
		out = append(out, ci)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out
}

func (b *Broker) clientInfo(clientID string) (clientResponse, bool) {
	b.mu.RLock()
	conn, ok := b.conns[clientID]
	sess := b.sessions[clientID]
	b.mu.RUnlock()
	if !ok || conn == nil {
		return clientResponse{}, false
	}
	ci := clientResponse{
		ClientID:   clientID,
		RemoteAddr: conn.RemoteAddr(),
		Version:    protocolName(conn.Version()),
	}
	if sess != nil {
		sess.Mu.Lock()
		ci.Username = sess.Username
		ci.KeepAlive = sess.KeepAlive
		ci.CleanStart = sess.CleanStart
		ci.Expiry = sess.ExpiryInterval
		ci.NodeID = sess.NodeID
		ci.Subscriptions = len(sess.Subscriptions)
		ci.Inflight = len(sess.Inflight)
		ci.ConnectedAt = sess.CreatedAt
		sess.Mu.Unlock()
	}
	return ci, true
}

// sessionInfos 返回本节点会话快照 (在线 + 离线持久会话, 按 clientID 排序)。
// 集群模式下仅返回本节点内存中的会话。
func (b *Broker) sessionInfos() []sessionResponse {
	b.mu.RLock()
	sessions := make(map[string]*session.Session, len(b.sessions))
	for k, v := range b.sessions {
		sessions[k] = v
	}
	conns := make(map[string]struct{}, len(b.conns))
	for k := range b.conns {
		conns[k] = struct{}{}
	}
	b.mu.RUnlock()

	out := make([]sessionResponse, 0, len(sessions))
	for id, sess := range sessions {
		si := sessionResponse{ClientID: id}
		if sess == nil {
			out = append(out, si)
			continue
		}
		sess.Mu.Lock()
		si.Username = sess.Username
		si.Version = protocolName(sess.Version)
		_, c := conns[id]
		si.Connected = sess.Connected || c
		si.CleanStart = sess.CleanStart
		si.Expiry = sess.ExpiryInterval
		si.KeepAlive = sess.KeepAlive
		si.CreatedAt = sess.CreatedAt
		si.NodeID = sess.NodeID
		si.Subscriptions = len(sess.Subscriptions)
		si.Inflight = len(sess.Inflight)
		sess.Mu.Unlock()
		out = append(out, si)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out
}

func (b *Broker) sessionInfo(clientID string) (sessionResponse, bool) {
	b.mu.RLock()
	sess, ok := b.sessions[clientID]
	_, connected := b.conns[clientID]
	b.mu.RUnlock()
	if !ok || sess == nil {
		return sessionResponse{}, false
	}
	si := sessionResponse{ClientID: clientID}
	sess.Mu.Lock()
	si.Username = sess.Username
	si.Version = protocolName(sess.Version)
	si.Connected = sess.Connected || connected
	si.CleanStart = sess.CleanStart
	si.Expiry = sess.ExpiryInterval
	si.KeepAlive = sess.KeepAlive
	si.CreatedAt = sess.CreatedAt
	si.NodeID = sess.NodeID
	si.Subscriptions = len(sess.Subscriptions)
	si.Inflight = len(sess.Inflight)
	sess.Mu.Unlock()
	return si, true
}

// kickClient 踢掉在线客户端: v5 先发 DISCONNECT 0x99 (administrative action) 再关闭连接。
// 会话按正常断开语义处理 (持久会话保留, will 触发, 离线队列保留)。
func (b *Broker) kickClient(clientID string) error {
	b.mu.RLock()
	conn, ok := b.conns[clientID]
	b.mu.RUnlock()
	if !ok || conn == nil {
		return fmt.Errorf("client %q not connected", clientID)
	}
	if conn.Version() == codec.ProtocolV5 {
		disc := &codec.Packet{Type: codec.TypeDISCONNECT, Version: codec.ProtocolV5, DiscReason: 0x99}
		_ = b.sendPacket(conn, disc)
	}
	_ = conn.Close()
	return nil
}

// deleteSession 删除会话: 断开在线连接、标记 Deleted 防止断开回调写回 store、
// 清订阅、清 store 会话与离线队列。
func (b *Broker) deleteSession(ctx context.Context, clientID string) error {
	b.mu.Lock()
	conn := b.conns[clientID]
	delete(b.conns, clientID)
	sess := b.sessions[clientID]
	delete(b.sessions, clientID)
	b.mu.Unlock()

	if sess != nil {
		sess.Mu.Lock()
		sess.Deleted = true
		subs := make([]string, 0, len(sess.Subscriptions))
		for f := range sess.Subscriptions {
			subs = append(subs, f)
		}
		sess.Will = nil
		sess.Mu.Unlock()
		for _, f := range subs {
			b.trie.Remove(f, clientID)
		}
	}
	if conn != nil {
		if conn.Version() == codec.ProtocolV5 {
			disc := &codec.Packet{Type: codec.TypeDISCONNECT, Version: codec.ProtocolV5, DiscReason: 0x99}
			_ = b.sendPacket(conn, disc)
		}
		_ = conn.Close()
	}
	if err := b.store.DeleteSession(ctx, clientID); err != nil {
		return err
	}
	return b.store.ClearOffline(ctx, clientID)
}

// protocolName 将 MQTT 协议级别映射为可读版本名。
func protocolName(v byte) string {
	switch v {
	case codec.ProtocolV31:
		return "3.1"
	case codec.ProtocolV311:
		return "3.1.1"
	case codec.ProtocolV5:
		return "5.0"
	default:
		return fmt.Sprintf("unknown(%d)", v)
	}
}
