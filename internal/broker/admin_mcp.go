package broker

// MCP (Model Context Protocol) 服务端 — 让用户通过 MCP 客户端 (Claude/Cursor 等)
// 直接操作 broker 管理 API。
//
// 传输: Streamable HTTP (无状态, POST /mcp, JSON-RPC 2.0, application/json 响应)。
// 鉴权: 独立的 MCP 授权 key (Config.MCPToken, -mcp-token)。Bearer 或 X-MCP-Token 头。
//   - 未配置 key: 仅允许 loopback 访问 (与管理 API 行为一致)。
//   - 配置了 key: 必须携带正确的 key, 管理 API 的 token 不通用。
//
// 工具集与 /api/v1 管理 REST 接口一一对应, 见 mcpTools()。

import (
	"context"
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

	"mqtt/internal/topic"
)

const mcpProtocolVersion = "2025-03-26"

// ---------------------------------------------------------------------------
// MCP JSON-RPC 信封
// ---------------------------------------------------------------------------

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments,omitempty"`
	} `json:"params"`
}

type mcpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *mcpRPCError    `json:"error,omitempty"`
}

type mcpRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ---------------------------------------------------------------------------
// 工具定义
// ---------------------------------------------------------------------------

type mcpTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

func objSchema(props map[string]any, required ...string) json.RawMessage {
	if len(required) == 0 {
		required = []string{}
	}
	b, _ := json.Marshal(map[string]any{
		"type":       "object",
		"properties": props,
		"required":   required,
	})
	return b
}

func strProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}

func intProp(desc string) map[string]any {
	return map[string]any{"type": "integer", "description": desc}
}

func boolProp(desc string) map[string]any {
	return map[string]any{"type": "boolean", "description": desc}
}

func mcpTools() []mcpTool {
	return []mcpTool{
		{Name: "get_info", Description: "获取 broker 节点与版本信息 (nodeId/version/uptime/mode)",
			InputSchema: objSchema(nil)},
		{Name: "get_stats", Description: "获取 broker 统计 (消息收发数/连接数/会话数/retain 条数与总大小/节点列表)",
			InputSchema: objSchema(nil)},
		{Name: "get_health", Description: "健康检查 (store ping + 资源水位)",
			InputSchema: objSchema(nil)},
		{Name: "list_clients", Description: "列出当前在线客户端",
			InputSchema: objSchema(nil)},
		{Name: "get_client", Description: "查询单个在线客户端详情",
			InputSchema: objSchema(map[string]any{"clientId": strProp("MQTT client id")}, "clientId")},
		{Name: "kick_client", Description: "踢掉在线客户端 (v5 发 DISCONNECT 0x99)",
			InputSchema: objSchema(map[string]any{"clientId": strProp("MQTT client id")}, "clientId")},
		{Name: "list_sessions", Description: "列出本节点会话 (含离线持久会话)",
			InputSchema: objSchema(nil)},
		{Name: "get_session", Description: "查询单个会话详情",
			InputSchema: objSchema(map[string]any{"clientId": strProp("MQTT client id")}, "clientId")},
		{Name: "delete_session", Description: "删除会话: 断开连接 + 清订阅 + 清 store 与离线队列",
			InputSchema: objSchema(map[string]any{"clientId": strProp("MQTT client id")}, "clientId")},
		{Name: "list_subscriptions", Description: "列出全部订阅 (clientId/filter/qos)",
			InputSchema: objSchema(nil)},
		{Name: "get_client_subscriptions", Description: "查询某客户端的全部订阅",
			InputSchema: objSchema(map[string]any{"clientId": strProp("MQTT client id")}, "clientId")},
		{Name: "match_subscriptions", Description: "按 MQTT 通配符规则查询会收到某具体主题消息的订阅者",
			InputSchema: objSchema(map[string]any{"topic": strProp("具体主题, 不含 +/#")}, "topic")},
		{Name: "list_retained", Description: "列出 retained 消息 (默认不含 payload)",
			InputSchema: objSchema(map[string]any{"withPayload": boolProp("true 时返回 base64 payload")})},
		{Name: "delete_retained", Description: "删除 retained 消息: 指定 topic 或清空全部",
			InputSchema: objSchema(map[string]any{
				"topic": strProp("要删除的 topic; all=true 时忽略"),
				"all":   boolProp("true 清空全部 retained"),
			})},
		{Name: "publish", Description: "通过管理通道发布 MQTT 消息 (本地投递 + 集群广播 + retain)",
			InputSchema: objSchema(map[string]any{
				"topic":      strProp("发布主题"),
				"payload":    strProp("UTF-8 文本负载 (payloadB64 优先)"),
				"payloadB64": strProp("二进制负载 base64 编码, 优先于 payload"),
				"qos":        intProp("0/1/2, 默认 0"),
				"retain":     boolProp("是否 retained, 默认 false"),
			}, "topic")},
		{Name: "list_nodes", Description: "列出集群节点",
			InputSchema: objSchema(nil)},
		{Name: "reload_acl", Description: "热加载 FileACL 规则",
			InputSchema: objSchema(nil)},
	}
}

// ---------------------------------------------------------------------------
// mcpServer
// ---------------------------------------------------------------------------

type mcpServer struct {
	b     *Broker
	token string
}

func (b *Broker) newMCPServer() *mcpServer {
	return &mcpServer{b: b, token: b.cfg.MCPToken}
}

// authorized 校验 MCP 授权 key。Bearer 头或 X-MCP-Token; 未配置 key 时仅 loopback。
func (s *mcpServer) authorized(r *http.Request) bool {
	if s.token == "" {
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
	} else if h = r.Header.Get("X-MCP-Token"); h == "" {
		return false
	}
	return h == s.token
}

func (s *mcpServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeMCPJSON(w, http.StatusMethodNotAllowed,
				mcpResponse{JSONRPC: "2.0", Error: &mcpRPCError{Code: -32600, Message: "MCP endpoint accepts POST only"}})
			return
		}
		if !s.authorized(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="mqtt-mcp"`)
			writeMCPJSON(w, http.StatusUnauthorized,
				mcpResponse{JSONRPC: "2.0", Error: &mcpRPCError{Code: -32000, Message: "invalid MCP auth key"}})
			return
		}

		var req mcpRequest
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil || json.Unmarshal(body, &req) != nil {
			writeMCPJSON(w, http.StatusBadRequest,
				mcpResponse{JSONRPC: "2.0", Error: &mcpRPCError{Code: -32700, Message: "parse error: invalid JSON-RPC"}})
			return
		}

		// 通知 (无 id) 不回包
		if len(req.ID) == 0 || string(req.ID) == "null" {
			w.WriteHeader(http.StatusAccepted)
			return
		}

		resp := mcpResponse{JSONRPC: "2.0", ID: req.ID}
		switch req.Method {
		case "initialize":
			resp.Result = map[string]any{
				"protocolVersion": mcpProtocolVersion,
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo": map[string]any{
					"name":    "mqtt-go-admin",
					"version": s.b.versionInfo.version,
				},
			}
		case "ping":
			resp.Result = map[string]any{}
		case "tools/list":
			resp.Result = map[string]any{"tools": mcpTools()}
		case "tools/call":
			resp.Result, resp.Error = s.callTool(r.Context(), req.Params.Name, req.Params.Arguments)
		default:
			resp.Error = &mcpRPCError{Code: -32601, Message: "method not found: " + req.Method}
		}
		writeMCPJSON(w, http.StatusOK, resp)
	})
}

func writeMCPJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// callTool 执行 tools/call。返回 (result, rpcErr)。
func (s *mcpServer) callTool(ctx context.Context, name string, args json.RawMessage) (any, *mcpRPCError) {
	var a map[string]any
	if len(args) > 0 {
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, &mcpRPCError{Code: -32602, Message: "invalid params: " + err.Error()}
		}
	}
	argStr := func(k string) string { v, _ := a[k].(string); return v }
	argBool := func(k string) bool { v, _ := a[k].(bool); return v }
	argInt := func(k string) byte {
		switch v := a[k].(type) {
		case float64:
			return byte(v)
		case string:
			var n int
			_, _ = fmt.Sscanf(v, "%d", &n)
			return byte(n)
		}
		return 0
	}

	switch name {
	case "get_info":
		return s.textResult(s.infoSnapshot()), nil
	case "get_stats":
		st, e := s.statsSnapshot(ctx)
		if e != nil {
			return s.textErrorResult(e.Error()), nil
		}
		return s.textResult(st), nil
	case "get_health":
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if e := s.b.Health(c); e != nil {
			return s.textErrorResult(e.Error()), nil
		}
		return s.textResult(map[string]any{"status": "ok"}), nil
	case "list_clients":
		return s.textResult(s.b.clientInfos()), nil
	case "get_client":
		ci, ok := s.b.clientInfo(argStr("clientId"))
		if !ok {
			return s.textErrorResult(fmt.Sprintf("client %q not connected", argStr("clientId"))), nil
		}
		return s.textResult(ci), nil
	case "kick_client":
		if e := s.b.kickClient(argStr("clientId")); e != nil {
			return s.textErrorResult(e.Error()), nil
		}
		return s.textResult(map[string]any{"ok": true, "clientId": argStr("clientId")}), nil
	case "list_sessions":
		return s.textResult(s.b.sessionInfos()), nil
	case "get_session":
		si, ok := s.b.sessionInfo(argStr("clientId"))
		if !ok {
			return s.textErrorResult(fmt.Sprintf("session %q not found", argStr("clientId"))), nil
		}
		return s.textResult(si), nil
	case "delete_session":
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if e := s.b.deleteSession(c, argStr("clientId")); e != nil {
			return s.textErrorResult(e.Error()), nil
		}
		return s.textResult(map[string]any{"ok": true, "clientId": argStr("clientId")}), nil
	case "list_subscriptions":
		return s.textResult(s.subscriptionList(s.b.trie.Subscriptions())), nil
	case "get_client_subscriptions":
		return s.textResult(s.subscriptionList(s.b.trie.SubscriptionsFor(argStr("clientId")))), nil
	case "match_subscriptions":
		t := argStr("topic")
		if t == "" {
			return s.textErrorResult("missing required argument: topic"), nil
		}
		return s.textResult(s.subscriptionList(s.b.trie.Match(t))), nil
	case "list_retained":
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		msgs, e := s.b.store.ListRetained(c)
		if e != nil {
			return s.textErrorResult(e.Error()), nil
		}
		type retained struct {
			Topic      string `json:"topic"`
			QoS        byte   `json:"qos"`
			Size       int    `json:"size"`
			PayloadB64 string `json:"payloadB64,omitempty"`
		}
		out := make([]retained, 0, len(msgs))
		for _, m := range msgs {
			r := retained{Topic: m.Topic, QoS: m.QoS, Size: len(m.Payload)}
			if argBool("withPayload") {
				r.PayloadB64 = base64.StdEncoding.EncodeToString(m.Payload)
			}
			out = append(out, r)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
		return s.textResult(out), nil
	case "delete_retained":
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if argBool("all") {
			msgs, e := s.b.store.ListRetained(c)
			if e != nil {
				return s.textErrorResult(e.Error()), nil
			}
			n := 0
			for _, m := range msgs {
				if e := s.b.store.DeleteRetained(c, m.Topic); e == nil {
					n++
				}
			}
			return s.textResult(map[string]any{"ok": true, "deleted": n}), nil
		}
		t := argStr("topic")
		if t == "" {
			return s.textErrorResult("missing required argument: topic (or all=true)"), nil
		}
		if e := s.b.store.DeleteRetained(c, t); e != nil {
			return s.textErrorResult(e.Error()), nil
		}
		return s.textResult(map[string]any{"ok": true, "topic": t}), nil
	case "publish":
		t := argStr("topic")
		if t == "" {
			return s.textErrorResult("missing required argument: topic"), nil
		}
		qos := argInt("qos")
		if qos > 2 {
			return s.textErrorResult("qos must be 0..2"), nil
		}
		var payload []byte
		if b64 := argStr("payloadB64"); b64 != "" {
			p, e := base64.StdEncoding.DecodeString(b64)
			if e != nil {
				return s.textErrorResult("invalid payloadB64: " + e.Error()), nil
			}
			payload = p
		} else {
			payload = []byte(argStr("payload"))
		}
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if e := s.b.Publish(c, t, payload, qos, argBool("retain")); e != nil {
			return s.textErrorResult(e.Error()), nil
		}
		slog.Info("mcp publish", "topic", t, "qos", qos, "retain", argBool("retain"), "by", "mcp")
		return s.textResult(map[string]any{"ok": true, "topic": t}), nil
	case "list_nodes":
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		nodes := []string{s.b.nodeID}
		if s.b.cluster != nil {
			if ns, e := s.b.cluster.Nodes(c); e == nil {
				nodes = ns
			}
		}
		sort.Strings(nodes)
		return s.textResult(map[string]any{"nodes": nodes}), nil
	case "reload_acl":
		acls := s.b.findFileACLs()
		if len(acls) == 0 {
			return s.textErrorResult("no FileACL configured"), nil
		}
		reloaded := 0
		for _, facl := range acls {
			ok, e := facl.Reload()
			if e != nil {
				return s.textErrorResult(e.Error()), nil
			}
			if ok {
				reloaded++
			}
		}
		return s.textResult(map[string]any{"ok": true, "reloaded": reloaded}), nil
	default:
		return nil, &mcpRPCError{Code: -32602, Message: "unknown tool: " + name}
	}
}

// infoSnapshot 复用 handleInfo 的逻辑生成节点信息快照。
func (s *mcpServer) infoSnapshot() infoResponse {
	b := s.b
	mode := "standalone"
	if b.cluster != nil {
		mode = "cluster"
	}
	uptime := int64(0)
	if !b.stats.StartedAt.IsZero() {
		uptime = int64(time.Since(b.stats.StartedAt).Seconds())
	}
	return infoResponse{
		NodeID:   b.nodeID,
		Version:  b.versionInfo.version,
		Commit:   b.versionInfo.commit,
		Date:     b.versionInfo.date,
		Uptime:   uptime,
		Mode:     mode,
		Redis:    b.cfg.RedisAddr,
		Admin:    b.cfg.AdminAddr != "",
		AdminTLS: b.cfg.AdminTLS,
		WSAddr:   b.cfg.WSAddr,
	}
}

// statsSnapshot 复用 handleStats 的逻辑生成统计快照。
func (s *mcpServer) statsSnapshot(ctx context.Context) (statsResponse, error) {
	b := s.b
	st := b.Stats()
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	retained, size, err := b.retainedStats(c)
	if err != nil {
		return statsResponse{}, err
	}
	nodes := []string{b.nodeID}
	if b.cluster != nil {
		if ns, e := b.cluster.Nodes(c); e == nil {
			nodes = ns
		}
	}
	sort.Strings(nodes)
	return statsResponse{
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
	}, nil
}

// subscriptionList 把 Trie 订阅条目转换为 MCP 工具输出 (按 clientId/filter 排序)。
func (s *mcpServer) subscriptionList(entries []*topic.SubEntry) []subscriptionResponse {
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
	return out
}

func (s *mcpServer) textResult(v any) map[string]any {
	b, _ := json.Marshal(v)
	return map[string]any{
		"content": []any{map[string]any{"type": "text", "text": string(b)}},
		"isError": false,
	}
}

func (s *mcpServer) textErrorResult(msg string) map[string]any {
	return map[string]any{
		"content": []any{map[string]any{"type": "text", "text": msg}},
		"isError": true,
	}
}
