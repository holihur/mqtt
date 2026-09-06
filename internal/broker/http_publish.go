package broker

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
	"strings"
	"time"
)

// 独立 HTTP 发布接口：与 dashboard / admin API 完全解耦，可单独监听端口，
// 供脚本、上游系统直接 POST 发布 MQTT 消息，无需引入 MQTT 客户端。
//
// 端点 (监听地址由 -http-publish 指定):
//
//	POST /publish   发布消息 {topic, payload|payloadB64, qos, retain}
//	GET  /healthz   存活检查
//
// 鉴权与 admin API 一致：配置 -http-publish-token 后要求 Bearer token，
// 未配置时仅允许 loopback 来源。

// httpPublishHandler 返回带鉴权中间件的独立发布路由。
func (b *Broker) httpPublishHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /publish", b.handleHTTPPublish)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !b.httpPublishAuthorized(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="mqtt-http-publish"`)
			writeJSONResp(w, http.StatusUnauthorized, apiError{Error: "unauthorized"})
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (b *Broker) httpPublishAuthorized(r *http.Request) bool {
	token := b.cfg.HTTPPublishToken
	if token == "" {
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
	return subtle.ConstantTimeCompare([]byte(h), []byte(token)) == 1
}

// parsePublishRequest 解析并校验发布请求 (POST /publish), 返回 (req, payload)。
// 复用 admin 包的 publishRequest 结构。
func parsePublishRequest(r *http.Request) (*publishRequest, []byte, error) {
	var req publishRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
	if err != nil {
		return nil, nil, fmt.Errorf("read body: %w", err)
	}
	if len(body) > 1<<20 {
		return nil, nil, fmt.Errorf("payload too large")
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, nil, fmt.Errorf("invalid json: %w", err)
	}
	if req.Topic == "" {
		return nil, nil, fmt.Errorf("topic is required")
	}
	if req.QoS > 2 {
		return nil, nil, fmt.Errorf("qos must be 0, 1 or 2")
	}
	var payload []byte
	if req.PayloadB64 != "" {
		payload, err = base64.StdEncoding.DecodeString(req.PayloadB64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid payloadB64: %w", err)
		}
	} else {
		payload = []byte(req.Payload)
	}
	return &req, payload, nil
}

// handleHTTPPublish 通过独立 HTTP 端口发布消息 (POST /publish)。
// 复用 Broker.Publish 嵌入式发布路径：本地 Trie 投递 + 集群广播 + retain 落库。
func (b *Broker) handleHTTPPublish(w http.ResponseWriter, r *http.Request) {
	req, payload, err := parsePublishRequest(r)
	if err != nil {
		writeJSONResp(w, http.StatusBadRequest, apiError{Error: err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := b.Publish(ctx, req.Topic, payload, req.QoS, req.Retain); err != nil {
		writeJSONResp(w, http.StatusBadRequest, apiError{Error: err.Error()})
		return
	}
	slog.Info("http publish", "topic", req.Topic, "qos", req.QoS, "retain", req.Retain, "by", r.RemoteAddr)
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "topic": req.Topic})
}

func writeJSONResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
