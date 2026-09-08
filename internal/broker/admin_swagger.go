package broker

// 管理 API 的 OpenAPI 3.0 描述与 Swagger UI。
//
// 端点：
//
//	GET /api/v1/openapi.json   OpenAPI 3.0 spec (JSON)
//	GET /swagger               Swagger UI (CDN 版 swagger-ui, 需浏览器可访问外网)
//
// 仅描述文档，不含敏感数据，因此与 /api/v1/* 不同，无需 Bearer 即可访问。

import (
	"encoding/json"
	"net/http"
)

type openAPIParam struct {
	Name     string `json:"name"`
	In       string `json:"in"`
	Required bool   `json:"required,omitempty"`
	Schema   any    `json:"schema"`
	Desc     string `json:"description,omitempty"`
}

type openAPIOperation struct {
	Tags        []string                 `json:"tags"`
	Summary     string                   `json:"summary"`
	OperationID string                   `json:"operationId,omitempty"`
	Parameters  []openAPIParam           `json:"parameters,omitempty"`
	RequestBody *openAPIRequestBody      `json:"requestBody,omitempty"`
	Responses   map[string]openAPIReturn `json:"responses"`
}

type openAPIReturn struct {
	Description string `json:"description"`
	Content     any    `json:"content,omitempty"`
}

type openAPIRequestBody struct {
	Required bool `json:"required"`
	Content  any  `json:"content"`
}

func jsonContent() any {
	return map[string]any{
		"application/json": map[string]any{"schema": map[string]any{"type": "object"}},
	}
}

func okJSON() openAPIReturn {
	return openAPIReturn{Description: "OK", Content: jsonContent()}
}

func errJSON(codes ...string) map[string]openAPIReturn {
	m := map[string]openAPIReturn{
		"200": okJSON(),
	}
	for _, c := range codes {
		m[c] = openAPIReturn{Description: "Error", Content: jsonContent()}
	}
	return m
}

// openAPISpec 构建管理 API 的 OpenAPI 3.0 文档。
func openAPISpec() map[string]any {
	get := func(id, summary string, params []openAPIParam, codes ...string) openAPIOperation {
		return openAPIOperation{Tags: []string{"admin"}, Summary: summary, OperationID: id, Parameters: params, Responses: errJSON(codes...)}
	}
	post := func(id, summary string, body any, codes ...string) openAPIOperation {
		return openAPIOperation{
			Tags: []string{"admin"}, Summary: summary, OperationID: id,
			RequestBody: &openAPIRequestBody{Required: true, Content: body},
			Responses:   errJSON(codes...),
		}
	}
	del := func(id, summary string, params []openAPIParam, codes ...string) openAPIOperation {
		return openAPIOperation{Tags: []string{"admin"}, Summary: summary, OperationID: id, Parameters: params, Responses: errJSON(codes...)}
	}

	pathParam := func(name, desc string) openAPIParam {
		return openAPIParam{Name: name, In: "path", Required: true, Schema: map[string]any{"type": "string"}, Desc: desc}
	}
	queryParam := func(name, desc string) openAPIParam {
		return openAPIParam{Name: name, In: "query", Schema: map[string]any{"type": "string"}, Desc: desc}
	}

	paths := map[string]any{
		"/api/v1/info": map[string]any{
			"get": get("getInfo", "节点与版本信息", nil, "500"),
		},
		"/api/v1/stats": map[string]any{
			"get": get("getStats", "broker 统计 (消息数/连接数/会话/retain)", nil, "500"),
		},
		"/api/v1/health": map[string]any{
			"get": get("getHealth", "健康检查 (store ping + 资源水位)", nil, "503"),
		},
		"/api/v1/clients": map[string]any{
			"get": get("listClients", "在线客户端列表", nil, "500"),
		},
		"/api/v1/clients/{clientID}": map[string]any{
			"get":    get("getClient", "单个在线客户端详情", []openAPIParam{pathParam("clientID", "MQTT client id")}, "404"),
			"delete": del("kickClient", "踢下线 (v5 发 DISCONNECT 0x99)", []openAPIParam{pathParam("clientID", "MQTT client id")}, "404"),
		},
		"/api/v1/sessions": map[string]any{
			"get": get("listSessions", "本节点会话列表 (含离线持久会话)", nil, "500"),
		},
		"/api/v1/sessions/{clientID}": map[string]any{
			"get":    get("getSession", "单个会话详情", []openAPIParam{pathParam("clientID", "MQTT client id")}, "404"),
			"delete": del("deleteSession", "删除会话 (断开+清订阅+清 store)", []openAPIParam{pathParam("clientID", "MQTT client id")}, "500"),
		},
		"/api/v1/subscriptions": map[string]any{
			"get": get("listSubscriptions", "全部订阅", nil, "500"),
		},
		"/api/v1/subscriptions/{clientID}": map[string]any{
			"get": get("getClientSubscriptions", "某客户端的订阅", []openAPIParam{pathParam("clientID", "MQTT client id")}, "404"),
		},
		"/api/v1/subscriptions/match": map[string]any{
			"get": get("matchSubscriptions", "按 MQTT 通配符规则匹配某具体主题的订阅者",
				[]openAPIParam{queryParam("topic", "具体主题 (不含 +/#)")}, "400"),
		},
		"/api/v1/retained": map[string]any{
			"get":    get("listRetained", "retain 列表", []openAPIParam{queryParam("with_payload", "true 时含 base64 payload")}, "500"),
			"delete": del("deleteRetained", "删除 retain (?topic=t 或 ?all=true)", []openAPIParam{queryParam("topic", "要删除的 topic"), queryParam("all", "true 清空全部")}, "400", "500"),
		},
		"/api/v1/publish": map[string]any{
			"post": post("publish", "通过管理 API 发布消息",
				jsonContent(), "400"),
		},
		"/api/v1/nodes": map[string]any{
			"get": get("listNodes", "集群节点列表", nil, "500"),
		},
		"/api/v1/acl/reload": map[string]any{
			"post": post("reloadACL", "热加载 FileACL", jsonContent(), "400", "500"),
		},
		"/mcp": map[string]any{
			"post": openAPIOperation{
				Tags: []string{"mcp"}, Summary: "MCP (Model Context Protocol) JSON-RPC 入口",
				Responses:  errJSON("401", "400"),
				Parameters: []openAPIParam{{Name: "Authorization", In: "header", Required: true, Schema: map[string]any{"type": "string"}, Desc: "Bearer <MCP 授权 key>"}},
			},
		},
	}

	return map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":       "mqtt-go Admin API",
			"description": "mqtt-go broker 管理 REST API + MCP 入口。除 openapi.json 与 /swagger 外均需 Bearer token (未配置 token 时仅限 loopback)。",
			"version":     "1.0.0",
		},
		"paths": paths,
		"components": map[string]any{
			"securitySchemes": map[string]any{
				"bearerAuth": map[string]any{"type": "http", "scheme": "bearer"},
			},
		},
		"security": []any{map[string]any{"bearerAuth": []any{}}},
	}
}

// registerSwagger 挂载 openapi.json 与 Swagger UI。
// 注意：注册在 auth 之外（文档本身非敏感），其余 /api/v1/* 仍走鉴权。
func (s *adminServer) registerSwagger(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(openAPISpec())
	})
	mux.HandleFunc("GET /swagger", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(swaggerUIHTML))
	})
}

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>mqtt-go Admin API — Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
<script>
  window.onload = () => {
    window.ui = SwaggerUIBundle({
      url: '/api/v1/openapi.json',
      dom_id: '#swagger-ui',
      persistAuthorization: true,
    });
  };
</script>
</body>
</html>
`
