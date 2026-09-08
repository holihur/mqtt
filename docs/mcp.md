# MCP 接入 (Model Context Protocol)

Broker 内置 MCP 服务端，让 MCP 客户端（Claude Desktop、Cursor、Cline 等）或任何
JSON-RPC 2.0 程序**直接操作 broker 管理能力**：查看/踢掉客户端、管理会话与订阅、
查询/清除 retained 消息、发布消息、热加载 ACL 等。

MCP 入口挂在管理 API / WebUI 监听端口上的 `/mcp` 路径，使用**独立的授权 key**，
与管理 API 的 `-admin-api-token` 互不相通。

## 启用

```bash
# 启用 MCP + 管理端口 + MCP 授权 key
./bin/broker ... -admin-api :6061 -mcp -mcp-token 'change-me-mcp'

# MCP 也可以挂在 WebUI 监听上（dashboard 与 MCP 同端口）
./bin/broker ... -webui 0.0.0.0:8080 -mcp -mcp-token 'change-me-mcp'

# 不设 key: 仅允许 loopback 访问（本机调试用）
./bin/broker ... -admin-api 127.0.0.1:6061 -mcp
```

参数：

| flag | 默认 | 说明 |
|---|---|---|
| `-mcp` | false | 启用 MCP 服务端。注意 Go bool flag 需写 `-mcp=true` 或单独 `-mcp` |
| `-mcp-token` | 空 | MCP 授权 key；未设置时仅允许 loopback |

安全要点：

- MCP key 与 admin token **互相独立**：拿着 admin token 访问 `/mcp` 会被 401 拒绝。
- key 通过 `Authorization: Bearer <key>` 或 `X-MCP-Token: <key>` 请求头携带。
- 未配置 key 时仅放行 loopback 来源（与管理 API 行为一致）。
- 鉴权失败返回 `401 {"jsonrpc":"2.0","error":{"code":-32000,"message":"invalid MCP auth key"}}`。

## 传输协议

Streamable HTTP（无状态）：

- 端点 `POST /mcp`
- 请求体：JSON-RPC 2.0 单条消息
- 响应：`application/json`（非 SSE），通知类消息返回 `202` 无 body

支持的方法：

| 方法 | 说明 |
|---|---|
| `initialize` | 握手，返回 `protocolVersion: 2025-03-26`、capabilities、serverInfo |
| `ping` | 心跳 |
| `tools/list` | 列出全部工具及 inputSchema |
| `tools/call` | 调用工具，`params: {name, arguments}` |

## 工具一览

| 工具 | 对应 REST | 说明 |
|---|---|---|
| `get_info` | GET /api/v1/info | 节点与版本信息 |
| `get_stats` | GET /api/v1/stats | 消息/连接/会话/retain 统计 |
| `get_health` | GET /api/v1/health | 健康检查 |
| `list_clients` | GET /api/v1/clients | 在线客户端列表 |
| `get_client` | GET /api/v1/clients/{id} | 客户端详情 |
| `kick_client` | DELETE /api/v1/clients/{id} | 踢下线 |
| `list_sessions` | GET /api/v1/sessions | 会话列表（含离线） |
| `get_session` | GET /api/v1/sessions/{id} | 会话详情 |
| `delete_session` | DELETE /api/v1/sessions/{id} | 删除会话 |
| `list_subscriptions` | GET /api/v1/subscriptions | 全部订阅 |
| `get_client_subscriptions` | GET /api/v1/subscriptions/{id} | 某客户端订阅 |
| `match_subscriptions` | GET /api/v1/subscriptions/match | 按通配符匹配订阅者 |
| `list_retained` | GET /api/v1/retained | retained 列表 |
| `delete_retained` | DELETE /api/v1/retained | 删除 retained |
| `publish` | POST /api/v1/publish | 发布消息 |
| `list_nodes` | GET /api/v1/nodes | 集群节点 |
| `reload_acl` | POST /api/v1/acl/reload | 热加载 FileACL |

## curl 快速验证

```bash
MCP=http://127.0.0.1:6061/mcp
KEY='change-me-mcp'

# 1. 握手
curl -s $MCP -H "Authorization: Bearer $KEY" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'

# 2. 列出工具
curl -s $MCP -H "X-MCP-Token: $KEY" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

# 3. 发布一条 retained 消息
curl -s $MCP -H "Authorization: Bearer $KEY" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call",
       "params":{"name":"publish",
                 "arguments":{"topic":"mcp/test","payload":"hello","qos":1,"retain":true}}}'

# 4. 查看 retained
curl -s $MCP -H "Authorization: Bearer $KEY" \
  -d '{"jsonrpc":"2.0","id":4,"method":"tools/call",
       "params":{"name":"list_retained","arguments":{"withPayload":true}}}'
```

工具调用结果统一为 MCP content 格式，`text` 字段内是工具输出的 JSON：

```json
{"jsonrpc":"2.0","id":3,
 "result":{"content":[{"type":"text","text":"{\"ok\":true,\"topic\":\"mcp/test\"}"}],
           "isError":false}}
```

业务级失败（如 clientId 不存在）不产生 JSON-RPC 错误，而是 `isError: true` 并在
`text` 中给出原因；协议级错误（未知方法/未知工具/参数非法）才返回 JSON-RPC error。

## MCP 客户端配置示例

Claude Desktop / Cline 等支持 streamable HTTP 的客户端，在 MCP 配置中添加：

```json
{
  "mcpServers": {
    "mqtt-go": {
      "type": "streamableHttp",
      "url": "http://127.0.0.1:6061/mcp",
      "headers": {
        "Authorization": "Bearer change-me-mcp"
      }
    }
  }
}
```

Cursor 的 `mcp.json` 同理。配置后即可在对话中直接让模型执行
"列出当前在线客户端"、"往 sensors/temp 发布 25.3"、"清空 retained" 等操作。

## Swagger / OpenAPI

管理 API 与 MCP 入口附带 OpenAPI 3.0 文档（无需鉴权）：

- `GET /api/v1/openapi.json` — 机器可读 spec
- `GET /swagger` — Swagger UI（浏览器打开，可在页面内填 token 调试）

```bash
curl -s http://127.0.0.1:6061/api/v1/openapi.json | jq .
```
