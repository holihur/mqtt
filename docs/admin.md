# 管理 API (Management API)

Broker 提供面向运维的 HTTP REST 接口 (JSON)，用于查看与控制运行中的 broker：
客户端、会话、订阅、retain 消息、集群节点、消息发布、ACL 热加载。

与 `/metrics`、`/debug/pprof`（`-pprof` 端口）分离，**默认禁用**，独立监听端口便于单独做网络隔离。

## 启用

```bash
# 启用管理 API, 监听 :6061, 要求 Bearer token
./bin/broker ... -admin-api :6061 -admin-api-token 'change-me'

# 无 token: 仅允许 loopback 访问 (适合本机运维)
./bin/broker ... -admin-api 127.0.0.1:6061

# 走 TLS (复用 -tls-cert / -tls-key)
./bin/broker ... -admin-api :6061 -admin-api-tls -tls-cert cert.pem -tls-key key.pem
```

参数：

| flag | 默认 | 说明 |
|---|---|---|
| `-admin-api` | 空 (禁用) | 管理 API 监听地址，如 `:6061` |
| `-admin-api-token` | 空 | Bearer token；未设置时仅允许 loopback |
| `-admin-api-tls` | false | 使用 `-tls-cert/-tls-key` 证书走 TLS |

## 鉴权

- 请求头 `Authorization: Bearer <token>`（或兼容 `X-Admin-Token: <token>`）
- token 常量时间比对（`crypto/subtle`）
- 未配置 token 时仅放行 loopback 来源
- 未授权返回 `401 {"error":"unauthorized"}`

```bash
curl -H 'Authorization: Bearer change-me' http://127.0.0.1:6061/api/v1/info
```

## 端点一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/v1/info` | 节点 ID、版本、uptime、模式、redis 地址 |
| GET | `/api/v1/stats` | 消息收发、连接/会话数、retain 配额、节点列表 |
| GET | `/api/v1/health` | 健康检查（redis ping + 资源水位），200/503 |
| GET | `/api/v1/clients` | 在线客户端列表 |
| GET | `/api/v1/clients/{clientId}` | 单个客户端详情，404 若离线 |
| DELETE | `/api/v1/clients/{clientId}` | 踢下线（v5 发 DISCONNECT 0x99 administrative action） |
| GET | `/api/v1/sessions` | 本节点会话列表（含离线持久会话） |
| GET | `/api/v1/sessions/{clientId}` | 单个会话详情 |
| DELETE | `/api/v1/sessions/{clientId}` | 删除会话（踢下线 + 清 store + 清订阅 + 清离线队列） |
| GET | `/api/v1/subscriptions` | 全部订阅（filter/clientId/qos/noLocal） |
| GET | `/api/v1/subscriptions/{clientId}` | 某客户端订阅 |
| GET | `/api/v1/retained` | retain 列表；`?with_payload=true` 附带 base64 负载 |
| DELETE | `/api/v1/retained?topic=t` | 删除单个 retain；`?all=true` 清空全部 |
| POST | `/api/v1/publish` | 发布消息 |
| GET | `/api/v1/nodes` | 集群节点列表 |
| POST | `/api/v1/acl/reload` | 热加载 FileACL（无 FileACL 返回 400） |

> 集群模式下 `/clients`、`/sessions`、`/subscriptions` 仅反映**本节点**内存状态；
> `/nodes` 通过 Redis 心跳键列出全部在线节点。

## 示例

```bash
API=http://127.0.0.1:6061/api/v1
AUTH='Authorization: Bearer change-me'

# 查看统计
curl -s -H "$AUTH" $API/stats | jq

# 踢掉客户端
curl -s -X DELETE -H "$AUTH" $API/clients/some-device

# 发布 retain 消息 (文本)
curl -s -X POST -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"topic":"dev/1/status","payload":"online","qos":1,"retain":true}' $API/publish

# 发布二进制消息 (base64)
curl -s -X POST -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"topic":"dev/1/fw","payloadB64":"AP8Q","qos":0}' $API/publish

# 查看 retain (含 payload)
curl -s -H "$AUTH" "$API/retained?with_payload=true" | jq

# 删除 retain / 清空
curl -s -X DELETE -H "$AUTH" "$API/retained?topic=dev%2F1%2Fstatus"
curl -s -X DELETE -H "$AUTH" "$API/retained?all=true"

# 热加载 ACL
curl -s -X POST -H "$AUTH" $API/acl/reload
```

## 设计要点

- **发布复用嵌入式发布路径**（`Broker.Publish`）：本地 Trie 投递 + 集群广播 + retain 配额校验与落库，与外部客户端行为一致。
- **踢下线**只断开连接，会话按正常断开语义处理（持久会话保留、will 触发、离线队列保留）；**删除会话**则同时清空 store 与离线队列。
- **删除在线会话**：先置 `Session.Deleted` 标记再关闭连接，防止异步断开回调把会话写回 store（`onClientDisconnect` 检测到标记后直接返回）。
- 所有读快照在 `b.mu` / `sess.Mu` 下取数，避免与连接生命周期竞态。
