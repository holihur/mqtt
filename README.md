# MQTT Broker — Go + Redis Cluster
[![CI](https://github.com/holihur/mqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/holihur/mqtt/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/holihur/mqtt/branch/main/graph/badge.svg)](https://codecov.io/gh/holihur/mqtt)

支持 **MQTT 3.1 (0x03) / 3.1.1 (0x04) / 5.0 (0x05)** 的分布式 Broker，单机可跑，多实例通过 Redis 水平扩展。

## 一键安装

```bash
curl -fsSL https://raw.githubusercontent.com/holihur/mqtt/main/install.sh | sh

# 或指定版本 / 安装目录 / 附带 systemd 服务 (需 root)
sh install.sh --version v0.2.0 --prefix /usr/local/bin --service
```

脚本会下载对应平台 (linux/darwin · amd64/arm64) 的 release 二进制、校验 sha256 后安装到 `/usr/local/bin/broker`。

## 架构

```
[TCP:1883] [WS:8083] ─┐
                      ├─ transport.Listener ── Connection (Parser + CodecRouter)
[Redis] ◄─────────────┘                            │
                                     Broker (Session/Trie/QoS/Retain/Will)
                                         │  ▲
                                    Redis Store + Cluster PubSub (mqtt:cluster)
```

- **编解码**: 手写 VarInt + Properties，v3/v5 分流，剩余长度 0-256MB 校验
- **路由**: 本地 Trie 匹配（`+`/`#`，`$SYS` 隔离）+ Redis 广播到集群其他节点
- **会话**: `CleanSession/CleanStart + SessionExpiryInterval` 统一模型，`0=断开即删, 0xFFFFFFFF=永不过期`，落 Redis 可跨节点接管
- **QoS**: 0 直通、1 PUBACK 重传、2 四步握手 + 幂等去重，`ReceiveMaximum` 背压
- **Retain/Will**: Retain 存 Redis，Will 支持 `DelayInterval`
- **集群**: Redis PubSub `mqtt:cluster` + 心跳 `mqtt:nodes:<id>` TTL 15s

## 快速开始

```bash
# 依赖 Redis
redis-server &

# 单机内存版
task run-memory

# 单机 Redis 版
task run

# 多实例集群（手动）
./bin/broker -tcp :1883 -ws :8083 -redis 127.0.0.1:6379 -node n1
./bin/broker -tcp :1884 -ws :8084 -redis 127.0.0.1:6379 -node n2

# Docker 集群
docker compose up --build
```

## 测试

```bash
go test ./... -race -count=1 -v
```

使用任意 MQTT 客户端验证双版本：

```bash
# v3.1.1
mosquitto_pub -h 127.0.0.1 -p 1883 -t "test/hello" -m "hi" -q 1
mosquitto_sub -h 127.0.0.1 -p 1883 -t "test/#" -v

# v5 (mosquitto 2.x)
mosquitto_pub -h 127.0.0.1 -p 1883 -t "test/v5" -m "v5 hi" -q 1 --property user-property k v
```

WebSocket: `ws://localhost:8083/mqtt`

## 配置

```
- -tcp   :1883            TCP 监听
- -ws    :8083            WS 监听 (空则禁用)
- -redis 127.0.0.1:6379  Redis 地址 (逗号分隔多地址即集群, 空则纯内存)
- -pprof :6060            pprof 监听 (空则禁用, 同时提供 /metrics /healthz /readyz)
- -admin-api :6061        管理 API 监听 (空则禁用, 见 docs/admin.md)
- -admin-api-token <t>    管理 API Bearer token (空则仅允许 loopback)
- -admin-api-tls          管理 API 走 TLS (复用 -tls-cert/-tls-key)
- -webui :8080            内嵌 dashboard 监听地址 (空则禁用, 同端口提供 /api/v1)
- -node  <id>            节点 ID
```

管理 API 提供客户端/会话/订阅/retain 查看与操作、消息发布、ACL 热加载，详见 [`docs/admin.md`](docs/admin.md)。

## Web Dashboard (内嵌)

React + Vite 构建的管理控制台已通过 `go:embed` 嵌入 broker 二进制，最终 release 为单个可执行文件。

```bash
# 启动内嵌 dashboard (:8080), 同端口自动提供 /api/v1
./bin/broker -webui :8080 -admin-api-token 'change-me'

# 浏览器访问 http://localhost:8080 , 在 Settings 中填入 Admin Token 即可
```

- 前端源码在 [`web/dashboard`](web/dashboard)，构建脚本 `task webui` 会将其产物嵌入 `internal/webui/dist`。
- `-webui` 与 `-admin-api` 独立：前者面向人（UI + API 同源），后者面向脚本（纯 API）。
- 开发模式：`cd web/dashboard && npm run dev`（Vite 已代理 `/api` → `127.0.0.1:6061`）。

### Live 订阅 (MQTT.js over WebSocket)

Dashboard 的 **Live** 页通过 [MQTT.js](https://github.com/mqttjs/MQTT.js) 经 WebSocket 直接订阅 broker 主题，实时查看消息。

```bash
# 需要开启 WS 与匿名访问 (或用 Live 页的用户名/密码接 SimpleAuth/DB/FileACL)
./bin/broker -ws :8083 -webui :8080 -allow-anonymous true
```

Dashboard 会自动从 `/api/v1/info` 读取 broker 配置的 WS 监听地址并拼接连接 URL，
所以修改 `-ws` 地址后无需在 Settings 里手动改。同 hostname 下即使 dashboard 与 WS 端口不同
（如 `-webui :8080 -ws :8083`）也能握手成功；仅当 dashboard 与 WS 部署在**不同主机名**时，
才需用 `-ws-allow-origins` 放行 dashboard 的 Origin：

```bash
./bin/broker -ws :8083 -webui :8080 -allow-anonymous true \
  -ws-allow-origins 'https://console.example.com'
# 内网简单场景可用 * 放行全部 Origin
./bin/broker -ws :8083 -webui :8080 -allow-anonymous true -ws-allow-origins '*'
```

> WS 连接地址优先级：Settings 手动配置 > 从 `/api/v1/info` 自动发现 > 默认 `ws://<host>:8083/mqtt`。
> 用户名/密码在 Live 页填写并保存在 localStorage。

## 消息持久化 (SQL 历史消息)

默认 broker 只持久化 retain/session/离线队列，普通 PUBLISH 仅存内存、重启即丢。
可通过 hook 将**全部 PUBLISH 异步批量写入 SQL**（PostgreSQL / MySQL / SQLite），
用于历史查询、审计、BI 分析。

```bash
# SQLite (文件路径)
./bin/broker -msg-persist-dsn ./data/mqtt_messages.db

# PostgreSQL (需先建表，见下方 DDL；hook 自动用 $1..$7 占位符)
./bin/broker -msg-persist-dsn "postgres://user:pass@localhost/mqtt?sslmode=disable" \
  -msg-persist-batch-size 1000 -msg-persist-flush-interval 5s

# 可选参数
-msg-persist-table <表名>            默认 mqtt_messages
-msg-persist-batch-size <n>          每批条数, 默认 1000
-msg-persist-flush-interval <d>      最大积压时间, 默认 5s
-msg-persist-queue-capacity <n>      内存队列容量, 默认 10000
-msg-persist-drop-policy drop|block  队列满策略, 默认 drop (不阻塞发布路径)
-msg-persist-skip-retain             跳过 retain 消息
-msg-persist-node-id <id>            落库节点 ID (默认取 -node)
```

PostgreSQL DDL（`created_at` 为 unix 毫秒，`message_expiry` 保留列，hook 不感知 MQTT 过期）：

```sql
CREATE TABLE mqtt_messages (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(256),
    topic VARCHAR(512) NOT NULL,
    payload BYTEA,
    qos SMALLINT,
    retain BOOLEAN,
    node_id VARCHAR(50),
    created_at BIGINT NOT NULL,
    message_expiry INT DEFAULT 0
);
CREATE INDEX idx_topic ON mqtt_messages (topic);
CREATE INDEX idx_client_time ON mqtt_messages (client_id, created_at);
CREATE INDEX idx_created ON mqtt_messages (created_at);
```

也可编程方式注入任意 SQL 驱动（hook 模式，见 `internal/hook/message_persister.go` 与
`examples/hook/message_persister`）：

```go
h, _ := hook.NewMessagePersisterHook(db, hook.MessagePersisterConfig{BatchSize: 1000})
b.RegisterHook(h)
defer h.Close()
```

设计要点：`OnPublish` 永远返回 nil（不拒绝/不阻塞路由），先复制 payload 再入队
（broker 的 packet 缓冲会被复用），批量事务写入失败即丢弃并计入 `InsertErrors`
（at-most-once），队列满时按 `drop-policy` 丢弃（默认立即丢弃，不阻塞发布路径）。

## 安全与扩展

- **Auth**：`AllowAll` / `SimpleAuth` / `JWT (HS256)` / `FileACL`（`--jwt-secret/--acl`），支持 `Chain`
- **Fuzz**：`FuzzDecode`/`FuzzSplitFrame` 10w exec/s
- **基准**：`docs/bench.md`（CI 自动更新，见下方），`pprof + /metrics` 已暴露
- **一键开发**：`task dev` 自动拉起 `redis` + `broker`（`:1883/:8083/:6060`）

## 基准与压测（CI 自动更新）
<!-- BENCH_START -->
> 更新: 2026-09-03 04:35 UTC | goos: linux goarch: amd64 | cpu: AMD EPYC 7763 64-Core Processor                
>
> ```
> goos: linux
goarch: amd64
cpu: AMD EPYC 7763 64-Core Processor                
Benchmark10kClients-4          	       1	    578985 ns/op	   18168 B/op	      76 allocs/op
BenchmarkPublishThroughput-4   	       1	     47529 ns/op	      64 B/op	       3 allocs/op
> ```
>
> - Benchmark10kClients-4 1 578985 ns/op 18168 B/op 76 allocs/op
> - BenchmarkPublishThroughput-4 1 47529 ns/op 64 B/op 3 allocs/op
> - 详见 [`docs/bench.md`](docs/bench.md) 与 Artifacts `bench.txt`/`c10k.txt`

<!-- BENCH_END -->
