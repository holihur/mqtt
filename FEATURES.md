# Features — MQTT Broker (Go + Redis Cluster)

> 覆盖协议、传输、会话、路由、集群、安全、可观测与长期运行能力。`✓` 已实现 · `—` 暂不支持

## 1. 协议

| 特性 | 3.1 (0x03) | 3.1.1 (0x04) | 5.0 (0x05) | 说明 |
|---|---|---|---|---|
| CONNECT / CONNACK | ✓ | ✓ | ✓ | `CleanSession/CleanStart` 统一模型；`SessionExpiryInterval 0=断开即删, 0xFFFFFFFF=永不过期` |
| PUBLISH | ✓ | ✓ | ✓ | `DUP/Retain` 语义完整 |
| PUBACK / PUBREC / PUBREL / PUBCOMP | ✓ | ✓ | ✓ | QoS 1/2 全链路 |
| SUBSCRIBE / SUBACK | ✓ | ✓ | ✓ | `NoLocal / RAP / RH` (v5) |
| UNSUBSCRIBE / UNSUBACK | ✓ | ✓ | ✓ | |
| PINGREQ / PINGRESP | ✓ | ✓ | ✓ | `KeepAlive 1.5 ×` 探活 |
| DISCONNECT | ✓ | ✓ | ✓ | `0x8B Server shutting down` 优雅关闭 |
| AUTH | — | — | ✓ | 预留编解码 |
| VarInt / Remaining Length | ✓ | ✓ | ✓ | 手写 VarInt，0-256MB 校验，恶意包 `ErrMalformedRemainingLength` |
| Properties | — | — | ✓ | 27 种属性全量编解码：`SessionExpiry, ReceiveMaximum, MaximumPacketSize, TopicAlias, UserProperty` 等；`FuzzDecode 10w exec/s` |
| TopicAlias | — | — | ✓ | `AliasToTopic/TopicToAlias` 双向映射，`TopicAliasMaximum ≤100` 限流 |

## 2. 传输

| 特性 | 说明 |
|---|---|
| TCP | `:1883` 默认，`TCP_NODELAY + KeepAlive 3m + 32KB R/W Buffer`，`sem 20k` 并发 |
| TLS / mTLS | `--tls-cert/--tls-key/--tls-ca`，`GetCertificate` 热加载，`MinVersion TLS1.2`，`RequireAndVerifyClientCert` |
| WebSocket | `:8083`，`/` 与 `/mqtt` 双路径，`gorilla/websocket`  `BinaryMessage`，`CheckOrigin` 防 CSRF，`wsConn` 统一 `net.Conn` |
| WS 互通 | 已验证 `WS↔WS` 与 `WS↔TCP` 跨传输投递（`ws_test.go` 5 用例） |

## 3. QoS 与流控

| 特性 | 说明 |
|---|---|
| QoS 0 | 直通，无状态 |
| QoS 1 | `PUBACK` + 20s `scheduleRetry` 重传，`Dup` 标记 |
| QoS 2 | 四步握手 `PUBLISH→PUBREC→PUBREL→PUBCOMP`，`(clientID,packetID)` 幂等去重 |
| ReceiveMaximum | 按 `CONNECT` 宣告背压，`inflight > ReceiveMaximum` 时 `CanSend()==false` 入 `offline` 队列 |
| 离线队列 | `EnqueueOffline/DequeueOffline/ClearOffline`，`cap 1000` 防爆 |
| 重传 | `handleRetry` 定时重发未 ack 的 `inflight` |

## 4. 会话与状态

| 特性 | 说明 |
|---|---|
| Session | `session.Session` 统一 `CleanStart + Expiry` 模型，`Sessions map + Store` 双层 |
| 持久化 | `Store` 接口：`MemoryStore`（并发安全 map+RWMutex） / `RedisStore`（JSON + `mqtt:session:` 前缀），`Save/Get/DeleteSession` |
| 跨节点接管 | `GetSession` 先内存后 Redis，`expiry 0` 清空 `Subscriptions/Inflight/offline` |
| TopicAlias 状态 | `sess.AliasToTopic/TopicToAlias` 随会话持久化 |
| 事务 | `SaveSession` 2s `bgCtx` 超时，失败 `slog.Warn` 非静默吞错 |

## 5. 主题路由

| 特性 | 说明 |
|---|---|
| Trie | `topic.Trie` 前缀树，按 `/` 分层，`children map[string]*node` + `subs map[client#filter]` |
| 通配 | `+` 单层，`#` 必须末层且独占，`IsValidFilter` 校验 |
| 隔离 | `$SYS/` 前缀不被 `# / +` 命中 |
| 剪枝 | `Remove` 后空分支自动 `prune` |
| 并发 | `RWMutex`，`Match` 栈式 DFS，`<1µs` |

## 6. Retain / Will / WAL

| 特性 | 说明 |
|---|---|
| Retain | `SaveRetained/DeleteRetained/ListRetained/GetRetainedStats` 落 `Store`（`Redis mqtt:retain:` / `Pebble` / `Memory`），`PUBLISH retain + payload==""` 清空；配额见 §11 |
| 配额 | `MaxRetainedMessages 10000 / MaxRetainedSize 1GB / MaxRetainPerTopic 1000 / MaxRetainSizePerTopic 100MB`，超限 `PUBACK 0x97` + `mqtt_retain_quota_exceeded_total{reason}` |
| WAL | `Store` 接口可插拔：`MemoryStore / RedisStore / PebbleStore (./data/wal, 默认开启)` + `FallbackStore(Redis→Pebble)`；`--wal-dir="-"` 禁用，`WithWALStore(s Store)` 注入 Badger/任意实现 |
| Will | `Will.Topic/Payload/QoS/Retain/DelayInterval`，`DelayInterval ≤86400` 限幅，`AfterFunc` 延迟投递，`$SYS/` 非法直接丢弃 |
| 遗嘱 ACL | `handleWill` 前二次 `Authorize` |

## 7. 集群

| 特性 | 说明 |
|---|---|
| 发现 | `SET mqtt:nodes:<id> <unix> EX 15s` 心跳 5s，`SCAN mqtt:nodes:*` 列节点 |
| 路由 | `PUBLISH mqtt:cluster` Redis PubSub 广播 JSON `ClusterMessage{From,Topic,Payload,QoS,Retain}`，本地 `deliverLocal` 二次匹配 |
| 隔离 | `From == nodeID` 自发自收跳过 |
| 共享订阅 | `$share/<group>/<filter>` 内存 `sharedSubs map[group]map[filter][]clientID` 轮询投递，`group` 粒度 `sharedIdx` |

## 8. 安全

| 特性 | 说明 |
|---|---|
| Auth | `AllowAll / DenyAll / SimpleAuth (ConstantTimeCompare) / JWT HS256-HS512 (exp/client_id 校验) / Chain` |
| FileACL | `user/client/topic/read|write|readwrite` 规则，`#`/`+` 通配，`matchMqttFilter` 二次校验；**热加载**：`5s` 轮询 `mtime`，`SIGHUP` 触达时 `slog.Info acl reloaded` |
| 默认拒绝 | `AllowAnonymous=false` 时无链即 `DenyAll`（`bbcf864` 修复） |
| TLS | 见传输；证书 `GetCertificate` 免重启 |

## 9. 长期运行（7×24）

| 特性 | 说明 |
|---|---|
| 优雅关闭 | `Broker.Shutdown(ctx 30s)`：`广播 DISCONNECT 0x8B → 5s 排空 → 100ms 轮询 inflight==0`；`main.go`  `SIGINT/SIGTERM → Shutdown 30s → cancel → 二次信号 force exit` |
| 探针 | `PprofAddr` 复用 `mux`：`/healthz 200 ok`，`/readyz` 200/503：`redis Ping <50ms` 且 `conns ≤16000 (80% of 20k sem)` |
| 背压 | `Config{MaxConnections 20000, MaxPublishPerSec 100, MaxSubscribePerSec 20}`；超限 `CONNACK 0x97 / PUBACK 0x97 / SUBACK 0x97`，`mqtt_packet_dropped_total{reason="max_connections|publish_rate|subscribe_rate|auth|acl"}` |
| 超时 | 全链路 `storeCtx 2s` / `bgCtx 2s` / `cluster Publish bgCtx` / `redis Ping 50ms` 替代裸 `Background` |
| 限包 | `MaxPacketSize 1MB` 默认，`Topic >4096 / Payload >1M` 直接丢弃或 `Close` |

## 10. 可观测

| 特性 | 说明 |
|---|---|
| 日志 | `logger.Init(level)`：`debug/info/warn/error`；`debug` 带 `AddSource`；分级：`INFO client connect/disconnect/publish/subscribe/auth failed`，`DEBUG packet recv/send hex(512截断) + type/topic/packetID` |
| 指标 | `promhttp /metrics`：`mqtt_messages_received_total / sent_total / inflight / auth_failed_total / packet_dropped_total{reason} / redis_latency_seconds (Histogram)` |
| 调试 | `pprof + /healthz + /readyz` 同端口；`go tool pprof /debug/pprof/profile` |
| 追踪 | `packetHex()` 截断 hex 供 `trace_id=clientID:packetID` 关联 |

## 11. 性能与压测

| 特性 | 说明 |
|---|---|
| 缓冲 | `codec.bufPool (sync.Pool, <64KB)` + `packetPool`，`transport 32KB R/W` |
| 基准 | `BenchmarkPublishThroughput ~19µs 176B 5 alloc`，`Benchmark10kClients ~478ms 74 alloc`（`bench` 包） |
| C10K | `task c10k / c10k-quick`：`10k 持有 60s`，`Tune limits: somaxconn/tcp_max_syn_backlog` |
| Fuzz | `FuzzDecode / FuzzSplitFrame 10w exec/s`，`go vet / golangci 0 / govulncheck` 全绿 |

## 12. 运维

| 特性 | 说明 |
|---|---|
| 配置 | 旗标：`-tcp/-ws/-redis/-pprof/-node/-acl/-jwt-secret/-allow-anonymous/-log-level/-tls-cert/-tls-key/-tls-ca` |
| 一键开发 | `task dev` 拉起 `redis + broker :1883/:8083/:6060`，`task dev-down/logs/bench/fuzz/c10k` |
| 容器 | `Dockerfile golang:1.25-alpine → alpine:3.22`，`docker-compose` 双 `broker1/2 + redis + prometheus` |
| CI | `build / Test(race) / Bench smoke / Fuzz 3s / Govulncheck / Bench report → README BENCH_* / mosquitto clients / c10k 10k/60s` |

## 13. 显式不支持

- `MQTT 5 AUTH` 报文仅预留编解码，未接入鉴权流程
- `Shared Subscription` 集群一致性哈希（当前仅单机内存轮询，未跨 Redis 选主）
- `Retain` `RH=1/2` 严格语义与 `MessageExpiry` 自动过期（当前均视为 `RH=0` 透传）
- `WAL` 本地持久化（当前 `Memory → Redis` 两级，`Pebble/Badger` 在 Not yet specified）
- 多租户计费、前台控制台

> `Retain` 全局/单 topic 配额已于 #2 落地（见 §11 与 `docs/mqtt-support.md §11`），上条“无配额”已废止。

---
> 详见 `README.md` 架构图、`docs/mqtt-support.md` 版本×特性矩阵、`docs/compliance.md` Paho 8 用例、`docs/bench.md` 基线。
