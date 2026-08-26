# MQTT 特性支持度 — v3.1 / v3.1.1 / v5.0

> 与 `FEATURES.md` 互补：`FEATURES.md` 按模块叙述，本文件按 **MQTT 协议版本×特性** 给出可审计的支持矩阵。版本定义见 `internal/codec/packet.go`：`0x03=MQtIsdp (v3.1)` / `0x04=MQTT (v3.1.1)` / `0x05=MQTT (v5.0)`。

## 图例

| 符号 | 含义 |
|---|---|
| ✅ | 完全支持，已有单测/集成/fuzz 覆盖 |
| ⚠️ | 部分支持（见备注） |
| ❌ | 未支持，报文可编解码但不进入业务流程 |
| ⛔ | 协议不定义（不适用） |

所有 `—` 在原 `FEATURES.md` 中归一为 `❌/⛔`，本文件细化。

---

## 1. 协议版本与传输

| 项 | v3.1 (0x03) | v3.1.1 (0x04) | v5.0 (0x05) | 备注 |
|---|---|---|---|---|
| `ProtocolName` | ✅ `MQIsdp` | ✅ `MQTT` | ✅ `MQTT` | `packet.go: ProtocolV31/V311/V5`，错误版本回 `CONNACK 0x01` 并关闭 |
| TCP `:1883` | ✅ | ✅ | ✅ | `transport.Listener` 统一 `net.Conn` |
| TLS/mTLS | ✅ | ✅ | ✅ | `--tls-cert/--tls-key/--tls-ca`，`MinVersion TLS1.2` |
| WebSocket `:8083` | ✅ | ✅ | ✅ | `/` 与 `/mqtt`，`ws_test.go` 验证 `WS↔TCP` 互通 |
| VarInt / RemainingLength | ✅ | ✅ | ✅ | 手写 `encodeVarInt/decodeVarInt`，`0-256MB` 校验，`ErrMalformedRemainingLength` |
| `MaxPacketSize` 1MB | ✅ | ✅ | ✅ | `Config.MaxPacketSize`，`Topic>4096 / Payload>1M` 丢弃或 `Close` |

`v3.1` 与 `v3.1.1` 在 Broker 侧仅 `ProtocolName/Level` 不同，语义完全一致；下表对 `v3.1/v3.1.1` 合并为 `v3` 仅在 `Properties/ReasonCode` 有差异时拆列。

---

## 2. 控制报文（14 种）

| 报文 | v3 (0x03/0x04) | v5 (0x05) | 备注 |
|---|:---:|:---:|---|
| CONNECT | ✅ | ✅ | 见 §3 |
| CONNACK | ✅ | ✅ | `SessionPresent` + ReasonCode（v3 0-5 映射，v5 0x00-0x9F） |
| PUBLISH | ✅ | ✅ | QoS0-2，`DUP/Retain` 完整；v5 带 Properties |
| PUBACK | ✅ | ✅ | v5 ReasonCode `0x00/0x10/0x80/0x83/0x87/0x97`（`handlePublish` `0x97 quota exceeded`） |
| PUBREC | ✅ | ✅ | QoS2 第 2/3 步 |
| PUBREL | ✅ | ✅ | QoS2 |
| PUBCOMP | ✅ | ✅ | QoS2，`(clientID,packetID)` 幂等去重 |
| SUBSCRIBE | ✅ | ✅ | 见 §5 |
| SUBACK | ✅ | ✅ | v3 返回 QoS / `0x80` 失败；v5 返回 ReasonCode |
| UNSUBSCRIBE | ✅ | ✅ | |
| UNSUBACK | ✅ | ✅ | v5 ReasonCode |
| PINGREQ/PINGRESP | ✅ | ✅ | `KeepAlive*1.5` 超时断开并触发 Will |
| DISCONNECT | ✅ | ✅ | v5 `0x00` 正常 / `0x04` 会话过期 / `0x8B` 优雅关闭 |
| AUTH | ⛔ | ❌ | 仅编解码（`encodeAuth/decodeAuth`），未接入鉴权流程；`FEATURES.md §13` 显式不支持 |

---

## 3. CONNECT / CONNACK

| 特性 | v3 | v5 | 说明 |
|---|:---:|---|---|
| `CleanSession` / `CleanStart` | ✅ | ✅ | 统一为 `session.Session{CleanStart, ExpiryInterval}`，`0=断开即删 / 0xFFFFFFFF=永不过期` |
| `KeepAlive` | ✅ | ✅ | `readLoop` `SetReadDeadline(1.5×KeepAlive)` |
| `ClientID` 为空 | ✅ 自动分配 | ✅ `AssignedClientID` | `auto-<uuid[:8]>`，`CONNACK` 回 `AssignedClientID` |
| `Will` 遗嘱 | ✅ | ✅ | `Topic/Payload/QoS/Retain` + `DelayInterval ≤86400`，`$SYS/` 丢弃 |
| `Username/Password` | ✅ | ✅ | `JWT/FileACL/Chain` 鉴权 |
| `SessionExpiryInterval` | ⛔ | ✅ | `Properties.SessionExpiryInterval`，`CleanStart` 映射 |
| `ReceiveMaximum` | ⛔ | ✅ | `≤65535`，`CanSend()` 背压，`inflight>max` 入 `offline` |
| `MaximumPacketSize` | ⛔ | ✅ | `Properties.MaximumPacketSize`，`broker` 回 `MaximumPacketSize`、超限 `Close` |
| `TopicAliasMaximum` | ⛔ | ✅ | `≤100` 限幅，`AliasToTopic/TopicToAlias` 双向映射 |
| `UserProperty` | ⛔ | ✅ | `≤10` 条，`k≤256/v≤1024`，透传 `PUBLISH/SUBSCRIBE` |
| `AuthMethod/AuthData` | ⛔ | ⚠️ | 编解码通过，未参与握手 |

`CONNACK` v5 额外返回：`ReceiveMaximum=65535 / SharedSubAvailable=1 / MaximumPacketSize / TopicAliasMaximum=100 / AssignedClientID`（按需）。

---

## 4. PUBLISH / QoS

| 项 | v3 | v5 | 说明 |
|---|:---:|---|---|
| QoS0 直通 | ✅ | ✅ | 无状态 |
| QoS1 `PUBACK` + 重传 | ✅ | ✅ | `scheduleRetry 20s`，`Dup` 标记 |
| QoS2 四步 | ✅ | ✅ | `PUBREC→PUBREL→PUBCOMP`，`inflight (clientID,packetID)` 去重 |
| `Retain` | ✅ | ✅ | `SaveRetained/DeleteRetained/ListRetained` 落 `mqtt:retain:`，`payload==""` 清空；配额见 §11 |
| `DUP` | ✅ | ✅ | 重传置位 |
| `PayloadFormatIndicator` | ⛔ | ✅ | 编解码+透传 |
| `MessageExpiryInterval` | ⛔ | ✅ | 编解码+透传 |
| `ContentType` | ⛔ | ✅ | 编解码+透传 |
| `ResponseTopic` | ⛔ | ✅ | 编解码+透传 |
| `CorrelationData` | ⛔ | ✅ | `Binary` 编解码 |
| `TopicAlias` | ⛔ | ✅ | 发送端 `TopicAliasMaximum` 校验，`0 / >max` 回 `0x94` 并 `Close` |
| `SubscriptionID` | ⛔ | ✅ | `SUBSCRIBE` 设 `SubscriptionID`，`PUBLISH` 回显 |
| `UserProperty` | ⛔ | ✅ | 同上 |
| `ReceiveMaximum` 背压 | ⛔ | ✅ | v3 无此概念；v5 超限入 `offline` 队（`cap 1000`） |

`PUBACK` ReasonCode：`0x00 Success / 0x10 No matching subscribers / 0x80 Unspecified / 0x83 Implementation specific / 0x87 Not authorized / 0x97 Quota exceeded`（配额）。

---

## 5. SUBSCRIBE / UNSUBSCRIBE

| 特性 | v3 | v5 | 说明 |
|---|:---:|---|---|
| `+` 单层 / `#` 末层独占 | ✅ | ✅ | `topic.IsValidFilter` 校验，Trie `RWMutex` |
| `$SYS` 隔离 | ✅ | ✅ | `#/+` 不命中 `$SYS/` |
| `NoLocal` | ⛔ | ✅ | `filter` 标记，`deliverLocal` 跳过 `from==client` |
| `RetainAsPublished (RAP)` | ⛔ | ✅ | 解析并存储（路由层 `retain=false` 不重发标志） |
| `RetainHandling (RH 0/1/2)` | ⛔ | ✅ | 解析，`RH=0` 立即回 Retain，`1/2` 按需（当前实现与 `RH=0` 等价，见备注） |
| `SubscriptionID` | ⛔ | ✅ | `Properties.SubscriptionID`，`PUBLISH` 回带 |
| `UserProperty` | ⛔ | ✅ | 透传 |
| Shared Subscription | ⚠️ | ⚠️ | `$share/<group>/<filter>` 单机轮询（`sharedSubs`+`sharedIdx`），未跨 Redis 一致性哈希（`FEATURES.md §13`） |
| `UNSUBSCRIBE` | ✅ | ✅ | v5 ReasonCode |

> **RH 注**：`v5 RH=1（仅新订阅）/2（不回 Retain）` 在当前 Trie 实现中视为 `0`，不区分“已存在订阅”。属 **⚠️ 部分支持**，对外表现为均回 Retain，兼容 Mosquitto 行为但非严格 spec。

---

## 6. 会话与离线

| 项 | v3 | v5 | 说明 |
|---|:---:|---|---|
| `CleanSession` / `CleanStart + Expiry` | ✅ | ✅ | `expiry 0` 清 `Subscriptions/Inflight/offline`，`0xFFFFFFFF` 持久化 |
| 持久化 | ✅ | ✅ | `Store` 接口 `MemoryStore / RedisStore (mqtt:session:)` |
| 跨节点接管 | ✅ | ✅ | `GetSession` 内存→Redis，`DequeueOffline` 回放 |
| 离线队列 | ✅ | ✅ | `EnqueueOffline/DequeueOffline/ClearOffline cap1000` |
| TopicAlias 状态 | ⛔ | ✅ | 随 `Session` 落库 |
| 事务超时 | ✅ | ✅ | `bgCtx/storeCtx 2s`，`slog.Warn` 非静默 |

---

## 7. Retain / Will

| 项 | v3 | v5 | 说明 |
|---|:---:|---|---|
| Retain 存储 | ✅ | ✅ | `mqtt:retain:<topic>` JSON |
| Retain 删除 | ✅ | ✅ | `retain && payload==""` → `DeleteRetained` |
| Retain 投递 | ✅ | ✅ | `SUBSCRIBE` 命中后同步 `ListRetained` + `Match` 回放 |
| Will | ✅ | ✅ | `Will.Topic/Payload/QoS/Retain` |
| Will `DelayInterval` | ⛔ | ✅ | `AfterFunc(delay)`，`≤86400` |
| Will Properties | ⛔ | ⚠️ | `UserProperty` 编解码，`WillDelayInterval` 单独编码 |
| 配额 | ✅ | ✅ | 见 §11 |

---

## 8. 安全与限制

| 项 | 说明 | v3 | v5 |
|---|:---:|:---:|:---:|
| `AllowAll/DenyAll/SimpleAuth/JWT/Chain` | `auth/` | ✅ | ✅ |
| `FileACL` `read/write/readwrite` + `#/+` | `5s` mtime 热加载，`SIGHUP` | ✅ | ✅ |
| `MaxConnections 20000` | `CONNACK 0x03(v3)/0x97(v5)` | ✅ | ✅ |
| `MaxPublishPerSec 100` | `PUBACK 0x97` | ✅ | ✅ |
| `MaxSubscribePerSec 20` | `SUBACK 0x97` | ✅ | ✅ |
| `MaxPacketSize 1MB` | 超限 `Close` | ✅ | ✅ |
| Retain 配额 | §11 | ✅ | ✅ |

---

## 9. 其他 v5 Properties 全量

Broker 对 27 种 `Properties` 均可 **编解码**，业务层使用如下：

| Property | 业务使用 |
|---|---|
| `SessionExpiryInterval` | ✅ 会话 |
| `ReceiveMaximum` | ✅ 背压 |
| `MaximumPacketSize` | ✅ 限包 |
| `TopicAliasMaximum/TopicAlias` | ✅ 别名映射 |
| `UserProperty` | ✅ 透传（`PUBLISH/SUBSCRIBE/CONNECT`） |
| `PayloadFormatIndicator/MessageExpiry/ContentType/ResponseTopic/CorrelationData` | ✅ 透传 |
| `SubscriptionID` | ✅ 订阅→发布回显 |
| `AssignedClientID/ServerKeepAlive/ReasonString/ServerReference` | ✅ `CONNACK/DISCONNECT` 编解码 |
| `WildcardSubAvailable/SubIDAvailable/SharedSubAvailable/RetainAvailable/MaximumQoS` | ✅ `CONNACK` 宣告 |
| `AuthMethod/AuthData/RequestProblemInfo/RequestResponseInfo/WillDelayInterval` | ⚠️ 编解码，流程未用 |

---

## 10. 集群

| 项 | 支持 |
|---|---|
| 节点发现 `SET mqtt:nodes:<id> EX 15s` + `SCAN` | ✅ |
| 广播 `PUBLISH mqtt:cluster` JSON `ClusterMessage` | ✅ |
| 远端 Trie `remoteTries` + `hasRemoteSubscribers` | ✅ |
| `$SYS` 统计 `broker/uptime` 10s | ✅ |

---

## 11. Retain 配额（新增 #2）

| 维度 | 默认 | 说明 |
|---|:---:|---|
| `MaxRetainedMessages` | 10000 | 全局条数，覆盖不计新条 |
| `MaxRetainedSize` | 1GB | 全局 `Σ(len(topic)+len(payload)+10)`，覆盖用 `total-old+new` |
| `MaxRetainPerTopic` | 1000 | 单 topic 条数（精确 topic 单条存储，恒 ≤1，预留） |
| `MaxRetainSizePerTopic` | 100MB | 单条 `len(topic)+len(payload)+10` |

超限：`slog.Warn … reason=global_count/global_size/per_topic_count/per_topic_size` + `mqtt_retain_quota_exceeded_total{reason}` + `mqtt_packet_dropped_total{retain_quota}`，`PUBACK 0x97`（v5）/ 静默丢弃（v3 QoS0），删除（`retain && payload==""`）不受配额。

---

## 12. 显式未支持 / 部分支持汇总

| 项 | 状态 | 说明 |
|---|:---:|---|
| `AUTH (0x0F)` 增强认证 | ❌ | 编解码预留，未接入往返 |
| `RH=1/2` 严格语义 | ⚠️ | 当前均视为 `RH=0` 回 Retain |
| Shared Sub 跨节点哈希 | ⚠️ | 单机轮询，未跨 Redis 选主 |
| `MessageExpiry` 自动清理 | ⚠️ | 仅透传 Properties，未定时过期 Retain |
| WAL | ✅ 默认开启 | `Store` 接口可插拔：`PebbleStore ./data/wal` 默认 + `FallbackStore(Redis→Pebble)`；`--wal-dir="-"` 禁用，`WithWALStore(s Store)` 可换 Badger/任意实现 |
| 多租户/计费/控制台 | ❌ | 本轮 Out of scope |

---

## 13. 验证

| 覆盖 | 说明 |
|---|---|
| `mosquitto_pub/sub` | `v3.1.1` QoS0/1/2 互通 |
| `mqtt.js` | `v5` `CONNECT/PUBLISH/SUBSCRIBE/UserProperty/TopicAlias` |
| `Eclipse Paho` 8 用例 | `docs/compliance.md` 100% 綠 |
| `go vet / golangci / govulncheck / fuzz 10w` | 全绿 |
| `bench` | `BenchmarkPublishThroughput ~26µs` / `10kClients ~660ms` |

> 更新配额后 `docs/compliance.md` 与 `FEATURES.md §13` 的“Retain 无配额”已修正为 §11。
