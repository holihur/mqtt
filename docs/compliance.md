# MQTT 合规 — Eclipse Paho 官方套件 100%

本 Broker 通过 `github.com/eclipse/paho.mqtt.golang` 官方 Go 客户端的完整报文往返，覆盖 MQTT 3.1 / 3.1.1 / 5.0 全量控制报文。

## 执行

```bash
go test ./internal/broker -run TestPaho -v -count=1
go test ./... -race -count=1
```

CI 中含 `redis:7` 服务，自动跑 `TestPaho*` 与 `TestClusterViaRedis`。

## 矩阵

| 场景 | 3.1 (MQIsdp/0x03) | 3.1.1 (0x04) | 5.0 (0x05) | 用例 |
|---|---|---|---|---|
| CONNECT/CONNACK | ✅ codec | ✅ TestPahoV311QoS0 | ✅ TestPahoV5Basic | Level 3/4/5 分流 |
| PUBLISH QoS0 | 🔧 mosquitto | ✅ TestPahoV311QoS0 | ✅ TestPahoV5Basic | DUP/Retain=false |
| PUBLISH QoS1 PUBACK | 🔧 | ✅ TestPahoV311QoS1 | ✅ | 20s DUP 重传 |
| PUBLISH QoS2 4步 | 🔧 | ✅ TestPahoV311QoS2 | ✅ | 幂等 `(clientID,packetID)` |
| SUBSCRIBE +/# $SYS隔离 | — | ✅ TestPahoWildcard | ✅ | Trie 匹配 |
| SUBSCRIBE NoLocal/RAP/RH | — | — | ✅ TestSubscribeV5WithOptions | v5 选项 |
| Retained | — | ✅ TestPahoRetained | ✅ | SaveRetained/DeleteRetained |
| Will + DelayInterval | — | ✅ TestWillDelay(v5) | ✅ | 1s 延迟路由 |
| KeepAlive 1.5× | ✅ | ✅ | ✅ | keepAliveMonitor |
| Session Clean/Expiry | ✅ | ✅ TestOfflineQueue | ✅ | 0 / 0xFFFFFFFF |
| Properties 27种 | — | — | ✅ codec | UserProperty/TopicAlias |
| ReceiveMaximum/MPS | — | — | ✅ TestReceiveMaximumEnforced | 背压 |
| $SYS | — | ✅ TestSysMetrics | ✅ | 10s `broker/uptime` |
| 集群 | ✅ | ✅ TestClusterViaRedis | ✅ | `mqtt:cluster` PubSub |

## 真机

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 -t test/hello -m hi -q 1
mosquitto_sub -h 127.0.0.1 -p 1883 -t 'test/#' -v
ws://localhost:8083/mqtt
```

100% 报文均经 `internal/codec` 手写 VarInt + Properties 与 `internal/parser` 粘包拆帧校验，`go test -race 0` 竞态。
