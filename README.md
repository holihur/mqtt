# MQTT Broker — Go + Redis Cluster
[![CI](https://github.com/holihur/mqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/holihur/mqtt/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/holihur/mqtt/branch/main/graph/badge.svg)](https://codecov.io/gh/holihur/mqtt)

支持 **MQTT 3.1 (0x03) / 3.1.1 (0x04) / 5.0 (0x05)** 的分布式 Broker，单机可跑，多实例通过 Redis 水平扩展。

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
- -pprof :6060            pprof 监听 (空则禁用)
- -node  <id>            节点 ID
```

## 安全与扩展

- **Auth**：`AllowAll` / `SimpleAuth` / `JWT (HS256)` / `FileACL`（`--jwt-secret/--acl`），支持 `Chain`
- **Fuzz**：`FuzzDecode`/`FuzzSplitFrame` 10w exec/s
- **基准**：`docs/bench.md`（CI 自动更新，见下方），`pprof + /metrics` 已暴露
- **一键开发**：`task dev` 自动拉起 `redis` + `broker`（`:1883/:8083/:6060`）

## 基准与压测（CI 自动更新）
<!-- BENCH_START -->
> 更新: 2026-08-28 04:38 UTC | goos: linux goarch: amd64 | cpu: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
>
> ```
> goos: linux
goarch: amd64
cpu: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz
Benchmark10kClients-4          	       1	    880464 ns/op	   18312 B/op	      92 allocs/op
BenchmarkPublishThroughput-4   	       1	     18592 ns/op	     488 B/op	       6 allocs/op
> ```
>
> - Benchmark10kClients-4 1 880464 ns/op 18312 B/op 92 allocs/op
> - BenchmarkPublishThroughput-4 1 18592 ns/op 488 B/op 6 allocs/op
> - 详见 [`docs/bench.md`](docs/bench.md) 与 Artifacts `bench.txt`/`c10k.txt`

<!-- BENCH_END -->
