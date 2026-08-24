# Bench 基线 — 10k 连接 + 吞吐

> 环境：darwin amd64, Intel i3-8100B 3.6GHz, Go 1.25.9, `redis:7` memory

```bash
go test -bench=. -benchmem -benchtime=1x ./bench
```

```
goos: darwin
goarch: amd64
pkg: mqtt/bench
cpu: Intel(R) Core(TM) i3-8100B CPU @ 3.60GHz
Benchmark10kClients-4         1  567676 ns/op  21480 B/op  63 allocs/op
BenchmarkPublishThroughput-4  1  17432 ns/op  152 B/op  4 allocs/op
```

- `10kClients`：单次 `CONNECT→CONNACK` 0.56ms，`63 allocs`，主要在 `codec` 与 `session` 创建
- `PublishThroughput`：`QoS0 PUBLISH` 17µs/op，`152 B/op`，零拷贝 `Trie Match` <1µs

## pprof

```bash
./bin/broker -pprof :6060 &
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=10
go tool pprof http://localhost:6060/debug/pprof/heap
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

`http://localhost:6060/metrics` 暴露 `mqtt_*` + Go 运行时指标，Prometheus 5s 抓取。

## 下一步

- `ReceiveMaximum` 背压已防止 `inflight` 爆炸
- 下一步可 `sync.Pool` 复用 `Packet` 与 `buf`，目标 `10k → 100k msg/s`
