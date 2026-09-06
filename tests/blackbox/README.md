# Black-box tests (pytest)

使用 `pytest` + `paho-mqtt` 对运行中的 broker 二进制进行黑盒测试。

## 前置条件

1. 编译 broker 二进制：
   ```bash
   go build -o bin/broker ./cmd/broker
   ```
   或
   ```bash
   task build
   ```

2. 安装 Python 依赖：
   ```bash
   pip3 install -r tests/blackbox/requirements.txt
   ```
   或
   ```bash
   task blackbox-install
   ```

## 运行

```bash
# 全量测试
python3 -m pytest tests/blackbox -v

# 或者通过 Taskfile
task blackbox
```

## 测试结构

| 文件 | 覆盖 |
|---|---|
| `conftest.py` | broker 启动/停止 fixture、配置参数化、paho client 辅助函数 |
| `test_connect.py` | 连接/断开、匿名/拒绝、MQTT 3.1/3.1.1/5.0、用户名密码 |
| `test_pubsub.py` | 发布/订阅、QoS 0/1/2、通配符 `+`/`#`、多订阅者、大 payload |
| `test_retain.py` | 保留消息、释放保留、QoS 变体 |
| `test_will.py` | 遗嘱消息（异常断开/优雅断开/QoS/retain） |
| `test_websocket.py` | WebSocket 传输、WS↔TCP 跨传输投递 |
| `test_config_variations.py` | 针对多配置（anon / wal）参数化运行核心场景 |

## 配置参数化

`BROKER_CONFIGS` 列表在 `conftest.py` 中定义。添加新配置只需在其中追加一个
`BrokerConfig` 实例，所有 `broker_param` fixture 的测试会自动覆盖新配置。

## 注意

- 每个测试场景使用独立 broker 进程（`scope="class"`），避免状态污染。
- 测试使用随机空闲端口，避免冲突。
- 被跳过的 `test_keepalive_ping` 因 keepalive 时序抖动在 CI 中不稳定，可按需启用。
