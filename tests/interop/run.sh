#!/usr/bin/env bash
# Eclipse Paho MQTT 互操作测试 (MQTT 3.1.1, client_test.py 全部用例)
# 用法: tests/interop/run.sh [broker 二进制路径] [端口]
# 前置: python3; broker 二进制已构建 (默认 bin/broker)
set -euo pipefail

BROKER_BIN="${1:-bin/broker}"
PORT="${2:-18890}"
HERE="$(cd "$(dirname "$0")" && pwd)"
PAHO_DIR="$HERE/paho.mqtt.testing"

if [ ! -d "$PAHO_DIR" ]; then
  git clone --depth 1 https://github.com/eclipse/paho.mqtt.testing.git "$PAHO_DIR"
fi

# 全新状态启动 broker, 避免历史会话/离线队列污染测试
"$BROKER_BIN" -tcp ":$PORT" -ws "" -redis "" -allow-anonymous true \
  -acl "$HERE/paho.acl" &
BROKER_PID=$!
trap 'kill $BROKER_PID 2>/dev/null || true; wait $BROKER_PID 2>/dev/null || true' EXIT
for _ in $(seq 1 20); do
  (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && { exec 3>&-; break; }
  sleep 0.5
done

python3 "$HERE/run_paho.py" "$PAHO_DIR/interoperability" "$PORT" 2>&1 | tee /tmp/paho_interop.log | grep -E "^(OK|FAILED|Ran)" || true

if grep -qE "^OK" /tmp/paho_interop.log; then
  echo "paho interop: PASS"
else
  echo "paho interop: FAIL" >&2
  exit 1
fi
