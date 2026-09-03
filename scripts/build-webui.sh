#!/usr/bin/env sh
# 构建 web/dashboard 前端并将其产物嵌入到 internal/webui/dist，
# 供 go:embed 打包进 broker 二进制。
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DASH="$ROOT/web/dashboard"
OUT="$ROOT/internal/webui/dist"

cd "$DASH"
if [ ! -d node_modules ]; then
  echo "[webui] installing dependencies (npm ci)..."
  npm ci --no-audit --no-fund
fi
echo "[webui] building dashboard..."
npm run build

echo "[webui] embedding dist into internal/webui/dist..."
rm -rf "$OUT"
mkdir -p "$OUT"
cp -r dist/. "$OUT/"
echo "[webui] done -> $OUT"
