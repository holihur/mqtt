#!/usr/bin/env sh
# 一键安装脚本：从 GitHub Releases 下载 mqtt broker 二进制并安装到本地。
#
# 用法：
#   curl -fsSL https://raw.githubusercontent.com/holihur/mqtt/main/install.sh | sh
#   或
#   sh install.sh [--version v0.1.0] [--prefix /usr/local/bin] [--service]
#
# 环境变量：REPO / PREFIX / VERSION 可覆盖默认值。
set -eu

REPO="${REPO:-holihur/mqtt}"
BIN_NAME="broker"
PREFIX="${PREFIX:-/usr/local/bin}"
VERSION="${VERSION:-latest}"
INSTALL_SERVICE=0

usage() {
  cat <<EOF
用法: install.sh [选项]

选项:
  --version <tag>   安装指定版本 (默认 latest), 例如 v0.1.0
  --prefix <dir>    安装目录 (默认 /usr/local/bin)
  --service         同时安装 systemd 服务 (仅 Linux, 需要 root)
  -h, --help        显示本帮助

环境变量: REPO / PREFIX / VERSION
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    --prefix)  PREFIX="$2"; shift 2 ;;
    --service) INSTALL_SERVICE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数: $1" >&2; usage >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# 平台检测
# ---------------------------------------------------------------------------
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
  linux|darwin) ;;
  *) echo "不支持的 OS: $OS (仅支持 linux / darwin)" >&2; exit 1 ;;
esac

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64)   ARCH="amd64" ;;
  aarch64|arm64)  ARCH="arm64" ;;
  *) echo "不支持的架构: $ARCH (仅支持 amd64 / arm64)" >&2; exit 1 ;;
esac

# ---------------------------------------------------------------------------
# 版本解析: GitHub tag 形如 vX.Y.Z, 而 goreleaser 归档名不含前导 v
# ---------------------------------------------------------------------------
if [ "$VERSION" = "latest" ]; then
  echo "==> 查询最新版本 ..."
  VERSION="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
    | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p')"
  if [ -z "$VERSION" ]; then
    echo "无法获取最新版本 (仓库 $REPO 可能还没有 release)" >&2
    exit 1
  fi
fi

case "$VERSION" in
  v*) TAG="$VERSION" ;;
  *)  TAG="v$VERSION" ;;
esac
FILE_VER="${TAG#v}"

BASE="https://github.com/$REPO/releases/download/$TAG"
ARCHIVE="${REPO##*/}_${FILE_VER}_${OS}_${ARCH}.tar.gz"
URL="$BASE/$ARCHIVE"

# ---------------------------------------------------------------------------
# 下载并校验
# ---------------------------------------------------------------------------
sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "未找到 sha256sum / shasum 工具" >&2
    return 1
  fi
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "==> 下载 $URL"
curl -fL --progress-bar "$URL" -o "$TMP/$ARCHIVE"
curl -fsSL "$BASE/checksums.txt" -o "$TMP/checksums.txt"

EXPECTED="$(awk -v f="$ARCHIVE" '$2==f {print $1}' "$TMP/checksums.txt")"
ACTUAL="$(sha256_of "$TMP/$ARCHIVE")"
if [ -z "$EXPECTED" ] || [ "$EXPECTED" != "$ACTUAL" ]; then
  echo "校验失败: checksum 不匹配 (expected=$EXPECTED actual=$ACTUAL)" >&2
  exit 1
fi
echo "==> checksum 校验通过"

# ---------------------------------------------------------------------------
# 安装
# ---------------------------------------------------------------------------
tar -xzf "$TMP/$ARCHIVE" -C "$TMP"
if [ ! -f "$TMP/$BIN_NAME" ]; then
  echo "归档中未找到 $BIN_NAME" >&2
  exit 1
fi

if ! mkdir -p "$PREFIX" 2>/dev/null; then
  echo "无法创建安装目录 $PREFIX, 请使用 sudo 或 --prefix 指定可写目录" >&2
  exit 1
fi

install -m 0755 "$TMP/$BIN_NAME" "$PREFIX/$BIN_NAME"
echo "==> 已安装: $PREFIX/$BIN_NAME ($TAG, $OS/$ARCH)"

# ---------------------------------------------------------------------------
# 可选: systemd 服务
# ---------------------------------------------------------------------------
if [ "$INSTALL_SERVICE" = "1" ]; then
  if [ "$OS" != "linux" ]; then
    echo "警告: --service 仅支持 Linux, 已跳过" >&2
  elif [ "$(id -u)" -ne 0 ]; then
    echo "警告: 安装 systemd 服务需要 root, 已跳过" >&2
  else
    UNIT_DIR="/etc/systemd/system"
    ENV_FILE="/etc/mqtt-broker.conf"
    UNIT="$UNIT_DIR/mqtt-broker.service"
    cat > "$ENV_FILE" <<'EOF'
# MQTT broker 启动参数 (systemd 会对 $BROKER_OPTS 做空白拆分)
# 参见: broker -h
BROKER_OPTS="-tcp :1883 -ws :8083 -webui :8080"
EOF
    cat > "$UNIT" <<EOF
[Unit]
Description=MQTT Broker
Documentation=https://github.com/$REPO
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=$ENV_FILE
ExecStart=$PREFIX/$BIN_NAME \$BROKER_OPTS
Restart=on-failure
RestartSec=3
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    echo "==> 已生成 systemd 服务: $UNIT"
    echo "    配置文件: $ENV_FILE (编辑后 systemctl restart mqtt-broker)"
    echo "    启动: systemctl enable --now mqtt-broker"
  fi
fi

echo ""
echo "快速开始:"
echo "  $PREFIX/$BIN_NAME -tcp :1883 -ws :8083 -webui :8080 -admin-api-token 'change-me'"
echo "  # 浏览器访问 http://<host>:8080 , Settings 中填入 admin token"
