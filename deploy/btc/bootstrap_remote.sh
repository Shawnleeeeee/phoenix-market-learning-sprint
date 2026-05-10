#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <pem_path> <host> [remote_root]" >&2
  exit 1
fi

PEM_PATH="$1"
HOST="$2"
REMOTE_ROOT="${3:-/opt/leiting-btc}"
LOCAL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_TAR="/tmp/leiting-btc-bootstrap.tar.gz"

cd "$LOCAL_ROOT"
export COPYFILE_DISABLE=1
tar -czf "$TMP_TAR" \
  btc_engine \
  btc_config \
  btc_data \
  deploy/btc \
  scripts/btc

scp -i "$PEM_PATH" "$TMP_TAR" "root@${HOST}:/tmp/leiting-btc-bootstrap.tar.gz"

ssh -i "$PEM_PATH" "root@${HOST}" bash <<EOF
set -euo pipefail
mkdir -p "$REMOTE_ROOT"
cd "$REMOTE_ROOT"
tar -xzf /tmp/leiting-btc-bootstrap.tar.gz
if ! python3 -m venv .venv; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y python3-venv
  python3 -m venv .venv
fi
.venv/bin/python -m pip install --upgrade pip
cp -n btc_config/live.env.example btc_config/live.env || true
cp -n btc_config/strategy.yaml.example btc_config/strategy.yaml || true
cp -n btc_config/risk.yaml.example btc_config/risk.yaml || true
chmod +x scripts/btc/run_*.sh
mkdir -p logs
echo "Deployed to $REMOTE_ROOT"
EOF

echo "雷霆已部署到 ${HOST}:${REMOTE_ROOT}"
