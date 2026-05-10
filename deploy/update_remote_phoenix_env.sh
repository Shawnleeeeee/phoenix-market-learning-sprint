#!/usr/bin/env bash
set -euo pipefail

SERVER_HOST="${SERVER_HOST:-}"
SERVER_USER="${SERVER_USER:-}"
SSH_KEY_PATH="${SSH_KEY_PATH:-}"
REMOTE_ENV_PATH="${REMOTE_ENV_PATH:-/etc/phoenix/phoenix.env}"
PROXY_URL="${PROXY_URL:-http://127.0.0.1:7890}"

if [[ -z "$SERVER_HOST" || -z "$SERVER_USER" || -z "$SSH_KEY_PATH" ]]; then
  echo "SERVER_HOST / SERVER_USER / SSH_KEY_PATH must be set explicitly." >&2
  echo "This script no longer keeps default remote server or SSH key values." >&2
  exit 1
fi

read -rsp "Binance API Key: " BINANCE_KEY
echo
read -rsp "Binance API Secret: " BINANCE_SECRET
echo

ssh -i "$SSH_KEY_PATH" "${SERVER_USER}@${SERVER_HOST}" "umask 077; cat > '$REMOTE_ENV_PATH' <<EOF
PHOENIX_BINANCE_API_KEY=$BINANCE_KEY
PHOENIX_BINANCE_API_SECRET=$BINANCE_SECRET
PHOENIX_BINANCE_ENV=prod
PHOENIX_BINANCE_ACCOUNT_API=auto
PHOENIX_ALL_PROXY=$PROXY_URL
EOF"

unset BINANCE_KEY BINANCE_SECRET

echo
echo "Wrote $REMOTE_ENV_PATH on ${SERVER_USER}@${SERVER_HOST}"
echo "Masked verification:"

ssh -i "$SSH_KEY_PATH" "${SERVER_USER}@${SERVER_HOST}" "python3 - <<'PY'
from pathlib import Path
for line in Path('$REMOTE_ENV_PATH').read_text().splitlines():
    if '=' not in line:
        continue
    k, v = line.split('=', 1)
    v = v.strip()
    if 'KEY' in k or 'SECRET' in k:
        print(k, len(v), v[:4], v[-4:])
    else:
        print(k, v)
PY"
