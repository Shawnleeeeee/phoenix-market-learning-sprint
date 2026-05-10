#!/usr/bin/env bash
set -euo pipefail

SERVER_IP="43.98.174.32"
SSH_KEY="/Users/yanshisan/Downloads/SGVPS.pem"
REMOTE_ENV_PATH="/opt/leiting-btc/btc_config/live.env"

if [[ ! -f "$SSH_KEY" ]]; then
  echo "SSH key not found: $SSH_KEY" >&2
  exit 1
fi

chmod 600 "$SSH_KEY"

read -rsp "Binance Testnet API Key: " BTC_API_KEY
echo
read -rsp "Binance Testnet API Secret: " BTC_API_SECRET
echo

BTC_API_KEY_B64="$(printf '%s' "$BTC_API_KEY" | base64)"
BTC_API_SECRET_B64="$(printf '%s' "$BTC_API_SECRET" | base64)"

ssh -i "$SSH_KEY" "root@$SERVER_IP" "BTC_API_KEY_B64='$BTC_API_KEY_B64' BTC_API_SECRET_B64='$BTC_API_SECRET_B64' REMOTE_ENV_PATH='$REMOTE_ENV_PATH' python3 - <<'PY'
import base64
import os
from pathlib import Path

key = base64.b64decode(os.environ['BTC_API_KEY_B64']).decode('utf-8')
secret = base64.b64decode(os.environ['BTC_API_SECRET_B64']).decode('utf-8')
path = Path(os.environ['REMOTE_ENV_PATH'])
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(
    '\n'.join(
        [
            'BTC_ACCOUNT_API=classic_um',
            'BTC_SYMBOL=BTCUSDT',
            f'BTC_BINANCE_API_KEY={key}',
            f'BTC_BINANCE_API_SECRET={secret}',
            'BTC_BINANCE_ENV=testnet',
            'BTC_FAPI_BASE_URL=https://demo-fapi.binance.com',
            'BTC_WS_BASE_URL=wss://fstream.binancefuture.com',
            '# BTC_ALL_PROXY=',
            'BTC_EXECUTION_MODE=paper',
            'BTC_USER_STREAM_ENABLED=true',
            'BTC_MARKET_STREAM_ENABLED=true',
            'BTC_KLINE_INTERVAL=5m',
            'BTC_BACKFILL_DAYS=90',
            'BTC_REVIEW_BATCH_TRADES=10',
            '',
        ]
    ),
    encoding='utf-8',
)
path.chmod(0o600)
PY"

unset BTC_API_KEY BTC_API_SECRET BTC_API_KEY_B64 BTC_API_SECRET_B64

echo
echo "Wrote $REMOTE_ENV_PATH on $SERVER_IP"
echo

ssh -i "$SSH_KEY" "root@$SERVER_IP" "python3 - <<'PY'
from pathlib import Path
path = Path('$REMOTE_ENV_PATH')
for line in path.read_text(encoding='utf-8').splitlines():
    if '=' not in line:
        continue
    k, v = line.split('=', 1)
    v = v.strip()
    if 'KEY' in k or 'SECRET' in k:
        masked = '<empty>' if not v else f\"{v[:4]}...{v[-4:]}\"
        print(k, len(v), masked)
    else:
        print(k, v)
PY"
