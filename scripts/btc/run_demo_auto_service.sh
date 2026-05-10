#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ -f "$ROOT/btc_config/live.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/btc_config/live.env"
  set +a
fi

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="python3"
fi

exec "$PYTHON" -m btc_engine.runtime.demo_auto_service "$@"
