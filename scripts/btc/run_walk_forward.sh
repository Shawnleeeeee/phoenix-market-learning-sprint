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

PROFILE="${BTC_RESEARCH_PROFILE:-long_only}"
ALLOWED_REGIMES="${BTC_RESEARCH_ALLOWED_REGIMES:-trend,compression}"
RESEARCH_DAYS="${BTC_RESEARCH_DAYS:-30}"
EXTRA_ARGS=(--research-profile "$PROFILE")
if [[ "$PROFILE" == "long_only" ]]; then
  EXTRA_ARGS+=(--target-column future_entry_edge_long_horizon_pct --no-short)
fi
if [[ -n "$ALLOWED_REGIMES" ]]; then
  EXTRA_ARGS+=(--allowed-regimes "$ALLOWED_REGIMES")
fi
if (( RESEARCH_DAYS <= 30 )); then
  EXTRA_ARGS+=(--train-bars 2880 --validation-bars 1440 --test-bars 1440 --step-bars 1440)
fi

exec "$PYTHON" -m btc_engine.research.walk_forward "${EXTRA_ARGS[@]}" "$@"
