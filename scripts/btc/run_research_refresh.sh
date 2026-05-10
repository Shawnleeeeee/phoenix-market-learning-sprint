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

export BTC_RESEARCH_PROFILE="${BTC_RESEARCH_PROFILE:-long_only}"
export BTC_RESEARCH_ALLOWED_REGIMES="${BTC_RESEARCH_ALLOWED_REGIMES:-trend,compression}"
export BTC_RESEARCH_DAYS="${BTC_RESEARCH_DAYS:-30}"

"$ROOT/scripts/btc/run_backfill.sh" --symbol BTCUSDT --interval 5m --days "$BTC_RESEARCH_DAYS"
"$ROOT/scripts/btc/run_dataset.sh"
"$ROOT/scripts/btc/run_backtest.sh"
"$ROOT/scripts/btc/run_train.sh"
"$ROOT/scripts/btc/run_walk_forward.sh"
"$ROOT/scripts/btc/run_sensitivity.sh"
"$ROOT/scripts/btc/run_research_review.sh"
