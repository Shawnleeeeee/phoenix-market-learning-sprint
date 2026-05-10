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

DATASET="${ROOT}/btc_data/research/datasets/BTCUSDT_5m/dataset.csv"
MODEL_DIR="${ROOT}/btc_data/research/models/BTCUSDT_5m"
BACKTEST_DIR="${ROOT}/btc_data/research/backtests"
PROFILE_DIR="${ROOT}/btc_data/research/profile_runs"
mkdir -p "$PROFILE_DIR"

REGIMES=("$@")
if [[ ${#REGIMES[@]} -eq 0 ]]; then
  REGIMES=("trend" "compression")
fi

for REGIME in "${REGIMES[@]}"; do
  RUN_NAME="${REGIME}_only"
  echo "[regime-batch] running ${RUN_NAME}"
  BTC_RESEARCH_PROFILE=long_only \
  BTC_RESEARCH_DAYS="${BTC_RESEARCH_DAYS:-30}" \
    "$ROOT/scripts/btc/run_backtest.sh" --allowed-regimes "$REGIME" --no-short >/tmp/leiting_backtest_${RUN_NAME}.json

  BTC_RESEARCH_PROFILE=long_only \
  BTC_RESEARCH_DAYS="${BTC_RESEARCH_DAYS:-30}" \
    "$ROOT/scripts/btc/run_train.sh" --research-profile long_only --target-column future_entry_edge_long_horizon_pct --no-short --allowed-regimes "$REGIME" >/tmp/leiting_train_${RUN_NAME}.json

  BTC_RESEARCH_PROFILE=long_only \
  BTC_RESEARCH_DAYS="${BTC_RESEARCH_DAYS:-30}" \
    "$ROOT/scripts/btc/run_walk_forward.sh" --research-profile long_only --target-column future_entry_edge_long_horizon_pct --no-short --allowed-regimes "$REGIME" >/tmp/leiting_walk_forward_${RUN_NAME}.json

  BTC_RESEARCH_PROFILE=long_only \
  BTC_RESEARCH_DAYS="${BTC_RESEARCH_DAYS:-30}" \
    "$ROOT/scripts/btc/run_sensitivity.sh" --research-profile long_only --no-short --allowed-regimes "$REGIME" >/tmp/leiting_sensitivity_${RUN_NAME}.json

  DEST="$PROFILE_DIR/$RUN_NAME"
  mkdir -p "$DEST"

  cp "$BACKTEST_DIR/latest_walk_forward.json" "$DEST/latest_walk_forward.json"
  cp "$BACKTEST_DIR/latest_walk_forward.md" "$DEST/latest_walk_forward.md"
  cp "$BACKTEST_DIR/latest_sensitivity.json" "$DEST/latest_sensitivity.json"
  cp "$BACKTEST_DIR/latest_sensitivity.md" "$DEST/latest_sensitivity.md"
  cp "$MODEL_DIR/latest_model.json" "$DEST/latest_model.json"
  echo "[regime-batch] saved ${RUN_NAME} -> $DEST"
done

echo "[regime-batch] complete"
