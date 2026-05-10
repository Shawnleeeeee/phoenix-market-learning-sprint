#!/usr/bin/env bash
set -euo pipefail

# Lightweight VPS-only forward shadow validation.
# This intentionally does not run historical replay/backtest jobs.
# Mainnet execution remains locked via MAINNET_SHADOW and no playbooks are
# allowed through the normal live-order path.

ROOT_DIR="${PHOENIX_ROOT_DIR:-$HOME/phoenix_work}"
cd "$ROOT_DIR"

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
elif [[ -x ".venv-win/Scripts/python.exe" ]]; then
  PYTHON_BIN=".venv-win/Scripts/python.exe"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

RUN_STAMP="$(date -u +%Y%m%d_%H%M%S)"
OUTPUT_ROOT="${PHOENIX_FORWARD_SHADOW_ROOT:-$ROOT_DIR/signal_lab_replay}"
OUTPUT_DIR="${PHOENIX_FORWARD_SHADOW_DIR:-$OUTPUT_ROOT/vps_forward_shadow_candidate_validation_$RUN_STAMP}"
BASELINE_REPORT="${PHOENIX_BASELINE_REPORT:-$ROOT_DIR/signal_lab_replay/local_merged_public_zip_20220101_20260427_top200_plus_retries_20260429/backtest_report.json}"

mkdir -p "$OUTPUT_DIR/mainnet_shadow"

if [[ ! -f "$BASELINE_REPORT" ]]; then
  echo "warning: baseline report not found: $BASELINE_REPORT" >&2
  echo "promotion gate will still stay locked, but baseline metrics may be unavailable." >&2
fi

COLLECT_STDOUT="$OUTPUT_DIR/collect_stdout.log"
COLLECT_STDERR="$OUTPUT_DIR/collect_stderr.log"
BRIDGE_STDOUT="$OUTPUT_DIR/bridge_stdout.log"
BRIDGE_STDERR="$OUTPUT_DIR/bridge_stderr.log"

nohup "$PYTHON_BIN" phoenix_signal_lab.py collect \
  --env prod \
  --output-dir "$OUTPUT_DIR" \
  --duration-sec "${PHOENIX_FORWARD_SHADOW_DURATION_SEC:-0}" \
  --cycle-sec "${PHOENIX_FORWARD_SHADOW_CYCLE_SEC:-45}" \
  --horizons-sec "60,180,300,900,1800,3600" \
  --scan-top "${PHOENIX_FORWARD_SHADOW_SCAN_TOP:-120}" \
  --universe-top "${PHOENIX_FORWARD_SHADOW_UNIVERSE_TOP:-200}" \
  --max-events-per-cycle "${PHOENIX_FORWARD_SHADOW_MAX_EVENTS_PER_CYCLE:-50}" \
  --worker-count "${PHOENIX_FORWARD_SHADOW_WORKER_COUNT:-3}" \
  --kline-concurrency "${PHOENIX_FORWARD_SHADOW_KLINE_CONCURRENCY:-5}" \
  --min-quote-volume "${PHOENIX_FORWARD_SHADOW_MIN_QUOTE_VOLUME:-5000000}" \
  --trigger-min-quote-volume "${PHOENIX_FORWARD_SHADOW_MIN_QUOTE_VOLUME:-5000000}" \
  --round-trip-fee-bps "${PHOENIX_FORWARD_SHADOW_FEE_BPS:-8}" \
  >"$COLLECT_STDOUT" 2>"$COLLECT_STDERR" &
COLLECT_PID=$!

nohup "$PYTHON_BIN" phoenix_signal_bridge.py \
  --snapshots-file "$OUTPUT_DIR/bridge_event_feed.jsonl" \
  --env prod \
  --execution-mode MAINNET_SHADOW \
  --mainnet-shadow-dir "$OUTPUT_DIR/mainnet_shadow" \
  --allowed-playbooks "__candidate_only__" \
  --disable-user-data-oms \
  --max-signal-age-sec "${PHOENIX_FORWARD_SHADOW_MAX_SIGNAL_AGE_SEC:-180}" \
  --poll-interval-sec 1 \
  --shadow-round-trip-fee-bps "${PHOENIX_FORWARD_SHADOW_FEE_BPS:-8}" \
  --baseline-equity-usdt "${PHOENIX_FORWARD_SHADOW_BASELINE_EQUITY_USDT:-5000}" \
  --backtest-report-file "$BASELINE_REPORT" \
  --live-report-interval-sec "${PHOENIX_FORWARD_SHADOW_REPORT_INTERVAL_SEC:-300}" \
  --memory-cleanup-interval-sec 900 \
  >"$BRIDGE_STDOUT" 2>"$BRIDGE_STDERR" &
BRIDGE_PID=$!

cat >"$OUTPUT_DIR/run_manifest.json" <<EOF
{
  "mode": "vps_forward_shadow_candidate_validation",
  "research_only": true,
  "live_trading_enabled": false,
  "normal_live_playbooks_allowed": false,
  "execution_mode": "MAINNET_SHADOW",
  "output_dir": "$OUTPUT_DIR",
  "baseline_report": "$BASELINE_REPORT",
  "collector_pid": $COLLECT_PID,
  "bridge_pid": $BRIDGE_PID,
  "logs": {
    "collect_stdout": "$COLLECT_STDOUT",
    "collect_stderr": "$COLLECT_STDERR",
    "bridge_stdout": "$BRIDGE_STDOUT",
    "bridge_stderr": "$BRIDGE_STDERR"
  }
}
EOF

echo "Started VPS forward shadow candidate validation"
echo "output_dir=$OUTPUT_DIR"
echo "collector_pid=$COLLECT_PID"
echo "bridge_pid=$BRIDGE_PID"
echo "manifest=$OUTPUT_DIR/run_manifest.json"
