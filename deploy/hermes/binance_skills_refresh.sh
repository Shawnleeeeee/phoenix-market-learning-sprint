#!/usr/bin/env bash
set -euo pipefail

HERMES_HOME="${HERMES_HOME:-/root/.hermes}"

python3 /opt/phoenix/deploy/hermes/binance_skills_import.py \
  --hermes-home "$HERMES_HOME" \
  --fetch-assets \
  --fetch-market-rank \
  --fetch-trading-signal \
  --signal-chain-id CT_501 \
  --signal-chain-id 56

# Rebuild downstream Hermes digests so "外部市场参考" in
# 雷霆控制面/研究摘要 tracks the latest upstream refresh.
python3 /opt/phoenix/deploy/hermes/leiting_control_digest.py --hermes-home "$HERMES_HOME"
python3 /opt/phoenix/deploy/hermes/leiting_research_digest.py --hermes-home "$HERMES_HOME"
