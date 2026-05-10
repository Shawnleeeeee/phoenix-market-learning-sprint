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

DATASET="${BTC_SHADOW_DATASET_PATH:-$ROOT/btc_data/research/datasets/BTCUSDT_5m/dataset.csv}"
OUTDIR="${BTC_SHADOW_OUTPUT_DIR:-$ROOT/btc_data/research/shadow}"
mkdir -p "$OUTDIR"

"$ROOT/.venv/bin/python" - <<'PY' "$DATASET" "$OUTDIR"
from __future__ import annotations

import json
import sys
from pathlib import Path

from btc_engine.research.shadow import (
    build_freqai_shadow_payload,
    compare_shadow_with_leiting,
    evaluate_shadow_promotion_guardrails,
)

dataset = Path(sys.argv[1])
outdir = Path(sys.argv[2])
payload = build_freqai_shadow_payload(dataset, outdir / "freqai_input.csv")

shadow_stub = {
    "mode": "shadow_only",
    "status": "not_executed",
    "net_return_pct": 0.0,
    "trade_count": 0,
    "max_drawdown_pct": 0.0,
}
leiting_stub = {"net_return_pct": 0.0}
summary = {
    "payload_path": str(payload),
    "shadow_summary": shadow_stub,
    "comparison": compare_shadow_with_leiting(shadow_stub, leiting_stub),
    "promotion_guardrails": evaluate_shadow_promotion_guardrails(shadow_stub),
}
(outdir / "latest_shadow_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
(outdir / "latest_shadow_summary.md").write_text(
    "\n".join(
        [
            "# FreqAI Shadow Summary",
            "",
            f"- payload: `{payload}`",
            "- status: `not_executed`",
            "- note: this stage only prepares shadow inputs and promotion guardrails.",
        ]
    )
)
print(outdir / "latest_shadow_summary.json")
PY
