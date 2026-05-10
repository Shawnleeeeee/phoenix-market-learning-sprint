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

OUT="${BTC_FLOW_FEATURE_SNAPSHOT_OUT:-$ROOT/btc_data/research/runtime/flow_feature_snapshot.json}"
SRC="${BTC_FLOW_FEATURE_SNAPSHOT_SRC:-$ROOT/btc_data/research/runtime/market_micro_snapshots.jsonl}"

mkdir -p "$(dirname "$OUT")"

"$ROOT/.venv/bin/python" - <<'PY' "$SRC" "$OUT"
from __future__ import annotations

import json
import sys
from pathlib import Path

from btc_engine.research.flow_features import (
    compute_changepoint_features,
    compute_metaorder_features,
    compute_orderbook_imbalance_features,
    compute_tca_features,
    load_snapshot_jsonl,
)

src = Path(sys.argv[1])
out = Path(sys.argv[2])
snapshots = load_snapshot_jsonl(src)
payload = {
    "exists": src.exists(),
    "snapshot_count": len(snapshots),
    "latest": {},
}
if snapshots:
    latest = snapshots[-1]
    payload["latest"] = {
        **compute_orderbook_imbalance_features(latest),
        **compute_metaorder_features(latest),
        **compute_tca_features(latest),
        **compute_changepoint_features(latest),
    }
out.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
print(out)
PY
