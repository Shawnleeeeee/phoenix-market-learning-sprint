from __future__ import annotations

import csv
from pathlib import Path


def build_freqai_shadow_payload(dataset_path: str | Path, output_path: str | Path) -> Path:
    """Export a slim CSV for external shadow-model tooling."""

    dataset_path = Path(dataset_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with dataset_path.open(newline="") as src, output_path.open("w", newline="") as dst:
        reader = csv.DictReader(src)
        fieldnames = [
            "open_time",
            "close",
            "regime",
            "future_entry_edge_long_horizon_pct",
            "baseline_long_score",
            "funding_rate_bps",
            "oi_change_1_pct",
            "taker_flow_imbalance",
            "realized_vol_12",
        ]
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            writer.writerow({name: row.get(name, "") for name in fieldnames})

    return output_path
