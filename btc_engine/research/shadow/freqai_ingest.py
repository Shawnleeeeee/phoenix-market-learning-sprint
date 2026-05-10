from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_shadow_predictions(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"exists": False, "predictions": []}
    if p.suffix == ".json":
        return json.loads(p.read_text())
    rows = [line.strip() for line in p.read_text().splitlines() if line.strip()]
    return {"exists": True, "predictions": rows}
