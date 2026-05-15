from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

DEPRECATED_RUNNER_MESSAGE = (
    "deprecated_runner_do_not_use: .tmp_stage2_exploration_v02.py is archived. "
    "Use .tmp_stage2_exploration_v04.py with runner_version=stage2_exploration_v04."
)


def _load_legacy_for_tests() -> None:
    legacy_path = Path(__file__).resolve().parent / "archive" / "legacy_runners" / "tmp_stage2_exploration_v02_legacy.py"
    spec = importlib.util.spec_from_file_location("tmp_stage2_exploration_v02_legacy", legacy_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("legacy_v02_runner_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name, value in vars(module).items():
        if name.startswith("__"):
            continue
        globals()[name] = value


if __name__ == "__main__":
    print(DEPRECATED_RUNNER_MESSAGE, file=sys.stderr)
    raise SystemExit(2)

_load_legacy_for_tests()
