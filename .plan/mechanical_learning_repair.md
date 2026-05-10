# Plan: Mechanical Learning Repair

## Scope
- Fix attribution and rolling learning gates for existing Phoenix testnet/shadow data.
- Keep MAINNET_LIVE blocked, Agent disabled, and HMM offline-only.
- Do not add strategies; only produce targeted proposals/experiments and gate bad symbol/setup combinations.

## Files
- `phoenix_trade_attribution.py`: shared loss classification and normalized execution fields.
- `phoenix_learning_gate.py`: rolling symbol/setup memory, quarantine, allow/downweight/block decisions.
- `phoenix_learning_diagnostics.py`: before/after reports for longrun data.
- `phoenix_learning_analyzer.py`: use shared attribution and lower unknown loss reasons.
- `phoenix_strategy_proposals.py`: prevent unknown targets and add targeted proposal rules.
- `phoenix_testnet_round_runner.py`: fee-aware TP, baseline/candidate tagging, learning gate decisions, dynamic exit reasons.
- `tests/test_mechanical_learning_repair.py`: focused unit coverage.
- `tests/test_testnet_round_runner_safety.py`: runner safety regression coverage.

## Verification
- `.venv-win\Scripts\python.exe -m unittest discover -s tests -p "test_*.py"`
- `.venv-win\Scripts\python.exe -m compileall -q phoenix_*.py tests`
- VPS `.venv/bin/python -m unittest discover -s tests`
- VPS `.venv/bin/python -m compileall phoenix_*.py`
