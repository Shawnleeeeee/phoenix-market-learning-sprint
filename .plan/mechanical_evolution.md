# Plan: Phoenix Mechanical Evolution Loop

## Scope
- Add offline mechanical learning analysis, proposals, candidate experiments, A/B evaluation, and promotion gate.
- Add testnet runner read-only/lightweight candidate loading and dynamic exit simulation telemetry.
- Do not connect Agent, do not enable mainnet live, and do not write live/mainnet config.

## Files
- `phoenix_learning_analyzer.py`: load learning/trades/shadow reports and classify recurring failures.
- `phoenix_strategy_proposals.py`: turn findings into rule-based proposals.
- `phoenix_strategy_experiments.py`: create candidate experiment configs and evaluate v1/v2 data.
- `phoenix_promotion_gate.py`: evaluate candidate promotion/paused/rejected states.
- `phoenix_learning_report.py`: CLI orchestration and markdown/json reports.
- `phoenix_strategy_registry.py`: extend registry writes for candidate experiment metadata.
- `phoenix_testnet_round_runner.py`: read active experiment configs and write simulated dynamic exit results.
- `tests/test_mechanical_evolution.py`: unit coverage for analyzer/proposal/experiment/promotion.
- `tests/test_testnet_round_runner_safety.py`: runner integration coverage if needed.

## Phases
1. [x] Offline modules and tests.
2. [x] Runner candidate/position manager integration.
3. [x] Run current longrun data and reports.
4. [x] VPS sync and verify without mainnet live changes.

## Verification
- Local focused tests: `.venv-win\Scripts\python.exe -m unittest tests.test_mechanical_evolution tests.test_testnet_round_runner_safety` -> 14 passed.
- Local full tests: `.venv-win\Scripts\python.exe -m unittest discover -s tests -p "test_*.py"` -> 253 passed.
- Local compile: `.venv-win\Scripts\python.exe -m compileall -q ...` -> passed.
- VPS focused tests: `.venv/bin/python -m unittest tests.test_mechanical_evolution tests.test_testnet_round_runner_safety` -> 14 passed.
- VPS full tests: `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'` -> 237 passed.
- VPS compile: `.venv/bin/python -m compileall -q ...` -> passed.
- VPS learning evolution pipeline generated 184 proposals/experiments from current longrun + mainnet shadow data.
