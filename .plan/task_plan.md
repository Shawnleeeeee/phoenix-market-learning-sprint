# Plan: Phoenix Long-Run Safety, Learning, and Research Modules

## Approach
Add modular offline/safety components around the existing Phoenix code instead of expanding the large bridge and runner files. Keep mainnet live disabled by construction, wire testnet-only execution safeguards into `phoenix_testnet_round_runner.py`, and extend diagnostics so they can read both mainnet shadow outcomes and futures testnet trade records.

## Scope
- **In**: runtime mode checks, centralized safe config, execution record normalization, dynamic position-manager simulation, learning-store JSONL/report tools, strategy registry metadata, read-only agent config stub, HMM offline report, testnet execution report, CLI support for mixed shadow/testnet diagnostics, testnet long-run controls.
- **Out**: enabling mainnet live trading, changing `phoenix_live_execute.py`, changing `phoenix_executor.py` order endpoints, real OpenAI calls, HMM trading gates, automatic trading-config mutation.

## File Structure
| File | Action | Responsibility |
|------|--------|----------------|
| `phoenix_runtime_modes.py` | Create | Define `MAINNET_SHADOW`, `TESTNET_LIVE`, `MAINNET_LIVE` and safety validation helpers. |
| `phoenix_config.py` | Create | Central Phoenix config facade with safe defaults for runtime, agent, HMM, learning, and testnet runner. |
| `phoenix_execution_records.py` | Create | Normalize shadow outcomes and testnet trade JSONL rows into one research record shape. |
| `phoenix_position_manager.py` | Create | Dynamic exit simulation and MFE/MAE/giveback metrics. |
| `phoenix_learning_store.py` | Create | Append-only JSONL learning event store and daily report builder. |
| `phoenix_strategy_registry.py` | Create | Strategy definition/status registry with no live auto-promotion. |
| `phoenix_agent_research.py` | Create | Read-only agent entrypoint config, disabled by default and fallback to code reports. |
| `phoenix_hmm_state_report.py` | Create | Offline HMM-like hidden-regime report with graceful dependency-free implementation. |
| `phoenix_testnet_execution_report.py` | Create | Testnet execution report over round trade JSONL files. |
| `phoenix_reports/` | Create | Package wrappers for report modules so future imports do not depend on root script paths. |
| `phoenix_research_diagnostics.py` | Modify | Load mixed mainnet shadow and testnet live records while preserving independent-event dedupe. |
| `phoenix_loss_attribution_report.py` | Modify | Accept `--testnet-dir` / `--testnet-trades-file`; report raw vs independent scopes across data sources. |
| `phoenix_monte_carlo_report.py` | Modify | Accept mixed inputs and maintain independent source-event simulation groups. |
| `phoenix_markov_state_report.py` | Modify | Accept mixed inputs and keep Markov offline reporting. |
| `phoenix_testnet_round_runner.py` | Modify | Add run-forever, mode checks, dynamic throttle, margin mode option, long-run records, learning output. |
| `.env.example` | Modify | Add agent/HMM/learning/testnet safety defaults without secrets. |
| `tests/test_long_run_safety_modules.py` | Create | Unit tests for runtime safety, config, throttling, position manager, learning store, HMM, agent disabled. |
| `tests/test_research_diagnostics_reports.py` | Modify | Cover mixed shadow/testnet report inputs and CLI writes. |

## Phase 1: Safety Modules and Tests
- Add runtime mode validation that rejects `MAINNET_LIVE`, rejects `TESTNET_LIVE` unless env is `testnet`/`demo`, and requires `MAINNET_SHADOW` with env `prod`.
- Add config dataclasses with `max_open_positions=10`, `margin_type=ISOLATED`, agent disabled, HMM report enabled, HMM trading gates disabled.
- Add tests proving safe defaults and rejection rules.
- Verify: `python -m unittest tests.test_long_run_safety_modules -v`.

## Phase 2: Records, Learning, Position Manager
- Add execution record normalization for shadow outcomes and testnet trades.
- Add dynamic exit simulation metrics for breakeven, partial take profit, trailing, momentum decay, no-follow-through, danger-before-stop, and time decay.
- Add append-only learning store and daily report builder.
- Verify: targeted module tests.

## Phase 3: Reports
- Add HMM offline report and testnet execution report.
- Extend loss attribution, Monte Carlo, and Markov CLIs to read both `MAINNET_SHADOW` and `TESTNET_LIVE` rows.
- Add `phoenix_reports` wrappers.
- Verify: report CLI tests with temporary JSONL fixtures.

## Phase 4: Testnet Runner
- Add `--run-forever`, `--rounds 0`, `--runtime-mode`, `--env`, `--max-open-positions`, `--margin-type`, `--learning-store-file`, and dynamic throttle settings.
- Add per-round before cleanup and after-round position checks; keep shutdown flatten.
- Record latency, slippage estimate, partial-fill flag, margin type, runtime mode, and learning record.
- Verify: targeted unit tests plus full unittest.

## Phase 5: VPS Sync and Safe Verification
- Compile locally.
- Sync new/changed Python files to `/opt/phoenix-testnet`.
- Run offline reports on existing samples.
- Check testnet key presence without printing secrets; do not launch long-running trading unless explicitly requested after code is deployed.
