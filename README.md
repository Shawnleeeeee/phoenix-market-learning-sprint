# Project Phoenix

Project Phoenix is being built in three stages:

1. `Scout`: read-only overlap discovery for Binance Alpha × USD-M perpetuals
2. `Judge`: score the overlap universe using social hype, smart money, topic rush, and token audit
3. `Executor`: build deterministic futures order plans and validate entry orders against Binance test-order endpoints while previewing protection algo payloads

Current status:

- `Scout` is live and runnable
- `Judge` is live and runnable
- `Executor` can already preview deterministic order plans without touching funds
- signed validation is implemented and now auto-detects classic USD-M vs unified-account Portfolio Margin APIs

## Why raw Binance endpoints

The current Python package available in this environment was not enough for a clean USD-M futures integration, so Phoenix talks directly to Binance public and signed endpoints.

Public market data:

- Alpha universe: `web3.binance.com`
- Futures exchange metadata: `fapi.binance.com/fapi/v1/exchangeInfo`
- Futures open interest: `fapi.binance.com/fapi/v1/openInterest`
- Mark price stream: `wss://fstream.binance.com/ws/!markPrice@arr@1s`

Signed futures flow:

- classic USD-M: margin type, leverage, `/fapi/v1/order/test` for entries, `/fapi/v1/algoOrder` for protection orders, `listenKey`
- unified account / Portfolio Margin UM: `papi` account detail, leverage, UM order, UM conditional order
- Phoenix auto-detects the signed API family when `PHOENIX_BINANCE_ACCOUNT_API=auto`

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

If you want to prepare for signed endpoints later, copy `.env.example` and fill in the sub-account values.

If your server needs a fixed egress proxy, Phoenix also supports:

- `PHOENIX_ALL_PROXY`
- `PHOENIX_HTTP_PROXY`
- `PHOENIX_HTTPS_PROXY`
- `PHOENIX_WS_PROXY`
- `PHOENIX_WSS_PROXY`
- `PHOENIX_NO_PROXY`

For a single HTTP CONNECT proxy used across Binance HTTPS and WSS traffic, setting `PHOENIX_ALL_PROXY=http://host:port` is usually enough.

Execution sizing can run in two modes:

- `PHOENIX_ALLOCATION_MODE=FIXED`: every plan uses the configured `PHOENIX_QUOTE_ALLOCATION_USDT`
- `PHOENIX_ALLOCATION_MODE=RISK_BUDGET`: Phoenix treats `PHOENIX_QUOTE_ALLOCATION_USDT` as a cap and sizes the actual quote allocation from `PHOENIX_RISK_BUDGET_PCT_OF_BALANCE`

This is useful when the account only holds `200 USDT` total and you do not want every trade to consume the full balance.

Execution control can also run in three modes:

- `PHOENIX_EXECUTION_MODE=DRY_RUN_ONLY`
- `PHOENIX_EXECUTION_MODE=MANUAL_CONFIRM`
- `PHOENIX_EXECUTION_MODE=AUTO_CONFIRM_WHEN_RULES_PASS`

The safe default is `MANUAL_CONFIRM`.

For a formal Phoenix trial run, the recommended profile is:

- `PHOENIX_ALLOCATION_MODE=RISK_BUDGET`
- `PHOENIX_RISK_BUDGET_PCT_OF_BALANCE=3.0`
- `PHOENIX_EXECUTION_MODE=MANUAL_CONFIRM`
- `PHOENIX_MAX_OPEN_POSITIONS=1`
- `PHOENIX_STRATEGY_MIN_SCORE=6.0`
- `PHOENIX_REJECT_BLOCKED_CANDIDATES=true`
- `PHOENIX_COOLDOWN_MINUTES_AFTER_CLOSE=30`
- `PHOENIX_SIGNIFICANT_SCORE_DELTA=2.0`
- `PHOENIX_SIGNIFICANT_SCORE_DELTA_PCT=0.2`

That trial profile means:

- one open position at a time across the account
- candidate-ranked entries must clear a minimum score
- candidates with `blocked_reasons` are rejected
- after a position closes, Phoenix waits 30 minutes before allowing another entry
- live entries remain token-gated even after a clean preflight

For a more autonomous server-side loop, use the new `autocycle` action:

```bash
python3 skills/phoenix-operator/phoenix_openclaw.py autocycle \
  --scan-duration-sec 20 \
  --top-n 10 \
  --audit-top-n 8 \
  --quote-allocation 200 \
  --leverage 5 \
  --env prod
```

`autocycle` runs a bounded `cycle`, compares the new top candidate to the previous run, and only calls `dispatch` when the candidate has changed significantly.

## OpenClaw Integration

Phoenix now includes a workspace skill for OpenClaw at `skills/phoenix-operator/SKILL.md`.
The OpenClaw-facing entrypoint is `skills/phoenix-operator/phoenix_openclaw.py`, which exposes bounded market reads, candidate ranking, signed account probing, deterministic previews, token-gated `arm`, and `confirm` for guarded live entry.
It does not allow direct untokened live order placement.

Common commands:

```bash
python3 skills/phoenix-operator/phoenix_openclaw.py cycle \
  --scan-duration-sec 20 \
  --top-n 10 \
  --audit-top-n 8

python3 skills/phoenix-operator/phoenix_openclaw.py probe \
  --symbol BTCUSDT \
  --env prod

python3 skills/phoenix-operator/phoenix_openclaw.py preflight \
  --symbol BTCUSDT \
  --quote-allocation 200 \
  --leverage 5 \
  --env prod
```

Phoenix already loads the workspace `.env` automatically.
If your OpenClaw runtime uses a separate secret file such as `.openclaw/secrets.env`, mirror the same `PHOENIX_BINANCE_*` variables there.

If your laptop IP is unstable, move signed Phoenix traffic to a fixed-IP VPS and let local OpenClaw call that server over SSH.
Deployment artifacts for this pattern live in `deploy/README.md`, `deploy/systemd/`, and `deploy/ssh/phoenix_remote.sh`.

## Hermes Integration

Phoenix can also run behind Hermes on the same server.

Use the Hermes-specific artifacts in `deploy/hermes/`:

- `deploy/hermes/README.md`
- `deploy/hermes/config.yaml.example`
- `deploy/hermes/.env.example`
- `deploy/hermes/phoenix_hermes_runner.sh`

This pattern keeps:

- Hermes as the Telegram-facing orchestrator
- Phoenix as the Binance-only execution core
- `/etc/phoenix/phoenix.env` as the single source of trading secrets

The current recommended Hermes flow is:

1. `cycle` for bounded market scan + ranking
2. `probe` for signed account health
3. `preflight` for authenticated dry-run validation
4. live execution only after a dedicated live-order wrapper exists

## 1. Run Scout

```bash
.venv/bin/python phoenix_data_scout.py
```

Useful flags:

```bash
.venv/bin/python phoenix_data_scout.py \
  --oi-refresh-sec 15 \
  --alpha-refresh-sec 60 \
  --oi-jump-pct 1.0 \
  --volume-spike-ratio 3.0 \
  --top-n 20
```

Scout output:

- JSON lines to stdout
- rolling snapshot file at `phoenix_snapshot.json`

Core event types:

- `scout_boot`
- `universe_refreshed`
- `mark_price_stream_connected`
- `oi_jump`
- `volume_spike`
- `oi_cycle_summary`

## 2. Run Judge

```bash
.venv/bin/python phoenix_judge.py \
  --snapshot-file phoenix_snapshot.json \
  --top-n 20 \
  --audit-top-n 8
```

Judge output:

- writes `phoenix_candidates.json`
- prints ranked candidate JSON to stdout

The current scoring model combines:

- OI acceleration
- Binance social hype leaderboard
- smart money signals
- topic rush net inflow
- token audit penalties
- fake-heat keyword blocking for giveaway / airdrop style campaigns

## 3. Preview Executor Plan

Preview a deterministic execution plan without credentials:

```bash
.venv/bin/python phoenix_executor.py \
  --candidate-file phoenix_candidates.json \
  --candidate-rank 1 \
  --quote-allocation 200 \
  --leverage 5
```

This prints:

- entry quantity
- initial stop price
- breakeven trigger
- breakeven stop replacement
- post-trigger trailing stop payload

## 4. Validate Test Orders Later

Once the Phoenix sub-account API key and secret are ready:

```bash
.venv/bin/python phoenix_executor.py \
  --symbol BTCUSDT \
  --quote-allocation 200 \
  --leverage 5 \
  --test-order
```

For unified-account / Portfolio Margin sub-accounts, Binance does not expose a direct `order/test` endpoint via `papi`.
Phoenix will still do an authenticated preflight and print the exact UM order / conditional-order payloads it would use.

If you also want to validate leverage / margin setup on that sub-account:

```bash
.venv/bin/python phoenix_executor.py \
  --symbol BTCUSDT \
  --quote-allocation 200 \
  --leverage 5 \
  --test-order \
  --apply-account-setup
```

## Notes

- The overlap universe is currently around 130 symbols, based on live public data at runtime.
- Open interest is polled because Binance does not provide a simple all-symbol OI public stream.

## 5. Run Signal Lab

If you want to optimize setups without paying for every sample in test trades, use the new signal lab workflow.

`phoenix_signal_lab.py` reuses the same Phoenix candidate scoring logic, but:

- does not place trades
- records the top-ranked candidates every cycle
- labels each observation with forward returns, MFE, and MAE across multiple horizons
- optionally merges richer market microstructure fields from a live scout snapshot

Typical observe run:

```bash
.venv/bin/python phoenix_signal_lab.py observe \
  --env prod \
  --duration-sec 1800 \
  --cycle-sec 20 \
  --top-n 8 \
  --horizons-sec 20,45,90,180 \
  --scan-top 140 \
  --universe-top 320 \
  --starting-min-score 40 \
  --execution-floor-offset 0
```

This creates a timestamped directory under `signal_lab_runs/` with:

- `candidate_observations.jsonl`
- `candidate_labels.jsonl`

If a scout snapshot is already running, signal lab auto-detects `phoenix_snapshot.openclaw.json` first, then `phoenix_snapshot.json`, and merges fields such as:

- funding rate
- taker flow ratios
- liquidation pressure
- spread
- depth imbalance
- estimated slippage

After collecting a run, analyze the labeled observations:

```bash
.venv/bin/python phoenix_signal_lab.py analyze \
  --labels-file signal_lab_runs/<run-id>/candidate_labels.jsonl \
  --round-trip-fee-bps 8
```

`--round-trip-fee-bps` is intentionally explicit so you can plug in the fee assumption that matches the account tier and maker/taker mix you actually care about.
- The current `Judge` is deterministic and rule-based; no LLM is placed in the execution loop.
- The current `Executor` is intentionally conservative: preview first, signed/auth preflight second, live execution later.

## 6. Summarize Testnet Execution

After a `phoenix_testnet_round_runner.py` run, build an execution report from its round output directory:

```bash
.venv/bin/python phoenix_testnet_execution_report.py --input-dir round_runner_reports_<run-id>
```

This writes `testnet_execution_report.json` and `testnet_execution_report.md` into the same directory. The report summarizes PnL, fills/rejects, latency, slippage estimates, drawdown, symbol/setup/exit breakdowns, and a placeholder promotion cross-validation section for a future shadow report comparison.
