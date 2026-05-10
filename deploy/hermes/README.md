# Hermes × Phoenix

This folder defines a Hermes-first control plane for Project Phoenix that is compatible with the current server reality:

- Hermes runs on the server
- Telegram is the user-facing inbox
- Phoenix remains the only Binance execution core
- Phoenix credentials stay in `/etc/phoenix/phoenix.env`
- Hermes never talks to Binance directly for trading

## Maintenance Index

For long-lived operational context, use these maintenance documents:

- Trading: [MAINTENANCE_TRADING.md](/Users/yanshisan/Desktop/币安交易/deploy/hermes/MAINTENANCE_TRADING.md)
- Hermes: [MAINTENANCE_HERMES.md](/Users/yanshisan/Desktop/币安交易/deploy/hermes/MAINTENANCE_HERMES.md)
- Local network: [MAINTENANCE_LOCAL_NETWORK.md](/Users/yanshisan/Desktop/币安交易/deploy/hermes/MAINTENANCE_LOCAL_NETWORK.md)
- Recent 5-day memory: [RECENT_5DAY_MEMORY_2026-04-20.md](/Users/yanshisan/Desktop/币安交易/deploy/hermes/RECENT_5DAY_MEMORY_2026-04-20.md)
- BTC integration: [LEITING_HERMES_FLOWPYLIB_FREQAI_INTEGRATION.md](/Users/yanshisan/Desktop/币安交易/deploy/btc/LEITING_HERMES_FLOWPYLIB_FREQAI_INTEGRATION.md)
- Binance skills digest: [leiting_skill_digest.py](/Users/yanshisan/Desktop/币安交易/deploy/hermes/leiting_skill_digest.py)
- Leiting control digest: [leiting_control_digest.py](/Users/yanshisan/Desktop/币安交易/deploy/hermes/leiting_control_digest.py)
- Leiting research digest: [leiting_research_digest.py](/Users/yanshisan/Desktop/币安交易/deploy/hermes/leiting_research_digest.py)
- Leiting control surface: [leiting_control.py](/Users/yanshisan/Desktop/币安交易/deploy/hermes/leiting_control.py)
- Binance skills import: [binance_skills_import.py](/Users/yanshisan/Desktop/币安交易/deploy/hermes/binance_skills_import.py)
- Binance assets example: [binance_assets_latest.example.json](/Users/yanshisan/Desktop/币安交易/deploy/hermes/binance_assets_latest.example.json)
- Binance crypto-market-rank example: [binance_crypto_market_rank_latest.example.json](/Users/yanshisan/Desktop/币安交易/deploy/hermes/binance_crypto_market_rank_latest.example.json)
- Binance trading-signal example: [binance_trading_signal_latest.example.json](/Users/yanshisan/Desktop/币安交易/deploy/hermes/binance_trading_signal_latest.example.json)

## Leiting Control

Hermes now reads Leiting control-plane files from:

- `~/.hermes/memories/btc_control/active_trade.json`
- `~/.hermes/memories/btc_control/demo_status.json`
- `~/.hermes/memories/btc_control/market_stream_state.json`
- `~/.hermes/memories/btc_control/user_stream_state.json`
- `~/.hermes/memories/btc_control/strategy_version.json`
- `~/.hermes/memories/btc_control/digest_latest.json`

Use the control surface on the Hermes server:

```bash
python3 /opt/phoenix/deploy/hermes/leiting_control.py --hermes-home /root/.hermes status
python3 /opt/phoenix/deploy/hermes/leiting_control.py --hermes-home /root/.hermes position
python3 /opt/phoenix/deploy/hermes/leiting_control.py --hermes-home /root/.hermes review
python3 /opt/phoenix/deploy/hermes/leiting_control.py --hermes-home /root/.hermes research
python3 /opt/phoenix/deploy/hermes/leiting_control.py --hermes-home /root/.hermes flatten --symbol ETHUSDT
python3 /opt/phoenix/deploy/hermes/leiting_control.py --hermes-home /root/.hermes pause
python3 /opt/phoenix/deploy/hermes/leiting_control.py --hermes-home /root/.hermes resume
python3 /opt/phoenix/deploy/hermes/leiting_control.py --hermes-home /root/.hermes square-preview
python3 /opt/phoenix/deploy/hermes/leiting_control.py --hermes-home /root/.hermes square-preview --symbol BTC
```

Telegram / Hermes gateway now supports the same controls directly:

- `/leiting status`
- `/leiting position`
- `/leiting flatten`
- `/leiting flatten ETHUSDT`
- `/leiting pause`
- `/leiting resume`
- `/leiting review`
- `/leiting research`
- `/leiting square-preview`
- `/leiting square-preview BTC`
- `/leiting square-preview ETH`

Chinese plain-text aliases are also supported in Telegram:

- `雷霆 状态`
- `雷霆 仓位`
- `雷霆 平仓`
- `雷霆 平仓 ETHUSDT`
- `雷霆 暂停`
- `雷霆 恢复`
- `雷霆 复盘`
- `雷霆 研究`
- `雷霆 发广场预览`
- `雷霆 发广场预览 BTC`
- `雷霆 发广场预览 ETH`

More natural Chinese phrases also work:

- `雷霆现在怎么样`
- `雷霆仓位怎么样`
- `雷霆停一下`
- `先暂停雷霆` 
- `把雷霆恢复`
- `雷霆 广场预览`
- `雷霆 发广场预览 BTC`
- `雷霆 发广场预览 ETH`

真实抓取外部市场参考输入文件：

```bash
python3 /opt/phoenix/deploy/hermes/binance_skills_import.py \
  --hermes-home /root/.hermes \
  --fetch-assets \
  --fetch-market-rank \
  --fetch-trading-signal \
  --signal-chain-id CT_501 \
  --signal-chain-id 56
```

如果已经有外部导出的真实 JSON，也可以继续直接导入：

```bash
python3 /opt/phoenix/deploy/hermes/binance_skills_import.py \
  --hermes-home /root/.hermes \
  --trading-signal-json /root/.hermes/memories/binance_skills/trading_signal_latest.json
```

外部市场参考定时刷新：

- 脚本：[binance_skills_refresh.sh](/Users/yanshisan/Desktop/币安交易/deploy/hermes/binance_skills_refresh.sh)
- Service：[hermes-binance-skills-refresh.service](/Users/yanshisan/Desktop/币安交易/deploy/hermes/systemd/hermes-binance-skills-refresh.service)
- Timer：[hermes-binance-skills-refresh.timer](/Users/yanshisan/Desktop/币安交易/deploy/hermes/systemd/hermes-binance-skills-refresh.timer)

Current split:

- Leiting remains the only execution core.
- Hermes reads synchronized state and sends Telegram notifications.
- `flatten/pause/resume` are forwarded over SSH to the Leiting node.

## Role Split

### Phoenix Sentinel

Purpose:
- Watch Binance public market data
- Refresh the Alpha × Futures overlap universe
- Detect short-lived price, volume, and open-interest anomalies

Implementation:
- `sentry.py`
- `phoenix_data_scout.py`
- bounded wrapper action: `scan` or `cycle`

Inputs:
- public Binance Alpha market data
- public USD-M exchange info
- public mark price and open-interest data

Outputs:
- `phoenix_snapshot.openclaw.json`
- short anomaly summaries for Hermes / Telegram

Hermes responsibility:
- schedule Sentinel runs
- summarize only changed or urgent conditions
- avoid alert spam when nothing materially changed

### Phoenix Analyst

Purpose:
- Rank the overlap universe into tradable candidates
- Keep deterministic scoring outside the LLM loop
- Let Hermes explain the ranking in natural language without changing the score

Implementation:
- `phoenix_judge.py`
- bounded wrapper action: `judge` or `cycle`

Inputs:
- Sentinel snapshot file
- deterministic scoring logic in `phoenix/judge.py`

Outputs:
- `phoenix_candidates.openclaw.json`
- ranked shortlist with reasons

Hermes responsibility:
- translate ranked candidates into concise Telegram summaries
- compare the latest shortlist to the previous one
- escalate only when the top candidate changes, score jumps, or urgency increases

### Phoenix Guardian

Purpose:
- Own account-state checks, PM config checks, and execution plans
- Keep hard trading rules outside the chat model
- Refuse any trade that fails preflight

Implementation:
- `phoenix_probe.py`
- `phoenix_executor.py`
- bounded wrapper action: `probe`, `preview`, `preflight`

Current live boundary:
- `probe`: signed account reachability and PM/classic mode detection
- `preview`: deterministic order plan only
- `preflight`: signed dry-run validation only
- `arm`: signed dry-run plus short-lived confirmation token issuance
- `autocycle`: bounded scan + judge + significant-change gate + conditional `dispatch`
- `confirm`: token-gated live entry plus initial protective stop, followed by a detached Guardian post-fill worker

Current guarded live boundary:
- live execution remains routed through Phoenix only
- the wrapper refuses direct live execution without a valid token
- the live path places `entry_market + initial_protective_stop`
- a detached Guardian worker then watches mark price, replaces the initial stop at breakeven, arms the trailing stop, and cancels residual protection after the position closes

Hermes responsibility:
- request Guardian checks before any user-facing trade recommendation
- never fabricate leverage, margin mode, or available balance
- require explicit human confirmation before any future live execution step

## Telegram Flow

The Telegram control flow should be two-stage even before live orders exist.

### Stage 1: Alerting

Hermes sends a Telegram message only when one of these happens:
- a new top candidate appears
- the current top candidate score increases materially
- account probe fails
- preflight fails
- a scheduled cycle completes with a clearly actionable shortlist

Recommended message shape:

```text
Phoenix alert

Top candidate: ROBOUSDT
Score: 8.7
Drivers: OI jump, topic inflow, smart-money support
Account mode: portfolio_margin
Suggested action: run preflight

Reply:
- preflight ROBOUSDT 200 5
- cycle
- probe BTCUSDT
```

### Stage 2: Manual Confirmation

Current server-runnable flow now supports a guarded two-step live path.

User commands in Telegram:
- `cycle`
- `probe BTCUSDT`
- `preflight BTCUSDT 200 5`
- `preview BTCUSDT 200 5`
- `arm BTCUSDT 200 5`
- `dispatch BTCUSDT 200 5`
- `confirm <token>`

Guardian responses:
- run the requested Phoenix command
- summarize the result
- if `arm` succeeds, return a short-lived token
- if `confirm` succeeds, place the live entry, place the initial protective stop, and start the detached Guardian worker
- never bypass the token gate

Current live-execution flow:
1. User sends `preflight BTCUSDT 200 5`
2. Hermes runs `probe + preflight`
3. Hermes returns the exact plan and a short-lived confirmation token
4. User replies `confirm <token>`
5. Hermes calls the live-order wrapper
6. Phoenix immediately places `entry_market` and `initial_protective_stop`
7. Phoenix starts a detached Guardian worker for post-fill management
8. Hermes records the consumed token and writes the compact execution summary

Current scheduled-auto flow:
1. A server-side timer runs `autocycle`
2. Phoenix refreshes the public snapshot and candidate shortlist
3. Hermes compares the new top candidate to the previous top candidate
4. If the change is not significant, no dispatch happens
5. If the change is significant:
   - `DRY_RUN_ONLY`: only records the changed candidate
   - `MANUAL_CONFIRM`: issues a token and sends a Telegram arm-ready notification with reasons
   - `AUTO_CONFIRM_WHEN_RULES_PASS`: auto-confirms after a clean preflight

Execution mode switch:
- `PHOENIX_EXECUTION_MODE=DRY_RUN_ONLY`: `dispatch` behaves like authenticated dry-run only
- `PHOENIX_EXECUTION_MODE=MANUAL_CONFIRM`: `dispatch` issues a token and waits for `confirm <token>`
- `PHOENIX_EXECUTION_MODE=AUTO_CONFIRM_WHEN_RULES_PASS`: `dispatch` auto-confirms after a clean preflight

Recommended progression:
1. `DRY_RUN_ONLY`
2. `MANUAL_CONFIRM`
3. `AUTO_CONFIRM_WHEN_RULES_PASS`

That two-step flow is not optional. It is the minimum human gate.

Recommended formal trial profile:
- `PHOENIX_EXECUTION_MODE=MANUAL_CONFIRM`
- `PHOENIX_ALLOCATION_MODE=RISK_BUDGET`
- `PHOENIX_RISK_BUDGET_PCT_OF_BALANCE=3.0`
- `PHOENIX_MAX_OPEN_POSITIONS=1`
- `PHOENIX_STRATEGY_MIN_SCORE=6.0`
- `PHOENIX_REJECT_BLOCKED_CANDIDATES=true`

What that profile enforces:
- only one open position across the account at a time
- live entry is rejected if the selected ranked candidate is below the score threshold
- live entry is rejected if the ranked candidate carries `blocked_reasons`
- live entry still requires a confirmation token even after a clean preflight

## Runnable Server Pattern

Use the runner in this folder:

```bash
/opt/phoenix/deploy/hermes/phoenix_hermes_runner.sh cycle
/opt/phoenix/deploy/hermes/phoenix_hermes_runner.sh probe --symbol BTCUSDT --env prod
/opt/phoenix/deploy/hermes/phoenix_hermes_runner.sh preflight --symbol BTCUSDT --quote-allocation 200 --leverage 5 --env prod
/opt/phoenix/deploy/hermes/phoenix_hermes_runner.sh arm --symbol BTCUSDT --quote-allocation 200 --leverage 5 --env prod
/opt/phoenix/deploy/hermes/phoenix_hermes_runner.sh confirm ABC123TOKEN
```

The runner:
- sources `/etc/phoenix/phoenix.env`
- switches to `/opt/phoenix`
- calls the existing bounded Phoenix wrapper

This keeps Hermes simple and keeps all Binance trading behavior inside Phoenix.

## Memory And Context Policy

Current recommendation:
- keep Hermes as a short-context orchestrator
- keep Phoenix state in structured files, not in long chat transcripts
- rotate execution conversations aggressively
- do not add vector memory yet

What Hermes should retain:
- the latest Phoenix status summary
- the latest top candidate
- the latest account mode and available balance
- whether a preflight is still pending manual confirmation
- the currently active confirmation token, if one exists

What Hermes should not retain inline for long:
- raw market snapshots
- full candidate lists from every cycle
- long trade-research conversations mixed into the execution thread

State files written by the runner:
- `~/.hermes/memories/phoenix_state.json`
- `~/.hermes/memories/phoenix_state.md`
- `~/.hermes/memories/phoenix_pending_confirmation.json`
- `~/.hermes/memories/phoenix_guardian_workers/<token>.json`
- `~/.hermes/memories/phoenix_reviews/latest.json`
- `~/.hermes/memories/phoenix_reviews/latest.md`

Those files are intended to be the single compact handoff between Phoenix and Hermes.

Bi-hourly review loop:
- `deploy/hermes/phoenix_bi_hourly_review.py` reviews the last 2 hours of:
  - auto-cycle history
  - closed trade journals
  - Guardian worker activity
  - the latest compact state snapshot
- It writes a timestamped review plus `latest.json/.md`
- It sends a Telegram review summary when `--notify-telegram` is enabled
- It is recommendations-only and does **not** mutate live config or code

Session rotation policy:
1. Keep Telegram execution threads short.

## Binance Skills Intake

Hermes can now read two Binance Skills input files from:

- `~/.hermes/memories/binance_skills/assets_latest.json`
- `~/.hermes/memories/binance_skills/crypto_market_rank_latest.json`

Run:

```bash
python3 /opt/phoenix/deploy/hermes/leiting_skill_digest.py --hermes-home /root/.hermes
```

Or import payloads and refresh digest in one step:

```bash
python3 /opt/phoenix/deploy/hermes/binance_skills_import.py \
  --hermes-home /root/.hermes \
  --assets-json /root/.hermes/memories/binance_skills/assets_latest.json \
  --market-rank-json /root/.hermes/memories/binance_skills/crypto_market_rank_latest.json \
  --notify-telegram
```

Outputs:

- `~/.hermes/memories/binance_skills/digest_latest.json`
- `~/.hermes/memories/binance_skills/digest_latest.md`
- Telegram status summary when `--notify-telegram` is enabled

### Square Post Channel

Hermes now supports a **Square preview channel** via:

- `~/.hermes/memories/binance_square/preview_latest.json`
- `~/.hermes/memories/binance_square/preview_latest.md`
- `~/.hermes/memories/binance_square/status.json`

Environment flags:

```bash
HERMES_BINANCE_SQUARE_ENABLED=false
HERMES_BINANCE_SQUARE_MODE=preview_only
```

Default is **off** for three reasons:

1. public posting should not be coupled to live trading decisions
2. a publication feedback loop can distort strategy evaluation
3. operationally, research summaries need human review before becoming public content

## Telegram Relay Fallback

If the Hermes control server cannot reach `api.telegram.org` through its local proxy, Hermes can fall back to a relay sender on the Leiting server.

Recommended env:

```bash
HERMES_TELEGRAM_RELAY_TARGET=root@43.98.174.32
HERMES_TELEGRAM_RELAY_KEY_PATH=/root/.hermes/keys/leiting_tg_relay
```

Relay script on the Leiting server:

- [deploy/btc/telegram_relay.py](/Users/yanshisan/Desktop/币安交易/deploy/btc/telegram_relay.py)
2. Use one execution session for live operational commands only.
3. Start a separate session for research and strategy discussion.
4. Archive or replace the execution session after a trade completes or after a stale preflight.
5. Let Hermes consult `phoenix_state.*` instead of replaying old raw outputs.

Vector memory policy:
- not required for the current live stack
- add later only when you have enough trade journals, post-trade notes, and repeated pattern data to justify semantic retrieval

Tools not required today:
- Open Viking
- Obsidian as a runtime dependency

Obsidian can still be useful as a manual research notebook, but it should stay outside the execution path.

## Hermes Config

Use `config.yaml.example` in this folder as the base for `~/.hermes/config.yaml`.

Use `.env.example` in this folder as the base for `~/.hermes/.env`.

For the v2 control-plane design, field handoff, and Hermes learning map, see:
- `deploy/hermes/HERMES_PHOENIX_V2.md`

Recommended provider layout:
- primary model: `gemini-3.1-pro-preview`
- main config must explicitly set `model.provider: gemini`
- if `OPENAI_API_KEY` is present and the main provider is left on `auto`, Hermes can auto-route the main model to `openrouter`
- fallback chain: `openai-direct / gpt-5.4`
- do not prefix the Gemini model with `google/` when using Hermes' native `gemini` provider
- ensure `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` are set if the server cannot reach Google directly
- define a named provider under `providers:` for direct OpenAI fallback; `fallback_providers` should point at that configured provider key, not a bare `openai` label

Important split:
- Hermes secrets: `~/.hermes/.env`
- Phoenix trading secrets: `/etc/phoenix/phoenix.env`

Do not duplicate Binance trading secrets into Hermes unless you intentionally want Hermes to call Phoenix without the wrapper.

## Recommended Commands For Hermes

These are the only Phoenix commands Hermes should use today:

- `cycle`
- `autocycle`
- `probe`
- `preview`
- `preflight`
- `arm`
- `dispatch`
- `confirm`

Hermes should not call:
- raw `phoenix_*.py` scripts directly
- Binance endpoints directly
- any future live-order command unless the user explicitly enables that path

## Suggested Operating Policy

1. Run `cycle` every 5 minutes on the server.
2. Only notify Telegram when the top candidate materially changes.
3. Require `preflight` before any trade recommendation.
4. In `MANUAL_CONFIRM`, let `autocycle` escalate only via short-lived Telegram tokens.
5. Keep Telegram as the inbox and Hermes as the orchestrator, not as the trading engine.
