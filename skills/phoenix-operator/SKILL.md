---
name: phoenix-operator
description: Safely operate Project Phoenix from OpenClaw. Use this skill for bounded Binance market scans, candidate ranking, unified-account auth probes, dry-run futures execution previews, and token-gated live entry confirmation.
metadata: {"author":"Project Phoenix","version":"0.1.0","openclaw":{"requires":{"bins":["python3"]}}}
---

# Phoenix Operator

Use this workspace skill when the user wants OpenClaw to run Project Phoenix inside this repository.

## Safety Rules

- Always call `python3 {baseDir}/phoenix_openclaw.py ...`.
- Do not shell directly into `phoenix_*.py` or `sentry.py` unless you are debugging this wrapper.
- Allowed actions are `sentry`, `scan`, `judge`, `cycle`, `autocycle`, `probe`, `preview`, `preflight`, `arm`, `dispatch`, and `confirm`.
- `preflight` is still dry-run only. It may authenticate against Binance, but it must not place live orders.
- `arm` may authenticate and issue a short-lived confirmation token, but it must not place live orders.
- `arm` now also sends a Telegram notification with the token, strategy reason, and key risk parameters when Telegram is configured.
- `confirm` is the only live execution step. It may place `entry_market` plus `initial_protective_stop` after a valid token is supplied, and it now starts a detached Guardian worker for post-fill protection management.
- Do not change leverage or margin mode through this skill.
- Do not ask the user to paste secrets into chat. Phoenix already reads the workspace `.env`. If an OpenClaw runtime mirrors secrets from `.openclaw/secrets.env`, it should define the same `PHOENIX_BINANCE_*` variables.

## Command Catalog

### Watch a short live ticker burst

```bash
python3 {baseDir}/phoenix_openclaw.py sentry --duration-sec 15
```

Optional explicit symbols:

```bash
python3 {baseDir}/phoenix_openclaw.py sentry --symbols BTCUSDT SOLUSDT
```

### Run a bounded public scout pass

```bash
python3 {baseDir}/phoenix_openclaw.py scan --duration-sec 20 --top-n 10
```

### Rank an existing snapshot

```bash
python3 {baseDir}/phoenix_openclaw.py judge \
  --snapshot-file phoenix_snapshot.openclaw.json \
  --output-file phoenix_candidates.openclaw.json \
  --top-n 10 \
  --audit-top-n 8
```

### One-shot scan + judge cycle

```bash
python3 {baseDir}/phoenix_openclaw.py cycle \
  --scan-duration-sec 20 \
  --top-n 10 \
  --audit-top-n 8
```

### Autonomous scan + conditional dispatch

```bash
python3 {baseDir}/phoenix_openclaw.py autocycle \
  --scan-duration-sec 20 \
  --top-n 10 \
  --audit-top-n 8 \
  --quote-allocation 200 \
  --leverage 5 \
  --env prod
```

Behavior:
- runs a bounded public scan
- rewrites `phoenix_candidates.openclaw.json`
- compares the new top candidate to the previous top candidate
- only triggers `dispatch` when the candidate changed materially
- in `MANUAL_CONFIRM` mode, issues a short-lived token and sends the token + reason to Telegram

### Probe signed Binance access

```bash
python3 {baseDir}/phoenix_openclaw.py probe --symbol BTCUSDT --env prod
```

### Preview an order plan without auth

```bash
python3 {baseDir}/phoenix_openclaw.py preview \
  --symbol BTCUSDT \
  --quote-allocation 200 \
  --leverage 5
```

Or preview from ranked candidates:

```bash
python3 {baseDir}/phoenix_openclaw.py preview \
  --candidate-file phoenix_candidates.openclaw.json \
  --candidate-rank 1 \
  --quote-allocation 200 \
  --leverage 5
```

### Run authenticated dry-run validation

```bash
python3 {baseDir}/phoenix_openclaw.py preflight \
  --symbol BTCUSDT \
  --quote-allocation 200 \
  --leverage 5 \
  --env prod
```

### Issue a short-lived live confirmation token

```bash
python3 {baseDir}/phoenix_openclaw.py arm \
  --symbol BTCUSDT \
  --quote-allocation 200 \
  --leverage 5 \
  --env prod
```

### Consume a confirmation token

```bash
python3 {baseDir}/phoenix_openclaw.py confirm ABC123TOKEN
```

## Recommended Flow

1. Use `cycle` when the user asks for the latest Phoenix scan and ranking.
2. Use `probe` before any authenticated action if account status is uncertain.
3. Use `preview` for deterministic order planning.
4. Use `preflight` only after the user explicitly asks for signed dry-run validation.
5. Use `arm` only after `preflight` succeeds and the user explicitly wants a live-ready confirmation token.
6. Use `autocycle` for scheduled server-side scans that should only escalate on significant candidate changes.
7. Use `confirm <token>` only after the user explicitly confirms they want the live order submitted now.
8. Live execution currently places `entry_market` plus `initial_protective_stop`, then starts a detached Guardian worker to move the stop to breakeven, arm the trailing stop, and clean up residual protection after the position closes.
