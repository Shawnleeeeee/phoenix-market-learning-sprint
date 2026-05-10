#!/usr/bin/env bash
set -euo pipefail
cd /opt/phoenix-testnet
set -a
. /etc/phoenix/phoenix-testnet.env
set +a
exec .venv/bin/python phoenix_testnet_round_runner.py   --runtime-mode TESTNET_LIVE   --env testnet   --run-forever   --max-open-positions 10   --margin-type ISOLATED   --max-concurrent-trades 3   --output-dir /opt/phoenix-testnet/round_runner_reports "$@"
