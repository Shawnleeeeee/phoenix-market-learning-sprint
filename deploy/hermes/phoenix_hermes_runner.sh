#!/usr/bin/env bash
set -euo pipefail

PHOENIX_ROOT="${PHOENIX_ROOT:-/opt/phoenix}"
PHOENIX_ENV_FILE="${PHOENIX_ENV_FILE:-/etc/phoenix/phoenix.env}"
DEFAULT_VENV_PYTHON="$PHOENIX_ROOT/.venv/bin/python"
if [[ -x "$DEFAULT_VENV_PYTHON" ]]; then
  PYTHON_BIN="${PYTHON_BIN:-$DEFAULT_VENV_PYTHON}"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
SUMMARY_SCRIPT="${SUMMARY_SCRIPT:-$PHOENIX_ROOT/deploy/hermes/update_state_summary.py}"

if [[ ! -d "$PHOENIX_ROOT" ]]; then
  echo "Phoenix root not found: $PHOENIX_ROOT" >&2
  exit 1
fi

if [[ -f "$PHOENIX_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$PHOENIX_ENV_FILE"
  set +a
fi

cd "$PHOENIX_ROOT"

stdout_file="$(mktemp)"
stderr_file="$(mktemp)"

set +e
"$PYTHON_BIN" "$PHOENIX_ROOT/skills/phoenix-operator/phoenix_openclaw.py" "$@" >"$stdout_file" 2>"$stderr_file"
exit_code=$?
set -e

cat "$stdout_file"
cat "$stderr_file" >&2

if [[ -f "$SUMMARY_SCRIPT" ]]; then
  "$PYTHON_BIN" "$SUMMARY_SCRIPT" \
    --phoenix-root "$PHOENIX_ROOT" \
    --hermes-home "$HERMES_HOME" \
    --exit-code "$exit_code" \
    --stdout-file "$stdout_file" \
    --stderr-file "$stderr_file" \
    -- "$@" || true
fi

rm -f "$stdout_file" "$stderr_file"
exit "$exit_code"
