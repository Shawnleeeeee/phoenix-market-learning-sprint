#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <ssh-target> <phoenix-action> [args...]" >&2
  exit 1
fi

target="$1"
shift

printf -v remote_args '%q ' "$@"

ssh "$target" \
  "cd /opt/phoenix && /opt/phoenix/.venv/bin/python /opt/phoenix/skills/phoenix-operator/phoenix_openclaw.py ${remote_args}"
