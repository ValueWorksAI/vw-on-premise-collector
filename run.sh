#!/usr/bin/env bash
# Thin launcher for cron / systemd. Usage: ./run.sh [full|delta] [sources] [--parallel]
set -euo pipefail
cd "$(dirname "$0")"

MODE="${1:-delta}"
SOURCES="${2:-}"

ARGS=(run python orchestrator.py --mode "$MODE")
[[ -n "$SOURCES" ]] && ARGS+=(--sources "$SOURCES")
[[ "${3:-}" == "--parallel" ]] && ARGS+=(--parallel)

exec poetry "${ARGS[@]}"
