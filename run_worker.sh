#!/bin/sh

# Start the worker with every Python bytecode cache redirected outside the
# project tree. PYTHONPYCACHEPREFIX must be present before Python starts;
# setting sys.pycache_prefix inside src would already be too late for src itself.

set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CACHE_USER_ID="${AUTOMATION_CACHE_USER_ID:-}"
if [ -z "$CACHE_USER_ID" ]; then
  CACHE_USER_ID=$(id -u 2>/dev/null || printf '%s' user)
fi
PYTHON_CACHE_ROOT="/tmp/automation_system_pycache_${CACHE_USER_ID}"

umask 077
mkdir -p -- "$PYTHON_CACHE_ROOT"

export PYTHONPYCACHEPREFIX="$PYTHON_CACHE_ROOT"
export PYTHONDONTWRITEBYTECODE=1
cd -- "$PROJECT_ROOT"

if [ "${1:-}" = "--preflight" ]; then
  exec "${PYTHON_BIN:-python3}" -m src.system.worker_preflight --json
fi

if [ "${AUTOMATION_SKIP_PREFLIGHT:-0}" != "1" ]; then
  "${PYTHON_BIN:-python3}" -m src.system.worker_preflight
fi

exec "${PYTHON_BIN:-python3}" -m src.sync_xserver_main "$@"
