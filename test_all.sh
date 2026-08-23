#!/bin/sh

# Run the complete dependency-light Worker validation suite without pytest.

set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PYTHON="${PYTHON_BIN:-python3}"
CACHE_USER_ID="${AUTOMATION_CACHE_USER_ID:-}"
if [ -z "$CACHE_USER_ID" ]; then
  CACHE_USER_ID=$(id -u 2>/dev/null || printf '%s' user)
fi

export PYTHONDONTWRITEBYTECODE=1
export PYTHONPYCACHEPREFIX="/tmp/automation_system_pycache_${CACHE_USER_ID}"

cd -- "$PROJECT_ROOT"

find . -type f -name '*.sh' -print | sort | while IFS= read -r shell_file; do
  if head -n 1 "$shell_file" | grep -q 'bash'; then
    bash -n "$shell_file"
  else
    sh -n "$shell_file"
  fi
done

"$PYTHON" -m compileall -q src test _tools

AUTOMATION_PROFILE=unit_test "$PYTHON" -m src.system.worker_preflight --json >/dev/null
AUTOMATION_PROFILE=standalone "$PYTHON" -m src.system.worker_preflight --json >/dev/null

test_count=0
for test_file in test/test_*.py; do
  printf 'RUN %s\n' "$test_file"
  "$PYTHON" "$test_file"
  test_count=$((test_count + 1))
done

if find . -type f \( -name '*.key' -o -name '*.p12' -o -name '*.pfx' \) \
  ! -path './_runtime/*' | grep . >/dev/null; then
  printf 'Private key material was found in the release tree.\n' >&2
  exit 1
fi

printf 'PASS complete Worker validation: %s executable tests\n' "$test_count"
