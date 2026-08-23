#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.}"
find "$ROOT" -type d -name '__pycache__' -prune -exec rm -rf {} +
find "$ROOT" -type f -name '*.pyc' -delete
find "$ROOT" -type d -name '.pytest_cache' -prune -exec rm -rf {} +
printf 'Python cache files removed below: %s\n' "$ROOT"
