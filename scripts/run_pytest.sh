#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_PYTHON="$ROOT_DIR/.venv/bin/python"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "[run_pytest] ERROR: .venv not found at $VENV_PYTHON" >&2
  echo "Run: bash scripts/setup_venv.sh" >&2
  exit 1
fi

export PYSPARK_PYTHON="$VENV_PYTHON"
export PYSPARK_DRIVER_PYTHON="$VENV_PYTHON"

cd "$ROOT_DIR"

if [[ $# -eq 0 ]]; then
  exec "$VENV_PYTHON" -m pytest -q tests/test_siebel_incremental.py
fi

exec "$VENV_PYTHON" -m pytest "$@"
