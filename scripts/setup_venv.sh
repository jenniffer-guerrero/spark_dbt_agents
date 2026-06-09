#!/usr/bin/env bash
set -euo pipefail

# Reproducible local environment bootstrap for this repository.
# Usage:
#   bash scripts/setup_venv.sh
# Optional:
#   VENV_DIR=.venv PYTHON_BIN=python3.10 bash scripts/setup_venv.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
PYTHON_BIN="${PYTHON_BIN:-python3.10}"
KERNEL_NAME="${KERNEL_NAME:-jguerrero-venv}"
KERNEL_DISPLAY_NAME="${KERNEL_DISPLAY_NAME:-Python (.venv) jguerrero}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] Python interpreter '$PYTHON_BIN' not found."
  echo "        Try: PYTHON_BIN=python3 bash scripts/setup_venv.sh"
  exit 1
fi

echo "[1/5] Creating virtual environment at: $VENV_DIR"
"$PYTHON_BIN" -m venv "$VENV_DIR"

# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

echo "[2/5] Upgrading pip tooling"
python -m pip install --upgrade pip setuptools wheel

echo "[3/5] Installing project dependencies"
python -m pip install \
  "dbt-spark[pyhive]>=1.9.0,<1.9.4" \
  "dbt-core>=1.10.1,<1.10.10" \
  "pandas==2.2.3" \
  "pyspark==3.4.1" \
  "pyarrow>=17.0.0,<18" \
  "ipykernel"

echo "[4/5] Registering Jupyter kernel: $KERNEL_DISPLAY_NAME"
python -m ipykernel install --user --name "$KERNEL_NAME" --display-name "$KERNEL_DISPLAY_NAME"

echo "[5/5] Validating critical imports"
python - <<'PY'
import pyspark
import pandas
import pyarrow
print('OK: pyspark', pyspark.__version__)
print('OK: pandas', pandas.__version__)
print('OK: pyarrow', pyarrow.__version__)
PY

echo
echo "Done. Activate env with:"
echo "  source '$VENV_DIR/bin/activate'"
echo "Then in VS Code notebooks, select kernel: $KERNEL_DISPLAY_NAME"
