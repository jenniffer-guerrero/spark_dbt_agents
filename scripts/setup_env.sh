#!/usr/bin/env bash
# Source this file to activate Python venv + Java + Spark env in one step.
# Usage:
#   source scripts/setup_env.sh
# Optional overrides:
#   source scripts/setup_env.sh --python python3.10 --venv .venv

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
PYTHON_BIN="python3"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --venv)
      VENV_DIR="$2"
      shift 2
      ;;
    --python)
      PYTHON_BIN="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: source scripts/setup_env.sh [--venv <path>] [--python <python_bin>]" >&2
      return 1 2>/dev/null || exit 1
      ;;
  esac
done

# shellcheck source=/dev/null
source "${ROOT_DIR}/spark_env.sh"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "[setup_env] venv not found at ${VENV_DIR}. Creating it with ${PYTHON_BIN}..."
  if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    echo "[setup_env] ERROR: Python interpreter '${PYTHON_BIN}' not found." >&2
    return 1 2>/dev/null || exit 1
  fi
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

# shellcheck source=/dev/null
source "${VENV_DIR}/bin/activate"

export PYSPARK_PYTHON="${VENV_DIR}/bin/python"
export PYSPARK_DRIVER_PYTHON="${VENV_DIR}/bin/python"

cat <<EOF
[setup_env] Environment ready
  ROOT_DIR: ${ROOT_DIR}
  VENV:     ${VENV_DIR}
  PYTHON:   $(python --version 2>/dev/null || true)
  JAVA_HOME:${JAVA_HOME:-<unset>}
  SPARK_CONF_DIR:${SPARK_CONF_DIR:-<unset>}

Next steps:
  1) Install deps if needed: bash scripts/setup_venv.sh
  2) Open notebook and select kernel: Python (.venv) jguerrero
EOF
