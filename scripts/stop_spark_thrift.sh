#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

SPARK_HOME="$($PYTHON_BIN - <<'PY'
import os
import pyspark

print(os.path.dirname(pyspark.__file__))
PY
)"

exec "$SPARK_HOME/sbin/stop-thriftserver.sh"
