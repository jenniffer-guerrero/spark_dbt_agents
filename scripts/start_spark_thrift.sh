#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

export SPARK_CONF_DIR="$ROOT_DIR/spark_conf"
export SPARK_NO_DAEMONIZE=true

# Set HADOOP and SPARK environment variables
export HADOOP_HOME="$ROOT_DIR/hadoop"
export SPARK_HOME="$ROOT_DIR/spark"
export PATH="$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"

SPARK_HOME="$($PYTHON_BIN - <<'PY'
import os
import pyspark

print(os.path.dirname(pyspark.__file__))
PY
)"

mkdir -p "$ROOT_DIR/.local/spark/warehouse" "$ROOT_DIR/.local/spark/logs"

# Set warehouse directory explicitly
export SPARK_SQL_WAREHOUSE_DIR="$ROOT_DIR/.local/spark/warehouse"

"$PYTHON_BIN" "$ROOT_DIR/scripts/bootstrap_spark_metastore.py"
