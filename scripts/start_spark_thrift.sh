#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
WAREHOUSE_DIR="$ROOT_DIR/.local/spark/warehouse"
METASTORE_DIR="$ROOT_DIR/.local/spark/metastore_db"
LOG_DIR="$ROOT_DIR/.local/spark/logs"

# Shared Java + Spark env (JAVA_HOME, SPARK_CONF_DIR, PATH)
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/spark_env.sh"

if [[ ! -x "$PYTHON_BIN" ]]; then
	echo "ERROR: Python venv not found at $PYTHON_BIN" >&2
	echo "Run: bash $ROOT_DIR/scripts/setup_venv.sh" >&2
	exit 1
fi

SPARK_HOME="$($PYTHON_BIN - <<'PY'
import os
import pyspark

print(os.path.dirname(pyspark.__file__))
PY
)"
export SPARK_HOME
export SPARK_LOG_DIR="$LOG_DIR"

mkdir -p "$WAREHOUSE_DIR" "$LOG_DIR" "$ROOT_DIR/.local/spark"

SPARK_THRIFT_HOST="${SPARK_THRIFT_HOST:-127.0.0.1}"
SPARK_THRIFT_PORT="${SPARK_THRIFT_PORT:-10001}"

"$PYTHON_BIN" "$ROOT_DIR/scripts/bootstrap_spark_metastore.py"

"$SPARK_HOME/sbin/start-thriftserver.sh" \
	--master "local[*]" \
	--hiveconf "hive.server2.thrift.bind.host=$SPARK_THRIFT_HOST" \
	--hiveconf "hive.server2.thrift.port=$SPARK_THRIFT_PORT" \
	--conf "spark.sql.warehouse.dir=$WAREHOUSE_DIR" \
	--conf "spark.sql.catalogImplementation=hive" \
	--conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$METASTORE_DIR;create=true" \
	--conf "spark.hadoop.javax.jdo.option.ConnectionDriverName=org.apache.derby.jdbc.EmbeddedDriver" \
	--conf "spark.hadoop.datanucleus.schema.autoCreateAll=true" \
	--conf "spark.hadoop.hive.metastore.schema.verification=false"

echo "Spark Thrift Server started at ${SPARK_THRIFT_HOST}:${SPARK_THRIFT_PORT}"
