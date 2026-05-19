#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$ROOT_DIR/spark_env.sh"

METASTORE_DIR="$ROOT_DIR/.local/spark/metastore_db"
ISOLATED_METASTORE_DIR="$ROOT_DIR/.local/spark/metastore_db_cli"

run_spark_sql() {
	cd "$ROOT_DIR/dbt_spark"
	spark-sql "$@"
}

if [[ "${SPARK_SQL_USE_ISOLATED_METASTORE:-0}" == "1" ]]; then
	# Derby must create this directory itself when create=true.
	# If an empty folder exists (for example from a prior failed run), remove it.
	if [[ -d "$ISOLATED_METASTORE_DIR" && ! -f "$ISOLATED_METASTORE_DIR/service.properties" ]]; then
		rmdir "$ISOLATED_METASTORE_DIR" 2>/dev/null || true
	fi
	cd "$ROOT_DIR/dbt_spark"
	exec spark-sql \
		--conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$ISOLATED_METASTORE_DIR;create=true" \
		"$@"
fi

TMP_LOG="$(mktemp -t open_spark_sql.XXXXXX.log)"
if run_spark_sql "$@" 2>"$TMP_LOG"; then
	rm -f "$TMP_LOG"
	exit 0
fi

if grep -q "ERROR XSDB6: Another instance of Derby may have already booted the database" "$TMP_LOG"; then
	rm -f "$TMP_LOG"
	{
		echo "open_spark_sql.sh failed: Derby metastore is locked by another Spark process."
		echo "Metastore path: $METASTORE_DIR"
		echo
		echo "How to resolve:"
		echo "1) Stop other Spark sessions (running notebooks, Spark Thrift, or spark-sql shells)."
		echo "2) Or run: bash ./stop_spark_holders.sh --apply"
		echo "3) Retry: bash ./open_spark_sql.sh"
		echo
		echo "If you need an immediate isolated shell, run:"
		echo "SPARK_SQL_USE_ISOLATED_METASTORE=1 bash ./open_spark_sql.sh"
	} >&2
	exit 1
fi

cat "$TMP_LOG" >&2
rm -f "$TMP_LOG"
exit 1
