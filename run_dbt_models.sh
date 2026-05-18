#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Shared Spark environment for consistent metastore/warehouse across tools.
source "$ROOT_DIR/spark_env.sh"

cd "$ROOT_DIR/dbt_spark"
"$ROOT_DIR/.venv/bin/dbt" run --profiles-dir "$ROOT_DIR/.dbt"