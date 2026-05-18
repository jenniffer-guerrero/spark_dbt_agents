#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$ROOT_DIR/spark_env.sh"

cd "$ROOT_DIR/dbt_spark"
exec spark-sql "$@"
