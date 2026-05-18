#!/usr/bin/env bash

# Source this file to standardize Java + Spark config across dbt, pyspark and spark-sql.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_CONF_DIR="$ROOT_DIR/spark_conf"
