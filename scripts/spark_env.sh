#!/usr/bin/env bash

# Source this file to standardize Java + Spark config across pyspark and spark-sql.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

die() {
	echo "[spark_env] ERROR: $*" >&2
	return 1 2>/dev/null || exit 1
}

pick_java_home() {
	if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
		echo "$JAVA_HOME"
		return 0
	fi

	local candidates=(
		"/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
		"/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
		"/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
	)

	local c
	for c in "${candidates[@]}"; do
		if [[ -x "$c/bin/java" ]]; then
			echo "$c"
			return 0
		fi
	done

	if command -v /usr/libexec/java_home >/dev/null 2>&1; then
		c="$(/usr/libexec/java_home -v 11+ 2>/dev/null || true)"
		if [[ -n "$c" && -x "$c/bin/java" ]]; then
			echo "$c"
			return 0
		fi
	fi

	return 1
}

parse_java_major() {
	local java_bin="$1"
	local line major
	line="$($java_bin -version 2>&1 | head -n1)"
	major="$(echo "$line" | sed -E 's/.*version "([0-9]+)(\.[0-9]+)?(.+)".*/\1/')"
	if [[ "$major" =~ ^[0-9]+$ ]]; then
		echo "$major"
		return 0
	fi
	return 1
}

JAVA_HOME="$(pick_java_home || true)"
if [[ -z "$JAVA_HOME" ]]; then
	die "No compatible Java installation found. Install Java 11-21 and re-run."
fi

JAVA_MAJOR="$(parse_java_major "$JAVA_HOME/bin/java" || true)"
if [[ -z "$JAVA_MAJOR" ]]; then
	die "Unable to parse Java version from: $JAVA_HOME/bin/java"
fi

if (( JAVA_MAJOR < 11 || JAVA_MAJOR > 21 )); then
	die "Detected Java $JAVA_MAJOR at $JAVA_HOME. Supported range for this project is 11-21."
fi

export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_CONF_DIR="$ROOT_DIR/spark_conf"
