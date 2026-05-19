#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
METASTORE_DIR="${METASTORE_DIR:-$ROOT_DIR/.local/spark/metastore_db}"
LOCK_FILES=("$METASTORE_DIR/db.lck" "$METASTORE_DIR/dbex.lck")
APPLY=0

usage() {
  cat <<'EOF'
Usage:
  bash ./stop_spark_holders.sh           # dry-run: show lock holder processes
  bash ./stop_spark_holders.sh --apply   # stop lock holder processes

Options:
  --apply  Terminate detected holder processes (TERM, then KILL if needed).
  -h       Show this help.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=1 ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "$METASTORE_DIR" ]]; then
  echo "Metastore directory not found: $METASTORE_DIR"
  exit 0
fi

declare -a PIDS=()

for lock_file in "${LOCK_FILES[@]}"; do
  if [[ -f "$lock_file" ]]; then
    while IFS= read -r pid; do
      [[ -n "$pid" ]] && PIDS+=("$pid")
    done < <(lsof -t "$lock_file" 2>/dev/null || true)
  fi
done

# Fallback process search if lock files are present but no lsof holder is returned.
if [[ ${#PIDS[@]} -eq 0 ]]; then
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && PIDS+=("$pid")
  done < <(pgrep -f "$METASTORE_DIR" || true)
fi

if [[ ${#PIDS[@]} -eq 0 ]]; then
  echo "No process currently holding metastore lock for: $METASTORE_DIR"
  exit 0
fi

UNIQUE_PIDS=()
while IFS= read -r pid; do
  [[ -n "$pid" ]] && UNIQUE_PIDS+=("$pid")
done < <(printf '%s\n' "${PIDS[@]}" | sort -u)

echo "Detected metastore lock holder process(es):"
ps -o pid=,ppid=,command= -p "${UNIQUE_PIDS[@]}" || true

if [[ $APPLY -eq 0 ]]; then
  echo
  echo "Dry-run only. Re-run with --apply to stop these processes."
  exit 0
fi

echo
echo "Stopping lock holder process(es) with SIGTERM..."
kill -TERM "${UNIQUE_PIDS[@]}" 2>/dev/null || true

REMAINING=()
for _ in 1 2 3 4 5; do
  sleep 1
  REMAINING=()
  for pid in "${UNIQUE_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      REMAINING+=("$pid")
    fi
  done
  if [[ ${#REMAINING[@]} -eq 0 ]]; then
    break
  fi
done

if [[ ${#REMAINING[@]} -gt 0 ]]; then
  echo "Some processes are still running; sending SIGKILL..."
  kill -KILL "${REMAINING[@]}" 2>/dev/null || true
fi

echo "Done. You can retry: bash ./open_spark_sql.sh"