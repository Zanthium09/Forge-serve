#!/usr/bin/env bash
# Sequential vs continuous-batching benchmark matrix.
#
# Run from the repo root in Git Bash:  bash benchmarks/run_bench.sh
#
# Identical infrastructure for both engines: only the worker's ENGINE env
# changes between phases, so the comparison isolates the scheduler.
set -euo pipefail

DURATION="${DURATION:-60s}"
WARMUP="${WARMUP:-10s}"
MAX_TOKENS="${MAX_TOKENS:-64}"
OUT_DIR="benchmarks/raw"
LOADGEN="loadgen/loadgen.exe"

mkdir -p "$OUT_DIR"

wait_healthy() {
  echo "waiting for worker to become healthy..."
  for _ in $(seq 1 60); do
    if docker compose ps worker | grep -q "healthy"; then
      echo "worker healthy"
      return 0
    fi
    sleep 5
  done
  echo "worker did not become healthy in time" >&2
  exit 1
}

run_engine() {
  local engine="$1"; shift
  local concurrencies="$1"; shift

  echo "=============================================="
  echo "ENGINE=$engine"
  echo "=============================================="
  ENGINE="$engine" docker compose up -d --force-recreate worker >/dev/null
  wait_healthy

  for conc in $concurrencies; do
    echo "--- $engine closed-loop concurrency=$conc ---"
    "$LOADGEN" -mode closed -concurrency "$conc" \
      -duration "$DURATION" -warmup "$WARMUP" -max-tokens "$MAX_TOKENS" \
      -label "$engine" -out "$OUT_DIR/${engine}_closed_c${conc}.csv"
  done
}

# Throughput comparison + the headline conc=16 number; conc 10/50 give the
# TTFT percentiles the blueprint asks for.
run_engine sequential "1 8 16"
run_engine batched    "1 8 16 10 50"

echo
echo "done. raw CSVs in $OUT_DIR"
