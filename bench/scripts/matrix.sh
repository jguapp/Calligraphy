#!/usr/bin/env bash
# Runs the benchmark matrix against native processes. Reproducible: every
# knob is explicit, every result lands in bench/results/ with provenance.
#
# Usage:
#   bench/scripts/matrix.sh sweep-io      # worker-count sweep, I/O-bound
#   bench/scripts/matrix.sh sweep-cpu     # worker-count sweep, CPU-bound
#   bench/scripts/matrix.sh ablation      # baseline -> optimized, one lever at a time
#   bench/scripts/matrix.sh reliability   # 10k jobs, injected failures, worker kills
#
# Requires: postgres + redis reachable at the DSNs below, bin/ built.
set -euo pipefail

cd "$(dirname "$0")/../.."

DB="${CALIGRAPHY_DATABASE_URL:-postgres://postgres:postgres@127.0.0.1:5432/caligraphy?sslmode=disable}"
TOKEN="cg_bench_token"
API=http://127.0.0.1:8080
LOGDIR="${BENCH_LOGDIR:-/tmp/caligraphy-bench-logs}"
mkdir -p "$LOGDIR"

api_pid=""
worker_pids=()

start_api() {
  CALIGRAPHY_DATABASE_URL="$DB" CALIGRAPHY_API_TOKENS="$TOKEN" \
    ./bin/caligraphy-api >"$LOGDIR/api.log" 2>&1 &
  api_pid=$!
  until curl -sf "$API/readyz" >/dev/null 2>&1; do sleep 0.2; done
}

# start_workers N CONCURRENCY EXTRA_ENV...
start_workers() {
  local n=$1 conc=$2; shift 2
  for i in $(seq 1 "$n"); do
    env CALIGRAPHY_DATABASE_URL="$DB" CALIGRAPHY_WORKER_ID="bench-w$i" \
        CALIGRAPHY_WORKER_METRICS_ADDR=":$((9100 + i))" CALIGRAPHY_CONCURRENCY="$conc" "$@" \
        ./bin/caligraphy-worker >"$LOGDIR/worker-$i.log" 2>&1 &
    worker_pids+=($!)
  done
  sleep 1.5 # registration + group join
}

stop_workers() {
  for p in "${worker_pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  for p in "${worker_pids[@]:-}"; do wait "$p" 2>/dev/null || true; done
  worker_pids=()
}

cleanup() {
  stop_workers
  [ -n "$api_pid" ] && kill "$api_pid" 2>/dev/null || true
}
trap cleanup EXIT

bench() { ./bin/caligraphy-bench -api "$API" -token "$TOKEN" "$@"; }

case "${1:-}" in
sweep-io)
  start_api
  for n in 1 2 4 7; do
    start_workers "$n" 4
    bench -name "sweep-io-${n}w" -type bench.sleep -jobs 3000 -sleep-ms 50 -jitter-ms 20 \
      -note "worker sweep, I/O-bound: $n procs x concurrency 4"
    stop_workers
  done
  ;;
sweep-cpu)
  start_api
  for n in 1 2 4 7; do
    start_workers "$n" 4
    bench -name "sweep-cpu-${n}w" -type bench.cpu -jobs 1500 -cpu-iters 200000 \
      -note "worker sweep, CPU-bound: $n procs x concurrency 4 on a 4-core host"
    stop_workers
  done
  ;;
ablation)
  start_api
  # v0 baseline: one worker, one job at a time, single connections, no
  # batching, one-at-a-time fetch. The honest naive implementation.
  start_workers 1 1 CALIGRAPHY_DB_MAX_CONNS=1 CALIGRAPHY_REDIS_POOL_SIZE=1 \
    CALIGRAPHY_BATCH_WRITES=false CALIGRAPHY_FETCH_BATCH=1
  bench -name "ablation-v0-baseline" -type bench.sleep -jobs 1000 -sleep-ms 50 -jitter-ms 20 \
    -note "v0 baseline: 1 worker, conc=1, pools=1, batch=off, fetch=1"
  stop_workers
  # v1: + connection pooling
  start_workers 1 1 CALIGRAPHY_DB_MAX_CONNS=8 CALIGRAPHY_REDIS_POOL_SIZE=8 \
    CALIGRAPHY_BATCH_WRITES=false CALIGRAPHY_FETCH_BATCH=1
  bench -name "ablation-v1-pooling" -type bench.sleep -jobs 1000 -sleep-ms 50 -jitter-ms 20 \
    -note "v1: v0 + connection pooling (pg 8, redis 8)"
  stop_workers
  # v2: + concurrency (goroutines) and batched fetch
  start_workers 1 8 CALIGRAPHY_DB_MAX_CONNS=8 CALIGRAPHY_REDIS_POOL_SIZE=8 \
    CALIGRAPHY_BATCH_WRITES=false CALIGRAPHY_FETCH_BATCH=8
  bench -name "ablation-v2-concurrency" -type bench.sleep -jobs 1000 -sleep-ms 50 -jitter-ms 20 \
    -note "v2: v1 + goroutine concurrency 8 + fetch batch 8"
  stop_workers
  # v3: + batched writes
  start_workers 1 8 CALIGRAPHY_DB_MAX_CONNS=8 CALIGRAPHY_REDIS_POOL_SIZE=8 \
    CALIGRAPHY_BATCH_WRITES=true CALIGRAPHY_FETCH_BATCH=8
  bench -name "ablation-v3-batching" -type bench.sleep -jobs 1000 -sleep-ms 50 -jitter-ms 20 \
    -note "v3: v2 + batched terminal writes (the fsync-amortization lever)"
  stop_workers
  # Same ladder, CPU-bound workload.
  start_workers 1 1 CALIGRAPHY_DB_MAX_CONNS=1 CALIGRAPHY_REDIS_POOL_SIZE=1 \
    CALIGRAPHY_BATCH_WRITES=false CALIGRAPHY_FETCH_BATCH=1
  bench -name "ablation-cpu-v0-baseline" -type article.analysis -jobs 1000 -text-bytes 4096 \
    -note "v0 baseline, CPU-bound (real TextRank over 4KB)"
  stop_workers
  start_workers 1 4 CALIGRAPHY_DB_MAX_CONNS=8 CALIGRAPHY_REDIS_POOL_SIZE=8 \
    CALIGRAPHY_BATCH_WRITES=true CALIGRAPHY_FETCH_BATCH=8
  bench -name "ablation-cpu-v3-optimized" -type article.analysis -jobs 1000 -text-bytes 4096 \
    -note "optimized, CPU-bound: conc=4 (cores), pools=8, batch=on"
  stop_workers
  ;;
reliability)
  start_api
  start_workers 7 4 CALIGRAPHY_LEASE_TTL=10s
  # Chaos alongside: SIGKILL two workers mid-run and do NOT replace them.
  # The reaper must recover their in-flight jobs (visible afterwards as
  # lease_expired attempt rows) and the run must finish on the surviving
  # five -- degraded capacity is part of what "under load" means here.
  (
    sleep 6
    kill -9 "${worker_pids[2]}" 2>/dev/null || true
    echo "chaos: SIGKILLed worker 3" >&2
    sleep 4
    kill -9 "${worker_pids[5]}" 2>/dev/null || true
    echo "chaos: SIGKILLed worker 6" >&2
  ) &
  chaos_pid=$!
  bench -name "reliability-10k" -type bench.flaky -jobs 10000 \
    -fail-rate 0.05 -perm-rate 0.01 -sleep-ms 20 -submitters 32 -timeout 20m \
    -note "10k jobs, 5% transient + 1% permanent injected failures, 7 workers x conc 4, 2 workers SIGKILLed mid-run"
  wait "$chaos_pid" 2>/dev/null || true
  stop_workers
  ;;
*)
  echo "usage: $0 sweep-io|sweep-cpu|ablation|reliability" >&2
  exit 2
  ;;
esac
