# Benchmarks

Every number in this document comes from a committed result file in
`bench/results/` produced by `forge-bench` against the real system — real
HTTP submission, real Redis dispatch, real Postgres recording, real
workers. Nothing is simulated, sampled, or extrapolated. To reproduce any
row: `make build`, start Postgres + Redis, and run the named
`bench/scripts/matrix.sh` suite.

## Methodology

- **The bench is a pure API client.** It submits through `POST
  /api/v1/jobs` and measures through `GET /api/v1/stats/summary`, so every
  measured job paid the full cost: HTTP parsing, validation, Postgres
  insert, Redis enqueue, claim arbitration, execution, durable recording,
  ack.
- **Percentiles are exact**, computed by `percentile_cont` over every
  completed job's real timestamps — not sampled, not estimated from
  histogram buckets.
- **Wall time is conservative.** The clock stops when a 250ms poll first
  observes full drain, so up to 250ms of overstatement is *included* —
  the error direction that makes throughput look worse, never better.
- **The queue must be empty before a run starts** (the bench refuses
  otherwise), and a run only ends when every submitted job is terminal.
- **Provenance is embedded in every result file**: host cores/memory/
  kernel, API runtime info, fleet shape, git SHA, full scenario config.

**Environment for all runs below** (from the result files): 4-core x86-64
VM, 15GB RAM, Linux 6.18, Go 1.25; Postgres 16.13 and Redis 7.0.15 on the
same host as the API and workers. Single-host numbers — the co-located
API, stores, and fleet all compete for the same four cores, which is
exactly why the CPU-bound curves bend where they do.

## 1. Worker-count sweep — the two curves that justify the architecture

3,000 I/O-bound jobs (50–70ms simulated downstream call) and 1,500
CPU-bound jobs (~18ms of SHA-256 chaining), each at 1 / 2 / 4 / 7 worker
processes × concurrency 4.

| workers | I/O jobs/s | I/O speedup | CPU jobs/s | CPU speedup | CPU exec p50 |
|---:|---:|---:|---:|---:|---:|
| 1 | 54.4 | 1.0× | 108.7 | 1.0× | 38ms |
| 2 | 107.7 | 2.0× | 164.8 | 1.5× | 38ms |
| 4 | 205.2 | 3.8× | 190.9 | 1.8× | 73ms |
| 7 | **370.5** | **6.8×** | 188.2 | 1.7× | **130ms** |

Two findings, both of which the README's scaling claims are built on:

- **I/O-bound work scales almost linearly** — 6.8× at 7 workers (97%
  efficiency) — because waiting workers don't compete for cores.
- **CPU-bound work saturates at core count and then degrades.** From 4→7
  workers, throughput went *down* 1.4% while per-job execution time went
  up 78% (73ms → 130ms): 28 goroutines fighting 4 cores is contention,
  not concurrency. Publishing this turnover is deliberate — a benchmark
  that only shows the rising part of the curve is an advertisement.

## 2. Ablation — what each optimization was actually worth

1,000 I/O-bound jobs, ONE worker process, one lever changed at a time:

| config | jobs/s | vs baseline |
|---|---:|---:|
| v0 — serial baseline (conc 1, pools 1, sync writes, fetch 1) | 16.0 | — |
| v1 — v0 + connection pooling (pg 8, redis 8) | 16.0 | **+0%** |
| v2 — v1 + goroutine concurrency 8 + batched fetch | **123.2** | **+670%** |
| v3 — v2 + batched terminal writes | 106.9 | +568% |

And at fleet scale (7 workers × concurrency 8, 8,000 × 20–30ms jobs):

| config | jobs/s | e2e p50 |
|---|---:|---:|
| fleet, sync writes | 1257.3 | 42ms |
| fleet, batched writes | 1284.6 | 731ms |

Honest readings, including the ones that flatter nothing:

- **Connection pooling alone bought 0%.** A pool of 8 serving a
  concurrency-1 consumer is capacity nothing uses. Pooling is an
  *enabler* for concurrency, not an optimization by itself.
- **Concurrency is the lever**: +670% on I/O-bound work from goroutines
  alone. This is the entire case for Go in this system, measured.
- **Write batching regressed 13% at low write rates** (its flush interval
  adds ack latency the fsync savings can't repay at 100 writes/s) and was
  **throughput-neutral at 1,280 jobs/s** — because on this 4-core host
  the ceiling at that rate is API-side submission, not the write path.
  Batching's fsync-amortization win needs a host where Postgres — not
  ingest — is the bottleneck; on this one it mostly buys e2e latency.
  The lever ships default-on for multi-host deployments but the honest
  single-host verdict is "measure yours".
- **The single-host end-to-end ceiling measured ~1,280 jobs/s** (both
  fleet rows), submission-bound.

## 3. Reliability — 10,000 jobs with injected failure and murdered workers

One run, everything at once: 10,000 `bench.flaky` jobs (deterministic
per-(job, attempt) failure draws: 5% transient rate, 1% permanent rate,
20–30ms work), 7 workers × concurrency 4 with 10s lease TTLs, and **two
workers SIGKILLed mid-run** (at t+6s and t+10s) and never replaced — the
run finished on the surviving five.

| metric | value |
|---|---:|
| submitted | 10,000 |
| completed | **9,904 (99.04%)** |
| failed (injected permanent) | 96 (0.96%) |
| dead-lettered | 0 |
| lost / non-terminal | **0** |
| jobs that needed retries | 485 |
| total attempts | 10,485 |
| wall time | 41.4s |
| throughput under chaos | 239 jobs/s |
| e2e p50 / p95 / p99 | 5.4s / 12.4s / 15.2s |
| exec p50 / p95 / **p99** | 38ms / 75ms / **11.0s** |

The forensic chain for the murdered workers, from `job_attempts`:
**8 jobs** were mid-execution in the two killed processes; the reaper
recorded `lease_expired` attempts for all 8; **all 8 subsequently
completed** on surviving workers. The exec p99 of 11.0s *is* the
crash-recovery latency made visible — those jobs waited out the 10s lease
before the reaper could prove their worker dead. That tail is the price
of crash detection, and it is a tunable (`FORGE_LEASE_TTL`), not a
mystery.

The 96 failures are exactly the injected permanent-failure population
(0.96% observed vs 1% configured, deterministic hash draw): jobs
*designed* to be unprocessable, correctly routed to `FAILED` on attempt 1
without wasting retries. Every transiently-failing job (485 of them)
retried to completion.

## 4. What these numbers do and don't support

Claims these runs support, in the words the runs support:

- *Processed 10,000+ jobs through a distributed Go worker pool with
  99.04% completion under injected failures (5% transient, 1% permanent)
  and two mid-run worker SIGKILLs — zero jobs lost, every job reaching a
  terminal state, crash-recovery visible in per-attempt history.*
- *Scaled I/O-bound throughput 6.8× across 7 worker processes (54 → 370
  jobs/s), and measured the CPU-bound saturation point at core count —
  including the regression past it.*
- *Measured optimizations one lever at a time: goroutine concurrency
  +670% on I/O-bound throughput; connection pooling 0% alone (an enabler,
  not an optimization); write batching negative at low rates and neutral
  at the measured 1,280 jobs/s single-host ceiling.*

Claims these runs do **not** support, so the README must not make them:

- Any specific throughput on other hardware (single 4-core host, co-located
  stores; the ceiling was submission-side).
- "Exactly-once" anything. 485 jobs ran more than once by design; the
  system's guarantee is at-least-once execution with exactly-once result
  persistence (fenced writes).
- Batching as a universal win — on this host it wasn't, and the numbers
  say so.
