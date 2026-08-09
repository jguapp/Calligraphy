# Kubernetes manifests

Apply order: `namespace` → `config` → `postgres` + `redis` → `api` →
`worker` → `hpa`.

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl create secret generic forge-secrets -n forge \
  --from-literal=api-tokens="$(openssl rand -hex 24)" \
  --from-literal=database-url="postgres://forge:forge@postgres:5432/forge?sslmode=disable"
kubectl apply -f deploy/k8s/
```

Honesty about scope: the Postgres and Redis manifests here are
**dev-grade** (single replica, small PVCs, no operator, no backups) —
they exist so the whole platform runs in a cluster with nothing external.
A production deployment should point `database-url` at managed Postgres
and `FORGE_REDIS_ADDR` at managed Redis and delete those two files; the
Forge deployments themselves are the production-shaped part (probes,
resources, graceful drain, anti-affinity hint).

Scaling is two composable axes:

- **Vertical, per pod**: `FORGE_AUTOSCALE=true` lets each worker move its
  own concurrency between min/max from queue depth (measured +670% for
  I/O-bound work — see `bench/BENCHMARKS.md`).
- **Horizontal, pods**: `hpa.yaml` ships CPU-based (works everywhere).
  The queue-depth-driven variant is included commented-out: it needs
  `forge_queue_depth` exposed to the HPA through prometheus-adapter,
  which is an extra install — the comment shows the exact
  seriesQuery/metricsQuery pair to configure when you have it.

Worker termination is tuned to the drain: `terminationGracePeriodSeconds`
exceeds `FORGE_DRAIN_TIMEOUT`, so a rolling update lets in-flight jobs
finish rather than relying on the reaper to mop up (which would also
work — that's the point of it — but gracefully is cheaper).
