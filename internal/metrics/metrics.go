// Package metrics owns every Prometheus series Caligraphy exposes. One place,
// so the metric names in the Grafana dashboard, the README, and the code
// cannot drift apart -- the names here ARE the contract.
//
// Two registries exist in practice: the API's (submission counters + the
// queue-depth collector) and each worker's (execution counters, duration
// histograms, pool gauges). Prometheus scrapes them all and the labels
// keep them apart.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jguapp/caligraphy/internal/queue"
)

// Metrics is the full series set. Fields are nil-safe to ignore; a
// process registers only what it observes.
type Metrics struct {
	reg *prometheus.Registry

	JobsSubmitted    *prometheus.CounterVec
	JobsCompleted    *prometheus.CounterVec
	JobsFailed       *prometheus.CounterVec
	JobsRetried      *prometheus.CounterVec
	JobsDeadLettered *prometheus.CounterVec
	JobsCancelled    *prometheus.CounterVec

	// ExecDuration is time inside the handler; E2EDuration is enqueue to
	// completion, queue wait included. The GAP between them is the
	// backpressure signal -- it's the queue wait made visible, which is
	// why both exist rather than one.
	ExecDuration *prometheus.HistogramVec
	E2EDuration  *prometheus.HistogramVec

	ClaimsSkipped prometheus.Counter
	WritesFenced  prometheus.Counter
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		reg: reg,
		JobsSubmitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "caligraphy_jobs_submitted_total",
			Help: "Jobs accepted by the API, by type and queue.",
		}, []string{"type", "queue"}),
		JobsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "caligraphy_jobs_completed_total",
			Help: "Jobs that reached COMPLETED.",
		}, []string{"type"}),
		JobsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "caligraphy_jobs_failed_total",
			Help: "Attempts that ended in a permanent (non-retryable) failure.",
		}, []string{"type"}),
		JobsRetried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "caligraphy_jobs_retried_total",
			Help: "Attempts that ended in a retry being scheduled.",
		}, []string{"type"}),
		JobsDeadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "caligraphy_jobs_dead_lettered_total",
			Help: "Jobs parked in the DLQ after exhausting attempts.",
		}, []string{"type"}),
		JobsCancelled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "caligraphy_jobs_cancelled_total",
			Help: "Jobs cancelled cooperatively mid-run.",
		}, []string{"type"}),
		ExecDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "caligraphy_job_duration_seconds",
			Help: "Handler execution time (excludes queue wait).",
			// 1ms .. ~2 minutes; the workloads span article analysis
			// (tens of ms) to slow external callbacks.
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 18),
		}, []string{"type"}),
		E2EDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "caligraphy_job_e2e_duration_seconds",
			Help:    "Enqueue-to-completion time (includes queue wait).",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 18),
		}, []string{"type"}),
		ClaimsSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "caligraphy_claims_skipped_total",
			Help: "Deliveries that lost claim arbitration (duplicate/stale) and were acked away.",
		}),
		WritesFenced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "caligraphy_writes_fenced_total",
			Help: "Terminal writes rejected by the lease-epoch fence.",
		}),
	}
	reg.MustRegister(
		m.JobsSubmitted, m.JobsCompleted, m.JobsFailed, m.JobsRetried,
		m.JobsDeadLettered, m.JobsCancelled, m.ExecDuration, m.E2EDuration,
		m.ClaimsSkipped, m.WritesFenced,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

func (m *Metrics) MustRegister(cs ...prometheus.Collector) {
	m.reg.MustRegister(cs...)
}

// ---------------------------------------------------------- worker gauges

// PoolStats is what the worker exposes about its pool -- an interface so
// metrics doesn't import worker (worker already imports the world).
type PoolStats interface {
	Active() int
	Target() int
	Processed() uint64
}

// RegisterWorkerGauges wires the live pool numbers as gauges. Utilization
// is computed at scrape time so it can never drift from its inputs.
func (m *Metrics) RegisterWorkerGauges(workerID string, p PoolStats) {
	labels := prometheus.Labels{"worker": workerID}
	m.reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "caligraphy_worker_active_jobs", Help: "Jobs executing right now.", ConstLabels: labels,
		}, func() float64 { return float64(p.Active()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "caligraphy_worker_target_concurrency", Help: "Current concurrency target.", ConstLabels: labels,
		}, func() float64 { return float64(p.Target()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "caligraphy_worker_utilization", Help: "active / target, 0..1.", ConstLabels: labels,
		}, func() float64 {
			t := p.Target()
			if t == 0 {
				return 0
			}
			return float64(p.Active()) / float64(t)
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "caligraphy_worker_processed_total", Help: "Deliveries finished by this worker.", ConstLabels: labels,
		}, func() float64 { return float64(p.Processed()) }),
	)
}

// ---------------------------------------------------------- the observer

// Observer adapts Metrics to the worker.Observer interface without the
// worker package importing prometheus. One method per route, matching the
// runner's actual routing -- so "retried" counts scheduled retries and
// nothing else.
type Observer struct{ M *Metrics }

func (o Observer) JobStarted(jobType string) {}

func (o Observer) JobCompleted(jobType string, execSeconds, e2eSeconds float64) {
	o.M.JobsCompleted.WithLabelValues(jobType).Inc()
	o.M.ExecDuration.WithLabelValues(jobType).Observe(execSeconds)
	o.M.E2EDuration.WithLabelValues(jobType).Observe(e2eSeconds)
}

func (o Observer) JobFailed(jobType string)  { o.M.JobsFailed.WithLabelValues(jobType).Inc() }
func (o Observer) JobRetried(jobType string) { o.M.JobsRetried.WithLabelValues(jobType).Inc() }
func (o Observer) JobDeadLettered(jobType string) {
	o.M.JobsDeadLettered.WithLabelValues(jobType).Inc()
}
func (o Observer) JobCancelled(jobType string) { o.M.JobsCancelled.WithLabelValues(jobType).Inc() }
func (o Observer) ClaimSkipped()               { o.M.ClaimsSkipped.Inc() }
func (o Observer) WriteFenced()                { o.M.WritesFenced.Inc() }

// ---------------------------------------------------- queue depth scrape

// DepthCollector reads live queue depths at scrape time. A collector
// rather than a polled gauge: the number is Redis's, and caching it in a
// gauge would add a staleness window for zero benefit.
type DepthCollector struct {
	Q *queue.Queue

	descReady    *prometheus.Desc
	descDelayed  *prometheus.Desc
	descInflight *prometheus.Desc
	descDLQ      *prometheus.Desc
}

func NewDepthCollector(q *queue.Queue) *DepthCollector {
	ql := []string{"queue", "priority"}
	return &DepthCollector{
		Q:            q,
		descReady:    prometheus.NewDesc("caligraphy_queue_depth", "Entries ready to claim.", ql, prometheus.Labels{"state": "ready"}),
		descDelayed:  prometheus.NewDesc("caligraphy_queue_delayed_depth", "Envelopes waiting on backoff/schedule.", ql, nil),
		descInflight: prometheus.NewDesc("caligraphy_queue_inflight", "Claimed, unacked entries.", []string{"queue"}, nil),
		descDLQ:      prometheus.NewDesc("caligraphy_queue_dlq_depth", "Entries in the DLQ log stream.", []string{"queue"}, nil),
	}
}

func (c *DepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.descReady
	ch <- c.descDelayed
	ch <- c.descInflight
	ch <- c.descDLQ
}

func (c *DepthCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d, err := c.Q.Depths(ctx)
	if err != nil {
		return // a failed scrape shows as absent series, which is honest
	}
	name := c.Q.QueueName()
	for prio, n := range d.Ready {
		ch <- prometheus.MustNewConstMetric(c.descReady, prometheus.GaugeValue, float64(n), name, string(prio))
	}
	for prio, n := range d.Delayed {
		ch <- prometheus.MustNewConstMetric(c.descDelayed, prometheus.GaugeValue, float64(n), name, string(prio))
	}
	ch <- prometheus.MustNewConstMetric(c.descInflight, prometheus.GaugeValue, float64(d.InFlight), name)
	ch <- prometheus.MustNewConstMetric(c.descDLQ, prometheus.GaugeValue, float64(d.DLQ), name)
}
