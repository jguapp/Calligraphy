// caligraphy-bench generates load against a running Caligraphy deployment and
// writes a result file with everything needed to believe -- or contest --
// the numbers.
//
// Honesty rules, enforced by construction:
//
//   - The bench is a pure API client. It submits through POST /api/v1/jobs
//     and measures through GET /api/v1/stats/summary, so it exercises the
//     real pipeline end to end -- HTTP parsing, idempotency checks, Redis
//     handoff, claim arbitration, recording -- not a private fast path.
//   - Every result embeds its provenance: host cores/memory/kernel, the
//     API's own runtime info, worker fleet shape at run time, git SHA, and
//     the full scenario config. A number without its environment is a
//     rumor.
//   - Nothing is sampled or approximated: percentiles come from
//     percentile_cont over every completed job's real timestamps.
//
// The wall clock stops when the summary shows no non-terminal jobs left;
// with a 250ms poll that overstates wall time by up to 250ms, which is
// recorded rather than subtracted (conservative in the direction that
// makes throughput look WORSE, never better).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type flags struct {
	api, token, name, out string
	jobs, submitters      int
	jobType               string
	sleepMs, jitterMs     int
	cpuIters              int
	textBytes             int
	failRate, permRate    float64
	timeout               time.Duration
	sha                   string
	note                  string
}

func main() {
	var f flags
	flag.StringVar(&f.api, "api", envOr("CALIGRAPHY_API_URL", "http://127.0.0.1:8080"), "caligraphy-api base URL")
	flag.StringVar(&f.token, "token", os.Getenv("CALIGRAPHY_API_TOKEN"), "bearer token")
	flag.StringVar(&f.name, "name", "adhoc", "scenario name (goes in the result filename)")
	flag.StringVar(&f.out, "out", "bench/results", "directory for result JSON")
	flag.IntVar(&f.jobs, "jobs", 1000, "number of jobs to submit")
	flag.IntVar(&f.submitters, "submitters", 16, "concurrent submitting goroutines")
	flag.StringVar(&f.jobType, "type", "bench.sleep", "bench.sleep | bench.cpu | bench.flaky | article.analysis")
	flag.IntVar(&f.sleepMs, "sleep-ms", 50, "bench.sleep: base duration")
	flag.IntVar(&f.jitterMs, "jitter-ms", 20, "bench.sleep: added uniform jitter")
	flag.IntVar(&f.cpuIters, "cpu-iters", 200_000, "bench.cpu: hash iterations")
	flag.IntVar(&f.textBytes, "text-bytes", 4096, "article.analysis: synthetic article size")
	flag.Float64Var(&f.failRate, "fail-rate", 0.05, "bench.flaky: per-attempt transient failure rate")
	flag.Float64Var(&f.permRate, "perm-rate", 0.01, "bench.flaky: per-job permanent failure rate")
	flag.DurationVar(&f.timeout, "timeout", 15*time.Minute, "give up waiting for drain")
	flag.StringVar(&f.sha, "sha", gitSHA(), "git revision recorded in the result")
	flag.StringVar(&f.note, "note", "", "free-form note recorded in the result (e.g. 'baseline: conc=1 pools=1 batch=off')")
	flag.Parse()

	if err := run(f); err != nil {
		fmt.Fprintln(os.Stderr, "caligraphy-bench:", err)
		os.Exit(1)
	}
}

type client struct {
	base, token string
	http        *http.Client
}

func (c *client) do(method, path string, body []byte, out any) error {
	req, err := http.NewRequest(method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s -> %d: %s", method, path, resp.StatusCode, truncate(raw, 200))
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func run(f flags) error {
	c := &client{base: f.api, token: f.token, http: &http.Client{Timeout: 30 * time.Second}}

	// Refuse to measure a dirty room: leftover backlog from a previous
	// run would pollute both throughput and latency.
	var depths struct {
		Ready    map[string]int64 `json:"ready"`
		Delayed  map[string]int64 `json:"delayed"`
		InFlight int64            `json:"inFlight"`
	}
	if err := c.do("GET", "/api/v1/queues/depths", nil, &depths); err != nil {
		return fmt.Errorf("preflight: %w", err)
	}
	var backlog int64
	for _, v := range depths.Ready {
		backlog += v
	}
	for _, v := range depths.Delayed {
		backlog += v
	}
	if backlog+depths.InFlight > 0 {
		return fmt.Errorf("queue is not empty (ready+delayed=%d inflight=%d); refusing to measure a dirty run", backlog, depths.InFlight)
	}

	payload := buildPayload(f)
	fmt.Printf("scenario %q: %d × %s via %d submitters\n", f.name, f.jobs, f.jobType, f.submitters)
	fmt.Printf("payload: %s\n", truncate([]byte(payload), 120))

	// ---- submit phase -------------------------------------------------
	since := time.Now().UTC().Add(-2 * time.Second) // clock-skew slack for the summary window
	submitStart := time.Now()
	var submitted, submitErrors atomic.Int64
	var wg sync.WaitGroup
	per := f.jobs / f.submitters
	extra := f.jobs % f.submitters
	for w := 0; w < f.submitters; w++ {
		n := per
		if w < extra {
			n++
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			body := []byte(fmt.Sprintf(`{"type":%q,"payload":%s}`, f.jobType, payload))
			for i := 0; i < n; i++ {
				if err := c.do("POST", "/api/v1/jobs", body, nil); err != nil {
					submitErrors.Add(1)
					continue
				}
				submitted.Add(1)
			}
		}(n)
	}
	wg.Wait()
	submitDur := time.Since(submitStart)
	fmt.Printf("submitted %d (%d errors) in %.2fs (%.0f/s)\n",
		submitted.Load(), submitErrors.Load(), submitDur.Seconds(), float64(submitted.Load())/submitDur.Seconds())

	// ---- drain phase --------------------------------------------------
	deadline := time.Now().Add(f.timeout)
	var sum summary
	lastPrint := time.Now()
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for drain; last summary: %+v", sum.Counts)
		}
		time.Sleep(250 * time.Millisecond)
		// A fresh struct every poll: unmarshal into the shared one would
		// leave stale counts behind on a partial response, and the drain
		// decision must never run on yesterday's numbers.
		var cur summary
		if err := c.do("GET", "/api/v1/stats/summary?since="+since.Format(time.RFC3339), nil, &cur); err != nil {
			fmt.Printf("  poll error (will retry): %v\n", err)
			continue
		}
		sum = cur
		pending := sum.Counts["PENDING"] + sum.Counts["RUNNING"] + sum.Counts["RETRYING"]
		terminal := sum.Counts["COMPLETED"] + sum.Counts["FAILED"] + sum.Counts["DEAD_LETTER"] + sum.Counts["CANCELLED"]
		if time.Since(lastPrint) > 2*time.Second {
			fmt.Printf("  … %d terminal, %d in progress %v\n", terminal, pending, sum.Counts)
			lastPrint = time.Now()
		}
		if terminal >= submitted.Load() && pending == 0 {
			break
		}
	}
	wall := time.Since(submitStart)

	// ---- gather -------------------------------------------------------
	var meta map[string]any
	c.do("GET", "/api/v1/meta", nil, &meta) //nolint:errcheck // provenance is best-effort
	var workers struct {
		Workers []struct {
			ID                string `json:"id"`
			TargetConcurrency int    `json:"targetConcurrency"`
			State             string `json:"state"`
			Processed         int64  `json:"processed"`
		} `json:"workers"`
	}
	c.do("GET", "/api/v1/workers", nil, &workers) //nolint:errcheck

	activeWorkers := 0
	fleet := []map[string]any{}
	for _, w := range workers.Workers {
		if w.State == "active" {
			activeWorkers++
			fleet = append(fleet, map[string]any{
				"id": w.ID, "targetConcurrency": w.TargetConcurrency, "processed": w.Processed,
			})
		}
	}

	completed := sum.Counts["COMPLETED"]
	result := map[string]any{
		"name":      f.name,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"gitSHA":    f.sha,
		"note":      f.note,
		"environment": map[string]any{
			"host": hostInfo(),
			"api":  meta,
		},
		"scenario": map[string]any{
			"jobs": f.jobs, "type": f.jobType, "submitters": f.submitters,
			"payload": json.RawMessage(payload),
		},
		"fleet": map[string]any{"activeWorkers": activeWorkers, "workers": fleet},
		"submit": map[string]any{
			"durationSec": round2(submitDur.Seconds()),
			"jobsPerSec":  round2(float64(submitted.Load()) / submitDur.Seconds()),
			"errors":      submitErrors.Load(),
		},
		"run": map[string]any{
			"wallSec":              round2(wall.Seconds()),
			"wallNote":             "submit start -> drain detected; poll adds <=250ms (recorded, not subtracted)",
			"throughputJobsPerSec": round2(float64(completed) / wall.Seconds()),
			"counts":               sum.Counts,
			"retriedJobs":          sum.RetriedJobs,
			"totalAttempts":        sum.TotalAttempts,
			"e2eSeconds":           sum.E2ESeconds,
			"execSeconds":          sum.ExecSeconds,
		},
		"verdict": map[string]any{
			"submitted":      submitted.Load(),
			"completed":      completed,
			"failed":         sum.Counts["FAILED"],
			"deadLettered":   sum.Counts["DEAD_LETTER"],
			"cancelled":      sum.Counts["CANCELLED"],
			"completionRate": round4(float64(completed) / float64(submitted.Load())),
			"allTerminal":    true,
		},
	}

	if err := os.MkdirAll(f.out, 0o755); err != nil {
		return err
	}
	fname := filepath.Join(f.out, fmt.Sprintf("%s-%s.json",
		time.Now().UTC().Format("20060102-150405"), f.name))
	blob, _ := json.MarshalIndent(result, "", "  ")
	if err := os.WriteFile(fname, blob, 0o644); err != nil {
		return err
	}

	fmt.Printf(`
── %s ──────────────────────────────────────────────
  wall            %.2fs        throughput   %.1f jobs/s
  completed       %d/%d (%.2f%%)
  failed          %d permanent, %d dead-lettered
  retried jobs    %d (%d total attempts for %d jobs)
  e2e latency     p50 %.0fms   p95 %.0fms   p99 %.0fms
  exec latency    p50 %.0fms   p95 %.0fms   p99 %.0fms
  fleet           %d active workers
  result          %s
`, f.name, wall.Seconds(), float64(completed)/wall.Seconds(),
		completed, submitted.Load(), 100*float64(completed)/float64(submitted.Load()),
		sum.Counts["FAILED"], sum.Counts["DEAD_LETTER"],
		sum.RetriedJobs, sum.TotalAttempts, submitted.Load(),
		sum.E2ESeconds.P50*1000, sum.E2ESeconds.P95*1000, sum.E2ESeconds.P99*1000,
		sum.ExecSeconds.P50*1000, sum.ExecSeconds.P95*1000, sum.ExecSeconds.P99*1000,
		activeWorkers, fname)
	return nil
}

type pct struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

type summary struct {
	Counts        map[string]int64 `json:"counts"`
	RetriedJobs   int64            `json:"retriedJobs"`
	TotalAttempts int64            `json:"totalAttempts"`
	E2ESeconds    pct              `json:"e2eSeconds"`
	ExecSeconds   pct              `json:"execSeconds"`
}

func buildPayload(f flags) string {
	switch f.jobType {
	case "bench.sleep":
		return fmt.Sprintf(`{"ms":%d,"jitterMs":%d}`, f.sleepMs, f.jitterMs)
	case "bench.cpu":
		return fmt.Sprintf(`{"iterations":%d}`, f.cpuIters)
	case "bench.flaky":
		return fmt.Sprintf(`{"failRate":%g,"permanentRate":%g,"sleepMs":%d}`, f.failRate, f.permRate, f.sleepMs)
	case "article.analysis":
		return fmt.Sprintf(`{"articleId":"bench","text":%q}`, syntheticArticle(f.textBytes))
	default:
		return `{}`
	}
}

// syntheticArticle builds deterministic prose of roughly n bytes -- real
// sentences with real structure, so TextRank has actual work to do
// instead of degenerating on repeated filler.
func syntheticArticle(n int) string {
	sentences := []string{
		"Distributed systems fail in ways that single machines never see.",
		"A queue that cannot survive a crash is a list with ambitions.",
		"The database records what happened, and the stream moves what happens next.",
		"Backpressure is not a feature you add but a property you refuse to lose.",
		"Every retry policy is a bet about the shape of failure.",
		"Leases expire because workers cannot be trusted to say goodbye.",
		"A fencing token turns a race into a decision made exactly once.",
		"Idempotency is the tax that at-least-once delivery collects.",
		"Observability means the dashboard answers the question you did not think to ask.",
		"Throughput without a latency distribution is a marketing number.",
	}
	var b strings.Builder
	for i := 0; b.Len() < n; i++ {
		b.WriteString(sentences[i%len(sentences)])
		b.WriteByte(' ')
		if i%4 == 3 {
			b.WriteString("\n\n")
		}
	}
	return b.String()
}

func hostInfo() map[string]any {
	info := map[string]any{
		"goVersion": runtime.Version(),
		"numCPU":    runtime.NumCPU(),
		"goos":      runtime.GOOS,
		"goarch":    runtime.GOARCH,
	}
	if out, err := exec.Command("uname", "-r").Output(); err == nil {
		info["kernel"] = strings.TrimSpace(string(out))
	}
	if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				info["memTotal"] = strings.TrimSpace(strings.TrimPrefix(line, "MemTotal:"))
				break
			}
		}
	}
	return info
}

func gitSHA() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

func round2(f float64) float64 { return float64(int(f*100)) / 100 }
func round4(f float64) float64 { return float64(int(f*10000)) / 10000 }
