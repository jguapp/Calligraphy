// Package bench holds the handlers the benchmark and failure-injection
// suites drive. They live in the normal registry -- the benchmark
// exercises the real pipeline end to end, not a special path -- but they
// exist for measurement, and their contract is precision:
//
//   - bench.sleep  simulates I/O-bound work (a downstream call of known
//     latency). Throughput on this workload should scale past core count,
//     because workers spend the time parked on a timer, not competing for
//     CPU.
//   - bench.cpu    is genuinely CPU-bound (SHA-256 chaining). Throughput
//     should plateau at roughly core count and then *degrade* with more
//     workers -- and the benchmark publishing that turnover, rather than
//     hiding it, is the point.
//   - bench.flaky  fails deterministically at configured rates, keyed on
//     (job id, attempt) -- so a retry usually succeeds, like real
//     transients, and reliability runs are reproducible bit-for-bit.
package bench

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"time"

	"github.com/jguapp/caligraphy/internal/handler"
	"github.com/jguapp/caligraphy/internal/job"
)

const (
	TypeSleep = "bench.sleep"
	TypeCPU   = "bench.cpu"
	TypeFlaky = "bench.flaky"
)

// RegisterAll adds the three bench handlers to a registry.
func RegisterAll(r *handler.Registry) {
	r.MustRegister(handler.Registration{Type: TypeSleep, Handler: handler.Func(handleSleep)})
	r.MustRegister(handler.Registration{Type: TypeCPU, Handler: handler.Func(handleCPU)})
	r.MustRegister(handler.Registration{Type: TypeFlaky, Handler: handler.Func(handleFlaky)})
}

// ------------------------------------------------------------------- sleep

type sleepPayload struct {
	Ms       int `json:"ms"`
	JitterMs int `json:"jitterMs"`
}

func handleSleep(ctx context.Context, j *job.Job) (json.RawMessage, error) {
	var p sleepPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return nil, job.NonRetryable(fmt.Errorf("bench.sleep: bad payload: %w", err))
	}
	if p.Ms <= 0 {
		p.Ms = 50
	}
	d := time.Duration(p.Ms) * time.Millisecond
	if p.JitterMs > 0 {
		d += time.Duration(rand.IntN(p.JitterMs)) * time.Millisecond
	}
	// A sleep that ignores cancellation would hold a drain hostage for
	// its full duration -- the exact bug the worker's shutdown tests
	// exist to catch. Timer + select, never time.Sleep.
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return json.RawMessage(fmt.Sprintf(`{"sleptMs":%d}`, d.Milliseconds())), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// --------------------------------------------------------------------- cpu

type cpuPayload struct {
	Iterations int `json:"iterations"`
}

func handleCPU(ctx context.Context, j *job.Job) (json.RawMessage, error) {
	var p cpuPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return nil, job.NonRetryable(fmt.Errorf("bench.cpu: bad payload: %w", err))
	}
	if p.Iterations <= 0 {
		p.Iterations = 100_000
	}
	// SHA-256 chaining: unfakeable work (each round depends on the last,
	// so nothing can be hoisted or vectorized away) with a knob that maps
	// linearly to CPU time.
	sum := sha256.Sum256([]byte(j.ID))
	for i := 0; i < p.Iterations; i++ {
		sum = sha256.Sum256(sum[:])
		// Cancellation check amortized to ~every millisecond of work, so
		// a drain isn't blocked by a long iteration count.
		if i%50_000 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return json.RawMessage(fmt.Sprintf(`{"iterations":%d,"digest":%q}`,
		p.Iterations, hex.EncodeToString(sum[:8]))), nil
}

// ------------------------------------------------------------------- flaky

type flakyPayload struct {
	// FailRate is the per-attempt probability of a transient failure.
	FailRate float64 `json:"failRate"`
	// PermanentRate is the per-JOB probability of being permanently
	// unprocessable (fails every attempt, non-retryably).
	PermanentRate float64 `json:"permanentRate"`
	SleepMs       int     `json:"sleepMs"`
}

// handleFlaky's draws are deterministic hashes, not rand calls:
// hash(jobID) decides permanence, hash(jobID, attempt) decides each
// transient draw. Two properties fall out, both load-bearing for honest
// benchmarks: the same submitted workload fails identically on every run,
// and a retry re-rolls (different attempt, different hash) so transients
// usually clear -- which is what real transients do.
func handleFlaky(ctx context.Context, j *job.Job) (json.RawMessage, error) {
	var p flakyPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return nil, job.NonRetryable(fmt.Errorf("bench.flaky: bad payload: %w", err))
	}
	if p.SleepMs > 0 {
		t := time.NewTimer(time.Duration(p.SleepMs) * time.Millisecond)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if draw("permanent", j.ID, 0) < p.PermanentRate {
		return nil, job.NonRetryable(fmt.Errorf("bench.flaky: job %s is permanently unprocessable (injected)", j.ID))
	}
	if draw("transient", j.ID, j.AttemptCount) < p.FailRate {
		return nil, fmt.Errorf("bench.flaky: injected transient failure (attempt %d)", j.AttemptCount)
	}
	return json.RawMessage(fmt.Sprintf(`{"attempt":%d}`, j.AttemptCount)), nil
}

// draw maps (label, id, attempt) to a uniform-ish [0,1) via FNV-1a.
func draw(label, id string, attempt int) float64 {
	h := fnv.New64a()
	h.Write([]byte(label))
	h.Write([]byte(id))
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(attempt))
	h.Write(b[:])
	return float64(h.Sum64()%100_000) / 100_000
}
