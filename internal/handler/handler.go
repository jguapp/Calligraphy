// Package handler defines the contract between Caligraphy's engine and the
// business logic it executes, and the registry that binds job types to
// implementations.
//
// The interface is one method on purpose. The engine knows how to claim,
// lease, record, retry, and ack; it knows nothing about what any job
// *does*. A handler knows what its job does and nothing about queues.
// Everything a handler needs to influence the engine travels through its
// return values: a result to persist, or an error whose classification
// (see job.NonRetryable) steers routing.
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jguapp/caligraphy/internal/job"
)

// Handler executes one job attempt. Implementations MUST:
//
//   - respect ctx: it carries the execution timeout, shutdown drain, and
//     cooperative cancellation, and a handler that ignores it holds a
//     worker slot hostage;
//   - be idempotent where they have external side effects: Caligraphy delivers
//     at-least-once, so a handler may run more than once for the same job
//     (see the README's idempotency section for what Caligraphy does and does
//     not guarantee).
//
// The returned result is persisted verbatim on success (nil is fine).
type Handler interface {
	Handle(ctx context.Context, j *job.Job) (json.RawMessage, error)
}

// Func adapts a plain function to Handler, for handlers with no state.
type Func func(ctx context.Context, j *job.Job) (json.RawMessage, error)

func (f Func) Handle(ctx context.Context, j *job.Job) (json.RawMessage, error) {
	return f(ctx, j)
}

// Options are per-type engine knobs. Zero values defer to the worker's
// configured defaults.
type Options struct {
	// LeaseTTL overrides how long this type's claims live between
	// heartbeats. Raise it for types whose *crash detection* can afford to
	// be slower; job duration is NOT the reason to raise it (heartbeats
	// renew the lease for as long as the handler genuinely runs).
	LeaseTTL time.Duration
	// ExecTimeout bounds one execution of this type.
	ExecTimeout time.Duration
	// MaxAttempts caps attempts for jobs submitted without an explicit max.
	MaxAttempts int
}

// Registration binds a type name to its handler and options.
type Registration struct {
	Type    string
	Handler Handler
	Options Options
}

// Registry is a plain map with validation. It is populated at startup and
// read-only afterwards, so it needs no lock -- and keeping it that way is
// why Register returns errors instead of being safe to call at runtime.
type Registry struct {
	m map[string]Registration
}

func NewRegistry() *Registry {
	return &Registry{m: make(map[string]Registration)}
}

func (r *Registry) Register(reg Registration) error {
	if !job.ValidName(reg.Type) {
		return fmt.Errorf("handler: invalid type name %q", reg.Type)
	}
	if reg.Handler == nil {
		return fmt.Errorf("handler: nil handler for %q", reg.Type)
	}
	if _, dup := r.m[reg.Type]; dup {
		return fmt.Errorf("handler: duplicate registration for %q", reg.Type)
	}
	r.m[reg.Type] = reg
	return nil
}

// MustRegister panics on error; registration happens at startup where a
// bad registration is a programming error, not a runtime condition.
func (r *Registry) MustRegister(reg Registration) {
	if err := r.Register(reg); err != nil {
		panic(err)
	}
}

func (r *Registry) Get(jobType string) (Registration, bool) {
	reg, ok := r.m[jobType]
	return reg, ok
}

// Types returns every registered type, for API-side submission validation.
func (r *Registry) Types() []string {
	out := make([]string, 0, len(r.m))
	for t := range r.m {
		out = append(out, t)
	}
	return out
}
