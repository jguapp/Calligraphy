// Package config loads Calligraphy's runtime configuration from the environment.
//
// One flat struct, loaded once at startup, passed by value to whatever needs
// it. No config files, no watchers, no framework: every deployment target
// this project has (compose, Kubernetes, a bare shell) already speaks
// environment variables, and a knob that can change mid-flight is a knob
// whose every reader needs a synchronization story.
//
// Several fields exist specifically as *benchmark ablation levers* -- the
// baseline-vs-optimized comparison in bench/ flips them (pool sizes, write
// batching, prefetch, dynamic scaling) so each optimization's contribution
// is measurable on its own. Those are marked "ablation" below.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// --- shared infrastructure ---

	// DatabaseURL is the Postgres DSN (pgx format). Postgres is the source
	// of truth for job state; Redis is only the transport.
	DatabaseURL string
	// RedisAddr is host:port, or a full redis:// URL.
	RedisAddr string
	// KeyPrefix namespaces every Redis key so a shared Redis (Booklet's TTS
	// cache lives in the same instance in compose) can't collide.
	KeyPrefix string
	// Queue is the queue this process serves (workers) or defaults
	// submissions to (API).
	Queue string

	// --- store ---

	// DBMaxConns caps the pgx pool. Ablation: baseline runs with 1.
	DBMaxConns int
	// BatchWrites coalesces terminal-state writes and attempt/event inserts
	// into periodic multi-row flushes instead of one round trip each.
	// Ablation: the single biggest write-amplification lever.
	BatchWrites bool
	// BatchMaxSize / BatchInterval bound a flush: whichever fills first.
	BatchMaxSize  int
	BatchInterval time.Duration

	// --- queue ---

	// RedisPoolSize caps go-redis's connection pool. Ablation: baseline 1.
	RedisPoolSize int

	// --- API server ---

	HTTPAddr string
	GRPCAddr string
	// APITokens authenticates submitters (Bearer). Comma-separated. Empty
	// means auth is DISABLED -- acceptable for local dev only, and the
	// server logs loudly at startup when it is.
	APITokens []string
	// MaxPayloadBytes bounds accepted payloads (never above job.MaxPayloadBytes).
	MaxPayloadBytes int

	// --- worker ---

	// WorkerID identifies this worker in claims, heartbeats, and metrics.
	// Defaults to hostname-pid.
	WorkerID string
	// Concurrency is the number of jobs a worker runs at once at startup.
	// With autoscaling on, the live target moves within [MinConcurrency,
	// MaxConcurrency]; with it off, Concurrency is fixed. Ablation.
	Concurrency    int
	MinConcurrency int
	MaxConcurrency int
	// AutoscaleEnabled turns on queue-depth-driven concurrency. Ablation.
	AutoscaleEnabled bool
	// FetchBatch caps how many jobs one XREADGROUP claims. Claiming more
	// than free slots is never allowed regardless. Ablation: baseline 1.
	FetchBatch int
	// FetchBlock is how long a fetch with no ready work blocks server-side
	// before the loop re-checks shutdown/resize.
	FetchBlock time.Duration
	// LeaseTTL is how long a claim lives without renewal before the reaper
	// may steal the job. Sized to crash-detection latency, NOT to job
	// duration -- running handlers renew at LeaseTTL/3.
	LeaseTTL time.Duration
	// ExecTimeout bounds a single handler execution (per-type overrides via
	// handler options).
	ExecTimeout time.Duration
	// DrainTimeout is how long shutdown waits for in-flight jobs before
	// cancelling their contexts and (briefly) waiting again.
	DrainTimeout time.Duration
	// MetricsAddr serves the worker's own /metrics + /healthz.
	MetricsAddr string
	// ControlAddr is the API's gRPC endpoint for the control-plane stream.
	// Empty disables the control connection (worker still works fully).
	ControlAddr string

	// --- retry policy ---

	// RetryBase / RetryCap parameterize full-jitter exponential backoff:
	// delay = rand(0, min(cap, base * 2^(attempt-1))).
	RetryBase time.Duration
	RetryCap  time.Duration

	// --- handlers ---

	// CallbackHMACSecret signs http.callback deliveries (X-Calligraphy-Signature).
	// Empty disables signing (the header is omitted).
	CallbackHMACSecret string
	// CallbackAllowedHosts, when non-empty, restricts http.callback targets
	// to these hostnames. Empty allows any target: unlike Booklet's
	// user-supplied webhook URLs, callback URLs here come from an
	// authenticated service holding an API token, and the common target is
	// a private address (the submitting app on the same compose network) --
	// so private-IP blocking would break the primary use case. The
	// allowlist exists for anyone running Calligraphy closer to untrusted input.
	CallbackAllowedHosts []string

	// --- recovery (leader duties, run inside calligraphy-api) ---

	// ReapMinIdle is the Redis-side idle threshold for XAUTOCLAIM; keep it
	// >= LeaseTTL or healthy-but-slow-to-renew workers get robbed.
	ReapMinIdle time.Duration
	// SweepGrace is how far past lease expiry the DB-side sweep waits
	// before declaring a RUNNING row abandoned (covers the ack-lost case).
	SweepGrace time.Duration
	// OrphanAge is how old an unenqueued PENDING row must be before the
	// sweep re-enqueues it (covers the enqueue-failed case).
	OrphanAge time.Duration
}

// Load reads the environment, applies defaults, and validates. It returns
// an error rather than exiting so main() owns the process's death.
func Load() (Config, error) {
	host, _ := os.Hostname()
	if host == "" {
		host = "calligraphy"
	}

	c := Config{
		DatabaseURL: getenv("CALLIGRAPHY_DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:5432/calligraphy?sslmode=disable"),
		RedisAddr:   getenv("CALLIGRAPHY_REDIS_ADDR", "127.0.0.1:6379"),
		KeyPrefix:   getenv("CALLIGRAPHY_KEY_PREFIX", "calligraphy"),
		Queue:       getenv("CALLIGRAPHY_QUEUE", "default"),

		DBMaxConns:    getenvInt("CALLIGRAPHY_DB_MAX_CONNS", 8),
		BatchWrites:   getenvBool("CALLIGRAPHY_BATCH_WRITES", true),
		BatchMaxSize:  getenvInt("CALLIGRAPHY_BATCH_MAX_SIZE", 128),
		BatchInterval: getenvDur("CALLIGRAPHY_BATCH_INTERVAL", 20*time.Millisecond),

		RedisPoolSize: getenvInt("CALLIGRAPHY_REDIS_POOL_SIZE", 8),

		HTTPAddr:        getenv("CALLIGRAPHY_HTTP_ADDR", ":8080"),
		GRPCAddr:        getenv("CALLIGRAPHY_GRPC_ADDR", ":9090"),
		APITokens:       splitNonEmpty(getenv("CALLIGRAPHY_API_TOKENS", "")),
		MaxPayloadBytes: getenvInt("CALLIGRAPHY_MAX_PAYLOAD_BYTES", 256*1024),

		WorkerID:         getenv("CALLIGRAPHY_WORKER_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		Concurrency:      getenvInt("CALLIGRAPHY_CONCURRENCY", 4),
		MinConcurrency:   getenvInt("CALLIGRAPHY_MIN_CONCURRENCY", 1),
		MaxConcurrency:   getenvInt("CALLIGRAPHY_MAX_CONCURRENCY", 16),
		AutoscaleEnabled: getenvBool("CALLIGRAPHY_AUTOSCALE", false),
		FetchBatch:       getenvInt("CALLIGRAPHY_FETCH_BATCH", 8),
		FetchBlock:       getenvDur("CALLIGRAPHY_FETCH_BLOCK", 2*time.Second),
		LeaseTTL:         getenvDur("CALLIGRAPHY_LEASE_TTL", 30*time.Second),
		ExecTimeout:      getenvDur("CALLIGRAPHY_EXEC_TIMEOUT", 5*time.Minute),
		DrainTimeout:     getenvDur("CALLIGRAPHY_DRAIN_TIMEOUT", 25*time.Second),
		MetricsAddr:      getenv("CALLIGRAPHY_WORKER_METRICS_ADDR", ":9100"),
		ControlAddr:      getenv("CALLIGRAPHY_CONTROL_ADDR", ""),

		RetryBase: getenvDur("CALLIGRAPHY_RETRY_BASE", time.Second),
		RetryCap:  getenvDur("CALLIGRAPHY_RETRY_CAP", 5*time.Minute),

		CallbackHMACSecret:   getenv("CALLIGRAPHY_CALLBACK_SECRET", ""),
		CallbackAllowedHosts: splitNonEmpty(getenv("CALLIGRAPHY_CALLBACK_ALLOWED_HOSTS", "")),

		ReapMinIdle: getenvDur("CALLIGRAPHY_REAP_MIN_IDLE", 0), // defaulted from LeaseTTL below
		SweepGrace:  getenvDur("CALLIGRAPHY_SWEEP_GRACE", 60*time.Second),
		OrphanAge:   getenvDur("CALLIGRAPHY_ORPHAN_AGE", 30*time.Second),
	}

	if c.ReapMinIdle == 0 {
		c.ReapMinIdle = c.LeaseTTL
	}

	switch {
	case c.DBMaxConns < 1:
		return c, fmt.Errorf("config: CALLIGRAPHY_DB_MAX_CONNS must be >= 1")
	case c.RedisPoolSize < 1:
		return c, fmt.Errorf("config: CALLIGRAPHY_REDIS_POOL_SIZE must be >= 1")
	case c.Concurrency < 1:
		return c, fmt.Errorf("config: CALLIGRAPHY_CONCURRENCY must be >= 1")
	case c.MinConcurrency < 1 || c.MaxConcurrency < c.MinConcurrency:
		return c, fmt.Errorf("config: need 1 <= CALLIGRAPHY_MIN_CONCURRENCY <= CALLIGRAPHY_MAX_CONCURRENCY")
	case c.FetchBatch < 1:
		return c, fmt.Errorf("config: CALLIGRAPHY_FETCH_BATCH must be >= 1")
	case c.LeaseTTL < 5*time.Second:
		// Below this, heartbeat renewal (TTL/3) races scheduling jitter and
		// healthy jobs get reaped. Refuse rather than misbehave subtly.
		return c, fmt.Errorf("config: CALLIGRAPHY_LEASE_TTL must be >= 5s")
	case c.ReapMinIdle < c.LeaseTTL:
		return c, fmt.Errorf("config: CALLIGRAPHY_REAP_MIN_IDLE must be >= CALLIGRAPHY_LEASE_TTL")
	case c.MaxPayloadBytes < 1 || c.MaxPayloadBytes > 1<<20:
		return c, fmt.Errorf("config: CALLIGRAPHY_MAX_PAYLOAD_BYTES must be in [1, 1MiB]")
	}
	if c.Concurrency < c.MinConcurrency {
		c.Concurrency = c.MinConcurrency
	}
	if c.Concurrency > c.MaxConcurrency {
		c.Concurrency = c.MaxConcurrency
	}
	return c, nil
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getenvBool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getenvDur(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
