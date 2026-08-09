// Package queue is Caligraphy's Redis layer: the transport that moves work.
//
// Why Redis Streams (and not lists, pub/sub, Kafka, or RabbitMQ):
//
//   - A consumer group gives per-message acknowledgment, and its pending
//     entries list (PEL) *is* the in-flight set: XREADGROUP moves an entry
//     into the PEL, XACK removes it. There is nothing to hand-roll.
//   - XAUTOCLAIM with min-idle-time *is* the visibility timeout: an entry
//     whose consumer went silent longer than the lease becomes claimable
//     by the reaper. This is the crash-recovery mechanism, built in.
//   - A sorted set alongside gives delayed delivery (retries, scheduled
//     jobs) with one ZADD, promoted back onto the stream by a Lua script
//     so the move is atomic.
//   - Kafka solves a different problem (an ordered, replayable log); its
//     ordering guarantee buys this workload nothing and its retry story
//     costs topic gymnastics. RabbitMQ would genuinely fit -- Redis wins
//     here because Booklet already operates Redis, so Caligraphy adds zero new
//     infrastructure to the deployment it exists to serve.
//
// Keyspace (prefix configurable, default "caligraphy"):
//
//	{p}:stream:{queue}:{prio}   stream   ready work, consumer group "caligraphy"
//	{p}:delayed:{queue}:{prio}  zset     score = ready-at unix ms, member = envelope JSON
//	{p}:dlq:{queue}             stream   dead-letter *log* (capped; DB is authoritative)
//	{p}:cancel:{jobID}          string   cooperative cancel flag, TTL'd
//	{p}:leader:{role}           string   leader lock for recovery duties
//
// Entries are acked AND deleted (XACK + XDEL): Redis is the transport, not
// the history -- Postgres keeps history -- so a fully-processed entry has
// no reason to occupy stream memory. This also keeps XLEN an honest
// "ready + in-flight" gauge instead of an ever-growing count.
package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jguapp/caligraphy/internal/job"
)

// Group is the consumer group every worker joins. One group: Caligraphy wants
// each job executed once, not fanned out to every consumer.
const Group = "caligraphy"

// ReaperConsumer is the consumer name XAUTOCLAIM claims abandoned entries
// under, so a stolen entry is attributable in XPENDING output.
const ReaperConsumer = "caligraphy-reaper"

// dlqMaxLen caps the dead-letter stream. The DLQ *log* in Redis is an
// operator convenience (caligraphyctl peek without a SQL prompt); the
// authoritative DLQ is `status = 'DEAD_LETTER'` in Postgres, which is why
// capping this loses nothing that matters.
const dlqMaxLen = 10_000

type Queue struct {
	rdb    *redis.Client
	prefix string
	queue  string
	log    *slog.Logger
}

type Config struct {
	// Addr is host:port or a redis:// URL.
	Addr string
	// PoolSize caps go-redis's connection pool -- benchmark ablation lever.
	PoolSize int
	Prefix   string
	Queue    string
	Logger   *slog.Logger
}

func New(ctx context.Context, cfg Config) (*Queue, error) {
	var opts *redis.Options
	if strings.Contains(cfg.Addr, "://") {
		parsed, err := redis.ParseURL(cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("queue: parsing redis URL: %w", err)
		}
		opts = parsed
	} else {
		opts = &redis.Options{Addr: cfg.Addr}
	}
	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	q := &Queue{rdb: redis.NewClient(opts), prefix: cfg.Prefix, queue: cfg.Queue, log: log}
	if q.prefix == "" {
		q.prefix = "caligraphy"
	}
	if q.queue == "" {
		q.queue = job.DefaultQueue
	}
	if err := q.rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("queue: ping: %w", err)
	}
	return q, nil
}

func (q *Queue) Close() error                   { return q.rdb.Close() }
func (q *Queue) Ping(ctx context.Context) error { return q.rdb.Ping(ctx).Err() }
func (q *Queue) QueueName() string              { return q.queue }

// FlushForTest wipes the Redis database. Integration suites outside this
// package need it; nothing else may call it, and the name says so.
func (q *Queue) FlushForTest(ctx context.Context) error {
	return q.rdb.FlushDB(ctx).Err()
}

func (q *Queue) streamKey(p job.Priority) string {
	return fmt.Sprintf("%s:stream:%s:%s", q.prefix, q.queue, p)
}
func (q *Queue) delayedKey(p job.Priority) string {
	return fmt.Sprintf("%s:delayed:%s:%s", q.prefix, q.queue, p)
}
func (q *Queue) dlqKey() string {
	return fmt.Sprintf("%s:dlq:%s", q.prefix, q.queue)
}
func (q *Queue) cancelKey(id string) string {
	return fmt.Sprintf("%s:cancel:%s", q.prefix, id)
}
func (q *Queue) leaderKey(role string) string {
	return fmt.Sprintf("%s:leader:%s", q.prefix, role)
}

// EnsureGroup creates the consumer group on every priority stream,
// idempotently (MKSTREAM creates the stream too; BUSYGROUP means done).
func (q *Queue) EnsureGroup(ctx context.Context) error {
	for _, p := range job.PriorityOrder {
		err := q.rdb.XGroupCreateMkStream(ctx, q.streamKey(p), Group, "0").Err()
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("queue: creating group on %s: %w", q.streamKey(p), err)
		}
	}
	return nil
}

// ----------------------------------------------------------------- enqueue

// Enqueue puts an envelope on the wire. If at is in the future the
// envelope parks in the delayed zset (streamID "delayed"); otherwise it
// lands on its priority stream and the real entry id comes back -- which
// the caller records via store.SetEnqueued, closing the orphan-sweep loop.
func (q *Queue) Enqueue(ctx context.Context, env job.Envelope, at time.Time) (streamID string, err error) {
	encoded, err := env.Encode()
	if err != nil {
		return "", err
	}
	if at.After(time.Now()) {
		err := q.rdb.ZAdd(ctx, q.delayedKey(env.Priority), redis.Z{
			Score:  float64(at.UnixMilli()),
			Member: encoded,
		}).Err()
		if err != nil {
			return "", fmt.Errorf("queue: delayed enqueue: %w", err)
		}
		return "delayed", nil
	}
	id, err := q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.streamKey(env.Priority),
		Values: map[string]any{"j": encoded},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("queue: enqueue: %w", err)
	}
	return id, nil
}

// ------------------------------------------------------------------- fetch

// Delivery is one claimed stream entry: the envelope plus the coordinates
// needed to ack or heartbeat it.
type Delivery struct {
	Env      job.Envelope
	EntryID  string
	Priority job.Priority
}

// Fetch claims up to max entries for consumer, strictly preferring higher
// priorities: each priority stream is drained non-blocking in order, and
// only if all three are empty does the call block (on all three at once)
// for up to `block`.
//
// The blocking pass returns whichever stream produces data first, which is
// *not* strictly priority-ordered during a simultaneous arrival -- a
// wake-up race Redis doesn't arbitrate across streams. That's accepted:
// strict priority matters under backlog (the non-blocking pass, which IS
// strict), not in the instant an idle worker wakes.
//
// Corrupt entries ("poison": undecodable envelope) are acked away and
// logged rather than returned -- redelivering them forever helps no one.
func (q *Queue) Fetch(ctx context.Context, consumer string, max int, block time.Duration) ([]Delivery, error) {
	out := make([]Delivery, 0, max)

	for _, p := range job.PriorityOrder {
		if len(out) >= max {
			return out, nil
		}
		streams, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    Group,
			Consumer: consumer,
			Streams:  []string{q.streamKey(p), ">"},
			Count:    int64(max - len(out)),
			Block:    -1, // no BLOCK argument: return immediately
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return out, fmt.Errorf("queue: fetch %s: %w", p, err)
		}
		out = q.appendDeliveries(ctx, out, streams, p)
	}
	if len(out) > 0 || block <= 0 {
		return out, nil
	}

	// Nothing ready anywhere: block on all three priorities at once.
	keys := make([]string, 0, len(job.PriorityOrder)*2)
	for _, p := range job.PriorityOrder {
		keys = append(keys, q.streamKey(p))
	}
	for range job.PriorityOrder {
		keys = append(keys, ">")
	}
	streams, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    Group,
		Consumer: consumer,
		Streams:  keys,
		Count:    int64(max),
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return out, nil // timed out empty-handed; the caller just loops
	}
	if err != nil {
		return out, fmt.Errorf("queue: blocking fetch: %w", err)
	}
	for _, s := range streams {
		out = q.appendDeliveries(ctx, out, []redis.XStream{s}, q.priorityOfStream(s.Stream))
	}
	return out, nil
}

func (q *Queue) priorityOfStream(stream string) job.Priority {
	for _, p := range job.PriorityOrder {
		if q.streamKey(p) == stream {
			return p
		}
	}
	return job.PriorityDefault
}

func (q *Queue) appendDeliveries(ctx context.Context, out []Delivery, streams []redis.XStream, p job.Priority) []Delivery {
	for _, s := range streams {
		for _, m := range s.Messages {
			d, ok := q.decodeMessage(ctx, m, p)
			if ok {
				out = append(out, d)
			}
		}
	}
	return out
}

func (q *Queue) decodeMessage(ctx context.Context, m redis.XMessage, p job.Priority) (Delivery, bool) {
	raw, _ := m.Values["j"].(string)
	env, err := job.DecodeEnvelope(raw)
	if err != nil {
		q.log.Warn("queue: poison entry acked away", "entry", m.ID, "err", err)
		_ = q.Ack(ctx, p, m.ID)
		return Delivery{}, false
	}
	return Delivery{Env: env, EntryID: m.ID, Priority: p}, true
}

// Ack acknowledges AND deletes an entry (see package comment for why both).
func (q *Queue) Ack(ctx context.Context, p job.Priority, entryID string) error {
	pipe := q.rdb.Pipeline()
	pipe.XAck(ctx, q.streamKey(p), Group, entryID)
	pipe.XDel(ctx, q.streamKey(p), entryID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("queue: ack %s: %w", entryID, err)
	}
	return nil
}

// Heartbeat renews a claimed entry's idle clock, which is the Redis half of
// lease renewal (XCLAIM JUSTID by the current consumer resets idle time and
// changes nothing else). Without this, a long-running job's entry would
// look abandoned to XAUTOCLAIM after LeaseTTL and get double-executed.
func (q *Queue) Heartbeat(ctx context.Context, consumer string, p job.Priority, entryID string) error {
	err := q.rdb.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream:   q.streamKey(p),
		Group:    Group,
		Consumer: consumer,
		MinIdle:  0,
		Messages: []string{entryID},
	}).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("queue: heartbeat %s: %w", entryID, err)
	}
	return nil
}

// ----------------------------------------------------------- retry / DLQ

// ScheduleRetry parks the (attempt-incremented) envelope in the delayed
// zset until `at`. The promoter moves it back onto its stream.
func (q *Queue) ScheduleRetry(ctx context.Context, env job.Envelope, at time.Time) error {
	encoded, err := env.Encode()
	if err != nil {
		return err
	}
	err = q.rdb.ZAdd(ctx, q.delayedKey(env.Priority), redis.Z{
		Score:  float64(at.UnixMilli()),
		Member: encoded,
	}).Err()
	if err != nil {
		return fmt.Errorf("queue: scheduling retry: %w", err)
	}
	return nil
}

// DeadLetter appends to the capped DLQ log stream.
func (q *Queue) DeadLetter(ctx context.Context, env job.Envelope, errMsg string) error {
	encoded, err := env.Encode()
	if err != nil {
		return err
	}
	err = q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.dlqKey(),
		MaxLen: dlqMaxLen,
		Approx: true,
		Values: map[string]any{"j": encoded, "error": errMsg, "at": time.Now().UTC().Format(time.RFC3339)},
	}).Err()
	if err != nil {
		return fmt.Errorf("queue: dead-lettering: %w", err)
	}
	return nil
}

// DLQEntry is one row of the DLQ log, for caligraphyctl display.
type DLQEntry struct {
	EntryID string       `json:"entryId"`
	Env     job.Envelope `json:"envelope"`
	Error   string       `json:"error"`
	At      string       `json:"at"`
}

func (q *Queue) ListDLQ(ctx context.Context, limit int) ([]DLQEntry, error) {
	msgs, err := q.rdb.XRevRangeN(ctx, q.dlqKey(), "+", "-", int64(limit)).Result()
	if err != nil {
		return nil, fmt.Errorf("queue: listing dlq: %w", err)
	}
	out := make([]DLQEntry, 0, len(msgs))
	for _, m := range msgs {
		raw, _ := m.Values["j"].(string)
		env, err := job.DecodeEnvelope(raw)
		if err != nil {
			continue
		}
		e := DLQEntry{EntryID: m.ID, Env: env}
		e.Error, _ = m.Values["error"].(string)
		e.At, _ = m.Values["at"].(string)
		out = append(out, e)
	}
	return out, nil
}

// ---------------------------------------------------------------- promote

// promoteScript atomically moves due members from a delayed zset onto its
// stream. Lua because the three steps (read due, XADD, ZREM) must not
// interleave with a competing promoter or a crash -- half-moved members
// would either vanish or double-deliver more than necessary.
var promoteScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for i, member in ipairs(due) do
  redis.call('XADD', KEYS[2], '*', 'j', member)
  redis.call('ZREM', KEYS[1], member)
end
return due
`)

// PromoteDue moves every due delayed envelope (across all priorities) onto
// its stream, returning the promoted envelopes so the caller can flip their
// DB rows RETRYING -> PENDING.
func (q *Queue) PromoteDue(ctx context.Context, batch int) ([]job.Envelope, error) {
	now := time.Now().UnixMilli()
	var promoted []job.Envelope
	for _, p := range job.PriorityOrder {
		res, err := promoteScript.Run(ctx, q.rdb,
			[]string{q.delayedKey(p), q.streamKey(p)}, now, batch).Result()
		if err != nil {
			return promoted, fmt.Errorf("queue: promoting %s: %w", p, err)
		}
		members, _ := res.([]any)
		for _, m := range members {
			s, _ := m.(string)
			env, err := job.DecodeEnvelope(s)
			if err != nil {
				q.log.Warn("queue: poison delayed member dropped", "err", err)
				continue
			}
			promoted = append(promoted, env)
		}
	}
	return promoted, nil
}

// ------------------------------------------------------------------- reap

// ReapAbandoned claims entries whose consumer has been silent for at least
// minIdle -- i.e. whose worker stopped heartbeating: crashed, hung, or
// partitioned. The entries come back claimed by ReaperConsumer; the caller
// decides retry vs DLQ against the database and then acks them.
func (q *Queue) ReapAbandoned(ctx context.Context, minIdle time.Duration, batch int) ([]Delivery, error) {
	var out []Delivery
	for _, p := range job.PriorityOrder {
		msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   q.streamKey(p),
			Group:    Group,
			Consumer: ReaperConsumer,
			MinIdle:  minIdle,
			Start:    "0-0",
			Count:    int64(batch),
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return out, fmt.Errorf("queue: autoclaim %s: %w", p, err)
		}
		for _, m := range msgs {
			d, ok := q.decodeMessage(ctx, m, p)
			if ok {
				out = append(out, d)
			}
		}
	}
	return out, nil
}

// ------------------------------------------------------------------ cancel

// RequestCancel raises the cooperative cancel flag. PENDING/RETRYING jobs
// are cancelled in the database (and any in-flight delivery collapses at
// claim time); RUNNING jobs observe this flag at their next heartbeat and
// cancel their handler's context.
func (q *Queue) RequestCancel(ctx context.Context, jobID string, ttl time.Duration) error {
	if err := q.rdb.Set(ctx, q.cancelKey(jobID), "1", ttl).Err(); err != nil {
		return fmt.Errorf("queue: requesting cancel: %w", err)
	}
	return nil
}

func (q *Queue) IsCancelRequested(ctx context.Context, jobID string) (bool, error) {
	n, err := q.rdb.Exists(ctx, q.cancelKey(jobID)).Result()
	if err != nil {
		return false, fmt.Errorf("queue: checking cancel: %w", err)
	}
	return n == 1, nil
}

// ------------------------------------------------------------------ depths

// Depths is the queue's live shape, the number every scaling and
// backpressure decision keys on.
type Depths struct {
	Ready    map[job.Priority]int64 `json:"ready"`
	Delayed  map[job.Priority]int64 `json:"delayed"`
	InFlight int64                  `json:"inFlight"`
	DLQ      int64                  `json:"dlq"`
}

func (d Depths) TotalReady() int64 {
	var n int64
	for _, v := range d.Ready {
		n += v
	}
	return n
}

// Depths reads XLEN/ZCARD/XPENDING for every priority in one pipeline.
// XLEN counts ready + in-flight (entries are XDEL'd on ack), XPENDING
// counts in-flight, so ready = XLEN - pending.
func (q *Queue) Depths(ctx context.Context) (Depths, error) {
	d := Depths{Ready: map[job.Priority]int64{}, Delayed: map[job.Priority]int64{}}
	pipe := q.rdb.Pipeline()
	xlens := make(map[job.Priority]*redis.IntCmd)
	zcards := make(map[job.Priority]*redis.IntCmd)
	pendings := make(map[job.Priority]*redis.XPendingCmd)
	for _, p := range job.PriorityOrder {
		xlens[p] = pipe.XLen(ctx, q.streamKey(p))
		zcards[p] = pipe.ZCard(ctx, q.delayedKey(p))
		pendings[p] = pipe.XPending(ctx, q.streamKey(p), Group)
	}
	dlq := pipe.XLen(ctx, q.dlqKey())
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return d, fmt.Errorf("queue: reading depths: %w", err)
	}
	for _, p := range job.PriorityOrder {
		var pending int64
		if xp, err := pendings[p].Result(); err == nil && xp != nil {
			pending = xp.Count
		}
		d.InFlight += pending
		ready := xlens[p].Val() - pending
		if ready < 0 {
			ready = 0
		}
		d.Ready[p] = ready
		d.Delayed[p] = zcards[p].Val()
	}
	d.DLQ = dlq.Val()
	return d, nil
}

// ------------------------------------------------------------------ leader

var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

var renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
return 0
`)

// AcquireLeader takes the named leader lock for ttl. The recovery duties
// (promoter, reaper, sweeps) are leader-elected so N API replicas don't
// all reap the same jobs -- not for correctness (every duty is idempotent
// and fenced) but to avoid N-way duplicated work and log noise.
func (q *Queue) AcquireLeader(ctx context.Context, role, id string, ttl time.Duration) (bool, error) {
	ok, err := q.rdb.SetNX(ctx, q.leaderKey(role), id, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("queue: acquiring leadership: %w", err)
	}
	return ok, nil
}

// RenewLeader extends the lock iff this process still holds it.
func (q *Queue) RenewLeader(ctx context.Context, role, id string, ttl time.Duration) (bool, error) {
	res, err := renewScript.Run(ctx, q.rdb, []string{q.leaderKey(role)}, id, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("queue: renewing leadership: %w", err)
	}
	return res == 1, nil
}

// ReleaseLeader drops the lock iff this process holds it (compare-and-del;
// unconditional DEL would release a successor's lock after a long GC pause).
func (q *Queue) ReleaseLeader(ctx context.Context, role, id string) error {
	if err := releaseScript.Run(ctx, q.rdb, []string{q.leaderKey(role)}, id).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("queue: releasing leadership: %w", err)
	}
	return nil
}
