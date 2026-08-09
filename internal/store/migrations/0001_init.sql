-- Calligraphy schema. Four tables, on purpose:
--
--   jobs         the record of a unit of work (source of truth)
--   job_attempts one row per execution attempt, kept forever
--   job_events   lifecycle history worth explaining later
--   workers      registration + heartbeat, for operations and metrics
--
-- Results live in jobs.result rather than a fifth table: a result is 1:1
-- with its job and is always read alongside it, so a separate table would
-- add a join and remove nothing.
--
-- Statuses/priorities are TEXT + CHECK rather than Postgres enums: adding a
-- value to an enum is a migration with locking behavior worth avoiding, and
-- the CHECK documents the domain in the schema itself.

CREATE TABLE jobs (
    id                 text PRIMARY KEY,
    type               text NOT NULL,
    queue              text NOT NULL DEFAULT 'default',
    status             text NOT NULL CHECK (status IN
                         ('PENDING','RUNNING','COMPLETED','FAILED','RETRYING','DEAD_LETTER','CANCELLED')),
    priority           text NOT NULL DEFAULT 'default' CHECK (priority IN ('high','default','low')),
    payload            jsonb NOT NULL DEFAULT 'null',
    result             jsonb,
    error              text,
    idempotency_key    text,
    attempt_count      int NOT NULL DEFAULT 0,
    max_attempts       int NOT NULL,

    -- Fencing token: bumped on every ownership change (claim, reap,
    -- requeue). Terminal writes carry the epoch their writer holds and
    -- match on it, so a worker that lost its lease cannot overwrite state
    -- written by the job's next owner.
    lease_epoch        bigint NOT NULL DEFAULT 0,
    lease_expires_at   timestamptz,
    worker_id          text,

    -- The Redis stream entry id this job was enqueued as (or 'delayed' for
    -- scheduled jobs parked in the ZSET). NULL means the handoff to Redis
    -- never confirmably happened -- which is exactly what the orphan sweep
    -- looks for to repair a submit that failed between INSERT and XADD.
    enqueued_stream_id text,

    created_at         timestamptz NOT NULL DEFAULT now(),
    scheduled_at       timestamptz NOT NULL DEFAULT now(),
    started_at         timestamptz,
    completed_at       timestamptz,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

-- Submission dedup: one live job per (type, idempotency_key). Partial --
-- jobs submitted without a key never contend.
CREATE UNIQUE INDEX jobs_idempotency_uq ON jobs (type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX jobs_status_idx  ON jobs (status);
CREATE INDEX jobs_created_idx ON jobs (created_at);

-- The reaper's sweep: RUNNING rows whose lease is long gone.
CREATE INDEX jobs_lease_sweep_idx ON jobs (lease_expires_at) WHERE status = 'RUNNING';

-- The orphan sweep: PENDING rows that never made it into Redis.
CREATE INDEX jobs_orphan_idx ON jobs (created_at)
    WHERE status = 'PENDING' AND enqueued_stream_id IS NULL;

-- The stuck-retry sweep: RETRYING rows whose promotion is overdue.
CREATE INDEX jobs_retry_stuck_idx ON jobs (scheduled_at) WHERE status = 'RETRYING';

-- DLQ browsing, newest first.
CREATE INDEX jobs_dlq_idx ON jobs (completed_at DESC) WHERE status = 'DEAD_LETTER';


CREATE TABLE job_attempts (
    id          bigserial PRIMARY KEY,
    job_id      text NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     int NOT NULL,
    worker_id   text,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz,
    outcome     text NOT NULL,
    error       text,
    UNIQUE (job_id, attempt)
);

CREATE INDEX job_attempts_job_idx ON job_attempts (job_id);


CREATE TABLE job_events (
    id     bigserial PRIMARY KEY,
    job_id text NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    at     timestamptz NOT NULL DEFAULT now(),
    event  text NOT NULL,
    detail jsonb
);

CREATE INDEX job_events_job_idx ON job_events (job_id, at);


CREATE TABLE workers (
    id                 text PRIMARY KEY,
    hostname           text,
    pid                int,
    concurrency        int NOT NULL,
    target_concurrency int NOT NULL,
    active_jobs        int NOT NULL DEFAULT 0,
    processed          bigint NOT NULL DEFAULT 0,
    state              text NOT NULL DEFAULT 'active' CHECK (state IN ('active','draining','gone')),
    started_at         timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at  timestamptz NOT NULL DEFAULT now()
);
