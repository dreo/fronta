-- Fronta V1 schema. Idempotent: every statement is IF NOT EXISTS. Applied by `fronta db init`.

CREATE SCHEMA IF NOT EXISTS fronta;

CREATE TABLE IF NOT EXISTS fronta.task_types (
    name                    text PRIMARY KEY CHECK (octet_length(name) BETWEEN 1 AND 255),
    executor                text NOT NULL CHECK (executor IN ('asyncio', 'process')),
    input_schema            jsonb NOT NULL,
    output_schema           jsonb,                                           -- NULL = any JSON value
    policy                  jsonb NOT NULL,                                  -- snapshot source for enqueue
    max_concurrency         integer CHECK (max_concurrency > 0),             -- authoritative limits,
    max_concurrency_per_key integer CHECK (max_concurrency_per_key > 0),     -- enforced by the claim
    fingerprint             text NOT NULL,
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fronta.tasks (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type                text NOT NULL CHECK (octet_length(type) BETWEEN 1 AND 255),
    state               text NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    priority            integer NOT NULL DEFAULT 0,
    key                 text CHECK (octet_length(key) BETWEEN 1 AND 1024),
    concurrency_key     text CHECK (octet_length(concurrency_key) BETWEEN 1 AND 1024),
    input               jsonb NOT NULL,
    result              jsonb,
    error               jsonb,
    progress            jsonb,
    attempt             integer NOT NULL DEFAULT 0,                          -- claims so far
    failures            integer NOT NULL DEFAULT 0,                          -- attempts that ended failed
    max_attempts        integer NOT NULL CHECK (max_attempts >= 1),          -- policy snapshot
    attempt_timeout_s   double precision NOT NULL CHECK (attempt_timeout_s > 0 AND attempt_timeout_s <= 2592000),
    backoff_base_s      double precision NOT NULL CHECK (backoff_base_s >= 0 AND backoff_base_s <= backoff_cap_s),
    backoff_factor      double precision NOT NULL CHECK (backoff_factor BETWEEN 1 AND 10),
    backoff_cap_s       double precision NOT NULL CHECK (backoff_cap_s >= 0 AND backoff_cap_s <= 2592000),
    token               uuid,                                                -- execution token while running
    lease_until         timestamptz,
    worker              text,
    cancel_requested_at timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    run_at              timestamptz NOT NULL DEFAULT now(),
    started_at          timestamptz,
    finished_at         timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS tasks_active_key_uidx ON fronta.tasks (type, key)
    WHERE key IS NOT NULL AND state IN ('queued', 'running');
CREATE INDEX IF NOT EXISTS tasks_queue_idx ON fronta.tasks (priority DESC, run_at, id) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS tasks_lease_idx ON fronta.tasks (lease_until) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS tasks_running_key_idx ON fronta.tasks (type, concurrency_key) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS tasks_finished_idx ON fronta.tasks (finished_at)
    WHERE state IN ('succeeded', 'failed', 'cancelled');
CREATE INDEX IF NOT EXISTS tasks_type_state_idx ON fronta.tasks (type, state, id);
CREATE INDEX IF NOT EXISTS tasks_state_idx ON fronta.tasks (state, id);
