-- Fronta V1 schema. Idempotent: every statement is IF NOT EXISTS (or a no-op on rerun). Applied
-- by `fronta db init`.

CREATE SCHEMA IF NOT EXISTS fronta;

-- Acquire existing tables together before DDL; release partial locks before retrying busy workers.
DO $fronta$
DECLARE existing text;
BEGIN
    SELECT string_agg(format('%I.%I', n.nspname, c.relname), ', ' ORDER BY c.oid) INTO existing
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'fronta' AND c.relname IN ('meta', 'task_types', 'tasks', 'subscriptions', 'events')
      AND c.relkind = 'r';
    IF existing IS NULL THEN RETURN; END IF;
    LOOP
        BEGIN
            EXECUTE 'LOCK TABLE ' || existing || ' IN ACCESS EXCLUSIVE MODE NOWAIT';
            EXIT;
        EXCEPTION WHEN lock_not_available THEN NULL;
        END;
        PERFORM pg_sleep(0.01);
    END LOOP;
END;
$fronta$;

CREATE TABLE IF NOT EXISTS fronta.meta (key text PRIMARY KEY, value text NOT NULL);

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
) WITH (fillfactor = 90);

-- Heartbeats rewrite `lease_until` every few seconds. It is deliberately not indexed and the table
-- keeps 10% free space per page, so those updates stay heap-only (no index entries, no dead index
-- tuples, a third of the WAL). The reaper finds the few running rows through tasks_state_idx.
ALTER TABLE fronta.tasks SET (fillfactor = 90);
DROP INDEX IF EXISTS fronta.tasks_lease_idx;

CREATE UNIQUE INDEX IF NOT EXISTS tasks_active_key_uidx ON fronta.tasks (type, key)
    WHERE key IS NOT NULL AND state IN ('queued', 'running');
CREATE INDEX IF NOT EXISTS tasks_queue_idx ON fronta.tasks (priority DESC, run_at, id) WHERE state = 'queued';
-- A worker accepting one type can seek directly into that type's claim order, even behind a
-- large higher-priority backlog belonging to other workers. Keep the global index for fleets
-- accepting multiple types, which must retain priority/run_at/id order across those types.
CREATE INDEX IF NOT EXISTS tasks_type_queue_idx ON fronta.tasks (type, priority DESC, run_at, id)
    WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS tasks_running_key_idx ON fronta.tasks (type, concurrency_key) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS tasks_finished_idx ON fronta.tasks (finished_at)
    WHERE state IN ('succeeded', 'failed', 'cancelled');
CREATE INDEX IF NOT EXISTS tasks_type_state_idx ON fronta.tasks (type, state, id);
CREATE INDEX IF NOT EXISTS tasks_state_idx ON fronta.tasks (state, id);
CREATE INDEX IF NOT EXISTS tasks_key_idx ON fronta.tasks (key, id) WHERE key IS NOT NULL;  -- list by key over history

CREATE TABLE IF NOT EXISTS fronta.subscriptions (
    name text PRIMARY KEY CHECK (octet_length(name) BETWEEN 1 AND 255),
    states text[] NOT NULL DEFAULT '{succeeded,failed,cancelled}'
        CHECK (states <@ ARRAY['queued','running','succeeded','failed','cancelled']::text[]),
    types text[],
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS fronta.events (
    seq bigint GENERATED ALWAYS AS IDENTITY,
    subscription text NOT NULL,
    task_id bigint NOT NULL,
    type text NOT NULL,
    state text NOT NULL,
    attempt integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS events_subscription_seq_uidx
    ON fronta.events (subscription, seq);
-- Expiry checks must not scan a large backlog that is still within retention.
CREATE INDEX IF NOT EXISTS events_created_at_idx ON fronta.events (created_at);
ALTER TABLE fronta.events SET (autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000, autovacuum_vacuum_cost_delay = 0);

ALTER TABLE fronta.task_types ADD COLUMN IF NOT EXISTS paused boolean NOT NULL DEFAULT false;
ALTER TABLE fronta.tasks ADD COLUMN IF NOT EXISTS metadata jsonb;
ALTER TABLE fronta.subscriptions ADD COLUMN IF NOT EXISTS backfill jsonb;
