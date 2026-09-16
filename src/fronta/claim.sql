-- Invoker privileges; VOLATILE gives the recount a fresh READ COMMITTED snapshot after locking.
CREATE OR REPLACE FUNCTION fronta.claim_v{version}(
    p_types text[], p_worker text, p_lease double precision, p_deadline double precision,
    p_count integer
) RETURNS SETOF fronta.tasks
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, fronta
AS $fronta$
DECLARE
    v_candidate record;
    v_limit integer;
    v_key_limit integer;
    v_paused boolean;
    v_key_count bigint;
    v_index integer;
    v_types text[] := '{}';
    v_limits integer[] := '{}';
    v_key_limits integer[] := '{}';
    v_counts bigint[] := '{}';
    v_ids bigint[] := '{}';
    v_reserved_types text[] := '{}';
    v_keys text[] := '{}';
    v_skip bigint[] := '{}';
    v_skip_types text[] := '{}';
    v_skip_key_types text[] := '{}';
    v_skip_keys text[] := '{}';
    v_seen integer;
    v_bytes bigint := 0;
    v_until timestamptz := clock_timestamp() + make_interval(secs => p_deadline);
    v_cap integer := least(p_count, 256);
BEGIN
    IF p_count < 1 THEN RETURN; END IF;
    <<admission>>
    LOOP
        v_seen := 0;
        FOR v_candidate IN
            WITH chosen AS MATERIALIZED (
{candidate}
            ), locked AS MATERIALIZED (
                -- Lock only chosen ids; apply eligibility filters after the primary-key lookup.
                SELECT t.id, t.type, t.state, t.run_at FROM fronta.tasks t
                WHERE t.id = ANY(ARRAY(SELECT id FROM chosen))
                ORDER BY t.id FOR UPDATE OF t SKIP LOCKED
            )
            SELECT c.*, l.id IS NOT NULL AND l.state = 'queued'
                AND l.type = c.type AND l.run_at <= now() AS locked
            FROM chosen c LEFT JOIN locked l USING (id)
            ORDER BY c.priority DESC, c.run_at, c.id
        LOOP
            v_seen := v_seen + 1;
            v_skip := array_append(v_skip, v_candidate.id);
            IF NOT v_candidate.locked OR v_candidate.type = ANY(v_skip_types) THEN CONTINUE; END IF;
            v_index := array_position(v_types, v_candidate.type);
            IF v_index IS NULL THEN
                IF NOT v_candidate.limited THEN
                    SELECT ty.max_concurrency, ty.max_concurrency_per_key, ty.paused
                        INTO v_limit, v_key_limit, v_paused
                    FROM fronta.task_types ty WHERE ty.name = v_candidate.type FOR SHARE SKIP LOCKED;
                    IF NOT FOUND THEN
                        v_skip_types := array_append(v_skip_types, v_candidate.type);
                        CONTINUE;
                    END IF;
                END IF;
                IF v_candidate.limited OR v_limit IS NOT NULL OR v_key_limit IS NOT NULL THEN
                    -- Also handles a limit enabled between the initial read and the share lock.
                    -- SKIP LOCKED makes a share-to-exclusive upgrade nonblocking.
                    SELECT ty.max_concurrency, ty.max_concurrency_per_key, ty.paused
                        INTO v_limit, v_key_limit, v_paused
                    FROM fronta.task_types ty WHERE ty.name = v_candidate.type FOR UPDATE SKIP LOCKED;
                    IF NOT FOUND THEN
                        v_skip_types := array_append(v_skip_types, v_candidate.type);
                        CONTINUE;
                    END IF;
                END IF;
                IF v_paused THEN
                    v_skip_types := array_append(v_skip_types, v_candidate.type);
                    CONTINUE;
                END IF;
                v_types := array_append(v_types, v_candidate.type);
                v_limits := array_append(v_limits, v_limit);
                v_key_limits := array_append(v_key_limits, v_key_limit);
                v_index := cardinality(v_types);
                v_counts := array_append(v_counts, CASE WHEN v_limit IS NULL THEN 0 ELSE
                    (SELECT count(*) FROM fronta.tasks r WHERE r.type = v_candidate.type AND r.state = 'running') END);
            END IF;
            v_limit := v_limits[v_index];
            v_key_limit := v_key_limits[v_index];
            IF v_limit IS NOT NULL AND v_counts[v_index] >= v_limit THEN
                v_skip_types := array_append(v_skip_types, v_candidate.type);
                CONTINUE;
            END IF;
            IF v_key_limit IS NOT NULL AND v_candidate.concurrency_key IS NOT NULL THEN
                SELECT count(*) INTO v_key_count FROM fronta.tasks r
                WHERE r.type = v_candidate.type AND r.state = 'running'
                    AND r.concurrency_key = v_candidate.concurrency_key;
                IF v_key_count + (SELECT count(*) FROM unnest(v_reserved_types, v_keys) AS k(type, key)
                    WHERE k.type = v_candidate.type AND k.key = v_candidate.concurrency_key) >= v_key_limit THEN
                    v_skip_key_types := array_append(v_skip_key_types, v_candidate.type);
                    v_skip_keys := array_append(v_skip_keys, v_candidate.concurrency_key);
                    CONTINUE;
                END IF;
            END IF;
            EXIT admission WHEN cardinality(v_ids) > 0 AND v_bytes + v_candidate.bytes > 524288;
            v_bytes := v_bytes + v_candidate.bytes;
            v_ids := array_append(v_ids, v_candidate.id);
            v_reserved_types := array_append(v_reserved_types, v_candidate.type);
            v_keys := array_append(v_keys, v_candidate.concurrency_key);
            v_counts[v_index] := v_counts[v_index] + 1;
            EXIT admission WHEN cardinality(v_ids) >= v_cap OR clock_timestamp() >= v_until;
        END LOOP;
        EXIT WHEN v_seen = 0 OR clock_timestamp() >= v_until;
    END LOOP;
    RETURN QUERY WITH applied AS (
        UPDATE fronta.tasks t SET state = 'running', attempt = attempt + 1,
            token = gen_random_uuid(), lease_until = clock_timestamp() + make_interval(secs => p_lease),
            started_at = now(), worker = p_worker, progress = NULL
        WHERE t.id = ANY(v_ids) AND t.state = 'queued' RETURNING t.*
    ), published AS ({published})
    SELECT applied.* FROM applied ORDER BY array_position(v_ids, applied.id);
END;
$fronta$;
