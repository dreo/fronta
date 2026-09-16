-- Lock bounded candidate heads in queue order, skipping busy rows during the search.
WITH eligible_types AS MATERIALIZED (
    SELECT ty.name, ty.max_concurrency IS NOT NULL OR ty.max_concurrency_per_key IS NOT NULL AS limited
    FROM fronta.task_types ty
    WHERE ty.name = ANY(%(types)s) AND NOT ty.paused AND ty.name <> ALL(%(skip_types)s)
      AND (ty.max_concurrency IS NULL OR ty.max_concurrency > (
        SELECT count(*) FROM fronta.tasks r WHERE r.type = ty.name AND r.state = 'running'))
), saturated_keys AS MATERIALIZED (
    SELECT r.type, r.concurrency_key
    FROM fronta.tasks r JOIN fronta.task_types ty ON ty.name = r.type
    WHERE r.state = 'running' AND r.type = ANY(%(types)s) AND r.concurrency_key IS NOT NULL
      AND ty.max_concurrency_per_key IS NOT NULL
    GROUP BY r.type, r.concurrency_key, ty.max_concurrency_per_key
    HAVING count(*) >= ty.max_concurrency_per_key
    UNION ALL SELECT * FROM unnest(%(skip_key_types)s::text[], %(skip_keys)s::text[])
), head AS MATERIALIZED (
    SELECT t.id, t.type, t.concurrency_key, pg_column_size(t.input) AS bytes, t.priority, t.run_at
    FROM fronta.tasks t WHERE t.state = 'queued' AND t.run_at <= now() AND t.id <> ALL(%(skip)s)
    ORDER BY t.priority DESC, t.run_at, t.id LIMIT %(count)s FOR UPDATE OF t SKIP LOCKED
), ready_head AS MATERIALIZED (
    SELECT h.*, ty.limited FROM head h JOIN eligible_types ty ON ty.name = h.type
    WHERE h.concurrency_key IS NULL OR (h.type, h.concurrency_key) NOT IN (
        SELECT type, concurrency_key FROM saturated_keys)
)
-- Use the bounded global head only if every row is eligible; otherwise use per-type seeks.
SELECT * FROM ready_head WHERE (SELECT count(*) FROM ready_head) = (SELECT count(*) FROM head)
UNION ALL (
SELECT c.id, c.type, c.concurrency_key, c.bytes, c.priority, c.run_at, ty.limited
FROM eligible_types ty CROSS JOIN LATERAL (
    SELECT t.id, t.type, t.concurrency_key, t.priority, t.run_at, pg_column_size(t.input) AS bytes
    FROM fronta.tasks t
    WHERE t.type = ty.name AND t.state = 'queued' AND t.run_at <= now()
      AND t.id <> ALL(%(skip)s)
      AND (t.concurrency_key IS NULL OR (t.type, t.concurrency_key) NOT IN (
        SELECT type, concurrency_key FROM saturated_keys))
    ORDER BY t.priority DESC, t.run_at, t.id
    LIMIT %(count)s FOR UPDATE OF t SKIP LOCKED
) c
WHERE (SELECT count(*) FROM ready_head) < (SELECT count(*) FROM head)
ORDER BY c.priority DESC, c.run_at, c.id LIMIT %(count)s
)
ORDER BY priority DESC, run_at, id LIMIT %(count)s
