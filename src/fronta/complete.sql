-- Every input row is independently fenced. A stale token cannot prevent other outcomes.
WITH outcomes AS ({outcomes}), locked AS (
    SELECT t.id, c.token, c.kind, c.data
    FROM fronta.tasks t JOIN outcomes c ON t.id = c.id
    WHERE t.state = 'running' AND t.token = c.token
    ORDER BY t.id FOR UPDATE OF t
), applied AS (
UPDATE fronta.tasks t SET
    state = CASE c.kind
        WHEN 'succeed' THEN 'succeeded'
        WHEN 'fail_final' THEN 'failed'
        WHEN 'cancel' THEN 'cancelled'
        WHEN 'release' THEN CASE WHEN t.cancel_requested_at IS NULL THEN 'queued' ELSE 'cancelled' END
        WHEN 'fail' THEN CASE WHEN t.cancel_requested_at IS NOT NULL THEN 'cancelled'
            WHEN t.failures + 1 < t.max_attempts THEN 'queued' ELSE 'failed' END END,
    result = CASE WHEN c.kind = 'succeed' THEN c.data::jsonb ELSE t.result END,
    error = CASE WHEN c.kind IN ('fail', 'fail_final') THEN c.data::jsonb ELSE t.error END,
    failures = t.failures + CASE WHEN c.kind IN ('fail', 'fail_final') THEN 1 ELSE 0 END,
    run_at = CASE WHEN c.kind = 'release' THEN now()
        WHEN c.kind = 'fail' AND t.cancel_requested_at IS NULL AND t.failures + 1 < t.max_attempts
        THEN {backoff} ELSE t.run_at END,
    finished_at = CASE WHEN c.kind IN ('succeed', 'fail_final', 'cancel')
        OR t.cancel_requested_at IS NOT NULL OR (c.kind = 'fail' AND t.failures + 1 >= t.max_attempts)
        THEN now() END,
    token = NULL, lease_until = NULL
FROM locked c WHERE t.id = c.id AND t.state = 'running' AND t.token = c.token
    AND (c.kind <> 'cancel' OR t.cancel_requested_at IS NOT NULL)
RETURNING t.id, t.type, t.state, t.attempt
), published AS ({published})
SELECT id, type, state FROM applied
