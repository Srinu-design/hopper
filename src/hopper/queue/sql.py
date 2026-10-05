"""Hand-written queue SQL. Every state transition is one guarded UPDATE."""

# Claim: SKIP LOCKED makes concurrent workers pass over rows another worker is claiming
# instead of waiting, so each gets a disjoint batch. The row lock lasts only for this
# statement; after commit the lease protects the job. A fresh lease_token on every claim
# is the fencing token for all later writes. The tenant's signing secret comes back with the
# job (a primary-key lookup) so the http task can sign its request without another query.
# wait_seconds is how long the job sat ready (run_at to claim), for the queue-wait metric;
# both ends come from the database clock.
CLAIM = """
WITH next AS (
  SELECT id FROM jobs
  WHERE queue = :queue AND status = 'queued' AND run_at <= now()
  ORDER BY priority DESC, run_at
  LIMIT :limit
  FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET status = 'running',
    attempts = j.attempts + 1,
    lease_owner = :worker_id,
    lease_token = gen_random_uuid(),
    lease_expires_at = now() + make_interval(secs => :lease_seconds),
    started_at = now()
FROM next
WHERE j.id = next.id
RETURNING j.id, j.tenant_id, j.queue, j.task, j.payload, j.attempts, j.max_attempts,
          j.timeout_seconds, j.lease_token, j.request_id,
          extract(epoch FROM j.started_at - j.run_at) AS wait_seconds,
          (SELECT signing_secret FROM tenants t WHERE t.id = j.tenant_id) AS signing_secret
"""

# Ack: fenced on the lease token and written together with the attempt row. Zero rows
# means the lease was lost (expired and reclaimed): the caller logs it and does nothing else.
# The same fencing guards nack, heartbeat and release below.
ACK = """
WITH done AS (
  UPDATE jobs
  SET status = 'succeeded', result = CAST(:result AS jsonb), finished_at = now(),
      lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
  WHERE id = :id AND status = 'running' AND lease_token = :token
  RETURNING id, attempts, started_at, finished_at
), attempt AS (
  INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome)
  SELECT id, attempts, :worker_id, started_at, finished_at, 'succeeded' FROM done
)
SELECT id FROM done
"""

# Nack: a failed run, fenced on the lease token like ack. The database decides between a
# retry and the DLQ, so the rule lives in one place: a permanent error, or no attempts left,
# means dead; otherwise the job is queued again with run_at pushed out by the backoff delay,
# and the claim query ignores it until then. Returns the new status; zero rows = lease lost.
NACK = """
WITH failed AS (
  UPDATE jobs
  SET status = CASE WHEN CAST(:permanent AS boolean) OR attempts >= max_attempts
                    THEN 'dead' ELSE 'queued' END,
      dead_at = CASE WHEN CAST(:permanent AS boolean) OR attempts >= max_attempts
                     THEN now() END,
      run_at = CASE WHEN CAST(:permanent AS boolean) OR attempts >= max_attempts
                    THEN run_at
                    ELSE now() + make_interval(secs => :delay_seconds) END,
      last_error = :error,
      lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
  WHERE id = :id AND status = 'running' AND lease_token = :token
  RETURNING id, attempts, started_at, status
), attempt AS (
  INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome, error)
  SELECT id, attempts, :worker_id, started_at, now(), :outcome, :error FROM failed
)
SELECT status FROM failed
"""

# Heartbeat: renew the lease on every job a worker is running, in one statement. Each row
# is fenced on its own token, so the ids that come back are exactly the leases the worker
# still holds; a job missing from the result was reclaimed and must never be acked.
HEARTBEAT = """
UPDATE jobs j
SET lease_expires_at = now() + make_interval(secs => :lease_seconds)
FROM unnest(CAST(:ids AS uuid[]), CAST(:tokens AS uuid[])) AS t(id, token)
WHERE j.id = t.id AND j.status = 'running' AND j.lease_token = t.token
RETURNING j.id
"""

# Reaper: a lease that ran out means the worker died or stalled. The expired run counts as
# an attempt (the claim already added it), so a job that crashes its worker every time (a
# poison pill) ends up dead instead of looping forever. SKIP LOCKED lets two reapers run at
# once, and the attempt row is written in the same statement. jobs_lease_idx covers the scan.
REAP = """
WITH expired AS (
  SELECT id, lease_owner FROM jobs
  WHERE status = 'running' AND lease_expires_at < now()
  ORDER BY lease_expires_at
  LIMIT :limit
  FOR UPDATE SKIP LOCKED
), reaped AS (
  UPDATE jobs j
  SET status = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'queued' END,
      dead_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
      run_at = now(),
      last_error = 'lease expired on ' || coalesce(expired.lease_owner, 'unknown'),
      lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
  FROM expired
  WHERE j.id = expired.id
  RETURNING j.id, j.tenant_id, j.queue, j.task, j.status, j.attempts, j.started_at,
            j.last_error, j.request_id, coalesce(expired.lease_owner, 'unknown') AS lease_owner
), attempt AS (
  INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome, error)
  SELECT id, attempts, lease_owner, started_at, now(), 'lease_expired', last_error FROM reaped
)
SELECT id, tenant_id, queue, task, status, attempts, lease_owner, request_id FROM reaped
"""

# Release on graceful shutdown: hand unfinished jobs back without an attempt penalty. The
# run is still recorded, as 'released' under the attempt number it had, and the next claim
# reuses that number. Fenced per job like the heartbeat.
RELEASE = """
WITH released AS (
  UPDATE jobs j
  SET status = 'queued', attempts = j.attempts - 1, run_at = now(),
      lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
  FROM unnest(CAST(:ids AS uuid[]), CAST(:tokens AS uuid[])) AS t(id, token)
  WHERE j.id = t.id AND j.status = 'running' AND j.lease_token = t.token
  RETURNING j.id, j.attempts, j.started_at
), attempt AS (
  INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome, error)
  SELECT id, attempts + 1, :worker_id, started_at, now(), 'released', 'worker shutting down'
  FROM released
)
SELECT id FROM released
"""

# Enqueue. With an idempotency key, a repeat insert hits jobs_idem_uidx and returns no row;
# the caller then loads the existing job and compares request hashes. A delayed job is just a
# future run_at: an absolute time, or now() + delay from the database clock. request_id is the
# enqueue call's X-Request-ID, carried into every worker log line for the job.
INSERT_JOB = """
INSERT INTO jobs (tenant_id, queue, task, payload, priority, max_attempts, timeout_seconds,
                  idempotency_key, request_hash, run_at, schedule_id, request_id)
VALUES (:tenant_id, :queue, :task, CAST(:payload AS jsonb), :priority, :max_attempts,
        :timeout_seconds, :idempotency_key, :request_hash,
        coalesce(CAST(:run_at AS timestamptz), now() + make_interval(secs => :delay_seconds)),
        :schedule_id, :request_id)
ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING *
"""

GET_JOB_BY_IDEMPOTENCY_KEY = """
SELECT * FROM jobs WHERE tenant_id = :tenant_id AND idempotency_key = :idempotency_key
"""

GET_JOB = "SELECT * FROM jobs WHERE id = :id AND tenant_id = :tenant_id"

# A tenant's jobs, newest first, keyset-paginated on (created_at, id) over jobs_tenant_idx.
# {filters} is built only from fixed fragments in queue/jobs.py, never from user input.
JOBS_LIST = """
SELECT * FROM jobs
WHERE tenant_id = :tenant_id {filters}
ORDER BY created_at DESC, id DESC
LIMIT :limit
"""

# Cancel: only a job still waiting in the queue. A running job belongs to its worker.
CANCEL_JOB = """
UPDATE jobs SET status = 'cancelled', finished_at = now()
WHERE id = :id AND tenant_id = :tenant_id AND status = 'queued'
RETURNING id
"""

# Ordered by id (insertion order), not attempt: a replay restarts attempt numbers at 1.
GET_ATTEMPTS = """
SELECT attempt, worker_id, started_at, finished_at, outcome, error
FROM job_attempts WHERE job_id = :job_id ORDER BY id
"""

# DLQ: dead jobs are rows with status = 'dead' (jobs_dead_idx), not a separate table.
# {filters} is built only from fixed fragments in queue/dlq.py, never from user input.
DLQ_LIST = """
SELECT * FROM jobs
WHERE tenant_id = :tenant_id AND status = 'dead' {filters}
ORDER BY dead_at DESC, id DESC
LIMIT :limit
"""

# Replay: one guarded UPDATE back to queued, attempts reset. run_at is spread over a window
# so a bulk replay does not stampede the dependency that caused the failures. Replaying a
# job that is no longer dead matches nothing, which makes replay idempotent.
DLQ_REPLAY = """
WITH picked AS (
  SELECT id FROM jobs
  WHERE tenant_id = :tenant_id AND status = 'dead' {filters}
  ORDER BY dead_at
  LIMIT :limit
  FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET status = 'queued', attempts = 0, replay_count = j.replay_count + 1,
    run_at = now() + random() * make_interval(secs => :spread_seconds),
    dead_at = NULL, last_error = NULL
FROM picked
WHERE j.id = picked.id
RETURNING j.id
"""

DLQ_ANY_LEFT = """
SELECT EXISTS (SELECT 1 FROM jobs WHERE tenant_id = :tenant_id AND status = 'dead' {filters})
"""

# Cron schedules. Every statement is scoped to the tenant, except the scheduler's own two.
INSERT_SCHEDULE = """
INSERT INTO schedules (tenant_id, name, cron, timezone, queue, task, payload, enabled,
                       next_run_at)
VALUES (:tenant_id, :name, :cron, :timezone, :queue, :task, CAST(:payload AS jsonb), :enabled,
        :next_run_at)
ON CONFLICT (tenant_id, name) DO NOTHING
RETURNING *
"""

LIST_SCHEDULES = """
SELECT * FROM schedules
WHERE tenant_id = :tenant_id AND name > :after
ORDER BY name
LIMIT :limit
"""

GET_SCHEDULE = "SELECT * FROM schedules WHERE id = :id AND tenant_id = :tenant_id"

# Re-enabling restarts the clock from now, so a schedule that was off for a week does not
# fire a stale tick the moment it comes back.
SET_SCHEDULE_ENABLED = """
UPDATE schedules
SET enabled = :enabled,
    next_run_at = CASE WHEN :enabled AND NOT enabled THEN :next_run_at ELSE next_run_at END
WHERE id = :id AND tenant_id = :tenant_id
RETURNING *
"""

DELETE_SCHEDULE = "DELETE FROM schedules WHERE id = :id AND tenant_id = :tenant_id RETURNING id"

# The cron loop: claim due schedules with SKIP LOCKED, so two schedulers never take the same
# one. The row locks hold until the transaction that also inserts the jobs and moves
# next_run_at commits, so a tick is never lost or doubled. schedules_due_idx covers the scan.
DUE_SCHEDULES = """
SELECT * FROM schedules
WHERE enabled AND next_run_at <= now()
ORDER BY next_run_at
LIMIT :limit
FOR UPDATE SKIP LOCKED
"""

ADVANCE_SCHEDULE = """
UPDATE schedules SET last_run_at = next_run_at, next_run_at = :next_run_at WHERE id = :id
"""

# A stored expression that no longer computes (it cannot happen through the API) must not
# stall every other schedule: it is switched off and logged.
DISABLE_SCHEDULE = "UPDATE schedules SET enabled = false WHERE id = :id"

# Queue depth, counted once a second by each scheduler for the depth gauges and backpressure.
# Each query reads one partial index (jobs_ready_idx, jobs_lease_idx, jobs_dead_idx), so the
# cost grows with the backlog, not with the millions of finished rows in jobs. The per-tenant
# grouping is what backpressure needs; the gauges sum it per queue.
QUEUED_DEPTH = """
SELECT tenant_id, queue,
       count(*) FILTER (WHERE run_at <= now()) AS ready,
       count(*) FILTER (WHERE run_at > now()) AS delayed,
       coalesce(extract(epoch FROM now() - min(run_at) FILTER (WHERE run_at <= now())), 0)
         AS oldest_ready_seconds
FROM jobs WHERE status = 'queued'
GROUP BY tenant_id, queue
"""

RUNNING_AND_DEAD_DEPTH = """
SELECT queue, 'running' AS state, count(*) AS jobs FROM jobs WHERE status = 'running'
GROUP BY queue
UNION ALL
SELECT queue, 'dead' AS state, count(*) AS jobs FROM jobs WHERE status = 'dead'
GROUP BY queue
"""

# Retention: succeeded and cancelled jobs are deleted once they finished more than :days ago,
# which also frees their idempotency keys; their attempt rows go with them (ON DELETE CASCADE).
# Dead jobs stay: they are the DLQ until a tenant replays them. Each batch is one short
# transaction, and SKIP LOCKED lets two schedulers delete side by side. No index covers
# finished_at, so a pass may scan the table; the loop runs hourly, and any old rows will do
# (no ORDER BY), so a batch stops scanning as soon as it is full.
DELETE_FINISHED = """
WITH old AS (
  SELECT id FROM jobs
  WHERE status IN ('succeeded', 'cancelled')
    AND finished_at < now() - make_interval(days => CAST(:days AS integer))
  LIMIT :limit
  FOR UPDATE SKIP LOCKED
), gone AS (
  DELETE FROM jobs j
  USING old
  WHERE j.id = old.id
  RETURNING j.id
)
SELECT count(*) FROM gone
"""
