"""Hand-written queue SQL. Every state transition is one guarded UPDATE."""

# Claim: SKIP LOCKED makes concurrent workers pass over rows another worker is claiming
# instead of waiting, so each gets a disjoint batch. The row lock lasts only for this
# statement; after commit the lease protects the job. A fresh lease_token on every claim
# is the fencing token for all later writes.
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
          j.timeout_seconds, j.lease_token
"""

# Ack: fenced on the lease token and written together with the attempt row. Zero rows
# means the lease was lost (expired and reclaimed): the caller logs it and does nothing else.
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

# Enqueue. With an idempotency key, a repeat insert hits jobs_idem_uidx and returns no row;
# the caller then loads the existing job and compares request hashes.
INSERT_JOB = """
INSERT INTO jobs (tenant_id, queue, task, payload, priority, max_attempts, timeout_seconds,
                  idempotency_key, request_hash)
VALUES (:tenant_id, :queue, :task, CAST(:payload AS jsonb), :priority, :max_attempts,
        :timeout_seconds, :idempotency_key, :request_hash)
ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING *
"""

GET_JOB_BY_IDEMPOTENCY_KEY = """
SELECT * FROM jobs WHERE tenant_id = :tenant_id AND idempotency_key = :idempotency_key
"""

GET_JOB = "SELECT * FROM jobs WHERE id = :id AND tenant_id = :tenant_id"

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

UPSERT_TENANT = """
INSERT INTO tenants (name) VALUES (:name)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id
"""
