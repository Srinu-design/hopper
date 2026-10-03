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

INSERT_JOB = """
INSERT INTO jobs (tenant_id, queue, task, payload, priority, max_attempts, timeout_seconds)
VALUES (:tenant_id, :queue, :task, CAST(:payload AS jsonb), :priority, :max_attempts,
        :timeout_seconds)
RETURNING *
"""

GET_JOB = "SELECT * FROM jobs WHERE id = :id AND tenant_id = :tenant_id"

GET_ATTEMPTS = """
SELECT attempt, worker_id, started_at, finished_at, outcome, error
FROM job_attempts WHERE job_id = :job_id ORDER BY attempt, id
"""

UPSERT_TENANT = """
INSERT INTO tenants (name) VALUES (:name)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id
"""
