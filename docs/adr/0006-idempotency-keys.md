# ADR-0006: Idempotency keys: per tenant, stored on the job row, 422 on a different body

Status: Accepted · Date: 2026-10-08 (decided in Week 3, written down in Week 8)

## Context

A client sends `POST /v1/jobs`, and the connection drops before the answer comes back. Did the job get created?
The client cannot know, so it retries, and without help that makes a second job: a second email, a second charge.
The client needs a way to say "this is the same request as before".

## Decision

Clients may send an `Idempotency-Key` header (1 to 255 printable characters) on `POST /v1/jobs`.

- The key is **unique per tenant**: `UNIQUE (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL`
  (`jobs_idem_uidx`). Two tenants can use the same key without seeing each other.
- The key lives **on the job row**, with `request_hash`: the SHA-256 of the request body in a canonical form
  (defaults filled in, keys sorted, no spaces), so `{"a":1,"b":2}` and `{"b":2,"a":1}` hash the same.
- The insert uses `ON CONFLICT DO NOTHING`. If the key exists:
  - **same hash**: answer `200` with the existing job as it is now, and `Idempotent-Replayed: true`;
  - **different hash**: answer `422 idempotency_key_reused`. The key was used for a different request, which is
    a client bug.
- The key lasts as long as the job row. The retention loop deletes `succeeded` and `cancelled` jobs 7 days after
  they finish, which frees their keys. Dead jobs keep theirs.
- Keys starting with `cron:` are reserved. The scheduler uses `cron:<schedule id>:<due time>` as a second guard
  against firing one tick twice (ADR-0007).

## Alternatives considered

- **A separate `idempotency_keys` table with its own expiry** (as Stripe does, 24 h). It needs two writes per
  enqueue that must agree, and a cleanup job of its own. On the job row, the key and the job commit together.
- **Keys in Redis (`SET NX`).** Fast, but a dual write: a Redis restart or failover forgets keys, and a crash
  between Redis and Postgres leaves them disagreeing. Redis must never be needed for correctness (ADR-0001).
- **Global keys, not per tenant.** One tenant could block another tenant's key, and learn that it exists.
- **Ignore the body and always return the first job.** A client that reuses a key by mistake would silently get
  the wrong job back. 422 makes the mistake visible.
- **409 instead of 422.** The IETF draft for the `Idempotency-Key` header uses 422 for a key reused with a
  different payload, and the build guide asks for it.

## Consequences

- Measured: 50 concurrent requests with one key created exactly one job; the other 49 got the same job back
  (`tests/integration/test_idempotency.py`). `ON CONFLICT` waits for the first insert to commit, so the loser
  always finds the winner.
- The answer to a retry is the job **now** (maybe already `succeeded`), not a copy of the first response. That is
  more useful to a client that is checking on its job.
- Rare race: if retention deletes the job between the failed insert and the lookup, the key is free again, so
  Hopper tries the insert once more (`insert_job`, two rounds).
- A key protects only for as long as its job row lives (7 days after it finishes). A client that retries after
  that creates a new job.
- During overload, backpressure answers 429 or 503 before the key is looked up (it costs no query). So a client
  retrying a request that did create its job may still be told to wait (ADR-0009).

## When we would revisit

If clients need keys to last longer than the job rows (then a separate key table with its own expiry), or if
idempotency is needed on other routes, such as schedule creation.
