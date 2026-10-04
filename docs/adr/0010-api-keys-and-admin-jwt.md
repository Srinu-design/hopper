# ADR-0010: API keys hashed with HMAC-SHA256 and a pepper; JWT only for admins

Status: Accepted · Date: 2026-10-04

## Context

Two kinds of caller reach Hopper. Tenant programs call `/v1` on every enqueue, so their check must be cheap and
stateless across API replicas. People (a platform admin, and each tenant's owner) call `/admin` rarely, log in with a
password, and must not hold a long-lived secret. Every `/v1` request must be pinned to one tenant by its credential,
never by anything in the request.

## Decision

**Machines use API keys**: `hop_live_<prefix>_<secret>`, an 8-character random prefix (public, used for lookup) and
32 random bytes. Only the prefix and `HMAC-SHA256(pepper, secret)` are stored; the pepper is an environment secret,
never in the database. Lookup by prefix, compare with `hmac.compare_digest`, reject revoked keys. Verified key
records are cached in each API process for 60 s, and `last_used_at` is written at most once a minute per key.
**People use a 15-minute HS256 JWT** from `POST /auth/token`, after an Argon2id password check. Claims: `sub`,
`tenant_id`, `role` (`owner` or `platform_admin`), `iat`, `exp`, `iss`, `aud`; decoding passes an explicit
`algorithms=["HS256"]` and checks `iss` and `aud`.

## Alternatives considered

- **bcrypt or Argon2 for API keys.** Built for low-entropy passwords; a 256-bit random secret cannot be guessed, so a
  slow hash would only add tens of milliseconds to every request. A keyed hash also means a leaked `api_keys` table
  cannot be checked offline without the pepper.
- **Plain SHA-256 of the key.** Fine against guessing, but a stolen table alone would let someone confirm a key.
- **JWTs for machines too.** Revocation needs a deny list or very short tokens plus refresh; a database-backed key
  is easier to revoke and to rotate per service.
- **No cache.** One database read per request; at the planned 1,000 enqueues/s that is 1,000 extra reads/s.
- **Sessions with cookies for admins.** Needs CSRF protection and a session store; a short bearer token suits an API.

## Consequences

- Better: one indexed read per key per minute per replica; nothing in the database alone verifies a key; an admin
  token is useless after 15 minutes; `alg: none` and algorithm-confusion tokens are rejected (tests cover both).
- Worse: **revocation takes up to 60 s** on the other API replicas (the replica that handled the `DELETE` evicts
  the key at once). Losing the pepper invalidates every key; rotating it means re-issuing keys.
- Unknown email and wrong password take the same time (a dummy Argon2 check) and get the same 401, so the login
  does not reveal which emails exist. Argon2 runs in a thread so it never blocks the event loop.
- Tenant isolation: every repository function takes `tenant_id`; another tenant's id gets 404, not 403. One test
  calls every id route with tenant B's key against tenant A's rows, and another fails when a new route is added
  without being classified, so isolation cannot be forgotten.
- Rate limiting of `/auth/token` arrives with the token bucket in Week 6; until then, password guessing is limited
  only by Argon2's cost.

## When we would revisit

If key revocation must be instant (publish revocations over Redis to all replicas), if keys need scopes beyond
"whole tenant", or if admins need SSO (OIDC in place of passwords).
