# AI usage notes

An honest, running log of how AI tools were used on Hopper. Only things that actually happened are recorded.

## Tool

Claude Code (Anthropic's coding agent, running Claude Sonnet 5.5).

## Week 1: skeleton

**Used for:** scaffolding the repo layout, `pyproject.toml`, Dockerfile, Compose file, Alembic setup and the CI
workflow, and for writing the first tests. Current GitHub Action major versions and pinned image versions were
looked up with `git ls-remote` and `docker manifest inspect` instead of being taken from memory.

**Not AI-written yet:** the queue core (claim, heartbeat, ack, reaper SQL), the Lua script and the state machine.
Those start in Week 2 and are meant to be written by hand.

**The migration is transcribed** from the schema in the build guide, then checked by tests (tables, partial-index
predicates, CHECK constraint, per-tenant idempotency uniqueness, downgrade/upgrade round trip).

**Process:** the first test (`tests/unit/test_healthz.py`) was run and seen to fail with
`ModuleNotFoundError: hopper.api.main` before the app existed, then passed once it was implemented.

### Mistakes caught

- **`localhost` on Windows.** The `/readyz` integration test failed with a timeout against a healthy stack, and the
  whole suite took 75 s. Cause: `localhost` resolved to `::1` first, but Docker publishes ports on `127.0.0.1` only.
  Host-side URLs now use `127.0.0.1`; the suite takes about 25 s.
- **Silent migrations.** The first Compose run applied the schema but printed nothing, because `migrations/env.py`
  never loaded the logging config. Found by reading the `migrate` container output on a clean volume; fixed with
  `fileConfig` in `env.py`.
