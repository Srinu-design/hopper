#!/usr/bin/env python3
"""Smoke test for a release: readiness, then one real job through the API, a worker and back.

    SMOKE_API_KEY=hop_live_... python3 smoke.py [--url http://localhost] [--timeout 20]

Exits 0 when /readyz answers 200 and a `sleep` job reaches `succeeded` within the timeout,
and 1 otherwise, saying why on stderr. A green /readyz alone does not prove that jobs flow:
this also needs the database, a worker and the claim path to work. Standard library only, so
it runs with the host's system Python and nothing installed. The key comes from SMOKE_API_KEY
(or --key): on a command line, any user of the host could read it.
"""

import argparse
import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

JOB = {"task": "sleep", "payload": {"ms": 10}}


def call(
    method: str, url: str, key: str | None, body: object, request_id: str
) -> tuple[int, dict[str, Any]]:
    """One HTTP call. Returns (status, JSON body); status 0 means no usable answer: none at
    all, or one cut short."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/json")
    req.add_header("X-Request-ID", request_id)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if key:
        req.add_header("Authorization", f"Bearer {key}")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, _json(resp.read())
    except urllib.error.HTTPError as exc:  # an error status; its body, if it arrives, says why
        try:
            body = exc.read()
        except (http.client.HTTPException, OSError):
            body = b""
        return exc.code, _json(body)
    except (urllib.error.URLError, http.client.HTTPException, OSError) as exc:
        # URLError and OSError: nothing answered. HTTPException: an answer cut short.
        return 0, {"error": str(getattr(exc, "reason", exc)) or type(exc).__name__}


def _json(raw: bytes) -> dict[str, Any]:
    try:
        parsed = json.loads(raw or b"{}")
    except ValueError:
        return {"body": raw[:200].decode(errors="replace")}
    return parsed if isinstance(parsed, dict) else {"body": parsed}


def smoke(base_url: str, key: str, timeout: float) -> tuple[bool, str]:
    """(passed, what happened)."""
    base = base_url.rstrip("/")
    request_id = f"smoke-{int(time.time())}"
    started = time.monotonic()
    status, body = call("GET", f"{base}/readyz", None, None, request_id)
    if status != 200:
        return False, f"/readyz answered {status or 'nothing'}: {body}"
    enqueued = time.monotonic()
    status, job = call("POST", f"{base}/v1/jobs", key, JOB, request_id)
    if status != 201:
        return False, f"enqueue answered {status or 'nothing'}: {job}"
    job_id = job.get("id")
    if not job_id:
        return False, f"enqueue answered 201 without a job id: {job}"
    state = job.get("status")
    while time.monotonic() - started < timeout:
        status, job = call("GET", f"{base}/v1/jobs/{job_id}", key, None, request_id)
        if status == 200:
            state = job.get("status")
            if state == "succeeded":
                took = time.monotonic() - enqueued
                return True, f"job {job_id} succeeded {took:.2f} s after enqueue ({request_id})"
            if state in ("dead", "cancelled"):
                return False, f"job {job_id} ended {state}: {job.get('last_error')}"
        time.sleep(0.5)
    return False, f"job {job_id} still {state} after {timeout:g} s: are the workers running?"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://localhost")
    parser.add_argument(
        "--key",
        default=os.environ.get("SMOKE_API_KEY"),
        help="the smoke tenant's API key (default: $SMOKE_API_KEY)",
    )
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args(argv)
    if not args.key:
        parser.error("no API key: set SMOKE_API_KEY (or pass --key)")
    passed, what = smoke(args.url, args.key, args.timeout)
    if passed:
        print(f"smoke ok: {what}")
    else:
        print(f"smoke FAILED: {what}", file=sys.stderr)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
