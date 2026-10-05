#!/usr/bin/env bash
# Rehearse delivery on a throwaway Linux Docker host, with deploy.sh exactly as the server runs it:
#
#   1. first deploy of good-1                must succeed (and create the smoke key)
#   2. upgrade to good-2 (the new code)      must succeed, running its migrations; sent through
#                                            the deploy key's forced command, as CI sends it
#   3. a release whose /readyz always fails  must roll back on its own
#   4. a release whose workers die at start  must roll back on its own: /readyz stays green,
#                                            only the smoke test's real job can catch it
#   5. deploy.sh --rollback                  must bring good-1 back, on good-2's schema
#
# A prober sends a request through Caddy every 100 ms the whole time and counts failures.
#
#   deploy/rehearse.sh <new image> [previous image]
#
# With a previous image (CI passes main's), good-1 is that release, so step 5 runs the release
# that is live today on the schema the new code migrated to: the proof that the new
# migrations are backward compatible, which every rollback depends on (ADR-0011). Without one,
# or when it predates the deploy bundle (before Week 7), good-1 is the new image too.
#
# CI runs it on every pull request (a fresh runner stands in for the EC2 host). Locally,
# deploy/rehearse-local.sh runs it inside a docker:dind container. It takes ports 80, 443 and
# 5000 and a compose project named hopper-rehearsal: never run it on the real server.
set -euo pipefail

GOOD="${1:?usage: rehearse.sh <new image> [previous image]}"
PREVIOUS="${2:-}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REG=localhost:5000
ROOT="$(mktemp -d)"
export HOPPER_ROOT="$ROOT" HOPPER_IMAGE_REPO="$REG/hopper" HOPPER_PROJECT=hopper-rehearsal
export HEALTH_TIMEOUT=60
FAILURES=0

say() { printf '\n=== %s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
check() { # <description> <expected> <actual>
	if [ "$2" = "$3" ]; then
		printf '    PASS  %s\n' "$1"
	else
		printf '    FAIL  %s (expected %s, got %s)\n' "$1" "$2" "$3"
		FAILURES=$((FAILURES + 1))
	fi
}
secret() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
deploy() { # <args>: prints deploy.sh's exit code; everything it says goes to stderr, live
	local code=0
	bash "$HERE/deploy.sh" "$@" >&2 || code=$?
	echo "$code"
}
deploy_ssh() { # <word>: the same, the way CI sends it, through the deploy key's forced command
	local code=0
	SSH_ORIGINAL_COMMAND="$1" bash "$HERE/deploy.sh" --from-ssh >&2 || code=$?
	echo "$code"
}
smoke_test() { # prints the smoke test's exit code
	local code=0
	SMOKE_API_KEY="$(sed -n 's/^SMOKE_API_KEY=//p' "$ROOT/.env")" python3 "$HERE/smoke.py" >&2 ||
		code=$?
	echo "$code"
}

cleanup() {
	touch "$ROOT/stop-probe"
	if [ "${KEEP:-0}" != 1 ]; then
		docker compose -p hopper-rehearsal down -v --remove-orphans >/dev/null 2>&1 || true
		docker rm -f hopper-rehearsal-registry >/dev/null 2>&1 || true
		rm -rf "$ROOT"
	fi
}
trap cleanup EXIT

FIRST="$GOOD"
if [ -n "$PREVIOUS" ]; then
	if docker pull -q "$PREVIOUS" >/dev/null 2>&1 &&
		docker run --rm --entrypoint test "$PREVIOUS" -f /app/release/deploy/deploy.sh; then
		FIRST="$PREVIOUS"
	else
		echo "note: $PREVIOUS is missing or has no deploy bundle; good-1 is the new image too"
	fi
fi
say "releases: good-1 is $FIRST, good-2 is $GOOD; two broken ones are built on good-2"
docker rm -f hopper-rehearsal-registry >/dev/null 2>&1 || true
docker run -d --name hopper-rehearsal-registry -p 5000:5000 registry:3.1.2 >/dev/null
for _ in $(seq 1 30); do
	if curl -fs "http://$REG/v2/" >/dev/null; then break; fi
	sleep 1
done
docker tag "$FIRST" "$REG/hopper:good-1"
docker tag "$GOOD" "$REG/hopper:good-2"
# Each broken release changes real code in the installed package, as a bad commit would
# (deploy/broken/*.Dockerfile, shared with the rollback drill workflow).
for broken in readyz worker; do
	docker build -q -f "$HERE/broken/$broken.Dockerfile" --build-arg BASE="$GOOD" \
		-t "$REG/hopper:broken-$broken" "$HERE/broken" >/dev/null
done
for tag in good-1 good-2 broken-readyz broken-worker; do
	docker push -q "$REG/hopper:$tag" >/dev/null
done

cat >"$ROOT/.env" <<EOF
POSTGRES_PASSWORD=$(secret)
API_KEY_PEPPER=$(secret)
JWT_SECRET=$(secret)
GRAFANA_ADMIN_PASSWORD=$(secret)
SITE_ADDRESS=:80
PUBLIC_URL=http://localhost
LOG_LEVEL=WARNING
EOF
chmod 600 "$ROOT/.env"

say "1. first deploy: good-1"
check "first deploy succeeds" 0 "$(deploy good-1)"
check "good-1 is live" good-1 "$(cat "$ROOT/current")"

# From here on, a request through Caddy every 100 ms; any answer but 200 counts as downtime.
(
	while [ ! -f "$ROOT/stop-probe" ]; do
		printf '%s %s\n' "$(date -u +%H:%M:%S)" \
			"$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://localhost/healthz || true)"
		sleep 0.1
	done
) >"$ROOT/probe.log" &

say "2. upgrade: good-2, through the forced command as CI sends it"
check "upgrade succeeds" 0 "$(deploy_ssh good-2)"
check "good-2 is live" good-2 "$(cat "$ROOT/current")"
check "its full log is kept on the host" 1 "$(find "$ROOT/logs" -name '*-good-2.log' | wc -l | tr -d ' ')"
check "the forced command refuses anything but one word" 2 "$(deploy_ssh 'good-1; touch /tmp/x')"

say "3. broken release: /readyz always answers 503"
check "deploy fails and rolls back (exit 1)" 1 "$(deploy broken-readyz)"
check "good-2 is still live" good-2 "$(cat "$ROOT/current")"

say "4. broken release: workers exit at start, /readyz stays green"
check "deploy fails and rolls back (exit 1)" 1 "$(deploy broken-worker)"
check "good-2 is still live" good-2 "$(cat "$ROOT/current")"
check "the smoke test passes on good-2 again" 0 "$(smoke_test)"

say "5. manual rollback: deploy.sh --rollback, so good-1 runs on the schema good-2 migrated"
check "rollback succeeds" 0 "$(deploy --rollback)"
check "good-1 is live" good-1 "$(cat "$ROOT/current")"

touch "$ROOT/stop-probe"
wait
total="$(wc -l <"$ROOT/probe.log" | tr -d ' ')"
bad="$(grep -vc ' 200$' "$ROOT/probe.log" || true)"
# What the whole stack takes, to size the server (docs/deploy.md). Only a measurement: it never
# fails the rehearsal.
mapfile -t containers < <(docker compose -p hopper-rehearsal ps -q)
memory="$(docker stats --no-stream --format '{{.MemUsage}}' "${containers[@]}" |
	awk '{ n = $1 + 0; if ($1 ~ /GiB/) n *= 1024; else if ($1 ~ /KiB/) n /= 1024; s += n }
		END { printf "%.0f MiB", s }')" || memory="not measured"
say "results"
echo "    requests through Caddy during steps 2-5: $total, failed: $bad"
echo "    memory used by the stack's ${#containers[@]} containers at the end: $memory"
grep -v ' 200$' "$ROOT/probe.log" | sed 's/^/    failed request at /' | head -20 || true
echo "    deploy history:"
sed 's/^/      /' "$ROOT/deploy-history.log"
check "no request failed during deploys and rollbacks" 0 "$bad"
if [ "$FAILURES" -gt 0 ]; then
	say "REHEARSAL FAILED: $FAILURES check(s)"
	exit 1
fi
say "REHEARSAL PASSED"
