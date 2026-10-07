#!/usr/bin/env bash
# Run the chaos test on the deployed server (docs/deploy.md), against the release that is live
# there, and copy its report back into chaos/results/. Normal mode only: the negative control
# loses jobs on purpose, which has no place on a live server.
#
#   chaos/on-server.sh ubuntu@<ip> -i ~/.ssh/hopper-login.pem
#
# Everything after the host is passed to ssh and scp. Use your own login key: the deploy key
# can only deploy, and IdentitiesOnly keeps ssh from offering it first when an agent holds it
# (the server would take it and answer every command with the forced one). The test runs
# detached on the server with its log kept there, so a dropped connection does not stop it
# halfway (rerun the last step by hand: see the end of this file).
set -euo pipefail
[ $# -ge 1 ] || { echo "usage: $0 user@host [ssh options]" >&2; exit 2; }
HOST="$1"
shift
SSH_OPTS=(-o IdentitiesOnly=yes "$@")
cd "$(dirname "$0")/.."

ssh "${SSH_OPTS[@]}" -- "$HOST" 'mkdir -p hopper-chaos/chaos/results'
scp -q "${SSH_OPTS[@]}" chaos/kill_workers.py "$HOST:hopper-chaos/chaos/"

# shellcheck disable=SC2016  # expanded on the server, on purpose
ssh "${SSH_OPTS[@]}" -- "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
tag="$(/opt/hopper/deploy.sh --status | sed -n 's/^current=\([^ ]*\).*/\1/p')"
[ -n "$tag" ] || { echo "nothing is deployed on this server" >&2; exit 2; }
# The same Compose command deploy.sh uses, so scaling the workers recreates nothing else.
export HOPPER_IMAGE="ghcr.io/srinu-design/hopper:$tag" HOPPER_ROOT=/opt/hopper HOPPER_RELEASE="$tag"
compose="docker compose --project-name hopper --env-file /opt/hopper/.env"
compose+=" -f /opt/hopper/releases/$tag/docker/compose.prod.yaml"
cd ~/hopper-chaos
echo "release $tag: starting the chaos test (about 7 minutes)"
# http://localhost: Caddy always answers it in plain HTTP, whatever SITE_ADDRESS is.
setsid nohup python3 chaos/kill_workers.py --compose "$compose" --env-file /opt/hopper/.env \
	--api-service api-1 --base-url http://localhost --grafana-url http://localhost/grafana \
	>run.log 2>&1 </dev/null &
echo $! >run.pid
REMOTE

# Follow the log until the test ends, then fetch the report.
ssh "${SSH_OPTS[@]}" -- "$HOST" 'tail -n +1 -f hopper-chaos/run.log --pid "$(cat hopper-chaos/run.pid)"'
scp -q "${SSH_OPTS[@]}" "$HOST:hopper-chaos/chaos/results/chaos-*" chaos/results/
echo "reports in chaos/results/ (newest last):"
printf '  %s\n' chaos/results/chaos-*.md | tail -3

# If the connection dropped while following the log, the test still ran to its end. Later:
#   ssh -o IdentitiesOnly=yes <same options> <host> tail hopper-chaos/run.log
#   scp -o IdentitiesOnly=yes <same options> '<host>:hopper-chaos/chaos/results/chaos-*' chaos/results/
