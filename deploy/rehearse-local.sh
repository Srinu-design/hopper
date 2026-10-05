#!/usr/bin/env bash
# Run deploy/rehearse.sh on your machine, inside a docker:dind container: an isolated Docker
# host, so the rehearsal cannot touch your development stack. Needs only Docker and bash
# (Git Bash on Windows is fine).
#
#   deploy/rehearse-local.sh [image [previous image]]   default hopper:dev (make up builds it)
#
# The inner Docker's images are kept in the volume hopper-rehearsal-cache, so only the first
# run downloads Postgres, Redis, Caddy, Prometheus and Grafana. The log is also written to
# rehearsal.log in the current directory.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash: leave container paths alone

IMAGE="${1:-hopper:dev}"
HOST=hopper-rehearsal-host
# pwd -W gives Git Bash's Windows form (C:/...), which the Windows docker CLI understands.
REPO_DIR="$(cd "$(dirname "$0")/.." && { pwd -W 2>/dev/null || pwd; })"

docker image inspect "$IMAGE" >/dev/null || {
	echo "no image $IMAGE: build it first (make up, or docker compose -f docker/compose.yaml build)" >&2
	exit 2
}
docker rm -f "$HOST" >/dev/null 2>&1 || true
docker run -d --privileged --name "$HOST" -v hopper-rehearsal-cache:/var/lib/docker \
	docker:29.8.2-dind >/dev/null
trap 'docker rm -f "$HOST" >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 60); do
	if docker exec "$HOST" docker info >/dev/null 2>&1; then break; fi
	sleep 1
done
# What deploy.sh needs from an Ubuntu host: GNU tail (--pid), GNU diff (-x), flock and setsid
# (util-linux).
docker exec "$HOST" apk add --no-cache -q bash coreutils curl diffutils python3 rsync \
	util-linux >/dev/null
echo "loading $IMAGE into the rehearsal host"
docker save "$IMAGE" | docker exec -i "$HOST" docker load -q >/dev/null
docker exec "$HOST" mkdir -p /rehearsal
docker cp "$REPO_DIR/deploy" "$HOST:/rehearsal/deploy"
docker exec "$HOST" bash /rehearsal/deploy/rehearse.sh "$IMAGE" ${2:+"$2"} 2>&1 | tee rehearsal.log
exit "${PIPESTATUS[0]}"
