#!/usr/bin/env bash
# Deploy one release of Hopper on this host, with a smoke test and automatic rollback.
#
#   deploy.sh <tag>        deploy $HOPPER_IMAGE_REPO:<tag> (CI passes the git sha)
#   deploy.sh --rollback   go back to the previous good release, without migrations
#   deploy.sh --status     print the live and previous tags
#   deploy.sh --from-ssh   forced command of the CI deploy key (see docs/deploy.md): the one
#                          word the key sent (a tag, --rollback or --status) arrives in
#                          SSH_ORIGINAL_COMMAND, and nothing else can run
#
# A new release goes through these steps, each logged with a time stamp:
#   1. pull the image and copy its deploy bundle (compose file, Caddy, Prometheus and Grafana
#      config, smoke test) out of it into releases/<tag>: config always matches the code
#   2. run the migrations with the new image; they are backward compatible, so the release
#      still running keeps working on the new schema (ADR-0011)
#   3. install the bundle's config into config/ and reload Caddy and Prometheus in place
#      (Grafana restarts only when its data sources or alerting changed)
#   4. replace api-1 and wait until it is healthy, then api-2; Caddy sends every request to
#      whichever replica is up, so the API never goes down, even for a broken release
#   5. replace the workers, schedulers and the rest; Caddy itself restarts (briefly refusing
#      connections) only when its image or settings change, such as a new SITE_ADDRESS
#   6. smoke test through Caddy: /readyz, then a real job must reach `succeeded`
# If 3 to 6 fail, the previous release comes back the same way, minus the migrations, and is
# smoke tested too. Exit codes: 0 deployed, 1 failed and rolled back (or nothing to roll back
# to), 2 bad usage or a failure before the running release was touched, 3 the rollback failed
# as well.
set -euo pipefail

ROOT="${HOPPER_ROOT:-/opt/hopper}"
REPO="${HOPPER_IMAGE_REPO:-ghcr.io/srinu-design/hopper}"
PROJECT="${HOPPER_PROJECT:-hopper}"
SMOKE_URL="${SMOKE_URL:-http://localhost}"
WAIT="${HEALTH_TIMEOUT:-120}"   # seconds a replica gets to become healthy
SETTLE="${SETTLE_SECONDS:-5}"   # then this long for Caddy's 2 s health probe to see it
KEEP_RELEASES=5
KEEP_LOGS=100

# Note: bash ignores `set -e` inside a function called from `if` or `||`, which is how most
# steps below are called, so every step checks its own commands with `|| return 1`.

log() { printf '%s deploy: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
fail() { log "ERROR: $*"; exit 2; }
valid_tag() { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; }

status() {
	printf 'current=%s previous=%s\n' "$(cat "$ROOT/current" 2>/dev/null || true)" \
		"$(cat "$ROOT/previous" 2>/dev/null || true)"
}

compose() { # <tag> <compose args...>: compose for that release's bundle and image
	local tag="$1"
	shift
	HOPPER_IMAGE="$REPO:$tag" HOPPER_ROOT="$ROOT" docker compose \
		--project-name "$PROJECT" --env-file "$ROOT/.env" \
		-f "$ROOT/releases/$tag/docker/compose.prod.yaml" "$@"
}

fetch() { # <tag>: pull the image and unpack its deploy bundle
	local tag="$1" image="$REPO:$1" tmp cid
	log "pulling $image"
	if ! docker pull --quiet "$image" >/dev/null; then
		# A rollback must not depend on the registry: the previous release is still on this host.
		docker image inspect "$image" >/dev/null 2>&1 || return 1
		log "could not pull $image; using the copy already on this host"
	fi
	[ -d "$ROOT/releases/$tag" ] && return 0
	tmp="$(mktemp -d "$ROOT/releases/.incoming.XXXXXX")" || return 1
	cid="$(docker create "$image")" || {
		rm -rf "$tmp"
		return 1
	}
	docker cp --quiet "$cid:/app/release/." "$tmp/" || {
		log "$image has no deploy bundle (/app/release): images built before Week 7 cannot be deployed"
		docker rm "$cid" >/dev/null
		rm -rf "$tmp"
		return 1
	}
	docker rm "$cid" >/dev/null || true
	mv "$tmp" "$ROOT/releases/$tag"
}

migrate() { # <tag>
	log "migrating with $1"
	compose "$1" run --rm migrate
}

ensure_smoke_key() { # <tag>: the first deploy creates the smoke tenant's key and keeps it in .env
	grep -q '^SMOKE_API_KEY=hop_live_' "$ROOT/.env" && return 0
	local key
	key="$(compose "$1" run --rm --no-deps -T migrate python -m hopper.bootstrap --smoke-key)"
	[[ "$key" =~ ^hop_live_[a-z0-9]{8}_[A-Za-z0-9_-]{43}$ ]] || fail "could not create a smoke key"
	sed -i '/^SMOKE_API_KEY=/d' "$ROOT/.env" || return 1
	printf 'SMOKE_API_KEY=%s\n' "$key" >>"$ROOT/.env" || return 1
	log "created the smoke test's API key (stored in .env)"
}

install_config() { # <tag>: config/ is what Caddy, Prometheus and Grafana mount
	local bundle="$ROOT/releases/$1/deploy" grafana_restart=0
	mkdir -p "$ROOT/config/caddy" "$ROOT/config/prometheus" "$ROOT/config/grafana" || return 1
	# Grafana rereads its dashboards every 30 s, but its data sources and alerting only when it
	# starts: if anything else in its provisioning changes, it must restart.
	diff -rq -x dashboards "$bundle/grafana" "$ROOT/config/grafana" >/dev/null 2>&1 ||
		grafana_restart=1
	# rsync keeps each directory itself (the mounts point at it) and replaces what is inside.
	rsync -a --delete "$bundle/prometheus/" "$ROOT/config/prometheus/" || return 1
	rsync -a --delete "$bundle/grafana/" "$ROOT/config/grafana/" || return 1
	rsync -a "$bundle/Caddyfile" "$ROOT/config/caddy/Caddyfile" || return 1
	# Caddy and Prometheus reload in place. On the first deploy none of the three runs yet, and
	# `up` starts them with this config.
	if [ -n "$(compose "$1" ps -q caddy)" ]; then
		compose "$1" exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile ||
			return 1
	fi
	if [ -n "$(compose "$1" ps -q prometheus)" ]; then
		compose "$1" kill -s SIGHUP prometheus >/dev/null || return 1
	fi
	if [ "$grafana_restart" = 1 ] && [ -n "$(compose "$1" ps -q grafana)" ]; then
		log "restarting grafana: its data sources or alerting changed"
		# Only the view-only dashboard depends on it: a failure here must not fail the release.
		compose "$1" restart grafana >/dev/null ||
			log "could not restart grafana; do it by hand: docker compose -p $PROJECT restart grafana"
	fi
}

start() { # <tag>: run a release, one API replica at a time, then everything else
	local tag="$1" replica
	install_config "$tag" || return 1
	# Postgres and Redis first (a no-op once they run), so the replicas start against them.
	compose "$tag" up -d --wait --wait-timeout "$WAIT" postgres redis || return 1
	for replica in api-1 api-2; do
		# Drain first: /readyz answers 503 while the replica still serves, so Caddy's 2 s probe
		# takes it out of rotation before it stops. Stopping it outright leaves Caddy sending it
		# requests until its next probe, and the rehearsal saw some of those time out.
		if [ -n "$(compose "$tag" ps -q "$replica")" ]; then
			log "draining $replica"
			compose "$tag" exec -T "$replica" python -c \
				"import pathlib; from hopper.config import get_settings as s; pathlib.Path(s().drain_file).touch()" ||
				log "could not drain $replica; replacing it anyway"
			sleep "$SETTLE"
		fi
		log "starting $replica on $tag"
		# --force-recreate: the new container never inherits the drain mark, even when the
		# image and config are unchanged (a redeploy of the live tag).
		compose "$tag" up -d --no-deps --force-recreate --wait --wait-timeout "$WAIT" "$replica" || {
			log "$replica did not become healthy"
			return 1
		}
		# Healthy for Docker is not yet healthy for Caddy: give its probe the same time to see it
		# before the other replica drains, or for a moment neither would be in rotation.
		sleep "$SETTLE"
	done
	log "starting workers, schedulers and the rest on $tag"
	compose "$tag" up -d --remove-orphans --wait --wait-timeout "$WAIT" || return 1
}

smoke() { # <tag>: a few tries, because Caddy's health probe needs a moment to notice a replica
	local tag="$1" key try
	key="$(sed -n 's/^SMOKE_API_KEY=//p' "$ROOT/.env" | tail -n 1)"
	for try in 1 2 3; do
		# The key goes in the environment: on the command line any local user could read it.
		if SMOKE_API_KEY="$key" python3 "$ROOT/releases/$tag/deploy/smoke.py" \
			--url "$SMOKE_URL" --timeout 30; then
			return 0
		fi
		log "smoke test failed (try $try of 3)"
		[ "$try" -eq 3 ] || sleep 5
	done
	return 1
}

record() { # <tag> <result>
	printf '%s %s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2" >>"$ROOT/deploy-history.log"
}

prune() { # keep the newest releases, and always the current and previous ones
	local current previous dir name
	current="$(cat "$ROOT/current" 2>/dev/null || true)"
	previous="$(cat "$ROOT/previous" 2>/dev/null || true)"
	# shellcheck disable=SC2012  # tags are [A-Za-z0-9._-], so ls output is safe to read
	ls -1t "$ROOT/releases" | tail -n +$((KEEP_RELEASES + 1)) | while read -r dir; do
		if [ "$dir" != "$current" ] && [ "$dir" != "$previous" ]; then
			rm -rf "${ROOT:?}/releases/$dir"
			# Its image too: a tagged image is never "dangling", so image prune keeps it forever.
			docker image rm "$REPO:$dir" >/dev/null 2>&1 || true
		fi
	done
	# shellcheck disable=SC2012  # log names are a time stamp, a process id and a tag
	ls -1t "$ROOT/logs" 2>/dev/null | tail -n +$((KEEP_LOGS + 1)) | while read -r name; do
		rm -f "${ROOT:?}/logs/$name"
	done
	docker image prune -f >/dev/null || true
}

roll_back() { # <to tag> <failed tag>
	local to="$1" failed="$2"
	if [ -z "$to" ]; then
		log "nothing to roll back to: no earlier release has run on this host"
		record "$failed" failed
		exit 1
	fi
	log "ROLLING BACK to $to (no migrations: the schema stays, and $to works with it)"
	if start "$to" && smoke "$to"; then
		log "rolled back: $to is serving again"
		# A failed redeploy of the live tag went back one further: record what runs now.
		if [ "$to" != "$(cat "$ROOT/current" 2>/dev/null || true)" ]; then
			printf '%s\n' "$failed" >"$ROOT/previous"
			printf '%s\n' "$to" >"$ROOT/current"
		fi
		record "$failed" "failed-rolled-back-to-$to"
		exit 1
	fi
	log "ROLLBACK FAILED: $to is not healthy either; look at: docker compose -p $PROJECT ps"
	record "$failed" rollback-failed
	exit 3
}

# The CI key's forced command. What the key sent must be exactly one word, a tag, --rollback or
# --status: never a command (valid_tag allows no spaces, quotes or newlines). The deploy runs
# detached, writing to a log file that this session follows. If the SSH connection drops
# halfway, the deploy still finishes (or rolls back) instead of dying at its next write.
from_ssh() {
	local word="${SSH_ORIGINAL_COMMAND:-}" logfile pid code=0
	case "$word" in
	--status)
		status
		exit 0
		;;
	--rollback) ;;
	*) valid_tag "$word" || fail "expected one tag, --rollback or --status, got: $(printf '%q' "$word")" ;;
	esac
	mkdir -p "$ROOT/logs" || exit 2
	# The process id keeps two sessions in the same second apart.
	logfile="$ROOT/logs/$(date -u +%Y%m%dT%H%M%SZ)-$$-${word#--}.log"
	: >"$logfile" # before tail opens it
	# Without SSH_ORIGINAL_COMMAND, the child can never take this path again.
	env -u SSH_ORIGINAL_COMMAND setsid --wait "$BASH" "$0" "$word" </dev/null >>"$logfile" 2>&1 &
	pid=$!
	tail -n +1 -f --pid="$pid" "$logfile" >&2 || true
	wait "$pid" || code=$?
	log "this log is kept on the host: $logfile"
	exit "$code"
}

main() {
	local target current previous started=$SECONDS
	[ "$#" -eq 1 ] || fail "usage: deploy.sh <tag> | --rollback | --status | --from-ssh"
	case "$1" in
	--status)
		status
		return 0
		;;
	--from-ssh) from_ssh ;;
	esac
	[ -f "$ROOT/.env" ] || fail "$ROOT/.env is missing (see docs/deploy.md)"
	mkdir -p "$ROOT/releases"
	exec 9>"$ROOT/.deploy.lock"
	flock -n 9 || fail "another deploy is running"
	current="$(cat "$ROOT/current" 2>/dev/null || true)"
	previous="$(cat "$ROOT/previous" 2>/dev/null || true)"

	if [ "$1" = --rollback ]; then
		[ -n "$previous" ] || fail "no previous release to roll back to"
		log "manual rollback from ${current:-nothing} to $previous"
		fetch "$previous" || fail "could not fetch $REPO:$previous; nothing was changed"
		ensure_smoke_key "$previous" || fail "could not create the smoke test's API key"
		if start "$previous" && smoke "$previous"; then
			printf '%s\n' "$previous" >"$ROOT/current"
			printf '%s\n' "$current" >"$ROOT/previous"
			record "$previous" manual-rollback
			log "done: $previous is live again ($((SECONDS - started)) s)"
			return 0
		fi
		roll_back "$current" "$previous"
	fi

	target="$1"
	valid_tag "$target" || fail "not a valid image tag: $(printf '%q' "$target")"
	log "deploying $target (live now: ${current:-nothing})"
	fetch "$target" || fail "could not fetch $REPO:$target; nothing was changed"
	migrate "$target" || fail "migrations failed; nothing else was changed"
	ensure_smoke_key "$target" || fail "could not create the smoke test's API key"
	if start "$target" && smoke "$target"; then
		if [ "$target" != "$current" ]; then
			printf '%s\n' "${current}" >"$ROOT/previous"
			printf '%s\n' "$target" >"$ROOT/current"
		fi
		record "$target" deployed
		prune || log "pruning old releases failed (harmless)"
		# This script is installed once by host-setup.sh, so it does not change with a release.
		if ! cmp -s "$0" "$ROOT/releases/$target/deploy/deploy.sh"; then
			log "note: $target ships a different deploy.sh from this one; to use it from the next deploy:"
			log "  install -m 755 $ROOT/releases/$target/deploy/deploy.sh $ROOT/deploy.sh"
		fi
		log "done: $target is live ($((SECONDS - started)) s)"
		return 0
	fi
	log "release $target FAILED its health checks"
	# Redeploying the live tag and failing goes back one further.
	if [ "$target" = "$current" ]; then current="$previous"; fi
	roll_back "$current" "$target"
}

main "$@"
