"""deploy/deploy.sh's decisions against a fake docker: what it deploys, when it rolls back,
what it records, and its exit codes. deploy/rehearse.sh runs the same script against real
containers; this covers the paths the rehearsal does not (a failed rollback, a failed redeploy of
the live tag, a broken first release, a rollback while the registry is down, a Grafana data source
change, pruning, the lock), in seconds."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.helpers import ROOT

DEPLOY_SH = ROOT / "deploy/deploy.sh"

pytestmark = pytest.mark.skipif(
    sys.platform == "win32"
    or any(shutil.which(tool) is None for tool in ("bash", "diff", "flock", "rsync", "setsid")),
    reason="needs bash, diff, flock, rsync and setsid (Linux)",
)

# Stands in for the docker CLI. Every call is logged as "<release tag> <arguments>"; the
# FAKE_* variables list the release tags that fail at each step.
FAKE_DOCKER = r"""#!/usr/bin/env bash
tag="${HOPPER_IMAGE##*:}"
echo "${tag:--} $*" >>"$FAKE_CALLS"
listed() { [[ ",$2," == *",$1,"* ]]; }
case "$1" in
pull) # FAKE_OFFLINE: the registry is down, but this host still has a copy
	! listed "${3##*:}" "${FAKE_MISSING:-},${FAKE_OFFLINE:-}"
	;;
image) # image inspect <image>: on this host unless it was never pulled (FAKE_MISSING)
	[ "$2" != inspect ] || ! listed "${3##*:}" "${FAKE_MISSING:-}"
	;;
create)
	echo "cid-${2##*:}"
	;;
cp) # cp --quiet cid-<tag>:/app/release/. <dir>/
	from="${3%%:*}"
	from="${from#cid-}"
	grafana="$4/deploy/grafana/provisioning"
	mkdir -p "$4/docker" "$4/deploy/prometheus" "$grafana/dashboards" "$grafana/datasources"
	touch "$4/docker/compose.prod.yaml" "$4/deploy/Caddyfile"
	echo "a dashboard from $from" >"$grafana/dashboards/hopper.json"   # new in every release
	echo "${FAKE_DATASOURCE:-prometheus}" >"$grafana/datasources/prometheus.yml"
	cat >"$4/deploy/smoke.py" <<EOF
import os, sys
assert os.environ["SMOKE_API_KEY"].startswith("hop_live_"), "no key in the environment"
sys.exit(1 if "$from" in os.environ.get("FAKE_BROKEN_SMOKE", "").split(",") else 0)
EOF
	;;
compose)
	args=" $* "
	case "$args" in
	*" --smoke-key "*) echo "hop_live_abcd1234_$(printf 'A%.0s' $(seq 43))" ;;
	*" run --rm migrate "*) ! listed "$tag" "${FAKE_BROKEN_MIGRATE:-}" ;;
	*" up "*)
		listed "$tag" "${FAKE_BROKEN_UP:-}" && exit 1
		touch "$FAKE_UP"
		;;
	*" ps -q "*) if [ -f "$FAKE_UP" ]; then echo c0ffee; fi ;;
	esac
	;;
esac
"""


class Host:
    """A deploy root with .env, and a PATH whose docker is the fake."""

    def __init__(self, root: Path) -> None:
        self.root = root
        (root / ".env").write_text("POSTGRES_PASSWORD=x\nGRAFANA_ADMIN_PASSWORD=x\n")
        bin_dir = root / "bin"
        bin_dir.mkdir()
        (bin_dir / "docker").write_text(FAKE_DOCKER)
        (bin_dir / "docker").chmod(0o755)
        self.env = {
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "HOPPER_ROOT": str(root),
            "SETTLE_SECONDS": "0",
            "FAKE_CALLS": str(root / "calls.log"),
            "FAKE_UP": str(root / "stack-is-up"),
        }

    def run(
        self, *args: str, extra_env: dict[str, str] | None = None, **broken: str
    ) -> subprocess.CompletedProcess[str]:
        env = {**self.env, **(extra_env or {})}
        env.update({f"FAKE_{name.upper()}": tags for name, tags in broken.items()})
        return subprocess.run(
            ["bash", str(DEPLOY_SH), *args],
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

    def deploy(self, *args: str, **broken: str) -> int:
        return self.run(*args, **broken).returncode

    @property
    def live(self) -> tuple[str, str]:
        def read(name: str) -> str:
            path = self.root / name
            return path.read_text().strip() if path.exists() else ""

        return read("current"), read("previous")

    def history(self) -> list[str]:
        lines = (self.root / "deploy-history.log").read_text().splitlines()
        return [line.split(" ", 1)[1] for line in lines]

    def calls(self, *words: str) -> list[str]:
        log = self.root / "calls.log"
        lines = log.read_text().splitlines() if log.exists() else []
        return [line for line in lines if all(w in line for w in words)]


@pytest.fixture
def host(tmp_path: Path) -> Host:
    return Host(tmp_path)


def test_a_first_deploy_then_an_upgrade(host: Host) -> None:
    assert host.deploy("a1") == 0
    assert host.live == ("a1", "")
    assert "SMOKE_API_KEY=hop_live_abcd1234_" in (host.root / ".env").read_text()
    assert host.deploy("a2") == 0
    assert host.live == ("a2", "a1")
    assert host.history() == ["a1 deployed", "a2 deployed"]
    assert [c.split()[0] for c in host.calls("run --rm migrate")] == ["a1", "a2"]
    assert host.calls("a2 compose", "exec -T api-1 python -c")  # drained before replacing


def test_a_release_that_fails_its_health_checks_is_rolled_back_without_migrations(
    host: Host,
) -> None:
    host.deploy("a1")
    host.deploy("a2")
    assert host.deploy("bad", broken_up="bad") == 1
    assert host.live == ("a2", "a1")
    assert host.history()[-1] == "bad failed-rolled-back-to-a2"
    assert [c.split()[0] for c in host.calls("run --rm migrate")] == ["a1", "a2", "bad"]


def test_a_release_that_fails_only_the_smoke_test_is_rolled_back(host: Host) -> None:
    host.deploy("a1")
    assert host.deploy("bad", broken_smoke="bad") == 1
    assert host.live == ("a1", "")
    assert host.history()[-1] == "bad failed-rolled-back-to-a1"


def test_a_failed_redeploy_of_the_live_tag_goes_back_one_further(host: Host) -> None:
    """For example after a bad change to .env. What runs afterwards is what --status says."""
    host.deploy("a1")
    host.deploy("a2")
    assert host.deploy("a2", broken_up="a2") == 1
    assert host.live == ("a1", "a2")
    assert host.history()[-1] == "a2 failed-rolled-back-to-a1"


def test_a_manual_rollback_swaps_the_releases_without_migrations(host: Host) -> None:
    host.deploy("a1")
    host.deploy("a2")
    assert host.deploy("--rollback") == 0
    assert host.live == ("a1", "a2")
    assert host.history()[-1] == "a1 manual-rollback"
    assert len(host.calls("run --rm migrate")) == 2


def test_a_manual_rollback_works_while_the_registry_is_down(host: Host) -> None:
    host.deploy("a1")
    host.deploy("a2")
    result = host.run("--rollback", offline="a1")
    assert result.returncode == 0, result.stderr
    assert "using the copy already on this host" in result.stderr
    assert host.live == ("a1", "a2")


def test_a_rollback_that_fails_too_exits_3(host: Host) -> None:
    host.deploy("a1")
    host.deploy("a2")
    assert host.deploy("bad", broken_up="bad,a2") == 3
    assert host.live == ("a2", "a1")
    assert host.history()[-1] == "bad rollback-failed"


def test_a_broken_first_release_has_nothing_to_roll_back_to(host: Host) -> None:
    assert host.deploy("a1", broken_up="a1") == 1
    assert host.live == ("", "")
    assert host.history() == ["a1 failed"]


def test_failures_before_anything_changes_exit_2(host: Host) -> None:
    host.deploy("a1")
    missing = host.run("gone", missing="gone")
    assert missing.returncode == 2 and "could not fetch" in missing.stderr
    unmigrated = host.run("b", broken_migrate="b")
    assert unmigrated.returncode == 2 and "migrations failed" in unmigrated.stderr
    assert not host.calls("gone compose") and not host.calls("b compose", " up ")
    assert host.live == ("a1", "")
    assert host.deploy("--rollback") == 2  # nothing to go back to yet


def test_grafana_restarts_only_when_its_data_sources_change(host: Host) -> None:
    """Grafana rereads its dashboards by itself, but reads data sources only when it starts."""
    host.deploy("a1")
    host.deploy("a2")  # a new dashboard only
    assert not host.calls("restart grafana")
    assert host.deploy("a3", datasource="another") == 0
    assert host.calls("a3 compose", "restart grafana")
    assert host.deploy("a4", datasource="another") == 0  # unchanged since a3
    assert len(host.calls("restart grafana")) == 1


def test_old_releases_are_pruned_with_their_images(host: Host) -> None:
    for n in range(1, 8):
        assert host.deploy(f"r{n}") == 0
    assert sorted(p.name for p in (host.root / "releases").iterdir()) == [
        "r3",
        "r4",
        "r5",
        "r6",
        "r7",
    ]
    assert host.calls("image rm ghcr.io/srinu-design/hopper:r1")
    assert host.calls("image rm ghcr.io/srinu-design/hopper:r2")


def test_only_one_deploy_runs_at_a_time(host: Host) -> None:
    import fcntl  # Linux only, like the rest of this module

    with (host.root / ".deploy.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        result = host.run("a1")
    assert result.returncode == 2 and "another deploy is running" in result.stderr
    assert not host.calls("compose")


def test_the_forced_command_deploys_and_keeps_the_log_on_the_host(host: Host) -> None:
    result = host.run("--from-ssh", extra_env={"SSH_ORIGINAL_COMMAND": "a1"})
    assert result.returncode == 0, result.stderr
    assert "done: a1 is live" in result.stderr  # followed live over the connection
    logs = list((host.root / "logs").glob("*-a1.log"))
    assert len(logs) == 1 and "done: a1 is live" in logs[0].read_text()
    assert host.live == ("a1", "")
