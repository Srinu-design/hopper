"""Checks on what runs on the server: docker/compose.prod.yaml, the Caddyfile, the image's deploy
bundle, deploy.sh, and how the workflows call it over SSH. Most read the files; a few run the
forced command for real (Linux only). Each one guards a rule from the build guide or ADR-0011."""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.helpers import ROOT

PROD = yaml.safe_load((ROOT / "docker/compose.prod.yaml").read_text(encoding="utf-8"))
DEV = yaml.safe_load((ROOT / "docker/compose.yaml").read_text(encoding="utf-8"))
SERVICES: dict[str, dict[str, Any]] = PROD["services"]
APP = ("migrate", "api-1", "api-2", "worker", "scheduler")
CADDYFILE = (ROOT / "deploy/Caddyfile").read_text(encoding="utf-8")
DEPLOY_SH = ROOT / "deploy/deploy.sh"

# On Windows "bash" is WSL's launcher, not a shell; CI runs these on Linux.
needs_bash = pytest.mark.skipif(
    sys.platform == "win32" or shutil.which("bash") is None, reason="needs bash"
)


def test_only_caddy_is_reachable_from_outside() -> None:
    """Never publish Postgres, Redis, Prometheus, Grafana or the metrics ports on the host."""
    published = {name for name, svc in SERVICES.items() if svc.get("ports")}
    assert published == {"caddy"}
    assert SERVICES["caddy"]["ports"] == ["80:80", "443:443", "443:443/udp"]
    assert all("expose" not in svc for svc in SERVICES.values())


def test_app_services_run_the_release_image_and_never_build() -> None:
    for name in APP:
        assert SERVICES[name]["image"].startswith("${HOPPER_IMAGE:?"), name
        assert "build" not in SERVICES[name], name


def test_the_project_name_is_fixed_so_volumes_survive_release_directories() -> None:
    assert PROD["name"] == "hopper"
    assert {"pgdata", "redisdata", "caddy_data"} <= set(PROD["volumes"])


def test_migrations_never_run_on_a_plain_up() -> None:
    """`up` (and so a rollback) must not migrate; deploy.sh runs migrate explicitly."""
    assert SERVICES["migrate"]["profiles"] == ["migrate"]
    assert SERVICES["migrate"]["restart"] == "no"


def test_two_api_replicas_behind_caddy_with_health_checks() -> None:
    for name in ("api-1", "api-2"):
        api = SERVICES[name]
        assert "/readyz" in " ".join(api["healthcheck"]["test"])
        assert api["networks"]["default"]["aliases"] == ["api"]  # Prometheus scrapes "api"
    assert "reverse_proxy api-1:8000 api-2:8000" in CADDYFILE
    assert "health_uri /readyz" in CADDYFILE
    assert re.search(r"lb_try_duration \d+s", CADDYFILE)


def test_grafana_is_served_under_its_sub_path() -> None:
    grafana = SERVICES["grafana"]["environment"]
    assert grafana["GF_SERVER_SERVE_FROM_SUB_PATH"] == "true"
    assert grafana["GF_SERVER_ROOT_URL"].endswith("/grafana/")
    assert grafana["GF_AUTH_ANONYMOUS_ORG_ROLE"] == "Viewer"
    assert "handle /grafana*" in CADDYFILE


def test_config_comes_from_the_installed_copy_not_the_release_directory() -> None:
    """deploy.sh installs config into $HOPPER_ROOT/config and reloads in place; mounting from a
    release directory would recreate Caddy on every deploy."""
    mounts = [m for svc in SERVICES.values() for m in svc.get("volumes", []) if ":/etc/" in m]
    assert mounts and all(m.startswith("${HOPPER_ROOT:-/opt/hopper}/config/") for m in mounts)


def test_every_container_has_capped_logs_and_restarts() -> None:
    for name, svc in SERVICES.items():
        assert svc["logging"]["options"]["max-size"] == "10m", name
        if name != "migrate":
            assert svc["restart"] == "unless-stopped", name


def test_workers_get_longer_than_their_shutdown_grace() -> None:
    assert SERVICES["worker"]["stop_grace_period"] == "30s"  # SHUTDOWN_GRACE_SECONDS is 25


def test_third_party_images_are_pinned_and_match_development() -> None:
    for name in ("postgres", "redis", "prometheus", "grafana"):
        assert SERVICES[name]["image"] == DEV["services"][name]["image"], name
    for svc in SERVICES.values():
        image = svc["image"]
        if not image.startswith("${"):
            assert re.search(r":v?\d+(\.\d+){1,2}$", image), image  # never latest


def test_secrets_are_required_not_defaulted() -> None:
    raw = (ROOT / "docker/compose.prod.yaml").read_text(encoding="utf-8")
    for secret in ("POSTGRES_PASSWORD", "GRAFANA_ADMIN_PASSWORD"):
        assert f"${{{secret}:?" in raw and f"${{{secret}:-" not in raw


def test_the_image_carries_its_deploy_bundle() -> None:
    dockerfile = (ROOT / "docker/Dockerfile").read_text(encoding="utf-8")
    assert "COPY docker/compose.prod.yaml /app/release/docker/compose.prod.yaml" in dockerfile
    assert "COPY deploy /app/release/deploy" in dockerfile
    ignored = [line.strip() for line in (ROOT / ".dockerignore").read_text().splitlines()]
    assert "deploy" not in ignored and "deploy/" not in ignored


def test_what_ships_to_the_server_keeps_lf_line_endings() -> None:
    """bash and Caddy fail on CRLF. .gitattributes keeps deploy/ and docker/ LF even in a Windows
    checkout; this catches a file written with CRLF since, before it is copied into an image."""
    files = [
        p
        for d in ("deploy", "docker")
        for p in (ROOT / d).rglob("*")
        if p.is_file() and "__pycache__" not in p.parts
    ]
    assert DEPLOY_SH in files
    crlf = [str(p.relative_to(ROOT)) for p in files if b"\r\n" in p.read_bytes()]
    assert crlf == []


@needs_bash
def test_deploy_script_parses() -> None:
    result = subprocess.run(
        ["bash", "-n", str(DEPLOY_SH)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


def test_rollback_skips_migrations() -> None:
    """The old image cannot run the new head: a rollback only starts the previous release."""
    body = DEPLOY_SH.read_text(encoding="utf-8")
    roll_back = body[body.index("roll_back() {") : body.index("main() {")]
    assert "migrate" not in roll_back
    assert 'start "$to" && smoke "$to"' in roll_back


def test_pruned_releases_take_their_images_with_them() -> None:
    """A tagged image is never dangling, so `docker image prune` alone would keep every
    release's image until the disk fills."""
    body = DEPLOY_SH.read_text(encoding="utf-8")
    prune = body[body.index("prune() {") : body.index("roll_back() {")]
    assert 'rm -rf "${ROOT:?}/releases/$dir"' in prune
    assert 'docker image rm "$REPO:$dir"' in prune


def test_the_ci_key_can_only_name_a_tag() -> None:
    """The forced command takes what the key sent as one word, and never runs it."""
    body = DEPLOY_SH.read_text(encoding="utf-8")
    from_ssh = body[body.index("from_ssh() {") : body.index("main() {")]
    assert 'local word="${SSH_ORIGINAL_COMMAND:-}"' in from_ssh
    assert 'valid_tag "$word" || fail' in from_ssh
    assert "env -u SSH_ORIGINAL_COMMAND setsid" in from_ssh  # the child never recurses
    assert "eval" not in body
    assert 'valid_tag() { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; }' in body


def _from_ssh(root: Path, sent: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "HOPPER_ROOT": str(root), "SSH_ORIGINAL_COMMAND": sent}
    return subprocess.run(
        ["bash", str(DEPLOY_SH), "--from-ssh"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


@needs_bash
@pytest.mark.parametrize(
    "sent",
    [
        "",
        "two words",
        "good; touch {marker}",
        "$(touch {marker})",
        "`touch {marker}`",
        "good\ntouch {marker}",
        "--from-ssh",
        "-rf",
        "../releases",
    ],
)
def test_the_forced_command_refuses_anything_but_one_word(sent: str, tmp_path: Path) -> None:
    marker = tmp_path / "ran"
    result = _from_ssh(tmp_path, sent.format(marker=marker))
    assert result.returncode == 2, result.stderr
    assert "expected one tag, --rollback or --status" in result.stderr
    assert not marker.exists()


@needs_bash
def test_the_forced_command_answers_status(tmp_path: Path) -> None:
    (tmp_path / "current").write_text("abc123\n")
    (tmp_path / "previous").write_text("def456\n")
    result = _from_ssh(tmp_path, "--status")
    assert (result.returncode, result.stdout) == (0, "current=abc123 previous=def456\n")


def test_workflows_end_ssh_options_before_the_host() -> None:
    """Without "--", ssh takes a --rollback or --status meant for deploy.sh as its own option
    and fails ("unknown option -- -"), as a test against a real OpenSSH showed."""
    calls = 0
    for path in (ROOT / ".github/workflows").glob("*.yml"):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.lstrip().startswith("#"):
                continue
            for first_argument in re.findall(r"(?<![\w.~/-])ssh\s+(\S+)", line):
                assert first_argument == "--", (path.name, line)
                calls += 1
    assert calls >= 4  # deploy.yml once, drill.yml three times
