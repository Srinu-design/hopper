"""chaos/demo.py: the one-minute demo kills containers, so it must only ever run locally."""

import inspect

import pytest

from chaos import demo
from hopper import bootstrap


@pytest.mark.parametrize(
    "url", ["http://3.104.222.203", "https://hopper.example.com", "http://10.0.0.5:8000"]
)
def test_the_demo_refuses_any_api_that_is_not_this_machine(url: str) -> None:
    with pytest.raises(SystemExit):
        demo.parse_args(["--base-url", url])


@pytest.mark.parametrize(
    "url", ["http://127.0.0.1:8000", "http://localhost:8000", "http://[::1]:8000"]
)
def test_the_demo_runs_against_a_local_api(url: str) -> None:
    assert demo.parse_args(["--base-url", url]).base_url == url


def test_the_demo_waits_for_enter_unless_told_not_to() -> None:
    assert demo.parse_args([]).pause
    assert not demo.parse_args(["--no-pause"]).pause


def test_the_slow_tenant_snippet_matches_bootstrap() -> None:
    """The 429 step creates its tenant through bootstrap's own helper, run inside the API
    container; this fails if that helper's signature changes under it."""
    assert "rotate_tenant_key('hopper-demo-slow'" in demo.SLOW_TENANT_KEY
    inspect.signature(bootstrap.rotate_tenant_key).bind(
        "hopper-demo-slow", rate_per_sec=1, burst=5, max_queue_depth=100, key_name="demo"
    )
