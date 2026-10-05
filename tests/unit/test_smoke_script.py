"""deploy/smoke.py against a fake API: it passes only when a real job reaches succeeded."""

import importlib.util
import json
import threading
from collections.abc import Iterator
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import ModuleType
from typing import Any

import pytest

from tests.helpers import ROOT


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("smoke", ROOT / "deploy" / "smoke.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


smoke = _load()


class FakeApi:
    """What the fake API answers; each test changes the parts it cares about."""

    def __init__(self) -> None:
        self.readyz = 200
        self.truncate = False  # /readyz promises more bytes than it sends, then hangs up
        self.enqueue = 201
        self.statuses = ["queued", "running", "succeeded"]  # successive GETs of the job
        self.seen: list[tuple[str, str, Message]] = []


@pytest.fixture
def api() -> Iterator[tuple[FakeApi, str]]:
    fake = FakeApi()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_: Any) -> None:
            pass

        def _send(self, status: int, body: dict[str, Any]) -> None:
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:
            fake.seen.append(("GET", self.path, self.headers))
            if self.path == "/readyz" and fake.truncate:
                self.send_response(fake.readyz)
                self.send_header("Content-Length", "100")
                self.end_headers()
                self.wfile.write(b'{"status"')
            elif self.path == "/readyz":
                self._send(fake.readyz, {"status": "ok" if fake.readyz == 200 else "unavailable"})
            elif self.path == "/v1/jobs/j1":
                state = fake.statuses.pop(0) if len(fake.statuses) > 1 else fake.statuses[0]
                self._send(200, {"id": "j1", "status": state, "last_error": "boom"})
            else:
                self._send(404, {})

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            self.rfile.read(length)
            fake.seen.append(("POST", self.path, self.headers))
            if fake.enqueue == 201:
                self._send(201, {"id": "j1", "status": "queued"})
            else:
                self._send(fake.enqueue, {"error": {"code": "unauthorized"}})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield fake, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def test_passes_when_the_job_succeeds(
    api: tuple[FakeApi, str], capsys: pytest.CaptureFixture[str]
) -> None:
    fake, url = api
    assert smoke.main(["--url", url, "--key", "hop_live_x", "--timeout", "10"]) == 0
    assert "smoke ok: job j1 succeeded" in capsys.readouterr().out
    enqueue = next(headers for method, _, headers in fake.seen if method == "POST")
    assert enqueue["Authorization"] == "Bearer hop_live_x"
    assert enqueue["X-Request-ID"].startswith("smoke-")


def test_fails_when_not_ready(api: tuple[FakeApi, str], capsys: pytest.CaptureFixture[str]) -> None:
    fake, url = api
    fake.readyz = 503
    assert smoke.main(["--url", url, "--key", "k"]) == 1
    assert "/readyz answered 503" in capsys.readouterr().err
    assert [m for m, _, _ in fake.seen] == ["GET"]  # gave up before enqueuing anything


def test_fails_when_the_key_is_refused(
    api: tuple[FakeApi, str], capsys: pytest.CaptureFixture[str]
) -> None:
    fake, url = api
    fake.enqueue = 401
    assert smoke.main(["--url", url, "--key", "wrong"]) == 1
    assert "enqueue answered 401" in capsys.readouterr().err


def test_a_ready_api_without_working_workers_fails(
    api: tuple[FakeApi, str], capsys: pytest.CaptureFixture[str]
) -> None:
    """The point of the smoke test: /readyz is green, but the job never runs."""
    fake, url = api
    fake.statuses = ["queued"]
    assert smoke.main(["--url", url, "--key", "k", "--timeout", "1.5"]) == 1
    assert "still queued after 1.5 s: are the workers running?" in capsys.readouterr().err


def test_a_job_that_dies_fails_at_once(
    api: tuple[FakeApi, str], capsys: pytest.CaptureFixture[str]
) -> None:
    fake, url = api
    fake.statuses = ["running", "dead"]
    assert smoke.main(["--url", url, "--key", "k", "--timeout", "10"]) == 1
    assert "ended dead: boom" in capsys.readouterr().err


def test_nothing_listening_fails_cleanly(capsys: pytest.CaptureFixture[str]) -> None:
    assert smoke.main(["--url", "http://127.0.0.1:1", "--key", "k"]) == 1
    assert "/readyz answered nothing" in capsys.readouterr().err


def test_the_key_comes_from_the_environment(
    api: tuple[FakeApi, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """deploy.sh passes the key this way, so it never shows in the host's process list."""
    fake, url = api
    monkeypatch.setenv("SMOKE_API_KEY", "hop_live_from_env")
    assert smoke.main(["--url", url, "--timeout", "10"]) == 0
    enqueue = next(headers for method, _, headers in fake.seen if method == "POST")
    assert enqueue["Authorization"] == "Bearer hop_live_from_env"


def test_no_key_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("SMOKE_API_KEY", raising=False)
    with pytest.raises(SystemExit) as exit_info:
        smoke.main(["--url", "http://127.0.0.1:1"])
    assert exit_info.value.code == 2
    assert "SMOKE_API_KEY" in capsys.readouterr().err


def test_an_answer_cut_short_fails_cleanly(
    api: tuple[FakeApi, str], capsys: pytest.CaptureFixture[str]
) -> None:
    """A truncated response raises http.client.IncompleteRead, which is not an OSError."""
    fake, url = api
    fake.truncate = True
    assert smoke.main(["--url", url, "--key", "k"]) == 1
    assert "/readyz answered nothing: {'error': 'IncompleteRead" in capsys.readouterr().err


def test_an_error_answer_cut_short_fails_cleanly(
    api: tuple[FakeApi, str], capsys: pytest.CaptureFixture[str]
) -> None:
    """The same, for an error status: its body is read in the error handler."""
    fake, url = api
    fake.readyz, fake.truncate = 503, True
    assert smoke.main(["--url", url, "--key", "k"]) == 1
    assert "/readyz answered 503: {}" in capsys.readouterr().err
