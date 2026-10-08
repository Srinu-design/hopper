"""The page at /: served with a strict Content-Security-Policy, nothing on it that the policy would
block, every file and anchor it names exists, every GitHub link points at a file in this
repository, and every measured number on it is one the README or the benchmarks report."""

import re
from collections.abc import AsyncIterator
from html.parser import HTMLParser

import httpx
import pytest

from hopper.api.main import create_app
from tests.helpers import ROOT

PAGE = ROOT / "src/hopper/web/static/index.html"
REPO = "https://github.com/Srinu-design/hopper"
_VOID = {"img", "input", "link", "meta", "br"}


class _Page(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []
        self.ids: set[str] = set()
        self.inline: list[str] = []
        self.figures: list[str] = []
        self._open: list[tuple[str, list[str] | None]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        found = self._attrs(tag, attrs)
        if tag not in _VOID:
            self._open.append((tag, [] if "data-figure" in found else None))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._attrs(tag, attrs)  # <path ... />: nothing to close

    def _attrs(self, tag: str, attrs: list[tuple[str, str | None]]) -> dict[str, str | None]:
        found = dict(attrs)
        for name in ("href", "src"):
            if found.get(name):
                self.links.append(str(found[name]))
        if found.get("id"):
            self.ids.add(str(found["id"]))
        # The CSP allows no inline script or style, so any of these would silently do nothing.
        if tag == "style" or (tag == "script" and "src" not in found):
            self.inline.append(f"<{tag}>")
        self.inline += [f"{tag} {n}=" for n in found if n == "style" or n.startswith("on")]
        return found

    def handle_endtag(self, tag: str) -> None:
        while self._open:
            name, text = self._open.pop()
            if text is not None:
                self.figures.append(" ".join("".join(text).split()))
            if name == tag:
                break

    def handle_data(self, data: str) -> None:
        for _, text in self._open:
            if text is not None:
                text.append(data)


@pytest.fixture(scope="module")
def page() -> _Page:
    parser = _Page()
    parser.feed(PAGE.read_text())
    return parser


@pytest.fixture
async def anon() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=create_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_the_page_is_served_at_the_root_with_a_strict_policy(anon: httpx.AsyncClient) -> None:
    response = await anon.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    csp = response.headers["content-security-policy"]
    assert "script-src 'self';" in csp and "connect-src 'self';" in csp
    assert "unsafe" not in csp
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "no-referrer"


async def test_the_page_is_not_part_of_the_api_schema(anon: httpx.AsyncClient) -> None:
    paths = (await anon.get("/openapi.json")).json()["paths"]
    assert "/" not in paths and not any(p.startswith("/static") for p in paths)


def test_nothing_on_the_page_needs_inline_script_or_style(page: _Page) -> None:
    assert page.inline == []


async def test_every_local_file_and_anchor_exists(page: _Page, anon: httpx.AsyncClient) -> None:
    for link in page.links:
        if link.startswith("#"):
            assert link[1:] in page.ids, link
        elif link == "/grafana/":
            continue  # served by Caddy, next to the API
        elif link.startswith("/"):
            response = await anon.get(link)
            assert response.status_code == 200, link
            if link.startswith("/static/"):
                assert response.headers["x-content-type-options"] == "nosniff", link


def test_every_github_link_points_at_a_file_in_this_repository(page: _Page) -> None:
    external = [link for link in page.links if link.startswith("http")]
    assert external, "the page links to the repository"
    for link in external:
        assert link.startswith(REPO), f"{link}: only this repository's own pages"
        path = re.sub(r"^/(blob|tree)/main/", "", link.removeprefix(REPO).split("#")[0])
        assert path == "" or (ROOT / path).exists(), link


def test_every_number_on_the_page_is_one_the_docs_report(page: _Page) -> None:
    """The page repeats the README's measured numbers; a re-run that changes them there must
    change them here too."""
    docs = " ".join(
        " ".join((ROOT / f).read_text().replace("**", "").split())
        for f in ("README.md", "docs/benchmarks.md")
    )
    assert len(page.figures) >= 10
    for figure in page.figures:
        assert figure in docs, f"{figure!r} is not in README.md or docs/benchmarks.md"


def test_the_image_carries_the_readme_images() -> None:
    """docs/ is left out of the image, except docs/images, which the page shows."""
    ignored = (ROOT / ".dockerignore").read_text().splitlines()
    assert ignored.index("!docs/images") > ignored.index("docs")
    dockerfile = (ROOT / "docker/Dockerfile").read_text()
    assert "COPY docs/images ./src/hopper/web/static/images" in dockerfile
