"""docs/diagrams.md is the only source of the diagrams; the SVGs the web page shows are drawn from
it by docs/render_diagrams.py. These fail when a diagram changes and is not drawn again
(MERMAID_JS=... make diagrams), or when an SVG is left over from a diagram that is gone."""

import importlib.util
import re
import xml.dom.minidom
from types import ModuleType

import pytest

from tests.helpers import ROOT

OUT = ROOT / "docs/images/diagrams"


@pytest.fixture(scope="module")
def render() -> ModuleType:
    script = ROOT / "docs/render_diagrams.py"
    spec = importlib.util.spec_from_file_location("render_diagrams", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_diagram_is_drawn_from_its_current_text(render: ModuleType) -> None:
    blocks = render.blocks((ROOT / "docs/diagrams.md").read_text())
    assert len(blocks) == 12
    for name, text in blocks.items():
        svg = (OUT / f"{name}.svg").read_text()
        stamp = re.match(r"<!-- Drawn by .*? Source SHA-256: ([0-9a-f]{64}) -->", svg)
        assert stamp, f"{name}.svg was not drawn by docs/render_diagrams.py"
        assert stamp.group(1) == render.source_hash(text), f"{name} changed: run make diagrams"


def test_no_svg_is_left_from_a_diagram_that_is_gone(render: ModuleType) -> None:
    blocks = render.blocks((ROOT / "docs/diagrams.md").read_text())
    assert {p.stem for p in OUT.glob("*.svg")} == set(blocks)


def test_each_svg_is_well_formed_with_a_size() -> None:
    """An <img> needs well-formed XML, and a width and height to know the diagram's shape."""
    for path in OUT.glob("*.svg"):
        root = xml.dom.minidom.parse(str(path)).documentElement
        assert root.tagName == "svg", path.name
        assert int(root.getAttribute("width")) > 0 and int(root.getAttribute("height")) > 0
