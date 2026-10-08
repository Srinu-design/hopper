#!/usr/bin/env python3
"""Render the Mermaid diagrams in docs/diagrams.md to SVG files in docs/images/diagrams/.

    MERMAID_JS=/path/to/mermaid.min.js python3 docs/render_diagrams.py    (or: make diagrams)

docs/diagrams.md is the only source: GitHub draws its Mermaid blocks itself, and this script
draws the same blocks for the web page, which cannot run Mermaid under its Content-Security-
Policy. Each block follows a `<!-- diagram: name -->` line and becomes images/diagrams/name.svg.
Every SVG starts with the SHA-256 of the text it was drawn from, and a test fails when a block
changes without being drawn again.

Needs Google Chrome or Chromium (headless) and Mermaid 11's mermaid.min.js, from npm
(mermaid/dist/mermaid.min.js) or jsDelivr. Standard library only.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from html import unescape
from pathlib import Path

DOCS = Path(__file__).resolve().parent
SOURCE = DOCS / "diagrams.md"
OUT = DOCS / "images" / "diagrams"
BLOCK = re.compile(r"<!-- diagram: ([a-z0-9-]+) -->\s*```mermaid\n(.*?)```", re.DOTALL)
STAMP = "<!-- Drawn by docs/render_diagrams.py from docs/diagrams.md. Source SHA-256: {} -->\n"

# Google's palette: blue for the parts of Hopper, grey lines, dark grey text.
CONFIG = {
    "startOnLoad": False,
    "theme": "base",
    "securityLevel": "strict",
    "fontFamily": "Arial, Helvetica, sans-serif",
    "themeVariables": {
        "fontFamily": "Arial, Helvetica, sans-serif",
        "fontSize": "14px",
        "background": "#ffffff",
        "primaryColor": "#e8f0fe",
        "primaryBorderColor": "#1a73e8",
        "primaryTextColor": "#202124",
        "secondaryColor": "#f1f3f4",
        "secondaryBorderColor": "#9aa0a6",
        "tertiaryColor": "#ffffff",
        "tertiaryBorderColor": "#dadce0",
        "lineColor": "#5f6368",
        "textColor": "#202124",
        "noteBkgColor": "#fef7e0",
        "noteBorderColor": "#f9ab00",
        "actorBkg": "#e8f0fe",
        "actorBorder": "#1a73e8",
        "signalColor": "#3c4043",
        "labelBoxBkgColor": "#f1f3f4",
        "labelBoxBorderColor": "#9aa0a6",
        "activationBkgColor": "#d2e3fc",
        "clusterBkg": "#f8f9fa",
        "clusterBorder": "#dadce0",
        "edgeLabelBackground": "#ffffff",
    },
    "flowchart": {"htmlLabels": True, "curve": "basis", "padding": 12},
    "sequence": {"mirrorActors": False, "actorMargin": 60, "messageMargin": 36},
    "er": {"layoutDirection": "TB"},
}

PAGE = """<!doctype html><meta charset="utf-8"><body>
<div id="stage"></div><pre id="out"></pre>
<script type="application/json" id="blocks">BLOCKS</script>
<script src="mermaid.min.js"></script>
<script>
(async () => {
  const blocks = JSON.parse(document.getElementById("blocks").textContent);
  mermaid.initialize(CONFIG);
  const out = {};
  for (const [name, text] of Object.entries(blocks)) {
    try {
      const { svg } = await mermaid.render("d-" + name, text);
      const stage = document.getElementById("stage");
      stage.innerHTML = svg;
      // A real width and height, so an <img> knows the diagram's size and shape.
      const drawn = stage.querySelector("svg");
      const box = drawn.viewBox.baseVal;
      drawn.setAttribute("width", Math.ceil(box.width));
      drawn.setAttribute("height", Math.ceil(box.height));
      drawn.style.removeProperty("max-width");
      // XMLSerializer, not the HTML string: a stand-alone .svg must be well-formed XML.
      out[name] = new XMLSerializer().serializeToString(drawn);
    } catch (e) {
      out[name] = "ERROR: " + e.message;
    }
  }
  document.getElementById("out").textContent = JSON.stringify(out);
})();
</script>
"""


def blocks(markdown: str) -> dict[str, str]:
    found: dict[str, str] = {}
    for name, text in BLOCK.findall(markdown):
        if name in found:
            raise SystemExit(f"two diagrams are named {name!r}")
        found[name] = text
    return found


def source_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def chrome() -> str:
    for name in (os.environ.get("CHROME"), "google-chrome", "chromium", "chromium-browser"):
        if name and shutil.which(name):
            return name
    raise SystemExit("no Chrome or Chromium found; set CHROME to its path")


def render(found: dict[str, str], mermaid_js: Path) -> dict[str, str]:
    with tempfile.TemporaryDirectory() as tmp:
        shutil.copy(mermaid_js, Path(tmp) / "mermaid.min.js")
        page = PAGE.replace("CONFIG", json.dumps(CONFIG)).replace(
            "BLOCKS", json.dumps(found).replace("</", "<\\/")
        )
        (Path(tmp) / "render.html").write_text(page)
        dom = subprocess.run(
            [
                chrome(),
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--allow-file-access-from-files",
                "--virtual-time-budget=60000",
                "--dump-dom",
                (Path(tmp) / "render.html").as_uri(),
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        ).stdout
    match = re.search(r'<pre id="out">(.*?)</pre>', dom, re.DOTALL)
    if not match or not match.group(1):
        raise SystemExit("Chrome returned no diagrams; is MERMAID_JS a Mermaid 11 build?")
    rendered: dict[str, str] = json.loads(unescape(match.group(1)))
    return rendered


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mermaid", default=os.environ.get("MERMAID_JS"), type=Path)
    args = parser.parse_args()
    if not args.mermaid or not args.mermaid.is_file():
        parser.error("set MERMAID_JS (or --mermaid) to Mermaid 11's mermaid.min.js")
    found = blocks(SOURCE.read_text())
    rendered = render(found, args.mermaid)
    OUT.mkdir(parents=True, exist_ok=True)
    failed = False
    for name, text in found.items():
        svg = rendered.get(name, "ERROR: not rendered")
        if svg.startswith("ERROR"):
            print(f"{name}: {svg}", file=sys.stderr)
            failed = True
            continue
        (OUT / f"{name}.svg").write_text(STAMP.format(source_hash(text)) + svg + "\n")
        print(f"{name}.svg")
    for old in OUT.glob("*.svg"):
        if old.stem not in found:
            old.unlink()
            print(f"removed {old.name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
