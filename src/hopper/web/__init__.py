"""The web page at /: what Hopper is, how to call it, and a small console that drives the API
with a tenant's API key.

Static files only. The console calls the same /v1 routes as any other client, with the key
the person types in, so the page adds no server logic and no new way in.
"""

from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI
from fastapi.responses import FileResponse
from starlette.responses import Response
from starlette.staticfiles import StaticFiles

STATIC = Path(__file__).parent / "static"

# The README's images live once, in docs/images. A checkout serves them from there; the image
# has no docs/, so the Dockerfile copies them next to the page instead.
_IMAGE_DIRS = (STATIC / "images", Path(__file__).resolve().parents[3] / "docs" / "images")

# The console keeps an API key where script can read it, so only the page's own script may run:
# no inline script or style, no other origin, and no framing.
PAGE_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; "
        "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    ),
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    # Revalidated on every load (the ETag makes that cheap), so a deploy shows at once.
    "Cache-Control": "no-cache",
}


class _StaticFiles(StaticFiles):
    def file_response(self, *args: Any, **kwargs: Any) -> Response:
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response


router = APIRouter(include_in_schema=False)


@router.get("/")
async def page() -> FileResponse:
    return FileResponse(STATIC / "index.html", headers=PAGE_HEADERS)


def install(app: FastAPI) -> None:
    images = next((d for d in _IMAGE_DIRS if d.is_dir()), _IMAGE_DIRS[0])
    app.mount("/static/images", _StaticFiles(directory=images, check_dir=False), name="images")
    app.mount("/static", _StaticFiles(directory=STATIC), name="static")
    app.include_router(router)
