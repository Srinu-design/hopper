import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run the real alembic CLI against database_url, exactly as the migrate container does."""
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        capture_output=True,
        text=True,
        check=False,
    )
