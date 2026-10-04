"""Keyset pagination cursors: an opaque bookmark of (timestamp, id) for the last row shown.

Paging by "everything after this bookmark" instead of OFFSET keeps pages fast on a large
table and never skips or repeats a row when new rows arrive between requests.
"""

import base64
import binascii
from datetime import datetime
from uuid import UUID

from hopper.api.errors import ApiError


def encode_cursor(at: datetime, row_id: UUID) -> str:
    return base64.urlsafe_b64encode(f"{at.isoformat()}|{row_id}".encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        at, row_id = base64.urlsafe_b64decode(cursor.encode()).decode().split("|")
        return datetime.fromisoformat(at), UUID(row_id)
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise ApiError(422, "invalid_cursor", "cursor is not valid") from exc
