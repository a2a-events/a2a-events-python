"""Opaque page tokens for list methods (DESIGN.md §14.5, §29).

List endpoints (``ListSubscriptions``, ``ListDeliveryAttempts``) return a
``nextPageToken`` the caller passes back to fetch the next page. The token is an
opaque, base64url-encoded keyset cursor — the stable id of the last item on the
page — so pagination is stable under concurrent inserts (new rows append after
the cursor rather than shifting an offset).
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from typing import TypeVar

from .errors import A2AEventsError, ErrorCode

_T = TypeVar("_T")


def encode_page_token(after: str) -> str:
    raw = json.dumps({"after": after}, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode_page_token(token: str | None) -> str | None:
    if not token:
        return None
    padding = "=" * (-len(token) % 4)
    try:
        data = json.loads(base64.urlsafe_b64decode(token + padding))
        after = data["after"]
    except (ValueError, KeyError, TypeError) as exc:
        raise A2AEventsError(
            ErrorCode.INVALID_CURSOR,
            "Invalid page token.",
            {"pageToken": token},
        ) from exc
    if not isinstance(after, str):
        raise A2AEventsError(
            ErrorCode.INVALID_CURSOR, "Invalid page token.", {"pageToken": token}
        )
    return after


def paginate(
    items: list[_T], key: Callable[[_T], str], page_token: str | None, page_size: int
) -> tuple[list[_T], str | None]:
    """Return one page of ``items`` plus the token for the following page.

    ``items`` must be in a stable total order; ``key`` extracts each item's
    stable id. A token whose id is no longer present yields an empty final page.
    """
    after = decode_page_token(page_token)
    start = 0
    if after is not None:
        ids = [key(i) for i in items]
        start = ids.index(after) + 1 if after in ids else len(items)
    page = items[start : start + page_size]
    next_token = None
    if page and start + page_size < len(items):
        next_token = encode_page_token(key(page[-1]))
    return page, next_token
