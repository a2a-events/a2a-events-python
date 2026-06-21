"""Error model for A2A Events (DESIGN.md §30).

Errors are surfaced as JSON-RPC 2.0 error objects. The symbolic A2A Events
code travels in ``data.code``; the numeric ``code`` is a JSON-RPC code.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    """Symbolic A2A Events error codes (carried in ``error.data.code``)."""

    EXTENSION_NOT_SUPPORTED = "EXTENSION_NOT_SUPPORTED"
    TOPIC_NOT_FOUND = "TOPIC_NOT_FOUND"
    TOPIC_NOT_AUTHORIZED = "TOPIC_NOT_AUTHORIZED"
    SELECTOR_NOT_SUPPORTED = "SELECTOR_NOT_SUPPORTED"
    INVALID_SELECTOR = "INVALID_SELECTOR"
    INVALID_CURSOR = "INVALID_CURSOR"
    CURSOR_EXPIRED = "CURSOR_EXPIRED"
    SUBSCRIPTION_NOT_FOUND = "SUBSCRIPTION_NOT_FOUND"
    SUBSCRIPTION_EXPIRED = "SUBSCRIPTION_EXPIRED"
    DELIVERY_MODE_NOT_SUPPORTED = "DELIVERY_MODE_NOT_SUPPORTED"
    SUBSCRIBER_CARD_UNREACHABLE = "SUBSCRIBER_CARD_UNREACHABLE"
    SUBSCRIBER_CARD_INVALID = "SUBSCRIBER_CARD_INVALID"
    DELIVERY_ENDPOINT_NOT_DECLARED = "DELIVERY_ENDPOINT_NOT_DECLARED"
    DELIVERY_ENDPOINT_BLOCKED = "DELIVERY_ENDPOINT_BLOCKED"
    SIGNATURE_INVALID = "SIGNATURE_INVALID"
    REPLAY_NOT_SUPPORTED = "REPLAY_NOT_SUPPORTED"
    RATE_LIMITED = "RATE_LIMITED"
    LEASE_TOO_LONG = "LEASE_TOO_LONG"
    LEASE_TOO_SHORT = "LEASE_TOO_SHORT"


# Mapping from symbolic code -> (JSON-RPC numeric code, HTTP status) per §30.
# A2A Events uses the JSON-RPC server-error range -32000..-32099.
_CODE_TABLE: dict[ErrorCode, tuple[int, int]] = {
    ErrorCode.EXTENSION_NOT_SUPPORTED: (-32001, 400),
    ErrorCode.TOPIC_NOT_FOUND: (-32010, 404),
    ErrorCode.TOPIC_NOT_AUTHORIZED: (-32011, 403),
    ErrorCode.SELECTOR_NOT_SUPPORTED: (-32012, 400),
    ErrorCode.INVALID_SELECTOR: (-32013, 400),
    ErrorCode.INVALID_CURSOR: (-32014, 400),
    ErrorCode.CURSOR_EXPIRED: (-32016, 409),
    ErrorCode.SUBSCRIPTION_NOT_FOUND: (-32020, 404),
    ErrorCode.SUBSCRIPTION_EXPIRED: (-32021, 409),
    ErrorCode.DELIVERY_MODE_NOT_SUPPORTED: (-32030, 400),
    ErrorCode.SUBSCRIBER_CARD_UNREACHABLE: (-32031, 502),
    ErrorCode.SUBSCRIBER_CARD_INVALID: (-32032, 400),
    ErrorCode.DELIVERY_ENDPOINT_NOT_DECLARED: (-32033, 400),
    ErrorCode.DELIVERY_ENDPOINT_BLOCKED: (-32034, 403),
    ErrorCode.SIGNATURE_INVALID: (-32040, 401),
    ErrorCode.REPLAY_NOT_SUPPORTED: (-32050, 400),
    ErrorCode.RATE_LIMITED: (-32060, 429),
    ErrorCode.LEASE_TOO_LONG: (-32070, 400),
    ErrorCode.LEASE_TOO_SHORT: (-32071, 400),
}


def http_status_for_error(error: dict[str, Any]) -> int:
    """Map a JSON-RPC error object to an HTTP status (§30, HTTP+JSON binding)."""
    symbolic = (error.get("data") or {}).get("code")
    if symbolic is not None:
        try:
            return _CODE_TABLE[ErrorCode(symbolic)][1]
        except (ValueError, KeyError):
            pass
    # Standard JSON-RPC codes for malformed requests.
    if error.get("code") in (-32600, -32602, -32700):
        return 400
    if error.get("code") == -32601:
        return 404
    return 500


class A2AEventsError(Exception):
    """An A2A Events protocol error that maps to a JSON-RPC error object."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    @property
    def jsonrpc_code(self) -> int:
        return _CODE_TABLE[self.code][0]

    @property
    def http_status(self) -> int:
        return _CODE_TABLE[self.code][1]

    def to_error_object(self) -> dict[str, Any]:
        """Render the JSON-RPC ``error`` member (DESIGN.md §30)."""
        return {
            "code": self.jsonrpc_code,
            "message": self.message,
            "data": {"code": self.code.value, **self.details},
        }
