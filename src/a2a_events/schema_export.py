"""Build published JSON Schemas from the typed models (spec §27 schemas/).

The committed files under ``schemas/`` are the language-agnostic contract;
``scripts/export_schemas.py`` writes them and ``tests/test_conformance.py``
fails on drift. Keep this logic importable so both can share it.
"""

from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from .errors import ErrorCode
from .models import CloudEvent, Selector, Subscription, Topic

SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
SCHEMA_BASE = "https://a2a-events.github.io/a2a-events/schemas"


def _wrap(name: str, schema: dict[str, Any]) -> dict[str, Any]:
    return {"$schema": SCHEMA_DIALECT, "$id": f"{SCHEMA_BASE}/{name}", **schema}


def error_schema() -> dict[str, Any]:
    """JSON-RPC error object for A2A Events (spec §30)."""
    return {
        "title": "A2AEventsError",
        "description": "JSON-RPC 2.0 error object with an A2A Events symbolic code.",
        "type": "object",
        "required": ["code", "message"],
        "properties": {
            "code": {"type": "integer", "description": "JSON-RPC numeric error code."},
            "message": {"type": "string"},
            "data": {
                "type": "object",
                "required": ["code"],
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Symbolic A2A Events error code.",
                        "enum": [c.value for c in ErrorCode],
                    }
                },
                "additionalProperties": True,
            },
        },
        "additionalProperties": False,
    }


def build_schemas() -> dict[str, dict[str, Any]]:
    """Return ``{filename: schema}`` for every published schema."""
    return {
        "topic.schema.json": _wrap(
            "topic.schema.json", Topic.model_json_schema(by_alias=True)
        ),
        "subscription.schema.json": _wrap(
            "subscription.schema.json", Subscription.model_json_schema(by_alias=True)
        ),
        "event.schema.json": _wrap(
            "event.schema.json", CloudEvent.model_json_schema(by_alias=True)
        ),
        "selector.schema.json": _wrap(
            "selector.schema.json", TypeAdapter(Selector).json_schema(by_alias=True)
        ),
        "error.schema.json": _wrap("error.schema.json", error_schema()),
    }
