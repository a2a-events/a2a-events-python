"""Conformance suite (spec §33).

Drives the language-agnostic fixtures under ``conformance/fixtures/`` against
this implementation, checks the published ``schemas/`` for drift, and
validates sample instances against those schemas.

``schemas/`` and ``conformance/fixtures/`` in this repo are vendored copies of
the language-neutral contract owned by the spec repo (the source of truth):
https://github.com/a2a-events/a2a-events . Re-sync them with
``scripts/sync_spec.py``; the drift test below also fails if the vendored
schemas fall behind the models.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from a2a_events import cursor
from a2a_events.errors import A2AEventsError, ErrorCode
from a2a_events.models import CloudEvent, FieldFilterSelector, Topic
from a2a_events.schema_export import build_schemas
from a2a_events.selectors import matches

REPO = Path(__file__).resolve().parent.parent  # up from tests/ to the repo root
FIXTURES = REPO / "conformance" / "fixtures"
SCHEMAS = REPO / "schemas"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _parse_selector(raw: dict):
    from pydantic import TypeAdapter

    from a2a_events.models import Selector

    return TypeAdapter(Selector).validate_python(raw)


# --- selector vectors (§10.4) -----------------------------------------------

_SELECTORS = _load("selectors.json")


@pytest.mark.parametrize("case", _SELECTORS["cases"], ids=lambda c: c["name"])
def test_selector_vectors(case: dict):
    selector = _parse_selector(case["selector"])
    event = _SELECTORS["event"]
    fields = case.get("default_search_fields", [])
    if "error" in case:
        with pytest.raises(A2AEventsError) as exc:
            matches(selector, event, fields)
        assert exc.value.code == ErrorCode[case["error"]]
    else:
        assert matches(selector, event, fields) is case["match"]


# --- cursor ordering (§10.9) ------------------------------------------------


def test_cursor_ordering_vectors():
    ordered = _load("cursors.json")["ordered"]
    assert sorted(ordered) == ordered  # lexicographic order == event order
    offsets = [cursor.offset_of(c) for c in ordered]
    assert offsets == sorted(offsets)


# --- error mapping (§30) ----------------------------------------------------


@pytest.mark.parametrize("row", _load("errors.json")["codes"], ids=lambda r: r["code"])
def test_error_mapping_vectors(row: dict):
    err = A2AEventsError(ErrorCode[row["code"]], "msg")
    assert err.jsonrpc_code == row["jsonrpc"]
    assert err.http_status == row["http"]
    assert err.to_error_object()["data"]["code"] == row["code"]


# --- schema drift + validity (§27) ------------------------------------------


@pytest.mark.parametrize("name", sorted(build_schemas()))
def test_committed_schema_matches_models(name: str):
    on_disk = json.loads((SCHEMAS / name).read_text())
    assert (
        on_disk == build_schemas()[name]
    ), f"{name} is stale; run scripts/export_schemas.py"


@pytest.mark.parametrize("name", sorted(build_schemas()))
def test_schema_is_valid_jsonschema(name: str):
    Draft202012Validator.check_schema(build_schemas()[name])


def test_sample_instances_validate():
    schemas = build_schemas()
    topic = Topic(name="agent_card.discovered", filterableFields=["data.cardUrl"])
    Draft202012Validator(schemas["topic.schema.json"]).validate(
        topic.model_dump(by_alias=True, mode="json")
    )

    selector = {"type": "field_filter", "where": {"data.cardUrl": ["https://x"]}}
    Draft202012Validator(schemas["selector.schema.json"]).validate(selector)

    event = CloudEvent(
        id="evt_1",
        source="a2a://agent-b.example.com",
        type="org.example.a2a.agent_card.discovered.v1",
        time=datetime(2026, 6, 19, 20, 30, tzinfo=UTC),
        data={"cardUrl": "https://x"},
        a2aevents={  # type: ignore[arg-type]
            "publisherCardUrl": "https://agent-b.example.com/.well-known/agent-card.json",
            "topic": "agent_card.discovered",
            "cursor": "agent_card.discovered:0000000000000000",
        },
    )
    Draft202012Validator(schemas["event.schema.json"]).validate(
        event.model_dump(by_alias=True, mode="json", exclude_none=True)
    )


def test_invalid_instance_is_rejected_by_schema():
    schemas = build_schemas()
    # A field_filter selector missing the required `where` must fail.
    with pytest.raises(ValidationError):
        Draft202012Validator(schemas["selector.schema.json"]).validate(
            {"type": "field_filter"}
        )


def test_field_filter_selector_model_roundtrip():
    # Sanity: the model the schema is generated from accepts a valid selector.
    sel = FieldFilterSelector(where={"data.cardUrl": ["https://x"]})
    assert sel.type == "field_filter"


def test_subscription_schema_present():
    schemas = build_schemas()
    assert "subscription.schema.json" in schemas
    Draft202012Validator.check_schema(schemas["subscription.schema.json"])
