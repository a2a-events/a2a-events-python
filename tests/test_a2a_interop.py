"""Interoperability with the official A2A SDK (``a2a-sdk``, A2A v1.0).

These tests distinguish **A2A core conformance** (the AgentCard, Message, and
JSON-RPC envelope shapes this project produces must be accepted by the
official SDK's types) from **A2A Events extension conformance** (covered by
the conformance-fixture tests). They skip when ``a2a-sdk`` is not installed,
so the SDK stays an optional dev dependency, never a runtime one.
"""

from __future__ import annotations

import inspect

import pytest
from conftest import SUBSCRIBER_CARD, TOPIC, Harness

from a2a_events import DeliveryMode, DeliveryPreference, methods
from a2a_events.models import EXTENSION_URI

a2a_types = pytest.importorskip("a2a.types", reason="a2a-sdk not installed")
json_format = pytest.importorskip("google.protobuf.json_format")


async def _captured_a2a_message(harness: Harness) -> dict:
    """Subscribe with a2a-message delivery and capture the raw wire message."""
    captured: list[dict] = []
    original = harness.transport.send_a2a_message

    async def capture(endpoint: str, message: dict):  # type: ignore[no-untyped-def]
        captured.append(message)
        return await original(endpoint, message)

    harness.transport.send_a2a_message = capture  # type: ignore[method-assign]
    await harness.publisher.subscribe(
        SUBSCRIBER_CARD,
        [TOPIC],
        DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
        lease_seconds=3600,
    )
    await harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://x"})
    assert captured
    return captured[0]


# --- A2A core conformance ----------------------------------------------------


async def test_delivery_message_parses_as_a2a_message(harness: Harness):
    envelope = await _captured_a2a_message(harness)
    msg = json_format.ParseDict(envelope["message"], a2a_types.Message())
    assert msg.role == a2a_types.Role.ROLE_AGENT
    assert msg.message_id, "A2A v1.0 requires messageId"
    assert len(msg.parts) == 1
    # The CloudEvent rides in a DataPart; the extension metadata is keyed by
    # the extension URI in Message.metadata.
    part_dict = json_format.MessageToDict(msg.parts[0])
    assert "data" in part_dict
    meta = json_format.MessageToDict(msg.metadata)
    assert EXTENSION_URI in meta
    assert meta[EXTENSION_URI]["kind"] == "event.delivery"


def test_send_message_method_name_matches_official_sdk():
    """Our A2A core method constant must equal the SDK's wire string."""
    import a2a.client.transports.jsonrpc as sdk_jsonrpc

    source = inspect.getsource(sdk_jsonrpc)
    assert f"method='{methods.A2A_SEND_MESSAGE}'" in source


def test_extension_header_matches_official_sdk():
    from a2a.extensions.common import HTTP_EXTENSION_HEADER

    from a2a_events.server import EXTENSIONS_HEADER

    assert EXTENSIONS_HEADER == HTTP_EXTENSION_HEADER


def test_subscriber_agent_card_parses_as_a2a_card():
    """The documented subscriber AgentCard shape is a valid A2A v1.0 card."""
    card = {
        "name": "Enrichment Agent",
        "description": "Enriches discovered AgentCards.",
        "version": "1.0.0",
        "supportedInterfaces": [
            {"url": "https://agent-a.example.com/a2a/v1", "protocolBinding": "JSONRPC"}
        ],
        "capabilities": {
            "extensions": [
                {
                    "uri": EXTENSION_URI,
                    "description": "A2A Events subscriber",
                    "params": {
                        "role": "subscriber",
                        "receiveUrl": "https://agent-a.example.com/a2a-events/receive",
                        "acceptedDeliveryModes": ["webhook", "a2a-message"],
                    },
                }
            ]
        },
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "skills": [],
    }
    parsed = json_format.ParseDict(card, a2a_types.AgentCard())
    assert parsed.supported_interfaces[0].url == "https://agent-a.example.com/a2a/v1"

    from a2a.extensions.common import find_extension_by_uri

    ext = find_extension_by_uri(parsed, EXTENSION_URI)
    assert ext is not None
    params = json_format.MessageToDict(ext.params)
    assert params["role"] == "subscriber"

    # And our own parser resolves the same endpoints from that card.
    from a2a_events import parse_subscriber_card

    ours = parse_subscriber_card("https://agent-a.example.com/card.json", card)
    assert ours.a2a_endpoint == "https://agent-a.example.com/a2a/v1"
    assert ours.receive_url == "https://agent-a.example.com/a2a-events/receive"


# --- A2A Events extension conformance (namespace disjointness) ---------------


def test_extension_methods_stay_out_of_core_namespace():
    """a2a.events.* dotted names can never collide with A2A core's
    unprefixed PascalCase method namespace."""
    import a2a.client.transports.jsonrpc as sdk_jsonrpc

    source = inspect.getsource(sdk_jsonrpc)
    for method in methods.CANONICAL_METHODS:
        assert method.startswith("a2a.events.")
        assert f"method='{method}'" not in source
