"""CloudEvents 1.0 conformance of the delivered envelope (spec §16).

The A2A Events metadata must ride as *flat scalar extension context
attributes*: CloudEvents extension attributes MUST use the scalar type system
(no map/object values) and lowercase-alphanumeric names of at most 20
characters. These tests pin that, and prove the produced JSON is accepted by
an independent CloudEvents implementation (the official ``cloudevents`` SDK).
"""

from __future__ import annotations

import json
import re

import pytest
from conftest import SUBSCRIBER_CARD, TOPIC, Harness

from a2a_events import DeliveryMode, DeliveryPreference

CORE_ATTRIBUTES = {
    "specversion",
    "id",
    "source",
    "type",
    "subject",
    "time",
    "datacontenttype",
    "dataschema",
    "data",
    "data_base64",
}

_NAME_RE = re.compile(r"^[a-z0-9]+$")


async def _delivered_event(harness: Harness) -> dict:
    await harness.publisher.subscribe(
        SUBSCRIBER_CARD,
        [TOPIC],
        DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
        lease_seconds=3600,
    )
    await harness.publisher.publish(
        TOPIC, "org.example.a2a.agent_card.discovered.v1", {"cardUrl": "https://x"}
    )
    assert harness.subscriber.received
    return harness.subscriber.received[0]


async def test_extension_attributes_are_flat_scalars(harness: Harness):
    event = await _delivered_event(harness)
    extensions = {k: v for k, v in event.items() if k not in CORE_ATTRIBUTES}
    assert extensions, "expected A2A Events extension attributes"
    for name, value in extensions.items():
        # CloudEvents attribute naming: lowercase a-z / 0-9 only, terse.
        assert _NAME_RE.fullmatch(name), name
        assert len(name) <= 20, name
        # CloudEvents type system is scalar-only: no maps, no arrays.
        assert not isinstance(value, (dict, list)), (name, value)


async def test_expected_attribute_set(harness: Harness):
    event = await _delivered_event(harness)
    assert event["a2aextension"].startswith("https://")
    assert event["a2atopic"] == TOPIC
    assert event["a2acursor"].startswith(TOPIC + ":")
    assert event["a2apublisher"].startswith("https://")
    assert event["a2asubscription"].startswith("sub_")
    assert isinstance(event["a2adeliveryattempt"], int)
    assert event["a2atraceid"].startswith("tr_")


async def test_accepted_by_independent_cloudevents_sdk(harness: Harness):
    # cloudevents 2.x moved the API under cloudevents.v1; support both layouts.
    try:
        from cloudevents.v1 import conversion
        from cloudevents.v1.http import CloudEvent as SdkCloudEvent
    except ImportError:
        conversion = pytest.importorskip(
            "cloudevents.conversion", reason="cloudevents SDK not installed"
        )
        from cloudevents.http import CloudEvent as SdkCloudEvent

    event = await _delivered_event(harness)
    parsed = conversion.from_json(SdkCloudEvent, json.dumps(event).encode())
    assert parsed["id"] == event["id"]
    assert parsed["a2atopic"] == TOPIC
    assert parsed["a2acursor"] == event["a2acursor"]
    assert parsed.data == event["data"]
    # Round-trip through the independent implementation preserves the
    # extension attributes.
    rt = json.loads(conversion.to_json(parsed))
    for key in ("a2aextension", "a2atopic", "a2acursor", "a2asubscription"):
        assert rt[key] == event[key]
