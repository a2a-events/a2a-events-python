"""Optional HTTP+JSON binding over the publisher app (spec §29 table)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from a2a_events import (
    A2AEventsPublisher,
    InMemoryTransport,
    PublisherConfig,
    SigningKey,
    SubscriberCard,
    Topic,
)
from a2a_events.server import create_publisher_app

PUB = "https://agent-b.example.com/.well-known/agent-card.json"
SUB = "https://agent-a.example.com/.well-known/agent-card.json"
TOPIC = "agent_card.discovered"


async def _no_sleep(_delay: float) -> None:
    """Skip retry backoff in tests."""


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    publisher = A2AEventsPublisher(
        agent_card_url=PUB,
        transport=InMemoryTransport(),
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(
            card_resolver=lambda _url: SubscriberCard(
                card_url=SUB, a2a_endpoint="https://agent-a.example.com/a2a"
            ),
            sleep=_no_sleep,
        ),
    )
    publisher.declare_topic(Topic(name=TOPIC, filterableFields=["data.cardUrl"]))
    app = create_publisher_app(publisher)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://pub") as c:
        c._publisher = publisher  # type: ignore[attr-defined]
        yield c


async def _subscribe(client: httpx.AsyncClient) -> str:
    resp = await client.post(
        "/a2a-events/subscriptions",
        json={
            "subscriberCardUrl": SUB,
            "topics": [TOPIC],
            "delivery": {"mode": "a2a-message"},
            "fromCursor": "earliest",
            "leaseSeconds": 3600,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["subscriptionId"]


async def test_list_topics(client: httpx.AsyncClient):
    resp = await client.get("/a2a-events/topics")
    assert resp.status_code == 200
    assert [t["name"] for t in resp.json()["topics"]] == [TOPIC]


async def test_subscription_lifecycle(client: httpx.AsyncClient):
    sid = await _subscribe(client)

    got = await client.get(f"/a2a-events/subscriptions/{sid}")
    assert got.status_code == 200
    assert got.json()["status"] == "active"

    listed = await client.get("/a2a-events/subscriptions")
    assert listed.status_code == 200
    assert sid in [s["subscriptionId"] for s in listed.json()["subscriptions"]]

    renewed = await client.post(
        f"/a2a-events/subscriptions/{sid}:renew", json={"leaseSeconds": 7200}
    )
    assert renewed.status_code == 200
    assert renewed.json()["status"] == "active"

    deleted = await client.delete(f"/a2a-events/subscriptions/{sid}")
    assert deleted.status_code == 200
    assert deleted.json()["status"] == "deleted"


async def test_replay_ack_and_deliveries(client: httpx.AsyncClient):
    sid = await _subscribe(client)
    publisher: A2AEventsPublisher = client._publisher  # type: ignore[attr-defined]
    # Publish an event; with no receiver wired it dead-letters, which both
    # records a delivery attempt and leaves a replayable event + cursor.
    record = await publisher.publish(TOPIC, "t", {"cardUrl": "https://x"})

    replay = await client.post(f"/a2a-events/subscriptions/{sid}:replay", json={})
    assert replay.status_code == 200
    assert len(replay.json()["events"]) == 1

    ack = await client.post(
        f"/a2a-events/subscriptions/{sid}:ack", json={"cursor": record.cursor}
    )
    assert ack.status_code == 200

    deliveries = await client.get(f"/a2a-events/subscriptions/{sid}/deliveries")
    assert deliveries.status_code == 200
    attempts = deliveries.json()["deliveryAttempts"]
    assert attempts and attempts[-1]["status"] == "dead_letter"


async def test_error_mapping_to_http_status(client: httpx.AsyncClient):
    missing = await client.get("/a2a-events/subscriptions/sub_nope")
    assert missing.status_code == 404
    assert missing.json()["data"]["code"] == "SUBSCRIPTION_NOT_FOUND"

    bad_topic = await client.post(
        "/a2a-events/subscriptions",
        json={
            "subscriberCardUrl": SUB,
            "topics": ["does-not-exist"],
            "delivery": {"mode": "a2a-message"},
        },
    )
    assert bad_topic.status_code == 404
    assert bad_topic.json()["data"]["code"] == "TOPIC_NOT_FOUND"


async def test_list_subscriptions_pagination_uses_page_token() -> None:
    """The HTTP+JSON binding must honour the `pageToken` query param (§14.5).

    Regression: the route bound a snake_case ``page_token`` parameter, so the
    spec's camelCase ``pageToken`` query param was never applied and every page
    request returned page 1.
    """
    publisher = A2AEventsPublisher(
        agent_card_url=PUB,
        transport=InMemoryTransport(),
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(
            card_resolver=lambda _url: SubscriberCard(
                card_url=SUB, a2a_endpoint="https://agent-a.example.com/a2a"
            ),
            page_size=2,
        ),
    )
    publisher.declare_topic(Topic(name=TOPIC, filterableFields=["data.cardUrl"]))
    app = create_publisher_app(publisher)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://pub") as c:
        sids = [await _subscribe(c) for _ in range(3)]

        first = (await c.get("/a2a-events/subscriptions")).json()
        assert len(first["subscriptions"]) == 2  # page_size honored
        token = first["nextPageToken"]
        assert token  # more to fetch

        second = (
            await c.get("/a2a-events/subscriptions", params={"pageToken": token})
        ).json()
        first_ids = {s["subscriptionId"] for s in first["subscriptions"]}
        second_ids = {s["subscriptionId"] for s in second["subscriptions"]}
        # The token advanced past the first page: disjoint, and the third sub lands here.
        assert first_ids.isdisjoint(second_ids)
        assert second_ids == {sids[2]}
        assert second["nextPageToken"] is None  # no further pages
