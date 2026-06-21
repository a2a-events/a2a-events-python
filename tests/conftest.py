"""Shared fixtures: a wired in-memory publisher + subscriber."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from a2a_events import (
    A2AEventsPublisher,
    InMemorySubscriber,
    InMemoryTransport,
    PublisherConfig,
    SigningKey,
    Topic,
)

PUBLISHER_CARD = "https://agent-b.example.com/.well-known/agent-card.json"
SUBSCRIBER_CARD = "https://agent-a.example.com/.well-known/agent-card.json"
TOPIC = "agent_card.discovered"


@dataclass
class Harness:
    publisher: A2AEventsPublisher
    subscriber: InMemorySubscriber
    transport: InMemoryTransport
    key: SigningKey
    sleeps: list[float]


@pytest.fixture
def harness() -> Harness:
    transport = InMemoryTransport()
    key = SigningKey.generate("key_2026_06")
    subscriber = InMemorySubscriber(
        card_url=SUBSCRIBER_CARD,
        transport=transport,
        key_resolver=lambda _kid: key.public_key,
    )
    # Record backoff delays instead of actually waiting, so retry tests are fast.
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:  # noqa: S7503
        sleeps.append(delay)

    publisher = A2AEventsPublisher(
        agent_card_url=PUBLISHER_CARD,
        transport=transport,
        signing_key=key,
        config=PublisherConfig(
            card_resolver=lambda _url: subscriber.card(),
            max_attempts=3,
            sleep=fake_sleep,
        ),
    )
    publisher.declare_topic(
        Topic(
            name=TOPIC,
            filterableFields=["data.cardUrl", "data.capabilities", "data.skills.tags"],
            retentionSeconds=604800,
        )
    )
    return Harness(
        publisher=publisher,
        subscriber=subscriber,
        transport=transport,
        key=key,
        sleeps=sleeps,
    )
