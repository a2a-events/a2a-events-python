"""Runnable quickstart for the in-memory vertical slice.

    uv run python examples/quickstart.py

Demonstrates: declare a topic, subscribe over the JSON-RPC surface with a
field_filter selector and A2A-message delivery, publish matching and
non-matching events, observe signed delivery + implicit ack, then replay.
"""

import asyncio

from a2a_events import (
    A2AEventsPublisher,
    InMemorySubscriber,
    InMemoryTransport,
    PublisherConfig,
    SigningKey,
    Topic,
)
from a2a_events.jsonrpc import handle

PUBLISHER_CARD = "https://agent-b.example.com/.well-known/agent-card.json"
SUBSCRIBER_CARD = "https://agent-a.example.com/.well-known/agent-card.json"
TOPIC = "agent_card.discovered"


async def main() -> None:
    transport = InMemoryTransport()
    key = SigningKey.generate("key_2026_06")

    subscriber = InMemorySubscriber(
        card_url=SUBSCRIBER_CARD,
        transport=transport,
        key_resolver=lambda _kid: key.public_key,
    )
    publisher = A2AEventsPublisher(
        agent_card_url=PUBLISHER_CARD,
        transport=transport,
        signing_key=key,
        config=PublisherConfig(card_resolver=lambda _url: subscriber.card()),
    )
    publisher.declare_topic(
        Topic(name=TOPIC, filterableFields=["data.cardUrl", "data.capabilities"])
    )

    # Subscribe: only streaming-capable cards, delivered as A2A messages.
    sub = await handle(
        publisher,
        {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "a2a.events.Subscribe",
            "params": {
                "subscriberCardUrl": SUBSCRIBER_CARD,
                "topics": [TOPIC],
                "selector": {
                    "type": "field_filter",
                    "where": {"data.capabilities": ["streaming"]},
                },
                "delivery": {"mode": "a2a-message"},
                "fromCursor": "latest",
                "leaseSeconds": 3600,
            },
        },
    )
    sub_id = sub["result"]["subscriptionId"]
    print(f"subscribed: {sub_id}")

    # Publish: one matches the selector, one does not.
    match = {"cardUrl": "https://x", "capabilities": ["streaming"]}
    miss = {"cardUrl": "https://y", "capabilities": ["batch"]}
    await publisher.publish(TOPIC, "discovered.v1", match)
    await publisher.publish(TOPIC, "discovered.v1", miss)

    print(f"delivered {len(subscriber.received)} event(s):")
    for event in subscriber.received:
        print(f"  - {event['data']['cardUrl']} @ cursor {event['a2aevents']['cursor']}")

    # Replay the topic from the start of retention.
    replay = await handle(
        publisher,
        {
            "jsonrpc": "2.0",
            "id": "2",
            "method": "a2a.events.Replay",
            "params": {"subscriptionId": sub_id, "fromCursor": "earliest"},
        },
    )
    print(f"replay returned {len(replay['result']['events'])} matching event(s)")


if __name__ == "__main__":
    asyncio.run(main())
