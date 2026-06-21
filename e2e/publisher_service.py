"""Publisher service for the multi-container E2E (DESIGN.md §12, §18, §21, §25).

Serves the canonical JSON-RPC surface, the HTTP+JSON binding, and JWKS, backed
by pooled Postgres with non-blocking offload. It wires the full feature set:
AgentCard discovery + trust, topic authorization, per-subscription delivery
tokens, a durable Postgres retry queue + worker, metrics, pagination, and
retention compaction. /admin routes are test scaffolding (not part of the
protocol) that let the driver publish, rotate keys, run retries, compact, read
metrics, and deliver a deliberately skewed event.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import Request

from a2a_events import (
    A2AEventsPublisher,
    AgentCardResolver,
    AllowlistAuthorizer,
    CardTrustPolicy,
    DeliveryTokenIssuer,
    InMemoryMetrics,
    PublisherConfig,
    RetryWorker,
    SigningKey,
    Topic,
)
from a2a_events.runtime.http_delivery import HttpxTransport
from a2a_events.runtime.postgres import (
    PostgresEventStore,
    PostgresRetryQueue,
    PostgresSubscriptionStore,
)
from a2a_events.server import create_publisher_app

DATABASE_URL = os.environ["DATABASE_URL"]
PUBLISHER_CARD = os.environ.get(
    "PUBLISHER_CARD_URL", "https://publisher.example/.well-known/agent-card.json"
)
SUBSCRIBER_RECEIVE = os.environ["SUBSCRIBER_RECEIVE_URL"]
KEY_SEED = bytes.fromhex(os.environ.get("PUBLISHER_KEY_SEED", "11" * 32))
TOPIC = "agent_card.discovered"
RESTRICTED_TOPIC = "restricted.events"
EPHEMERAL_TOPIC = "ephemeral.events"


def _key_from_seed(seed: bytes, kid: str) -> SigningKey:
    return SigningKey(kid=kid, _private=Ed25519PrivateKey.from_private_bytes(seed))


event_store = PostgresEventStore(DATABASE_URL)
sub_store = PostgresSubscriptionStore(DATABASE_URL)
retry_queue = PostgresRetryQueue(DATABASE_URL)
event_store.create_schema()
sub_store.create_schema()
retry_queue.create_schema()

metrics = InMemoryMetrics()
issuer = DeliveryTokenIssuer(secret=KEY_SEED)
# TOPIC is public; RESTRICTED_TOPIC is granted to nobody, so an anonymous
# subscribe to it must be rejected (§21.4).
authorizer = AllowlistAuthorizer(public_topics={TOPIC, EPHEMERAL_TOPIC})

# Discover delivery endpoints from the subscriber's real AgentCard (§12.2, §21.2).
card_resolver = AgentCardResolver(
    client=httpx.Client(timeout=10.0), policy=CardTrustPolicy()
)

publisher = A2AEventsPublisher(
    agent_card_url=PUBLISHER_CARD,
    transport=HttpxTransport(httpx.AsyncClient()),
    signing_key=_key_from_seed(KEY_SEED, "key-2026-06"),
    config=PublisherConfig(
        store=event_store,
        subscription_store=sub_store,
        retry_queue=retry_queue,
        card_resolver=card_resolver,
        authorizer=authorizer,
        delivery_token_issuer=issuer,
        metrics=metrics,
        offload_store=True,
        store_thread_safe=True,
        page_size=2,
        retry_initial_delay=0.5,
    ),
)
publisher.declare_topic(
    Topic(
        name=TOPIC,
        filterableFields=["data.cardUrl", "data.capabilities"],
        retentionSeconds=604800,
    )
)
publisher.declare_topic(Topic(name=RESTRICTED_TOPIC, retentionSeconds=604800))
publisher.declare_topic(Topic(name=EPHEMERAL_TOPIC, retentionSeconds=1))

worker = RetryWorker(publisher, retry_queue, lease_seconds=30)

app = create_publisher_app(publisher)


@app.get("/admin/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/admin/publish")
async def admin_publish(request: Request) -> dict[str, Any]:
    body = await request.json()
    record = await publisher.publish(
        body["topic"], body["type"], body["data"], body.get("subject")
    )
    return {"eventId": record.event_id, "cursor": record.cursor}


@app.post("/admin/rotate-key")
async def admin_rotate_key(request: Request) -> dict[str, str]:
    body = await request.json()
    publisher.add_signing_key(
        _key_from_seed(bytes.fromhex(body["seed"]), body["kid"]), activate=True
    )
    return {"activeKid": publisher.signing_key.kid}


@app.post("/admin/run-retries")
async def admin_run_retries() -> dict[str, int]:
    return {"processed": await worker.run_once()}


@app.post("/admin/compact")
async def admin_compact() -> dict[str, int]:
    return {"removed": await publisher.compact()}


@app.get("/admin/observability")
async def admin_observability() -> dict[str, Any]:
    return await publisher.observability_snapshot()


@app.post("/admin/deliver-skewed")
async def admin_deliver_skewed() -> dict[str, Any]:
    # Sign a CloudEvent with a 1-hour-old timestamp and deliver it straight to
    # the subscriber's webhook; the subscriber must reject it on skew (§21.3).
    ts = (datetime.now(UTC) - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    event = {
        "specversion": "1.0",
        "id": "evt_skewtest",
        "source": publisher.source,
        "type": "discovered.v1",
        "time": ts,
        "datacontenttype": "application/json",
        "data": {"cardUrl": "https://skew"},
        "a2aevents": {
            "extension": "https://example.com/a2a-events/extensions/events/v1",
            "publisherCardUrl": PUBLISHER_CARD,
            "topic": TOPIC,
            "cursor": f"{TOPIC}:0000000000000000",
        },
    }
    signature = publisher.signing_key.sign(ts, event)
    headers = {
        "A2A-Event-Signature": signature,
        "A2A-Event-Timestamp": ts,
        "A2A-Event-Key-ID": publisher.signing_key.kid,
    }
    result = await publisher.transport.send_webhook(SUBSCRIBER_RECEIVE, headers, event)
    return {"ack": result.ack, "statusCode": result.status_code}


@app.get("/admin/dead-letters")
async def admin_dead_letters() -> dict[str, Any]:
    return {"deadLetters": [d.__dict__ for d in publisher.dead_letters]}
