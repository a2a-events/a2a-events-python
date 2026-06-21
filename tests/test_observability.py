"""Observability: metrics, traceId, and the §32 snapshot (DESIGN.md §32)."""

from __future__ import annotations

from a2a_events import (
    A2AEventsPublisher,
    DeliveryResult,
    InMemoryMetrics,
    PublisherConfig,
    SigningKey,
    SubscriberCard,
    Topic,
    trace_id_for,
)
from a2a_events.models import (
    DeliveryMode,
    DeliveryPreference,
    FieldFilterSelector,
)
from a2a_events.runtime.contracts import Transport

TOPIC = "agent_card.discovered"
SUB_CARD = "https://sub.example/card"


class _ScriptedTransport(Transport):
    def __init__(self, results: list[DeliveryResult]) -> None:
        self._results = list(results)

    def _next(self) -> DeliveryResult:
        if self._results:
            return self._results.pop(0)
        return DeliveryResult(ack=True, status_code=204)

    async def send_a2a_message(self, endpoint, message):
        return self._next()

    async def send_webhook(self, url, headers, body):
        return self._next()


def _build(metrics, transport=None, **kwargs):
    pub = A2AEventsPublisher(
        agent_card_url="https://pub.example/card",
        transport=transport or _ScriptedTransport([]),
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(
            card_resolver=lambda u: SubscriberCard(
                card_url=u, a2a_endpoint="https://sub.example/a2a"
            ),
            metrics=metrics,
            sleep=lambda _d: _noop(),
            **kwargs,
        ),
    )
    pub.declare_topic(Topic(name=TOPIC, filterableFields=["data.n", "data.k"]))
    return pub


async def _noop() -> None:  # noqa: S7503
    return None


async def _subscribe(pub, selector=None):
    return await pub.subscribe(
        subscriber_card_url=SUB_CARD,
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
        selector=selector,
    )


def test_trace_id_is_stable_for_event() -> None:
    assert trace_id_for("evt_abc") == "tr_abc"
    assert trace_id_for("evt_abc") == trace_id_for("evt_abc")


async def test_event_carries_trace_id() -> None:
    metrics = InMemoryMetrics()
    seen: list[dict] = []
    transport = _ScriptedTransport([])
    pub = _build(metrics, transport)
    sub = await _subscribe(pub)

    # Capture the delivered event by wrapping the transport.
    orig = transport.send_a2a_message

    async def capture(endpoint, message):
        seen.append(message["message"]["parts"][0]["data"])
        return await orig(endpoint, message)

    transport.send_a2a_message = capture  # type: ignore[method-assign]
    rec = await pub.publish(TOPIC, "v1", {"n": 1})
    assert seen[0]["a2aevents"]["traceId"] == trace_id_for(rec.event_id)
    assert sub.subscription_id


async def test_delivery_and_selector_metrics() -> None:
    metrics = InMemoryMetrics()
    pub = _build(metrics)
    await _subscribe(pub, selector=FieldFilterSelector(where={"data.k": ["x"]}))

    await pub.publish(TOPIC, "v1", {"k": "x"})  # matches -> delivered
    await pub.publish(TOPIC, "v1", {"k": "y"})  # no match -> filtered

    assert metrics.total("published_events") == 2
    assert metrics.get("selector_evaluations", result="match") == 1
    assert metrics.get("selector_evaluations", result="miss") == 1
    assert metrics.get("delivery_attempts", status="delivered", topic=TOPIC) == 1
    assert len(metrics.observations_for("delivery_latency_seconds")) == 1


async def test_dead_letter_and_retry_metrics() -> None:
    metrics = InMemoryMetrics()
    transport = _ScriptedTransport(
        [DeliveryResult(ack=False, retry=True, status_code=503)] * 5
    )
    pub = _build(metrics, transport, max_attempts=2)
    await _subscribe(pub)
    await pub.publish(TOPIC, "v1", {"n": 1})

    assert metrics.get("delivery_attempts", status="retry", topic=TOPIC) == 1
    assert metrics.get("delivery_attempts", status="dead_letter", topic=TOPIC) == 1


async def test_observability_snapshot() -> None:
    metrics = InMemoryMetrics()
    pub = _build(metrics)
    await _subscribe(pub)
    await pub.publish(TOPIC, "v1", {"n": 1})

    snap = await pub.observability_snapshot()
    assert snap["subscriptionCount"] == 1
    assert snap["expiredSubscriptionCount"] == 0
    assert "counters" in snap["metrics"]


async def test_lease_renewal_metric() -> None:
    metrics = InMemoryMetrics()
    pub = _build(metrics)
    sub = await _subscribe(pub)
    await pub.renew(sub.subscription_id, 7200)
    assert metrics.total("lease_renewals") == 1
