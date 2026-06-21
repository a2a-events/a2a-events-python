"""End-to-end driver for the multi-container A2A Events stack.

Runs on the host against the published ports of the publisher and subscriber
services and exercises every feature over the real network: AgentCard discovery
and trust, signed HTTP delivery with JWKS, per-subscription delivery tokens,
topic authorization, selectors, replay, ack, ListDeliveryAttempts, renew, key
rotation, the SSRF guard, webhook delivery, the durable retry queue + worker,
pagination, retention compaction, observability, cross-container timestamp-skew
rejection, and client-side auto lease renewal. Exits non-zero on first failure.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import UTC, datetime
from typing import Any

import httpx

from a2a_events import AutoLeaseRenewer

PUB = os.environ.get("PUB_URL", "http://localhost:18080")
SUB = os.environ.get("SUB_URL", "http://localhost:18081")
# In-network card URLs the publisher (not the host) resolves.
SUB_CARD = os.environ.get(
    "SUB_CARD", "http://a2a-e2e-sub:8000/.well-known/agent-card.json"
)
SUB_LOOPBACK_CARD = os.environ.get(
    "SUB_LOOPBACK_CARD", "http://a2a-e2e-sub:8000/.well-known/loopback-card.json"
)
TOPIC = "agent_card.discovered"
RESTRICTED_TOPIC = "restricted.events"
EPHEMERAL_TOPIC = "ephemeral.events"

client = httpx.Client(timeout=15.0)
_passed = 0


def ok(name: str) -> None:
    global _passed
    _passed += 1
    print(f"  PASS  {name}")


def fail(name: str, detail: str) -> None:
    print(f"  FAIL  {name}: {detail}")
    sys.exit(1)


def check(name: str, condition: bool, detail: str = "") -> None:
    ok(name) if condition else fail(name, detail)


def wait_health(url: str, name: str) -> None:
    for _ in range(60):
        try:
            if client.get(f"{url}/admin/health").status_code == 200:
                ok(f"{name} healthy")
                return
        except httpx.HTTPError:
            pass
        time.sleep(1)
    fail(f"{name} healthy", "timed out")


def clear_received() -> None:
    client.post(f"{SUB}/admin/clear")


def received() -> list[dict[str, Any]]:
    return client.get(f"{SUB}/admin/received").json()["events"]


def wait_received(
    expected: int, name: str, timeout: float = 15.0
) -> list[dict[str, Any]]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        events = received()
        if len(events) >= expected:
            return events
        time.sleep(0.5)
    fail(name, f"expected >= {expected} events, saw {len(received())}")
    return []


def subscribe(
    card_url: str,
    mode: str,
    *,
    topic: str = TOPIC,
    selector: dict[str, Any] | None = None,
) -> httpx.Response:
    body: dict[str, Any] = {
        "subscriberCardUrl": card_url,
        "topics": [topic],
        "delivery": {"mode": mode},
        "fromCursor": "latest",
        "leaseSeconds": 3600,
    }
    if selector is not None:
        body["selector"] = selector
    return client.post(f"{PUB}/a2a-events/subscriptions", json=body)


def delete_sub(sid: str) -> None:
    client.delete(f"{PUB}/a2a-events/subscriptions/{sid}")


def register_token(sid: str, token: str) -> None:
    client.post(
        f"{SUB}/admin/register-token", json={"subscriptionId": sid, "token": token}
    )


def publish(data: dict[str, Any], topic: str = TOPIC) -> dict[str, Any]:
    resp = client.post(
        f"{PUB}/admin/publish",
        json={"topic": topic, "type": "discovered.v1", "data": data},
    )
    resp.raise_for_status()
    return resp.json()


STREAMING = {"type": "field_filter", "where": {"data.capabilities": ["streaming"]}}


def test_discovery_token_delivery() -> None:
    resp = subscribe(SUB_CARD, "a2a-message", selector=STREAMING)
    check("subscribe via AgentCard discovery", resp.status_code == 201, resp.text)
    body = resp.json()
    sid = body["subscriptionId"]
    auth = body.get("delivery", {}).get("auth", {})
    check(
        "delivery token issued",
        auth.get("scheme") == "bearer" and auth.get("token"),
        str(body),
    )

    register_token(sid, auth["token"])
    clear_received()
    publish({"cardUrl": "https://x", "capabilities": ["streaming"]})
    events = wait_received(1, "signed token delivery via discovery")
    check("delivered event matches", events[0]["cardUrl"] == "https://x", str(events))
    check("event carries traceId", bool(events[0].get("traceId")), str(events))

    # Selector filters non-matching events.
    clear_received()
    publish({"cardUrl": "https://y", "capabilities": ["batch"]})
    time.sleep(2)
    check("selector filters non-match", received() == [], str(received()))
    delete_sub(sid)


def test_bad_delivery_token_rejected() -> None:
    resp = subscribe(SUB_CARD, "a2a-message")
    sid = resp.json()["subscriptionId"]
    register_token(sid, "dtok_wrong")  # publisher will present the correct one
    clear_received()
    publish({"cardUrl": "https://z", "capabilities": ["streaming"]})
    time.sleep(2)
    check("bad delivery token blocks delivery", received() == [], str(received()))
    dls = client.get(f"{PUB}/admin/dead-letters").json()["deadLetters"]
    check(
        "mismatch dead-letters", any(d["subscription_id"] == sid for d in dls), str(dls)
    )
    delete_sub(sid)


def test_topic_authorization() -> None:
    resp = subscribe(SUB_CARD, "a2a-message", topic=RESTRICTED_TOPIC)
    check(
        "restricted topic denied",
        resp.status_code == 403
        and resp.json().get("data", {}).get("code") == "TOPIC_NOT_AUTHORIZED",
        f"{resp.status_code} {resp.text}",
    )


def test_durable_retry() -> None:
    resp = subscribe(SUB_CARD, "a2a-message")
    sid = resp.json()["subscriptionId"]
    clear_received()
    client.post(f"{SUB}/admin/fail-next", json={"count": 1})  # first attempt fails
    publish({"cardUrl": "https://retry", "capabilities": ["streaming"]})
    time.sleep(2)
    check(
        "first attempt deferred (not yet delivered)", received() == [], str(received())
    )
    processed = client.post(f"{PUB}/admin/run-retries").json()["processed"]
    check("retry worker processed a due retry", processed >= 1, str(processed))
    events = wait_received(1, "delivery after durable retry")
    check(
        "retried event delivered", events[0]["cardUrl"] == "https://retry", str(events)
    )
    delete_sub(sid)


def test_replay_ack_attempts_renew() -> None:
    resp = subscribe(SUB_CARD, "a2a-message")
    sid = resp.json()["subscriptionId"]
    clear_received()
    pub1 = publish({"cardUrl": "https://ra", "capabilities": ["streaming"]})
    wait_received(1, "delivery for replay/ack")

    replay = client.post(
        f"{PUB}/a2a-events/subscriptions/{sid}:replay", json={"fromCursor": "earliest"}
    )
    check(
        "replay returns events", len(replay.json().get("events", [])) >= 1, replay.text
    )

    ack = client.post(
        f"{PUB}/a2a-events/subscriptions/{sid}:ack", json={"cursor": pub1["cursor"]}
    )
    check("ack accepted", ack.status_code == 200, ack.text)

    deliveries = client.get(f"{PUB}/a2a-events/subscriptions/{sid}/deliveries")
    attempts = deliveries.json().get("deliveryAttempts", [])
    check(
        "delivery attempts recorded",
        any(a["status"] == "delivered" for a in attempts),
        str(attempts),
    )

    renew = client.post(
        f"{PUB}/a2a-events/subscriptions/{sid}:renew", json={"leaseSeconds": 7200}
    )
    check("renew subscription", renew.json().get("status") == "active", renew.text)
    delete_sub(sid)


def test_key_rotation() -> None:
    resp = subscribe(SUB_CARD, "a2a-message")
    sid = resp.json()["subscriptionId"]
    rot = client.post(
        f"{PUB}/admin/rotate-key", json={"kid": "key-2026-07", "seed": "22" * 32}
    )
    check("rotate signing key", rot.json().get("activeKid") == "key-2026-07", rot.text)
    clear_received()
    publish({"cardUrl": "https://rotated", "capabilities": ["streaming"]})
    events = wait_received(1, "delivery after key rotation")
    check(
        "post-rotation delivery", events[0]["cardUrl"] == "https://rotated", str(events)
    )
    delete_sub(sid)


def test_webhook_and_ssrf() -> None:
    wsub = subscribe(SUB_CARD, "webhook")
    check("subscribe webhook (discovery)", wsub.status_code == 201, wsub.text)
    sid = wsub.json()["subscriptionId"]
    clear_received()
    publish({"cardUrl": "https://w", "capabilities": ["streaming"]})
    whook = wait_received(1, "webhook delivery")
    check(
        "webhook delivered", any(e["cardUrl"] == "https://w" for e in whook), str(whook)
    )
    delete_sub(sid)

    blocked = subscribe(SUB_LOOPBACK_CARD, "a2a-message")
    check(
        "SSRF guard blocks loopback endpoint",
        blocked.status_code == 403
        and blocked.json().get("data", {}).get("code") == "DELIVERY_ENDPOINT_BLOCKED",
        f"{blocked.status_code} {blocked.text}",
    )


def test_pagination() -> None:
    sids = [
        subscribe(SUB_CARD, "a2a-message").json()["subscriptionId"] for _ in range(3)
    ]
    first = client.get(f"{PUB}/a2a-events/subscriptions").json()
    check("page size honored", len(first["subscriptions"]) == 2, str(first))
    token = first.get("nextPageToken")
    check("nextPageToken present", bool(token), str(first))
    second = client.get(
        f"{PUB}/a2a-events/subscriptions", params={"pageToken": token}
    ).json()
    first_ids = {s["subscriptionId"] for s in first["subscriptions"]}
    second_ids = {s["subscriptionId"] for s in second["subscriptions"]}
    check(
        "pages are disjoint",
        first_ids.isdisjoint(second_ids),
        f"{first_ids} {second_ids}",
    )
    for sid in sids:
        delete_sub(sid)


def test_retention_compaction() -> None:
    publish({"cardUrl": "https://ephemeral"}, topic=EPHEMERAL_TOPIC)
    time.sleep(2)  # exceed the 1s retention window
    removed = client.post(f"{PUB}/admin/compact").json()["removed"]
    check("retention compaction removed expired event", removed >= 1, str(removed))


def test_skew_rejection() -> None:
    clear_received()
    res = client.post(f"{PUB}/admin/deliver-skewed").json()
    check(
        "cross-container skew rejected",
        res["ack"] is False and res["statusCode"] == 403,
        str(res),
    )
    check("skewed event not recorded", received() == [], str(received()))


def test_observability() -> None:
    snap = client.get(f"{PUB}/admin/observability").json()
    check(
        "observability has subscription gauge", "subscriptionCount" in snap, str(snap)
    )
    counters = snap.get("metrics", {}).get("counters", {})
    check(
        "published_events counted",
        any(k.startswith("published_events") for k in counters),
        str(counters),
    )


def test_auto_lease_renewal() -> None:
    resp = subscribe(SUB_CARD, "a2a-message")
    sid = resp.json()["subscriptionId"]

    async def run() -> tuple[int, str, str]:
        before = client.get(f"{PUB}/a2a-events/subscriptions/{sid}").json()[
            "leaseUntil"
        ]
        async with httpx.AsyncClient(timeout=10.0) as ac:

            async def renew(sub_id: str, lease_seconds: int) -> datetime:
                r = await ac.post(
                    f"{PUB}/a2a-events/subscriptions/{sub_id}:renew",
                    json={"leaseSeconds": lease_seconds},
                )
                r.raise_for_status()
                return datetime.fromisoformat(
                    r.json()["leaseUntil"].replace("Z", "+00:00")
                )

            renewer = AutoLeaseRenewer(renew, min_sleep=1.0)
            # Start with leaseUntil=now so the renewer renews almost immediately,
            # and renew with a lease longer than the original subscribe (3600s)
            # so the new leaseUntil is observably later than `before`.
            renewer.start(sid, 7200, datetime.now(UTC))
            await asyncio.sleep(3.0)
            await renewer.aclose()
        after = client.get(f"{PUB}/a2a-events/subscriptions/{sid}").json()["leaseUntil"]
        return len(renewer.renewals), before, after

    renewals, before, after = asyncio.run(run())
    check("auto lease renewer renewed", renewals >= 1, f"renewals={renewals}")
    check("leaseUntil advanced", after > before, f"{before} -> {after}")
    delete_sub(sid)


def leave_survivor() -> None:
    """Leave one active subscription behind (not deleted) so run.sh's
    post-restart durability check finds a Postgres-persisted subscription."""
    resp = subscribe(SUB_CARD, "a2a-message")
    check(
        "survivor subscription persisted for restart check",
        resp.status_code == 201,
        resp.text,
    )


def main() -> None:
    print("E2E: A2A Events multi-container stack")
    wait_health(PUB, "publisher")
    wait_health(SUB, "subscriber")

    test_discovery_token_delivery()
    test_bad_delivery_token_rejected()
    test_topic_authorization()
    test_durable_retry()
    test_replay_ack_attempts_renew()
    test_key_rotation()
    test_webhook_and_ssrf()
    test_pagination()
    test_retention_compaction()
    test_skew_rejection()
    test_observability()
    test_auto_lease_renewal()
    leave_survivor()

    print(f"\nALL {_passed} CHECKS PASSED")


if __name__ == "__main__":
    main()
