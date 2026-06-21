"""SSRF guard on resolved delivery endpoints (DESIGN.md §21.2)."""

from __future__ import annotations

import pytest

from a2a_events.errors import A2AEventsError, ErrorCode
from a2a_events.runtime.ssrf import SSRFPolicy, check_endpoint


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/receive",
        "http://localhost:8080/receive",
        "https://foo.localhost/receive",
        "http://10.0.0.5/receive",
        "http://192.168.1.10/receive",
        "http://169.254.169.254/latest/meta-data",  # cloud metadata
        "http://[::1]/receive",
        "http://[fe80::1]/receive",
        "https://0.0.0.0/receive",
    ],
)
def test_blocks_non_routable_targets(url: str):
    with pytest.raises(A2AEventsError) as exc:
        check_endpoint(url, SSRFPolicy())
    assert exc.value.code == ErrorCode.DELIVERY_ENDPOINT_BLOCKED


@pytest.mark.parametrize(
    "url",
    [
        "https://agent-a.example.com/a2a-events/receive",
        "http://sub/a2a/v1",  # bare hostname, allowed without DNS
        "https://8.8.8.8/receive",  # public IP literal
    ],
)
def test_allows_routable_targets(url: str):
    check_endpoint(url, SSRFPolicy())  # no raise


def test_rejects_non_http_scheme():
    with pytest.raises(A2AEventsError) as exc:
        check_endpoint("file:///etc/passwd", SSRFPolicy())
    assert exc.value.code == ErrorCode.DELIVERY_ENDPOINT_BLOCKED


def test_require_https_policy():
    policy = SSRFPolicy(require_https=True)
    with pytest.raises(A2AEventsError):
        check_endpoint("http://agent-a.example.com/receive", policy)
    check_endpoint("https://agent-a.example.com/receive", policy)  # no raise


def test_allow_hosts_bypasses_checks():
    # An explicit allowlist entry wins even for loopback.
    policy = SSRFPolicy(allow_hosts=frozenset({"localhost"}))
    check_endpoint("http://localhost:9000/receive", policy)  # no raise


def test_injected_resolver_screens_rebinding():
    # A public-looking hostname that resolves to a private address is blocked.
    policy = SSRFPolicy(resolve=lambda _host: ["10.0.0.9"])
    with pytest.raises(A2AEventsError) as exc:
        check_endpoint("https://sneaky.example.com/receive", policy)
    assert exc.value.code == ErrorCode.DELIVERY_ENDPOINT_BLOCKED
