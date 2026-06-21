"""SSRF guard for resolved delivery endpoints (spec §21.2).

The publisher already resolves delivery targets *only* from the subscriber's
AgentCard (never from a raw URL in the subscribe request), which satisfies the
§21.2 baseline. This module adds the recommended stricter policy: reject
endpoints that point at loopback, private, link-local, or otherwise non-routable
addresses — the classic SSRF sinks (``http://localhost``, cloud metadata at
``169.254.169.254``, RFC 1918 hosts, ...).

IP *literals* are checked synchronously and deterministically. Hostnames are
allowed by default (no DNS) so the guard stays test- and offline-friendly; a
deployment that wants to defeat DNS-rebinding can inject ``resolve`` to look up
and screen every resolved address. ``require_https`` is off by default so plain
``http`` intra-mesh endpoints keep working.
"""

from __future__ import annotations

import ipaddress
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from ..errors import A2AEventsError, ErrorCode

# Hostnames that resolve to loopback without needing DNS.
_LOOPBACK_HOSTNAMES = frozenset({"localhost"})


@dataclass
class SSRFPolicy:
    """Which classes of delivery endpoint to refuse.

    ``resolve`` is an optional hostname -> list[IP-string] resolver. When set,
    every resolved address is screened, defeating DNS rebinding; when ``None``
    (the default) only IP literals and known loopback hostnames are screened.
    """

    block_loopback: bool = True
    block_private: bool = True
    block_link_local: bool = True
    block_reserved: bool = True
    require_https: bool = False
    allow_hosts: frozenset[str] = field(default_factory=frozenset)
    resolve: Callable[[str], list[str]] | None = None


def _reject(url: str, reason: str) -> A2AEventsError:
    return A2AEventsError(
        ErrorCode.DELIVERY_ENDPOINT_BLOCKED,
        f"Delivery endpoint is not allowed: {reason}.",
        {"endpoint": url, "reason": reason},
    )


def _screen_ip(
    url: str,
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
    policy: SSRFPolicy,
) -> None:
    if policy.block_loopback and ip.is_loopback:
        raise _reject(url, "loopback address")
    if policy.block_link_local and ip.is_link_local:
        raise _reject(url, "link-local address")
    # is_private covers RFC 1918 / ULA (and, in stdlib, loopback + link-local,
    # already handled above with clearer reasons).
    if policy.block_private and ip.is_private:
        raise _reject(url, "private address")
    if policy.block_reserved and (
        ip.is_reserved or ip.is_unspecified or ip.is_multicast
    ):
        raise _reject(url, "reserved address")


def check_endpoint(url: str, policy: SSRFPolicy) -> None:
    """Raise :class:`A2AEventsError` if ``url`` violates ``policy``."""
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise _reject(url, f"unsupported scheme {scheme!r}")
    if policy.require_https and scheme != "https":
        raise _reject(url, "https required")

    host = parts.hostname
    if not host:
        raise _reject(url, "missing host")
    host = host.lower()
    if host in policy.allow_hosts:
        return

    if policy.block_loopback and (
        host in _LOOPBACK_HOSTNAMES or host.endswith(".localhost")
    ):
        raise _reject(url, "loopback hostname")

    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        _screen_ip(url, literal, policy)
        return

    if policy.resolve is not None:
        addresses = policy.resolve(host)
        if not addresses:
            raise _reject(url, "host did not resolve")
        for addr in addresses:
            _screen_ip(url, ipaddress.ip_address(addr), policy)
