"""Event signing and verification (spec §21.3).

Signature input is ``timestamp + "." + canonical_json(event)`` where the
canonical form is JCS (RFC 8785). Default algorithm is EdDSA (Ed25519).

The canonicalizer is a full RFC 8785 implementation: object members are sorted
by UTF-16 code unit, strings use JSON.stringify-compatible escaping, and
numbers use the ECMAScript ``Number::toString`` serialization (shortest
round-trip digits, ES exponent rules) so ``1.0`` serializes as ``1`` and
``1e-7`` as ``1e-7``.
"""

from __future__ import annotations

import base64
import json
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

SIGNATURE_PREFIX = "v1="


def _es_number(value: float) -> str:
    """Serialize a float per ECMAScript ``Number::toString`` (RFC 8785 §3.2.2.3)."""
    if math.isnan(value) or math.isinf(value):
        raise ValueError("NaN and Infinity are not valid JSON numbers")
    if value == 0:
        return "0"  # also normalizes -0.0 to "0"

    # Shortest round-tripping decimal digits and decimal exponent. ``value`` is
    # finite here, so the exponent is always an int (never 'n'/'N'/'F').
    _, digit_tuple, exp_raw = Decimal(repr(abs(value))).normalize().as_tuple()
    exp = cast(int, exp_raw)
    digits = "".join(str(d) for d in digit_tuple)
    k = len(digits)
    n = exp + k  # value == int(digits) * 10**(n - k)

    if k <= n <= 21:
        body = digits + "0" * (n - k)
    elif 0 < n <= 21:
        body = digits[:n] + "." + digits[n:]
    elif -6 < n <= 0:
        body = "0." + "0" * -n + digits
    else:
        exponent = n - 1
        mantissa = digits if k == 1 else digits[0] + "." + digits[1:]
        body = f"{mantissa}e{'+' if exponent >= 0 else '-'}{abs(exponent)}"

    return ("-" if value < 0 else "") + body


def _canon(obj: Any) -> str:
    if obj is True:
        return "true"
    if obj is False:
        return "false"
    if obj is None:
        return "null"
    if isinstance(obj, str):
        return json.dumps(obj, ensure_ascii=False)
    if isinstance(obj, int):  # bool already handled above (bool is an int subclass)
        return str(obj)  # integers are exact
    if isinstance(obj, float):
        return _es_number(obj)
    if isinstance(obj, dict):
        # RFC 8785 sorts members by their UTF-16 code-unit sequence.
        members = sorted(obj.items(), key=lambda kv: str(kv[0]).encode("utf-16-be"))
        return "{" + ",".join(f"{_canon(str(k))}:{_canon(v)}" for k, v in members) + "}"
    if isinstance(obj, (list, tuple)):
        return "[" + ",".join(_canon(x) for x in obj) + "]"
    raise TypeError(f"Cannot canonicalize value of type {type(obj).__name__}")


def canonicalize(obj: Any) -> bytes:
    """Return the RFC 8785 (JCS) canonical JSON encoding of ``obj``."""
    return _canon(obj).encode("utf-8")


def signing_input(timestamp: str, event: dict[str, Any]) -> bytes:
    return timestamp.encode("utf-8") + b"." + canonicalize(event)


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


@dataclass
class SigningKey:
    """A publisher signing key with a stable ``kid`` (spec §21.3)."""

    kid: str
    _private: Ed25519PrivateKey

    @classmethod
    def generate(cls, kid: str) -> SigningKey:
        return cls(kid=kid, _private=Ed25519PrivateKey.generate())

    def sign(self, timestamp: str, event: dict[str, Any]) -> str:
        """Sign and return the ``A2A-Event-Signature`` header value."""
        sig = self._private.sign(signing_input(timestamp, event))
        return SIGNATURE_PREFIX + _b64url(sig)

    @property
    def public_key(self) -> Ed25519PublicKey:
        return self._private.public_key()

    def public_raw_b64url(self) -> str:
        from cryptography.hazmat.primitives import serialization

        raw = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        return _b64url(raw)

    def to_jwk(self) -> dict[str, str]:
        """Render an OKP/Ed25519 JWK for the JWKS endpoint (spec §21.3)."""
        return {
            "kty": "OKP",
            "crv": "Ed25519",
            "kid": self.kid,
            "alg": "EdDSA",
            "x": self.public_raw_b64url(),
        }


class SigningKeySet:
    """A publisher's set of signing keys with one active key (spec §21.3).

    Supports key rotation: pre-publish the next key (so subscribers can fetch it
    by ``kid`` before it is used), activate it, and retire the old one. The JWKS
    endpoint serves every key in the set; only the active key signs.
    """

    def __init__(self, active: SigningKey, *also_published: SigningKey) -> None:
        self._keys: dict[str, SigningKey] = {active.kid: active}
        for key in also_published:
            self._keys[key.kid] = key
        self._active_kid = active.kid

    @property
    def active(self) -> SigningKey:
        return self._keys[self._active_kid]

    def get(self, kid: str) -> SigningKey | None:
        return self._keys.get(kid)

    def add(self, key: SigningKey, *, activate: bool = False) -> None:
        """Publish ``key`` (servable from JWKS); optionally make it active."""
        self._keys[key.kid] = key
        if activate:
            self._active_kid = key.kid

    def activate(self, kid: str) -> None:
        if kid not in self._keys:
            raise KeyError(f"Key {kid} is not in the set; add() it before activating.")
        self._active_kid = kid

    def retire(self, kid: str) -> None:
        """Stop publishing ``kid``. The active key cannot be retired."""
        if kid == self._active_kid:
            raise ValueError("Cannot retire the active signing key.")
        self._keys.pop(kid, None)

    def jwks(self) -> list[dict[str, str]]:
        """Render the JWKS ``keys`` array for the discovery endpoint."""
        return [key.to_jwk() for key in self._keys.values()]


def verify(
    public_key: Ed25519PublicKey,
    timestamp: str,
    event: dict[str, Any],
    signature: str,
) -> bool:
    """Verify an ``A2A-Event-Signature`` value against ``event``."""
    if signature.startswith(SIGNATURE_PREFIX):
        signature = signature[len(SIGNATURE_PREFIX) :]
    try:
        public_key.verify(_b64url_decode(signature), signing_input(timestamp, event))
        return True
    except (InvalidSignature, ValueError):
        return False


def public_key_from_jwk(jwk: dict[str, str]) -> Ed25519PublicKey:
    return Ed25519PublicKey.from_public_bytes(_b64url_decode(jwk["x"]))
