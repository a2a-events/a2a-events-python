"""RFC 8785 (JCS) canonicalization, especially ECMAScript number rules."""

from __future__ import annotations

import pytest

from a2a_events.signing import SigningKey, canonicalize, verify

# (value, expected canonical text) per ECMAScript Number::toString / RFC 8785.
NUMBER_VECTORS = [
    (0.0, "0"),
    (-0.0, "0"),
    (1.0, "1"),
    (-1.0, "-1"),
    (100.0, "100"),
    (1.5, "1.5"),
    (3.14, "3.14"),
    (0.5, "0.5"),
    (0.001, "0.001"),
    (1e-7, "1e-7"),
    (1e20, "100000000000000000000"),
    (1e21, "1e+21"),
    (1.5e300, "1.5e+300"),
    (5e-324, "5e-324"),  # smallest positive subnormal double
    (9007199254740992.0, "9007199254740992"),  # 2**53
    (-42.0, "-42"),
]


@pytest.mark.parametrize("value,expected", NUMBER_VECTORS)
def test_number_serialization(value: float, expected: str):
    assert canonicalize(value) == expected.encode("utf-8")


def test_integers_are_exact():
    assert canonicalize(123456789) == b"123456789"
    assert canonicalize(-7) == b"-7"


def test_object_members_sorted_and_compact():
    obj = {"b": 1, "a": 2, "Z": 3}
    # Sorted by code unit: uppercase 'Z' (0x5A) precedes 'a' (0x61), 'b'.
    assert canonicalize(obj) == b'{"Z":3,"a":2,"b":1}'


def test_nested_and_mixed_types():
    obj = {"x": [1, 2.5, "s", True, None], "y": {"k": 1.0}}
    assert canonicalize(obj) == b'{"x":[1,2.5,"s",true,null],"y":{"k":1}}'


def test_string_escaping_matches_json():
    assert canonicalize('a"b\\c\n') == b'"a\\"b\\\\c\\n"'
    assert canonicalize("é") == '"é"'.encode()  # non-ASCII kept literal


def test_nan_and_inf_rejected():
    with pytest.raises(ValueError):
        canonicalize(float("nan"))
    with pytest.raises(ValueError):
        canonicalize(float("inf"))


def test_float_payload_signs_and_verifies_round_trip():
    key = SigningKey.generate("k1")
    event = {"id": "evt_1", "data": {"amount": 1.0, "ratio": 0.001, "n": 1e21}}
    ts = "2026-06-19T20:30:00+00:00"
    sig = key.sign(ts, event)
    assert verify(key.public_key, ts, event, sig) is True
    # A different float value must not verify against the old signature.
    tampered = {"id": "evt_1", "data": {"amount": 1.5, "ratio": 0.001, "n": 1e21}}
    assert verify(key.public_key, ts, tampered, sig) is False
