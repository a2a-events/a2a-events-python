"""Cursor ordering and signing (spec §10.9, §21.3)."""

from a2a_events import cursor
from a2a_events.signing import SigningKey, verify


def test_cursor_lexicographic_equals_numeric_order():
    cursors = [cursor.encode("t", n) for n in (0, 1, 9, 10, 100, 999999)]
    assert cursors == sorted(cursors)  # byte-wise order matches numeric order
    assert cursor.offset_of(cursor.encode("t", 42)) == 42
    assert (
        cursor.topic_of(cursor.encode("agent_card.discovered", 7))
        == "agent_card.discovered"
    )


def test_signature_roundtrip_and_tamper():
    key = SigningKey.generate("key_2026_06")
    event = {"id": "evt_1", "data": {"a": 1, "b": "x"}, "time": "2026-06-19T20:30:00Z"}
    ts = "2026-06-19T20:30:00Z"
    sig = key.sign(ts, event)

    assert verify(key.public_key, ts, event, sig) is True
    # Tampered payload fails.
    tampered = {**event, "data": {"a": 2, "b": "x"}}
    assert verify(key.public_key, ts, tampered, sig) is False
    # Wrong timestamp fails.
    assert verify(key.public_key, "2026-06-19T20:30:01Z", event, sig) is False


def test_jwk_roundtrip():
    from a2a_events.signing import public_key_from_jwk

    key = SigningKey.generate("k1")
    pub = public_key_from_jwk(key.to_jwk())
    event = {"id": "e", "x": 1}
    sig = key.sign("t", event)
    assert verify(pub, "t", event, sig) is True
