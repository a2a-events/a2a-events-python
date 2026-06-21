"""Selector algebra (spec §10.4)."""

import pytest

from a2a_events.errors import A2AEventsError, ErrorCode
from a2a_events.models import FieldFilterSelector, KeywordSearchSelector, Topic
from a2a_events.selectors import matches, validate_selector

EVENT = {
    "type": "org.example.a2a.agent_card.discovered.v1",
    "subject": "agent-card/x",
    "data": {
        "cardUrl": "https://new.example.com/card.json",
        "capabilities": ["streaming", "pushNotifications"],
        "count": 3,
        "active": True,
        "title": "New Coding Agent",
        "skills": {"tags": ["coding", "search"]},
    },
}


def test_field_filter_any_of_on_array():
    sel = FieldFilterSelector(where={"data.capabilities": ["streaming", "batch"]})
    assert matches(sel, EVENT, []) is True


def test_field_filter_no_intersection():
    sel = FieldFilterSelector(where={"data.capabilities": ["batch"]})
    assert matches(sel, EVENT, []) is False


def test_field_filter_and_across_fields():
    sel = FieldFilterSelector(
        where={"data.capabilities": ["streaming"], "data.skills.tags": ["coding"]}
    )
    assert matches(sel, EVENT, []) is True
    sel2 = FieldFilterSelector(
        where={"data.capabilities": ["streaming"], "data.skills.tags": ["nope"]}
    )
    assert matches(sel2, EVENT, []) is False


def test_field_filter_absent_does_not_match():
    sel = FieldFilterSelector(where={"data.missing": ["x"]})
    assert matches(sel, EVENT, []) is False


def test_field_filter_typed_equality_bool_vs_int():
    # True must not match 1 (exact, type-sensitive).
    assert matches(FieldFilterSelector(where={"data.active": [1]}), EVENT, []) is False
    assert (
        matches(FieldFilterSelector(where={"data.active": [True]}), EVENT, []) is True
    )
    # 3 (number) must not match "3" (string).
    assert matches(FieldFilterSelector(where={"data.count": ["3"]}), EVENT, []) is False
    assert matches(FieldFilterSelector(where={"data.count": [3]}), EVENT, []) is True


def test_field_filter_object_path_is_invalid():
    sel = FieldFilterSelector(where={"data.skills": ["x"]})
    with pytest.raises(A2AEventsError) as exc:
        matches(sel, EVENT, [])
    assert exc.value.code == ErrorCode.INVALID_SELECTOR


def test_keyword_search_all_and_any():
    # title = "New Coding Agent"; "missing" is absent.
    kw = ["coding", "missing"]
    all_sel = KeywordSearchSelector(keywords=kw, match="all", fields=["data.title"])
    assert matches(all_sel, EVENT, []) is False  # "missing" not in title
    any_sel = KeywordSearchSelector(keywords=kw, match="any", fields=["data.title"])
    assert matches(any_sel, EVENT, []) is True  # "coding" in title (case-insensitive)


def test_keyword_search_default_fields():
    sel = KeywordSearchSelector(keywords=["coding"], match="all")
    assert matches(sel, EVENT, default_search_fields=["data.skills.tags"]) is True


def test_validate_selector_rejects_unsupported_type():
    topic = Topic(name="t", selectorTypes=["field_filter"])
    with pytest.raises(A2AEventsError) as exc:
        validate_selector(KeywordSearchSelector(keywords=["x"]), topic)
    assert exc.value.code == ErrorCode.SELECTOR_NOT_SUPPORTED


def test_validate_selector_rejects_unfilterable_field():
    topic = Topic(name="t", filterableFields=["data.cardUrl"])
    with pytest.raises(A2AEventsError) as exc:
        validate_selector(
            FieldFilterSelector(where={"data.capabilities": ["x"]}), topic
        )
    assert exc.value.code == ErrorCode.SELECTOR_NOT_SUPPORTED
