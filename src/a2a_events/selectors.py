"""Selector matching engine — the normative algebra from spec §10.4.

Two conformant publishers must match the same selector against the same event
identically, so these rules are intentionally explicit.
"""

from __future__ import annotations

from typing import Any

from .errors import A2AEventsError, ErrorCode
from .models import (
    FieldFilterSelector,
    KeywordSearchSelector,
    Selector,
    Topic,
)

_SCALAR_TYPES = (str, int, float, bool, type(None))


class _Absent:
    """Sentinel for a field path that does not resolve in the event."""


ABSENT = _Absent()


def resolve_path(root: dict[str, Any], path: str) -> Any:
    """Resolve a dotted ``path`` against ``root`` (the full CloudEvent dict).

    Returns a scalar, a list of scalars, or :data:`ABSENT`. Raises
    ``INVALID_SELECTOR`` if the path resolves to an object (per §10.4).
    """
    current: Any = root
    for segment in path.split("."):
        if isinstance(current, dict) and segment in current:
            current = current[segment]
        else:
            return ABSENT

    if isinstance(current, dict):
        raise A2AEventsError(
            ErrorCode.INVALID_SELECTOR,
            f"field path {path!r} resolves to an object",
            {"field": path},
        )
    if isinstance(current, list) and not all(
        isinstance(x, _SCALAR_TYPES) for x in current
    ):
        raise A2AEventsError(
            ErrorCode.INVALID_SELECTOR,
            f"field path {path!r} resolves to a non-scalar array",
            {"field": path},
        )
    return current


def _typed_eq(a: Any, b: Any) -> bool:
    """Exact, type-sensitive equality (no coercion); ``True`` != ``1`` (§10.4)."""
    if isinstance(a, bool) != isinstance(b, bool):
        return False
    return bool(a == b)


def _field_matches(event_value: Any, declared: list[Any]) -> bool:
    if event_value is ABSENT:
        return False
    if isinstance(event_value, list):
        # Array field: match if intersection with declared values is non-empty.
        return any(_typed_eq(ev, dv) for ev in event_value for dv in declared)
    # Scalar field: any-of membership.
    return any(_typed_eq(event_value, dv) for dv in declared)


def _match_field_filter(sel: FieldFilterSelector, event: dict[str, Any]) -> bool:
    # AND across fields; any-of within a field.
    for path, values in sel.where.items():
        if not _field_matches(resolve_path(event, path), values):
            return False
    return True


def _collect_text(event: dict[str, Any], fields: list[str]) -> list[str]:
    texts: list[str] = []
    for path in fields:
        value = resolve_path(event, path)
        if isinstance(value, str):
            texts.append(value)
        elif isinstance(value, list):
            texts.extend(x for x in value if isinstance(x, str))
    return texts


def _match_keyword_search(
    sel: KeywordSearchSelector,
    event: dict[str, Any],
    default_fields: list[str],
) -> bool:
    fields = sel.fields if sel.fields is not None else default_fields
    haystack = " \n ".join(_collect_text(event, fields)).lower()
    hits = (kw.lower() in haystack for kw in sel.keywords)
    return all(hits) if sel.match == "all" else any(hits)


def matches(
    selector: Selector | None,
    event: dict[str, Any],
    default_search_fields: list[str] | None = None,
) -> bool:
    """Return ``True`` if ``event`` matches ``selector`` (None matches all)."""
    if selector is None:
        return True
    if isinstance(selector, FieldFilterSelector):
        return _match_field_filter(selector, event)
    if isinstance(selector, KeywordSearchSelector):
        return _match_keyword_search(selector, event, default_search_fields or [])
    raise A2AEventsError(ErrorCode.INVALID_SELECTOR, "unknown selector type")


def validate_selector(selector: Selector | None, topic: Topic) -> None:
    """Validate a selector against a topic's declared capabilities (§10.4).

    Raises ``SELECTOR_NOT_SUPPORTED`` for an unsupported type or out-of-bounds
    field, ``INVALID_SELECTOR`` for structural problems.
    """
    if selector is None:
        return
    if selector.type not in topic.selector_types:
        raise A2AEventsError(
            ErrorCode.SELECTOR_NOT_SUPPORTED,
            f"selector type {selector.type!r} not supported by topic {topic.name!r}",
            {"topic": topic.name, "selectorType": selector.type},
        )
    if isinstance(selector, FieldFilterSelector):
        if not selector.where:
            raise A2AEventsError(
                ErrorCode.INVALID_SELECTOR, "field_filter.where is empty"
            )
        if topic.filterable_fields is not None:
            for field in selector.where:
                if field not in topic.filterable_fields:
                    raise A2AEventsError(
                        ErrorCode.SELECTOR_NOT_SUPPORTED,
                        f"field {field!r} is not filterable on topic {topic.name!r}",
                        {"topic": topic.name, "field": field},
                    )
    if isinstance(selector, KeywordSearchSelector) and not selector.keywords:
        raise A2AEventsError(
            ErrorCode.INVALID_SELECTOR, "keyword_search.keywords is empty"
        )
