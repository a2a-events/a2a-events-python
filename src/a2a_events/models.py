"""Typed protocol models for A2A Events (DESIGN.md §10, §16, §23)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

EXTENSION_URI = "https://example.com/a2a-events/extensions/events/v1"


class DeliveryMode(StrEnum):
    WEBHOOK = "webhook"
    A2A_MESSAGE = "a2a-message"


class SubscriptionStatus(StrEnum):
    ACTIVE = "active"
    EXPIRED = "expired"
    DELETED = "deleted"


# --- Selectors (DESIGN.md §10.4) ---------------------------------------------


class FieldFilterSelector(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["field_filter"] = "field_filter"
    where: dict[str, list[Any]]


class KeywordSearchSelector(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["keyword_search"] = "keyword_search"
    keywords: list[str]
    match: Literal["all", "any"] = "all"
    fields: list[str] | None = None


Selector = Annotated[
    FieldFilterSelector | KeywordSearchSelector,
    Field(discriminator="type"),
]


# --- Topics (DESIGN.md §10.3, §13) -------------------------------------------


class Topic(BaseModel):
    name: str
    description: str = ""
    schema_url: str | None = Field(default=None, alias="schemaUrl")
    retention_seconds: int = Field(default=604800, alias="retentionSeconds")
    replay: bool = True
    selector_types: list[str] = Field(
        default_factory=lambda: ["field_filter", "keyword_search"],
        alias="selectorTypes",
    )
    filterable_fields: list[str] | None = Field(default=None, alias="filterableFields")
    delivery_modes: list[DeliveryMode] = Field(
        default_factory=lambda: [DeliveryMode.A2A_MESSAGE, DeliveryMode.WEBHOOK],
        alias="deliveryModes",
    )

    model_config = ConfigDict(populate_by_name=True)


# --- Delivery preference (DESIGN.md §14.1, §23.1) ----------------------------


class DeliveryPreference(BaseModel):
    """A subscriber's requested delivery configuration."""

    mode: DeliveryMode
    # AgentCard-relative reference to the receive capability, e.g.
    # "agent-card:events.receive" (DESIGN.md §15 safe routing).
    endpoint_ref: str = Field(default="agent-card:events.receive", alias="endpointRef")

    model_config = ConfigDict(populate_by_name=True)


class ResolvedDelivery(BaseModel):
    """The delivery target the publisher resolved from the subscriber's card."""

    mode: DeliveryMode
    resolved_url: str | None = Field(default=None, alias="resolvedUrl")
    resolved_endpoint: str | None = Field(default=None, alias="resolvedEndpoint")

    model_config = ConfigDict(populate_by_name=True)


# --- CloudEvents envelope (DESIGN.md §16) ------------------------------------


class A2AEventsAttributes(BaseModel):
    extension: str = EXTENSION_URI
    publisher_card_url: str = Field(alias="publisherCardUrl")
    topic: str
    cursor: str
    schema_url: str | None = Field(default=None, alias="schemaUrl")
    subscription_id: str | None = Field(default=None, alias="subscriptionId")
    delivery_attempt: int | None = Field(default=None, alias="deliveryAttempt")
    trace_id: str | None = Field(default=None, alias="traceId")

    model_config = ConfigDict(populate_by_name=True)


class CloudEvent(BaseModel):
    specversion: Literal["1.0"] = "1.0"
    id: str
    source: str
    type: str
    subject: str | None = None
    time: datetime
    datacontenttype: str = "application/json"
    data: dict[str, Any]
    a2aevents: A2AEventsAttributes

    model_config = ConfigDict(populate_by_name=True)


# --- Subscription (DESIGN.md §23.1) ------------------------------------------


class Subscription(BaseModel):
    subscription_id: str = Field(alias="subscriptionId")
    status: SubscriptionStatus
    publisher_card_url: str = Field(alias="publisherCardUrl")
    subscriber_card_url: str = Field(alias="subscriberCardUrl")
    topics: list[str]
    selector: Selector | None = None
    delivery: ResolvedDelivery
    created_at: datetime = Field(alias="createdAt")
    lease_until: datetime = Field(alias="leaseUntil")
    # Per-topic last-acked cursor map (DESIGN.md §10.9 "per-topic cursor state").
    cursors: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)
