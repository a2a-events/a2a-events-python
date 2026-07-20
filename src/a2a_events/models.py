"""Typed protocol models for A2A Events (spec §10, §16, §23)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

EXTENSION_URI = "https://a2a-events.github.io/a2a-events/extensions/events/v1"


class DeliveryMode(StrEnum):
    WEBHOOK = "webhook"
    A2A_MESSAGE = "a2a-message"


class SubscriptionStatus(StrEnum):
    ACTIVE = "active"
    EXPIRED = "expired"
    DELETED = "deleted"


# --- Selectors (spec §10.4) ---------------------------------------------


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


# --- Topics (spec §10.3, §13) -------------------------------------------


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


# --- Delivery preference (spec §14.1, §23.1) ----------------------------


class DeliveryPreference(BaseModel):
    """A subscriber's requested delivery configuration."""

    mode: DeliveryMode
    # AgentCard-relative reference to the receive capability, e.g.
    # "agent-card:events.receive" (spec §15 safe routing).
    endpoint_ref: str = Field(default="agent-card:events.receive", alias="endpointRef")

    model_config = ConfigDict(populate_by_name=True)


class ResolvedDelivery(BaseModel):
    """The delivery target the publisher resolved from the subscriber's card."""

    mode: DeliveryMode
    resolved_url: str | None = Field(default=None, alias="resolvedUrl")
    resolved_endpoint: str | None = Field(default=None, alias="resolvedEndpoint")

    model_config = ConfigDict(populate_by_name=True)


# --- CloudEvents envelope (spec §16) ------------------------------------


class CloudEvent(BaseModel):
    """A CloudEvents 1.0 event with the A2A Events extension attributes.

    The A2A Events metadata travels as *flat scalar extension context
    attributes* (``a2atopic``, ``a2acursor``, ...), because the CloudEvents
    type system has no map/object type: extension attributes MUST use the
    scalar type system and lowercase-alphanumeric names (CloudEvents 1.0
    "Extension Context Attributes" + "Type System"; names SHOULD be at most
    20 characters). A nested ``a2aevents`` object would not be a conformant
    extension attribute.
    """

    specversion: Literal["1.0"] = "1.0"
    id: str
    source: str
    type: str
    subject: str | None = None
    time: datetime
    datacontenttype: str = "application/json"
    data: dict[str, Any]
    # A2A Events extension context attributes (spec §16). Flat scalars only.
    a2aextension: str = EXTENSION_URI
    a2apublisher: str
    a2atopic: str
    a2acursor: str
    a2aschemaurl: str | None = None
    a2asubscription: str | None = None
    a2adeliveryattempt: int | None = None
    a2atraceid: str | None = None

    model_config = ConfigDict(populate_by_name=True)


# --- Subscription (spec §23.1) ------------------------------------------


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
    # Per-topic last-acked cursor map (spec §10.9 "per-topic cursor state").
    cursors: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)
