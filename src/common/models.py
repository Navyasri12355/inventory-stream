"""
src/common/models.py
─────────────────────
Pydantic v2 models for every message type in the pipeline.

  InventoryEvent  – published to inventory-events topic
  Alert           – published to inventory-alerts topic
  DLQEnvelope     – wraps a bad message sent to inventory-dlq

All models use strict JSON serialisation (datetime → ISO-8601 UTC string).
Message key = "{warehouse_id}:{sku}" — never publish without a key.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator


# ─────────────────────────────────────────────────────────────────────────────
# Enumerations
# ─────────────────────────────────────────────────────────────────────────────

class EventType(str, Enum):
    INBOUND    = "INBOUND"
    OUTBOUND   = "OUTBOUND"
    TRANSFER   = "TRANSFER"
    ADJUSTMENT = "ADJUSTMENT"
    DAMAGE     = "DAMAGE"


class EventSource(str, Enum):
    UCI_REPLAY = "UCI_REPLAY"
    SIMULATOR  = "SIMULATOR"


class TransferDirection(str, Enum):
    IN  = "IN"
    OUT = "OUT"


class AlertType(str, Enum):
    LOW_STOCK       = "LOW_STOCK"
    STOCKOUT_RISK   = "STOCKOUT_RISK"
    ABNORMAL_OUTFLOW = "ABNORMAL_OUTFLOW"
    STALE_DATA      = "STALE_DATA"


class AlertSeverity(str, Enum):
    INFO     = "info"
    WARNING  = "warning"
    CRITICAL = "critical"


# ─────────────────────────────────────────────────────────────────────────────
# InventoryEvent
# ─────────────────────────────────────────────────────────────────────────────

class InventoryEvent(BaseModel):
    """
    Published to the `inventory-events` topic.
    Message key = f"{warehouse_id}:{sku}" (set by the producer).

    Schema §5.1 (AGENTS.md).
    """
    event_id:      str        = Field(..., description="UUID string")
    event_time:    datetime   = Field(..., description="ISO-8601 UTC — rebased or original")
    warehouse_id:  str        = Field(..., description="e.g. WH-01")
    sku:           str        = Field(..., description="Upper-case product code")
    event_type:    EventType
    quantity:      int        = Field(..., description="Positive except ADJUSTMENT (may be signed)")
    zone:          str        = Field(..., description="Z0-Z7 or RETURNS or OPENING or DOCK")
    source:        EventSource

    # UCI_REPLAY only
    original_time: Optional[datetime] = Field(None, description="Original dataset timestamp")

    # TRANSFER only
    direction:   Optional[TransferDirection] = None
    transfer_id: Optional[str]               = None

    @field_validator("sku")
    @classmethod
    def sku_uppercase(cls, v: str) -> str:
        return v.upper()

    @field_validator("event_time", "original_time", mode="before")
    @classmethod
    def ensure_utc(cls, v: object) -> object:
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    @model_validator(mode="after")
    def validate_quantity_and_transfer(self) -> "InventoryEvent":
        if self.event_type != EventType.ADJUSTMENT and self.quantity < 0:
            raise ValueError(
                f"quantity must be positive for {self.event_type}; "
                f"got {self.quantity}"
            )
        if self.event_type == EventType.TRANSFER:
            if self.direction is None or self.transfer_id is None:
                raise ValueError("TRANSFER events require direction and transfer_id")
        return self

    @property
    def kafka_key(self) -> str:
        """Message key that routes this event to a deterministic partition."""
        return f"{self.warehouse_id}:{self.sku}"

    @property
    def stock_delta(self) -> int:
        """Signed stock effect of this event on the owning (warehouse, sku) pair."""
        match self.event_type:
            case EventType.INBOUND:
                return +self.quantity
            case EventType.OUTBOUND | EventType.DAMAGE:
                return -self.quantity
            case EventType.ADJUSTMENT:
                return self.quantity   # already signed
            case EventType.TRANSFER:
                return +self.quantity if self.direction == TransferDirection.IN else -self.quantity
            case _:
                return 0

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json(exclude_none=False).encode()


# ─────────────────────────────────────────────────────────────────────────────
# Alert
# ─────────────────────────────────────────────────────────────────────────────

class Alert(BaseModel):
    """
    Published to `inventory-alerts` topic.
    Message key = sku.
    """
    ts:          datetime
    warehouse_id: str
    sku:          str
    alert_type:   AlertType
    severity:     AlertSeverity
    message:      str = ""

    @field_validator("ts", mode="before")
    @classmethod
    def ensure_utc(cls, v: object) -> object:
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode()


# ─────────────────────────────────────────────────────────────────────────────
# DLQEnvelope
# ─────────────────────────────────────────────────────────────────────────────

class DLQEnvelope(BaseModel):
    """
    Wraps a bad/late message sent to `inventory-dlq`.
    Original payload + error metadata live in Kafka message headers:
      x-error, x-source-topic, x-source-partition, x-source-offset
    The value field carries the raw original bytes as a UTF-8 string.
    """
    raw_value:       str
    error:           str
    source_topic:    str
    source_partition: int
    source_offset:   int
    received_at:     datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode()

    def to_headers(self) -> list[tuple[str, bytes]]:
        return [
            ("x-error",            self.error.encode()),
            ("x-source-topic",     self.source_topic.encode()),
            ("x-source-partition", str(self.source_partition).encode()),
            ("x-source-offset",    str(self.source_offset).encode()),
        ]
