"""
tests/unit/test_models.py
──────────────────────────
Unit tests for src/common/models.py  — no Kafka or DB required.
Run with:  pytest tests/unit/test_models.py -v
"""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from src.common.models import (
    Alert,
    AlertSeverity,
    AlertType,
    DLQEnvelope,
    EventSource,
    EventType,
    InventoryEvent,
    TransferDirection,
)


def _base_event(**overrides) -> dict:
    base = {
        "event_id":     "550e8400-e29b-41d4-a716-446655440000",
        "event_time":   "2026-01-01T00:00:00Z",
        "warehouse_id": "WH-01",
        "sku":          "85123a",          # lowercase — should be upper-cased
        "event_type":   "OUTBOUND",
        "quantity":     10,
        "zone":         "Z3",
        "source":       "UCI_REPLAY",
    }
    base.update(overrides)
    return base


class TestInventoryEvent:
    def test_roundtrip_json(self):
        ev = InventoryEvent(**_base_event())
        raw = ev.to_kafka_value()
        ev2 = InventoryEvent.model_validate_json(raw)
        assert ev2.event_id == ev.event_id

    def test_sku_uppercased(self):
        ev = InventoryEvent(**_base_event())
        assert ev.sku == "85123A"

    def test_kafka_key(self):
        ev = InventoryEvent(**_base_event())
        assert ev.kafka_key == "WH-01:85123A"

    def test_stock_delta_outbound(self):
        ev = InventoryEvent(**_base_event(event_type="OUTBOUND", quantity=5))
        assert ev.stock_delta == -5

    def test_stock_delta_inbound(self):
        ev = InventoryEvent(**_base_event(event_type="INBOUND", quantity=20))
        assert ev.stock_delta == +20

    def test_stock_delta_damage(self):
        ev = InventoryEvent(**_base_event(event_type="DAMAGE", quantity=3))
        assert ev.stock_delta == -3

    def test_stock_delta_adjustment_positive(self):
        ev = InventoryEvent(**_base_event(event_type="ADJUSTMENT", quantity=7))
        assert ev.stock_delta == +7

    def test_stock_delta_adjustment_negative(self):
        ev = InventoryEvent(**_base_event(event_type="ADJUSTMENT", quantity=-4))
        assert ev.stock_delta == -4

    def test_stock_delta_transfer_in(self):
        ev = InventoryEvent(**_base_event(
            event_type="TRANSFER", quantity=10,
            direction="IN", transfer_id="abc"
        ))
        assert ev.stock_delta == +10

    def test_stock_delta_transfer_out(self):
        ev = InventoryEvent(**_base_event(
            event_type="TRANSFER", quantity=10,
            direction="OUT", transfer_id="abc"
        ))
        assert ev.stock_delta == -10

    def test_negative_quantity_non_adjustment_raises(self):
        with pytest.raises(Exception):
            InventoryEvent(**_base_event(event_type="OUTBOUND", quantity=-5))

    def test_transfer_missing_direction_raises(self):
        with pytest.raises(Exception):
            InventoryEvent(**_base_event(event_type="TRANSFER", quantity=5))

    def test_naive_event_time_gets_utc(self):
        ev = InventoryEvent(**_base_event(event_time=datetime(2026, 1, 1, 0, 0, 0)))
        assert ev.event_time.tzinfo is not None


class TestAlert:
    def test_roundtrip_json(self):
        a = Alert(
            ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
            warehouse_id="WH-02",
            sku="85123A",
            alert_type=AlertType.LOW_STOCK,
            severity=AlertSeverity.WARNING,
            message="Stock below reorder point",
        )
        raw = a.to_kafka_value()
        a2 = Alert.model_validate_json(raw)
        assert a2.sku == "85123A"


class TestDLQEnvelope:
    def test_headers(self):
        dlq = DLQEnvelope(
            raw_value='{"bad": "json"',
            error="JSONDecodeError",
            source_topic="inventory-events",
            source_partition=2,
            source_offset=999,
        )
        headers = dict(dlq.to_headers())
        # to_headers() returns (str, bytes) tuples
        assert headers["x-error"] == b"JSONDecodeError"
        assert headers["x-source-topic"] == b"inventory-events"
        assert headers["x-source-partition"] == b"2"
        assert headers["x-source-offset"] == b"999"
