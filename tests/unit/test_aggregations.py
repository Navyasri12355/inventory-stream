"""
tests/unit/test_aggregations.py
────────────────────────────────
Unit tests for src/processor/aggregations.py
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.processor.aggregations import (
    AlertCooldown,
    check_abnormal_outflow,
    check_low_stock,
    check_stockout_risk,
    days_of_cover,
    top_k_skus,
    update_ema,
    update_stock,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Running stock ─────────────────────────────────────────────────────────────

class TestUpdateStock:
    def test_inbound_increases_stock(self):
        state: dict = {}
        new = update_stock(state, "WH-01", "SKU1", +10)
        assert new == 10.0

    def test_outbound_decreases_stock(self):
        state: dict = {"WH-01": {"SKU1": 10}}
        state2: dict = {}
        update_stock(state2, "WH-01", "SKU1", +10)
        new = update_stock(state2, "WH-01", "SKU1", -4)
        assert new == 6.0

    def test_stock_clamped_at_zero(self):
        state: dict = {}
        update_stock(state, "WH-01", "SKU1", +5)
        new = update_stock(state, "WH-01", "SKU1", -100)
        assert new == 0.0

    def test_multiple_keys_independent(self):
        state: dict = {}
        update_stock(state, "WH-01", "SKU1", +10)
        update_stock(state, "WH-02", "SKU1", +20)
        assert state[("WH-01", "SKU1")] == 10.0
        assert state[("WH-02", "SKU1")] == 20.0


# ── EMA ───────────────────────────────────────────────────────────────────────

class TestUpdateEma:
    def test_first_call_returns_quantity(self):
        state: dict = {}
        ema = update_ema(state, "WH-01", "SKU1", 10, alpha=0.3)
        assert ema == 10.0  # first call: EMA = x

    def test_ema_smoothing(self):
        state: dict = {}
        update_ema(state, "WH-01", "SKU1", 10, alpha=0.3)
        ema = update_ema(state, "WH-01", "SKU1", 0, alpha=0.3)
        # EMA = 0.3 * 0 + 0.7 * 10 = 7
        assert abs(ema - 7.0) < 1e-9


# ── Days of cover ─────────────────────────────────────────────────────────────

class TestDaysOfCover:
    def test_zero_demand_returns_inf(self):
        assert days_of_cover(100.0, 0.0, 144.0) == float("inf")

    def test_normal_calculation(self):
        # stock=100, ema=10 units/event, spd=144s/data-day
        doc = days_of_cover(100.0, 10.0, 144.0)
        assert doc > 0


# ── Top-K ─────────────────────────────────────────────────────────────────────

class TestTopKSkus:
    def test_returns_top_k(self):
        counts = {"A": 5, "B": 20, "C": 1, "D": 15}
        top = top_k_skus(counts, k=2)
        assert [s for s, _ in top] == ["B", "D"]

    def test_k_larger_than_dict(self):
        counts = {"A": 5}
        top = top_k_skus(counts, k=10)
        assert len(top) == 1


# ── Alert: LOW_STOCK ──────────────────────────────────────────────────────────

class TestLowStock:
    def test_fires_below_rop(self):
        cd = AlertCooldown(cooldown_seconds=0)
        alert = check_low_stock(5.0, "WH-01", "SKU1", 10.0, 7.0, 1.2, _now(), cd)
        assert alert is not None
        assert alert.alert_type.value == "LOW_STOCK"

    def test_no_alert_above_rop(self):
        cd = AlertCooldown(cooldown_seconds=0)
        alert = check_low_stock(1000.0, "WH-01", "SKU1", 10.0, 7.0, 1.2, _now(), cd)
        assert alert is None

    def test_cooldown_suppresses_repeat(self):
        cd = AlertCooldown(cooldown_seconds=300)
        t = _now()
        check_low_stock(5.0, "WH-01", "SKU1", 10.0, 7.0, 1.2, t, cd)
        alert2 = check_low_stock(5.0, "WH-01", "SKU1", 10.0, 7.0, 1.2, t, cd)
        assert alert2 is None


# ── Alert: STOCKOUT_RISK ──────────────────────────────────────────────────────

class TestStockoutRisk:
    def test_fires_when_doc_low(self):
        cd = AlertCooldown(cooldown_seconds=0)
        alert = check_stockout_risk(1.0, "WH-01", "SKU1", 3.0, _now(), cd)
        assert alert is not None
        assert alert.alert_type.value == "STOCKOUT_RISK"

    def test_no_alert_when_doc_ok(self):
        cd = AlertCooldown(cooldown_seconds=0)
        alert = check_stockout_risk(10.0, "WH-01", "SKU1", 3.0, _now(), cd)
        assert alert is None


# ── Alert: ABNORMAL_OUTFLOW ───────────────────────────────────────────────────

class TestAbnormalOutflow:
    def test_fires_when_outflow_exceeds_3x_avg(self):
        cd = AlertCooldown(cooldown_seconds=0)
        alert = check_abnormal_outflow(100, 10.0, "WH-01", "SKU1", _now(), cd)
        assert alert is not None

    def test_no_alert_when_normal(self):
        cd = AlertCooldown(cooldown_seconds=0)
        alert = check_abnormal_outflow(25, 10.0, "WH-01", "SKU1", _now(), cd)
        assert alert is None

    def test_no_alert_when_sliding_avg_zero(self):
        cd = AlertCooldown(cooldown_seconds=0)
        alert = check_abnormal_outflow(1000, 0.0, "WH-01", "SKU1", _now(), cd)
        assert alert is None
