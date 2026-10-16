"""
src/processor/aggregations.py
──────────────────────────────
Pure aggregation functions — no Kafka, no DB.

Implements AGENTS.md §6:
  5.  Running stock (keyed state, updated per event)
  6.  EMA of demand:  EMA_t = alpha * x_t + (1 - alpha) * EMA_{t-1}
  8.  Top-K SKUs per window (exact counter)
  10. Alert generation:
        LOW_STOCK       stock < reorder_point
        STOCKOUT_RISK   days_of_cover < threshold
        ABNORMAL_OUTFLOW window outflow > 3x sliding average

  Days-of-cover (§6 rule 7):
    doc = stock / (ema_rate * SECONDS_PER_DATA_DAY)
    where ema_rate is the EMA expressed in units/second of event time.

All functions take explicit state dicts and return new state + optional alert.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from src.common.models import Alert, AlertSeverity, AlertType


# ─────────────────────────────────────────────────────────────────────────────
# Running stock
# ─────────────────────────────────────────────────────────────────────────────

def update_stock(
    stock_state: dict[tuple[str, str], float],
    warehouse_id: str,
    sku: str,
    delta: int,
) -> float:
    """Apply delta and return the new stock level. Never goes below 0 by clamping."""
    key = (warehouse_id, sku)
    new_stock = stock_state.get(key, 0.0) + delta
    # Clamp to 0 (stock can't be negative in reality; negative delta beyond stock = anomaly)
    stock_state[key] = max(0.0, new_stock)
    return stock_state[key]


# ─────────────────────────────────────────────────────────────────────────────
# EMA of demand
# ─────────────────────────────────────────────────────────────────────────────

def update_ema(
    ema_state: dict[tuple[str, str], float],
    warehouse_id: str,
    sku: str,
    outbound_qty: int,
    alpha: float = 0.3,
) -> float:
    """
    Update EMA for outbound quantity.
    EMA_t = alpha * x_t + (1 - alpha) * EMA_{t-1}
    Returns the new EMA value.
    """
    key = (warehouse_id, sku)
    prev = ema_state.get(key, float(outbound_qty))
    new_ema = alpha * outbound_qty + (1.0 - alpha) * prev
    ema_state[key] = new_ema
    return new_ema


def days_of_cover(
    stock: float,
    ema_per_event: float,
    seconds_per_data_day: float,
) -> float:
    """
    Estimate how many data-days of stock remain.
    ema_per_event: EMA expressed in units per event (outbound per event call).
    seconds_per_data_day: from settings (86400/speedup in rebased mode).
    Returns float('inf') if demand is zero.
    """
    if ema_per_event <= 0:
        return float("inf")
    # Convert ema_per_event to units/second using seconds_per_data_day
    # (rough heuristic: assume one event per data-second on average)
    rate_per_second = ema_per_event / seconds_per_data_day
    if rate_per_second <= 0:
        return float("inf")
    return stock / (rate_per_second * seconds_per_data_day)


# ─────────────────────────────────────────────────────────────────────────────
# Top-K SKUs per window
# ─────────────────────────────────────────────────────────────────────────────

def top_k_skus(
    outbound_counts: dict[str, int],
    k: int = 10,
) -> list[tuple[str, int]]:
    """Return the top-k (sku, count) pairs sorted descending by count."""
    return sorted(outbound_counts.items(), key=lambda x: x[1], reverse=True)[:k]


# ─────────────────────────────────────────────────────────────────────────────
# Alert generation
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AlertCooldown:
    """Per-(sku, alert_type) cooldown to avoid alert floods."""
    last_alert_time: dict[tuple[str, str], datetime] = field(default_factory=dict)
    cooldown_seconds: float = 300.0  # 5 minutes default

    def can_alert(self, sku: str, alert_type: str, now: datetime) -> bool:
        key = (sku, alert_type)
        last = self.last_alert_time.get(key)
        if last is None:
            return True
        return (now - last).total_seconds() >= self.cooldown_seconds

    def record(self, sku: str, alert_type: str, now: datetime) -> None:
        self.last_alert_time[(sku, alert_type)] = now


def check_low_stock(
    stock: float,
    warehouse_id: str,
    sku: str,
    avg_daily_demand: float,
    lead_time_days: float,
    safety_factor: float,
    event_time: datetime,
    cooldown: AlertCooldown,
) -> Optional[Alert]:
    """Fire LOW_STOCK if stock <= ROP."""
    rop = safety_factor * avg_daily_demand * lead_time_days
    if stock <= rop and cooldown.can_alert(sku, "LOW_STOCK", event_time):
        cooldown.record(sku, "LOW_STOCK", event_time)
        return Alert(
            ts=event_time,
            warehouse_id=warehouse_id,
            sku=sku,
            alert_type=AlertType.LOW_STOCK,
            severity=AlertSeverity.WARNING,
            message=(
                f"Stock {stock:.0f} <= ROP {rop:.0f} "
                f"(demand={avg_daily_demand:.1f}/day, lead={lead_time_days}d)"
            ),
        )
    return None


def check_stockout_risk(
    doc: float,
    warehouse_id: str,
    sku: str,
    threshold_days: float,
    event_time: datetime,
    cooldown: AlertCooldown,
) -> Optional[Alert]:
    """Fire STOCKOUT_RISK if days-of-cover < threshold."""
    if doc < threshold_days and cooldown.can_alert(sku, "STOCKOUT_RISK", event_time):
        cooldown.record(sku, "STOCKOUT_RISK", event_time)
        sev = AlertSeverity.CRITICAL if doc < threshold_days / 2 else AlertSeverity.WARNING
        return Alert(
            ts=event_time,
            warehouse_id=warehouse_id,
            sku=sku,
            alert_type=AlertType.STOCKOUT_RISK,
            severity=sev,
            message=f"Days of cover {doc:.1f} < threshold {threshold_days}",
        )
    return None


def check_abnormal_outflow(
    window_outflow: int,
    sliding_avg_outflow: float,
    warehouse_id: str,
    sku: str,
    event_time: datetime,
    cooldown: AlertCooldown,
    threshold_multiplier: float = 3.0,
) -> Optional[Alert]:
    """Fire ABNORMAL_OUTFLOW if window outflow > threshold_multiplier * sliding avg."""
    if (
        sliding_avg_outflow > 0
        and window_outflow > threshold_multiplier * sliding_avg_outflow
        and cooldown.can_alert(sku, "ABNORMAL_OUTFLOW", event_time)
    ):
        cooldown.record(sku, "ABNORMAL_OUTFLOW", event_time)
        return Alert(
            ts=event_time,
            warehouse_id=warehouse_id,
            sku=sku,
            alert_type=AlertType.ABNORMAL_OUTFLOW,
            severity=AlertSeverity.WARNING,
            message=(
                f"Outflow {window_outflow} > "
                f"{threshold_multiplier}x avg {sliding_avg_outflow:.1f}"
            ),
        )
    return None
