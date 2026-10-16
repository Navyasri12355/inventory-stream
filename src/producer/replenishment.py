"""
src/producer/replenishment.py
──────────────────────────────
Simulated opening balances, replenishment orders, transfers, and damage/adjustments.

All logic here runs inside the producer before events are published.
The processor is the sole source of truth for stored stock levels.

Implements AGENTS.md §5.5 "Opening balances and replenishment":
  1. Pre-pass: compute average daily demand d per (warehouse, sku)
  2. Emit ADJUSTMENT (zone=OPENING) at replay_start minus 1 second
  3. ROP = SAFETY_FACTOR * d * LEAD_TIME_DAYS_MAX
  4. On restocking: optionally TRANSFER from another warehouse (prob=TRANSFER_PROB),
     else INBOUND from supplier after Uniform(LEAD_TIME_MIN, LEAD_TIME_MAX) data-days
  5. Per OUTBOUND: with adjust_rate → ADJUSTMENT; with damage_rate → DAMAGE

Usage:
    planner = ReplenishmentPlanner(cfg, rng)
    opening = planner.opening_adjustments(demand_df, replay_start)
    # During replay, after each UCI event:
    new_events = planner.on_outbound(event, current_sim_stock)
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterator

import numpy as np

from config.settings import Settings, settings as _default_settings
from src.common.logging_setup import get_logger
from src.common.models import (
    EventSource,
    EventType,
    InventoryEvent,
    TransferDirection,
)

log = get_logger(__name__)


def _sim_stock_key(warehouse_id: str, sku: str) -> tuple[str, str]:
    return (warehouse_id, sku)


class ReplenishmentPlanner:
    """
    Tracks simulated stock and schedules replenishment events.
    All times are in event-time (rebased or original depending on TIME_MODE).
    """

    def __init__(
        self,
        cfg: Settings | None = None,
        rng: np.random.Generator | None = None,
    ) -> None:
        self.cfg = cfg or _default_settings
        self.rng = rng or np.random.default_rng(self.cfg.seed)

        # Simulated stock: (warehouse_id, sku) -> int
        self._stock: dict[tuple[str, str], int] = {}
        # Open orders: key -> scheduled_arrival_event_time
        self._open_orders: set[tuple[str, str]] = set()
        # Average daily demand computed from data
        self._avg_daily_demand: dict[tuple[str, str], float] = {}

    # ─────────────────────────────────────────────────────────────────────────
    # Pre-pass: demand estimation
    # ─────────────────────────────────────────────────────────────────────────

    def compute_demand(
        self,
        events_df,  # cleaned DataFrame with warehouse_id, StockCode, Quantity, InvoiceDate
        warehouses: list[str],
    ) -> None:
        """Pre-pass to compute average daily demand d per (warehouse, sku)."""
        import pandas as pd
        import zlib

        def _wh(row) -> str:
            cid = row.get("CustomerID")
            import pandas as _pd
            key = str(int(float(cid))) if _pd.notna(cid) and cid else str(row["Invoice"])
            return warehouses[zlib.crc32(key.encode()) & 0xFFFFFFFF % len(warehouses)]

        df = events_df[~events_df["_is_return"]].copy()
        df["warehouse_id"] = df.apply(_wh, axis=1)
        df["date"] = pd.to_datetime(df["InvoiceDate"]).dt.date

        grouped = (
            df.groupby(["warehouse_id", "StockCode", "date"])["Quantity"]
            .sum()
            .reset_index()
        )
        avg = grouped.groupby(["warehouse_id", "StockCode"])["Quantity"].mean()
        for (wh, sku), d in avg.items():
            self._avg_daily_demand[(wh, sku.upper())] = float(d)

        log.info("demand computed", keys=len(self._avg_daily_demand))

    # ─────────────────────────────────────────────────────────────────────────
    # Opening adjustments (emitted before first UCI event)
    # ─────────────────────────────────────────────────────────────────────────

    def opening_adjustments(self, replay_start: datetime) -> list[InventoryEvent]:
        """
        Emit one ADJUSTMENT(zone=OPENING) per (warehouse, sku) pair,
        timestamped replay_start - 1 second.
        """
        open_time = replay_start - timedelta(seconds=1)
        events: list[InventoryEvent] = []
        cfg = self.cfg

        for (wh, sku), d in self._avg_daily_demand.items():
            qty = max(cfg.min_opening, int(np.ceil(cfg.opening_days_cover * d)))
            self._stock[_sim_stock_key(wh, sku)] = qty

            ev = InventoryEvent(
                event_id=str(uuid.uuid4()),
                event_time=open_time,
                warehouse_id=wh,
                sku=sku,
                event_type=EventType.ADJUSTMENT,
                quantity=qty,
                zone="OPENING",
                source=EventSource.SIMULATOR,
            )
            events.append(ev)

        log.info("opening adjustments", count=len(events))
        return events

    # ─────────────────────────────────────────────────────────────────────────
    # Per-event replenishment logic
    # ─────────────────────────────────────────────────────────────────────────

    def apply_event(self, ev: InventoryEvent) -> None:
        """Update simulated stock after each emitted event."""
        key = _sim_stock_key(ev.warehouse_id, ev.sku)
        self._stock[key] = self._stock.get(key, 0) + ev.stock_delta

    def check_reorder(
        self,
        warehouse_id: str,
        sku: str,
        event_time: datetime,
        speedup: float | None = None,
    ) -> list[InventoryEvent]:
        """
        After updating stock, check if we're at or below ROP.
        If so schedule INBOUND (or TRANSFER) and return the resulting events.
        Events may be scheduled in the future — caller must hold them in a heap.
        """
        cfg = self.cfg
        key = _sim_stock_key(warehouse_id, sku)
        d = self._avg_daily_demand.get(key, 1.0)
        rop = cfg.safety_factor * d * cfg.lead_time_days_max
        current = self._stock.get(key, 0)

        if current > rop or key in self._open_orders:
            return []

        # Order-up-to quantity
        order_qty = max(cfg.min_opening, int(np.ceil(cfg.opening_days_cover * d)))
        spd = speedup or cfg.speedup

        # Decide: transfer from another warehouse or supplier inbound?
        if self.rng.random() < cfg.transfer_prob:
            return self._make_transfer(warehouse_id, sku, order_qty, event_time, d)

        # Supplier lead time in data-days → real seconds
        lead_days = int(self.rng.integers(cfg.lead_time_days_min, cfg.lead_time_days_max + 1))
        if spd > 0:
            delay_seconds = lead_days * 86400.0 / spd
        else:
            delay_seconds = 0.0
        arrival_time = event_time + timedelta(seconds=delay_seconds)

        self._open_orders.add(key)
        ev = InventoryEvent(
            event_id=str(uuid.uuid4()),
            event_time=arrival_time,
            warehouse_id=warehouse_id,
            sku=sku,
            event_type=EventType.INBOUND,
            quantity=order_qty,
            zone="DOCK",
            source=EventSource.SIMULATOR,
        )
        log.debug("scheduled INBOUND", wh=warehouse_id, sku=sku, qty=order_qty,
                  arrival=arrival_time.isoformat())
        return [ev]

    def on_order_fulfilled(self, warehouse_id: str, sku: str) -> None:
        """Call when an INBOUND from a replenishment order is emitted."""
        self._open_orders.discard(_sim_stock_key(warehouse_id, sku))

    # ─────────────────────────────────────────────────────────────────────────
    # Per-outbound: optional adjust / damage events
    # ─────────────────────────────────────────────────────────────────────────

    def maybe_adjust_damage(
        self,
        trigger: InventoryEvent,
        event_time: datetime,
        adjust_rate: float | None = None,
        damage_rate: float | None = None,
    ) -> list[InventoryEvent]:
        """
        Optionally emit a small ADJUSTMENT or DAMAGE event after an OUTBOUND.

        adjust_rate / damage_rate override cfg values when supplied.
        In replay mode pass adjust_rate=0, damage_rate=0 (AGENTS.md §5.5 rule 5)
        because real write-offs already come from the dataset.
        In simulator-only mode the defaults in cfg apply (0.002 / 0.001).
        """
        extras: list[InventoryEvent] = []
        cfg = self.cfg
        _adj_rate = adjust_rate if adjust_rate is not None else cfg.adjust_rate
        _dmg_rate = damage_rate if damage_rate is not None else cfg.damage_rate

        if self.rng.random() < _adj_rate:
            # Small signed adjustment: ±1 to ±3 units
            delta = int(self.rng.integers(1, 4)) * (1 if self.rng.random() > 0.5 else -1)
            extras.append(InventoryEvent(
                event_id=str(uuid.uuid4()),
                event_time=event_time,
                warehouse_id=trigger.warehouse_id,
                sku=trigger.sku,
                event_type=EventType.ADJUSTMENT,
                quantity=delta,
                zone=trigger.zone,
                source=EventSource.SIMULATOR,
            ))

        if self.rng.random() < _dmg_rate:
            dmg_qty = int(self.rng.integers(1, 4))
            extras.append(InventoryEvent(
                event_id=str(uuid.uuid4()),
                event_time=event_time,
                warehouse_id=trigger.warehouse_id,
                sku=trigger.sku,
                event_type=EventType.DAMAGE,
                quantity=dmg_qty,
                zone=trigger.zone,
                source=EventSource.SIMULATOR,
            ))

        return extras

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _make_transfer(
        self,
        dst_wh: str,
        sku: str,
        qty: int,
        event_time: datetime,
        demand: float,
    ) -> list[InventoryEvent]:
        """Transfer from the richest other warehouse if it has surplus."""
        order_up_to = max(self.cfg.min_opening, int(np.ceil(self.cfg.opening_days_cover * demand)))
        best_src: str | None = None
        best_stock = -1

        for src_wh in self.cfg.warehouses:
            if src_wh == dst_wh:
                continue
            src_stock = self._stock.get(_sim_stock_key(src_wh, sku), 0)
            if src_stock > order_up_to and src_stock > best_stock:
                best_stock = src_stock
                best_src = src_wh

        if best_src is None:
            # No suitable source → fall back to supplier inbound
            key = _sim_stock_key(dst_wh, sku)
            self._open_orders.add(key)
            return [InventoryEvent(
                event_id=str(uuid.uuid4()),
                event_time=event_time,
                warehouse_id=dst_wh,
                sku=sku,
                event_type=EventType.INBOUND,
                quantity=qty,
                zone="DOCK",
                source=EventSource.SIMULATOR,
            )]

        tid = str(uuid.uuid4())
        out_ev = InventoryEvent(
            event_id=str(uuid.uuid4()),
            event_time=event_time,
            warehouse_id=best_src,
            sku=sku,
            event_type=EventType.TRANSFER,
            quantity=qty,
            zone="DOCK",
            source=EventSource.SIMULATOR,
            direction=TransferDirection.OUT,
            transfer_id=tid,
        )
        in_ev = InventoryEvent(
            event_id=str(uuid.uuid4()),
            event_time=event_time + timedelta(seconds=1),
            warehouse_id=dst_wh,
            sku=sku,
            event_type=EventType.TRANSFER,
            quantity=qty,
            zone="DOCK",
            source=EventSource.SIMULATOR,
            direction=TransferDirection.IN,
            transfer_id=tid,
        )
        self._open_orders.add(_sim_stock_key(dst_wh, sku))
        log.debug("scheduled TRANSFER", src=best_src, dst=dst_wh, sku=sku, qty=qty)
        return [out_ev, in_ev]
