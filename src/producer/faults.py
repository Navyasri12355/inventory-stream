"""
src/producer/faults.py
───────────────────────
Fault injection wrapper (AGENTS.md §5.5).

Wraps the combined event stream and injects:
  - Duplicates  (--dup-rate):      re-emit same event_id 1–3 times after a short delay
  - Late events (--late-rate):     hold back and emit with original event_time after
                                   later events; two bands relative to watermark W:
                                     accepted [0.1W, 0.8W]  and  rejected [1.5W, 3W]
  - Malformed   (--malformed-rate): corrupt JSON / drop required field / negative qty
  - Bursts      (--burst-factor):   periodically multiply speedup

Each injected fault is recorded in the emitted-event log with its kind.

Usage:
    injector = FaultInjector(cfg, rng)
    for ev_time, payload in injector.wrap(base_stream):
        producer.produce(...)
"""
from __future__ import annotations

import json
import random
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Iterator

import numpy as np

from config.settings import Settings, settings as _default_settings
from src.common.logging_setup import get_logger
from src.common.models import EventType, InventoryEvent

log = get_logger(__name__)

# Watermark in seconds (used to pick lateness bands)
_WATERMARK_S = 120  # default; overridden from settings at runtime


class FaultInjector:
    """
    Wraps a (event_time, InventoryEvent | bytes) stream and injects faults.

    Yields (scheduled_event_time, payload, fault_kind) triples where:
      fault_kind is one of: None | "dup" | "late_accepted" | "late_rejected" | "malformed"
    """

    def __init__(
        self,
        cfg: Settings | None = None,
        rng: np.random.Generator | None = None,
    ) -> None:
        self.cfg = cfg or _default_settings
        self.rng = rng or np.random.default_rng(self.cfg.seed + 1)  # offset seed
        self._watermark_s = self.cfg.watermark_seconds
        # Buffer for holding late events: list of (emit_at, payload, kind)
        self._late_buffer: list[tuple[datetime, InventoryEvent | bytes, str]] = []

    def wrap(
        self,
        stream: Iterator[tuple[datetime, InventoryEvent]],
    ) -> Iterator[tuple[datetime, InventoryEvent | bytes, str | None]]:
        """
        Consume stream and yield (emit_time, payload, fault_kind).
        Interleaves held-back late events when their release time arrives.
        """
        for ev_time, ev in stream:
            # Release any buffered late events whose release time has passed
            yield from self._flush_late(ev_time)

            # Malformed injection
            if self.rng.random() < self.cfg.malformed_rate:
                yield ev_time, self._make_malformed(ev), "malformed"
                continue  # skip normal emission for this event

            # Late injection (hold it back)
            if self.rng.random() < self.cfg.late_rate:
                self._hold_late(ev_time, ev)
                continue

            # Normal emission
            yield ev_time, ev, None

            # Duplicate injection (emit same event again, shortly after)
            if self.rng.random() < self.cfg.dup_rate:
                n_dups = int(self.rng.integers(1, 4))
                for i in range(n_dups):
                    # Slightly later time (1-5 seconds after), same event_id
                    dup_time = ev_time + timedelta(seconds=float(i + 1))
                    yield dup_time, ev, "dup"

        # Flush remaining late events at end
        yield from self._flush_all_late()

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _hold_late(self, ev_time: datetime, ev: InventoryEvent) -> None:
        """Buffer this event to be emitted after a lateness drawn from two bands."""
        W = float(self._watermark_s)
        # 50/50 between accepted and rejected band
        if self.rng.random() < 0.5:
            # Accepted: [0.1W, 0.8W] lateness
            lateness = self.rng.uniform(0.1 * W, 0.8 * W)
            kind = "late_accepted"
        else:
            # Rejected: [1.5W, 3W] lateness (will miss the watermark)
            lateness = self.rng.uniform(1.5 * W, 3.0 * W)
            kind = "late_rejected"

        # Release time = event_time + lateness (producer wall-clock perspective)
        release_at = ev_time + timedelta(seconds=lateness)
        self._late_buffer.append((release_at, ev, kind))
        log.debug("held late event", sku=ev.sku, lateness_s=round(lateness, 1), kind=kind)

    def _flush_late(
        self, current_time: datetime
    ) -> Iterator[tuple[datetime, InventoryEvent, str]]:
        remaining = []
        for release_at, ev, kind in self._late_buffer:
            if release_at <= current_time:
                yield release_at, ev, kind
            else:
                remaining.append((release_at, ev, kind))
        self._late_buffer = remaining

    def _flush_all_late(self) -> Iterator[tuple[datetime, InventoryEvent, str]]:
        for release_at, ev, kind in sorted(self._late_buffer, key=lambda x: x[0]):
            yield release_at, ev, kind
        self._late_buffer = []

    def _make_malformed(self, ev: InventoryEvent) -> bytes:
        """Return corrupted bytes that will fail parsing."""
        choice = int(self.rng.integers(0, 3))
        if choice == 0:
            # Truncated JSON
            raw = ev.to_kafka_value()
            return raw[: max(1, len(raw) // 2)]
        elif choice == 1:
            # Missing required field
            d = ev.model_dump(mode="json")
            d.pop("event_type", None)
            return json.dumps(d).encode()
        else:
            # Non-ADJUSTMENT event with negative quantity
            d = ev.model_dump(mode="json")
            d["event_type"] = "OUTBOUND"
            d["quantity"] = -abs(d.get("quantity", 1))
            return json.dumps(d).encode()
