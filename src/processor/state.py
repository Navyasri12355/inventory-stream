"""
src/processor/state.py
───────────────────────
Keyed state management with checkpoint/restore.

State is persisted to the consumer_offsets table in TimescaleDB inside the
same transaction as window results — guaranteeing exactly-once semantics
(AGENTS.md §7: commit offsets only after results are durably written).

Checkpoint structure (per partition):
  - running stock per (warehouse_id, sku)
  - EMA state per (warehouse_id, sku)
  - dedup seen-set (serialised as list of {event_id, expiry})
  - window accumulators (serialised)
  - committed offsets per (group_id, topic, partition)
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import psycopg

from config.settings import settings as _settings
from src.common.logging_setup import get_logger
from src.processor.dedup import Deduplicator
from src.processor.windows import WindowState

log = get_logger(__name__)


@dataclass
class ProcessorState:
    """All mutable state for one processor instance."""
    partition_id:  int
    stock:         dict[tuple[str, str], float] = field(default_factory=dict)
    ema:           dict[tuple[str, str], float] = field(default_factory=dict)
    deduper:       Deduplicator = field(default_factory=lambda: Deduplicator(
        watermark_seconds=_settings.watermark_seconds
    ))
    window_state:  WindowState = field(default_factory=WindowState)
    # Per-partition committed offset (−1 means "not yet committed")
    committed_offset: int = -1


class StateManager:
    """
    Checkpoint and restore per-partition processor state to/from TimescaleDB.
    Uses the consumer_offsets table as the durable store.
    All checkpoint writes must happen inside the same DB transaction as
    the window result upserts (handled by the Sink class).
    """

    GROUP_ID = "inventory-processor"

    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn

    # ─────────────────────────────────────────────────────────────────────────
    # Save (called inside the sink transaction — do NOT commit here)
    # ─────────────────────────────────────────────────────────────────────────

    def save_offset(
        self,
        cur: psycopg.Cursor,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """Upsert committed offset. Must be called inside an open transaction."""
        cur.execute(
            """
            INSERT INTO consumer_offsets (group_id, topic, partition, committed_offset)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (group_id, topic, partition)
            DO UPDATE SET committed_offset = EXCLUDED.committed_offset
            """,
            (self.GROUP_ID, topic, partition, offset),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Restore (called in on_assign callback)
    # ─────────────────────────────────────────────────────────────────────────

    def load_offset(self, topic: str, partition: int) -> int | None:
        """Return the last committed offset, or None if never committed."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT committed_offset FROM consumer_offsets
                WHERE group_id = %s AND topic = %s AND partition = %s
                """,
                (self.GROUP_ID, topic, partition),
            )
            row = cur.fetchone()
        return row[0] if row else None
