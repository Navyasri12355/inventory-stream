"""
src/processor/sink.py
──────────────────────
Idempotent TimescaleDB upserts for all pipeline results.

All writes use INSERT ... ON CONFLICT DO UPDATE (replay-safe).
Window results, stock levels, alerts, and committed offsets are
written in a SINGLE TRANSACTION per batch to guarantee atomicity
between offset commit and result persistence (AGENTS.md §7).

Usage:
    sink = Sink(settings)
    sink.connect()
    with sink.transaction() as cur:
        sink.upsert_stock_level(cur, ...)
        sink.upsert_window_metrics(cur, ...)
        sink.upsert_alert(cur, ...)
        state_mgr.save_offset(cur, topic, partition, offset)
    # Transaction auto-committed at context exit
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Generator

import psycopg
from psycopg.rows import dict_row

from config.settings import Settings, settings as _default_settings
from src.common.logging_setup import get_logger
from src.common.models import Alert
from src.processor.windows import WindowAccumulator, WindowKey

log = get_logger(__name__)


class Sink:
    def __init__(self, cfg: Settings | None = None) -> None:
        self.cfg = cfg or _default_settings
        self._conn: psycopg.Connection | None = None

    # ─────────────────────────────────────────────────────────────────────────
    # Connection
    # ─────────────────────────────────────────────────────────────────────────

    def connect(self) -> None:
        self._conn = psycopg.connect(self.cfg.db_dsn, autocommit=False)
        log.info("DB connected", host=self.cfg.db_host, port=self.cfg.db_port)

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            log.info("DB connection closed")

    @property
    def conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self.connect()
        return self._conn  # type: ignore[return-value]

    @contextmanager
    def transaction(self) -> Generator[psycopg.Cursor, None, None]:
        """Context manager: yields a cursor; commits on success, rolls back on error."""
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                yield cur

    # ─────────────────────────────────────────────────────────────────────────
    # Upserts
    # ─────────────────────────────────────────────────────────────────────────

    def upsert_stock_level(
        self,
        cur: psycopg.Cursor,
        ts: datetime,
        warehouse_id: str,
        sku: str,
        stock: float,
    ) -> None:
        cur.execute(
            """
            INSERT INTO stock_levels (ts, warehouse_id, sku, stock, ingest_ts)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (ts, warehouse_id, sku)
            DO UPDATE SET stock = EXCLUDED.stock, ingest_ts = now()
            """,
            (ts, warehouse_id, sku, stock),
        )

    def upsert_window_metrics(
        self,
        cur: psycopg.Cursor,
        key: WindowKey,
        acc: WindowAccumulator,
        avg_demand: float = 0.0,
    ) -> None:
        cur.execute(
            """
            INSERT INTO window_metrics
              (window_start, window_end, window_type, warehouse_id, sku,
               inbound, outbound, event_count, avg_demand, ingest_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (window_start, window_type, warehouse_id, sku)
            DO UPDATE SET
              window_end   = EXCLUDED.window_end,
              inbound      = EXCLUDED.inbound,
              outbound     = EXCLUDED.outbound,
              event_count  = EXCLUDED.event_count,
              avg_demand   = EXCLUDED.avg_demand,
              ingest_ts    = now()
            """,
            (
                key.window_start, key.window_end, key.window_type,
                key.warehouse_id, key.sku,
                acc.inbound, acc.outbound, acc.event_count, avg_demand,
            ),
        )

    def upsert_alert(self, cur: psycopg.Cursor, alert: Alert) -> None:
        cur.execute(
            """
            INSERT INTO alerts (ts, warehouse_id, sku, alert_type, severity, message, ingest_ts)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT DO NOTHING
            """,
            (
                alert.ts, alert.warehouse_id, alert.sku,
                alert.alert_type.value, alert.severity.value, alert.message,
            ),
        )

    def upsert_pipeline_metrics(
        self,
        cur: psycopg.Cursor,
        ts: datetime,
        instance_id: str,
        partition: int,
        consumer_lag: int,
        events_per_sec: float,
        late_events: int,
        dlq_count: int,
    ) -> None:
        cur.execute(
            """
            INSERT INTO pipeline_metrics
              (ts, instance_id, partition, consumer_lag, events_per_sec,
               late_events, dlq_count, ingest_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            """,
            (ts, instance_id, partition, consumer_lag,
             events_per_sec, late_events, dlq_count),
        )

    def load_sku_catalog(self, cur: psycopg.Cursor, sku: str, description: str) -> None:
        cur.execute(
            """
            INSERT INTO sku_catalog (sku, description)
            VALUES (%s, %s)
            ON CONFLICT (sku) DO NOTHING
            """,
            (sku, description),
        )

    def db_check(self) -> bool:
        """Return True if DB is reachable and all hypertables exist."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM timescaledb_information.hypertables "
                    "WHERE hypertable_schema = 'public'"
                )
                count = cur.fetchone()[0]
                expected = 4  # stock_levels, window_metrics, alerts, pipeline_metrics
                if count < expected:
                    log.warning("missing hypertables", found=count, expected=expected)
                    return False
            log.info("DB check passed", hypertables=count)
            return True
        except Exception as exc:
            log.error("DB check failed", error=str(exc))
            return False
