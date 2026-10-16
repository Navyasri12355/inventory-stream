"""
src/processor/main.py
──────────────────────
Main stream processor — Phase 8.

Consume loop with:
  - on_assign / on_revoke rebalance callbacks (checkpoint on revoke)
  - Deduplication (event_id within watermark horizon)
  - Tumbling + sliding window aggregation
  - Alert generation and publish to inventory-alerts
  - Malformed messages → inventory-dlq
  - Atomic DB write + Kafka offset commit per batch
  - SIGTERM / SIGINT graceful shutdown with final flush

Run 3 instances in 3 separate terminals to share the 6 partitions:
    python -m src.processor.main --instance-id proc-1
    python -m src.processor.main --instance-id proc-2
    python -m src.processor.main --instance-id proc-3
"""
from __future__ import annotations

import argparse
import json
import signal
import socket
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition

from config.settings import settings as _settings
from src.common.kafka_utils import make_consumer, make_producer, safe_flush
from src.common.logging_setup import get_logger, setup_logging
from src.common.models import (
    Alert,
    AlertSeverity,
    AlertType,
    DLQEnvelope,
    EventType,
    InventoryEvent,
)
from src.processor.aggregations import (
    AlertCooldown,
    check_abnormal_outflow,
    check_low_stock,
    check_stockout_risk,
    days_of_cover,
    update_ema,
    update_stock,
)
from src.processor.dedup import Deduplicator
from src.processor.sink import Sink
from src.processor.state import ProcessorState, StateManager
from src.processor.windows import (
    WindowState,
    emit_closed_windows,
    force_flush_windows,
    is_late,
    sliding_window_keys,
    tumbling_window_key,
    update_watermark,
)

log = get_logger(__name__)
_running = True


def _handle_signal(sig, frame) -> None:  # noqa: ANN001
    global _running
    log.info("shutdown signal received", signal=sig)
    _running = False


class Processor:
    def __init__(self, instance_id: str, cfg=None) -> None:
        self.instance_id = instance_id
        self.cfg = cfg or _settings

        self.sink = Sink(self.cfg)
        self.producer = make_producer()

        # Per-partition state
        self._states: dict[int, ProcessorState] = {}
        self._state_mgr: StateManager | None = None

        self._cooldown = AlertCooldown(cooldown_seconds=300)

        # Metrics
        self._events_processed = 0
        self._late_count = 0
        self._dlq_count = 0
        self._last_metrics_ts = time.monotonic()

    # ─────────────────────────────────────────────────────────────────────────
    # Rebalance callbacks
    # ─────────────────────────────────────────────────────────────────────────

    def on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("partitions assigned", partitions=[p.partition for p in partitions])
        for tp in partitions:
            p = tp.partition
            if p not in self._states:
                self._states[p] = ProcessorState(partition_id=p)
            # Restore committed offset and seek
            if self._state_mgr:
                offset = self._state_mgr.load_offset(tp.topic, p)
                if offset is not None:
                    tp.offset = offset + 1  # resume from next
                    consumer.seek(tp)
                    log.info("restored offset", partition=p, offset=offset)

    def on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("partitions revoked", partitions=[p.partition for p in partitions])
        # Checkpoint: flush open windows and commit state
        for tp in partitions:
            state = self._states.get(tp.partition)
            if state:
                self._flush_windows(state, tp.topic, tp.partition)

    # ─────────────────────────────────────────────────────────────────────────
    # Main consume loop
    # ─────────────────────────────────────────────────────────────────────────

    def run(self) -> None:
        setup_logging("INFO")
        signal.signal(signal.SIGINT,  _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        self.sink.connect()
        self._state_mgr = StateManager(self.sink.conn)

        consumer = make_consumer(
            group_id="inventory-processor",
            topics=[self.cfg.topic_events],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
        )

        last_message_time = time.monotonic()
        idle_flush_seconds = self.cfg.idle_flush_seconds

        try:
            while _running:
                msg: Message | None = consumer.poll(timeout=1.0)

                if msg is None:
                    # Idle flush if no events for a while
                    if time.monotonic() - last_message_time > idle_flush_seconds:
                        for p, state in self._states.items():
                            self._idle_flush(state, self.cfg.topic_events, p)
                        last_message_time = time.monotonic()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("consumer error", error=str(msg.error()))
                    continue

                last_message_time = time.monotonic()
                self._handle_message(msg)
                self._maybe_emit_metrics(msg.partition())

        finally:
            log.info("shutting down processor")
            for p, state in self._states.items():
                self._flush_windows(state, self.cfg.topic_events, p)
            safe_flush(self.producer)
            consumer.close()
            self.sink.close()

    # ─────────────────────────────────────────────────────────────────────────
    # Message handling
    # ─────────────────────────────────────────────────────────────────────────

    def _handle_message(self, msg: Message) -> None:
        partition = msg.partition()
        offset    = msg.offset()
        state     = self._states.setdefault(partition, ProcessorState(partition_id=partition))

        # Parse and validate
        try:
            ev = InventoryEvent.model_validate_json(msg.value())
        except Exception as exc:
            log.warning("malformed message", partition=partition, offset=offset, error=str(exc))
            self._send_to_dlq(msg, str(exc))
            self._dlq_count += 1
            return

        # Dedup
        if state.deduper.is_duplicate(ev.event_id, ev.event_time):
            log.debug("duplicate", event_id=ev.event_id, sku=ev.sku)
            return

        # Update watermark
        update_watermark(state.window_state, ev.event_time, self.cfg.watermark_seconds)

        # Late event
        if is_late(ev.event_time, state.window_state.watermark):
            log.debug("late event", sku=ev.sku, event_time=ev.event_time.isoformat())
            self._send_to_dlq(msg, "LATE", reason="event_time before watermark")
            self._late_count += 1
            return

        # Update windows
        tw_key = tumbling_window_key(
            ev.warehouse_id, ev.sku, ev.event_time,
            self.cfg.tumbling_window_seconds
        )
        if tw_key not in state.window_state.tumbling:
            from src.processor.windows import WindowAccumulator
            state.window_state.tumbling[tw_key] = WindowAccumulator()
        state.window_state.tumbling[tw_key].add(ev.stock_delta)

        for sw_key in sliding_window_keys(
            ev.warehouse_id, ev.sku, ev.event_time,
            self.cfg.sliding_window_seconds, self.cfg.slide_seconds
        ):
            if sw_key not in state.window_state.sliding:
                from src.processor.windows import WindowAccumulator
                state.window_state.sliding[sw_key] = WindowAccumulator()
            state.window_state.sliding[sw_key].add(ev.stock_delta)

        # Update running stock and EMA
        new_stock = update_stock(state.stock, ev.warehouse_id, ev.sku, ev.stock_delta)
        if ev.event_type == EventType.OUTBOUND:
            update_ema(state.ema, ev.warehouse_id, ev.sku, ev.quantity, self.cfg.ema_alpha)

        # Emit closed windows + stock + alerts atomically
        closed = list(emit_closed_windows(state.window_state))
        if closed or ev.event_type == EventType.OUTBOUND:
            self._write_batch(
                state, closed, ev, new_stock,
                topic=msg.topic(), partition=partition, offset=offset
            )

        self._events_processed += 1

    def _write_batch(
        self,
        state: ProcessorState,
        closed_windows,
        ev: InventoryEvent,
        new_stock: float,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """Write all results + offset in one DB transaction, then commit to Kafka."""
        try:
            with self.sink.transaction() as cur:
                # Stock level
                self.sink.upsert_stock_level(cur, ev.event_time, ev.warehouse_id, ev.sku, new_stock)

                # Window metrics
                ema_val = state.ema.get((ev.warehouse_id, ev.sku), 0.0)
                for wk, acc in closed_windows:
                    self.sink.upsert_window_metrics(cur, wk, acc, avg_demand=ema_val)

                # Alerts
                doc = days_of_cover(new_stock, ema_val, self.cfg.seconds_per_data_day)
                alerts: list[Alert] = []
                low = check_low_stock(
                    new_stock, ev.warehouse_id, ev.sku,
                    avg_daily_demand=ema_val * self.cfg.tumbling_window_seconds,
                    lead_time_days=float(self.cfg.lead_time_days_max),
                    safety_factor=self.cfg.safety_factor,
                    event_time=ev.event_time,
                    cooldown=self._cooldown,
                )
                if low:
                    alerts.append(low)
                sr = check_stockout_risk(doc, ev.warehouse_id, ev.sku, 3.0, ev.event_time, self._cooldown)
                if sr:
                    alerts.append(sr)

                # Sliding-avg outflow for ABNORMAL check
                sw_keys = [
                    k for k in state.window_state.sliding
                    if k.warehouse_id == ev.warehouse_id and k.sku == ev.sku
                ]
                if sw_keys and closed_windows:
                    latest_sw = max(sw_keys, key=lambda k: k.window_start)
                    sw_acc = state.window_state.sliding.get(latest_sw)
                    if sw_acc:
                        abn = check_abnormal_outflow(
                            sum(acc.outbound for _, acc in closed_windows if _.warehouse_id == ev.warehouse_id and _.sku == ev.sku),
                            float(sw_acc.outbound) / max(1, sw_acc.event_count),
                            ev.warehouse_id, ev.sku, ev.event_time, self._cooldown,
                        )
                        if abn:
                            alerts.append(abn)

                for alert in alerts:
                    self.sink.upsert_alert(cur, alert)
                    self.producer.produce(
                        topic=self.cfg.topic_alerts,
                        key=alert.sku.encode(),
                        value=alert.to_kafka_value(),
                    )

                # Offset checkpoint
                if self._state_mgr:
                    self._state_mgr.save_offset(cur, topic, partition, offset)

            state.committed_offset = offset

        except Exception as exc:
            log.error("write_batch failed — not committing offset", error=str(exc))

    def _flush_windows(self, state: ProcessorState, topic: str, partition: int) -> None:
        """Force-flush all open windows (called on revoke/shutdown)."""
        closed = list(force_flush_windows(state.window_state))
        if closed and self._state_mgr:
            try:
                with self.sink.transaction() as cur:
                    ema_val = 0.0
                    for wk, acc in closed:
                        self.sink.upsert_window_metrics(cur, wk, acc, avg_demand=ema_val)
                    self._state_mgr.save_offset(cur, topic, partition, state.committed_offset)
            except Exception as exc:
                log.error("flush_windows failed", error=str(exc))

    def _idle_flush(self, state: ProcessorState, topic: str, partition: int) -> None:
        self._flush_windows(state, topic, partition)

    def _send_to_dlq(self, msg: Message, error: str, reason: str = "") -> None:
        envelope = DLQEnvelope(
            raw_value=msg.value().decode(errors="replace"),
            error=f"{error} {reason}".strip(),
            source_topic=msg.topic(),
            source_partition=msg.partition(),
            source_offset=msg.offset(),
        )
        self.producer.produce(
            topic=self.cfg.topic_dlq,
            key=b"dlq",
            value=envelope.to_kafka_value(),
            headers=envelope.to_headers(),
        )
        self.producer.poll(0)

    def _maybe_emit_metrics(self, partition: int) -> None:
        now = time.monotonic()
        elapsed = now - self._last_metrics_ts
        if elapsed < 10:
            return
        eps = self._events_processed / max(elapsed, 1)
        try:
            with self.sink.transaction() as cur:
                self.sink.upsert_pipeline_metrics(
                    cur,
                    ts=datetime.now(timezone.utc),
                    instance_id=self.instance_id,
                    partition=partition,
                    consumer_lag=0,  # TODO: query actual lag
                    events_per_sec=eps,
                    late_events=self._late_count,
                    dlq_count=self._dlq_count,
                )
        except Exception:
            pass
        self._events_processed = 0
        self._late_count = 0
        self._dlq_count = 0
        self._last_metrics_ts = now


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Inventory stream processor")
    p.add_argument("--instance-id", default=socket.gethostname())
    return p


if __name__ == "__main__":
    args = _build_parser().parse_args()
    Processor(instance_id=args.instance_id).run()
