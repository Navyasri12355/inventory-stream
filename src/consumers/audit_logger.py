"""
src/consumers/audit_logger.py
──────────────────────────────
Second independent consumer group on inventory-events (demonstrates fan-out).

Group: audit-logger (1 instance, reads all 6 partitions).
Logs every event to stdout and appends to data/truth/audit.jsonl.
Does NOT affect the processor group's offsets.

Run:
    python -m src.consumers.audit_logger
"""
from __future__ import annotations

import json
import signal
from pathlib import Path

from confluent_kafka import KafkaError

from config.settings import settings as _settings
from src.common.kafka_utils import make_consumer
from src.common.logging_setup import get_logger, setup_logging
from src.common.models import InventoryEvent

log = get_logger(__name__)
_running = True


def _handle_signal(sig, frame) -> None:  # noqa: ANN001
    global _running
    _running = False


def run() -> None:
    setup_logging("INFO")
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    cfg = _settings
    audit_path = cfg.truth_dir / "audit.jsonl"
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    consumer = make_consumer(
        group_id="audit-logger",
        topics=[cfg.topic_events],
    )

    with audit_path.open("a", encoding="utf-8") as fh:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("error", error=str(msg.error()))
                continue

            try:
                ev = InventoryEvent.model_validate_json(msg.value())
                log.info(
                    "audit",
                    event_type=ev.event_type.value,
                    sku=ev.sku,
                    warehouse=ev.warehouse_id,
                    qty=ev.quantity,
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
                fh.write(json.dumps(ev.model_dump(mode="json"), default=str) + "\n")
            except Exception as exc:
                log.warning("parse error", error=str(exc), partition=msg.partition())

    consumer.close()
    log.info("audit logger stopped")


if __name__ == "__main__":
    run()
