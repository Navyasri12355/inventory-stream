"""
src/consumers/alert_listener.py
────────────────────────────────
Consumer that reads `inventory-alerts` (group: alert-listener).

Demonstrates:
  - A third independent consumer group
  - Reading a topic that is written by the processor (not the producer)
  - Severity-coloured log output suitable for live demo

Run:
    python -m src.consumers.alert_listener
"""
from __future__ import annotations

import json
import signal

from confluent_kafka import KafkaError

from config.settings import settings as _settings
from src.common.kafka_utils import make_consumer
from src.common.logging_setup import get_logger, setup_logging
from src.common.models import Alert

log = get_logger(__name__)
_running = True

_SEVERITY_EMOJI = {
    "critical": "🔴",
    "warning":  "🟡",
    "info":     "🔵",
}


def _handle_signal(sig, frame) -> None:  # noqa: ANN001
    global _running
    _running = False


def run() -> None:
    setup_logging("INFO")
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer = make_consumer(
        group_id="alert-listener",
        topics=[_settings.topic_alerts],
    )

    log.info("alert listener started", topic=_settings.topic_alerts)

    while _running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("consumer error", error=str(msg.error()))
            continue

        try:
            alert = Alert.model_validate_json(msg.value())
            emoji = _SEVERITY_EMOJI.get(alert.severity.value, "⚪")
            log.info(
                f"{emoji} ALERT",
                alert_type=alert.alert_type.value,
                severity=alert.severity.value,
                warehouse=alert.warehouse_id,
                sku=alert.sku,
                message=alert.message,
                ts=alert.ts.isoformat(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
        except Exception as exc:
            log.warning("parse error", error=str(exc), raw=msg.value()[:120])

    consumer.close()
    log.info("alert listener stopped")


if __name__ == "__main__":
    run()
