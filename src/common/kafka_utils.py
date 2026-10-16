"""
src/common/kafka_utils.py
──────────────────────────
Factory functions for confluent-kafka Producer and Consumer with all required
settings from AGENTS.md §7 (fault-tolerance rules) baked in.

Usage:
    from src.common.kafka_utils import make_producer, make_consumer

    producer = make_producer()
    consumer = make_consumer("inventory-processor", ["inventory-events"])
"""
from __future__ import annotations

from typing import Any, Callable, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from config.settings import settings
from src.common.logging_setup import get_logger

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Delivery callback (used by Producer.produce())
# ─────────────────────────────────────────────────────────────────────────────

def _default_delivery_callback(err: Optional[KafkaError], msg: Any) -> None:
    if err:
        log.error(
            "delivery failed",
            topic=msg.topic(),
            partition=msg.partition(),
            error=str(err),
        )
    else:
        log.debug(
            "delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Producer factory
# ─────────────────────────────────────────────────────────────────────────────

def make_producer(
    extra_config: Optional[dict[str, Any]] = None,
    on_delivery: Optional[Callable] = None,
) -> Producer:
    """
    Return a confluent-kafka Producer configured per AGENTS.md §7:
    - acks=all
    - enable.idempotence=true
    - retries with exponential backoff
    """
    conf: dict[str, Any] = {
        "bootstrap.servers":   settings.kafka_bootstrap,
        "acks":                "all",
        "enable.idempotence":  True,
        "retries":             10,
        "retry.backoff.ms":    250,
        "retry.backoff.max.ms": 5_000,
        "linger.ms":           5,
        "compression.type":    "snappy",
        # Delivery callback is set globally here; can be overridden per produce() call
        "on_delivery":         on_delivery or _default_delivery_callback,
    }
    if extra_config:
        conf.update(extra_config)
    return Producer(conf)


# ─────────────────────────────────────────────────────────────────────────────
# Consumer factory
# ─────────────────────────────────────────────────────────────────────────────

def make_consumer(
    group_id: str,
    topics: list[str],
    extra_config: Optional[dict[str, Any]] = None,
    on_assign: Optional[Callable] = None,
    on_revoke: Optional[Callable] = None,
) -> Consumer:
    """
    Return a confluent-kafka Consumer configured per AGENTS.md §7:
    - enable.auto.commit=false  (offsets committed only after durable writes)
    - auto.offset.reset=earliest (safe: processor will seek to stored offsets on assign)
    """
    conf: dict[str, Any] = {
        "bootstrap.servers":        settings.kafka_bootstrap,
        "group.id":                 group_id,
        "enable.auto.commit":       False,
        "auto.offset.reset":        "earliest",
        "isolation.level":          "read_committed",
        "max.poll.interval.ms":     300_000,
        "session.timeout.ms":       30_000,
        "heartbeat.interval.ms":    10_000,
    }
    if extra_config:
        conf.update(extra_config)

    consumer = Consumer(conf)

    consumer.subscribe(
        topics,
        on_assign=on_assign,
        on_revoke=on_revoke,
    )
    log.info("subscribed", group_id=group_id, topics=topics)
    return consumer


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def safe_flush(producer: Producer, timeout: float = 30.0) -> None:
    """Flush with a timeout; log a warning if not all messages are delivered."""
    remaining = producer.flush(timeout)
    if remaining > 0:
        log.warning("flush incomplete", undelivered=remaining)


def is_retriable(err: KafkaError) -> bool:
    """Return True for transient Kafka errors that should be retried."""
    return err.retriable()
