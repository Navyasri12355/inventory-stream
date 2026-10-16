"""
src/producer/simulator.py
──────────────────────────
Simulator-only producer: generates synthetic inventory events without the
UCI dataset.  Use when the CSV is unavailable or for throughput benchmarks.

CLI:
    python -m src.producer.simulator [--rate 200] [--duration 300] [--seed 42]

Simulates for all (warehouse, sku) combinations in the configured warehouses.
Generates: OUTBOUND demand, INBOUND replenishment, TRANSFER, ADJUSTMENT, DAMAGE.
Uses the same replenishment and fault-injection logic as the UCI replay.
"""
from __future__ import annotations

import argparse
import signal
import time
import uuid
import zlib
from datetime import datetime, timedelta, timezone

import numpy as np

from config.settings import settings as _settings
from src.common.kafka_utils import make_producer, safe_flush
from src.common.logging_setup import get_logger, setup_logging
from src.common.models import (
    EventSource,
    EventType,
    InventoryEvent,
)
from src.producer.faults import FaultInjector
from src.producer.replenishment import ReplenishmentPlanner

log = get_logger(__name__)

# A small fixed set of synthetic SKUs for the simulator
_SIM_SKUS = [
    "10001A", "10002B", "10003C", "10004D", "10005E",
    "20001A", "20002B", "20003C", "20004D", "20005E",
    "30001A", "30002B", "30003C",
]

_running = True


def _handle_signal(sig, frame) -> None:  # noqa: ANN001
    global _running
    _running = False


def _zone(sku: str) -> str:
    return f"Z{zlib.crc32(sku.encode()) & 0xFFFFFFFF % 8}"


def run_simulator(args: argparse.Namespace) -> None:
    setup_logging("INFO")
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    cfg     = _settings
    rate    = args.rate        # events per second
    duration = args.duration   # seconds; 0 = run forever
    rng     = np.random.default_rng(args.seed)

    warehouses = cfg.warehouses
    skus = _SIM_SKUS

    # Pre-seed simulated stock
    planner = ReplenishmentPlanner(cfg, rng)
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    for wh in warehouses:
        for sku in skus:
            planner._avg_daily_demand[(wh, sku)] = float(rng.integers(5, 50))
    opening = planner.opening_adjustments(now_utc)

    injector = FaultInjector(cfg, rng) if not args.clean else None
    producer = make_producer()

    published = 0
    deadline  = (now_utc + timedelta(seconds=duration)) if duration > 0 else None
    interval  = 1.0 / rate if rate > 0 else 0.0

    # Emit opening adjustments first
    for ev in opening:
        _emit(producer, ev)
        published += 1
        planner.apply_event(ev)

    log.info("simulator running", rate=rate, warehouses=warehouses, skus=len(skus))

    while _running:
        if deadline and datetime.now(timezone.utc) >= deadline:
            break

        ev_time = datetime.now(timezone.utc)
        wh  = warehouses[int(rng.integers(0, len(warehouses)))]
        sku = skus[int(rng.integers(0, len(skus)))]
        qty = int(rng.integers(1, 20))

        ev = InventoryEvent(
            event_id=str(uuid.uuid4()),
            event_time=ev_time,
            warehouse_id=wh,
            sku=sku,
            event_type=EventType.OUTBOUND,
            quantity=qty,
            zone=_zone(sku),
            source=EventSource.SIMULATOR,
        )

        # Fault injection
        if injector:
            for _, payload, _ in injector.wrap(iter([(ev_time, ev)])):
                _emit(producer, payload)
                published += 1
        else:
            _emit(producer, ev)
            published += 1

        planner.apply_event(ev)
        extras = planner.check_reorder(wh, sku, ev_time)
        # Simulator has no real write-offs; use non-zero defaults (AGENTS.md §5.5 rule 5)
        extras += planner.maybe_adjust_damage(ev, ev_time, adjust_rate=0.002, damage_rate=0.001)
        for ex in extras:
            _emit(producer, ex)
            published += 1
            planner.apply_event(ex)

        producer.poll(0)
        if published % 500 == 0:
            log.info("simulated", count=published)

        if interval > 0:
            time.sleep(interval)

    log.info("simulator done", published=published)
    safe_flush(producer)


def _emit(producer, payload: InventoryEvent | bytes) -> None:
    if isinstance(payload, InventoryEvent):
        producer.produce(
            topic=_settings.topic_events,
            key=payload.kafka_key.encode(),
            value=payload.to_kafka_value(),
        )
    else:
        producer.produce(
            topic=_settings.topic_events,
            key=b"malformed",
            value=payload,
        )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Inventory event simulator (no dataset needed)")
    p.add_argument("--rate",     type=float, default=50.0, help="events/second")
    p.add_argument("--duration", type=int,   default=0,    help="seconds; 0=forever")
    p.add_argument("--seed",     type=int,   default=_settings.seed)
    p.add_argument("--clean",    action="store_true", help="Disable fault injection")
    return p


if __name__ == "__main__":
    run_simulator(_build_parser().parse_args())
