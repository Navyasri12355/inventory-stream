"""
src/producer/replay.py
───────────────────────
UCI replay producer — the main entry point for Phase 5.

Reads cleaned online_retail_ii.csv, maps rows to InventoryEvents via mapping.py,
merges simulated replenishment/fault events through a min-heap ordered by event_time,
and publishes to Kafka with pacing (sleep until wall clock reaches event_time).

CLI:
    python -m src.producer.replay [options]

Key options (all have env-var equivalents in config/settings.py):
    --speedup S         data-seconds per real second (default 600)
    --time-mode         rebased | original
    --top-skus N        top N SKUs (default 500)
    --replay-start ISO  fix replay start time
    --max-gap-seconds   cap per gap in real seconds (default 1)
    --max-events N      stop after N events (default: unlimited)
    --seed N            RNG seed (default 42)
    --clean             disable fault injection
    --adjust-rate R     extra simulated ADJUSTMENT probability (default 0 in replay)
    --damage-rate R     extra simulated DAMAGE probability (default 0 in replay)
    --warehouse-strategy hash | country-group
"""
from __future__ import annotations

import argparse
import heapq
import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import numpy as np

from config.settings import settings as _settings
from src.common.kafka_utils import make_producer, safe_flush
from src.common.logging_setup import get_logger, setup_logging
from src.common.models import InventoryEvent
from src.producer.faults import FaultInjector
from src.producer.mapping import DataCleaner
from src.producer.replenishment import ReplenishmentPlanner

log = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Emitted-event log (ground truth)
# ─────────────────────────────────────────────────────────────────────────────

class EventLog:
    """Appends every emitted event to data/truth/events.jsonl for verification."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open("a", encoding="utf-8")

    def record(
        self,
        ev: InventoryEvent | bytes,
        fault_kind: str | None,
        partition: int | None = None,
        offset: int | None = None,
    ) -> None:
        if isinstance(ev, InventoryEvent):
            d = ev.model_dump(mode="json")
        else:
            d = {"_raw": ev.decode(errors="replace")}
        d["_fault_kind"] = fault_kind
        d["_partition"]  = partition
        d["_offset"]     = offset
        self._fh.write(json.dumps(d, default=str) + "\n")

    def close(self) -> None:
        self._fh.flush()
        self._fh.close()


# ─────────────────────────────────────────────────────────────────────────────
# Heap-based event merger
# ─────────────────────────────────────────────────────────────────────────────

def _heap_merge(
    uci_stream: Iterator[tuple[datetime, InventoryEvent]],
    sim_events: list[InventoryEvent],
) -> Iterator[tuple[datetime, InventoryEvent]]:
    """
    Merge UCI stream and pre-scheduled sim events into a single time-ordered stream.
    Uses a min-heap keyed on event_time.
    """
    heap: list[tuple[float, int, datetime, InventoryEvent]] = []
    counter = 0  # tie-breaker

    # Seed heap with the first UCI event and all pre-scheduled sim events
    for sev in sim_events:
        ts = sev.event_time.timestamp()
        heapq.heappush(heap, (ts, counter, sev.event_time, sev))
        counter += 1

    uci_iter = iter(uci_stream)
    next_uci: tuple[datetime, InventoryEvent] | None = None

    def _load_next_uci() -> None:
        nonlocal next_uci
        try:
            next_uci = next(uci_iter)
            t, ev = next_uci
            heapq.heappush(heap, (t.timestamp(), counter, t, ev))
        except StopIteration:
            next_uci = None

    _load_next_uci()

    while heap:
        _, _, ev_time, ev = heapq.heappop(heap)
        yield ev_time, ev
        # If this was the UCI event, load the next one
        if next_uci and next_uci[1] is ev:
            _load_next_uci()


# ─────────────────────────────────────────────────────────────────────────────
# Main replay loop
# ─────────────────────────────────────────────────────────────────────────────

_running = True

def _handle_signal(sig, frame):  # noqa: ANN001
    global _running
    log.info("shutdown signal received", signal=sig)
    _running = False


def run_replay(args: argparse.Namespace) -> None:
    setup_logging("INFO")
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    cfg = _settings
    replay_start = (
        datetime.fromisoformat(args.replay_start).replace(tzinfo=timezone.utc)
        if args.replay_start
        else datetime.now(timezone.utc).replace(microsecond=0)
    )
    speedup      = args.speedup
    max_gap      = args.max_gap_seconds
    max_events   = args.max_events   # None means unlimited
    clean_mode   = args.clean
    # In replay mode ADJUSTMENT/DAMAGE come from the real dataset;
    # extra simulated events default to 0 (AGENTS.md §5.5 rule 5).
    adjust_rate  = args.adjust_rate
    damage_rate  = args.damage_rate

    rng = np.random.default_rng(cfg.seed)

    # ── Load and clean data ────────────────────────────────────────────────
    cleaner = DataCleaner(cfg)
    df = cleaner.load_and_clean()
    df = cleaner.filter_top_skus(df)

    # ── Replenishment planner ──────────────────────────────────────────────
    planner = ReplenishmentPlanner(cfg, rng)
    planner.compute_demand(df, cfg.warehouses)
    opening_events = planner.opening_adjustments(replay_start)

    # ── Fault injector ─────────────────────────────────────────────────────
    # FaultInjector handles dup/late/malformed only — not adjust/damage rates.
    # adjust_rate and damage_rate are passed explicitly to maybe_adjust_damage()
    # below so replay uses 0.0 (real write-offs from dataset) and simulator
    # can pass its own non-zero values.
    injector = FaultInjector(cfg, rng) if not clean_mode else None

    # ── Kafka producer ─────────────────────────────────────────────────────
    producer = make_producer()
    event_log = EventLog(cfg.truth_dir / "events.jsonl")

    # ── Build the merged stream ────────────────────────────────────────────
    uci_stream = cleaner.iter_events(
        df, replay_start=replay_start, speedup=speedup,
        max_gap_seconds=max_gap, max_events=max_events,
    )
    base_stream = _heap_merge(uci_stream, opening_events)

    # Wrap with fault injection
    if injector:
        wrapped = injector.wrap(base_stream)
    else:
        wrapped = ((et, ev, None) for et, ev in base_stream)

    # ── Replay loop ────────────────────────────────────────────────────────
    published = 0
    scheduled_replenishments: list[InventoryEvent] = []

    for emit_time, payload, fault_kind in wrapped:
        if not _running:
            break

        # Pace: sleep until wall clock reaches emit_time
        now = datetime.now(timezone.utc)
        delay = (emit_time - now).total_seconds()
        if delay > 0:
            time.sleep(delay)

        # Determine topic and key
        if isinstance(payload, InventoryEvent):
            value = payload.to_kafka_value()
            key   = payload.kafka_key.encode()
            topic = cfg.topic_events
        else:
            # Malformed bytes → DLQ is handled by the consumer; still publish to events
            value = payload
            key   = b"malformed"
            topic = cfg.topic_events

        producer.produce(
            topic=topic,
            key=key,
            value=value,
        )
        producer.poll(0)  # trigger delivery callbacks

        # Log event
        event_log.record(payload, fault_kind)
        published += 1

        if published % 1000 == 0:
            log.info("published", count=published)

        # Post-event: update sim stock and check reorder
        if isinstance(payload, InventoryEvent) and fault_kind is None:
            planner.apply_event(payload)
            new_repls = planner.check_reorder(
                payload.warehouse_id, payload.sku, emit_time, speedup
            )
            # Inject extra adj/damage after OUTBOUND if rates > 0
            # (default 0 in replay mode; real write-offs come from dataset)
            if payload.event_type.value == "OUTBOUND":
                new_repls += planner.maybe_adjust_damage(
                    payload, emit_time,
                    adjust_rate=adjust_rate,
                    damage_rate=damage_rate,
                )
            scheduled_replenishments.extend(new_repls)

    log.info("replay complete", published=published)
    safe_flush(producer)
    event_log.close()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="UCI Online Retail II replay producer")
    p.add_argument("--speedup",        type=float, default=_settings.speedup)
    p.add_argument("--time-mode",      default=_settings.time_mode,
                   choices=["rebased", "original"])
    p.add_argument("--top-skus",       type=int,   default=_settings.top_skus)
    p.add_argument("--replay-start",   default=None,
                   help="ISO-8601 UTC start time (default: now)")
    p.add_argument("--max-gap-seconds",type=float, default=_settings.max_gap_seconds)
    p.add_argument("--max-events",     type=int,   default=None,
                   help="Stop after N events (default: unlimited). "
                        "E.g. 28000 events at --speedup 600 takes ~20 min.")
    p.add_argument("--seed",           type=int,   default=_settings.seed)
    p.add_argument("--clean",          action="store_true",
                   help="Disable fault injection")
    p.add_argument("--adjust-rate",    type=float, default=0.0,
                   help="Extra simulated ADJUSTMENT probability per OUTBOUND "
                        "(default 0 in replay; real write-offs come from dataset)")
    p.add_argument("--damage-rate",    type=float, default=0.0,
                   help="Extra simulated DAMAGE probability per OUTBOUND "
                        "(default 0 in replay; real write-offs come from dataset)")
    p.add_argument("--warehouse-strategy", default=_settings.warehouse_strategy,
                   choices=["hash", "country-group"])
    return p


if __name__ == "__main__":
    run_replay(_build_parser().parse_args())
