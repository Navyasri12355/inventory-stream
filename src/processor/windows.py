"""
src/processor/windows.py
─────────────────────────
Pure (no Kafka, no DB) windowing logic.

Implements AGENTS.md §6:
  - Tumbling windows:  fixed-width, non-overlapping
  - Sliding windows:   overlapping, emit one per slide interval
  - Watermark:         max seen event_time minus WATERMARK_SECONDS
  - Window emission:   emitted when watermark passes window end
  - Late events:       any event whose event_time < current watermark

All functions are stateless or operate on explicitly passed state dicts,
making them straightforward to unit-test.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterator


@dataclass(frozen=True)
class WindowKey:
    warehouse_id: str
    sku: str
    window_start: datetime
    window_end: datetime
    window_type: str  # 'tumbling' | 'sliding'


@dataclass
class WindowAccumulator:
    """Accumulated stats for one (key, window) pair."""
    inbound:     int   = 0
    outbound:    int   = 0
    event_count: int   = 0

    def add(self, delta: int) -> None:
        self.event_count += 1
        if delta > 0:
            self.inbound  += delta
        elif delta < 0:
            self.outbound += abs(delta)


@dataclass
class WindowState:
    """
    Per-partition mutable windowing state.

    tumbling: dict[WindowKey, WindowAccumulator]
    sliding:  dict[WindowKey, WindowAccumulator]
    watermark: current watermark datetime
    """
    watermark:       datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))
    max_event_time:  datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))
    tumbling:        dict[WindowKey, WindowAccumulator] = field(default_factory=dict)
    sliding:         dict[WindowKey, WindowAccumulator] = field(default_factory=dict)


def tumbling_window_key(
    warehouse_id: str,
    sku: str,
    event_time: datetime,
    window_seconds: int,
) -> WindowKey:
    """Return the WindowKey for the tumbling window that contains event_time."""
    epoch = event_time.timestamp()
    w     = float(window_seconds)
    start_ts = (epoch // w) * w
    end_ts   = start_ts + w
    return WindowKey(
        warehouse_id=warehouse_id,
        sku=sku,
        window_start=datetime.fromtimestamp(start_ts, tz=timezone.utc),
        window_end=datetime.fromtimestamp(end_ts,   tz=timezone.utc),
        window_type="tumbling",
    )


def sliding_window_keys(
    warehouse_id: str,
    sku: str,
    event_time: datetime,
    window_seconds: int,
    slide_seconds: int,
) -> list[WindowKey]:
    """
    Return all sliding WindowKeys that cover event_time.
    A sliding window of size W and slide S: an event at time t belongs to
    all windows whose start is in (t - W, t] aligned on slide boundaries.
    """
    W  = float(window_seconds)
    S  = float(slide_seconds)
    ts = event_time.timestamp()

    # Earliest start that still contains ts
    earliest_start = ts - W + S
    # Latest aligned start <= ts
    latest_start   = (ts // S) * S

    keys: list[WindowKey] = []
    start = latest_start
    while start >= earliest_start:
        end = start + W
        if end > ts:  # window actually covers ts
            keys.append(WindowKey(
                warehouse_id=warehouse_id,
                sku=sku,
                window_start=datetime.fromtimestamp(start, tz=timezone.utc),
                window_end=datetime.fromtimestamp(end,   tz=timezone.utc),
                window_type="sliding",
            ))
        start -= S
    return keys


def update_watermark(
    state: WindowState,
    event_time: datetime,
    watermark_seconds: int,
) -> bool:
    """
    Advance max_event_time and recompute watermark.
    Returns True if the watermark advanced (new windows may be emittable).
    """
    if event_time > state.max_event_time:
        state.max_event_time = event_time
        new_wm = event_time - timedelta(seconds=watermark_seconds)
        if new_wm > state.watermark:
            state.watermark = new_wm
            return True
    return False


def is_late(event_time: datetime, watermark: datetime) -> bool:
    """True if event arrived after the watermark — it is too late for its window."""
    return event_time < watermark


def emit_closed_windows(
    state: WindowState,
) -> Iterator[tuple[WindowKey, WindowAccumulator]]:
    """
    Yield and remove all windows whose window_end <= current watermark.
    Call this after every watermark advance.
    """
    closed_tumbling = [
        (k, v) for k, v in state.tumbling.items()
        if k.window_end <= state.watermark
    ]
    for k, v in closed_tumbling:
        del state.tumbling[k]
        yield k, v

    closed_sliding = [
        (k, v) for k, v in state.sliding.items()
        if k.window_end <= state.watermark
    ]
    for k, v in closed_sliding:
        del state.sliding[k]
        yield k, v


def force_flush_windows(
    state: WindowState,
) -> Iterator[tuple[WindowKey, WindowAccumulator]]:
    """
    Idle flush: advance watermark to close all open windows.
    Call when no event has arrived for IDLE_FLUSH_SECONDS wall-clock time.
    """
    if state.tumbling or state.sliding:
        # Push watermark past the latest window end
        all_ends = (
            [k.window_end for k in state.tumbling]
            + [k.window_end for k in state.sliding]
        )
        if all_ends:
            state.watermark = max(all_ends) + timedelta(seconds=1)
        yield from emit_closed_windows(state)
