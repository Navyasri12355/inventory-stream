"""
tests/unit/test_windows.py
───────────────────────────
Unit tests for src/processor/windows.py  — no Kafka or DB required.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.processor.windows import (
    WindowAccumulator,
    WindowState,
    emit_closed_windows,
    force_flush_windows,
    is_late,
    sliding_window_keys,
    tumbling_window_key,
    update_watermark,
)


def _dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


# ── Tumbling window key ───────────────────────────────────────────────────────

class TestTumblingWindowKey:
    def test_aligns_to_window_boundary(self):
        # Window size = 60s; event at t=90 → window [60, 120)
        key = tumbling_window_key("WH-01", "85123A", _dt(90), window_seconds=60)
        assert key.window_start == _dt(60)
        assert key.window_end   == _dt(120)
        assert key.window_type  == "tumbling"

    def test_event_at_boundary_belongs_to_next_window(self):
        # t=120 → window [120, 180)
        key = tumbling_window_key("WH-01", "85123A", _dt(120), window_seconds=60)
        assert key.window_start == _dt(120)

    def test_same_event_time_same_key(self):
        k1 = tumbling_window_key("WH-01", "SKU1", _dt(75), 60)
        k2 = tumbling_window_key("WH-01", "SKU1", _dt(75), 60)
        assert k1 == k2

    def test_different_skus_different_keys(self):
        k1 = tumbling_window_key("WH-01", "SKU1", _dt(75), 60)
        k2 = tumbling_window_key("WH-01", "SKU2", _dt(75), 60)
        assert k1 != k2


# ── Sliding window keys ───────────────────────────────────────────────────────

class TestSlidingWindowKeys:
    def test_event_covered_by_multiple_windows(self):
        # W=300s, S=60s → up to 5 windows cover any event
        keys = sliding_window_keys("WH-01", "SKU1", _dt(300), 300, 60)
        assert len(keys) >= 1
        # All windows must contain t=300
        for k in keys:
            assert k.window_start.timestamp() <= 300 < k.window_end.timestamp()

    def test_all_keys_are_sliding_type(self):
        keys = sliding_window_keys("WH-01", "SKU1", _dt(300), 300, 60)
        for k in keys:
            assert k.window_type == "sliding"


# ── Watermark ────────────────────────────────────────────────────────────────

class TestWatermark:
    def test_watermark_advances(self):
        state = WindowState()
        advanced = update_watermark(state, _dt(200), watermark_seconds=120)
        assert advanced
        assert state.watermark == _dt(80)   # 200 - 120

    def test_watermark_does_not_go_backwards(self):
        state = WindowState()
        update_watermark(state, _dt(200), 120)
        advanced = update_watermark(state, _dt(150), 120)
        assert not advanced
        assert state.watermark == _dt(80)


# ── Late event detection ──────────────────────────────────────────────────────

class TestIsLate:
    def test_event_before_watermark_is_late(self):
        wm = _dt(100)
        assert is_late(_dt(99), wm)

    def test_event_at_watermark_is_not_late(self):
        # Watermark boundary is exclusive: event_time == watermark is NOT late
        wm = _dt(100)
        assert not is_late(_dt(100), wm)

    def test_event_just_before_watermark_is_late(self):
        wm = _dt(100)
        assert is_late(_dt(99.999), wm)

    def test_event_after_watermark_not_late(self):
        wm = _dt(100)
        assert not is_late(_dt(101), wm)


# ── Window emission ───────────────────────────────────────────────────────────

class TestEmitClosedWindows:
    def test_emits_windows_past_watermark(self):
        from src.processor.windows import WindowKey, WindowAccumulator
        state = WindowState()
        state.watermark = _dt(200)

        key = WindowKey("WH-01", "SKU1", _dt(0), _dt(60), "tumbling")
        acc = WindowAccumulator()
        acc.add(-5)
        state.tumbling[key] = acc

        emitted = list(emit_closed_windows(state))
        assert len(emitted) == 1
        assert emitted[0][0] == key
        assert len(state.tumbling) == 0

    def test_does_not_emit_open_windows(self):
        from src.processor.windows import WindowKey
        state = WindowState()
        state.watermark = _dt(50)

        key = WindowKey("WH-01", "SKU1", _dt(0), _dt(60), "tumbling")
        state.tumbling[key] = WindowAccumulator()

        emitted = list(emit_closed_windows(state))
        assert len(emitted) == 0
        assert len(state.tumbling) == 1


# ── Force flush ───────────────────────────────────────────────────────────────

class TestForceFlush:
    def test_flush_emits_all_open_windows(self):
        from src.processor.windows import WindowKey
        state = WindowState()
        for i in range(3):
            key = WindowKey("WH-01", f"SKU{i}", _dt(i * 60), _dt((i + 1) * 60), "tumbling")
            state.tumbling[key] = WindowAccumulator()

        emitted = list(force_flush_windows(state))
        assert len(emitted) == 3
        assert len(state.tumbling) == 0
