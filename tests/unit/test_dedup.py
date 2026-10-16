"""
tests/unit/test_dedup.py
─────────────────────────
Unit tests for src/processor/dedup.py
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.processor.dedup import Deduplicator


def _dt(offset_s: float) -> datetime:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_s)


class TestDeduplicator:
    def test_first_occurrence_not_duplicate(self):
        d = Deduplicator(watermark_seconds=120)
        assert not d.is_duplicate("abc", _dt(0))

    def test_second_occurrence_is_duplicate(self):
        d = Deduplicator(watermark_seconds=120)
        d.is_duplicate("abc", _dt(0))
        assert d.is_duplicate("abc", _dt(1))

    def test_different_ids_not_duplicates(self):
        d = Deduplicator(watermark_seconds=120)
        d.is_duplicate("abc", _dt(0))
        assert not d.is_duplicate("xyz", _dt(0))

    def test_eviction_allows_reuse_after_horizon(self):
        d = Deduplicator(watermark_seconds=10)  # horizon = 20s
        d.is_duplicate("abc", _dt(0))
        # Trigger eviction by presenting an event far in the future
        d.is_duplicate("new", _dt(30))  # evicts "abc" (expiry was _dt(20))
        # Now "abc" can be registered again
        assert not d.is_duplicate("abc", _dt(31))

    def test_seen_count_correct(self):
        d = Deduplicator(watermark_seconds=120)
        d.is_duplicate("a", _dt(0))
        d.is_duplicate("b", _dt(0))
        assert d.seen_count == 2

    def test_total_evictions_increments(self):
        d = Deduplicator(watermark_seconds=5)
        d.is_duplicate("a", _dt(0))
        d.is_duplicate("b", _dt(20))  # evicts "a"
        assert d.total_evictions >= 1
