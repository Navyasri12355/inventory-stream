"""
src/processor/dedup.py
───────────────────────
Event-ID deduplication within the watermark horizon (AGENTS.md §6 rule 9).

Stores seen event_ids in a bounded set.  When the watermark advances past
an event's event_time + watermark_seconds, its ID is eligible for eviction.

Design:
  - dict[event_id -> expiry_time] where expiry = event_time + 2 * watermark_window
  - On each event, evict all expired IDs first, then check/register the new one
  - This is intentionally simple; a production system would use a Bloom filter

Usage:
    deduper = Deduplicator(watermark_seconds=120)
    if deduper.is_duplicate(event_id, event_time):
        send_to_dlq(...)
    else:
        process(...)
"""
from __future__ import annotations

from datetime import datetime, timedelta


class Deduplicator:
    def __init__(self, watermark_seconds: int = 120) -> None:
        self._horizon = timedelta(seconds=watermark_seconds * 2)
        # event_id -> expiry datetime
        self._seen: dict[str, datetime] = {}
        self._evictions = 0

    def is_duplicate(self, event_id: str, event_time: datetime) -> bool:
        """Return True if this event_id was already seen within the horizon."""
        self._evict(event_time)
        if event_id in self._seen:
            return True
        # Register
        self._seen[event_id] = event_time + self._horizon
        return False

    def _evict(self, now: datetime) -> None:
        expired = [eid for eid, exp in self._seen.items() if exp <= now]
        for eid in expired:
            del self._seen[eid]
        self._evictions += len(expired)

    @property
    def seen_count(self) -> int:
        return len(self._seen)

    @property
    def total_evictions(self) -> int:
        return self._evictions
