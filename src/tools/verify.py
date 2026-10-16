"""
src/tools/verify.py
────────────────────
Post-run verification: compare TimescaleDB stock levels to the ground-truth
event log written by audit_logger (data/truth/audit.jsonl).

For each (warehouse_id, sku) key it replays the audit log in event-time order,
computes the expected final stock, and compares it to the last value in the DB.

Usage (from repo root):
    python -m src.tools.verify
    python -m src.tools.verify --audit data/truth/audit.jsonl --tolerance 0

Exit code 0 = all keys match within tolerance.
Exit code 1 = mismatches found.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg

from config.settings import settings as _settings
from src.common.models import EventType, InventoryEvent, TransferDirection


def load_audit(path: Path) -> list[InventoryEvent]:
    events: list[InventoryEvent] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(InventoryEvent.model_validate_json(line))
            except Exception as exc:
                print(f"  WARN: skipping unparseable line: {exc}", file=sys.stderr)
    events.sort(key=lambda e: e.event_time)
    return events


def compute_expected(events: list[InventoryEvent]) -> dict[tuple[str, str], float]:
    """Replay events in order, return final stock per (warehouse_id, sku)."""
    stock: dict[tuple[str, str], float] = {}
    seen_ids: set[str] = set()
    for ev in events:
        if ev.event_id in seen_ids:
            continue  # deduplicate same as processor
        seen_ids.add(ev.event_id)
        key = (ev.warehouse_id, ev.sku)
        stock[key] = stock.get(key, 0.0) + ev.stock_delta
    return stock


def load_db_stock(conn) -> dict[tuple[str, str], float]:
    """Fetch the latest stock per (warehouse_id, sku) from TimescaleDB."""
    db_stock: dict[tuple[str, str], float] = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (warehouse_id, sku)
                warehouse_id, sku, stock
            FROM stock_levels
            ORDER BY warehouse_id, sku, ts DESC
        """)
        for row in cur.fetchall():
            db_stock[(row[0], row[1])] = float(row[2])
    return db_stock


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--audit",     default="data/truth/audit.jsonl", type=Path)
    ap.add_argument("--tolerance", default=1.0, type=float,
                    help="Allowed absolute difference in stock units (default 1.0)")
    ap.add_argument("--top", default=20, type=int,
                    help="Show at most this many mismatches")
    args = ap.parse_args()

    if not args.audit.exists():
        print(f"ERROR: audit log not found: {args.audit}", file=sys.stderr)
        print("  Run audit_logger while the producer is active to generate it.", file=sys.stderr)
        return 1

    print(f"Loading audit log: {args.audit}")
    events = load_audit(args.audit)
    print(f"  {len(events):,} events, {len({e.event_id for e in events}):,} unique IDs")

    expected = compute_expected(events)
    print(f"  {len(expected):,} (warehouse, sku) keys")

    print(f"Connecting to DB: {_settings.db_host}:{_settings.db_port}/{_settings.db_name}")
    try:
        conn = psycopg.connect(_settings.db_dsn)
    except Exception as exc:
        print(f"ERROR: cannot connect to DB: {exc}", file=sys.stderr)
        return 1

    db_stock = load_db_stock(conn)
    conn.close()
    print(f"  {len(db_stock):,} keys in DB")

    # Compare
    mismatches = []
    missing_in_db = []
    for key, exp in sorted(expected.items()):
        if key not in db_stock:
            missing_in_db.append(key)
            continue
        diff = abs(db_stock[key] - exp)
        if diff > args.tolerance:
            mismatches.append((key, exp, db_stock[key], diff))

    extra_in_db = [k for k in db_stock if k not in expected]

    print(f"\n{'='*60}")
    print(f"Keys matched within ±{args.tolerance}:  "
          f"{len(expected) - len(mismatches) - len(missing_in_db):,} / {len(expected):,}")
    print(f"Mismatches (diff > tolerance): {len(mismatches)}")
    print(f"Keys in audit log missing from DB: {len(missing_in_db)}")
    print(f"Extra keys in DB (not in audit):   {len(extra_in_db)}")
    print(f"{'='*60}\n")

    if mismatches:
        print(f"Top {min(args.top, len(mismatches))} mismatches:")
        print(f"  {'warehouse':<8} {'sku':<12} {'expected':>10} {'db':>10} {'diff':>8}")
        print(f"  {'-'*52}")
        for (wh, sku), exp, db, diff in sorted(mismatches, key=lambda r: -r[3])[:args.top]:
            print(f"  {wh:<8} {sku:<12} {exp:>10.1f} {db:>10.1f} {diff:>8.1f}")

    if missing_in_db:
        shown = missing_in_db[:args.top]
        print(f"\nMissing from DB (first {len(shown)}):")
        for wh, sku in shown:
            print(f"  {wh}:{sku}  expected={expected[(wh,sku)]:.1f}")

    if not mismatches and not missing_in_db:
        print("✅  All keys match — pipeline is correct.")
        return 0
    else:
        print("\n❌  Mismatches found. Check processor logs and DLQ for clues.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
