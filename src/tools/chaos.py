"""
src/tools/chaos.py
───────────────────
Helpers for the fault-tolerance demo scenarios (AGENTS.md demo-script).

Sub-commands
────────────
  broker-kill   Kill one Kafka broker process (simulates broker failure).
  inject-bad    Produce a malformed message → should land in DLQ.
  inject-dup    Produce duplicate event_ids → should be silently deduplicated.
  inject-late   Produce an event with an event_time far in the past → DLQ.
  show-dlq      Pretty-print recent messages from inventory-dlq.
  show-alerts   Pretty-print recent messages from inventory-alerts.

Usage:
    python -m src.tools.chaos broker-kill --broker-id 2
    python -m src.tools.chaos inject-bad
    python -m src.tools.chaos inject-dup --count 5
    python -m src.tools.chaos inject-late --lag-seconds 3600
    python -m src.tools.chaos show-dlq --count 10
    python -m src.tools.chaos show-alerts --count 20
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition

from config.settings import settings as _settings
from src.common.kafka_utils import make_producer
from src.common.models import EventSource, EventType, InventoryEvent


# ─────────────────────────────────────────────────────────────────────────────
# broker-kill
# ─────────────────────────────────────────────────────────────────────────────

def cmd_broker_kill(args: argparse.Namespace) -> None:
    """Kill broker process by reading its PID file."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    pid_file = repo_root / f"broker-{args.broker_id}.pid"
    if not pid_file.exists():
        print(f"ERROR: PID file not found: {pid_file}", file=sys.stderr)
        print("  Is the broker running? Start it with scripts/kafka-start.ps1", file=sys.stderr)
        sys.exit(1)

    pid = int(pid_file.read_text().strip())
    print(f"Killing broker {args.broker_id} (PID {pid}) ...")
    try:
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/PID", str(pid)], check=True)
        else:
            os.kill(pid, signal.SIGKILL)
        pid_file.unlink(missing_ok=True)
        print(f"  ✅  Broker {args.broker_id} killed. "
              f"The other 2 brokers now form the quorum.")
        print(f"  Watch producer logs — it should keep producing after a brief retry.")
        print(f"  To restart: scripts/kafka-start.ps1 -BrokerId {args.broker_id}")
    except ProcessLookupError:
        print(f"  WARN: PID {pid} not found — broker may already be down.")
        pid_file.unlink(missing_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# inject-bad
# ─────────────────────────────────────────────────────────────────────────────

def cmd_inject_bad(args: argparse.Namespace) -> None:
    """Send a malformed JSON message to inventory-events."""
    producer = make_producer()
    bad_payloads = [
        b'{"event_id": "not-a-uuid", "missing_required_fields": true}',
        b"definitely not json {{{",
        b'{"event_type": "UNKNOWN_TYPE", "quantity": -999}',
    ]
    count = min(args.count, len(bad_payloads))
    for i in range(count):
        key = f"WH-01:BAD{i:02d}".encode()
        producer.produce(
            topic=_settings.topic_events,
            key=key,
            value=bad_payloads[i % len(bad_payloads)],
        )
        print(f"  Injected bad message {i+1}/{count}")
    producer.flush()
    print(f"✅  {count} malformed message(s) sent → expect them in inventory-dlq")
    print(f"  Verify: python -m src.tools.chaos show-dlq")


# ─────────────────────────────────────────────────────────────────────────────
# inject-dup
# ─────────────────────────────────────────────────────────────────────────────

def cmd_inject_dup(args: argparse.Namespace) -> None:
    """Send the same event_id multiple times to test deduplication."""
    producer = make_producer()
    fixed_id = str(uuid.uuid4())
    ev = InventoryEvent(
        event_id=fixed_id,
        event_time=datetime.now(timezone.utc),
        warehouse_id="WH-01",
        sku="85123A",
        event_type=EventType.OUTBOUND,
        quantity=1,
        zone="Z1",
        source=EventSource.SIMULATOR,
    )
    payload = ev.to_kafka_value()
    key = ev.kafka_key.encode()

    for i in range(args.count):
        producer.produce(topic=_settings.topic_events, key=key, value=payload)
        print(f"  Sent duplicate {i+1}/{args.count}  event_id={fixed_id[:8]}…")
        time.sleep(0.05)

    producer.flush()
    print(f"✅  {args.count} duplicate events sent with the same event_id.")
    print(f"  The processor should process exactly 1 and drop {args.count - 1}.")
    print(f"  Watch processor logs for 'duplicate' debug messages.")


# ─────────────────────────────────────────────────────────────────────────────
# inject-late
# ─────────────────────────────────────────────────────────────────────────────

def cmd_inject_late(args: argparse.Namespace) -> None:
    """Send an event with event_time well behind the current watermark."""
    producer = make_producer()
    late_time = datetime.now(timezone.utc) - timedelta(seconds=args.lag_seconds)
    ev = InventoryEvent(
        event_id=str(uuid.uuid4()),
        event_time=late_time,
        warehouse_id="WH-02",
        sku="85123A",
        event_type=EventType.OUTBOUND,
        quantity=5,
        zone="Z2",
        source=EventSource.SIMULATOR,
    )
    producer.produce(
        topic=_settings.topic_events,
        key=ev.kafka_key.encode(),
        value=ev.to_kafka_value(),
    )
    producer.flush()
    print(f"✅  Late event sent  event_time={late_time.isoformat()}")
    print(f"  lag={args.lag_seconds}s behind now, watermark_seconds={_settings.watermark_seconds}s")
    if args.lag_seconds > _settings.watermark_seconds:
        print(f"  → This WILL be dropped to DLQ (lag > watermark).")
    else:
        print(f"  → This may still be accepted (lag ≤ watermark).")
    print(f"  Verify: python -m src.tools.chaos show-dlq")


# ─────────────────────────────────────────────────────────────────────────────
# show-dlq
# ─────────────────────────────────────────────────────────────────────────────

def cmd_show_dlq(args: argparse.Namespace) -> None:
    """Read and pretty-print the most recent DLQ messages."""
    _consume_and_print(
        topic=_settings.topic_dlq,
        group_id=f"chaos-dlq-reader-{uuid.uuid4().hex[:6]}",
        count=args.count,
        label="DLQ",
    )


# ─────────────────────────────────────────────────────────────────────────────
# show-alerts
# ─────────────────────────────────────────────────────────────────────────────

def cmd_show_alerts(args: argparse.Namespace) -> None:
    """Read and pretty-print recent alert messages."""
    _consume_and_print(
        topic=_settings.topic_alerts,
        group_id=f"chaos-alert-reader-{uuid.uuid4().hex[:6]}",
        count=args.count,
        label="ALERT",
    )


def _consume_and_print(topic: str, group_id: str, count: int, label: str) -> None:
    consumer = Consumer({
        "bootstrap.servers": _settings.kafka_bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    print(f"Reading up to {count} messages from {topic} …\n")

    seen = 0
    deadline = time.monotonic() + 15  # 15 s timeout
    while seen < count and time.monotonic() < deadline:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            continue
        if msg.error():
            continue
        seen += 1
        print(f"── {label} #{seen}  partition={msg.partition()} offset={msg.offset()} ──")
        try:
            data = json.loads(msg.value())
            print(json.dumps(data, indent=2, default=str))
        except Exception:
            print(msg.value()[:300])
        print()

    consumer.close()
    if seen == 0:
        print(f"(no messages found in {topic})")
    else:
        print(f"─── {seen} message(s) shown ───")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    bk = sub.add_parser("broker-kill", help="Kill a broker by ID")
    bk.add_argument("--broker-id", type=int, default=2, choices=[1, 2, 3])

    ib = sub.add_parser("inject-bad", help="Send malformed messages to events topic")
    ib.add_argument("--count", type=int, default=1)

    id_ = sub.add_parser("inject-dup", help="Send duplicate event_ids")
    id_.add_argument("--count", type=int, default=3)

    il = sub.add_parser("inject-late", help="Send a late event")
    il.add_argument("--lag-seconds", type=int, default=300)

    sd = sub.add_parser("show-dlq", help="Print recent DLQ messages")
    sd.add_argument("--count", type=int, default=10)

    sa = sub.add_parser("show-alerts", help="Print recent alert messages")
    sa.add_argument("--count", type=int, default=20)

    return p


if __name__ == "__main__":
    parser = build_parser()
    ns = parser.parse_args()
    dispatch = {
        "broker-kill":  cmd_broker_kill,
        "inject-bad":   cmd_inject_bad,
        "inject-dup":   cmd_inject_dup,
        "inject-late":  cmd_inject_late,
        "show-dlq":     cmd_show_dlq,
        "show-alerts":  cmd_show_alerts,
    }
    dispatch[ns.cmd](ns)
