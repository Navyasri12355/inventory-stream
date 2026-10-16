# Demo Script — Real-Time Warehouse Inventory Event Analytics

**Duration:** ~15 minutes for a full run, or follow the ⚡ Fast sections for a 5-minute version.

---

## 0. Before you start (setup, do once)

```powershell
# 1. Activate venv
.\venv\Scripts\Activate.ps1

# 2. Check everything is running
.\scripts\check-env.ps1

# 3. Start Kafka (if not running)
.\scripts\kafka-start.ps1
.\scripts\create-topics.ps1   # idempotent

# 4. Start DB + Grafana
docker compose up -d           # docker mode
# OR
.\scripts\db-native-setup.ps1
.\scripts\grafana-native-start.ps1

# 5. Open Grafana in browser: http://localhost:3000
#    Dashboard "Inventory Pipeline" should load automatically.
```

---

## 1. Show the EDA (2 min)

> *"Before building the pipeline, I analysed the dataset."*

Open [`docs/eda/dataset_description.md`](../docs/eda/dataset_description.md).

Key charts to show:
- **01_cleaning_funnel.png** → "1.07M rows cleaned down to ~475K events"
- **02_event_mix.png** → "96% sales, 4% returns — this is the real demand signal"
- **05_sku_skew.png** → "Power-law: top 500 SKUs hold ~49% of events — we keep those"
- **08_warehouse_partition_estimate.png** → "CRC32 hash on CustomerID → 3 balanced warehouses, 6 Kafka partitions"

---

## 2. Show the Kafka cluster (1 min)

> *"3 brokers, KRaft mode — no ZooKeeper."*

```powershell
# List topics and their partition/replica config
$env:KAFKA_HOME\bin\windows\kafka-topics.bat `
  --bootstrap-server localhost:9092 `
  --describe `
  --topic inventory-events
```

Point out: **6 partitions, replication-factor 3, min.insync.replicas 2**.

---

## 3. Start consumers (open 5 terminals) ⚡

```powershell
# Terminal A — processor 1
python -m src.processor.main --instance-id proc-1

# Terminal B — processor 2
python -m src.processor.main --instance-id proc-2

# Terminal C — processor 3
python -m src.processor.main --instance-id proc-3

# Terminal D — audit logger (fan-out / second group)
python -m src.consumers.audit_logger

# Terminal E — alert listener (third group)
python -m src.consumers.alert_listener
```

Show the **rebalance logs** in Terminals A-C: each instance gets 2 of the 6 partitions.

---

## 4. Start the producer ⚡

```powershell
# Terminal F — replay UCI data at 600× speed
python -m src.producer.replay --speedup 600 --top-skus 500
```

Grafana should start filling within 10–15 seconds:
- **Stock Level Over Time** — lines diverging per SKU
- **Tumbling Window** — inbound/outbound bars
- **Events/sec per Processor** — 3 separate lines

---

## 5. Show windowing live (2 min)

> *"Two window types running simultaneously."*

In Grafana, point to:
- **Tumbling Window panel** — fixed 60-second windows, no overlap
- **Sliding Window panel** — 300-second window sliding every 60 seconds, smoother curve

Show in DB directly:

```sql
SELECT window_type, COUNT(*) as windows, SUM(outbound) as total_out
FROM window_metrics
GROUP BY window_type;
```

---

## 6. Fault demo 1 — DLQ (1 min) ⚡

> *"Malformed messages are routed to the Dead Letter Queue, not crashing the processor."*

```powershell
python -m src.tools.chaos inject-bad --count 3
python -m src.tools.chaos show-dlq
```

Show the processor logs — it logs `malformed message` and keeps running.
Show DLQ output — original payload + error headers preserved.

---

## 7. Fault demo 2 — Duplicate deduplication (1 min)

> *"Same event_id arriving multiple times is silently dropped."*

```powershell
python -m src.tools.chaos inject-dup --count 5
```

Watch processor Terminal A/B/C — you should see `duplicate` log lines at DEBUG level.
Stock level in DB does not change (only one event counted).

---

## 8. Fault demo 3 — Late events (1 min)

> *"Events arriving past the watermark go to DLQ, not corrupting the windows."*

```powershell
python -m src.tools.chaos inject-late --lag-seconds 300
python -m src.tools.chaos show-dlq --count 3
```

Watermark is 120 s, lag is 300 s → late event goes to DLQ.

---

## 9. Fault demo 4 — Broker failure (2 min)

> *"One broker dies; the cluster keeps running with the remaining 2."*

```powershell
python -m src.tools.chaos broker-kill --broker-id 2
```

Watch the **producer terminal** — it logs a brief `Leader not available` then recovers.
Watch Grafana — events keep flowing (minor blip at most).

```powershell
# Restart the killed broker to show recovery
.\scripts\kafka-start.ps1 -BrokerId 2
```

---

## 10. Consumer group rebalancing (1 min)

> *"Adding a fourth processor instance triggers a rebalance — partitions redistribute automatically."*

```powershell
# Terminal G — fourth processor instance
python -m src.processor.main --instance-id proc-4
```

Watch Terminals A-C: each logs `partitions revoked` then `partitions assigned` with the new split.

Kill Terminal G (Ctrl+C) → another rebalance back to 3 instances.

---

## 11. Show alerts (1 min)

Open the **Alerts** panel in Grafana.
Watch Terminal E (alert listener) for coloured emoji output.

```sql
-- Check alert counts by type
SELECT alert_type, severity, COUNT(*) FROM alerts GROUP BY 1, 2;
```

---

## 12. Verify correctness ⚡

> *"The audit log is ground truth. Let's check the DB matches it."*

```powershell
python -m src.tools.verify
```

Expected output: `✅  All keys match — pipeline is correct.`

---

## 13. Run unit tests

```powershell
pytest tests/unit -v
```

4 test files, all green: windows, aggregations, dedup, models.

---

## Things to mention / explain

| Topic | What to say |
|---|---|
| **Why KRaft?** | No ZooKeeper dependency — simpler, production direction for Kafka |
| **Why 3 brokers?** | Replication factor 3, min.insync.replicas 2 → can lose 1 broker and still write |
| **Why partition by `wh:sku`?** | Same key → same partition → stateful aggregation without cross-partition coordination |
| **Why TimescaleDB?** | PostgreSQL compatibility + automatic time-based partitioning (chunks) + compression |
| **Why Pydantic?** | Schema enforcement at producer and consumer — bad events caught at the edge |
| **Tumbling vs Sliding?** | Tumbling = non-overlapping snapshots (billing period style); Sliding = rolling average (trend detection) |
| **EMA vs SMA?** | EMA weights recent demand more — better for lead-time and reorder-point calculation |
| **Watermark?** | Allows late events up to `WATERMARK_SECONDS` (120 s by default); events later than that go to DLQ rather than silently corrupting window state |
