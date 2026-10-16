# Real-Time Warehouse Inventory Event Analytics

End-to-end streaming pipeline demo built with **Apache Kafka 3.9.1**, **Python 3.11**, **TimescaleDB**, and **Grafana**.

```
Event Sources (UCI replay + simulator)
    → Apache Kafka 3.9.1 (3-broker KRaft cluster, 6 partitions)
    → Python Stream Processor (windows, aggregations, dedup, checkpointing)
    → TimescaleDB (6 hypertables)
    → Grafana (real-time dashboard + alerts)
```

> **Data honesty:** demand events (OUTBOUND, INBOUND) are replayed from the real [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) dataset. Replenishment, transfers, opening balances, and fault injection are simulated.

---

## Quick Start

### 1 — Prerequisites

```powershell
java -version          # JDK 11+
python --version       # 3.11+
docker --version       # optional, for DB/Grafana
```

Set `KAFKA_HOME` to your Kafka 3.9.1 install directory.

### 2 — Install Python dependencies

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3 — Configure environment

```powershell
Copy-Item .env.example .env
# Edit .env — set DB_PASSWORD and DEPLOY_MODE (native or docker)
```

### 4 — Start the database and Grafana

**Docker mode (recommended):**
```powershell
docker compose up -d
```

**Native mode:**
```powershell
.\scripts\db-native-setup.ps1
.\scripts\grafana-native-start.ps1
```

### 5 — Format and start Kafka (one-time format)

```powershell
.\scripts\kafka-format.ps1    # one-time only — creates cluster ID
.\scripts\kafka-start.ps1
.\scripts\create-topics.ps1
```

### 6 — Prepare the dataset

```powershell
# Place online_retail_II.xlsx in data/ then:
python scripts/xlsx_to_csv.py data/online_retail_II.xlsx data/raw/online_retail_ii.csv
python src/tools/eda.py       # generates docs/eda/
```

### 7 — Start consumers (3 terminals)

```powershell
# Terminal 1 — processor instance 1
python -m src.processor.main --instance-id proc-1

# Terminal 2 — processor instance 2
python -m src.processor.main --instance-id proc-2

# Terminal 3 — processor instance 3
python -m src.processor.main --instance-id proc-3
```

### 8 — Start secondary consumers (2 more terminals)

```powershell
# Terminal 4 — audit logger (fan-out demo)
python -m src.consumers.audit_logger

# Terminal 5 — alert listener
python -m src.consumers.alert_listener
```

### 9 — Start the producer

```powershell
# Replay UCI dataset at 600× speed (default)
python -m src.producer.replay --speedup 600

# Or simulator-only (no dataset needed)
python -m src.producer.simulator
```

### 10 — Open Grafana

Visit [http://localhost:3000](http://localhost:3000) — dashboard loads automatically.

---

## Fault-tolerance demos

```powershell
# Kill broker 2 (producer keeps going, quorum survives)
python -m src.tools.chaos broker-kill --broker-id 2

# Inject malformed messages → DLQ
python -m src.tools.chaos inject-bad --count 3

# Inject duplicate event_ids → silently dropped
python -m src.tools.chaos inject-dup --count 5

# Inject a late event (past watermark) → DLQ
python -m src.tools.chaos inject-late --lag-seconds 300

# Inspect the DLQ
python -m src.tools.chaos show-dlq

# DB outage demo
.\scripts\db-stop-start.ps1 stop   # processor checkpoints, retries
.\scripts\db-stop-start.ps1 start  # processor resumes from saved offset
```

---

## Verify correctness

```powershell
python -m src.tools.verify
```

Compares DB stock levels against the ground-truth event log (`data/truth/audit.jsonl`).

---

## Run tests

```powershell
pytest tests/unit -v
```

---

## Repository layout

```
config/          # settings.py + kafka broker configs
src/
  common/        # models (Pydantic), kafka_utils, logging
  producer/      # replay.py, simulator.py, mapping.py, replenishment.py, faults.py
  processor/     # main.py, windows.py, aggregations.py, dedup.py, state.py, sink.py
  consumers/     # audit_logger.py, alert_listener.py
  tools/         # eda.py, verify.py, chaos.py
sql/             # init.sql — all hypertables
grafana/         # provisioned datasource + dashboard
scripts/         # PowerShell scripts for Kafka, DB, Grafana
docs/eda/        # generated charts + dataset_description.md
tests/unit/      # pytest — windows, aggregations, dedup, models
```

---

## Grading checklist

| Requirement | Where |
|---|---|
| Apache Kafka as central broker | 3-broker KRaft cluster, `config/kafka/` |
| At least one producer | `src/producer/replay.py`, `simulator.py` |
| At least one consumer | `src/processor/main.py` (3 instances) |
| At least two topics | `inventory-events`, `inventory-alerts`, `inventory-dlq` |
| Partitioning + consumer groups + rebalancing | key=`wh:sku`, group `inventory-processor`, `audit-logger`, `alert-listener` |
| Window-based operation | Tumbling (60 s) + Sliding (300 s / 60 s slide) in `windows.py` |
| Aggregation / summarisation | Running stock, EMA, top-K, days-of-cover in `aggregations.py` |
| Fault tolerance + recovery | Idempotent producer, dedup, DLQ, checkpoint/restore, replication=3 |
| Time-series storage | TimescaleDB hypertables in `sql/init.sql` |
| Real-time visualisation + alerting | Grafana dashboard with 11 panels, alert rows in `alerts` table |

---

## Attribution

Dataset: Chen, D. (2019). *Online Retail II* [Dataset]. UCI Machine Learning Repository. https://archive.ics.uci.edu/dataset/502/online+retail+ii — CC BY 4.0.
