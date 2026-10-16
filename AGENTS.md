# AGENTS.md

Guidance for AI coding agents (and humans) building this project from an empty repository.

## 1. Project summary

**Real-Time Warehouse Inventory Event Analytics** is an end-to-end streaming pipeline:

```
Event Sources (UCI replay + simulator) -> Apache Kafka 3.9.1 -> Stream Processor -> TimescaleDB -> Grafana (dashboard + alerts)
```

The event source is a **hybrid**: real retail demand, returns and stock write-offs replayed from the UCI _Online Retail II_ dataset, plus simulated replenishment, opening balances, transfers, and injected faults (section 5.5). Be honest about this split everywhere (README, charts, slides).

It exists to demonstrate, in one reproducible repo, all of the following (this is the grading checklist; do not drop any item):

- [ ] Apache Kafka is the central messaging platform
- [ ] At least one producer and one consumer
- [ ] At least two topics (we use three: events, alerts, DLQ)
- [ ] Partitioning (keyed by SKU) and consumer groups (with rebalancing demo)
- [ ] At least one window-based operation (tumbling + sliding)
- [ ] At least one aggregation/summarisation algorithm (running stock, EMA, top-K, days-of-cover)
- [ ] Fault tolerance and recovery (replication, idempotent producer, offset handling, checkpoint, DLQ)
- [ ] Results stored in a time-series store (TimescaleDB)
- [ ] Real-time visualisation and alerting (Grafana)

## 2. Environment and fixed technology decisions

The developer already has **Apache Kafka 3.9.1** and **Java** installed locally. Do not substitute anything below without asking the human first.

| Concern            | Decision                                                                                                                                                                                                                                                                      |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Event source       | **Hybrid**: replay of UCI _Online Retail II_ (real: sales, returns, and stock write-offs as `ADJUSTMENT` / `DAMAGE`) plus a simulator (replenishment, opening balances, transfers, fault injection). Details in section 5.5                                                   |
| Broker             | **Locally installed Apache Kafka 3.9.1**, **KRaft mode** (no ZooKeeper). Location comes from the `KAFKA_HOME` env var                                                                                                                                                         |
| Cluster shape      | **3 brokers on one machine** (combined broker+controller nodes), each with its own config file, port, and log dir                                                                                                                                                             |
| Java               | Needed **only to run Kafka itself**. No Java application code in this project                                                                                                                                                                                                 |
| Language           | Python 3.11+                                                                                                                                                                                                                                                                  |
| Kafka client       | `confluent-kafka` (a client library only; it talks to plain Apache Kafka)                                                                                                                                                                                                     |
| Stream processing  | **Custom Python processor** on `confluent-kafka` (explicit windowing, state, checkpointing). PySpark Structured Streaming is the only approved alternative                                                                                                                    |
| Serialisation      | JSON validated with `pydantic`. No Schema Registry (Confluent product, not Apache Kafka)                                                                                                                                                                                      |
| Storage            | TimescaleDB (PostgreSQL + extension) via `psycopg[binary]`. Run **natively or in Docker** (section 2.2)                                                                                                                                                                       |
| Dashboard / alerts | Grafana, provisioned from files (no manual clicking). Run **natively or in Docker** (section 2.2)                                                                                                                                                                             |
| Docker             | **Optional**, supported as an alternative way to run TimescaleDB and Grafana. Kafka itself is NOT run in Docker, and no Kafka UI container is used (the brokers advertise `localhost`, which a container could not reach). Use the Kafka CLI tools and Grafana for visibility |
| React dashboard    | **Out of scope** unless the human asks                                                                                                                                                                                                                                        |

### 2.1 First step for the agent: detect the OS and tooling

Before generating any scripts, check the operating system and confirm with the human.

- **Linux / macOS**: provide a `Makefile` and `scripts/*.sh`, using `$KAFKA_HOME/bin/*.sh`.
- **Windows**: strongly recommend running everything inside **WSL2**. Native Windows Kafka is known to crash on log-segment deletion due to file locking, which would sabotage the recovery demos. If the human insists on native Windows, provide PowerShell scripts (`scripts/*.ps1`) using `%KAFKA_HOME%\bin\windows\*.bat`.
- Verify prerequisites and report versions: `java -version`, `kafka-topics.sh --version` (expect 3.9.1), `python --version`.
- Detect what is available for the database and dashboard: `psql --version` and whether the `timescaledb` extension is installed, `grafana-server --version` (or `grafana server --version`), and `docker --version` / `docker compose version`.
- Recommend a `DEPLOY_MODE` (section 2.2): `native` if PostgreSQL with TimescaleDB and Grafana are already installed, otherwise `docker`. Confirm with the human and record it in `.env`.
- Check for the dataset: if `data/raw/online_retail_ii.csv` is missing but the original two-sheet `data/online_retail_II.xlsx` exists, run `python scripts/xlsx_to_csv.py data/online_retail_II.xlsx data/raw/online_retail_ii.csv` from the repo root. If neither exists, tell the human to download _Online Retail II_ (UCI dataset id 502) and wait; do not fabricate data.

### 2.2 TimescaleDB and Grafana: native or Docker (both must work)

Support **both** deployment modes from the start. Application code must be mode-agnostic: only environment variables differ. Selection is `DEPLOY_MODE=native|docker` in `.env`, and `make db-up` / `make db-down` dispatch on it.

**Shared environment variables** (defined in `.env.example`):

| Variable                              | Default                                                                           | Used by                                                            |
| ------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `DB_HOST`                             | `localhost`                                                                       | processor, tools (always reaches the DB from the host)             |
| `DB_PORT`                             | `5432`                                                                            | processor, tools; Docker publishes the container on this host port |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `inventory` / `inventory` / dev value                                             | everything                                                         |
| `GRAFANA_PORT`                        | `3000`                                                                            | Grafana (either mode)                                              |
| `TSDB_HOST` / `TSDB_PORT`             | `localhost` / `DB_PORT` (native); `timescaledb` / `5432` (Docker, set in compose) | Grafana datasource provisioning only                               |
| `GRAFANA_DASHBOARD_PATH`              | repo `grafana/dashboards` (native); `/var/lib/grafana/dashboards` (Docker)        | Grafana dashboard provisioning only                                |

Grafana provisioning files must reference these with `${VAR}` environment expansion (for example `url: ${TSDB_HOST}:${TSDB_PORT}`), so **one** set of provisioning files serves both modes.

**Native mode**

- PostgreSQL (a version supported by TimescaleDB) with the TimescaleDB extension installed, and `shared_preload_libraries = 'timescaledb'` set in `postgresql.conf` followed by a restart.
- `scripts/db-native-setup.sh` (idempotent): create the role and database, `CREATE EXTENSION IF NOT EXISTS timescaledb`, then apply `sql/init.sql`.
- Grafana OSS installed. `scripts/grafana-native-start.sh` starts it with environment overrides: `GF_PATHS_PROVISIONING=<repo>/grafana/provisioning`, `GF_PATHS_DATA=<repo>/grafana-data`, `GF_SERVER_HTTP_PORT=$GRAFANA_PORT`, plus `TSDB_HOST`, `TSDB_PORT` and `GRAFANA_DASHBOARD_PATH`. This keeps the repo's config separate from any system-wide Grafana.
- If a system Grafana service already occupies port 3000, either stop it or set `GRAFANA_PORT=3001`.

**Docker mode**

- `docker-compose.yml` defines `timescaledb` and `grafana` only, with pinned image tags (no `latest`), named volumes, health checks, `restart: unless-stopped`, and published ports `${DB_PORT}:5432` and `${GRAFANA_PORT}:3000`.
- `sql/init.sql` is mounted into `/docker-entrypoint-initdb.d/`. Grafana provisioning and dashboards are mounted read-only, and compose sets `TSDB_HOST=timescaledb`, `TSDB_PORT=5432`, `GRAFANA_DASHBOARD_PATH=/var/lib/grafana/dashboards`.
- If a native PostgreSQL already uses 5432, set `DB_PORT=5433` so the container's published port does not clash.
- Kafka stays on the host. The processor and producer also run on the host and reach the DB through the published port.

**Both modes must pass the same checks:** `make db-check` (connects, confirms the `timescaledb` extension and that all hypertables exist via `timescaledb_information.hypertables`) and `make verify`.

**Docs:** provide `docs/setup-native.md` and `docs/setup-docker.md` with install steps, start/stop commands, and troubleshooting. The README quick start shows both paths side by side.

## 3. Repository layout (create this)

```
.
├── AGENTS.md
├── README.md                  # how to run + demo script + screenshots
├── docker-compose.yml         # Docker mode: timescaledb + grafana ONLY
├── .env.example
├── .gitignore
├── venv/                      # local Python virtualenv (gitignored)
├── pyproject.toml / requirements.txt
├── Makefile                   # (or scripts/*.ps1 on native Windows)
├── config/
│   ├── settings.py            # all app config via env vars with defaults
│   └── kafka/
│       ├── server-1.properties
│       ├── server-2.properties
│       └── server-3.properties
├── scripts/
│   ├── xlsx_to_csv.py         # data/online_retail_II.xlsx (2 sheets) -> data/raw/online_retail_ii.csv
│   ├── kafka-format.sh        # one-time: generate cluster id, format 3 log dirs
│   ├── kafka-start.sh         # start brokers 1-3 in background, write PID files
│   ├── kafka-stop.sh
│   ├── kafka-kill.sh          # kill -9 a single broker (for failure demo)
│   ├── create-topics.sh       # idempotent topic creation
│   ├── db-native-setup.sh     # native mode: role, database, extension, init.sql
│   ├── db-init.sh             # apply sql/init.sql via DATABASE_URL (either mode)
│   ├── db-stop-start.sh       # stop/start the DB in either mode (for outage demo)
│   └── grafana-native-start.sh  # native mode: run Grafana with repo provisioning
├── src/
│   ├── common/
│   │   ├── models.py          # pydantic InventoryEvent, Alert
│   │   ├── kafka_utils.py     # producer/consumer factories, delivery callbacks
│   │   └── logging_setup.py
│   ├── producer/
│   │   ├── replay.py          # UCI replay producer (--speedup, --top-skus, ...)
│   │   ├── mapping.py         # UCI row -> InventoryEvent, cleaning rules, warehouse hashing
│   │   ├── replenishment.py   # opening balances, reorder points, transfers
│   │   ├── faults.py          # duplicate / late / malformed injection (shared)
│   │   └── simulator.py       # simulator-only mode (no dataset needed)
│   ├── processor/
│   │   ├── main.py            # consume loop, rebalance callbacks
│   │   ├── windows.py         # tumbling + sliding windows, watermark
│   │   ├── aggregations.py    # running stock, EMA, top-K, days-of-cover
│   │   ├── dedup.py           # event_id dedup within watermark horizon
│   │   ├── state.py           # keyed state + checkpoint/restore
│   │   └── sink.py            # idempotent TimescaleDB upserts
│   ├── consumers/
│   │   ├── audit_logger.py    # second consumer group on inventory-events
│   │   └── alert_listener.py  # reads inventory-alerts
│   └── tools/
│       ├── eda.py             # EDA charts + tables (from CSV, event log, or TimescaleDB)
│       ├── verify.py          # compare DB to ground truth (section 11)
│       └── chaos.py           # helpers for failure demos
├── data/                      # all gitignored
│   ├── online_retail_II.xlsx  # original UCI download (two sheets)
│   ├── raw/online_retail_ii.csv   # cleaned, merged CSV (output of scripts/xlsx_to_csv.py)
│   └── truth/                 # emitted-event log + ground truth
├── sql/
│   └── init.sql               # hypertables, indexes, continuous aggregates
├── grafana/
│   ├── provisioning/datasources/
│   ├── provisioning/dashboards/
│   └── dashboards/inventory.json
├── tests/
│   ├── unit/                  # windows, aggregations, dedup, models
│   └── integration/           # needs Kafka + DB running
└── docs/
    ├── architecture.md
    ├── eda/                   # charts + tables generated by tools/eda.py
    ├── setup-native.md
    ├── setup-docker.md
    └── demo-script.md
```

Make sure `.gitignore` contains `venv/`, `kafka-data/`, `grafana-data/`, `data/`, `.env`, and `*.pid`.

## 4. Local Kafka cluster setup (3 brokers, KRaft, v3.9.1)

Each broker is a combined `broker,controller` node. Per-broker values:

| Setting                  | Broker 1                | Broker 2                | Broker 3                |
| ------------------------ | ----------------------- | ----------------------- | ----------------------- |
| `node.id`                | 1                       | 2                       | 3                       |
| client listener port     | 9092                    | 9094                    | 9096                    |
| controller listener port | 9093                    | 9095                    | 9097                    |
| `log.dirs`               | `./kafka-data/broker-1` | `./kafka-data/broker-2` | `./kafka-data/broker-3` |

Settings common to all three:

```
process.roles=broker,controller
controller.quorum.voters=1@localhost:9093,2@localhost:9095,3@localhost:9097
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://localhost:<client_port>,CONTROLLER://localhost:<controller_port>
advertised.listeners=PLAINTEXT://localhost:<client_port>
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
num.partitions=6
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
unclean.leader.election.enable=false
auto.create.topics.enable=false
```

One-time format (same cluster ID for all three):

```
CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
for i in 1 2 3; do
  $KAFKA_HOME/bin/kafka-storage.sh format -t $CLUSTER_ID -c config/kafka/server-$i.properties
done
```

Applications connect with `bootstrap.servers=localhost:9092,localhost:9094,localhost:9096`.

**Low-resource fallback:** if the machine cannot handle 3 brokers, run a single broker with replication factor 1 and `min.insync.replicas=1`. The broker-failure demo then cannot be shown; document that clearly.

## 5. Contracts

### 5.1 Event schema (`inventory-events`)

```json
{
  "event_id": "uuid string (uuid5 for replayed rows, uuid4 for simulated events)",
  "event_time": "ISO-8601 UTC (rebased to replay time by default, see 5.5)",
  "warehouse_id": "WH-01",
  "sku": "85123A",
  "event_type": "INBOUND | OUTBOUND | TRANSFER | ADJUSTMENT | DAMAGE",
  "quantity": 24,
  "zone": "Z3",
  "source": "UCI_REPLAY | SIMULATOR",
  "original_time": "ISO-8601, the dataset's original timestamp (UCI_REPLAY only)",
  "direction": "OUT | IN (TRANSFER only)",
  "transfer_id": "uuid (TRANSFER only)"
}
```

Rules:

- `quantity` is a positive integer, except `ADJUSTMENT`, which may be signed.
- Stock effect: `INBOUND +q`, `OUTBOUND -q`, `DAMAGE -q`, `ADJUSTMENT +/-q`. `TRANSFER` is emitted as **two events** sharing a `transfer_id`: one with `direction=OUT` (stock -q, keyed by the source warehouse) and one with `direction=IN` (stock +q, keyed by the destination warehouse). Two events are required because each stock balance lives on the partition that owns its key, and one event cannot update state on two partitions.
- **Message key = `"{warehouse_id}:{sku}"`**. Never publish without a key.

### 5.2 Topics

| Topic              | Partitions | Replication | Notes                                                                                         |
| ------------------ | ---------- | ----------- | --------------------------------------------------------------------------------------------- |
| `inventory-events` | 6          | 3           | raw events                                                                                    |
| `inventory-alerts` | 3          | 3           | key = sku                                                                                     |
| `inventory-dlq`    | 1          | 3           | original payload plus `error`, `source_topic`, `source_partition`, `source_offset` in headers |

`create-topics.sh` must be idempotent (`--if-not-exists`) and set `min.insync.replicas=2`.

### 5.3 Consumer groups

- `inventory-processor`: 3 instances, shares the 6 partitions
- `audit-logger`: 1 instance, independent group on the same topic (shows fan-out)
- `alert-listener`: reads `inventory-alerts`

### 5.4 Database tables (TimescaleDB hypertables)

- `stock_levels(ts, warehouse_id, sku, stock, ingest_ts)` PK `(ts, warehouse_id, sku)`; `ts` is event time
- `window_metrics(window_start, window_end, window_type, warehouse_id, sku, inbound, outbound, avg_demand, ingest_ts)` PK `(window_start, window_type, warehouse_id, sku)`
- `alerts(ts, warehouse_id, sku, alert_type, severity, message, ingest_ts)`
- `pipeline_metrics(ts, instance_id, partition, consumer_lag, events_per_sec, late_events, dlq_count)` so Grafana can show lag and health without extra tooling
- `consumer_offsets(group_id, topic, partition, committed_offset)` for atomic offset + result commits
- `sku_catalog(sku, description)` loaded once from the dataset so Grafana can show readable product names

All writes use `INSERT ... ON CONFLICT DO UPDATE` so replays never create duplicates.

**Time columns.** `ts` / `window_start` are **event time**. `ingest_ts timestamptz NOT NULL DEFAULT now()` is the wall-clock write time, refreshed on every upsert; index it. Hypertables are partitioned on the event-time column (chunk interval 1 day).

- **Default (`--time-mode rebased`, section 5.5):** event time tracks the wall clock, so Grafana's "last 5 minutes" works directly on `ts` / `window_start`. Use `ingest_ts` for latency measurement. Retention and continuous-aggregate policies relative to `now()` are fine, but keep retention well above the length of a demo.
- **`--time-mode original` (historical timestamps, 2009 to 2011):** live panels must use `ingest_ts` as their time column, and panels showing the replayed history use a fixed dashboard range. Do **not** create retention, compression, or continuous-aggregate refresh policies relative to `now()` in this mode: they would drop or ignore all data immediately.

### 5.5 Data source: hybrid replay (UCI + simulator)

**Real part: UCI Online Retail II.** Transaction lines from a UK online retailer, Dec 2009 to Dec 2011 (about 1.07M raw rows; always report the count from your own run). License is CC BY 4.0, so attribution is required: _Chen, D. (2019). Online Retail II [Dataset]. UCI Machine Learning Repository. https://archive.ics.uci.edu/dataset/502/online+retail+ii_. It supplies sales, returns, and recorded **stock write-offs** (negative-quantity, zero-price lines described as damaged, check, missing and similar). It contains no restocking, transfers, or stock levels.

**Simulated part:** opening balances, `INBOUND` replenishment, `TRANSFER`, and all fault injection. Random `ADJUSTMENT` / `DAMAGE` events are optional extras (off by default in replay mode).

**Input.** `data/raw/online_retail_ii.csv`, produced by `scripts/xlsx_to_csv.py` from `data/online_retail_II.xlsx` (both sheets merged, exact duplicates dropped, sorted by `InvoiceDate`, `Invoice` and `StockCode` kept as text). Columns: `Invoice, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country`.

**Cleaning rules** (in `mapping.py`; log how many rows each rule removes at startup):

1. Strip and upper-case `StockCode`, then keep only product rows: it matches `^\d{5}[A-Za-z]*$`. This removes non-product codes (postage, fees, manual entries, gift vouchers) and merges case variants such as `72349b` / `72349B`. Inspect the excluded codes in EDA before trusting the rule.
2. Drop rows with `Quantity == 0` or `UnitPrice < 0` (accounting entries). Rows with `UnitPrice == 0` are kept for now and handled by rule 4.
3. Keep rows with a missing `CustomerID`.
4. Keep only rows that fit one of the four mappings below (sale, return, damage write-off, adjustment write-off). Drop everything else and count it, for example zero-price rows with a positive quantity, or a positive quantity on a cancellation invoice.
5. Keep only the top `--top-skus` SKUs by remaining row count (default 500), so each SKU has enough events for windows to be meaningful. In the current data the top 500 SKUs hold about 49% of events and the write-off rows are concentrated in the long tail (see the note after the mapping table).

**Damage keywords** (matched case-insensitively as substrings of `Description`): `damage`, `smash`, `broken`, `destroy`, `thrown`, `unsaleable`, `faulty`, `crushed`, `wet`, `mouldy`, `rust`. Keep this list in one constant in `mapping.py`; `tools/eda.py` uses the same list.

**Field mapping**

| UCI field                                                                                                    | Event field                                                    | Rule                                                                                                                                                                                                   |
| ------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `InvoiceDate`                                                                                                | `original_time` (always) and `event_time`                      | `original_time` is the naive timestamp interpreted as UTC (document this assumption). `event_time` is **rebased** by default (see Replay clock); with `--time-mode original` it equals `original_time` |
| `StockCode`                                                                                                  | `sku`                                                          | Stripped and upper-cased (rule 1)                                                                                                                                                                      |
| `Quantity` > 0 and `UnitPrice` > 0 on an invoice not starting with `C`                                       | `event_type=OUTBOUND`, `quantity=Quantity`                     | A sale                                                                                                                                                                                                 |
| Invoice starting with `C` with `Quantity` < 0 and `UnitPrice` > 0                                            | `event_type=INBOUND`, `quantity=abs(Quantity)`, `zone=RETURNS` | A customer return back into stock (documented assumption)                                                                                                                                              |
| `Quantity` < 0 on an invoice not starting with `C`, `UnitPrice == 0`, `Description` matches a damage keyword | `event_type=DAMAGE`, `quantity=abs(Quantity)`                  | A recorded write-off                                                                                                                                                                                   |
| Same, any other `Description` (including none)                                                               | `event_type=ADJUSTMENT`, `quantity=Quantity` (negative)        | A stock-count correction (assumption: rows without a description are treated as adjustments)                                                                                                           |
| `CustomerID`, or `Invoice` if missing                                                                        | `warehouse_id`                                                 | Hashing rule below                                                                                                                                                                                     |
| `Invoice` + row number in `online_retail_ii.csv`                                                             | `event_id`                                                     | `uuid5(NAMESPACE, f"{Invoice}\|{row_number}")`: stable across runs and filters, needed for dedup, idempotent sinks, and ground truth                                                                   |
| `StockCode`                                                                                                  | `zone`                                                         | `Z{crc32(sku) % 8}`, stable per SKU (`RETURNS` for returns); write-offs use the SKU zone                                                                                                               |
| none                                                                                                         | `source`                                                       | `UCI_REPLAY`                                                                                                                                                                                           |

**Write-offs are real but sparse.** The EDA found 3,362 product-code write-off rows in the CSV, but only 362 survive into the default replay set (42 `DAMAGE`, 320 `ADJUSTMENT`, about 0.07% of events), because most of them sit on SKUs outside the top 500. A few are huge (up to 9,600 units in one line, mostly with no description). They are real and kept as is, but inspect `docs/eda/tables/largest_writeoff_events.csv` before relying on them. If you want more adjustment and damage activity for the demo, use `--adjust-rate` and `--damage-rate` (below) as a supplement, or raise `--top-skus`.

**Warehouse assignment (hashing rule).** `Country` is a poor warehouse proxy because the dataset is dominated by one country (confirm in EDA and report the skew as a finding). Instead:

```
warehouse_id = WAREHOUSES[crc32(str(CustomerID or Invoice)) % len(WAREHOUSES)]
```

Default `WAREHOUSES=WH-01,WH-02,WH-03`. A given customer always ships from the same warehouse, and runs are repeatable. `--warehouse-strategy country-group` is an optional alternative (the dominant country split off, other countries pooled). This hash is unrelated to Kafka's key hashing: Kafka partitions on `warehouse_id:sku`.

**Replay clock and time rebasing**

Dashboards need data that looks current, so by default (`--time-mode rebased`) the producer **rewrites `event_time`** while keeping the original timestamp in `original_time`.

- Events are processed in original-time order, merged with scheduled simulated events through a min-heap.
- Rebased time: `event_time = replay_start + offset`, where `offset` accumulates, over consecutive events, `min(gap_seconds / SPEEDUP, MAX_GAP_SECONDS)` (gaps measured in original time). The producer sleeps until the wall clock reaches `event_time` before sending, so `event_time` is approximately the arrival time.
- Use the **scheduled** time as `event_time`, not the moment of the send call, so timestamps are monotonic and reproducible.
- `--speedup S`: data-seconds per real second. Default **600** (ten data-minutes per real second, so one real minute is roughly one trading day). Use `3600` or more for stress tests. `--speedup 0` disables pacing for throughput benchmarks, and `event_time` is then simply the send time. Measured durations and rates for the default replay set are in section 6.1: a full replay at 600 takes about 6 hours, so use `--max-events` for short demos.
- `--max-events N` (default: unlimited) stops the replay after N events. For example, 20,000 events at the default speedup of 600 take about 14 minutes at the measured average of 23.5 events/s.
- `--max-gap-seconds` (real seconds, default 1) caps any single gap, so nights, weekends and the Christmas closure do not stall the replay.
- `--replay-start ISO` (default: now, truncated to the second) fixes the start. The same start, the same `--seed` and the same data give identical timestamps, which makes tests and verification reproducible.
- `--time-mode original` keeps the historical timestamps as `event_time` (use the `historical` window preset, section 6.1).
- **Rebase once, in the producer.** Never stamp or rewrite `event_time` in the consumer or processor: that would be processing time and would defeat event-time windows and the late-data demo.
- **Honest labelling:** relative spacing between events is preserved but compressed, and time-of-day and weekday patterns do not show up on a live dashboard. Time-pattern EDA uses `original_time`.
- **Every run is a new run.** `event_id`s repeat across runs but rebased times do not, so start each demo with `make reset`; otherwise restored state and dedup history from an earlier run corrupt the results.

**Opening balances and replenishment** (simulated). The producer keeps a simulated stock position per `(warehouse, sku)` to decide when to restock. The processor remains the source of truth for stored stock. The simulated position is updated by every emitted event (sales, returns, and write-offs).

1. A pre-pass over the cleaned data computes average daily demand `d` per `(warehouse, sku)` from `OUTBOUND` events only.
2. At replay start, emit one `ADJUSTMENT` per pair with `zone=OPENING`, quantity `max(MIN_OPENING, ceil(OPENING_DAYS_COVER * d))` (defaults: 30 days, minimum 20), `event_time` one second before the first replayed event (in rebased mode, `replay_start` minus one second).
3. Reorder point `ROP = SAFETY_FACTOR * d * LEAD_TIME_DAYS_MAX` (default safety factor 1.2). When simulated stock is at or below `ROP` and no order is open for that pair, order up to `ceil(OPENING_DAYS_COVER * d)`. The order arrives after `Uniform(LEAD_TIME_DAYS_MIN=2, LEAD_TIME_DAYS_MAX=7)` **data days** (converted to rebased time as `days * 86400 / SPEEDUP` seconds), at which point an `INBOUND` event (`zone=DOCK`, `source=SIMULATOR`) is emitted.
4. With probability `TRANSFER_PROB` (default 0.2), if another warehouse holds more than the order-up-to level of that SKU, move the needed quantity as a `TRANSFER` (two events, `direction=OUT` and `direction=IN`, same `transfer_id`) instead of ordering from a supplier.
5. Real `ADJUSTMENT` and `DAMAGE` events come from the dataset (see the mapping table). Optionally, per `OUTBOUND` event, with probability `--adjust-rate` emit a small signed `ADJUSTMENT` and with `--damage-rate` a `DAMAGE`. Both default to **0 in replay mode** and to 0.002 and 0.001 in simulator-only mode.
6. Use a seeded RNG (`--seed`, default 42).

Tune the parameters so that a full run raises a handful of `LOW_STOCK` / `STOCKOUT_RISK` alerts, rather than none or thousands.

**Fault injection** (`faults.py`, applied to the combined stream, disabled by `--clean`). Each injected fault is recorded in the emitted-event log with its kind, for ground truth.

- **Duplicates** (`--dup-rate`, default 0.01): re-emit the same event (same `event_id`) 1 to 3 times shortly after the original.
- **Late events** (`--late-rate`, default 0.01): hold the event back and emit it after later events, keeping its `event_time`. Draw lateness from two bands relative to the watermark `W`: an **accepted** band `[0.1W, 0.8W]` and a **rejected** band `[1.5W, 3W]`, 50/50. Never use values near `W`, so ground truth is unambiguous. Add a `x-late-by-seconds` header.
- **Malformed** (`--malformed-rate`, default 0.002): corrupt JSON, drop a required field, or give a non-`ADJUSTMENT` event a negative quantity. These must reach `inventory-dlq`.
- **Bursts** (`--burst-factor`, default 1): temporarily multiply the speedup for short periods, to create lag.

All parameters above are env vars with defaults in `config/settings.py` and `.env.example`; CLI flags override them.

### 5.6 EDA deliverables

`tools/eda.py` reads the cleaned CSV (and, after a run, the emitted-event log or TimescaleDB) and saves PNG charts and CSV/markdown tables to `docs/eda/`. Required outputs:

1. Dataset overview: row counts raw and after each cleaning rule, date range, unique SKUs / customers / countries, missing values.
2. Event mix after cleaning: sales, returns, damage, adjustment (counts and units).
3. Country share (shows the skew and justifies the hashing rule).
4. Quantity distribution (log scale), plus p50 / p95 / p99 of daily outbound quantity per SKU (basis for the `ABNORMAL_OUTFLOW` threshold).
5. Events per SKU (rank plot, log-log) as a skew check.
6. Events by hour of day, day of week, and per day over time (shows the idle gaps). Use `original_time`, since rebased time hides these patterns.
7. Excluded `StockCode` table (non-product codes removed by cleaning rule 1).
8. Write-off events: counts, units, top descriptions, and the largest individual write-offs.
9. After a run: events per Kafka partition (validates the key), lateness distribution, duplicate / late / malformed rates, stock trajectories for a few SKUs, alert counts by type.

Every chart title or caption states its provenance: `UCI replay (real demand)`, `simulated`, or `combined`. Numbers in docs and slides must come from these outputs, never typed by hand.

## 6. Processing semantics (the important part)

1. **Event time, not processing time.** Windows are assigned from `event_time`.
2. **Watermark** = max seen event_time minus `WATERMARK` (preset values in 6.1). A window emits when the watermark passes its end. Events later than the watermark are counted in `late_events` and sent to the DLQ with reason `LATE`.
3. **Tumbling window**: `TUMBLING_WINDOW` (preset values in 6.1) per `(warehouse_id, sku)`: inbound units, outbound units, event count.
4. **Sliding window**: `SLIDING_WINDOW` with slide `SLIDE` (preset values in 6.1): moving average of outbound units per tumbling-window unit of time.
5. **Running stock** is keyed state updated per event.
6. **EMA** of demand: `EMA_t = alpha * x_t + (1 - alpha) * EMA_{t-1}`, default `alpha = 0.3`.
7. **Days-of-cover** = `current_stock / (EMA_rate_per_event_second * SECONDS_PER_DATA_DAY)` (guard against divide-by-zero), expressed in data-days. `SECONDS_PER_DATA_DAY` is `86400 / SPEEDUP` in rebased mode and `86400` in original and simulator-only modes. Alert thresholds are set in data-days. Producer and processor must read the same `SPEEDUP` from `.env`.
8. **Top-K SKUs** per window (exact counters by default; Count-Min Sketch as an optional extension).
9. **Dedup** by `event_id`, held for the watermark horizon.
10. **Alerts** (published to `inventory-alerts` and written to the `alerts` table):
    - `LOW_STOCK`: stock below reorder point
    - `STOCKOUT_RISK`: days-of-cover below threshold
    - `ABNORMAL_OUTFLOW`: window outflow above 3x the sliding average
    - `STALE_DATA`: no events for N seconds (Grafana alert rule)
      Use a per-`(sku, alert_type)` cooldown to avoid alert floods.

### 6.1 Window presets and replay-mode behaviour

Window sizes are **event-time** durations and must match the time mode:

| Setting                           | `live` (default: simulator, and replay with `--time-mode rebased`) | `historical` (only with `--time-mode original`) |
| --------------------------------- | ------------------------------------------------------------------ | ----------------------------------------------- |
| `WATERMARK`                       | 2 minutes                                                          | 30 minutes                                      |
| `TUMBLING_WINDOW`                 | 1 minute                                                           | 1 hour                                          |
| `SLIDING_WINDOW` / `SLIDE`        | 5 minutes / 1 minute                                               | 6 hours / 1 hour                                |
| `IDLE_FLUSH_SECONDS` (wall clock) | 30                                                                 | 10                                              |

Select with `WINDOW_PRESET=live|historical`; individual values can be overridden by env vars. In rebased mode at the default speedup, one 1-minute window holds about one trading day of demand.

- The watermark is derived **only from event times**, never from the wall clock.
- **Idle flush:** if no event arrives for `IDLE_FLUSH_SECONDS` of wall-clock time, advance the watermark far enough to close every open window, so the final windows emit when the replay ends.
- Because the real data is sorted by time, **all lateness comes from the injected late events** (section 5.5), which makes the late-data demo deterministic.
- **Measured replay rates** for the default replay set (505,081 events, top 500 SKUs, `--max-gap-seconds 1`), computed by `tools/eda.py` into `docs/eda/tables/replay_rate_estimates.csv`:

  | `--speedup`   | Full replay | Average events/s | p95 events/s | Peak events/s |
  | ------------- | ----------- | ---------------- | ------------ | ------------- |
  | 60            | 10.7 hours  | 13.2             | 42           | 335           |
  | 600 (default) | 6.0 hours   | 23.5             | 74           | 472           |
  | 3600          | 1.6 hours   | 86.9             | 228          | 737           |
  | 21600         | 25 minutes  | 341.3            | 882          | 1,595         |

  Peaks come from minute-resolution timestamps: invoice lines share a timestamp, so up to 335 events can arrive at the same instant. Re-run the EDA if you change `--top-skus` or the cleaning rules, since these figures depend on them. For a live demo of roughly 25 minutes use `--speedup 21600`; for roughly 20 minutes use `--speedup 600 --max-events 28000`.

## 7. Fault tolerance rules (non-negotiable)

- Producer: `acks=all`, `enable.idempotence=true`, retries with backoff, delivery callback that logs failures, `flush()` on shutdown.
- Consumer: `enable.auto.commit=false`. Commit offsets **only after** results are durably written. Preferred pattern: write window results, state checkpoint, and offsets to Postgres in **one transaction**, then commit to Kafka.
- On startup the processor restores state and seeks to stored offsets. Implement `on_assign` / `on_revoke` rebalance callbacks (checkpoint on revoke).
- Malformed or schema-invalid messages go to `inventory-dlq`; **never crash the loop on a bad record**.
- Handle SIGTERM/SIGINT gracefully: stop polling, checkpoint, close the consumer.
- Docker mode: services use `restart: unless-stopped` and health checks. Native mode: rely on the OS service manager (systemd, launchd, or a Windows service) and document how to check status.
- Kafka brokers are local processes, so `kafka-start.sh` must wait until the cluster is healthy (for example by polling `kafka-broker-api-versions.sh`) before returning.

## 8. Build order

Work in phases. Each phase must run and be verified before starting the next. Commit after each.

1. **Environment check** (section 2.1): report versions, confirm OS, confirm WSL2 on Windows.
2. **Kafka cluster**: write the three config files and scripts, format, start, create topics. Verify with `kafka-topics.sh --describe` (6 partitions, replication 3, ISR 3) and a console producer/consumer round trip.
3. **Database and Grafana (both modes)**: `.env.example`, `docker-compose.yml`, the native setup scripts, `sql/init.sql`, and provisioning files that use `${VAR}` expansion. Verify with `make db-check` in each mode you can test, and confirm the Grafana datasource shows healthy.
4. **Models and config**: pydantic models, settings, logging.
5. **Data and producers**:
   - Data prep: run `scripts/xlsx_to_csv.py`, then `tools/eda.py` on the cleaned CSV (section 5.6). Record the real row counts and exclusions.
   - Replay producer (`replay.py`, `mapping.py`) with `--time-mode`, `--replay-start`, `--speedup`, `--max-gap-seconds`, `--max-events`, `--top-skus`, `--warehouse-strategy`, `--seed`.
   - Replenishment (`replenishment.py`) and fault injection (`faults.py`: `--dup-rate`, `--late-rate`, `--malformed-rate`, `--burst-factor`, `--clean`).
   - Simulator-only mode (`simulator.py`) so the pipeline also runs without the dataset.
   - Verify: events are emitted in event-time order, keyed events spread across partitions, and the emitted-event log and ground-truth files are written to `data/truth/`.
6. **Basic consumer**: print events and partition assignment. Run 3 instances and show the split.
7. **Windows and aggregations**: pure functions first, with unit tests (no Kafka needed).
8. **Processor + sink**: wire everything together, with checkpointing, DLQ, alerts, idempotent upserts, and pipeline metrics.
9. **Second consumer group** (`audit-logger`) and `alert_listener`.
10. **Grafana**: provisioned datasource and dashboard JSON.
11. **Failure demos** (section 10).
12. **Docs**: README, architecture, demo script, screenshots.

## 9. Commands (Makefile targets, or equivalent scripts)

```
make kafka-format     # one-time cluster format
make kafka-up         # start 3 brokers, wait until healthy
make kafka-down       # stop brokers
make topics           # create topics idempotently
make db-up            # start TimescaleDB + Grafana per DEPLOY_MODE (native|docker)
make db-down          # stop them per DEPLOY_MODE
make db-check         # verify connection, extension, hypertables
make data-convert     # python scripts/xlsx_to_csv.py data/online_retail_II.xlsx data/raw/online_retail_ii.csv
make eda              # charts and tables into docs/eda/
make replay           # UCI replay producer (SPEEDUP=600 TIME_MODE=rebased TOP_SKUS=500 SEED=42 ...)
make producer         # alias for make replay
make simulate         # simulator-only producer (RATE=200 etc.)
make processor        # run one processor instance (run in 3 terminals)
make audit            # run audit-logger group
make reset            # clean slate: truncate tables, clear checkpoints, recreate topics
make verify           # compare DB to ground truth
make demo-rebalance
make demo-broker-kill
make demo-crash-recovery
make test             # unit tests
make test-int         # integration tests (needs Kafka + DB up)
make lint             # ruff + black --check + mypy
```

## 10. Failure demonstrations to script and document

Each demo needs: setup steps, the command that injects the failure, the expected observable behaviour, and a screenshot or log excerpt in `docs/`.

1. **Consumer rebalance**: kill one of three processor instances, observe partitions redistribute and lag recover.
2. **Broker failure**: `kill -9` one broker process (`scripts/kafka-kill.sh 2`), observe leader election via `kafka-topics.sh --describe` and no data loss (`acks=all`, `min.insync.replicas=2`). Restart the broker and watch it rejoin the ISR.
3. **Processor crash**: `kill -9` the processor mid-window, restart, verify state restore and that stock levels and window metrics match ground truth.
4. **Poison pill**: send malformed JSON, verify it lands in `inventory-dlq` and the pipeline keeps running.
5. **Duplicate and late events**: inject duplicates (same `event_id`) and late events in both lateness bands (section 5.5). Verify dedup, that accepted-band late events land in the correct window, and that rejected-band events appear in `inventory-dlq` and the `late_events` metric.
6. **Database outage**: stop the database (`docker compose pause timescaledb` in Docker mode, or stop the PostgreSQL service in native mode, wrapped by `scripts/db-stop-start.sh`), verify the processor retries without committing offsets, then catches up after restart with no duplicates.

## 11. Correctness checks

- The producers write an **emitted-event log** and **ground truth** (final stock per `(warehouse, sku)`, per-window totals) to `data/truth/`. The log records the rebased `event_time` actually emitted. Truth is computed independently of the processor, from the log, using the section 6 rules: duplicates count once, and injected late events from the rejected band are treated as rejected. `tools/verify.py` compares DB contents to it after any demo. Expected result: **zero mismatches, zero duplicates**.
- Metrics to record for the report: throughput (events/s), end-to-end latency (`ingest_ts` minus `event_time` in rebased mode; in original mode use a send-timestamp header instead), consumer lag under load, recovery time after each failure.
- Run a throughput benchmark with `--speedup 0` (no pacing) and record the sustained events/s.

## 12. Code conventions

- Type hints everywhere; `ruff`, `black`, and `mypy` clean.
- No hard-coded hosts, ports, or credentials. Everything comes from env vars with defaults in `config/settings.py`, documented in `.env.example`.
- Keep window and aggregation logic **pure** (no Kafka or DB calls) so it is unit-testable.
- Structured logging with partition, offset, and sku in context where relevant.
- Small, focused commits with descriptive messages (`feat:`, `fix:`, `docs:`, `test:`).
- Pin Python dependency versions and Docker image tags (no `latest`).
- Use `zlib.crc32` (or another stable hash) for warehouse assignment and any bucketing. Never use Python's built-in `hash()`: it is salted per process and would break replay determinism.
- All randomness comes from a seeded RNG (`--seed`, default 42), so runs and ground truth are reproducible.

## 13. Do and don't

**Do**

- Ask the human before changing a decision in section 2.
- Run the code you write (start the cluster, producer, processor) and report actual output rather than assuming it works.
- Keep the README demo path runnable in under 15 minutes on a laptop.
- Verify both `DEPLOY_MODE=native` and `DEPLOY_MODE=docker`. If you can only test one in the current environment, say which one is untested.
- Note any deviation from this file in `docs/architecture.md`.
- Report every dataset statistic (row counts, exclusions, rates) from an actual run. Never write placeholder numbers into docs or slides.
- Label data provenance everywhere: demand, returns and stock write-offs are real (UCI replay), while replenishment, opening balances, transfers, and faults are simulated (plus optional random adjustments and damage), and timestamps are rebased to replay time (relative spacing preserved, but compressed).

**Don't**

- Don't use auto-commit offsets.
- Don't publish messages without a key.
- Don't run Kafka in Docker (it is already installed locally).
- Don't write code that depends on `DEPLOY_MODE`; only env vars, scripts, and compose may differ.
- Don't hard-code `localhost` or container hostnames inside Grafana provisioning files; use the `TSDB_*` variables.
- Don't write Java application code; Java is only the Kafka runtime here.
- Don't commit secrets, `.env`, `kafka-data/`, or volume data.
- Don't commit the dataset or generated truth/log files; cite the dataset in the README instead.
- Don't rewrite or stamp `event_time` in the consumer or processor; rebasing happens once, in the producer.
- Don't use wall-clock time for windows or watermarks; only event time (the idle flush is the single, documented exception).
- Don't add Confluent-only components (Schema Registry, ksqlDB, Control Center).
- Don't build a React UI unless asked.
- Don't mark a phase done without running its verification step.

## 14. Definition of done

- `make kafka-up && make topics && make db-up && make producer && make processor` yields a live Grafana dashboard with no manual setup, in **either** `DEPLOY_MODE`.
- `make db-check` passes in both modes, and the README documents both setups.
- All nine checklist items in section 1 are demonstrably satisfied and referenced in the README.
- All six failure demos run and are documented.
- `make test` and `make lint` pass.
- `make verify` reports zero mismatches after each demo.
- Both source modes work: `make replay` (UCI) and `make simulate`.
- In the default rebased mode, Grafana panels with a "last 5 minutes" range show live data.
- `docs/eda/` contains every output listed in section 5.6, generated from real runs.
- The README cites the dataset (section 5.5) and states clearly which events are real and which are simulated.
