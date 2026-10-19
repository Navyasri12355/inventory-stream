# AGENTS.md

Guidance for AI coding agents (and humans) building this project from an empty repository.

## 1. Project summary

**Real-Time Warehouse Inventory Event Analytics** is an end-to-end streaming pipeline:

```
Kafka Connect Datagen -> gen-* topics -> Python shaper -> inventory-events (Apache Kafka 3.9.1)
   -> Stream Processor -> TimescaleDB -> Grafana (dashboard + alerts)
```

The event source is **calibrated synthetic data, not a replay of real transactions**. Three public datasets supply *parameters* (SKU master, city and channel demand shares, quantity distributions, return and write-off rates, threshold references). Kafka Connect Datagen produces the stateless bulk of the demand events. A Python **shaper** adds everything stateful or time-dependent: event time, pacing, returns, replenishment, transfers, write-offs, festive bursts, and fault injection (section 5.5). Be honest about this everywhere (README, charts, slides): say "synthetic data calibrated to public datasets", never "real demand".

It exists to demonstrate, in one reproducible repo, all of the following (this is the grading checklist; do not drop any item):

- [ ] Apache Kafka is the central messaging platform
- [ ] At least one producer and one consumer (the shaper is both; the processor is a consumer)
- [ ] At least two topics (we use `inventory-events`, `inventory-alerts`, `inventory-dlq`, plus three `gen-*` staging topics)
- [ ] Partitioning (keyed by `warehouse_id:sku`) and consumer groups (with rebalancing demo)
- [ ] At least one window-based operation (tumbling + sliding)
- [ ] At least one aggregation/summarisation algorithm (running stock, EMA, top-K, days-of-cover)
- [ ] Fault tolerance and recovery (replication, idempotent producer, offset handling, checkpoint, DLQ)
- [ ] Results stored in a time-series store (TimescaleDB)
- [ ] Real-time visualisation and alerting (Grafana)

## 2. Environment and fixed technology decisions

The developer already has **Apache Kafka 3.9.1** and **Java** installed locally. Do not substitute anything below without asking the human first.

| Concern            | Decision                                                                                                                                                                                                                                                                      |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Event source       | **Calibrated synthetic generation**: parameters from three datasets (Zepto Inventory, India Retail Sales, UCI Online Retail II), bulk events from Kafka Connect Datagen, stateful finishing in a Python shaper. Details in section 5.5                                          |
| Generator          | **Kafka Connect Datagen** connector (runs on Apache Kafka's own Connect framework, standalone mode). Requires **Java 17 or newer**                                                                                                                                            |
| Broker             | **Locally installed Apache Kafka 3.9.1**, **KRaft mode** (no ZooKeeper). Location comes from the `KAFKA_HOME` env var                                                                                                                                                         |
| Cluster shape      | **3 brokers on one machine** (combined broker+controller nodes), each with its own config file, port, and log dir                                                                                                                                                             |
| Java               | Needed to run Kafka and Kafka Connect + Datagen (**17 or newer**). No Java application code in this project                                                                                                                                                                   |
| Language           | Python 3.11+                                                                                                                                                                                                                                                                  |
| Kafka client       | `confluent-kafka` (a client library only; it talks to plain Apache Kafka)                                                                                                                                                                                                     |
| Stream processing  | **Custom Python processor** on `confluent-kafka` (explicit windowing, state, checkpointing). PySpark Structured Streaming is the only approved alternative                                                                                                                    |
| Serialisation      | JSON validated with `pydantic`. Connect uses `JsonConverter` with schemas disabled. The Avro `.avsc` files are only Datagen's rule files. No Schema Registry                                                                                                                  |
| Confluent parts    | Only the Datagen connector plugin. No Schema Registry, ksqlDB, or Control Center                                                                                                                                                                                              |
| Storage            | TimescaleDB (PostgreSQL + extension) via `psycopg[binary]`. Run **natively or in Docker** (section 2.2)                                                                                                                                                                       |
| Dashboard / alerts | Grafana, provisioned from files (no manual clicking). Run **natively or in Docker** (section 2.2)                                                                                                                                                                             |
| Docker             | **Optional**, supported as an alternative way to run TimescaleDB and Grafana. Kafka and Connect are NOT run in Docker, and no Kafka UI container is used (the brokers advertise `localhost`, which a container could not reach). Use the Kafka CLI tools and Grafana for visibility |
| React dashboard    | **Out of scope** unless the human asks                                                                                                                                                                                                                                        |

### 2.1 First step for the agent: detect the OS and tooling

Before generating any scripts, check the operating system and confirm with the human.

- **Linux / macOS**: provide a `Makefile` and `scripts/*.sh`, using `$KAFKA_HOME/bin/*.sh`.
- **Windows**: strongly recommend running everything inside **WSL2**. Native Windows Kafka is known to crash on log-segment deletion due to file locking, which would sabotage the recovery demos. If the human insists on native Windows, provide PowerShell scripts (`scripts/*.ps1`) using `%KAFKA_HOME%\bin\windows\*.bat`.
- Verify prerequisites and report versions: `java -version` (**must be 17 or newer**, because Datagen requires it; if older, stop and tell the human), `kafka-topics.sh --version` (expect 3.9.1), `python --version`.
- Detect what is available for the database and dashboard: `psql --version` and whether the `timescaledb` extension is installed, `grafana-server --version` (or `grafana server --version`), and `docker --version` / `docker compose version`.
- Recommend a `DEPLOY_MODE` (section 2.2): `native` if PostgreSQL with TimescaleDB and Grafana are already installed, otherwise `docker`. Confirm with the human and record it in `.env`.
- Check for the three datasets in `data/raw/`: `zepto_inventory.csv`, `india_retail_sales.csv`, `online_retail_ii.csv`. If `online_retail_ii.csv` is missing but `data/online_retail_II.xlsx` exists, run `python scripts/xlsx_to_csv.py data/online_retail_II.xlsx data/raw/online_retail_ii.csv`. For any dataset that is missing, tell the human where to get it (Kaggle needs an account; UCI dataset id 502) and wait. Never fabricate data, and confirm the file names with the human.
- Check whether the Datagen plugin is installed under `plugins/` (section 5.5.4). If not, tell the human how to obtain it (Confluent Hub download or build from the GitHub repository) and wait.

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
- Kafka, Connect, the shaper and the processor run on the host and reach the DB through the published port.

**Both modes must pass the same checks:** `make db-check` (connects, confirms the `timescaledb` extension and that all hypertables exist via `timescaledb_information.hypertables`) and `make verify`.

**Docs:** provide `docs/setup-native.md` and `docs/setup-docker.md` with install steps, start/stop commands, and troubleshooting. The README quick start shows both paths side by side.

## 3. Repository layout (create this)

```
.
├── AGENTS.md
├── README.md                  # how to run + demo script + dataset licenses + screenshots
├── docker-compose.yml         # Docker mode: timescaledb + grafana ONLY
├── .env.example
├── .gitignore
├── venv/                      # local Python virtualenv (gitignored)
├── pyproject.toml / requirements.txt
├── Makefile                   # (or scripts/*.ps1 on native Windows)
├── config/
│   ├── settings.py            # all app config via env vars with defaults
│   ├── columns.yaml           # per-dataset column-name mapping (edit if headers differ)
│   ├── params.json            # calibrated parameters; generated by tools/calibrate.py (section 5.5.2)
│   ├── kafka/
│   │   ├── server-1.properties
│   │   ├── server-2.properties
│   │   └── server-3.properties
│   ├── connect/
│   │   └── connect-standalone.properties
│   └── datagen/               # GENERATED by scripts/build_datagen.py from params.json
│       ├── retail.avsc   retail.properties
│       ├── bulk.avsc     bulk.properties
│       └── festive.avsc  festive.properties
├── plugins/                   # Connect plugin.path: unzipped Datagen plugin (gitignored)
├── scripts/
│   ├── xlsx_to_csv.py         # data/online_retail_II.xlsx (2 sheets) -> data/raw/online_retail_ii.csv
│   ├── build_datagen.py       # params.json -> config/datagen/*
│   ├── kafka-format.sh        # one-time: generate cluster id, format 3 log dirs
│   ├── kafka-start.sh         # start brokers 1-3 in background, write PID files
│   ├── kafka-stop.sh
│   ├── kafka-kill.sh          # kill -9 a single broker (for failure demo)
│   ├── connect-start.sh       # start standalone Connect worker + the three Datagen connectors
│   ├── connect-stop.sh
│   ├── create-topics.sh       # idempotent topic creation
│   ├── db-native-setup.sh     # native mode: role, database, extension, init.sql
│   ├── db-init.sh             # apply sql/init.sql via DATABASE_URL (either mode)
│   ├── db-stop-start.sh       # stop/start the DB in either mode (for outage demo)
│   └── grafana-native-start.sh  # native mode: run Grafana with repo provisioning
├── src/
│   ├── common/
│   │   ├── models.py          # pydantic InventoryEvent, Alert
│   │   ├── kafka_utils.py     # producer/consumer factories, delivery callbacks
│   │   ├── uci_clean.py       # UCI cleaning rules + damage keywords (shared by eda.py and calibrate.py)
│   │   └── logging_setup.py
│   ├── producer/
│   │   ├── shaper.py          # main loop: consume gen-*, mix, pace, stamp time, publish
│   │   ├── pools.py           # --source local: sample tuples in-process from params.json
│   │   ├── stock_model.py     # simulated stock per (warehouse, sku)
│   │   ├── replenishment.py   # opening balances, reorder points, transfers
│   │   ├── derived_events.py  # returns and write-offs
│   │   └── faults.py          # duplicate / late / malformed injection
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
│       ├── eda.py             # UCI EDA (exists): charts, tables, dataset description
│       ├── calibrate.py       # all three datasets -> params.json + calibration report
│       ├── fidelity.py        # generated vs source distributions and correlations
│       ├── verify.py          # compare DB to ground truth (section 11)
│       └── chaos.py           # helpers for failure demos
├── data/                      # all gitignored
│   ├── online_retail_II.xlsx  # original UCI download (two sheets)
│   ├── raw/
│   │   ├── online_retail_ii.csv    # output of scripts/xlsx_to_csv.py
│   │   ├── zepto_inventory.csv
│   │   └── india_retail_sales.csv
│   └── truth/                 # emitted-event log + ground truth
├── sql/
│   └── init.sql               # hypertables, indexes, continuous aggregates
├── grafana/
│   ├── provisioning/datasources/
│   ├── provisioning/dashboards/
│   └── dashboards/inventory.json
├── tests/
│   ├── unit/                  # windows, aggregations, dedup, models, stock model, pools
│   └── integration/           # needs Kafka + DB running
└── docs/
    ├── architecture.md
    ├── eda/                   # charts + tables + calibration_report.md + dataset_description.md
    ├── setup-native.md
    ├── setup-docker.md
    └── demo-script.md
```

Make sure `.gitignore` contains `venv/`, `kafka-data/`, `grafana-data/`, `plugins/`, `data/`, `.env`, and `*.pid`. `config/params.json` and `config/datagen/` are **committed** (they let the repo run without the datasets), unless a dataset license forbids publishing derived aggregates. In that case gitignore them and document how to regenerate.

## 4. Local Kafka cluster setup (3 brokers, KRaft, v3.9.1)

Each broker is a combined `broker,controller` node. Per-broker values:

| Setting                  | Broker 1                | Broker 2                | Broker 3                |
| ------------------------ | ----------------------- | ----------------------- | ----------------------- |
| `node.id`                | 1                       | 2                       | 3                       |
| client listener port     | 9092                    | 9094                    | 9096                    |
| controller listener port | 9093                    | 9095                    | 9097                    |
| `log.dirs`               | `./kafka-data/broker-1` | `./kafka-data/broker-2` | `./kafka-data/broker-3` |

Use **absolute** `log.dirs` paths in practice (a relative path depends on the directory Kafka is started from).

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

Applications connect with `bootstrap.servers=localhost:9092,localhost:9094,localhost:9096`. If a port is already in use at startup (for example a stock single-node Kafka still running), stop the old process first.

**Low-resource fallback:** if the machine cannot handle 3 brokers, run a single broker with replication factor 1 and `min.insync.replicas=1`. The broker-failure demo then cannot be shown; document that clearly.

## 5. Contracts

### 5.1 Event schema (`inventory-events`)

```json
{
  "event_id": "uuid string, uuid5(run_id, sequence number)",
  "event_time": "ISO-8601 UTC, the shaper's scheduled emission time",
  "warehouse_id": "WH-BLR",
  "city": "Pune (demand events only, otherwise null)",
  "channel": "Online (demand events only, otherwise null)",
  "sku": "ZPT-00417",
  "event_type": "INBOUND | OUTBOUND | TRANSFER | ADJUSTMENT | DAMAGE",
  "quantity": 3,
  "zone": "Z3",
  "source": "DATAGEN | SHAPER",
  "direction": "OUT | IN (TRANSFER only)",
  "transfer_id": "uuid (TRANSFER only)"
}
```

Rules:

- `quantity` is a positive integer, except `ADJUSTMENT`, which may be signed.
- Stock effect: `INBOUND +q`, `OUTBOUND -q`, `DAMAGE -q`, `ADJUSTMENT +/-q`. `TRANSFER` is emitted as **two events** sharing a `transfer_id`: one with `direction=OUT` (stock -q, keyed by the source warehouse) and one with `direction=IN` (stock +q, keyed by the destination warehouse). Two events are required because each stock balance lives on the partition that owns its key, and one event cannot update state on two partitions.
- `source=DATAGEN` marks `OUTBOUND` demand drawn from a Datagen tuple. `source=SHAPER` marks everything the shaper derives (opening balances, returns, replenishment, transfers, write-offs).
- Special zones: `OPENING`, `RETURNS`, `DOCK`. Otherwise `zone = Z{crc32(sku) % 8}`.
- **Message key = `"{warehouse_id}:{sku}"`**. Never publish without a key.
- Product attributes (name, category, MRP, price) are **not** in the event. They come from `sku_catalog` (section 5.5.3, rule 1).

### 5.2 Topics

| Topic              | Partitions | Replication | Notes                                                                                         |
| ------------------ | ---------- | ----------- | --------------------------------------------------------------------------------------------- |
| `inventory-events` | 6          | 3           | the pipeline's event stream                                                                   |
| `inventory-alerts` | 3          | 3           | key = sku                                                                                     |
| `inventory-dlq`    | 1          | 3           | original payload plus `error`, `source_topic`, `source_partition`, `source_offset` in headers |
| `gen-retail`       | 1          | 3           | Datagen output, B2C tuples (staging buffer, retention of at least a few hours)                |
| `gen-bulk`         | 1          | 3           | Datagen output, bulk tuples                                                                   |
| `gen-festive`      | 1          | 3           | Datagen output, festive-scenario tuples                                                       |

`create-topics.sh` must be idempotent (`--if-not-exists`) and set `min.insync.replicas=2`. The `gen-*` topics use one partition so the shaper reads them in a deterministic order.

### 5.3 Consumer groups

- `shaper`: 1 instance, reads the three `gen-*` topics
- `inventory-processor`: 3 instances, shares the 6 partitions of `inventory-events`
- `audit-logger`: 1 instance, independent group on `inventory-events` (shows fan-out)
- `alert-listener`: reads `inventory-alerts`

### 5.4 Database tables (TimescaleDB hypertables)

- `stock_levels(ts, warehouse_id, sku, stock, ingest_ts)` PK `(ts, warehouse_id, sku)`; `ts` is event time
- `window_metrics(window_start, window_end, window_type, warehouse_id, sku, inbound, outbound, avg_demand, ingest_ts)` PK `(window_start, window_type, warehouse_id, sku)`
- `alerts(ts, warehouse_id, sku, alert_type, severity, message, ingest_ts)`
- `pipeline_metrics(ts, instance_id, partition, consumer_lag, events_per_sec, late_events, dlq_count)` so Grafana can show lag and health without extra tooling
- `consumer_offsets(group_id, topic, partition, committed_offset)` for atomic offset + result commits
- `sku_catalog(sku, product_name, category, mrp_inr, selling_price_inr)` loaded once from `params.json` (Zepto master), for readable names, category panels, and stock value in rupees
- `hubs(warehouse_id, region, hub_city)` loaded once from `params.json`

All writes use `INSERT ... ON CONFLICT DO UPDATE` so replays never create duplicates.

**Time columns.** `ts` / `window_start` are **event time**, which tracks the wall clock because the shaper stamps it at emission, so Grafana's "last 5 minutes" works directly on them. `ingest_ts timestamptz NOT NULL DEFAULT now()` is the write time, refreshed on every upsert; index it, and use `ingest_ts - event_time` for latency. Hypertables are partitioned on the event-time column (chunk interval 1 day). Retention and continuous-aggregate policies relative to `now()` are fine, but keep retention well above the length of a demo.

### 5.5 Data source: calibrated synthetic generation

**Principle.** Real datasets are used only to estimate *parameters*. Generated data is synthetic. Where datasets cannot say something (cross-dataset links, time dynamics, restocking), the design states an **assumption** and the report lists it. Never present an assumption as a finding.

#### 5.5.1 Datasets and roles

The facts in the second column are **as stated by the human and must be verified when the files are loaded**.

| Dataset                           | Facts to verify on load                                                                                                                                                                                                                   | Used for                                                                                                                                                        | Not used for                                                                                |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| **Zepto Inventory Dataset** (Kaggle) | 3,732 rows, 9 columns, 1,681 unique product names. Quick-commerce grocery: category, product name, MRP, discount percent, available quantity, discounted selling price, out-of-stock flag                                                  | SKU master (`sku_catalog`), category mix, prices, opening-stock profile, out-of-stock share                                                                     | Demand: it is a snapshot with no sales and no time dimension                                |
| **India Retail Sales Dataset** (Kaggle) | 20,000 orders, 17 columns, about 4,000 customers, Jan 2023 to Dec 2025, 17 cities in 4 regions (North, South, East, West). Columns: order ID, date, customer, region, city, category, product, unit price, quantity (1 to 5), discount, sales, profit, channel, payment method, rating | City and region demand shares (warehouse hubs), channel mix, B2C quantity, **joint (city, channel, quantity) tuples**                                         | Its categories and products (the taxonomy differs from Zepto's: do not map one to the other) |
| **UCI Online Retail II**          | About 1.03M rows after the xlsx conversion; see `docs/eda/dataset_description.md` for the measured profile                                                                                                                                | Heavy-tailed bulk quantity distribution, return rate, write-off rates and quantities, SKU popularity skew (Zipf exponent), daily-outflow percentiles for sanity checks | Geography, products, timestamps                                                             |

**Provenance and licensing rules**

- Record each dataset's license from its source page in the README. Do not commit the datasets.
- **India Retail Sales may itself be synthetic.** Exactly 20,000 orders and quantities limited to 1 to 5 are typical of generated datasets. `calibrate.py` must run provenance indicators and report them: chi-square tests against a uniform distribution for quantity, city, channel and weekday, plus checks for suspiciously regular patterns. If the data looks synthetic, every document says "calibrated to a public India retail dataset" and never "real Indian demand".
- Zepto is a snapshot: confirm that the currency is rupees and note the snapshot date if the file has one.

#### 5.5.2 Calibration (`src/tools/calibrate.py`)

Inspect each CSV's headers first and map columns through `config/columns.yaml`. Fail loudly on a mismatch; never guess a column. Outputs: `config/params.json` (machine-readable) and `docs/eda/calibration_report.md` (human-readable, with charts). Re-run it whenever a dataset or a cleaning rule changes.

UCI statistics use the shared cleaning rules in `src/common/uci_clean.py` (move `clean()` and the damage keywords out of `eda.py` and import them from both tools, with no copy-paste):

1. Strip and upper-case `StockCode`, keep only product rows (`^\d{5}[A-Za-z]*$`).
2. Drop `Quantity == 0` and `UnitPrice < 0`.
3. Keep rows with a missing `CustomerID`.
4. Classify rows: **sale** (`Quantity` > 0, `UnitPrice` > 0, invoice not starting with `C`), **return** (`C` invoice, `Quantity` < 0, `UnitPrice` > 0), **damage write-off** (negative quantity, zero price, non-`C` invoice, description matches a damage keyword), **adjustment write-off** (same, any other description). Drop and count everything else.

Damage keywords (case-insensitive substrings): `damage`, `smash`, `broken`, `destroy`, `thrown`, `unsaleable`, `faulty`, `crushed`, `wet`, `mouldy`, `rust`.

**Parameters in `params.json`**

| Parameter                          | Source                | How it is computed                                                                                                                                                                                              | Reference value (UCI figures come from the earlier EDA run: top-500 SKUs, 3 hashed warehouses; regenerate) |
| ---------------------------------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `catalog`                          | Zepto                 | One SKU per unique (product name, category, MRP). SKU id is a stable slug plus short hash. Fields: name, category, MRP, selling price, discount, available quantity, out-of-stock flag                          | count from the run (at least 1,681)                                                                       |
| `sku_weights`                      | Zepto + UCI           | Category share from Zepto times a within-category Zipf weight. The exponent is fitted to UCI's rank-frequency curve. Ranks come from a seeded shuffle (Zepto has no sales counts, so this is an **assumption**)  | exponent from the run                                                                                     |
| `opening_profile`                  | Zepto                 | Per-SKU ratio `available_quantity / median(available_quantity)`; out-of-stock SKUs get 0                                                                                                                        | out-of-stock share from the run                                                                           |
| `hubs`, `city_to_hub`, `city_shares` | India Retail Sales  | One hub per region by default (hub city = the region's highest-demand city). Hub share = region share. Optional `--hub-strategy city` makes every city a hub                                                    | from the run                                                                                              |
| `channel_mix`                      | India Retail Sales    | Share of orders per channel                                                                                                                                                                                     | from the run                                                                                              |
| `retail_pool`                      | India Retail Sales + Zepto | `POOL_SIZE` tuples `(sku, city, channel, quantity)`: `(city, channel, quantity)` is resampled **jointly** from real rows, `sku` is drawn independently from `sku_weights`                                | `POOL_SIZE` default 20,000                                                                                |
| `bulk_pool`, `bulk_quantity_table` | UCI                   | Quantile table of sale quantities above `BULK_MIN` (default 6). The pool pairs these quantities with independently drawn `sku`, `city` and `channel`                                                            | UCI sale quantity: p50 4, p90 24, p95 36, p99 120, max 10,000                                             |
| `return_rate`, `return_qty_table`  | UCI                   | Return events divided by sale events, and the return quantity distribution                                                                                                                                      | 8,545 / 496,174 = 1.72%; return quantity p50 2, p95 36, p99 231                                           |
| `damage_rate`, `adjustment_rate`   | UCI                   | Write-off events divided by sale events                                                                                                                                                                         | 42 / 496,174 = 0.0085%; 320 / 496,174 = 0.065%                                                            |
| `writeoff_qty_tables`              | UCI                   | Quantity percentile tables for damage and adjustment                                                                                                                                                            | damage p50 35, p95 732; adjustment p50 76, p95 1,022 (max 9,600)                                          |
| `daily_outflow_ref`                | UCI                   | Units sold per key-day: p50, p95, p99, and the share of key-days above 3x the key's own mean                                                                                                                    | p50 6, p95 62, p99 194; 5.9% above 3x                                                                     |

**Assumptions (not estimated from data; list them in the README and report)**

| Assumption                  | Default                | Why it is an assumption                                                    |
| --------------------------- | ---------------------- | -------------------------------------------------------------------------- |
| `BULK_SHARE`                | 0.03                   | The datasets cannot say how much of Indian demand is bulk. If India Retail Sales has a wholesale-like channel, set it from that share instead |
| `RETURN_DELAY_DAYS`         | 1 to 7 simulated days  | No dataset gives the delay between a sale and its return                   |
| `LEAD_TIME_DAYS`            | 2 to 7 simulated days  | Supplier lead times are not in any dataset                                 |
| `OPENING_DAYS_COVER`        | 15 simulated days (median SKU) | Scales Zepto's stock profile to the simulated demand level          |
| `SAFETY_FACTOR`             | 1.2                    | Reorder-point policy                                                       |
| `SIM_DAY_SECONDS`           | 60                     | One simulated day lasts 60 real seconds, so a 1-minute window is one simulated day |
| `TARGET_RATE`               | 50 events/s            | Tune to the machine; measure the maximum with `--rate 0`                   |
| `FESTIVE_RATE_MULT`, `FESTIVE_CITIES` | 5x, top 3 demand cities | A stress scenario, not an estimate of any real festival              |
| `WRITEOFF_MAX_FRACTION`     | 0.2 of current stock   | Keeps rare, huge UCI write-offs from wiping out a Zepto-scale stock level  |
| SKU independent of city and channel | n/a            | No dataset links products to cities or channels                            |

#### 5.5.3 Correlation rules (binding)

Datagen is stateless and draws fields independently, so dependencies must be preserved by design:

1. **Lookup for functional dependence.** Category, name, MRP and price are determined by `sku`. They are joined from `sku_catalog`, never generated independently.
2. **Joint tuples for statistical dependence.** `(city, channel, quantity)` is always drawn together from real India Retail Sales rows. Datagen picks whole tuples from the pool, so the source's joint structure survives (a bootstrap resample).
3. **Separate segments for different populations.** B2C (`gen-retail`), bulk (`gen-bulk`) and festive (`gen-festive`) are separate connectors and topics with their own pools. The shaper mixes them by ratio.
4. **Stateful and temporal dependence lives in the shaper only.** Returns follow earlier sales, replenishment reacts to stock, write-offs are capped by stock, and the festive window changes the mix over time.
5. **Cross-dataset links are independence assumptions.** Do not invent links between datasets (for example Zepto categories and India Retail categories).
6. **Verify.** `tools/fidelity.py` compares generated data to the source (section 11).

#### 5.5.4 Datagen layer

- **Plugin:** `kafka-connect-datagen`, built on the Avro Random Generator. Install from Confluent Hub or build from its GitHub repository, unzip into `plugins/`, and list that directory in `plugin.path`. Java 17 or newer is required. Confirm the plugin loaded through the Connect REST API (`GET /connector-plugins` on port 8083).
- **Worker:** run Connect in standalone mode from Apache Kafka (`connect-standalone.sh`) with `config/connect/connect-standalone.properties`: `bootstrap.servers` set to the three brokers, `key.converter=org.apache.kafka.connect.storage.StringConverter`, `value.converter=org.apache.kafka.connect.json.JsonConverter`, `value.converter.schemas.enable=false`, `plugin.path`, and an absolute `offset.storage.file.filename`.
- **Connectors:** three, named `datagen-retail`, `datagen-bulk`, `datagen-festive`, each with `connector.class=io.confluent.kafka.connect.datagen.DatagenConnector`, its own `kafka.topic`, `schema.filename` pointing at its `.avsc`, and `tasks.max=1`. For pacing, a message-count limit and a random seed, **look up the exact configuration keys in the documentation of the installed version; do not guess them.** Set the count limit to about twice the expected events for a run.
- **Schemas:** `scripts/build_datagen.py` writes each `.avsc` from `params.json`. Each record is chosen from a record-level `options` list of whole tuples. Use non-null primitive field types (`string`, `int`). Illustrative shape (confirm the exact syntax against the Avro Random Generator README; use its options-file variant if an inline list is too large or slow to parse):

```json
{
  "type": "record", "name": "RetailTuple", "namespace": "inventory.gen",
  "fields": [
    {"name": "sku", "type": "string"}, {"name": "city", "type": "string"},
    {"name": "channel", "type": "string"}, {"name": "quantity", "type": "int"}
  ],
  "arg.properties": { "options": [
    {"sku": "ZPT-00417", "city": "Pune", "channel": "Online", "quantity": 2}
  ]}
}
```

- **Role:** Datagen is an **unpaced bulk supplier**. The `gen-*` topics are staging buffers, and the shaper alone controls the event rate.
- **Fallback:** `--source local` makes the shaper sample the same pools in-process (`pools.py`). Use it when Connect or Java 17 is unavailable, and say so in the README. The pipeline downstream is identical.

#### 5.5.5 The shaper (`src/producer/shaper.py`)

The shaper is the project's Kafka producer (and a consumer of `gen-*`). In processing order:

1. **Consume** `gen-retail`, `gen-bulk` and `gen-festive` (group `shaper`, manual commit after publishing).
2. **Mix.** Each demand event comes from `gen-bulk` with probability `BULK_SHARE`, otherwise from `gen-retail`. Inside a festive window, retail demand comes from `gen-festive` and the rate is multiplied by `FESTIVE_RATE_MULT`.
3. **Pace.** Exponential inter-arrival times with mean `1 / rate` (seeded). `--rate 0` disables pacing for throughput benchmarks. `--max-events N` or `--duration S` ends the run. The run also ends cleanly when the `gen-*` topics are exhausted, followed by the idle flush.
4. **Stamp.** `event_time` is the **scheduled** emission time in UTC (monotonic, not the moment of the send call). `--start ISO` fixes the start. `event_id = uuid5(run_id, sequence)`.
5. **Resolve location.** `warehouse_id = city_to_hub[city]`; compute `zone` and the key.
6. **Opening balances** at run start, one `ADJUSTMENT` per `(hub, sku)` with `zone=OPENING` and `event_time` one second before the first event:

```
mean_qty   = (1 - BULK_SHARE) * mean_retail_qty + BULK_SHARE * mean_bulk_qty
d(hub,sku) = RATE * SIM_DAY_SECONDS * hub_share * sku_weight * mean_qty      # expected units per simulated day
opening    = max(MIN_OPENING, round(OPENING_DAYS_COVER * d * profile_ratio(sku)))   # 0 for out-of-stock SKUs
```

   Zepto's *relative* stock profile is preserved, its absolute level is scaled to the simulated demand, and out-of-stock SKUs start at 0 so that stock-out alerts appear early. Only the `--active-skus N` most popular SKUs (default 300) receive demand and opening stock; the rest stay in the catalog only. With 4 hubs this gives about 1,200 keys, which keeps per-key windows populated at the default rate.
7. **Replenishment** (closed loop, using the simulated stock): reorder point `ROP = SAFETY_FACTOR * d * LEAD_TIME_DAYS_MAX`. When stock is at or below `ROP` and no order is open, order up to `ceil(OPENING_DAYS_COVER * d)`. It arrives after `Uniform(2, 7)` simulated days (`days * SIM_DAY_SECONDS` real seconds) as `INBOUND` (`zone=DOCK`, `source=SHAPER`). With probability `TRANSFER_PROB` (default 0.2), if another hub holds more than its order-up-to level of that SKU, move stock as a `TRANSFER` (two events) instead.
8. **Returns.** For each demand event, with probability `return_rate`, schedule an `INBOUND` (`zone=RETURNS`) for the same hub and SKU after `RETURN_DELAY_DAYS`, with quantity `min(original, sample from return_qty_table)`.
9. **Write-offs.** For each demand event, with probabilities `damage_rate` and `adjustment_rate` (both multiplied by `--writeoff-scale`, default 1), emit a `DAMAGE` or a negative `ADJUSTMENT`. Quantity is drawn from the UCI tables and capped at `WRITEOFF_MAX_FRACTION` of current simulated stock (at least 1). The real rates are tiny, so use `--writeoff-scale` to make them visible in a demo.
10. **Festive scenario:** `--festive-at S --festive-duration S`. It doubles as the burst and consumer-lag demo.
11. **Fault injection** (disabled by `--clean`). Every injected fault is recorded in the emitted-event log with its kind, for ground truth.
    - **Duplicates** (`--dup-rate`, default 0.01): re-emit the same event (same `event_id`) 1 to 3 times shortly after the original.
    - **Late events** (`--late-rate`, default 0.01): hold the event back and emit it after later events, keeping its `event_time`. Draw lateness from two bands relative to the watermark `W`: an **accepted** band `[0.1W, 0.8W]` and a **rejected** band `[1.5W, 3W]`, 50/50. Never use values near `W`, so ground truth is unambiguous. Add an `x-late-by-seconds` header.
    - **Malformed** (`--malformed-rate`, default 0.002): corrupt JSON, drop a required field, or give a non-`ADJUSTMENT` event a negative quantity. These must reach `inventory-dlq`.
12. **Publish** to `inventory-events` (`acks=all`, idempotent producer) and append to the emitted-event log.

All parameters are env vars with defaults in `config/settings.py` and `.env.example`; CLI flags override them. The shaper keeps in-memory state, so **every run is a new run**: start each demo with `make reset`.

### 5.6 Calibration and EDA deliverables

`tools/eda.py` (UCI) and `tools/calibrate.py` (all three datasets) write PNG charts and CSV/markdown tables to `docs/eda/`. Required outputs:

1. A description of each dataset: rows, columns, date range, missing values, license, provenance notes (including the India Retail Sales provenance indicators).
2. Zepto: category mix, price and discount distributions, available-quantity distribution, out-of-stock share, SKU count after deduplication.
3. India Retail Sales: city and region shares, channel mix, quantity distribution, the city-by-channel contingency table, monthly volume.
4. UCI: sale and return quantity percentiles, event mix, SKU rank-frequency with the fitted Zipf exponent, daily-outflow percentiles, write-off table, excluded stock codes (existing `eda.py` outputs).
5. The rendered parameter table from `params.json`, with the assumptions listed separately.
6. After a run: the fidelity report (section 11), events per Kafka partition (validates the key), lateness distribution, fault rates, stock trajectories for a few SKUs, alert counts by type.

Every chart title or caption states its provenance: `Zepto`, `India Retail Sales`, `UCI`, or `generated`. Numbers in docs and slides must come from these outputs, never typed by hand.

### 5.7 India locale

- **Warehouses** are regional hubs named after real cities (for example `WH-BLR`), derived from the calibrated hub list.
- **Currency:** rupees. `sku_catalog` prices are in rupees, and value panels (stock value = stock x selling price) use Grafana's INR unit.
- **Timezone:** timestamps are stored in UTC and Grafana dashboards display `Asia/Kolkata` (IST, no daylight saving).
- **Festive-sale scenario** (section 5.5.5) stands in for sale-season spikes. It is a labelled stress test.
- **Not modelled (future work):** GST and HSN codes, e-way bills, cash-on-delivery handling, return-to-origin (RTO) logistics, and store-as-warehouse fulfilment.

## 6. Processing semantics (the important part)

1. **Event time, not processing time.** Windows are assigned from `event_time`.
2. **Watermark** = max seen event_time minus `WATERMARK` (defaults in 6.1). A window emits when the watermark passes its end. Events later than the watermark are counted in `late_events` and sent to the DLQ with reason `LATE`.
3. **Tumbling window**: `TUMBLING_WINDOW` per `(warehouse_id, sku)`: inbound units, outbound units, event count.
4. **Sliding window**: `SLIDING_WINDOW` with slide `SLIDE`: moving average of outbound units per tumbling-window unit of time.
5. **Running stock** is keyed state updated per event.
6. **EMA** of demand: `EMA_t = alpha * x_t + (1 - alpha) * EMA_{t-1}`, default `alpha = 0.3`.
7. **Days-of-cover** = `current_stock / (EMA_rate_per_second * SIM_DAY_SECONDS)` (guard against divide-by-zero), expressed in simulated days. Producer and processor must read the same `SIM_DAY_SECONDS` from `.env`.
8. **Top-K SKUs** per window (exact counters by default; Count-Min Sketch as an optional extension).
9. **Dedup** by `event_id`, held for the watermark horizon.
10. **Alerts** (published to `inventory-alerts` and written to the `alerts` table):
    - `LOW_STOCK`: stock at or below `SAFETY_FACTOR * EMA_demand_per_sim_day * LEAD_TIME_DAYS_MAX` (the processor uses its own EMA and shares these env vars with the shaper)
    - `STOCKOUT_RISK`: days-of-cover below `COVER_ALERT_DAYS` (default 3 simulated days, an assumption)
    - `ABNORMAL_OUTFLOW`: window outflow above `ABNORMAL_OUTFLOW_MULT` (default 3.0) times the sliding average. The UCI reference shows that about 5.9% of key-days naturally exceed 3x their own mean, so a few of these alerts are expected even without a burst.
    - `STALE_DATA`: no events for N seconds (Grafana alert rule)
      Use a per-`(sku, alert_type)` cooldown to avoid alert floods.

### 6.1 Window defaults

| Setting                           | Default              |
| --------------------------------- | -------------------- |
| `WATERMARK`                       | 2 minutes            |
| `TUMBLING_WINDOW`                 | 1 minute             |
| `SLIDING_WINDOW` / `SLIDE`        | 5 minutes / 1 minute |
| `IDLE_FLUSH_SECONDS` (wall clock) | 30                   |

All are env vars. With `SIM_DAY_SECONDS=60`, one tumbling window equals one simulated day.

- The watermark is derived **only from event times**, never from the wall clock.
- **Idle flush:** if no event arrives for `IDLE_FLUSH_SECONDS` of wall-clock time, advance the watermark far enough to close every open window, so the final windows emit when a run ends.
- Because the shaper stamps monotonic event times, **all lateness comes from the injected late events**, which makes the late-data demo deterministic.

## 7. Fault tolerance rules (non-negotiable)

- Producers (shaper): `acks=all`, `enable.idempotence=true`, retries with backoff, delivery callback that logs failures, `flush()` on shutdown.
- Consumers: `enable.auto.commit=false`. For the processor, commit offsets **only after** results are durably written. Preferred pattern: write window results, state checkpoint, and offsets to Postgres in **one transaction**, then commit to Kafka. For the shaper, commit `gen-*` offsets only after the derived events are published.
- On startup the processor restores state and seeks to stored offsets. Implement `on_assign` / `on_revoke` rebalance callbacks (checkpoint on revoke).
- Malformed or schema-invalid messages go to `inventory-dlq`; **never crash the loop on a bad record**.
- Handle SIGTERM/SIGINT gracefully: stop polling, checkpoint, close the consumer.
- The shaper's simulated state is not checkpointed. Restarting it is a new run (`make reset`).
- Docker mode: services use `restart: unless-stopped` and health checks. Native mode: rely on the OS service manager (systemd, launchd, or a Windows service) and document how to check status.
- Kafka brokers are local processes, so `kafka-start.sh` must wait until the cluster is healthy (for example by polling `kafka-broker-api-versions.sh`) before returning. `connect-start.sh` must wait until the Connect REST API responds and the three connectors report `RUNNING`.

## 8. Build order

Work in phases. Each phase must run and be verified before starting the next. Commit after each.

1. **Environment check** (section 2.1): report versions, confirm OS, confirm WSL2 on Windows, confirm Java 17 or newer, locate the three datasets and the Datagen plugin.
2. **Kafka cluster**: write the three config files and scripts, format, start, create topics (including the `gen-*` topics). Verify with `kafka-topics.sh --describe` (replication 3, ISR 3) and a console producer/consumer round trip.
3. **Database and Grafana (both modes)**: `.env.example`, `docker-compose.yml`, the native setup scripts, `sql/init.sql`, and provisioning files that use `${VAR}` expansion. Verify with `make db-check` in each mode you can test, and confirm the Grafana datasource shows healthy.
4. **Models and config**: pydantic models, settings, `columns.yaml`, logging.
5. **Calibration**: move the UCI cleaning rules into `src/common/uci_clean.py`, then write `calibrate.py`. Run it on the three datasets, review `calibration_report.md` (especially the provenance indicators and the assumption table) with the human, then commit `params.json`.
6. **Datagen**: write `build_datagen.py`, install the plugin, start the Connect worker and the three connectors. Verify that each `gen-*` topic receives JSON tuples with the expected fields, and that tuple frequencies match `params.json` on a sample.
7. **Shaper**: `stock_model`, `replenishment`, `derived_events`, `faults`, pacing, truth log, and `--source local`. Unit-test the stateful parts without Kafka. Verify keyed events spread across partitions and the truth log is written to `data/truth/`.
8. **Basic consumer**: print events and partition assignment. Run 3 instances and show the split.
9. **Windows and aggregations**: pure functions first, with unit tests (no Kafka needed).
10. **Processor + sink**: wire everything together, with checkpointing, DLQ, alerts, idempotent upserts, and pipeline metrics.
11. **Second consumer group** (`audit-logger`) and `alert_listener`.
12. **Grafana**: provisioned datasource and dashboard JSON (IST timezone, INR panels).
13. **Failure demos** (section 10) and `fidelity.py` (section 11).
14. **Docs**: README (including dataset licenses and the assumption table), architecture, demo script, screenshots.

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
make eda              # UCI charts and tables into docs/eda/
make calibrate        # all three datasets -> config/params.json + docs/eda/calibration_report.md
make datagen-build    # params.json -> config/datagen/*
make connect-up       # start Connect worker + the three Datagen connectors
make connect-down     # stop them
make shaper           # run the shaper (RATE=50 SIM_DAY_SECONDS=60 ACTIVE_SKUS=300 SEED=42 ...)
make producer         # alias for make shaper
make simulate         # shaper with --source local (no Connect needed)
make processor        # run one processor instance (run in 3 terminals)
make audit            # run audit-logger group
make reset            # clean slate: truncate tables, clear checkpoints, recreate topics
make verify           # compare DB to ground truth
make fidelity         # generated vs source distributions and correlations
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
4. **Poison pill**: inject malformed JSON, verify it lands in `inventory-dlq` and the pipeline keeps running.
5. **Duplicate and late events**: inject duplicates (same `event_id`) and late events in both lateness bands (section 5.5.5). Verify dedup, that accepted-band late events land in the correct window, and that rejected-band events appear in `inventory-dlq` and the `late_events` metric.
6. **Database outage**: stop the database (`docker compose pause timescaledb` in Docker mode, or stop the PostgreSQL service in native mode, wrapped by `scripts/db-stop-start.sh`), verify the processor retries without committing offsets, then catches up after restart with no duplicates.
7. **Festive burst** (load test): run with `--festive-at` and watch consumer lag rise and recover on the dashboard.

## 11. Correctness checks

- **Ground truth.** The shaper writes an **emitted-event log** (including fault kinds) to `data/truth/`. Truth (final stock per `(warehouse, sku)`, per-window totals) is computed independently of the processor, from the log, using the section 6 rules: duplicates count once, and injected late events from the rejected band are treated as rejected. `tools/verify.py` compares DB contents to it after any demo. Expected result: **zero mismatches, zero duplicates**. Exact reproducibility of the generated stream is not required, because truth comes from the log.
- **Fidelity.** `tools/fidelity.py` compares the emitted log with `params.json` and reports, never tunes to pass: city, hub and channel shares; the city-by-channel contingency table; quantity percentiles per segment; return and write-off rates; the SKU rank-frequency curve. Default tolerances to start from: total variation distance of at most 0.02 for shares and contingency tables, Cramer's V within 0.05, quantity percentiles within 10%. Report the actual values and any misses.
- Metrics to record for the report: throughput (events/s), end-to-end latency (`ingest_ts` minus `event_time`), consumer lag under load, recovery time after each failure.
- Run a throughput benchmark with `--rate 0` (no pacing) and record the sustained events/s. Also run it once with producer `acks=1` and once with `acks=all`, and report the difference (a data point for the comparison with existing systems).

## 12. Code conventions

- Type hints everywhere; `ruff`, `black`, and `mypy` clean.
- No hard-coded hosts, ports, or credentials. Everything comes from env vars with defaults in `config/settings.py`, documented in `.env.example`.
- Keep window, aggregation, stock-model and sampling logic **pure** (no Kafka or DB calls) so it is unit-testable.
- Structured logging with partition, offset, and sku in context where relevant.
- Small, focused commits with descriptive messages (`feat:`, `fix:`, `docs:`, `test:`).
- Pin Python dependency versions and Docker image tags (no `latest`).
- Use `zlib.crc32` (or another stable hash) for zones and any bucketing. Never use Python's built-in `hash()`: it is salted per process.
- All randomness comes from a seeded RNG (`--seed`, default 42).

## 13. Do and don't

**Do**

- Ask the human before changing a decision in section 2.
- Run the code you write (start the cluster, Connect, shaper, processor) and report actual output rather than assuming it works.
- Look up Datagen's connector configuration keys and the Avro Random Generator syntax in the installed version's documentation before using them.
- Keep the README demo path runnable in under 15 minutes on a laptop.
- Verify both `DEPLOY_MODE=native` and `DEPLOY_MODE=docker`. If you can only test one in the current environment, say which one is untested.
- Note any deviation from this file in `docs/architecture.md`.
- Report every dataset statistic (row counts, shares, rates) from an actual run. Never write placeholder numbers into docs or slides.
- Label provenance everywhere: parameters are estimated from three public datasets, the stream is **synthetic**, and the assumptions in section 5.5.2 are assumptions.

**Don't**

- Don't use auto-commit offsets.
- Don't publish messages without a key.
- Don't run Kafka or Connect in Docker (Kafka is already installed locally).
- Don't write code that depends on `DEPLOY_MODE`; only env vars, scripts, and compose may differ.
- Don't hard-code `localhost` or container hostnames inside Grafana provisioning files; use the `TSDB_*` variables.
- Don't write Java application code; Java is only the Kafka and Connect runtime here.
- Don't commit secrets, `.env`, `kafka-data/`, `plugins/`, volume data, or the datasets themselves.
- Don't guess dataset columns or fabricate data when a dataset is missing; ask the human.
- Don't link datasets to each other without data (Zepto categories to India Retail categories, SKUs to cities). Record an independence assumption instead.
- Don't call the generated data "real" or "a replay". Don't call India Retail Sales "real Indian demand" without checking its provenance.
- Don't stamp or rewrite `event_time` in the consumer or processor; the shaper stamps it once.
- Don't use wall-clock time for windows or watermarks; only event time (the idle flush is the single, documented exception).
- Don't add Confluent components other than the Datagen plugin (no Schema Registry, ksqlDB, Control Center).
- Don't build a React UI unless asked.
- Don't mark a phase done without running its verification step.

## 14. Definition of done

- `make kafka-up && make topics && make db-up && make connect-up && make shaper && make processor` yields a live Grafana dashboard with no manual setup, in **either** `DEPLOY_MODE`. `make simulate` also works without Connect.
- `make db-check` passes in both modes, and the README documents both setups.
- All nine checklist items in section 1 are demonstrably satisfied and referenced in the README.
- All seven failure demos run and are documented.
- `make test` and `make lint` pass.
- `make verify` reports zero mismatches after each demo, and `make fidelity` reports its actual deviations.
- In the default setup, Grafana panels with a "last 5 minutes" range show live data, with IST timestamps and rupee values.
- `docs/eda/` contains every output listed in section 5.6, generated from real runs.
- The README cites all three datasets with their licenses, states clearly that the stream is synthetic, and lists every assumption from section 5.5.2.