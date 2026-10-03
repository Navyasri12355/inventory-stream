# AGENTS.md

Guidance for AI coding agents (and humans) building this project from an empty repository.

## 1. Project summary

**Real-Time Warehouse Inventory Event Analytics** is an end-to-end streaming pipeline:

```
Event Simulator (producer) -> Apache Kafka 3.9.1 -> Stream Processor -> TimescaleDB -> Grafana (dashboard + alerts)
```

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

| Concern | Decision |
|---|---|
| Broker | **Locally installed Apache Kafka 3.9.1**, **KRaft mode** (no ZooKeeper). Location comes from the `KAFKA_HOME` env var |
| Cluster shape | **3 brokers on one machine** (combined broker+controller nodes), each with its own config file, port, and log dir |
| Java | Needed **only to run Kafka itself**. No Java application code in this project |
| Language | Python 3.11+ |
| Kafka client | `confluent-kafka` (a client library only; it talks to plain Apache Kafka) |
| Stream processing | **Custom Python processor** on `confluent-kafka` (explicit windowing, state, checkpointing). PySpark Structured Streaming is the only approved alternative |
| Serialisation | JSON validated with `pydantic`. No Schema Registry (Confluent product, not Apache Kafka) |
| Storage | TimescaleDB (PostgreSQL + extension) via `psycopg[binary]`. Run **natively or in Docker** (section 2.2) |
| Dashboard / alerts | Grafana, provisioned from files (no manual clicking). Run **natively or in Docker** (section 2.2) |
| Docker | **Optional**, supported as an alternative way to run TimescaleDB and Grafana. Kafka itself is NOT run in Docker, and no Kafka UI container is used (the brokers advertise `localhost`, which a container could not reach). Use the Kafka CLI tools and Grafana for visibility |
| React dashboard | **Out of scope** unless the human asks |

### 2.1 First step for the agent: detect the OS and tooling

Before generating any scripts, check the operating system and confirm with the human.

- **Linux / macOS**: provide a `Makefile` and `scripts/*.sh`, using `$KAFKA_HOME/bin/*.sh`.
- **Windows**: strongly recommend running everything inside **WSL2**. Native Windows Kafka is known to crash on log-segment deletion due to file locking, which would sabotage the recovery demos. If the human insists on native Windows, provide PowerShell scripts (`scripts/*.ps1`) using `%KAFKA_HOME%\bin\windows\*.bat`.
- Verify prerequisites and report versions: `java -version`, `kafka-topics.sh --version` (expect 3.9.1), `python --version`.
- Detect what is available for the database and dashboard: `psql --version` and whether the `timescaledb` extension is installed, `grafana-server --version` (or `grafana server --version`), and `docker --version` / `docker compose version`.
- Recommend a `DEPLOY_MODE` (section 2.2): `native` if PostgreSQL with TimescaleDB and Grafana are already installed, otherwise `docker`. Confirm with the human and record it in `.env`.

### 2.2 TimescaleDB and Grafana: native or Docker (both must work)

Support **both** deployment modes from the start. Application code must be mode-agnostic: only environment variables differ. Selection is `DEPLOY_MODE=native|docker` in `.env`, and `make db-up` / `make db-down` dispatch on it.

**Shared environment variables** (defined in `.env.example`):

| Variable | Default | Used by |
|---|---|---|
| `DB_HOST` | `localhost` | processor, tools (always reaches the DB from the host) |
| `DB_PORT` | `5432` | processor, tools; Docker publishes the container on this host port |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `inventory` / `inventory` / dev value | everything |
| `GRAFANA_PORT` | `3000` | Grafana (either mode) |
| `TSDB_HOST` / `TSDB_PORT` | `localhost` / `DB_PORT` (native); `timescaledb` / `5432` (Docker, set in compose) | Grafana datasource provisioning only |
| `GRAFANA_DASHBOARD_PATH` | repo `grafana/dashboards` (native); `/var/lib/grafana/dashboards` (Docker) | Grafana dashboard provisioning only |

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
├── pyproject.toml / requirements.txt
├── Makefile                   # (or scripts/*.ps1 on native Windows)
├── config/
│   ├── settings.py            # all app config via env vars with defaults
│   └── kafka/
│       ├── server-1.properties
│       ├── server-2.properties
│       └── server-3.properties
├── scripts/
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
│   │   └── simulator.py       # event generator (rate, burst, duplicates, late, malformed)
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
│       ├── verify.py          # compare DB to simulator ground truth
│       └── chaos.py           # helpers for failure demos
├── data/                      # ground-truth output (gitignored)
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
    ├── setup-native.md
    ├── setup-docker.md
    └── demo-script.md
```

Add `kafka-data/`, `grafana-data/`, `data/`, `.env`, and `*.pid` to `.gitignore`.

## 4. Local Kafka cluster setup (3 brokers, KRaft, v3.9.1)

Each broker is a combined `broker,controller` node. Per-broker values:

| Setting | Broker 1 | Broker 2 | Broker 3 |
|---|---|---|---|
| `node.id` | 1 | 2 | 3 |
| client listener port | 9092 | 9094 | 9096 |
| controller listener port | 9093 | 9095 | 9097 |
| `log.dirs` | `./kafka-data/broker-1` | `./kafka-data/broker-2` | `./kafka-data/broker-3` |

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
  "event_id": "uuid4 string",
  "event_time": "ISO-8601 UTC, e.g. 2026-09-30T10:15:32Z",
  "warehouse_id": "WH-BLR-01",
  "sku": "SKU-10452",
  "event_type": "INBOUND | OUTBOUND | TRANSFER | ADJUSTMENT | DAMAGE",
  "quantity": 24,
  "zone": "A3"
}
```

Rules:
- `quantity` is a positive integer, except `ADJUSTMENT`, which may be signed.
- Stock effect: `INBOUND +q`, `OUTBOUND -q`, `DAMAGE -q`, `ADJUSTMENT +/-q`. `TRANSFER` is modelled as an outbound at the source plus an inbound at the destination (via a `dest_warehouse_id` field). Document the choice.
- **Message key = `"{warehouse_id}:{sku}"`**. Never publish without a key.

### 5.2 Topics

| Topic | Partitions | Replication | Notes |
|---|---|---|---|
| `inventory-events` | 6 | 3 | raw events |
| `inventory-alerts` | 3 | 3 | key = sku |
| `inventory-dlq` | 1 | 3 | original payload plus `error`, `source_topic`, `source_partition`, `source_offset` in headers |

`create-topics.sh` must be idempotent (`--if-not-exists`) and set `min.insync.replicas=2`.

### 5.3 Consumer groups

- `inventory-processor`: 3 instances, shares the 6 partitions
- `audit-logger`: 1 instance, independent group on the same topic (shows fan-out)
- `alert-listener`: reads `inventory-alerts`

### 5.4 Database tables (TimescaleDB hypertables)

- `stock_levels(ts, warehouse_id, sku, stock)` PK `(ts, warehouse_id, sku)`
- `window_metrics(window_start, window_end, window_type, warehouse_id, sku, inbound, outbound, avg_demand)` PK `(window_start, window_type, warehouse_id, sku)`
- `alerts(ts, warehouse_id, sku, alert_type, severity, message)`
- `pipeline_metrics(ts, instance_id, partition, consumer_lag, events_per_sec, late_events, dlq_count)` so Grafana can show lag and health without extra tooling
- `consumer_offsets(group_id, topic, partition, committed_offset)` for atomic offset + result commits

All writes use `INSERT ... ON CONFLICT DO UPDATE` so replays never create duplicates.

## 6. Processing semantics (the important part)

1. **Event time, not processing time.** Windows are assigned from `event_time`.
2. **Watermark** = max seen event_time minus 2 minutes (configurable). A window emits when the watermark passes its end. Events later than the watermark are counted in `late_events` and sent to the DLQ with reason `LATE`.
3. **Tumbling window**: 1 minute per `(warehouse_id, sku)`: inbound units, outbound units, event count.
4. **Sliding window**: 5 minutes, slide 1 minute: moving average of outbound units per minute.
5. **Running stock** is keyed state updated per event.
6. **EMA** of demand: `EMA_t = alpha * x_t + (1 - alpha) * EMA_{t-1}`, default `alpha = 0.3`.
7. **Days-of-cover** = `current_stock / EMA_demand_rate` (guard against divide-by-zero).
8. **Top-K SKUs** per window (exact counters by default; Count-Min Sketch as an optional extension).
9. **Dedup** by `event_id`, held for the watermark horizon.
10. **Alerts** (published to `inventory-alerts` and written to the `alerts` table):
    - `LOW_STOCK`: stock below reorder point
    - `STOCKOUT_RISK`: days-of-cover below threshold
    - `ABNORMAL_OUTFLOW`: window outflow above 3x the sliding average
    - `STALE_DATA`: no events for N seconds (Grafana alert rule)
    Use a per-`(sku, alert_type)` cooldown to avoid alert floods.

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
5. **Producer**: simulator with CLI flags (`--rate`, `--skus`, `--warehouses`, `--burst`, `--dup-rate`, `--late-rate`, `--malformed-rate`). Verify keyed events spread across partitions and write the ground-truth file.
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
make producer         # run simulator (RATE=200 etc.)
make processor        # run one processor instance (run in 3 terminals)
make audit            # run audit-logger group
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
5. **Duplicate and late events**: verify dedup and watermark behaviour.
6. **Database outage**: stop the database (`docker compose pause timescaledb` in Docker mode, or stop the PostgreSQL service in native mode, wrapped by `scripts/db-stop-start.sh`), verify the processor retries without committing offsets, then catches up after restart with no duplicates.

## 11. Correctness checks

- The simulator writes a **ground-truth** file (final stock per SKU, per-window totals) to `data/`. `tools/verify.py` compares DB contents to it after any demo. Expected result: **zero mismatches, zero duplicates**.
- Metrics to record for the report: throughput (events/s), end-to-end latency (event_time to DB write), consumer lag under load, recovery time after each failure.

## 12. Code conventions

- Type hints everywhere; `ruff`, `black`, and `mypy` clean.
- No hard-coded hosts, ports, or credentials. Everything comes from env vars with defaults in `config/settings.py`, documented in `.env.example`.
- Keep window and aggregation logic **pure** (no Kafka or DB calls) so it is unit-testable.
- Structured logging with partition, offset, and sku in context where relevant.
- Small, focused commits with descriptive messages (`feat:`, `fix:`, `docs:`, `test:`).
- Pin Python dependency versions and Docker image tags (no `latest`).

## 13. Do and don't

**Do**
- Ask the human before changing a decision in section 2.
- Run the code you write (start the cluster, producer, processor) and report actual output rather than assuming it works.
- Keep the README demo path runnable in under 15 minutes on a laptop.
- Verify both `DEPLOY_MODE=native` and `DEPLOY_MODE=docker`. If you can only test one in the current environment, say which one is untested.
- Note any deviation from this file in `docs/architecture.md`.

**Don't**
- Don't use auto-commit offsets.
- Don't publish messages without a key.
- Don't run Kafka in Docker (it is already installed locally).
- Don't write code that depends on `DEPLOY_MODE`; only env vars, scripts, and compose may differ.
- Don't hard-code `localhost` or container hostnames inside Grafana provisioning files; use the `TSDB_*` variables.
- Don't write Java application code; Java is only the Kafka runtime here.
- Don't commit secrets, `.env`, `kafka-data/`, or volume data.
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
