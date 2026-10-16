-- sql/init.sql
-- Idempotent schema for the inventory streaming pipeline.
-- Applied automatically on first Docker start (via docker-entrypoint-initdb.d)
-- or manually via scripts/db-init.ps1 / scripts/db-native-setup.ps1.
--
-- All event-time columns use timestamptz.
-- ingest_ts = wall-clock write time (refreshed on every upsert), indexed.
-- Hypertables partition on the event-time column, 1-day chunks.
-- All writes use INSERT ... ON CONFLICT DO UPDATE (idempotent / replay-safe).

-- ─────────────────────────────────────────────────────────────────────────────
-- Enable extension (no-op if already enabled)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─────────────────────────────────────────────────────────────────────────────
-- sku_catalog  (regular table, loaded once from the dataset)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sku_catalog (
    sku         TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT ''
);

-- ─────────────────────────────────────────────────────────────────────────────
-- stock_levels  (hypertable — partitioned by ts, event time)
-- PK: (ts, warehouse_id, sku)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stock_levels (
    ts           TIMESTAMPTZ     NOT NULL,
    warehouse_id TEXT            NOT NULL,
    sku          TEXT            NOT NULL,
    stock        DOUBLE PRECISION NOT NULL,
    ingest_ts    TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (ts, warehouse_id, sku)
);

SELECT create_hypertable(
    'stock_levels', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_stock_levels_ingest ON stock_levels (ingest_ts DESC);
CREATE INDEX IF NOT EXISTS idx_stock_levels_wh_sku ON stock_levels (warehouse_id, sku, ts DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- window_metrics  (hypertable — partitioned by window_start, event time)
-- PK: (window_start, window_type, warehouse_id, sku)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS window_metrics (
    window_start TIMESTAMPTZ     NOT NULL,
    window_end   TIMESTAMPTZ     NOT NULL,
    window_type  TEXT            NOT NULL,   -- 'tumbling' | 'sliding'
    warehouse_id TEXT            NOT NULL,
    sku          TEXT            NOT NULL,
    inbound      BIGINT          NOT NULL DEFAULT 0,
    outbound     BIGINT          NOT NULL DEFAULT 0,
    event_count  BIGINT          NOT NULL DEFAULT 0,
    avg_demand   DOUBLE PRECISION NOT NULL DEFAULT 0,
    ingest_ts    TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_type, warehouse_id, sku)
);

SELECT create_hypertable(
    'window_metrics', 'window_start',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_wm_ingest   ON window_metrics (ingest_ts DESC);
CREATE INDEX IF NOT EXISTS idx_wm_wh_sku   ON window_metrics (warehouse_id, sku, window_start DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- alerts  (hypertable — partitioned by ts, event time)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    ts           TIMESTAMPTZ NOT NULL,
    warehouse_id TEXT        NOT NULL,
    sku          TEXT        NOT NULL,
    alert_type   TEXT        NOT NULL,  -- LOW_STOCK | STOCKOUT_RISK | ABNORMAL_OUTFLOW | STALE_DATA
    severity     TEXT        NOT NULL,  -- info | warning | critical
    message      TEXT        NOT NULL DEFAULT '',
    ingest_ts    TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable(
    'alerts', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_alerts_ingest   ON alerts (ingest_ts DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_wh_sku   ON alerts (warehouse_id, sku, ts DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_type     ON alerts (alert_type, ts DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- pipeline_metrics  (hypertable — processor health, partitioned by ts)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    ts              TIMESTAMPTZ NOT NULL,
    instance_id     TEXT        NOT NULL,
    partition       INTEGER     NOT NULL,
    consumer_lag    BIGINT      NOT NULL DEFAULT 0,
    events_per_sec  DOUBLE PRECISION NOT NULL DEFAULT 0,
    late_events     BIGINT      NOT NULL DEFAULT 0,
    dlq_count       BIGINT      NOT NULL DEFAULT 0,
    ingest_ts       TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable(
    'pipeline_metrics', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_pm_instance ON pipeline_metrics (instance_id, ts DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- consumer_offsets  (regular table — atomic offset + result commits)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS consumer_offsets (
    group_id         TEXT    NOT NULL,
    topic            TEXT    NOT NULL,
    partition        INTEGER NOT NULL,
    committed_offset BIGINT  NOT NULL,
    PRIMARY KEY (group_id, topic, partition)
);
