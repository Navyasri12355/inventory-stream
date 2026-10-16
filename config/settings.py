"""
config/settings.py
──────────────────
Central configuration: every env var used by the pipeline is read here with
a documented default.  No hard-coded hosts, ports, or credentials anywhere else.

Usage:
    from config.settings import settings
    print(settings.kafka_bootstrap)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv  # python-dotenv

# Load .env from the repository root (two levels up from this file)
_repo_root = Path(__file__).resolve().parent.parent
load_dotenv(_repo_root / ".env", override=False)


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


def _env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))


class Settings:
    # ── Kafka ──────────────────────────────────────────────────────────────────
    kafka_home: str      = _env("KAFKA_HOME", r"C:\kafka")
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")

    topic_events: str = "inventory-events"
    topic_alerts: str = "inventory-alerts"
    topic_dlq:    str = "inventory-dlq"

    # ── Database ───────────────────────────────────────────────────────────────
    db_host:     str = _env("DB_HOST",     "localhost")
    db_port:     int = _env_int("DB_PORT", 5432)
    db_name:     str = _env("DB_NAME",     "inventory")
    db_user:     str = _env("DB_USER",     "inventory")
    db_password: str = _env("DB_PASSWORD", "inventorydev")

    @property
    def db_dsn(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} "
            f"dbname={self.db_name} user={self.db_user} "
            f"password={self.db_password}"
        )

    # ── Grafana ────────────────────────────────────────────────────────────────
    grafana_port: int = _env_int("GRAFANA_PORT", 3000)

    # ── Deployment ─────────────────────────────────────────────────────────────
    deploy_mode: Literal["native", "docker"] = _env("DEPLOY_MODE", "native")  # type: ignore[assignment]

    # ── Replay / producer ──────────────────────────────────────────────────────
    speedup:          float = _env_float("SPEEDUP", 600.0)
    time_mode:        str   = _env("TIME_MODE", "rebased")   # rebased | original
    top_skus:         int   = _env_int("TOP_SKUS", 500)
    seed:             int   = _env_int("SEED", 42)
    max_gap_seconds:  float = _env_float("MAX_GAP_SECONDS", 1.0)
    warehouses:       list[str] = _env("WAREHOUSES", "WH-01,WH-02,WH-03").split(",")
    warehouse_strategy: str  = _env("WAREHOUSE_STRATEGY", "hash")  # hash | country-group

    # ── Replenishment ──────────────────────────────────────────────────────────
    opening_days_cover:  float = _env_float("OPENING_DAYS_COVER", 30.0)
    min_opening:         int   = _env_int("MIN_OPENING", 20)
    safety_factor:       float = _env_float("SAFETY_FACTOR", 1.2)
    lead_time_days_min:  int   = _env_int("LEAD_TIME_DAYS_MIN", 2)
    lead_time_days_max:  int   = _env_int("LEAD_TIME_DAYS_MAX", 7)
    transfer_prob:       float = _env_float("TRANSFER_PROB", 0.2)
    adjust_rate:         float = _env_float("ADJUST_RATE", 0.0)
    damage_rate:         float = _env_float("DAMAGE_RATE", 0.0)

    # ── Fault injection ────────────────────────────────────────────────────────
    dup_rate:       float = _env_float("DUP_RATE", 0.01)
    late_rate:      float = _env_float("LATE_RATE", 0.01)
    malformed_rate: float = _env_float("MALFORMED_RATE", 0.002)
    burst_factor:   float = _env_float("BURST_FACTOR", 1.0)

    # ── Processing windows ─────────────────────────────────────────────────────
    window_preset:           str   = _env("WINDOW_PRESET", "live")  # live | historical
    watermark_seconds:       int   = _env_int("WATERMARK_SECONDS", 120)
    tumbling_window_seconds: int   = _env_int("TUMBLING_WINDOW_SECONDS", 60)
    sliding_window_seconds:  int   = _env_int("SLIDING_WINDOW_SECONDS", 300)
    slide_seconds:           int   = _env_int("SLIDE_SECONDS", 60)
    idle_flush_seconds:      int   = _env_int("IDLE_FLUSH_SECONDS", 30)
    ema_alpha:               float = _env_float("EMA_ALPHA", 0.3)

    # ── Data paths ─────────────────────────────────────────────────────────────
    data_dir:      Path = _repo_root / "data"
    raw_csv:       Path = _repo_root / "data" / "raw" / "online_retail_ii.csv"
    truth_dir:     Path = _repo_root / "data" / "truth"
    docs_eda_dir:  Path = _repo_root / "docs" / "eda"

    # Seconds per data-day used for days-of-cover calculation (§6 rule 7)
    @property
    def seconds_per_data_day(self) -> float:
        if self.time_mode == "rebased" and self.speedup > 0:
            return 86400.0 / self.speedup
        return 86400.0


settings = Settings()
