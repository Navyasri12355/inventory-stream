"""
src/producer/mapping.py
────────────────────────
Maps a cleaned UCI Online Retail II row to an InventoryEvent.

Cleaning rules (AGENTS.md §5.5):
  1. Strip + upper-case StockCode, keep only product codes: ^\\d{5}[A-Za-z]*$
  2. Drop Quantity == 0 or UnitPrice < 0. UnitPrice == 0 is KEPT (write-offs, rule 4).
  3. Keep rows with missing CustomerID (fall back to Invoice for warehouse hash)
  4. Keep only rows mapping to one of: sale, return, damage write-off, adjustment write-off
  5. Keep only top --top-skus SKUs by remaining row count

Field mapping → InventoryEvent (AGENTS.md §5.5 table):
  Quantity > 0, UnitPrice > 0, non-cancellation invoice → OUTBOUND  (sale)
  Quantity < 0, UnitPrice > 0, cancellation invoice    → INBOUND    (return, zone=RETURNS)
  Quantity < 0, UnitPrice == 0, non-cancellation,
      Description matches DAMAGE_KEYWORDS               → DAMAGE     (write-off)
  Quantity < 0, UnitPrice == 0, non-cancellation,
      any other Description                             → ADJUSTMENT (signed qty)

Warehouse assignment: crc32(str(CustomerID or Invoice)) % len(WAREHOUSES)
Zone assignment:      Z{crc32(sku) % 8} (stable per SKU; RETURNS for returns)
event_id:             uuid5(NAMESPACE, f"{Invoice}|{row_number}")

Usage:
    from src.producer.mapping import DataCleaner, DAMAGE_KEYWORDS
    cleaner = DataCleaner(settings)
    df_clean = cleaner.load_and_clean()
"""
from __future__ import annotations

import re
import uuid
import zlib
from datetime import datetime, timezone
from typing import Iterator

import pandas as pd

from config.settings import settings as _default_settings, Settings
from src.common.logging_setup import get_logger
from src.common.models import EventSource, EventType, InventoryEvent

log = get_logger(__name__)

# UUID namespace for stable event_id generation (uuid5)
_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # URL namespace

# StockCode pattern: exactly 5 digits optionally followed by letters
_PRODUCT_CODE_RE = re.compile(r"^\d{5}[A-Za-z]*$")

# ── Damage write-off keywords (AGENTS.md §5.5) ────────────────────────────────
# Matched case-insensitively as substrings of Description.
# Keep this list here; tools/eda.py imports it from this module.
DAMAGE_KEYWORDS: tuple[str, ...] = (
    "damage", "smash", "broken", "destroy", "thrown",
    "unsaleable", "faulty", "crushed", "wet", "mouldy", "rust",
)

# Pre-compiled pattern for fast vectorised match
_DAMAGE_RE = re.compile(
    "|".join(re.escape(kw) for kw in DAMAGE_KEYWORDS),
    re.IGNORECASE,
)


def _crc32(s: str) -> int:
    """Stable unsigned CRC32. Never use Python's built-in hash()."""
    return zlib.crc32(s.encode()) & 0xFFFFFFFF


def _warehouse_id(customer_id: object, invoice: str, warehouses: list[str]) -> str:
    key = str(int(float(customer_id))) if pd.notna(customer_id) and customer_id else invoice
    return warehouses[_crc32(key) % len(warehouses)]


def _zone(sku: str) -> str:
    return f"Z{_crc32(sku) % 8}"


def _event_id(invoice: str, row_number: int) -> str:
    return str(uuid.uuid5(_NAMESPACE, f"{invoice}|{row_number}"))


def _is_damage_writeoff(description: object) -> bool:
    """Return True if the Description matches a damage keyword."""
    if not description or not isinstance(description, str):
        return False
    return bool(_DAMAGE_RE.search(description))


class DataCleaner:
    """
    Loads, cleans, and maps the UCI CSV to InventoryEvents.
    Logs the row count removed at each cleaning step (AGENTS.md requirement).
    """

    def __init__(self, cfg: Settings | None = None) -> None:
        self.cfg = cfg or _default_settings

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def load_and_clean(self) -> pd.DataFrame:
        """Load raw CSV, apply rules 1-4, log counts, return cleaned DataFrame."""
        log.info("loading CSV", path=str(self.cfg.raw_csv))
        df = pd.read_csv(
            self.cfg.raw_csv,
            dtype={"Invoice": str, "StockCode": str, "CustomerID": str},
            parse_dates=["InvoiceDate"],
        )
        # Normalise CustomerID: keep string or None
        df["CustomerID"] = df["CustomerID"].where(df["CustomerID"].notna(), other=None)
        n_raw = len(df)
        log.info("rows in CSV", count=n_raw)

        # Rule 1: strip + upper-case StockCode; keep only product codes
        df["StockCode"] = df["StockCode"].str.strip().str.upper()
        mask1 = df["StockCode"].str.match(_PRODUCT_CODE_RE, na=False)
        removed1 = (~mask1).sum()
        df = df[mask1].copy()
        log.info("rule 1 (product codes)", removed=int(removed1), remaining=len(df))

        # Rule 2: drop Quantity == 0 or UnitPrice < 0  (UnitPrice == 0 is KEPT)
        mask2 = (df["Quantity"] != 0) & (df["UnitPrice"] >= 0)
        removed2 = (~mask2).sum()
        df = df[mask2].copy()
        log.info("rule 2 (qty/price)", removed=int(removed2), remaining=len(df))

        # Rule 3: keep rows with missing CustomerID (no removal)
        log.info("rule 3 (missing CustomerID kept)", removed=0, remaining=len(df))

        # Rule 4: keep only the four recognised row types; count and drop the rest
        is_cancel = df["Invoice"].str.startswith("C", na=False)
        # Sale: non-cancellation, Qty > 0, UnitPrice > 0
        is_sale      = ~is_cancel & (df["Quantity"] > 0) & (df["UnitPrice"] > 0)
        # Return: cancellation, Qty < 0, UnitPrice > 0
        is_return    = is_cancel  & (df["Quantity"] < 0) & (df["UnitPrice"] > 0)
        # Write-off: non-cancellation, Qty < 0, UnitPrice == 0
        is_writeoff  = ~is_cancel & (df["Quantity"] < 0) & (df["UnitPrice"] == 0)

        mask4 = is_sale | is_return | is_writeoff
        removed4 = (~mask4).sum()
        df = df[mask4].copy()
        log.info(
            "rule 4 (sale | return | write-off)",
            removed=int(removed4), remaining=len(df),
            sales=int(is_sale[mask4].sum()),
            returns=int(is_return[mask4].sum()),
            writeoffs=int(is_writeoff[mask4].sum()),
        )

        # Tag row types for iter_events()
        df["_row_type"] = "sale"
        df.loc[df["Invoice"].str.startswith("C", na=False), "_row_type"] = "return"
        writeoff_mask = ~df["Invoice"].str.startswith("C", na=False) & (df["Quantity"] < 0)
        damage_desc   = df["Description"].apply(_is_damage_writeoff)
        df.loc[writeoff_mask &  damage_desc, "_row_type"] = "damage"
        df.loc[writeoff_mask & ~damage_desc, "_row_type"] = "adjustment"

        return df

    def filter_top_skus(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rule 5: keep only the top N SKUs by row count."""
        sku_counts = df["StockCode"].value_counts()
        top_skus   = sku_counts.head(self.cfg.top_skus).index
        df_top     = df[df["StockCode"].isin(top_skus)].copy()
        removed    = len(df) - len(df_top)
        log.info(
            "rule 5 (top SKUs)", top_n=self.cfg.top_skus,
            removed=removed, remaining=len(df_top),
        )
        # Log write-off counts after filter for transparency
        wo = df_top[df_top["_row_type"].isin(["damage", "adjustment"])]
        log.info(
            "write-offs after top-sku filter",
            damage=int((df_top["_row_type"] == "damage").sum()),
            adjustment=int((df_top["_row_type"] == "adjustment").sum()),
            writeoff_rows=len(wo),
        )
        return df_top

    def iter_events(
        self,
        df: pd.DataFrame,
        replay_start: datetime | None = None,
        speedup: float | None = None,
        max_gap_seconds: float | None = None,
        max_events: int | None = None,
    ) -> Iterator[tuple[datetime, InventoryEvent]]:
        """
        Yield (scheduled_event_time, InventoryEvent) in original-time order.

        Replay clock (AGENTS.md §5.5):
          offset accumulates min(gap / SPEEDUP, MAX_GAP_SECONDS) per event.
          event_time = replay_start + offset  (monotone, deterministic).

        max_events: stop after emitting this many events (default: unlimited).
        """
        spd       = speedup         or self.cfg.speedup
        max_gap   = max_gap_seconds or self.cfg.max_gap_seconds
        r_start   = replay_start    or datetime.now(timezone.utc).replace(microsecond=0)
        mode      = self.cfg.time_mode
        whs       = self.cfg.warehouses

        offset_seconds: float = 0.0
        prev_orig: datetime | None = None
        emitted = 0

        for row_number, row in df.iterrows():
            if max_events is not None and emitted >= max_events:
                break

            orig: datetime = row["InvoiceDate"]
            if orig.tzinfo is None:
                orig = orig.replace(tzinfo=timezone.utc)

            # Compute rebased time
            if prev_orig is not None and mode == "rebased" and spd > 0:
                gap = (orig - prev_orig).total_seconds()
                offset_seconds += min(gap / spd, max_gap)
            prev_orig = orig

            if mode == "rebased":
                ev_time = datetime.fromtimestamp(
                    r_start.timestamp() + offset_seconds, tz=timezone.utc
                )
            else:
                ev_time = orig

            sku      = row["StockCode"]
            row_type = row["_row_type"]   # sale | return | damage | adjustment
            raw_qty  = int(row["Quantity"])

            # Map row type to event
            if row_type == "sale":
                event_type = EventType.OUTBOUND
                quantity   = abs(raw_qty)
                zone       = _zone(sku)
            elif row_type == "return":
                event_type = EventType.INBOUND
                quantity   = abs(raw_qty)
                zone       = "RETURNS"
            elif row_type == "damage":
                event_type = EventType.DAMAGE
                quantity   = abs(raw_qty)
                zone       = _zone(sku)   # write-offs use SKU zone
            else:  # adjustment
                event_type = EventType.ADJUSTMENT
                quantity   = raw_qty       # signed (always negative here)
                zone       = _zone(sku)

            event = InventoryEvent(
                event_id=_event_id(row["Invoice"], int(row_number)),
                event_time=ev_time,
                warehouse_id=_warehouse_id(row.get("CustomerID"), row["Invoice"], whs),
                sku=sku,
                event_type=event_type,
                quantity=quantity,
                zone=zone,
                source=EventSource.UCI_REPLAY,
                original_time=orig,
            )
            yield ev_time, event
            emitted += 1
