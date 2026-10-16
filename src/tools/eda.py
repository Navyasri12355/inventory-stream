#!/usr/bin/env python3
"""EDA and dataset description for the UCI Online Retail II replay source.

Reads the cleaned CSV produced by scripts/xlsx_to_csv.py, applies the same
cleaning rules as the replay producer (AGENTS.md section 5.5), and writes
charts, tables and a data-driven dataset description to docs/eda/.

Usage (from the repo root):
    pip install pandas numpy matplotlib
    python src/tools/eda.py
    python src/tools/eda.py --input data/raw/online_retail_ii.csv --out docs/eda --top-skus 500

Every number in docs/eda/dataset_description.md is computed from the input
file, so re-run this script instead of editing numbers by hand.
"""
from __future__ import annotations

import argparse
import json
import zlib
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

# Import DAMAGE_KEYWORDS from mapping.py — single source of truth (AGENTS.md §5.5)
try:
    from src.producer.mapping import DAMAGE_KEYWORDS
except ImportError:
    # Fallback if run outside the package (e.g. directly from repo root)
    DAMAGE_KEYWORDS = (
        "damage", "smash", "broken", "destroy", "thrown",
        "unsaleable", "faulty", "crushed", "wet", "mouldy", "rust",
    )
DAMAGE_WORDS = DAMAGE_KEYWORDS  # alias for existing code
REQUIRED_COLUMNS = {
    "Invoice", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country",
}
NOTE_REAL = "Source: UCI Online Retail II (real demand, to be replayed). Cleaned per AGENTS.md 5.5."
NOTE_DERIVED = (
    "Source: UCI Online Retail II. Warehouse and partition assignment is DERIVED by the "
    "project's hashing rule (not real data)."
)
COLORS = {"main": "#2f6f9f", "alt": "#d9822b", "grey": "#8a8a8a", "bad": "#b5473a"}


# --------------------------------------------------------------------------- #
# Loading and cleaning
# --------------------------------------------------------------------------- #
def load(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        dtype={"Invoice": str, "StockCode": str},
        parse_dates=["InvoiceDate"],
    )
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SystemExit(f"Input is missing columns: {sorted(missing)}")
    return df


def clean(df: pd.DataFrame, top_n: int):
    """Apply cleaning rules 1-5 from AGENTS.md section 5.5.

    Rule 2 keeps UnitPrice == 0 (write-offs), only drops UnitPrice < 0.
    Rule 4 maps 4 types: sale, return, damage write-off, adjustment write-off.

    Returns (events_after_rule4, events_final, funnel, excluded_codes, extra_stats).
    """
    d = df.copy()
    d["sku"] = d["StockCode"].str.strip().str.upper()
    extra = {
        "stockcodes_raw": int(d["StockCode"].nunique()),
        "skus_after_upper": int(d["sku"].nunique()),
        "lowercase_rows": int(d["StockCode"].str.contains("[a-z]", regex=True).sum()),
    }

    funnel = [("Rows in CSV", len(d))]

    is_prod = d["sku"].str.match(PRODUCT_RE)
    excluded = (
        d.loc[~is_prod]
        .groupby("sku")
        .agg(
            rows=("sku", "size"),
            units=("Quantity", "sum"),
            example_description=(
                "Description",
                lambda s: s.dropna().mode().iloc[0] if s.notna().any() else "",
            ),
        )
        .sort_values("rows", ascending=False)
        .reset_index()
    )
    d = d[is_prod]
    funnel.append(("Rule 1: product StockCode only", len(d)))

    # Rule 2: drop Quantity == 0 or UnitPrice < 0 (UnitPrice == 0 is KEPT for write-offs)
    mask2 = (d["Quantity"] != 0) & (d["UnitPrice"] >= 0)
    d = d[mask2].copy()
    funnel.append(("Rule 2: drop Qty==0 or UnitPrice<0 (UnitPrice==0 kept)", len(d)))

    funnel.append(("Rule 3: keep missing CustomerID (nothing removed)", len(d)))

    # Rule 4: 4 recognised row types
    is_cancel = d["Invoice"].str.startswith("C", na=False)
    sale      = ~is_cancel & (d["Quantity"] > 0) & (d["UnitPrice"] > 0)   # sale
    ret       = is_cancel  & (d["Quantity"] < 0) & (d["UnitPrice"] > 0)   # return
    writeoff  = ~is_cancel & (d["Quantity"] < 0) & (d["UnitPrice"] == 0)  # write-off
    d = d[sale | ret | writeoff].copy()
    funnel.append(("Rule 4: sale | return | damage/adjustment write-off", len(d)))

    # Tag event types
    is_damage_desc = d["Description"].fillna("").str.lower().apply(
        lambda x: any(kw in x for kw in DAMAGE_WORDS)
    )
    conditions = [
        ~d["Invoice"].str.startswith("C", na=False) & (d["Quantity"] > 0),
        d["Invoice"].str.startswith("C", na=False) & (d["Quantity"] < 0),
        ~d["Invoice"].str.startswith("C", na=False) & (d["Quantity"] < 0) & is_damage_desc,
    ]
    choices = ["OUTBOUND", "INBOUND", "DAMAGE"]
    d["event_type"] = np.select(conditions, choices, default="ADJUSTMENT")
    d["quantity"] = d["Quantity"].abs()
    after_rule4 = d

    keep = d["sku"].value_counts().head(top_n).index
    final = d[d["sku"].isin(keep)].copy()
    funnel.append((f"Rule 5: top {top_n} SKUs by rows", len(final)))

    funnel_df = pd.DataFrame(funnel, columns=["step", "rows_after"])
    funnel_df["rows_removed"] = (-funnel_df["rows_after"].diff()).fillna(0).astype(int)
    funnel_df["pct_of_raw"] = (100 * funnel_df["rows_after"] / funnel_df["rows_after"].iloc[0]).round(2)
    return after_rule4, final, funnel_df, excluded, extra


def writeoff_candidates(df: pd.DataFrame):
    """Negative-quantity rows on non-cancellation invoices (likely stock write-offs)."""
    sku = df["StockCode"].str.strip().str.upper()
    mask = (
        (df["Quantity"] < 0)
        & ~df["Invoice"].str.startswith("C")
        & sku.str.match(PRODUCT_RE)
    )
    w = df.loc[mask].copy()
    desc = w["Description"].fillna("").str.strip().str.lower()
    is_damage = desc.apply(lambda x: any(k in x for k in DAMAGE_WORDS))
    w["category"] = np.where(
        is_damage,
        "DAMAGE-like",
        np.where(desc == "", "ADJUSTMENT-like (no description)", "ADJUSTMENT-like (check / unknown / other)"),
    )
    w["units"] = w["Quantity"].abs()
    summary = (
        w.groupby("category")
        .agg(rows=("category", "size"), units=("units", "sum"))
        .reset_index()
        .sort_values("rows", ascending=False)
    )
    top_desc = (
        w["Description"].fillna("(no description)").str.strip().str.lower().value_counts().head(15)
        .rename_axis("description").reset_index(name="rows")
    )
    return summary, top_desc, int(mask.sum()), int(
        ((df["Quantity"] < 0) & ~df["Invoice"].str.startswith("C")).sum()
    )


# --------------------------------------------------------------------------- #
# Derived (non-real) assignments
# --------------------------------------------------------------------------- #
def assign_warehouse(ev: pd.DataFrame, warehouses: list[str]):
    """warehouse_id = WAREHOUSES[crc32(str(CustomerID or Invoice)) % N]  (AGENTS.md 5.5)."""
    cid = ev["CustomerID"].astype("Int64").astype("string")
    fallback = cid.isna()
    key = cid.fillna(ev["Invoice"].astype("string"))
    mapping = {k: warehouses[zlib.crc32(str(k).encode()) % len(warehouses)] for k in key.unique()}
    return key.map(mapping), float(fallback.mean())


def replay_rates(ev: pd.DataFrame, speedups=(60, 600, 3600, 21600), max_gap: float = 1.0) -> pd.DataFrame:
    """Estimate replay duration and event rates in rebased mode (gap = min(gap/speedup, max_gap))."""
    counts = ev["InvoiceDate"].value_counts().sort_index()
    gaps = np.diff(counts.index.values) / np.timedelta64(1, "s")
    rows = []
    for sp in speedups:
        offset = np.concatenate([[0.0], np.cumsum(np.minimum(gaps / sp, max_gap))])
        duration = float(offset[-1])
        per_sec = np.bincount(np.floor(offset).astype(int), weights=counts.values)
        rows.append({
            "speedup": sp,
            "replay_hours": round(duration / 3600, 2),
            "avg_events_per_s": round(len(ev) / duration, 1),
            "p95_events_per_s": round(float(np.percentile(per_sec, 95)), 1),
            "max_events_per_s": int(per_sec.max()),
        })
    return pd.DataFrame(rows)


def partition_estimate(ev: pd.DataFrame, partitions: int) -> pd.DataFrame:
    """Estimate events per partition for key 'warehouse_id:sku' using CRC32 % partitions.

    This mirrors librdkafka's default 'consistent_random' partitioner, which is what
    confluent-kafka uses. It is an ESTIMATE: confirm against the real topic after a run.
    """
    pair = ev.groupby(["warehouse_id", "sku"]).size().rename("events").reset_index()
    pair["partition"] = [
        zlib.crc32(f"{w}:{s}".encode()) % partitions
        for w, s in zip(pair["warehouse_id"], pair["sku"])
    ]
    out = pair.groupby("partition")["events"].sum().reindex(range(partitions), fill_value=0)
    return out.rename("events").reset_index()


# --------------------------------------------------------------------------- #
# Plot helpers
# --------------------------------------------------------------------------- #
def finish(fig, path: Path, note: str, dpi: int) -> None:
    fig.text(0.01, 0.005, note, fontsize=7, color=COLORS["grey"])
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def thousands(v) -> str:
    return f"{int(round(v)):,}"


# --------------------------------------------------------------------------- #
# Markdown helpers
# --------------------------------------------------------------------------- #
def fmt(v) -> str:
    if isinstance(v, (int, np.integer)):
        return f"{int(v):,}"
    if isinstance(v, (float, np.floating)):
        if np.isnan(v):
            return ""
        if float(v).is_integer():
            return f"{int(v):,}"
        return f"{v:,.2f}"
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d %H:%M")
    return str(v).replace("|", "\\|")


def md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join(fmt(v) for v in row) + " |")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Main analysis
# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default="data/raw/online_retail_ii.csv", type=Path)
    ap.add_argument("--out", default="docs/eda", type=Path)
    ap.add_argument("--top-skus", default=500, type=int)
    ap.add_argument("--warehouses", default="WH-01,WH-02,WH-03")
    ap.add_argument("--partitions", default=6, type=int)
    ap.add_argument("--dpi", default=150, type=int)
    args = ap.parse_args()

    warehouses = [w.strip() for w in args.warehouses.split(",") if w.strip()]
    charts = args.out / "charts"
    tables = args.out / "tables"
    charts.mkdir(parents=True, exist_ok=True)
    tables.mkdir(parents=True, exist_ok=True)

    print(f"Loading {args.input} ...")
    raw = load(args.input)
    print(f"  {len(raw):,} rows")

    S: dict = {}  # key numbers, saved to summary.json and used in the description

    # ---- 1. Dataset overview ------------------------------------------------
    S["rows_raw"] = len(raw)
    S["date_min"] = str(raw["InvoiceDate"].min())
    S["date_max"] = str(raw["InvoiceDate"].max())
    S["days_span"] = int((raw["InvoiceDate"].max() - raw["InvoiceDate"].min()).days)
    S["invoices"] = int(raw["Invoice"].nunique())
    S["skus_raw"] = int(raw["StockCode"].nunique())
    S["customers"] = int(raw["CustomerID"].nunique())
    S["countries"] = int(raw["Country"].nunique())
    S["sorted_by_time"] = bool(raw["InvoiceDate"].is_monotonic_increasing)
    S["exact_duplicate_rows"] = int(raw.duplicated().sum())
    S["customer_missing"] = int(raw["CustomerID"].isna().sum())
    S["customer_missing_pct"] = round(100 * raw["CustomerID"].isna().mean(), 2)
    S["description_missing"] = int(raw["Description"].isna().sum())

    schema_rows = []
    descriptions = {
        "Invoice": "Invoice number; a leading 'C' marks a cancellation",
        "StockCode": "Product (SKU) code",
        "Description": "Product name",
        "Quantity": "Units per line; negative on cancellations and write-offs",
        "InvoiceDate": "Invoice date and time (minute resolution, naive UK local time)",
        "UnitPrice": "Price per unit in pounds sterling",
        "CustomerID": "Customer identifier (missing for many rows)",
        "Country": "Customer's country",
    }
    for c in raw.columns:
        s = raw[c]
        schema_rows.append({
            "column": c,
            "type": str(s.dtype),
            "meaning": descriptions.get(c, ""),
            "missing": int(s.isna().sum()),
            "missing_%": round(100 * s.isna().mean(), 2),
            "unique": int(s.nunique()),
            "example": str(s.dropna().iloc[0]),
        })
    schema = pd.DataFrame(schema_rows)
    schema.to_csv(tables / "schema_summary.csv", index=False)

    is_cancel_raw = raw["Invoice"].str.startswith("C")
    S["cancel_rows"] = int(is_cancel_raw.sum())
    S["cancel_pct_raw"] = round(100 * is_cancel_raw.mean(), 2)
    S["zero_price_rows"] = int((raw["UnitPrice"] == 0).sum())
    S["negative_price_rows"] = int((raw["UnitPrice"] < 0).sum())

    # ---- 2. Cleaning funnel and exclusions --------------------------------
    after4, ev, funnel, excluded, extra = clean(raw, args.top_skus)
    S.update(extra)
    S["rows_final"] = len(ev)
    S["skus_final"] = int(ev["sku"].nunique())
    S["top_skus"] = args.top_skus
    funnel.to_csv(tables / "cleaning_funnel.csv", index=False)
    excluded.to_csv(tables / "excluded_stockcodes.csv", index=False)
    S["nonproduct_rows"] = int(excluded["rows"].sum())
    S["nonproduct_codes"] = int(len(excluded))

    wo_summary, wo_top, wo_rows, neg_noncancel_rows = writeoff_candidates(raw)
    wo_summary.to_csv(tables / "writeoff_candidates.csv", index=False)
    wo_top.to_csv(tables / "writeoff_top_descriptions.csv", index=False)
    S["writeoff_rows"] = wo_rows
    S["neg_noncancel_rows"] = neg_noncancel_rows

    # ---- 3. Event mix: all 4 types ------------------------------------------
    mix = (
        ev.groupby("event_type")
        .agg(events=("quantity", "size"), units=("quantity", "sum"))
        .reset_index()
    )
    mix["event_share_%"] = (100 * mix["events"] / mix["events"].sum()).round(2)
    meaning_map = {
        "OUTBOUND":    "sale",
        "INBOUND":     "customer return",
        "DAMAGE":      "stock write-off (damage keyword)",
        "ADJUSTMENT":  "stock write-off (count correction)",
    }
    mix["meaning"] = mix["event_type"].map(meaning_map)
    mix.to_csv(tables / "event_mix.csv", index=False)
    out_ev  = ev[ev["event_type"] == "OUTBOUND"]
    in_ev   = ev[ev["event_type"] == "INBOUND"]
    dmg_ev  = ev[ev["event_type"] == "DAMAGE"]
    adj_ev  = ev[ev["event_type"] == "ADJUSTMENT"]
    S["outbound_events"]   = len(out_ev)
    S["inbound_events"]    = len(in_ev)
    S["damage_events"]     = len(dmg_ev)
    S["adjustment_events"] = len(adj_ev)
    S["return_event_pct"]  = round(100 * len(in_ev) / len(ev), 2)
    S["writeoff_events_final"] = len(dmg_ev) + len(adj_ev)

    # ---- 4. Country share ----------------------------------------------------
    c_raw = raw["Country"].value_counts().rename("raw_rows")
    c_ev = ev["Country"].value_counts().rename("replay_events")
    country = pd.concat([c_raw, c_ev], axis=1).fillna(0).astype(int).sort_values("raw_rows", ascending=False)
    country["raw_share_%"] = (100 * country["raw_rows"] / country["raw_rows"].sum()).round(2)
    country["replay_share_%"] = (100 * country["replay_events"] / country["replay_events"].sum()).round(2)
    country = country.rename_axis("country").reset_index()
    country.to_csv(tables / "country_share.csv", index=False)
    S["top_country"] = str(country.iloc[0]["country"])
    S["top_country_raw_pct"] = float(country.iloc[0]["raw_share_%"])
    S["top_country_replay_pct"] = float(country.iloc[0]["replay_share_%"])

    # ---- 5. Quantity distribution ------------------------------------------
    qs = [0.5, 0.9, 0.95, 0.99, 0.999]
    qrows = []
    for name, frame in (("OUTBOUND (sale)", out_ev), ("INBOUND (return)", in_ev)):
        q = frame["quantity"].quantile(qs)
        qrows.append({"event_type": name, **{f"p{round(p * 100, 1):g}": float(q[p]) for p in qs},
                      "max": int(frame["quantity"].max())})
    qty = pd.DataFrame(qrows)
    qty.to_csv(tables / "quantity_percentiles.csv", index=False)
    S["outbound_qty_p50"] = float(out_ev["quantity"].quantile(0.5))
    S["outbound_qty_p99"] = float(out_ev["quantity"].quantile(0.99))
    S["outbound_qty_max"] = int(out_ev["quantity"].max())

    biggest = (
        out_ev.nlargest(10, "quantity")[["Invoice", "sku", "quantity", "InvoiceDate", "CustomerID", "Country"]]
        .reset_index(drop=True)
    )
    ret_keys = set(zip(in_ev["CustomerID"], in_ev["sku"], in_ev["quantity"]))
    biggest["returned_in_full"] = [
        bool(pd.notna(c) and (c, sku, q) in ret_keys)
        for c, sku, q in zip(biggest["CustomerID"], biggest["sku"], biggest["quantity"])
    ]
    S["biggest_returned"] = int(biggest["returned_in_full"].sum())
    biggest["CustomerID"] = biggest["CustomerID"].astype("Int64").astype(str)
    biggest.to_csv(tables / "largest_outbound_events.csv", index=False)

    # Write-off events: counts, units, top descriptions, largest individual events
    # (AGENTS.md §5.6 output #8)
    wo_ev = ev[ev["event_type"].isin(["DAMAGE", "ADJUSTMENT"])].copy()
    if len(wo_ev) > 0:
        wo_type_tbl = (
            wo_ev.groupby("event_type")
            .agg(events=("quantity", "size"), units=("quantity", "sum"))
            .reset_index()
        )
        wo_type_tbl.to_csv(tables / "writeoff_by_type.csv", index=False)

        wo_desc_tbl = (
            wo_ev["Description"].fillna("(no description)").str.strip().str.lower()
            .value_counts().head(20).rename_axis("description").reset_index(name="rows")
        )
        wo_desc_tbl.to_csv(tables / "writeoff_top_descriptions.csv", index=False)

        largest_wo = (
            wo_ev.nlargest(20, "quantity")[["Invoice", "sku", "event_type", "quantity", "InvoiceDate", "Description"]]
            .reset_index(drop=True)
        )
        largest_wo.to_csv(tables / "largest_writeoff_events.csv", index=False)

        S["damage_units_final"]     = int(dmg_ev["quantity"].sum())
        S["adjustment_units_final"] = int(adj_ev["quantity"].sum())
    else:
        S["damage_units_final"] = 0
        S["adjustment_units_final"] = 0
        log_msg = "No write-off events in filtered set (write-offs may be outside top-SKU cut)"
        print(f"  NOTE: {log_msg}")

    # ---- 6. SKU skew (before top-N cut) --------------------------------------
    sku_counts = after4["sku"].value_counts().rename("events").rename_axis("sku").reset_index()
    sku_counts["rank"] = np.arange(1, len(sku_counts) + 1)
    sku_counts["share_%"] = (100 * sku_counts["events"] / sku_counts["events"].sum()).round(4)
    sku_counts["cum_share_%"] = sku_counts["share_%"].cumsum().round(2)
    sku_counts["in_top_n"] = sku_counts["rank"] <= args.top_skus
    sku_counts.to_csv(tables / "events_per_sku.csv", index=False)
    S["skus_before_topn"] = len(sku_counts)
    S["topn_share_pct"] = float(sku_counts.loc[args.top_skus - 1, "cum_share_%"]) if len(sku_counts) >= args.top_skus else 100.0
    S["top10_share_pct"] = float(sku_counts.loc[9, "cum_share_%"])
    S["median_events_per_sku"] = float(sku_counts["events"].median())

    # SKU catalogue (most common description per SKU) for Grafana labels
    cat_src = after4.dropna(subset=["Description"]).assign(desc=lambda x: x["Description"].str.strip())
    catalog = (
        cat_src.groupby("sku")["desc"]
        .agg(description=lambda s: s.mode().iloc[0], distinct_descriptions="nunique")
        .reset_index()
    )
    catalog = catalog.merge(sku_counts[["sku", "events", "in_top_n"]], on="sku", how="right")
    catalog["description"] = catalog["description"].fillna("")
    catalog.sort_values("events", ascending=False).to_csv(tables / "sku_catalog.csv", index=False)
    S["skus_multi_description"] = int((catalog["distinct_descriptions"] > 1).sum())

    # ---- 7. Time patterns -----------------------------------------------------
    hours = ev["InvoiceDate"].dt.hour.value_counts().sort_index()
    wk = ev["InvoiceDate"].dt.dayofweek.value_counts().reindex(range(7), fill_value=0)
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    months = ev["InvoiceDate"].dt.to_period("M").value_counts().sort_index()
    pd.DataFrame({"hour": hours.index, "events": hours.values}).to_csv(tables / "events_by_hour.csv", index=False)
    pd.DataFrame({"weekday": day_names, "events": wk.values}).to_csv(tables / "events_by_weekday.csv", index=False)
    pd.DataFrame({"month": months.index.astype(str), "events": months.values}).to_csv(
        tables / "events_by_month.csv", index=False)
    S["peak_hour"] = int(hours.idxmax())
    S["quiet_weekdays"] = [
        f"{day_names[i]} ({int(wk.iloc[i]):,} events)" for i in range(7) if wk.iloc[i] < 0.01 * wk.sum()
    ]
    S["peak_month"] = str(months.idxmax())
    S["peak_month_events"] = int(months.max())
    S["median_month_events"] = float(months.median())

    daily = ev.set_index("InvoiceDate").resample("D").size()
    S["days_with_events"] = int((daily > 0).sum())
    S["days_without_events"] = int((daily == 0).sum())
    S["avg_events_per_active_day"] = round(float(daily[daily > 0].mean()), 1)

    ts = np.sort(ev["InvoiceDate"].unique())
    gaps = pd.DataFrame({"gap_start": ts[:-1], "gap_end": ts[1:]})
    gaps["gap_hours"] = (gaps["gap_end"] - gaps["gap_start"]) / np.timedelta64(1, "h")
    big_gaps = gaps.nlargest(10, "gap_hours").reset_index(drop=True)
    big_gaps.to_csv(tables / "largest_gaps.csv", index=False)
    S["largest_gap_hours"] = round(float(gaps["gap_hours"].max()), 1)
    S["gaps_over_24h"] = int((gaps["gap_hours"] > 24).sum())

    # ---- 8. Timestamp bursts ---------------------------------------------------
    per_ts = ev.groupby("InvoiceDate").size()
    burst = pd.DataFrame([{
        "distinct_timestamps": len(per_ts),
        "median_events_per_timestamp": float(per_ts.median()),
        "p95": float(per_ts.quantile(0.95)),
        "p99": float(per_ts.quantile(0.99)),
        "max": int(per_ts.max()),
        "events_sharing_a_timestamp_%": round(100 * per_ts[per_ts > 1].sum() / per_ts.sum(), 1),
    }])
    burst.to_csv(tables / "timestamp_bursts.csv", index=False)
    S["timestamp_burst_max"] = int(per_ts.max())
    S["timestamp_burst_p95"] = float(per_ts.quantile(0.95))
    S["share_sharing_ts_pct"] = float(burst.iloc[0]["events_sharing_a_timestamp_%"])

    replay = replay_rates(ev)
    replay.to_csv(tables / "replay_rate_estimates.csv", index=False)
    r600 = replay.set_index("speedup").loc[600]
    r3600 = replay.set_index("speedup").loc[3600]
    S["replay_600_hours"] = float(r600["replay_hours"])
    S["replay_600_avg_eps"] = float(r600["avg_events_per_s"])
    S["replay_3600_hours"] = float(r3600["replay_hours"])
    S["replay_3600_avg_eps"] = float(r3600["avg_events_per_s"])

    # ---- 9. Derived: warehouses, partitions, outflow --------------------------
    ev["warehouse_id"], fallback_share = assign_warehouse(ev, warehouses)
    S["warehouse_fallback_pct"] = round(100 * fallback_share, 2)
    wh = ev["warehouse_id"].value_counts().reindex(warehouses, fill_value=0).rename("events")
    wh_tbl = wh.rename_axis("warehouse_id").reset_index()
    wh_tbl["share_%"] = (100 * wh_tbl["events"] / wh_tbl["events"].sum()).round(2)
    wh_tbl.to_csv(tables / "events_per_warehouse.csv", index=False)
    S["warehouse_share_min_pct"] = float(wh_tbl["share_%"].min())
    S["warehouse_share_max_pct"] = float(wh_tbl["share_%"].max())

    part = partition_estimate(ev, args.partitions)
    part["share_%"] = (100 * part["events"] / part["events"].sum()).round(2)
    part.to_csv(tables / "events_per_partition_estimate.csv", index=False)
    S["partition_share_min_pct"] = float(part["share_%"].min())
    S["partition_share_max_pct"] = float(part["share_%"].max())
    S["partitions"] = args.partitions
    S["warehouses"] = warehouses

    keys = ev.groupby(["warehouse_id", "sku"]).size()
    S["keys"] = int(len(keys))
    S["median_events_per_key"] = float(keys.median())
    S["keys_under_100_events_pct"] = round(100 * float((keys < 100).mean()), 1)
    S["min_events_per_key"] = int(keys.min())

    out_ev = ev[ev["event_type"] == "OUTBOUND"]
    daily_out = out_ev.groupby(["warehouse_id", "sku", out_ev["InvoiceDate"].dt.floor("D")])["quantity"].sum()
    dq = daily_out.quantile([0.5, 0.9, 0.95, 0.99])
    pair_mean = daily_out.groupby(level=[0, 1]).transform("mean")
    spike_share = float((daily_out > 3 * pair_mean).mean())
    active_days = daily_out.groupby(level=[0, 1]).size()
    outflow_tbl = pd.DataFrame([{
        "sku_warehouse_days": int(len(daily_out)),
        "p50_units": float(dq[0.5]), "p90_units": float(dq[0.9]),
        "p95_units": float(dq[0.95]), "p99_units": float(dq[0.99]),
        "max_units": int(daily_out.max()),
        "days_over_3x_pair_mean_%": round(100 * spike_share, 2),
        "median_active_days_per_key": float(active_days.median()),
    }])
    outflow_tbl.to_csv(tables / "daily_outflow_percentiles.csv", index=False)
    S["daily_outflow_p50"] = float(dq[0.5])
    S["daily_outflow_p99"] = float(dq[0.99])
    S["spike_share_pct"] = round(100 * spike_share, 2)
    S["median_active_days_per_key"] = float(active_days.median())

    # ====================== CHARTS ===========================================
    plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})

    # 01 cleaning funnel
    fig, ax = plt.subplots(figsize=(9, 4))
    y = np.arange(len(funnel))[::-1]
    ax.barh(y, funnel["rows_after"], color=COLORS["main"])
    ax.set_yticks(y)
    ax.set_yticklabels(funnel["step"])
    for yi, v in zip(y, funnel["rows_after"]):
        ax.text(v, yi, f" {thousands(v)}", va="center", fontsize=8)
    ax.set_xlim(0, funnel["rows_after"].max() * 1.15)
    ax.set_xlabel("Rows remaining")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.set_title("Cleaning funnel: rows remaining after each rule")
    finish(fig, charts / "01_cleaning_funnel.png", NOTE_REAL, args.dpi)

    # 02 event mix (all 4 types: sale, return, damage, adjustment)
    all_types  = ["OUTBOUND", "INBOUND", "DAMAGE", "ADJUSTMENT"]
    type_labels = ["OUTBOUND\n(sale)", "INBOUND\n(return)", "DAMAGE\n(write-off)", "ADJUSTMENT\n(write-off)"]
    bar_colors  = [COLORS["main"], COLORS["alt"], COLORS["bad"], COLORS["grey"]]
    mix_ordered = mix.set_index("event_type").reindex(all_types).fillna(0)
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    for ax, col, title in ((axes[0], "events", "Events"), (axes[1], "units", "Units")):
        vals = mix_ordered[col].values
        bars = ax.bar(type_labels, vals, color=bar_colors)
        for b, v in zip(bars, vals):
            if v > 0:
                ax.text(b.get_x() + b.get_width() / 2, v, f"{thousands(v)}\n({100 * v / max(vals.sum(), 1):.1f}%)",
                        ha="center", va="bottom", fontsize=7)
        ax.set_title(title)
        ax.set_ylim(0, max(vals.max() * 1.2, 1))
    fig.suptitle("Event mix after cleaning: sales, returns, damage, adjustment (replay set)")
    finish(fig, charts / "02_event_mix.png", NOTE_REAL, args.dpi)

    # 03 country share
    top = country.head(10).iloc[::-1]
    other = country.iloc[10:]["replay_share_%"].sum()
    fig, ax = plt.subplots(figsize=(8, 4.5))
    names = ([f"Other ({len(country) - 10} countries)"] if other > 0 else []) + list(top["country"])
    vals = ([other] if other > 0 else []) + list(top["replay_share_%"])
    ax.barh(names, vals, color=COLORS["main"])
    for i, v in enumerate(vals):
        ax.text(v, i, f" {v:.1f}%", va="center", fontsize=8)
    ax.set_xlim(0, max(vals) * 1.15)
    ax.set_xlabel("Share of replay events (%)")
    ax.set_title("Country share: one country dominates, so Country is a poor warehouse proxy")
    finish(fig, charts / "03_country_share.png", NOTE_REAL, args.dpi)

    # 04 quantity distribution
    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    bins = np.logspace(0, np.log10(out_ev["quantity"].max()) + 0.1, 50)
    ax.hist(out_ev["quantity"], bins=bins, color=COLORS["main"])
    ax.set_xscale("log")
    ax.set_yscale("log")
    for p, ls in ((0.5, "--"), (0.95, ":"), (0.99, "-.")):
        v = out_ev["quantity"].quantile(p)
        ax.axvline(v, color=COLORS["bad"], ls=ls, label=f"p{int(p * 100)} = {v:g}")
    ax.set_xlabel("Quantity per OUTBOUND event (log scale)")
    ax.set_ylabel("Events (log scale)")
    ax.set_title("Quantity distribution of sales: heavy right tail")
    ax.legend()
    finish(fig, charts / "04_quantity_distribution.png", NOTE_REAL, args.dpi)

    # 05 SKU rank / skew
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    axes[0].loglog(sku_counts["rank"], sku_counts["events"], color=COLORS["main"])
    axes[0].axvline(args.top_skus, color=COLORS["bad"], ls="--", label=f"top {args.top_skus} cutoff")
    axes[0].set_xlabel("SKU rank (log)")
    axes[0].set_ylabel("Events per SKU (log)")
    axes[0].set_title("Events per SKU (rank plot)")
    axes[0].legend()
    axes[1].plot(sku_counts["rank"], sku_counts["cum_share_%"], color=COLORS["main"])
    axes[1].axvline(args.top_skus, color=COLORS["bad"], ls="--")
    axes[1].set_xlabel("SKU rank")
    axes[1].set_ylabel("Cumulative share of events (%)")
    axes[1].set_title(f"Top {args.top_skus} SKUs cover {S['topn_share_pct']:.1f}% of events")
    finish(fig, charts / "05_sku_skew.png", NOTE_REAL, args.dpi)

    # 06 time patterns
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8))
    axes[0].bar(hours.index, hours.values, color=COLORS["main"])
    axes[0].set_xlabel("Hour of day (original time)")
    axes[0].set_title("Events by hour")
    axes[1].bar(day_names, wk.values, color=COLORS["main"])
    axes[1].set_title("Events by weekday")
    axes[2].bar(months.index.astype(str), months.values, color=COLORS["main"])
    axes[2].set_title("Events by month")
    axes[2].tick_params(axis="x", rotation=90, labelsize=7)
    finish(fig, charts / "06_time_patterns.png", NOTE_REAL + " Original (not rebased) timestamps.", args.dpi)

    # 07 daily events
    fig, ax = plt.subplots(figsize=(11, 3.6))
    ax.plot(daily.index, daily.values, color=COLORS["main"], lw=0.8)
    ax.set_ylabel("Events per day")
    ax.set_title("Daily events over time (zero days are closures and gaps)")
    finish(fig, charts / "07_daily_events.png", NOTE_REAL + " Original timestamps.", args.dpi)

    # 08 warehouses + partitions (derived)
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    bars = axes[0].bar(wh.index, wh.values, color=COLORS["alt"])
    for b, v in zip(bars, wh.values):
        axes[0].text(b.get_x() + b.get_width() / 2, v, f"{100 * v / wh.sum():.1f}%", ha="center", va="bottom", fontsize=8)
    axes[0].set_title("Events per warehouse (customer-hash rule)")
    axes[0].set_ylim(0, wh.max() * 1.15)
    axes[1].bar(part["partition"].astype(str), part["events"], color=COLORS["alt"])
    axes[1].axhline(part["events"].mean(), color=COLORS["grey"], ls="--", label="even split")
    axes[1].set_xlabel("Partition")
    axes[1].set_title(f"Estimated events per partition ({args.partitions} partitions)")
    axes[1].legend()
    finish(fig, charts / "08_warehouse_partition_estimate.png", NOTE_DERIVED, args.dpi)

    # 09 daily outflow per key
    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    bins = np.logspace(0, np.log10(daily_out.max()) + 0.1, 50)
    ax.hist(daily_out, bins=bins, color=COLORS["main"])
    ax.set_xscale("log")
    ax.set_yscale("log")
    for p, ls in ((0.5, "--"), (0.95, ":"), (0.99, "-.")):
        v = daily_out.quantile(p)
        ax.axvline(v, color=COLORS["bad"], ls=ls, label=f"p{int(p * 100)} = {v:g}")
    ax.set_xlabel("Units sold per (warehouse, SKU) per day (log scale)")
    ax.set_ylabel("Key-days (log scale)")
    ax.set_title("Daily outflow per key (basis for the ABNORMAL_OUTFLOW threshold)")
    ax.legend()
    finish(fig, charts / "09_daily_outflow.png", NOTE_DERIVED, args.dpi)

    # 10 timestamp bursts
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.hist(per_ts.values, bins=min(60, int(per_ts.max())), color=COLORS["main"])
    ax.set_yscale("log")
    ax.set_xlabel("Events sharing one timestamp (minute resolution)")
    ax.set_ylabel("Distinct timestamps (log)")
    ax.set_title("Timestamp bursts: invoice lines share the same minute")
    finish(fig, charts / "10_timestamp_bursts.png", NOTE_REAL, args.dpi)

    # ====================== SUMMARY + DESCRIPTION ===========================
    (args.out / "summary.json").write_text(json.dumps(S, indent=2, default=str))
    write_description(args, S, schema, funnel, excluded, wo_summary, wo_top, mix, country, qty,
                      biggest, big_gaps, burst, wh_tbl, part, outflow_tbl, replay)
    print(f"Done. Open {args.out / 'dataset_description.md'}")


def write_description(args, S, schema, funnel, excluded, wo_summary, wo_top, mix, country, qty,
                      biggest, big_gaps, burst, wh_tbl, part, outflow_tbl, replay) -> None:
    n = S["rows_raw"]
    empty = ", ".join(S["quiet_weekdays"]) if S["quiet_weekdays"] else "none"
    country_top = country.head(6)[["country", "raw_rows", "raw_share_%", "replay_share_%"]]
    text = f"""# Dataset description: UCI Online Retail II

*Generated by `src/tools/eda.py` on {datetime.now():%Y-%m-%d} from `{args.input.name}`. Every number below was computed from that file.*

## 1. Source and provenance

- **Dataset:** Online Retail II, UCI Machine Learning Repository (dataset id 502).
- **What it is:** real transaction lines from a UK-based online retailer (mostly gift-ware, with many wholesale customers), covering {S['date_min'][:10]} to {S['date_max'][:10]}.
- **License:** CC BY 4.0. Attribution required.
- **Citation:** Chen, D. (2019). *Online Retail II* [Dataset]. UCI Machine Learning Repository. https://archive.ics.uci.edu/dataset/502/online+retail+ii
- **Role in this project:** the *real demand* half of a hybrid source. Sales become `OUTBOUND` events and cancellations become `INBOUND` (return) events and are replayed into Kafka. The dataset contains **no restocking, transfers, damage records, or stock levels**: those are simulated.
- **Timestamps:** original times are naive UK local time at **minute resolution**. In the default replay mode they are rebased to the replay clock, and the original is kept as `original_time`.

## 2. Schema

{md_table(schema)}

## 3. Size and coverage

| Measure | Value |
|---|---|
| Rows in the CSV (after merging the two sheets and dropping exact duplicates) | {n:,} |
| Time span | {S['date_min']} to {S['date_max']} ({S['days_span']:,} days) |
| Distinct invoices | {S['invoices']:,} |
| Distinct stock codes | {S['skus_raw']:,} |
| Distinct customers | {S['customers']:,} |
| Countries | {S['countries']} |
| Sorted by `InvoiceDate` | {'yes' if S['sorted_by_time'] else 'no'} |
| Exact duplicate rows remaining | {S['exact_duplicate_rows']:,} |

## 4. Data quality findings

- **Missing `CustomerID`:** {S['customer_missing']:,} rows ({S['customer_missing_pct']}%). The warehouse-hashing rule falls back to the invoice number for these rows.
- **Missing `Description`:** {S['description_missing']:,} rows.
- **Cancellations:** {S['cancel_rows']:,} rows ({S['cancel_pct_raw']}%) are on invoices starting with `C`.
- **Zero or negative prices:** {S['zero_price_rows']:,} rows have `UnitPrice = 0` and {S['negative_price_rows']} have a negative price (the latter are accounting entries such as "Adjust bad debt").
- **Non-product stock codes:** {S['nonproduct_rows']:,} rows across {S['nonproduct_codes']} codes (postage, fees, manual entries, gift vouchers and similar) are removed by cleaning rule 1. See `tables/excluded_stockcodes.csv`.
- **Case-inconsistent stock codes:** {S['lowercase_rows']:,} rows use lower-case letters (for example `72349b`). Upper-casing merges {S['stockcodes_raw'] - S['skus_after_upper']:,} duplicate variants ({S['stockcodes_raw']:,} raw codes become {S['skus_after_upper']:,}).
- **Inconsistent descriptions:** {S['skus_multi_description']:,} SKUs have more than one distinct description. `tables/sku_catalog.csv` keeps the most common one.
- **Likely stock write-offs hidden in the data:** {S['neg_noncancel_rows']:,} rows have a negative quantity on a non-cancellation invoice, all with `UnitPrice = 0`. Of these, {S['writeoff_rows']:,} have a product stock code:

{md_table(wo_summary)}

  The most common descriptions are things like "check", "damaged" and "missing", which look like real write-offs and stock-count corrections. Cleaning rule 2 currently drops them. They are a candidate source of genuine `ADJUSTMENT` and `DAMAGE` events (see the optional extension in AGENTS.md section 5.5). Top descriptions:

{md_table(wo_top.head(10))}

## 5. Cleaning rules applied (AGENTS.md section 5.5)

{md_table(funnel)}

The replay set therefore contains **{S['rows_final']:,} events across {S['skus_final']:,} SKUs**.

## 6. Mapping to pipeline events

| Dataset rows | Event | Count | Share |
|---|---|---|---|
| Sales (positive quantity, normal invoice) | `OUTBOUND` | {S['outbound_events']:,} | {100 - S['return_event_pct']:.2f}% |
| Cancellations (`C` invoice, negative quantity) | `INBOUND` (return) | {S['inbound_events']:,} | {S['return_event_pct']:.2f}% |

{md_table(mix[['event_type', 'meaning', 'events', 'units', 'event_share_%']])}

Charts: `charts/02_event_mix.png`, `charts/01_cleaning_funnel.png`.

## 7. Findings and design implications

**Country skew.** {S['top_country']} accounts for {S['top_country_raw_pct']}% of raw rows ({S['top_country_replay_pct']}% of the replay set).

{md_table(country_top)}

*Implication:* Country cannot serve as the warehouse. The customer-hash rule is used instead and yields {S['warehouse_share_min_pct']}% to {S['warehouse_share_max_pct']}% of events per warehouse (`charts/08_warehouse_partition_estimate.png`). Chart: `charts/03_country_share.png`.

**SKU skew.** The top 10 SKUs hold {S['top10_share_pct']}% of events and the top {S['top_skus']} hold {S['topn_share_pct']}% (out of {S['skus_before_topn']:,} SKUs). The median SKU has {S['median_events_per_sku']:,.0f} events over two years. Chart: `charts/05_sku_skew.png`.

*Implication:* keeping the top {S['top_skus']} SKUs drops {S['skus_before_topn'] - S['top_skus']:,} sparse SKUs but also about {100 - S['topn_share_pct']:.0f}% of events. Raise `--top-skus` for more volume and more keys; lower it for faster demos.

**Key activity.** With {len(S['warehouses'])} warehouses there are {S['keys']:,} `(warehouse, SKU)` keys. The median key has {S['median_events_per_key']:,.0f} events and the least active key has {S['min_events_per_key']:,}. The median key is active on {S['median_active_days_per_key']:,.0f} distinct days.

**Partitioning (estimate).** With {S['partitions']} partitions and key `warehouse_id:sku`, the estimated share per partition ranges from {S['partition_share_min_pct']}% to {S['partition_share_max_pct']}%. This is an estimate using a CRC32 partitioner. Confirm it with the real topic after a run.

{md_table(part)}

**Quantities.** Median sale quantity is {S['outbound_qty_p50']:g} units, the 99th percentile is {S['outbound_qty_p99']:g}, and the maximum is {S['outbound_qty_max']:,}.

{md_table(qty)}

The ten largest sales. {S['biggest_returned']} of them have a matching return (same customer, SKU and quantity, column `returned_in_full`). Huge orders swing running stock sharply, so inspect them before replaying:

{md_table(biggest)}

Chart: `charts/04_quantity_distribution.png`.

**Daily outflow per key.** Median {S['daily_outflow_p50']:g} units per key per active day, p99 {S['daily_outflow_p99']:g}. {S['spike_share_pct']}% of key-days exceed 3x that key's own mean daily outflow, so a fixed "3x the average" `ABNORMAL_OUTFLOW` rule will fire on a small but non-trivial share of days. Chart: `charts/09_daily_outflow.png`.

{md_table(outflow_tbl)}

**Time patterns (original timestamps).** Peak hour is {S['peak_hour']}:00. Near-empty weekdays (under 1% of events): {empty}. The busiest month ({S['peak_month']}) has {S['peak_month_events']:,} events versus a monthly median of {S['median_month_events']:,.0f}. There are {S['days_without_events']} days with no events and {S['days_with_events']} active days averaging {S['avg_events_per_active_day']:,.1f} events. Charts: `charts/06_time_patterns.png`, `charts/07_daily_events.png`.

*Implication:* the replay needs the `--max-gap-seconds` cap. The largest gap is {S['largest_gap_hours']} hours and there are {S['gaps_over_24h']} gaps longer than 24 hours.

{md_table(big_gaps.head(5))}

**Timestamp bursts.** Timestamps have minute resolution, so {S['share_sharing_ts_pct']}% of events share a timestamp with at least one other event, with up to {S['timestamp_burst_max']:,} events in one minute (95th percentile: {S['timestamp_burst_p95']:g}).

{md_table(burst)}

*Implication:* in the replay these events arrive as instantaneous bursts, which is realistic for invoice lines, and gives the consumer lag panels something to show. Chart: `charts/10_timestamp_bursts.png`.

**Replay duration and rate (estimate).** In rebased mode each gap between distinct timestamps becomes `min(gap / speedup, 1 s)`. For these {S['rows_final']:,} events a full replay takes about {S['replay_600_hours']:g} hours at the default speedup of 600 (average {S['replay_600_avg_eps']:g} events/s) and about {S['replay_3600_hours']:g} hours at 3600 (average {S['replay_3600_avg_eps']:g} events/s). Peak rates are driven by the minute-resolution timestamp bursts above.

{md_table(replay)}

*Implication:* pick the speedup from the length of demo you want. Use a higher speedup for a short live demo, and a lower one for a long, steady dashboard run.

## 8. Limitations (state these in the report and slides)

1. Demand is real, but it is **one retailer's** demand (a UK online gift-ware business with many wholesale customers), so it is not a model of a general warehouse.
2. Warehouses are **assigned by a hashing rule**. The dataset has no warehouse information.
3. There is **no restocking, transfer, or stock-level data**. Replenishment, opening balances, transfers and faults are simulated.
4. Returns are modelled as stock coming back in, which is an assumption.
5. Timestamps are naive local time at minute resolution. They are treated as UTC for simplicity, and in the default replay mode they are rebased and time-compressed, so time-of-day patterns do not appear on live dashboards.
6. Partition shares above are estimates, not measurements.
"""
    (args.out / "dataset_description.md").write_text(text)


if __name__ == "__main__":
    main()