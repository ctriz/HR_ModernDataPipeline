#!/usr/bin/env python3
"""
delta_inspector.py
Incremental CDC reader for HR Bronze Delta tables.

Features:
- Reads changes since last watermark (per table)
- Prints inserts, updates (before & after), and deletes
- Keeps watermarks in ./watermarks/<table>_reader_watermark.txt
"""

import os
import sys
import argparse
from deltalake import DeltaTable
import pandas as pd
from config.config import load_config

# ---------------------------
# Config & Paths
# ---------------------------
config = load_config()
if not config:
    print("Error: Could not load configuration")
    sys.exit(1)

def normalize_path(path: str) -> str:
    return path.replace("file:///", "").replace("file://", "")

delta_base_path = normalize_path(config["delta"]["base_path"])
bronze_base_path = os.path.join(delta_base_path, "bronze")

TABLES = ["departments", "jobs", "locations", "employees"]

from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # go 2 levels up from jobs/bronze
WATERMARK_DIR = PROJECT_ROOT / "watermarks"
WATERMARK_DIR.mkdir(exist_ok=True)


# ---------------------------
# Watermark helpers
# ---------------------------
def watermark_file(table_name):
    return os.path.join(WATERMARK_DIR, f"{table_name}_reader_watermark.txt")

def get_last_watermark(table_name):
    path = watermark_file(table_name)
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    return "1970-01-01 00:00:00"

def set_last_watermark(table_name, ts: str):
    with open(watermark_file(table_name), "w") as f:
        f.write(ts)

# ---------------------------
# CDC Reader with before/after
# ---------------------------
def read_cdc_table(table_name: str):
    delta_path = os.path.join(bronze_base_path, table_name)

    if not os.path.exists(delta_path):
        print(f" Delta path not found for {table_name}: {delta_path}")
        return

    print(f"\n=== Bronze CDC Reader: {table_name} ===")
    print(f" Path: {delta_path}")

    try:
        dt = DeltaTable(delta_path)
        df = dt.to_pyarrow_table().to_pandas()
    except Exception as e:
        print(f" Failed to load {table_name}: {e}")
        return

    if "updated_at" not in df.columns:
        print(f" Table {table_name} has no updated_at column")
        return

    last_ts = get_last_watermark(table_name)
    print(f" Fetching rows where updated_at > {last_ts}")

    cdc_df = df[df["updated_at"] > last_ts]

    if last_ts == "1970-01-01 00:00:00":
        print(f" First run detected for {table_name}, skipping {len(cdc_df)} initial rows.")
        if not df.empty:
            max_ts = str(df["updated_at"].max())
            set_last_watermark(table_name, max_ts)
            print(f" Initialized watermark for {table_name} → {max_ts}")
        return

    if cdc_df.empty:
        print(f"ℹ No new changes for {table_name}")
        return

    # ---------------------------
    # Classify Inserts, Updates, Deletes
    # ---------------------------
    id_col = f"{table_name[:-1]}_id"  # crude assumption: table_id column
    changed_ids = cdc_df[id_col].unique()

    for cid in changed_ids:
        all_versions = df[df[id_col] == cid].sort_values("updated_at")
        new_versions = all_versions[all_versions["updated_at"] > last_ts]

        if len(new_versions) == 1:
            row = new_versions.iloc[0]
            if row.get("is_deleted", False):
                print(f"\n DELETE: {table_name} {id_col}={cid}")
                print(row.to_dict())
            else:
                print(f"\n INSERT: {table_name} {id_col}={cid}")
                print(row.to_dict())
        else:
            before = new_versions.iloc[0].to_dict()
            after = new_versions.iloc[-1].to_dict()
            print(f"\n UPDATE: {table_name} {id_col}={cid}")
            print("Before:", before)
            print("After :", after)

    # ---------------------------
    # Advance watermark
    # ---------------------------
    max_ts = str(cdc_df["updated_at"].max())
    set_last_watermark(table_name, max_ts)
    print(f"\n Updated watermark for {table_name} → {max_ts}")

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=TABLES, help="Inspect only one table")
    args = parser.parse_args()

    if args.table:
        read_cdc_table(args.table)
    else:
        print(" Inspecting CDC changes for all Bronze tables...")
        for tbl in TABLES:
            read_cdc_table(tbl)

    print("\n Bronze CDC inspection complete.")
