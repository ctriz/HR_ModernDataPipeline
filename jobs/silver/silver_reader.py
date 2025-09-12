#!/usr/bin/env python3
"""
silver_reader.py
Quick inspection of HR Delta Silver (SCD2) tables.

Tables inspected:
- employees_scd2
- departments_scd2
- jobs_scd2
- locations_scd2

Options:
- Default: inspect all tables
- With --table <name>: inspect just one table

For each table:
- Print schema (columns + types)
- Print row count
- Print Delta version
- Print first 5 rows
"""

import os
import sys
import argparse
from deltalake import DeltaTable
from config.config import load_config

# ---------------------------
# Load Config
# ---------------------------
config = load_config()
if not config:
    print(" Error: Could not load configuration")
    sys.exit(1)

def normalize_path(path: str) -> str:
    return path.replace("file:///", "").replace("file://", "")

delta_base_path = normalize_path(config["delta"]["base_path"])
silver_base_path = os.path.join(delta_base_path, "silver")

TABLES = ["employees_scd2", "departments_scd2", "jobs_scd2", "locations_scd2"]

# ---------------------------
# Inspector
# ---------------------------
def inspect_table(table_name: str):
    delta_path = os.path.join(silver_base_path, table_name)

    if not os.path.exists(delta_path):
        print(f" Table not found: {table_name} at {delta_path}")
        return

    print(f"\n=== Silver Table: {table_name} ===")
    print(f" Path: {delta_path}")

    try:
        dt = DeltaTable(delta_path)
        df = dt.to_pyarrow_table().to_pandas()
    except Exception as e:
        print(f" Failed to load {table_name}: {e}")
        return

    # Schema
    print("\n--- Schema ---")
    print(df.dtypes.to_string())

    # Row count + version
    print(f"\n--- Row Count: {len(df)} ---")
    print(f"--- Delta Version: {dt.version()} ---")

    # Sample rows
    print("\n--- First 5 Rows ---")
    try:
        print(df.head().to_markdown(index=False))
    except ImportError:
        print(df.head().to_string(index=False))


# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=TABLES, help="Inspect only one Silver table")
    args = parser.parse_args()

    if args.table:
        inspect_table(args.table)
    else:
        print(" Inspecting all HR Silver Tables...")
        for tbl in TABLES:
            inspect_table(tbl)

    print("\n Silver inspection complete.")
