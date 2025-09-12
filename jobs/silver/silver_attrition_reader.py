#!/usr/bin/env python3
"""
silver_attrition_reader.py
Quick inspection of Silver Attrition Fact tables.

Tables inspected:
- fact_employee_attrition_events
- fact_employee_headcount
- fact_employee_attrition_rate

Options:
- Default: inspect all tables
- With --table <name>: inspect just one table
    (choices: attrition, headcount, rate)

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

TABLES = {
    "attrition": "fact_employee_attrition_events",
    "headcount": "fact_employee_headcount",
    "rate": "fact_employee_attrition_rate",
}

# ---------------------------
# Inspector
# ---------------------------
def inspect_table(key: str, delta_name: str):
    delta_path = os.path.join(silver_base_path, delta_name)

    if not os.path.exists(delta_path):
        print(f" Table not found: {delta_name} at {delta_path}")
        return

    print(f"\n=== Silver Fact Table: {delta_name} ===")
    print(f" Path: {delta_path}")

    try:
        dt = DeltaTable(delta_path)
        df = dt.to_pyarrow_table().to_pandas()
    except Exception as e:
        print(f" Failed to load {delta_name}: {e}")
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
    parser.add_argument(
        "--table",
        choices=TABLES.keys(),
        help="Inspect only one table (attrition, headcount, rate)"
    )
    args = parser.parse_args()

    if args.table:
        inspect_table(args.table, TABLES[args.table])
    else:
        print("🔎 Inspecting all Silver Attrition Fact Tables...")
        for key, val in TABLES.items():
            inspect_table(key, val)

    print("\n Silver Attrition Fact inspection complete.")
