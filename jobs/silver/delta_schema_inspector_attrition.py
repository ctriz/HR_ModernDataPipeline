#!/usr/bin/env python3
"""
delta_schema_inspector.py
Inspect schema and sample data from a Delta table (Bronze/Silver).
"""

from deltalake import DeltaTable
import pandas as pd
from config.config import load_config

# ---------------------------
# Config
# ---------------------------
config = load_config()
if not config:
    raise RuntimeError("Could not load config.yaml")

def normalize_path(path: str) -> str:
    return path.replace("file:///", "").replace("file://", "")

# Example: Inspect employees Bronze table
delta_path = normalize_path(f"{config['delta']['base_path']}/silver/fact_employee_attrition_events")

# ---------------------------
# Load Delta table
# ---------------------------
dt = DeltaTable(delta_path)
schema = dt.schema().json()
df = dt.to_pyarrow_table().to_pandas()

# ---------------------------
# Pretty print schema
# ---------------------------
print("\n=== Schema ===")
for field in dt.schema().fields:
    nullable = "NULL" if field.nullable else "NOT NULL"
    print(f"- {field.name:<15} {field.type} ({nullable})")

# ---------------------------
# Sample data preview
# ---------------------------
print("\n=== Sample Data (first 10 rows) ===")
try:
    import tabulate
    print(tabulate.tabulate(df.head(10), headers="keys", tablefmt="psql", showindex=False))
except ImportError:
    print(df.head(10).to_string(index=False))
