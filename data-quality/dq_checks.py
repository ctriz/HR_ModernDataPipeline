#!/usr/bin/env python3
"""
dq_checks.py
Data Quality checks for employees, driven by dq_rules.yaml.
Invalid rows written to delta/quarantine/employees_bad_records
"""

import os
import sys
import yaml
import pandas as pd
from deltalake import DeltaTable
from pyspark.sql import SparkSession
from config.config import load_config
from utils.db_utils import get_jdbc

# ---------------------------
# Config
# ---------------------------
config = load_config()
if not config:
    raise RuntimeError(" Could not load config.yaml")

def normalize_path(path: str) -> str:
    return path.replace("file:///", "").replace("file://", "")

delta_base_path = config["delta"]["base_path"]
bronze_base_path = f"{delta_base_path}/bronze"
quarantine_base = "{delta_base_path}/quarantine"


# Spark for writing quarantine tables
spark = (
    SparkSession.builder
    .appName("DQ Checks with YAML Rules")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master(config['spark']['master'])
    .config("spark.driver.bindAddress", config['spark']['host'])
    .config("spark.driver.host", config['spark']['host'])
    # Extra configs to stabilize streaming
    .config("spark.executor.heartbeatInterval", "30s")
    .config("spark.network.timeout", "120s")
    # JARS
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

# ---------------------------
# Load Helpers
# ---------------------------
def load_table(name):
    path = os.path.join(bronze_base, name)
    return DeltaTable(path).to_pyarrow_table().to_pandas()

def load_rules():
    with open("dq_rules.yaml", "r") as f:
        return yaml.safe_load(f)

# ---------------------------
# Rule Evaluation
# ---------------------------
def apply_rules(df, rules, ref_tables):
    bad_records = pd.DataFrame()
    errors = []

    for rule in rules:
        col = rule["column"]
        msg = rule["message"]

        if rule["rule"] == "not_null":
            mask = df[col].isnull()
            if mask.any():
                errors.append(f"❌ {msg}")
                bad_records = pd.concat([bad_records, df[mask]])

        elif rule["rule"] == "unique":
            mask = df[col].duplicated(keep=False)
            if mask.any():
                errors.append(f"❌ {msg}")
                bad_records = pd.concat([bad_records, df[mask]])

        elif rule["rule"] == "between":
            min_val, max_val = rule["min"], rule["max"]
            mask = ~df[col].between(min_val, max_val)
            if mask.any():
                errors.append(f"❌ {msg}")
                bad_records = pd.concat([bad_records, df[mask]])

        elif rule["rule"] == "foreign_key":
            ref_table, ref_col = rule["reference"].split(".")
            valid_ids = ref_tables[ref_table][ref_col]
            mask = ~df[col].isin(valid_ids)
            if mask.any():
                errors.append(f"❌ {msg}")
                bad_records = pd.concat([bad_records, df[mask]])

    return errors, bad_records.drop_duplicates()

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    print("🔎 Running Data Quality Checks...")

    rules = load_rules()
    ref_tables = {
        "departments": load_table("departments"),
        "jobs": load_table("jobs"),
        "locations": load_table("locations"),
    }

    employees = load_table("employees")
    errs, bad_emps = apply_rules(employees, rules["employees"], ref_tables)

    if not errs:
        print("✅ Employees passed all checks")
    else:
        print("\n".join(errs))
        if not bad_emps.empty:
            q_path = os.path.join(quarantine_base, "employees_bad_records")
            bad_emps_spark = spark.createDataFrame(bad_emps)
            bad_emps_spark.write.format("delta").mode("overwrite").save(q_path)
            print(f"⚠️ {len(bad_emps)} bad records written to {q_path}")

    print("✅ DQ checks complete")
    spark.stop()
