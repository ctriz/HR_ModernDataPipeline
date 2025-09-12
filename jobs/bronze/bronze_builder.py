#!/usr/bin/env python3
#src/jobs/bronze/bronze_builder.py
"""
bronze_builder.py
Incremental CDC ingestion from Postgres (hrdbt schema) into Delta Bronze layer.

Tables:
- departments
- jobs
- locations
- employees

CDC logic:
- Default: fetch rows where updated_at > last watermark
- Full refresh (--full-refresh): fetch all rows, overwrite Delta, reset watermark
"""

import os
import argparse
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from config.config import load_config
from utils.db_utils import get_jdbc


# ---------------------------
# CLI Args
# ---------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--full-refresh", action="store_true", help="Load all records and reset watermark")
args = parser.parse_args()
        
# ---------------------------
# Config
# ---------------------------
config = load_config()
if not config:
    raise RuntimeError(" Could not load config.yaml")

# ---------------------------
# DB Connection Details
# ---------------------------
url, props = get_jdbc()

delta_base_path = config["delta"]["base_path"]
bronze_base_path = f"{delta_base_path}/bronze"

from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # go 2 levels up from jobs/bronze
WATERMARK_DIR = PROJECT_ROOT / "watermarks"
WATERMARK_DIR.mkdir(exist_ok=True)


# ---------------------------
# Spark Session
# ---------------------------
spark = (
    SparkSession.builder
        .appName("CDC Ingest HR SCD2")
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
# Helpers
# ---------------------------
def watermark_file(table_name):
    return os.path.join(WATERMARK_DIR, f"{table_name}_watermark.txt")

def get_last_watermark(table_name):
    path = watermark_file(table_name)
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    return "1970-01-01 00:00:00"

def set_last_watermark(table_name, ts):
    with open(watermark_file(table_name), "w") as f:
        f.write(ts)

# ---------------------------
# Ingest Function
# ---------------------------
def ingest_table(table_name: str, full_refresh=False):
    if full_refresh:
        print(f"\n Full refresh for {table_name}")
        query = f"(SELECT * FROM hrdbt.{table_name}) as {table_name}_full"
    else:
        last_ts = get_last_watermark(table_name)
        print(f"\n Processing {table_name} | Last watermark = {last_ts}")
        query = f"(SELECT * FROM hrdbt.{table_name} WHERE updated_at > '{last_ts}') as {table_name}_cdc"

    df = spark.read.jdbc(url, table=query, properties=props)

    if df.count() == 0:
        print(f"No new changes for {table_name}")
        return

    delta_path = f"{bronze_base_path}/{table_name}"

    if full_refresh or not DeltaTable.isDeltaTable(spark, delta_path):
        df.write.format("delta").mode("overwrite").save(delta_path)
        print(f" Wrote {df.count()} rows to {delta_path} (full load)")
    else:
        delta_tbl = DeltaTable.forPath(spark, delta_path)
        (
            delta_tbl.alias("t")
            .merge(df.alias("s"), f"t.{table_name[:-1]}_id = s.{table_name[:-1]}_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f" Merged {df.count()} changes into {delta_path}")

    # Update watermark
    max_ts = df.agg({"updated_at": "max"}).collect()[0][0]
    set_last_watermark(table_name, str(max_ts))
    print(f"⏱ Updated watermark for {table_name}: {max_ts}")

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    tables = ["departments", "jobs", "locations", "employees"]
    for tbl in tables:
        ingest_table(tbl, full_refresh=args.full_refresh)

    print("\n CDC Ingest Complete - Bronze layer updated.")
    spark.stop()
