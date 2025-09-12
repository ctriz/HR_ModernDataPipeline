#!/usr/bin/env python3
"""
silver_builder.py
Build Silver layer (SCD2 history tables) from Bronze Delta tables.

Features:
- Processes employees, departments, jobs, locations
- Uses watermark on updated_at to ingest only changed rows
- Adds SCD2 columns (effective_start_date, effective_end_date, is_current)
- Supports --full-refresh to rebuild entire Silver table
"""

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable
from config.config import load_config
from utils.db_utils import get_jdbc

# ---------------------------
# CLI Args
# ---------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--full-refresh", action="store_true", help="Rebuild Silver tables from scratch")
args = parser.parse_args()

# ---------------------------
# Config    

config = load_config()
if not config:
    raise RuntimeError(" Could not load config.yaml")

# ---------------------------
# Spark Session
# ---------------------------
spark = (
    SparkSession.builder
    .appName("HR Silver Builder")
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
# Config
# ---------------------------


delta_base_path = config["delta"]["base_path"]
bronze_base = f"{delta_base_path}/bronze"
silver_base = f"{delta_base_path}/silver"

WATERMARK_DIR = "watermarks_silver"
os.makedirs(WATERMARK_DIR, exist_ok=True)

# ---------------------------
# Watermark helpers
# ---------------------------
def watermark_file(table_name):
    return os.path.join(WATERMARK_DIR, f"{table_name}_silver_watermark.txt")

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
# Generic SCD2 Builder
# ---------------------------
def build_scd2(table_name: str, pk: str, tracked_cols: list, full_refresh=False):
    bronze_path = f"{bronze_base}/{table_name}"
    silver_path = f"{silver_base}/{table_name}_scd2"

    print(f"\n Processing {table_name} (full_refresh={full_refresh})")

    if full_refresh:
        bronze_df = spark.read.format("delta").load(bronze_path)
    else:
        last_ts = get_last_watermark(table_name)
        print(f" Using watermark for {table_name}: {last_ts}")
        bronze_df = spark.read.format("delta").load(bronze_path).filter(col("updated_at") > last_ts)

    if bronze_df.count() == 0:
        print(f"ℹ No new changes for {table_name}")
        return

    scd2_df = (
        bronze_df
        .withColumn("effective_start_date", col("updated_at"))
        .withColumn("effective_end_date", lit("2099-12-31").cast("timestamp"))
        .withColumn("is_current", lit(True))
    )

    if full_refresh or not DeltaTable.isDeltaTable(spark, silver_path):
        scd2_df.write.format("delta").mode("overwrite").save(silver_path)
        print(f" Created/Overwritten Silver {table_name}_scd2 with {scd2_df.count()} rows")
    else:
        silver_tbl = DeltaTable.forPath(spark, silver_path)
        change_cond = " OR ".join([f"t.{c} <> s.{c}" for c in tracked_cols])

        (
            silver_tbl.alias("t")
            .merge(
                scd2_df.alias("s"),
                f"t.{pk} = s.{pk} AND t.is_current = true"
            )
            .whenMatchedUpdate(
                condition=change_cond,
                set={
                    "effective_end_date": "s.updated_at",
                    "is_current": "false"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f" Updated Silver {table_name}_scd2 with {scd2_df.count()} new/changed rows")

    # Update watermark
    max_ts = scd2_df.agg({"effective_start_date": "max"}).collect()[0][0]
    set_last_watermark(table_name, str(max_ts))
    print(f"⏱️ Updated watermark for {table_name}: {max_ts}")

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    build_scd2(
        table_name="employees",
        pk="employee_id",
        tracked_cols=["salary", "department_id", "job_id", "location_id", "is_deleted"],
        full_refresh=args.full_refresh,
    )
    build_scd2(
        table_name="departments",
        pk="department_id",
        tracked_cols=["department_code", "name", "is_deleted"],
        full_refresh=args.full_refresh,
    )
    build_scd2(
        table_name="jobs",
        pk="job_id",
        tracked_cols=["job_code", "title", "grade", "is_deleted"],
        full_refresh=args.full_refresh,
    )
    build_scd2(
        table_name="locations",
        pk="location_id",
        tracked_cols=["location_code", "name", "country", "is_deleted"],
        full_refresh=args.full_refresh,
    )

    print("\n Silver layer build complete.")
    spark.stop()
