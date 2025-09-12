#!/usr/bin/env python3
##src/jobs/bootstrap/bootstrap_ingest.py
"""
bootstrap_ingest.py
One-time full load of HR tables from Postgres into Delta Bronze layer.

Tables:
- departments
- jobs
- locations
- employees
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession
from config.config import load_config
from utils.db_utils import get_jdbc


# --- Config ---
SCHEMA = "hrdbt"
# keep watermarks in a dedicated folder at project root
WATERMARK_FILE = Path("watermarks") / "last_watermark.txt"

# ensure folder exists
WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)


# ---------------------------
# Load Config
# ---------------------------
config = load_config()
if not config:
    raise RuntimeError(" Could not load config.yaml")

delta_base_path = config["delta"]["base_path"]
bronze_base_path = f"{delta_base_path}/bronze"

# ---------------------------
# Spark Session
# ---------------------------


spark = (
    SparkSession.builder
        .appName("CDC Ingest Employees SCD2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master(config['spark']['master'])
        .config("spark.driver.bindAddress", config['spark']['host']) # Networking fixes
        .config("spark.driver.host", config['spark']['host'])
        # Extra configs to stabilize streaming
        .config("spark.executor.heartbeatInterval", "30s")
        .config("spark.network.timeout", "120s")
        # # Required JARs for Delta + Postgres
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0")
        .getOrCreate()

)

spark.sparkContext.setLogLevel(config['spark']['log_level'])

# ---------------------------
# DB Connection Details
# ---------------------------
url, props = get_jdbc()

# ---------------------------
# Ingest Function
# ---------------------------
def ingest_table(table_name: str):
    query = f"(SELECT * FROM hrdbt.{table_name}) as {table_name}"
    print(f" Reading {table_name} from Postgres...")
    df = spark.read.jdbc(url, query, properties=props)

    delta_path = f"{bronze_base_path}/{table_name}"
    print(f" Writing {table_name} to Delta → {delta_path}")
    df.write.format("delta").mode("overwrite").save(delta_path)

    print(f" Finished {table_name}: {df.count()} rows")

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    tables = ["departments", "jobs", "locations", "employees"]
    for tbl in tables:
        ingest_table(tbl)

    print("\n Bootstrap Ingest Complete! Bronze layer is ready.")
    spark.stop()
