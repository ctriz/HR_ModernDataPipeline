#!/usr/bin/env python3
"""
silver_attrition_builder.py
Builds pre-Gold Silver Fact tables for attrition analysis:
1. fact_employee_attrition_events
2. fact_employee_headcount

Adds synthetic HR features for BI/ML exploration:
- marital_status
- persona
- travel_time
- age
- overtime
- last_appraisal_score
- years_at_company
- years_in_current_role
- last_promotion_yrs_ago
"""

import os, sys
#detect Python exeutables
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import random
from datetime import datetime
from pyspark.sql import SparkSession
from config.config import load_config
from pyspark.sql.functions import (
    col, lit, year, month, datediff, expr, trunc
)
from pyspark.sql.types import StringType, IntegerType
from config.config import load_config
from utils.db_utils import get_jdbc


config = load_config()
if not config:
    raise RuntimeError(" Could not load config.yaml")

# ---------------------------
# Spark Session
# ---------------------------
spark = (
    SparkSession.builder
    .appName("Silver Attrition Builder")
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
# Paths
# ---------------------------
delta_base = config["delta"]["base_path"]
silver_emps = f"{delta_base}/silver/employees_scd2"
silver_fact_attrition = f"{delta_base}/silver/fact_employee_attrition_events"
silver_fact_headcount = f"{delta_base}/silver/fact_employee_headcount"

# ---------------------------
# Synthetic attribute generators
# ---------------------------
def random_marital(): return random.choice(["Single", "Married"])
def random_persona(): return random.choice(["Remote", "Office"])
def random_travel(): return random.choice([1, 2, 3])
def random_age(): return random.randint(22, 55)
def random_overtime(): return random.choice(["Yes", "No"])
def random_appraisal(): return random.randint(1, 5)
def random_years(): return random.randint(0, 20)

# Register as UDFs
spark.udf.register("rand_marital", random_marital, StringType())
spark.udf.register("rand_persona", random_persona, StringType())
spark.udf.register("rand_travel", random_travel, IntegerType())
spark.udf.register("rand_age", random_age, IntegerType())
spark.udf.register("rand_overtime", random_overtime, StringType())
spark.udf.register("rand_appraisal", random_appraisal, IntegerType())
spark.udf.register("rand_years", random_years, IntegerType())

# ---------------------------
# Load Silver Employees
# ---------------------------
df = spark.read.format("delta").load(silver_emps)

# ---------------------------
# 1. Attrition Events Fact
# ---------------------------
attritions = (
    df.filter((col("is_deleted") == True) & (col("is_current") == False))
    .withColumn("tenure_days", datediff(col("effective_end_date"), col("effective_start_date")))
    .withColumn("attrition_year", year(col("effective_end_date")))
    .withColumn("attrition_month", month(col("effective_end_date")))
    # Add synthetic features
    .withColumn("marital_status", expr("rand_marital()"))
    .withColumn("persona", expr("rand_persona()"))
    .withColumn("travel_time_hrs", expr("rand_travel()"))
    .withColumn("age", expr("rand_age()"))
    .withColumn("overtime", expr("rand_overtime()"))
    .withColumn("last_appraisal_score", expr("rand_appraisal()"))
    .withColumn("years_at_company", expr("rand_years()"))
    .withColumn("years_in_current_role", expr("rand_years()"))
    .withColumn("last_promotion_yrs_ago", expr("rand_years()"))
)

attritions.write.format("delta").mode("overwrite").save(silver_fact_attrition)
print(f" fact_employee_attrition_events built → {silver_fact_attrition}")

# ---------------------------
# 2. Monthly Headcount Fact
# ---------------------------
# Build monthly snapshots based on effective date ranges
headcount = (
    df.withColumn("month", trunc(col("effective_start_date"), "MM"))
    .withColumn("snapshot_year", year(col("month")))
    .withColumn("snapshot_month", month(col("month")))
    .withColumn("is_active", col("is_current"))
    # Synthetic features
    .withColumn("marital_status", expr("rand_marital()"))
    .withColumn("persona", expr("rand_persona()"))
    .withColumn("travel_time_hrs", expr("rand_travel()"))
    .withColumn("age", expr("rand_age()"))
    .withColumn("overtime", expr("rand_overtime()"))
    .withColumn("last_appraisal_score", expr("rand_appraisal()"))
    .withColumn("years_at_company", expr("rand_years()"))
    .withColumn("years_in_current_role", expr("rand_years()"))
    .withColumn("last_promotion_yrs_ago", expr("rand_years()"))
)

headcount.write.format("delta").mode("overwrite").save(silver_fact_headcount)
print(f" fact_employee_headcount built → {silver_fact_headcount}")

spark.stop()
