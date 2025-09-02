from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from delta.tables import DeltaTable
from utils.config import load_config

# Load the application configuration
config = load_config()
if not config:
    # Exit if configuration loading failed
    exit()

spark = (SparkSession.builder
    .appName(config['spark']['app_name'])
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master(config['spark']['master'])
    .config("spark.driver.bindAddress", config['spark']['host'])
    .config("spark.driver.host", config['spark']['host'])
    .getOrCreate()
)
spark.sparkContext.setLogLevel(config['spark']['log_level'])
# ---------------------------
# Read and display Delta tables for debugging
# ---------------------------

# ---------------------------
# Paths for Delta tables
# ---------------------------
delta_base_path = config['delta']['base_path']
checkpoint_base_path = config['delta']['checkpoint_base_path']
dept_delta_path = f"{delta_base_path}/dim_department"
fact_delta_path = f"{delta_base_path}/fact_hr_snapshot"
dept_chk_path = f"{checkpoint_base_path}/dim_department"
salary_chk_path = f"{checkpoint_base_path}/fact_hr_snapshot"

print(f"\nReading Delta table from: {dept_delta_path}")
df= spark.read.format("delta").load(dept_delta_path)
print("\n=== Dimension Department Table ===")
print("=== Show all rows  ===")
df.show(truncate=False) # see all rows
print("\n=== Show schema ===")
df.printSchema()
print("\n=== Current records ===")
df.filter("is_current = true").show(truncate=False) # see current rows
print("\n=== Historical records ===")
df.filter("is_current = false").show(truncate=False) # see historical rows



print(f"\nReading Delta table from: {fact_delta_path}")
print("\n=== Fact HR Snapshot Table ===")
df = spark.read.format("delta").load(fact_delta_path)
print("\n=== Show all rows ===")
df.show(truncate=False) # see all rows

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

w = Window.partitionBy("employee_id").orderBy(desc("date_key"))

latest = (df.withColumn("rn", row_number().over(w))
            .filter("rn = 1")
            .drop("rn"))
latest.show(truncate=False)
print("\n=== Show latest rows per employee_id ===")
latest.show(truncate=False) # see all rows
spark.stop()