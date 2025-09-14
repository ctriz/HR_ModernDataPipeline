
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from delta.tables import DeltaTable

import os, sys



# Add the parent directory to the system path to allow imports from sibling directories.
# This assumes the script is run from within the 'test' directory.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load the application configuration from the YAML file


from utils.config import load_config


# Load the application configuration
config = load_config()
if not config:
    # Exit if configuration loading failed
    exit()


# Create SparkSession with Kafka package
spark = (SparkSession.builder
    .appName(config['spark']['app_name'])
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master(config['spark']['master'])
    .config("spark.driver.bindAddress", config['spark']['host'])
    .config("spark.driver.host", config['spark']['host'])
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Kafka source
# ---------------------------
kafka_brokers = config['kafka']['brokers']
cdc_department_topic = config['kafka']['topics']['departments']
cdc_salary_topic = config['kafka']['topics']['salaries']

# ---------------------------
# Schema for CDC payload
# ---------------------------

department_schema = StructType([
    StructField("department_id", IntegerType()),
    StructField("department_code", StringType()),
    StructField("name", StringType()),
    StructField("created_at", StringType()),   # or TimestampType if you want parsing
    StructField("updated_at", StringType())
])


salary_schema = StructType([
    StructField("employee_id", IntegerType()),
    StructField("salary", DoubleType()),
    StructField("effective_date", StringType())
])

# Department Stream
dept_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_brokers)
    .option("subscribe", cdc_department_topic)
    .option("startingOffsets", "earliest")
    .load()
)

# Salary Stream
salary_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_brokers)
    .option("subscribe", cdc_salary_topic)
    .option("startingOffsets", "earliest")
    .load()
)

from pyspark.sql.functions import get_json_object

# Transform the Kafka stream to extract JSON and apply schema


from pyspark.sql.functions import get_json_object

dept_df = (
    dept_stream.selectExpr("CAST(value AS STRING) as json")
        .select(
            get_json_object(col("json"), "$.payload.after.department_id").cast("int").alias("department_id"),
            get_json_object(col("json"), "$.payload.after.department_code").alias("department_code"),
            get_json_object(col("json"), "$.payload.after.name").alias("department_name"),
            get_json_object(col("json"), "$.payload.after.created_at").alias("created_at"),
            get_json_object(col("json"), "$.payload.after.updated_at").alias("updated_at")
        )
)

print("Department DataFrame Schema:")

dept_df.printSchema()
dept_df.writeStream.format("console").option()start()

dept_query = (
    dept_df.writeStream
        .foreachBatch(upsert_department_to_delta)
        .outputMode("update")
        .option("checkpointLocation", dept_chk_path)  # ✅ stable checkpoint
        .start()
)

salary_df = (
    salary_stream.selectExpr("CAST(value AS STRING) as json")
        .select(
            get_json_object(col("json"), "$.payload.after.employee_id").cast("int").alias("employee_id"),
            get_json_object(col("json"), "$.payload.after.salary").cast("double").alias("salary"),
            get_json_object(col("json"), "$.payload.after.effective_date").alias("effective_date")
        )
)
print("Salary DataFrame Schema:")
salary_df.printSchema()
salary_df.writeStream.format("console").start()


#dept_stream.selectExpr("CAST(value AS STRING)").writeStream.format("console").start()

dept_df.writeStream.format("console").outputMode("append").start()
salary_df.writeStream.format("console").outputMode("append").start()
spark.stop()


 

