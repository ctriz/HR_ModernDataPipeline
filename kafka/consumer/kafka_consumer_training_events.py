from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

spark = SparkSession.builder \
    .appName("TrainingEventsToDelta") \
    .getOrCreate()

# Define Spark schema matching JSON Schema (adjust types to your exact fields)
training_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_ts", StringType(), nullable=False),   # or TimestampType if you convert
    StructField("employee_id", IntegerType(), nullable=False),
    StructField("course_id", StringType(), nullable=False),
    StructField("course_name", StringType(), nullable=False),
    StructField("completion_date", StringType(), nullable=False),
    StructField("is_mandatory", BooleanType(), nullable=True),
    StructField("actor", StructType([
        StructField("user_id", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True)
    ]), nullable=True),
    StructField("context", StructType([
        StructField("employee_name", StringType(), nullable=True),
        StructField("manager_emp_id", IntegerType(), nullable=True),
        StructField("org_unit_id", IntegerType(), nullable=True)
    ]), nullable=True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hr.employee_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka value is binary; cast to string then parse json
json_df = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")

events = json_df.withColumn("data", from_json(col("json_str"), training_schema)).select("data.*", "kafka_ts")

# (Optional) cast event_ts string to timestamp if needed:
from pyspark.sql.functions import to_timestamp
events = events.withColumn("event_ts_ts", to_timestamp(col("event_ts")))

# Write to Delta Bronze

# Write to Delta Bronze (hardcoded Windows path)
events.writeStream \
    .format("delta") \
    .option("checkpointLocation", "C:/UpScale/HRProject/HRDataPipeline/analytics/delta/bronze/_checkpoints/training_events") \
    .outputMode("append") \
    .start("C:/UpScale/HRProject/HRDataPipeline/analytics/delta/bronze/hr_training_events") 
