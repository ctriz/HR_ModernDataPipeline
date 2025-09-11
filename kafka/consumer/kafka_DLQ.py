# spark_consumer_contractors_to_delta_with_dlq.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# ---------- configuration (hard-coded as requested) ----------
kafka_bootstrap = "localhost:9092"
input_topic = "hr.contractor_signup"
dlq_topic = "hr.contractor_signup.dlq"
delta_out_path = r"C:\UpScale\HRProject\HRDataPipeline\analytics\delta\bronze\contractor_signup"
checkpoint_loc = r"C:\UpScale\HRProject\HRDataPipeline\kafka\consumer\checkpoints\contractor_signup"
dlq_checkpoint = r"C:\UpScale\HRProject\HRDataPipeline\kafka\consumer\checkpoints\contractor_signup_dlq"

# choose dlq sink type: "kafka" or "file"
dlq_sink_type = "kafka"   # or "file"
dlq_file_path = r"C:\UpScale\HRProject\HRDataPipeline\analytics\delta\dlq\contractor_signup_invalid"

# ---------- define the expected schema ----------
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_ts", StringType(), False),  # you can parse to TimestampType later if desired
    StructField("worker_id", IntegerType(), False),
    StructField("worker_company", StringType(), False),
    StructField("worker_location", StringType(), False),
    StructField("worker_tenure", DoubleType(), True)
])

# ---------- create Spark session ----------
spark = SparkSession.builder \
    .appName("contractor-signup-to-delta-with-dlq") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.schemaInference", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- read raw messages from Kafka ----------
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# raw.value is binary; convert to string
raw_str = raw.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value", "timestamp")

# ---------- parse JSON using the schema -----------
# from_json returns null on invalid JSON or on structural mismatch.
parsed = raw_str.withColumn("parsed", from_json(col("value"), schema))

# valid rows: parsed is not null
valid = parsed.filter(col("parsed").isNotNull()).selectExpr("parsed.*", "key AS kafka_key", "timestamp AS kafka_ts")

# invalid rows: parsed is null -> keep original payload and metadata
invalid = parsed.filter(col("parsed").isNull()).select("key", "value", "timestamp")

# ---------- write valid rows to Delta (append) ----------
valid_write = valid.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_loc) \
    .start(delta_out_path)

# ---------- write invalid rows to DLQ ----------
if dlq_sink_type == "kafka":
    # write invalid messages to DLQ Kafka topic as JSON with original key and value + reason
    dlq_msg = invalid.selectExpr(
        "CAST(key AS STRING) AS key",
        "to_json(named_struct('value', value, 'kafka_ts', timestamp)) AS value"
    )
    dlq_writer = dlq_msg \
        .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", dlq_topic) \
        .option("checkpointLocation", dlq_checkpoint) \
        .start()
else:
    # write invalid messages to files (one file per microbatch)
    # Save as JSON lines: { "key": "...", "value": "...", "timestamp": "..." }
    invalid_out = invalid \
        .selectExpr("CAST(key AS STRING) AS key", "value AS value", "timestamp") \
        .writeStream \
        .format("json") \
        .option("path", dlq_file_path) \
        .option("checkpointLocation", dlq_checkpoint) \
        .outputMode("append") \
        .start()

# ---------- await termination ----------
spark.streams.awaitAnyTermination()
