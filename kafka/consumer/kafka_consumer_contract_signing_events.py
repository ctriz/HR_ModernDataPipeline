# kafka_consumer_contract_signing_debug.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time
import sys

# ------- Config (hard-coded paths) -------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "hr.contractor_signup"
DELTA_PATH = r"C:/UpScale/HRProject/HRDataPipeline/analytics/delta/bronze/hr_contractor_signup"
CHECKPOINT = r"C:/UpScale/HRProject/HRDataPipeline/analytics/delta/bronze/_checkpoints/contractor_signup_debug"
STARTING_OFFSETS = "earliest"   # change to "latest" if you only want new messages

# ------- Spark session -------
spark = SparkSession.builder \
    .appName("ContractorSignUpToDelta_Debug") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")   # INFO or DEBUG for more logs

# ------- Schema (match your JSON schema) -------
contractor_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_ts", StringType(), nullable=False),
    StructField("worker_id", IntegerType(), nullable=False),
    StructField("worker_company", StringType(), nullable=False),
    StructField("worker_location", StringType(), nullable=False),
    StructField("worker_tenure", DoubleType(), nullable=True)
])

# ------- Read from Kafka -------
kdf = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", STARTING_OFFSETS) \
    .option("failOnDataLoss", "false") \
    .load()

# Cast value to string and parse JSON
json_df = kdf.selectExpr("CAST(value AS STRING) as json_str", "CAST(key AS STRING) as key_str", "timestamp as kafka_ts")

parsed = json_df.withColumn("data", from_json(col("json_str"), contractor_schema)) \
    .select("key_str", "json_str", "kafka_ts", "data.*")

# Cast event_ts to timestamp for convenience
parsed = parsed.withColumn("event_ts_ts", to_timestamp(col("event_ts")))

# ------- Two sinks: console (for debugging) AND delta (for persistence) -------
# Console sink (prints rows to stdout)
console_query = parsed.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .start()

# Delta sink (append)
delta_query = parsed.writeStream \
    .format("delta") \
    .option("checkpointLocation", CHECKPOINT) \
    .outputMode("append") \
    .start(DELTA_PATH)

# ------- Monitor the query, print status/progress periodically -------
print("Started streams. Console query id:", console_query.id, " Delta query id:", delta_query.id)
try:
    while True:
        # print high-level status and last progress
        for q in spark.streams.active:
            print("---- QUERY:", q.id, "Name:", q.name)
            print("  isActive:", q.isActive)
            st = q.status
            print("  status:", st)
            lp = q.lastProgress
            print("  lastProgress (summary):", ("None" if lp is None else {k: lp.get(k) for k in ["id", "runId", "numInputRows"] if k in lp}))
        print("Sleeping 5s ... (Ctrl-C to stop)")
        time.sleep(5)
except KeyboardInterrupt:
    print("Stopping streams...")
finally:
    console_query.stop()
    delta_query.stop()
    spark.stop()
    print("Stopped.")
    sys.exit(0)
