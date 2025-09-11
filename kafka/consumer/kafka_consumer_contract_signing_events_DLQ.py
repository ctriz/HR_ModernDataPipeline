# kafka_consumer_contract_signing_debug.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time
import sys
from config.config import load_config
from utils.db_utils import get_jdbc

# ---------------------------
# Config
# ---------------------------
config = load_config()
if not config:
    raise RuntimeError(" Could not load config.yaml")

# ------- Config (hard-coded paths) -------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "hr.contractor_signup"
DLQ_TOPIC = "hr.contractor_signup.dlq"
DELTA_PATH = r"C:/UpScale/HRProject/HRDataPipeline/analytics/delta/bronze/hr_contractor_signup"
CHECKPOINT = r"C:/UpScale/HRProject/HRDataPipeline/analytics/delta/bronze/_checkpoints/contractor_signup_debug"
DLQ_CHECKPOINT = r"C:\UpScale\HRProject\HRDataPipeline\kafka\consumer\checkpoints\contractor_signup_dlq"
STARTING_OFFSETS = "earliest"   # change to "latest" if you only want new messages

# choose dlq sink type: "kafka" or "file"
DLQ_SINK_TYPE = "KAFKA"   # or "file"
DLQ_FILE_PATH = r"C:\UpScale\HRProject\HRDataPipeline\analytics\delta\dlq\contractor_signup_invalid"



# ------- Spark session -------


spark = (
    SparkSession.builder
        .appName("ContractorSignUpToDelta_DLQ")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master(config['spark']['master'])
        .config("spark.driver.bindAddress", config['spark']['host'])
        .config("spark.driver.host", config['spark']['host'])
        # Extra configs to stabilize streaming
        .config("spark.executor.heartbeatInterval", "30s")
        .config("spark.network.timeout", "120s")
        # JARS
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.1")
        .getOrCreate()
)






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

# keep the parsed object as a single column named "data"
parsed = json_df.withColumn("data", from_json(col("json_str"), contractor_schema)) \
                .select("key_str", "json_str", "kafka_ts", "data")

# valid rows = data is not null
valid = parsed.filter(col("data").isNotNull()) \
              .selectExpr("key_str AS key", "json_str AS value", "data.*", "kafka_ts") \
              .withColumn("event_ts_ts", to_timestamp(col("event_ts")))
           
              
# invalid rows = data is null
invalid = parsed.filter(col("data").isNull()) \
                .selectExpr("key_str AS key", "json_str AS value", "kafka_ts")




# ------- Two sinks: console (for debugging) AND delta (for persistence) -------
# Console sink (prints rows to stdout)
console_query = valid.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .start()

# Delta sink (append)
delta_query = valid.writeStream \
    .format("delta") \
    .option("checkpointLocation", CHECKPOINT) \
    .outputMode("append") \
    .start(DELTA_PATH)
    
# ---------- write invalid rows to DLQ ----------

if DLQ_SINK_TYPE == "KAFKA":
    # write invalid messages to DLQ Kafka topic as JSON with original key and value + kafka_ts
    dlq_msg = invalid.selectExpr(
        "CAST(key AS STRING) AS key",
        # use kafka_ts, not timestamp
        "to_json(named_struct('value', value, 'kafka_ts', kafka_ts,'reason', 'schema_parse_failed')) AS value"
    )

    dlq_writer = dlq_msg \
        .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", DLQ_TOPIC) \
        .option("checkpointLocation", DLQ_CHECKPOINT) \
        .start()

else:
    # write invalid messages to files (one file per microbatch)
    # Save as JSON lines: { "key": "...", "value": "...", "kafka_ts": "..." }
    invalid_out = invalid \
        .selectExpr("CAST(key AS STRING) AS key", "value AS value", "kafka_ts AS kafka_ts") \
        .writeStream \
        .format("json") \
        .option("path", DLQ_FILE_PATH) \
        .option("checkpointLocation", DLQ_CHECKPOINT) \
        .outputMode("append") \
        .start()

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
