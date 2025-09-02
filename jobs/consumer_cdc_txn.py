# consumer_cdc_txn.py
# Consumes CDC data from Kafka topics and writes to Delta Lake
# - Departments: SCD2 merge
# - Salaries: snapshot fact append

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, to_date, current_timestamp, lit
from delta.tables import DeltaTable
from utils.config import load_config
from pyspark.sql.functions import expr

# ---------------------------
# Load config
# ---------------------------
config = load_config()
if not config:
    exit("Failed to load config")

# ---------------------------
# Spark Session with Delta
# ---------------------------
spark = (
    SparkSession.builder
        .appName(config['spark']['app_name'])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master(config['spark']['master'])
        .config("spark.driver.bindAddress", config['spark']['host'])
        .config("spark.driver.host", config['spark']['host'])
        # Extra configs to stabilize streaming
        .config("spark.executor.heartbeatInterval", "30s")
        .config("spark.network.timeout", "120s")
        .getOrCreate()
)

spark.sparkContext.setLogLevel(config['spark']['log_level'])

# ---------------------------
# Kafka source
# ---------------------------
kafka_brokers = config['kafka']['brokers']
cdc_department_topic = config['kafka']['topics']['departments']
cdc_salary_topic = config['kafka']['topics']['salaries']

# ---------------------------
# Department Stream (now flat JSON)
# ---------------------------
dept_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", cdc_department_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
)

dept_df = (
    dept_stream.selectExpr("CAST(value AS STRING) as json")
        .select(
            get_json_object(col("json"), "$.department_id").cast("int").alias("department_id"),
            get_json_object(col("json"), "$.department_code").alias("department_code"),
            get_json_object(col("json"), "$.name").alias("department_name"),
            get_json_object(col("json"), "$.created_at").alias("created_at"),
            get_json_object(col("json"), "$.updated_at").alias("updated_at")
        )
)

# ---------------------------
# Salary Stream (now flat JSON)
# ---------------------------
salary_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", cdc_salary_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
)

salary_df = (
    salary_stream.selectExpr("CAST(value AS STRING) as json")
        .select(
            get_json_object(col("json"), "$.comp_id").cast("long").alias("comp_id"),
            get_json_object(col("json"), "$.employee_id").cast("long").alias("employee_id"),
            get_json_object(col("json"), "$.base_salary").cast("double").alias("base_salary"),
            get_json_object(col("json"), "$.bonus").cast("double").alias("bonus"),
            get_json_object(col("json"), "$.currency").alias("currency"),
            get_json_object(col("json"), "$.effective_from").alias("effective_from"),
            get_json_object(col("json"), "$.effective_to").alias("effective_to"),
            get_json_object(col("json"), "$.created_at").alias("created_at"),
            get_json_object(col("json"), "$.updated_at").alias("updated_at")
        )
)

salary_df = salary_df.withColumn(
    "effective_from",
    expr("date_add('1970-01-01', effective_from)")
)

# ---------------------------
# Paths for Delta tables
# ---------------------------
delta_base_path = config['delta']['base_path']
checkpoint_base_path = config['delta']['checkpoint_base_path']
dept_delta_path = f"{delta_base_path}/dim_department"
fact_delta_path = f"{delta_base_path}/fact_hr_snapshot"
dept_chk_path = f"{checkpoint_base_path}/dim_department"
salary_chk_path = f"{checkpoint_base_path}/fact_hr_snapshot"

# ---------------------------
# Function: SCD2 merge for departments
# ---------------------------
def upsert_department_to_delta(batch_df, batch_id):
    if not batch_df.isEmpty():
        staged = (
            batch_df
                .withColumn("effective_from", current_timestamp())
                .withColumn("effective_to", lit("9999-12-31"))
                .withColumn("is_current", lit(True))
        )

        if DeltaTable.isDeltaTable(spark, dept_delta_path):
            delta_table = DeltaTable.forPath(spark, dept_delta_path)

            (
                delta_table.alias("t")
                    .merge(
                        staged.alias("s"),
                        "t.department_id = s.department_id AND t.is_current = true"
                    )
                    .whenMatchedUpdate(set={
                        "effective_to": "s.effective_from",
                        "is_current": "false"
                    })
                    .whenNotMatchedInsertAll()
                    .execute()
            )
        else:
            staged.write.format("delta").mode("overwrite").save(dept_delta_path)

# ---------------------------
# Function: Fact snapshot append
# ---------------------------
def append_salary_snapshot(batch_df, batch_id):
    if not batch_df.isEmpty():
        staged = (
            batch_df
                .withColumn("date_key", to_date(col("effective_from")))  # if you want date
                .withColumn("load_ts", current_timestamp())
        )
        staged.write.format("delta").mode("append").save(fact_delta_path)

# ---------------------------
# Write Streams
# ---------------------------
dept_query = (
    dept_df.writeStream
        .foreachBatch(upsert_department_to_delta)
        .outputMode("update")
        .option("checkpointLocation", dept_chk_path)
        .start()
)

salary_query = (
    salary_df.writeStream
        .foreachBatch(append_salary_snapshot)
        .outputMode("append")
        .option("checkpointLocation", salary_chk_path)
        .start()
)

dept_query.awaitTermination()
salary_query.awaitTermination()

spark.stop()
