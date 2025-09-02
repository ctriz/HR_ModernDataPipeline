from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("TrainingEventsToDelta") \
    .getOrCreate()

# Avro schema (can be loaded from file too)
training_schema = """
{
  "type": "record",
  "name": "TrainingEvent",
  "fields": [
    {"name": "employee_id", "type": "string"},
    {"name": "training_id", "type": "string"},
    {"name": "event_type", "type": {"type": "enum", "symbols": ["ENROLLED", "STARTED", "COMPLETED"]}},
    {"name": "event_timestamp", "type": "long"},
    {"name": "deadline_date", "type": "string"}
  ]
}
"""

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hr.training.events") \
    .load()

# Decode Avro using Schema Registry
events = df.select(from_avro(col("value"), training_schema).alias("data")) \
           .select("data.*")

# Write to Delta Bronze
events.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/training_events") \
    .start("/delta/bronze/hr_training_events")
