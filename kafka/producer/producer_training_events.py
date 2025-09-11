import json
import time
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Kafka + Schema Registry configs
conf = {
    'bootstrap.servers': 'localhost:29092',
    'schema.registry.url': 'http://localhost:8081'
}

# Avro schema (same as before)
value_schema_str = """
{
  "type": "record",
  "name": "TrainingEvent",
  "fields": [
    {"name": "employee_id", "type": "string"},
    {"name": "training_id", "type": "string"},
    {"name": "event_type", "type": {"type": "enum", "name": "EventType", "symbols": ["ENROLLED", "STARTED", "COMPLETED"]}},
    {"name": "event_timestamp", "type": "long"},
    {"name": "deadline_date", "type": "string"}
  ]
}
"""


value_schema = avro.loads(value_schema_str)

producer = AvroProducer(conf, default_value_schema=value_schema)
topic = "hr.training.events"

# Read seed events from JSON file
with open("seed_training_events.json", "r") as f:
    events = json.load(f)

# Produce events
for e in events:
    # Optionally override timestamp with "now"
    # e["event_timestamp"] = int(time.time() * 1000)
    producer.produce(topic=topic, value=e)
    print(f"Produced event: {e}")
    time.sleep(1)  # slow down for demo

producer.flush()
print("All events flushed!")
