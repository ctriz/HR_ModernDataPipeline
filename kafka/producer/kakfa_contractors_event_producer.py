import os
import uuid
import random
from datetime import datetime, timezone
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

# Configuration - change if needed
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "hr.contractor_signup")

# Load schema file
with open("contractor_sign_up.json", "r", encoding="utf-8") as f:
    json_schema_str = f.read()

# Schema Registry client
schema_registry_conf = {"url": SR_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Helper to convert Python object to a dict expected by JSONSerializer
def contractor_to_dict(obj, ctx):
    # obj is already a dict; ensure types are correct
    return {
        "event_id": obj["event_id"],
        "event_ts": obj["event_ts"],
        "worker_id": int(obj["worker_id"]),
        "worker_company": obj["worker_company"],
        "worker_location": obj["worker_location"],
        # worker_tenure is optional; include when present as number
        **({"worker_tenure": float(obj["worker_tenure"])} if obj.get("worker_tenure") is not None else {})
    }

# Create JSONSerializer
json_serializer = JSONSerializer(json_schema_str, schema_registry_client, contractor_to_dict)

# Producer config: value serializer is the JSONSchema serializer
producer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": json_serializer
}

producer = SerializingProducer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print("❌ Delivery failed:", err)
    else:
        print(f"✅ Delivered key={msg.key()} to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def make_event(worker_id=None):
    now = datetime.now(timezone.utc).isoformat()
    wid = worker_id if worker_id is not None else random.randint(1000, 9999)
    event = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now,
        "worker_id": wid,
        "worker_company": random.choice(["AcmeCorp", "BuildersInc", "QuickFix Ltd"]),
        "worker_location": random.choice(["Bengaluru", "Pune", "Remote", "Delhi"]),
        # produce worker_tenure occasionally to show optional field
        **({"worker_tenure": round(random.random() * 5, 2)} if random.random() < 0.6 else {})
    }
    return event

def produce_n(n=1, sleep=0.2):
    for _ in range(n):
        ev = make_event()
        key = str(ev["worker_id"])
        try:
            producer.produce(topic=TOPIC, key=key, value=ev, on_delivery=delivery_report)
        except Exception as e:
            print("Producer error:", e)
        producer.poll(0)  # serve delivery callbacks
        time.sleep(sleep)
    producer.flush(10)

if __name__ == "__main__":
    produce_n(10)
