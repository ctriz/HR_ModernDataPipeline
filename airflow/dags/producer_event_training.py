import json
import time
from datetime import datetime
from kafka import KafkaProducer

def produce_training_events():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic = "hr.training.events"

    training_events = [
        {
            "training_event_id": 1,
            "employee_id": 101,
            "training_name": "Data Privacy Compliance",
            "training_date": "2025-09-05",
            "status": "Scheduled",
            "created_at": datetime.now().isoformat()
        },
        {
            "training_event_id": 2,
            "employee_id": 202,
            "training_name": "Advanced Spark Workshop",
            "training_date": "2025-09-10",
            "status": "Scheduled",
            "created_at": datetime.now().isoformat()
        }
    ]

    for event in training_events:
        producer.send(topic, event)
        print(f"Sent: {event}")
        time.sleep(1)

    producer.flush()
    print("🎉 All seeded training events published successfully.")
