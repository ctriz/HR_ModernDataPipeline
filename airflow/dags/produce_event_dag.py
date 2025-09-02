from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from producer_event_training import produce_training_events

# Minimal DAG (manual trigger only, no schedule)
with DAG(
    dag_id="training_event_seed_dag",
    start_date=datetime(2025, 8, 31),
    schedule_interval=None,   # run manually
    catchup=False,
    tags=["hr", "kafka", "seed"]
) as dag:

    produce_seed_events = PythonOperator(
        task_id="produce_training_events",
        python_callable=produce_training_events
    )

    produce_seed_events
