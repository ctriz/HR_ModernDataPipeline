


# HR Data Pipeline:  Ingestion Framework

This document outlines the initial phase of the HR data pipeline, focusing on the ingestion of data from a transactional database into the raw Bronze layer of a Delta Lake.

## Architecture

The pipeline begins with a **Postgres** OLTP database, where changes are captured using **Debezium** and streamed to **Kafka**. **Apache Airflow** orchestrates this process, and a **Spark Structured Streaming** job reads the data from Kafka and lands it into the **Bronze Layer** of the Delta Lake. Spark continues to be the choice of transformation from the Bronze to the Silver layer, offering heavy lifting of cleaning up the table, maintaing the history. It also creates aggregated pre-Gold tables for easier SQL like consumption. Finally, core dbt is used for building business friendly SQL queries like aggregates and joins. 

<img width="627" height="582" alt="HR Data   Analytics drawio (4)" src="https://github.com/user-attachments/assets/577fa8ee-a8e4-40fb-9b41-05579e6790db" />


----------


## Core Components

-   **PostgreSQL OLTP**: The source transactional database where employee data is stored.
    
-   **Debezium & Kafka Connect**: A distributed platform used to capture real-time changes from the Postgres database. The `debezium-postgres-connector.json` configuration specifies the two tables to be monitored: `hr_txn.departments` and `hr_txn.employee_compensation`.
    
-   **Kafka**: The central messaging platform that acts as an event log, decoupling the source database from the downstream consumers. The `docker-compose.yml` file defines the Kafka and Schema Registry services.
    
-   **Apache Spark**: The processing engine for ingesting the real-time data from Kafka. The `consumer_cdc_txn.py` application reads the Kafka topics.
    
-   **Delta Lake (Bronze Layer)**: This serves as the raw data landing zone for the pipeline.
    
-   **Apache Airflow**: The orchestration engine. The `cdc_pipeline_dag.py` DAG is responsible for simulating transactions in Postgres and triggering the Spark job to consume the CDC events.
    

----------

## Data Flow & Pipeline Logic

The process is managed by the `cdc_pipeline_dag.py` Airflow DAG.

1.  **Simulate Transactions**: The `mutate_postgres` task in the Airflow DAG runs a Python script that simulates data changes in the Postgres OLTP database.
    
2.  **CDC & Ingestion**: Debezium captures these changes and publishes them as events to Kafka topics (`hr_txn.hr_txn.departments` and `hr_txn.hr_txn.employee_compensation`).
    
3.  **Spark Processing**: The `consume_cdc.py` Spark Structured Streaming application reads these events directly from Kafka.
    
4.  **Write to Delta Lake Bronze**: The Spark application writes the raw, un-transformed data to the Bronze layer of the Delta Lake. The process handles two different data types:
    
    -   **Departments**: Data is written to the `dim_department` table using a Type 2 Slowly Changing Dimension (SCD2) merge strategy, which preserves a complete history of all changes.
        
    -   **Employee Compensation**: Data is treated as a fact table (`fact_hr_snapshot`) and is simply appended as new snapshots arrive.
        

----------

## Dockerization

The entire stack is containerized for easy deployment and testing. The `docker-compose.yml` file orchestrates the necessary services, including `zookeeper`, `kafka`, `schema-registry`, `kafka-connect`, `airflow-webserver`, and `airflow-scheduler`.

----------

## Discussion

**Kafka was chosen to act as a central messaging backbone and a durable event log, not a traditional message queue.** Unlike a Pub-Sub model, where messages are consumed and then deleted, Kafka retains messages for a configurable period. This allows multiple, independent consumers (like Spark Streaming, or future applications like dbt or machine learning models) to read the same data stream at their own pace without affecting each other. This decoupling is crucial for a data pipeline, as it enables the same real-time event stream to feed multiple downstream systems.

The `debezium-postgres-connector.json` configures Debezium to capture changes from the Postgres database and send them as events to specific Kafka topics. This approach is ideal for Change Data Capture (CDC) because it provides a reliable, ordered, and fault-tolerant way to transport changes from a source to a sink.

**The Schema Registry provides a centralized repository for managing schemas, ensuring data consistency and enabling schema evolution across the entire data ecosystem.** While the project's Debezium connector configuration explicitly disables schema inclusion in the message payload (`value.converter.schemas.enable: "false"`) to keep the messages small and flexible, the Schema Registry still provides critical value. It acts as a central source of truth for schema definitions, which can be programmatically retrieved and used by consumers like the Spark streaming job. This allows for data validation and prevents issues that might arise from schema changes.

**JSON was chosen for its human-readable and flexible nature, contrasting with Avro's more compact binary format.** The `debezium-postgres-connector.json` file uses `JsonConverter` to serialize data. The decision to use JSON often prioritizes ease of development, debugging, and integration with a wide variety of tools, especially in a prototype or hobby project, over the performance and space savings of a binary format like Avro.

The architectural design is based on a **separation of concerns**, where each component is responsible for a single, well-defined task.

-   **Data Ingestion:** Debezium and Kafka Connect are solely responsible for capturing and streaming data from the source database.
    
-   **Orchestration:** Airflow's only job is to manage the end-to-end workflow, triggering tasks like the transaction simulation and the Spark job.
    
-   **Stream Processing:** The Spark application (`consumer_cdc_txn.py`) focuses specifically on consuming data from Kafka and writing it to the Delta Lake.
    
-   **Storage:** Delta Lake provides a reliable storage layer with ACID properties for the data lake.
    
This modular design makes the pipeline **scalable and extensible** to other use cases. Each component can be individually scaled or replaced without impacting the others. For example, a new team could easily create a separate consumer that reads from the same Kafka topics to build a new dashboard or train a machine learning model, without needing to change the core ingestion or processing jobs.



## Getting Started

1.  Clone the repository and ensure you have Docker and Docker Compose installed.
    
2.  Start the environment by running `docker-compose up`.
    
3.  Use the Airflow UI to manually trigger the `cdc_pipeline_dag` to see the initial data flow from Postgres to the Delta Lake Bronze layer.
    

----------

## Copyright and Licensing

Copyright © 2025 Tridib C[@ctriz]. This project is licensed under the MIT License.
