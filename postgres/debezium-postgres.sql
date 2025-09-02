CREATE ROLE debezium WITH LOGIN PASSWORD 'debezium' REPLICATION;
GRANT CONNECT ON DATABASE postgres TO debezium;
GRANT USAGE ON SCHEMA hr_txn TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA hr_txn TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA hr_txn GRANT SELECT ON TABLES TO debezium;


-- Optional

CREATE PUBLICATION hr_txn_publication
FOR TABLE hr_txn.departments, hr_txn.employee_compensation;

