#!/usr/bin/env python3
"""
Continuously mutate ONE dimension (departments) and ONE fact (employee_compensation).

- Department rename drift (SCD driver): randomly appends or removes a suffix
- Salary changes: close current row (effective_to = today) and insert a new row (effective_from = today)

Env vars required:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD  (same as seed script)

Usage:
  python produce_cdc_data.py --cycles 50 --sleep 5 --salary-k 5
"""

import os
import time
import random
import argparse
import datetime as dt
from sqlalchemy import create_engine, text

# Load the application configuration from the YAML file
from sqlalchemy import create_engine, text
from utils.config import load_config
import os, sys

config = load_config()
if not config:
    exit()

SCHEMA = "hr_txn"

def build_pg_url() -> str:
    """
    Builds the PostgreSQL connection URL using a hybrid approach:
    non-sensitive info from config, sensitive info from env vars.
    """
    # Get database config from the loaded YAML file
    db_config = config.get("database", {})
    host = db_config.get("host", "localhost")
    port = db_config.get("port", "5432")
    db_name = db_config.get("name", "postgres")
    user = db_config.get("user", "postgres")

    # The password is a secret and should NEVER be in a config file.
    # We retrieve it from an environment variable for security.
    pwd = os.getenv("PGPASSWORD", "postgres")
    
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db_name}"

def rename_random_department(conn):
    row = conn.execute(
        text("SELECT department_id, name FROM hr_txn.departments ORDER BY random() LIMIT 1")
    ).mappings().first()
    if not row:
        return None
    suffix = random.choice([" Team", " Group", " & Ops", " Unit", " Squad"])
    new_name = row["name"]
    if suffix not in row["name"]:
        new_name = row["name"] + suffix
    else:
        new_name = row["name"].replace(suffix, "")

    conn.execute(
        text("UPDATE hr_txn.departments SET name=:n WHERE department_id=:i"),
        {"n": new_name, "i": row["department_id"]},
    )
    return (row["department_id"], row["name"], new_name)

def bump_random_salaries(conn, k=5):
    open_rows = conn.execute(text("""
        SELECT comp_id, employee_id, base_salary
        FROM hr_txn.employee_compensation
        WHERE effective_to IS NULL
        ORDER BY random() LIMIT :k
    """), {"k": k}).mappings().all()

    today = dt.date.today()
    yesterday = today - dt.timedelta(days=1)
    results = []
   
    for r in open_rows:
        bump = 1.0 + random.uniform(0.03, 0.12)
        new_salary = round(float(r["base_salary"]) * bump, 2)

        # close current row (set effective_to = yesterday)
        conn.execute(text("""
            UPDATE hr_txn.employee_compensation
               SET effective_to = :yesterday
             WHERE comp_id = :cid AND effective_to IS NULL
        """), {"yesterday": yesterday, "cid": r["comp_id"]})

        # open new (starts today)
        conn.execute(text("""
            INSERT INTO hr_txn.employee_compensation
                (employee_id, base_salary, currency, effective_from)
            VALUES (:emp_id, :sal, 'USD', :today)
        """), {"emp_id": r["employee_id"], "sal": new_salary, "today": today})

        results.append((r["employee_id"], r["base_salary"], new_salary))
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycles", type=int, default=20, help="number of mutation cycles to run")
    parser.add_argument("--sleep", type=int, default=5, help="sleep seconds between cycles")
    parser.add_argument("--salary-k", type=int, default=5, help="number of salaries to bump per cycle")
    args = parser.parse_args()

    engine = create_engine(build_pg_url(), future=True)
    random.seed()

    for i in range(1, args.cycles + 1):
        with engine.begin() as conn:
            conn.execute(text(f"SET search_path TO {SCHEMA}"))
            dep_change = rename_random_department(conn)
            sal_changes = bump_random_salaries(conn, k=args.salary_k)

        print(f"[Cycle {i}] Department change: {dep_change}; Salary changes: {len(sal_changes)} rows")
        time.sleep(args.sleep)

if __name__ == "__main__":
    main()

# --- IGNORE ---
# python mutate_dim_and_fact.py --cycles 50 --sleep 5 --salary-k 5
# --- IGNORE ---