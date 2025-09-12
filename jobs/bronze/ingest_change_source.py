#!/usr/bin/env python3
##src\jobs\bootstrap\ingest_change_source.py
"""
cdc_hr.py
Simulate CDC changes on HR schema (hrdbt).

- Random INSERT, UPDATE, DELETE across employees, departments, jobs, locations
- Can repeat changes for N records
- Can restrict to specific tables with --tables

Usage:
  python ingest_change_source.py                      # 1 change per table, all tables
  python ingest_change_source.py --count 5            # 5 changes per table, all tables
  python ingest_change_source.py --tables employees,jobs --count 3
"""

import os
import argparse
import random
import datetime as dt
from faker import Faker
from sqlalchemy import create_engine, text
from config.config import load_config
from utils.db_utils import get_engine

SCHEMA = "hrdbt"
fake = Faker("en_IN")

# ---------------------------
# Parse arguments
# ---------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--count", type=int, default=1, help="Number of records to modify per table (default=1)")
parser.add_argument("--tables", type=str, default="employees,departments,jobs,locations",
                    help="Comma-separated list of tables to simulate CDC (default=all)")
args = parser.parse_args()

selected_tables = [t.strip().lower() for t in args.tables.split(",")]


# ---------------------------
# Load Config
# ---------------------------
config = load_config()
if not config:
    raise RuntimeError(" Could not load config.yaml")

delta_base_path = config["delta"]["base_path"]
bronze_base_path = f"{delta_base_path}/bronze"





# ---------------------------
# Simulators (same as before, but modularized)
# ---------------------------
def simulate_employees(conn, count):
    now = dt.datetime.now()
    for _ in range(count):
        action = random.choice(["insert", "update", "delete"])
        if action == "insert":
            conn.execute(text("""
                INSERT INTO employees (employee_code, first_name, last_name, email, phone,
                                       hire_date, salary, department_id, job_id, location_id,
                                       updated_at, is_deleted)
                VALUES (:ecode, :fn, :ln, :email, :phone,
                        :hire, :salary, 1, 1, 1, :updated, false)
            """), {
                "ecode": f"EMP-{random.randint(20000,99999)}",
                "fn": fake.first_name(),
                "ln": fake.last_name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "hire": fake.date_between(start_date="-2y", end_date="today"),
                "salary": random.randint(60000, 200000),
                "updated": now
            })
            print(" Inserted employee")
        elif action == "update":
            emp = conn.execute(text("SELECT employee_id FROM employees WHERE is_deleted=false ORDER BY random() LIMIT 1")).fetchone()
            if emp:
                conn.execute(text("UPDATE employees SET salary=:sal, updated_at=:updated WHERE employee_id=:id"), {
                    "sal": random.randint(60000, 200000),
                    "updated": now,
                    "id": emp[0]
                })
                print(f" Updated employee_id={emp[0]}")
        elif action == "delete":
            emp = conn.execute(text("SELECT employee_id FROM employees WHERE is_deleted=false ORDER BY random() LIMIT 1")).fetchone()
            if emp:
                conn.execute(text("UPDATE employees SET is_deleted=true, updated_at=:updated WHERE employee_id=:id"), {
                    "updated": now,
                    "id": emp[0]
                })
                print(f" Soft deleted employee_id={emp[0]}")


def simulate_departments(conn, count):
    now = dt.datetime.now()
    for _ in range(count):
        action = random.choice(["insert", "update", "delete"])
        if action == "insert":
            conn.execute(text("""
                INSERT INTO departments (department_code, name, updated_at, is_deleted)
                VALUES (:code, :name, :updated, false)
            """), {
                "code": f"DEP-{fake.lexify('???').upper()}",
                "name": fake.job(),
                "updated": now
            })
            print(" Inserted department")
        elif action == "update":
            dep = conn.execute(text("SELECT department_id FROM departments WHERE is_deleted=false ORDER BY random() LIMIT 1")).fetchone()
            if dep:
                conn.execute(text("UPDATE departments SET name=:name, updated_at=:updated WHERE department_id=:id"), {
                    "name": fake.bs(),
                    "updated": now,
                    "id": dep[0]
                })
                print(f" Updated department_id={dep[0]}")
        elif action == "delete":
            dep = conn.execute(text("SELECT department_id FROM departments WHERE is_deleted=false ORDER BY random() LIMIT 1")).fetchone()
            if dep:
                conn.execute(text("UPDATE departments SET is_deleted=true, updated_at=:updated WHERE department_id=:id"), {
                    "updated": now,
                    "id": dep[0]
                })
                print(f" Soft deleted department_id={dep[0]}")


def simulate_jobs(conn, count):
    now = dt.datetime.now()
    for _ in range(count):
        action = random.choice(["insert", "update", "delete"])
        if action == "insert":
            conn.execute(text("""
                INSERT INTO jobs (job_code, title, grade, updated_at, is_deleted)
                VALUES (:code, :title, :grade, :updated, false)
            """), {
                "code": f"JOB-{random.randint(1000,9999)}",
                "title": fake.job(),
                "grade": random.choice(["IC2", "IC3", "M2"]),
                "updated": now
            })
            print(" Inserted job")
        elif action == "update":
            job = conn.execute(text("SELECT job_id FROM jobs WHERE is_deleted=false ORDER BY random() LIMIT 1")).fetchone()
            if job:
                conn.execute(text("UPDATE jobs SET title=:title, updated_at=:updated WHERE job_id=:id"), {
                    "title": fake.catch_phrase(),
                    "updated": now,
                    "id": job[0]
                })
                print(f" Updated job_id={job[0]}")
        elif action == "delete":
            job = conn.execute(text("SELECT job_id FROM jobs WHERE is_deleted=false ORDER BY random() LIMIT 1")).fetchone()
            if job:
                conn.execute(text("UPDATE jobs SET is_deleted=true, updated_at=:updated WHERE job_id=:id"), {
                    "updated": now,
                    "id": job[0]
                })
                print(f" Soft deleted job_id={job[0]}")


def simulate_locations(conn, count):
    now = dt.datetime.now()
    for _ in range(count):
        action = random.choice(["insert", "update", "delete"])
        if action == "insert":
            conn.execute(text("""
                INSERT INTO locations (location_code, name, country, updated_at, is_deleted)
                VALUES (:code, :name, :country, :updated, false)
            """), {
                "code": f"LOC-{random.randint(100,999)}",
                "name": f"{fake.city()} Campus",
                "country": fake.country(),
                "updated": now
            })
            print(" Inserted location")
        elif action == "update":
            loc = conn.execute(text("SELECT location_id FROM locations WHERE is_deleted=false ORDER BY random() LIMIT 1")).fetchone()
            if loc:
                conn.execute(text("UPDATE locations SET name=:name, updated_at=:updated WHERE location_id=:id"), {
                    "name": fake.city(),
                    "updated": now,
                    "id": loc[0]
                })
                print(f" Updated location_id={loc[0]}")
        elif action == "delete":
            loc = conn.execute(text("SELECT location_id FROM locations WHERE is_deleted=false ORDER BY random() LIMIT 1")).fetchone()
            if loc:
                conn.execute(text("UPDATE locations SET is_deleted=true, updated_at=:updated WHERE location_id=:id"), {
                    "updated": now,
                    "id": loc[0]
                })
                print(f" Soft deleted location_id={loc[0]}")


# ---------------------------
# Main
# ---------------------------
def main():
    #  build engine using db_utils
    engine = get_engine()
    
    with engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {SCHEMA}"))

        print(f"\n Simulating CDC with {args.count} changes for tables: {', '.join(selected_tables)}\n")

        if "departments" in selected_tables:
            simulate_departments(conn, args.count)
        if "jobs" in selected_tables:
            simulate_jobs(conn, args.count)
        if "locations" in selected_tables:
            simulate_locations(conn, args.count)
        if "employees" in selected_tables:
            simulate_employees(conn, args.count)


if __name__ == "__main__":
    main()
