#!/usr/bin/env python3
"""
Seed the HR transactional schema (hr_txn) with small, realistic fake data.

- 3 locations
- 5 departments
- For each department: 3 jobs (job_family = department name)
- 100 employees (linked to department/job/location)
- 1 initial compensation row per employee

.env expected (example):
  PGHOST=<>
  PGPORT=5432
  PGDATABASE=postgres
  PGUSER=<>
  PGPASSWORD=<>

Usage:
  python seed_transactional_data.py
"""

import os
import random
import datetime as dt
from typing import Dict, List

from faker import Faker
from sqlalchemy import create_engine, text

from utils.config import load_config

# --- Config ---
SCHEMA = "hr_txn"
RESET_BEFORE_INSERT = True  # True => truncate existing rows

N_LOCATIONS = 3
N_DEPARTMENTS = 5
JOBS_PER_DEPARTMENT = 3
N_EMPLOYEES = 100

BASE_SALARY_RANGE = (60000, 200000)  # USD annual
BONUS_RANGE = (0, 20000)

DEPARTMENT_NAMES = ["Engineering", "Human Resources", "Finance", "Sales", "Operations"]
JOB_GRADES = ["IC2", "IC3", "M2"]
JOB_TITLE_TEMPLATES = ["{} Associate", "{} Specialist", "{} Manager"]

fake = Faker("en_IN")

# Load the application configuration from the YAML file
from sqlalchemy import create_engine, text
from utils.config import load_config
import os, sys

config = load_config()
if not config:
    exit()


# --- Connection string builder ---
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

def main():
    random.seed(17)
    engine = create_engine(build_pg_url(), future=True)

    with engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {SCHEMA}"))

        if RESET_BEFORE_INSERT:
            conn.execute(text("DELETE FROM employee_compensation"))
            conn.execute(text("DELETE FROM employees"))
            conn.execute(text("DELETE FROM jobs"))
            conn.execute(text("DELETE FROM departments"))
            conn.execute(text("DELETE FROM locations"))

        # --- Locations ---
        locations_payload = []
        for i in range(1, N_LOCATIONS + 1):
            code = f"LOC-{i:02d}"
            name = f"{fake.city()} Campus"
            locations_payload.append({"code": code, "name": name, "country": "USA"})

        conn.execute(
            text("INSERT INTO locations (location_code, name, country) VALUES (:code, :name, :country)"),
            locations_payload,
        )
        location_ids = [r[0] for r in conn.execute(text("SELECT location_id FROM locations")).all()]

        # --- Departments ---
        dep_names = (DEPARTMENT_NAMES * ((N_DEPARTMENTS + len(DEPARTMENT_NAMES) - 1) // len(DEPARTMENT_NAMES)))[:N_DEPARTMENTS]
        departments_payload = []
        for i, dep_name in enumerate(dep_names, start=1):
            dep_code = f"DEP-{dep_name[:3].upper()}"
            if any(d["department_code"] == dep_code for d in departments_payload):
                dep_code = f"{dep_code}-{i}"
            departments_payload.append({"department_code": dep_code, "name": dep_name})

        conn.execute(
            text("INSERT INTO departments (department_code, name) VALUES (:department_code, :name)"),
            departments_payload,
        )
        dep_rows = conn.execute(text("SELECT department_id, name, department_code FROM departments")).mappings().all()

        # --- Jobs ---
        jobs_payload = []
        for d in dep_rows:
            for j in range(JOBS_PER_DEPARTMENT):
                title = JOB_TITLE_TEMPLATES[j % len(JOB_TITLE_TEMPLATES)].format(d["name"])
                job_code = f"{d['department_code']}-J{j+1}"
                grade = JOB_GRADES[j % len(JOB_GRADES)]
                jobs_payload.append({"job_code": job_code, "title": title, "job_family": d["name"], "grade": grade})

        conn.execute(
            text("INSERT INTO jobs (job_code, title, job_family, grade) VALUES (:job_code, :title, :job_family, :grade)"),
            jobs_payload,
        )
        job_rows = conn.execute(text("SELECT job_id, job_family FROM jobs")).mappings().all()
        family_to_jobs: Dict[str, List[int]] = {}
        for r in job_rows:
            family_to_jobs.setdefault(r["job_family"], []).append(r["job_id"])

        # --- Employees + Compensation ---
        created_emp_ids: List[int] = []
        for i in range(N_EMPLOYEES):
            first, last = fake.first_name(), fake.last_name()
            email = f"{first.lower()}.{last.lower()}{i}@example.com"

            dep = random.choice(dep_rows)
            job_id = random.choice(family_to_jobs[dep["name"]])
            location_id = random.choice(location_ids)
            hire_date = fake.date_between(start_date="-4y", end_date="-30d")

            manager_id = None
            if created_emp_ids and random.random() < 0.35:
                manager_id = random.choice(created_emp_ids)

            r = conn.execute(
                text("""
                    INSERT INTO employees (employee_code, first_name, last_name, email, phone,
                                            hire_date, department_id, job_id, manager_employee_id, location_id)
                    VALUES (:ecode, :fn, :ln, :email, :phone,
                            :hire, :dep_id, :job_id, :mgr_id, :loc_id)
                    RETURNING employee_id
                """),
                {
                    "ecode": f"EMP-{10000+i}",
                    "fn": first,
                    "ln": last,
                    "email": email,
                    "phone": fake.phone_number(),
                    "hire": hire_date,
                    "dep_id": dep["department_id"],
                    "job_id": job_id,
                    "mgr_id": manager_id,
                    "loc_id": location_id,
                },
            )
            emp_id = r.scalar_one()
            created_emp_ids.append(emp_id)

            base_salary = random.randint(*BASE_SALARY_RANGE)
            bonus = random.randint(*BONUS_RANGE) if random.random() < 0.6 else 0
            conn.execute(
                text("INSERT INTO employee_compensation (employee_id, base_salary, bonus, currency, effective_from) "
                     "VALUES (:emp_id, :sal, :bonus, 'USD', :eff_from)"),
                {"emp_id": emp_id, "sal": float(base_salary), "bonus": float(bonus), "eff_from": hire_date.replace(day=1)},
            )

        counts = {tbl: conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar_one()
                  for tbl in ["locations", "departments", "jobs", "employees", "employee_compensation"]}
        print("Inserted counts:", counts)

if __name__ == "__main__":
    main()
