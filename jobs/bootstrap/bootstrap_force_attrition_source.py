#!/usr/bin/env python3
"""
cdc_force_attrition.py
Force attrition events by soft deleting N random employees in hrdbt.employees.

Idempotent:
- Only picks from employees where is_deleted=false
- If fewer than N active employees remain, deletes only the available ones

Usage:
  python cdc_force_attrition.py --count 5
"""

import os
import argparse
import datetime as dt
from sqlalchemy import create_engine, text
from config.config import load_config
from config.config import load_config
from utils.db_utils import get_engine

SCHEMA = "hrdbt"

# ---------------------------
# Parse arguments
# ---------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--count", type=int, default=5, help="Number of employees to soft delete (default=5)")
args = parser.parse_args()

# ---------------------------
# Main
# ---------------------------

def main():
    engine = get_engine()
    now = dt.datetime.now()

    with engine.begin() as conn:
        conn.execute(text(f"SET search_path TO {SCHEMA}"))

        # Count active employees
        active_count = conn.execute(
            text("SELECT COUNT(*) FROM employees WHERE is_deleted=false")
        ).scalar_one()

        if active_count == 0:
            print(" No active employees left to delete. All attrition already simulated.")
            return

        n_to_delete = min(args.count, active_count)

        # Pick N random active employees
        rows = conn.execute(
            text(
                "SELECT employee_id, employee_code "
                "FROM employees WHERE is_deleted=false "
                "ORDER BY random() LIMIT :n"
            ),
            {"n": n_to_delete},
        ).fetchall()

        emp_ids = [r[0] for r in rows]

        # Soft delete them
        conn.execute(
            text("UPDATE employees SET is_deleted=true, updated_at=:now WHERE employee_id = ANY(:ids)"),
            {"now": now, "ids": emp_ids},
        )

        print(f" Forced attrition for {len(rows)} employees (out of {active_count} remaining active):")
        for r in rows:
            print(f"   employee_id={r[0]} | code={r[1]}")


if __name__ == "__main__":
    main()
