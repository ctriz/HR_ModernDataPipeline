#execute_sql_db.py
# Utility to execute SQL scripts against a PostgreSQL database

import os
import sys
from sqlalchemy import create_engine, text
from getpass import getpass

# Add the project's root directory to the system path to allow imports from 'utils'.
# This assumes the script is run from a subdirectory like 'test'.


import os
import sys

# Get the path of the parent directory ()
current_dir = os.path.dirname(__file__)
print(f"Current directory: {current_dir}")
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print(f"Parent directory (added to sys.path): {parent_dir}")
# Add the parent directory to the system path
sys.path.append(parent_dir)

from config.config import load_config

# ---------------------------
# Load config
# ---------------------------
config = load_config()
if not config:
    exit("Failed to load config")




def build_pg_url(db_config: dict) -> str:
    """
    Builds the PostgreSQL connection URL using a hybrid approach:
    non-sensitive info from config, sensitive info from env vars.
    """
    host = db_config.get("host", "localhost")
    port = db_config.get("port", "5432")
    db_name = db_config.get("name", "postgres")
    user = db_config.get("user", "postgres")

    # The password is a secret and should never be stored in a config file.
    # We retrieve it from an environment variable for security.
    pwd = os.getenv("PGPASSWORD", None)

    if not pwd:
        print("Warning: PGPASSWORD environment variable is not set.")
        # As a fallback for interactive use, we can prompt for it.
        # This is not recommended for automated scripts.
        pwd = getpass("Please enter the database password: ")

    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db_name}"

def execute_sql_file(file_path: str):
    """
    Reads an SQL file and executes its content against the database after confirmation.
    """
    # Load the application configuration from the YAML file
    config = load_config()
    if not config:
        print("Failed to load configuration. Exiting.")
        return

    # Get database config from the loaded YAML file
    db_config = config.get("database", {})
    
    # Check if the SQL file exists
    if not os.path.exists(file_path):
        print(f"Error: The SQL file '{file_path}' was not found.")
        return

    # Read the SQL content from the file
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()

    # Ask for confirmation before execution
    print(f"\n--- About to execute the following SQL script from '{file_path}' ---")
    print(sql_script)
    confirmation = input("\nDo you want to continue with the execution? (y/n): ").strip().lower()

    if confirmation == 'y':
        try:
            pg_url = build_pg_url(db_config)
            engine = create_engine(pg_url, future=True)
            
            with engine.connect() as conn:
                print("Successfully connected to the database. Executing script...")
                # Execute the entire script as a single transaction
                conn.execute(text(sql_script))
                conn.commit()
                print("\nSQL script executed successfully.")
                
        except Exception as e:
            print(f"An error occurred during execution: {e}")
    else:
        print("SQL script execution cancelled.")

if __name__ == "__main__":
    # You can specify the SQL file name here
    # For example, create a file named 'sample_query.sql' in the same folder.
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the full path to the SQL file
    sql_file_to_run = os.path.join(script_dir, "producer_cdc_data.sql")
    execute_sql_file(sql_file_to_run)
# Example usage:
# execute_sql_file("path/to/your_script.sql")