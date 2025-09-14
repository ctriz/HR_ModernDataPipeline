# query_db.py


import os, sys

# Add the parent directory to the system path to allow imports from sibling directories.
# This assumes the script is run from within the 'test' directory.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load the application configuration from the YAML file
from sqlalchemy import create_engine, text
from utils.config import load_config

config = load_config()
if not config:
    exit()

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

def fetch_employees():
    """
    Connects to the database and fetches all records from the 'hr_txn.employees' table.
    """
    # Build the connection URL
    pg_url = build_pg_url()
    
    try:
        # Create a database engine
        engine = create_engine(pg_url, future=True)
        print("Successfully created database engine.")
        
        # Connect and execute a query
        with engine.connect() as conn:
            print("Successfully connected to the database.")
            
            # The SQL query to execute
            query = text("SELECT * FROM hr_txn.employees;")
            
            # Execute the query and fetch all results
            result = conn.execute(query)
            
            # Print the results
            print("\n--- Employee Records ---")
            for row in result:
                print(row)
            print("--- End of Records ---\n")
            
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_employees()
