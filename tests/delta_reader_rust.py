import os
import sys
from deltalake import DeltaTable
import pandas as pd
import pyarrow as pa
from utils.config import load_config

# ---------------------------
# Load configuration
# ---------------------------
config = load_config()
if not config:
    print(" Error: Could not load configuration")
    sys.exit(1)

# Normalize local paths (strip file:/// if present)
def normalize_path(path: str) -> str:
    return path.replace("file:///", "").replace("file://", "")

delta_base_path = normalize_path(config['delta']['base_path'])
checkpoint_base_path = normalize_path(config['delta']['checkpoint_base_path'])

# Paths
dept_delta_path = f"{delta_base_path}/dim_department"
fact_delta_path = f"{delta_base_path}/fact_hr_snapshot"


# ---------------------------
# Reader function
# ---------------------------
def read_delta_table(delta_path: str, table_name: str = ""):
    """
    Reads a Delta table and displays schema, row count, and a sample of data.
    Falls back to forcing all columns as strings if parsing fails.
    """

    if not os.path.exists(delta_path):
        print(f"❌ Path not found for {table_name}: {delta_path}")
        return

    print(f"\n=== Reading Delta Table: {table_name} ===")
    print(f"📂 Path: {delta_path}")

    # Load Delta table
    dt = DeltaTable(delta_path)

    try:
        # Normal load
        df = dt.to_pyarrow_table().to_pandas()
    except Exception as e:
        print(f"⚠️ Warning: Failed to parse schema for {table_name}: {e}")
        print("🔄 Retrying with all columns forced to string...")

        import pyarrow as pa
        from pyarrow import compute as pc

        arrow_table = dt.to_pyarrow_table()

        # Cast all columns to string inside Arrow
        string_fields = [pc.cast(arrow_table[col.name], pa.string())
                         for col in arrow_table.schema]
        field_names = [col.name for col in arrow_table.schema]

        arrow_table_str = pa.table(string_fields, names=field_names)

        # Convert to Pandas safely (disable any auto-parsing)
        df = arrow_table_str.to_pandas(
            strings_to_categorical=False,
            date_as_object=True,
            timestamp_as_object=True
        )

        # Absolute last resort: force all Pandas columns to str
        df = df.astype(str)

    # ---------------------------
    # Print schema & data preview
    # ---------------------------
    print("\n--- Schema ---")
    print(df.dtypes.to_string())

    print(f"\n--- Row Count: {len(df)} ---")
    print(f"--- Delta Version: {dt.version()} ---")

    print("\n--- First 5 Rows ---")
    try:
        print(df.head().to_markdown(index=False))
    except ImportError:
        print(df.head().to_string(index=False))


# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    # If user passes a path, read only that
    if len(sys.argv) > 1:
        delta_table_path = normalize_path(sys.argv[1])
        read_delta_table(delta_table_path, "Custom Table")
    else:
        # Otherwise, read both Department + Compensation Fact
        read_delta_table(dept_delta_path, "Department Dimension")
        read_delta_table(fact_delta_path, "Compensation Fact (HR Snapshot)")

    print("\n Done")
