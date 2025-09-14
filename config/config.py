# utils/config.py

import os
import yaml

def load_config(path="/opt/airflow/config/config.yaml"):
    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    def resolve_env(val):
        if isinstance(val, str) and val.startswith("${") and val.endswith("}"):
            env_var = val[2:-1]
            return os.getenv(env_var, "")
        return val

    def walk(node):
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        elif isinstance(node, list):
            return [walk(x) for x in node]
        else:
            return resolve_env(node)

    return walk(raw)
