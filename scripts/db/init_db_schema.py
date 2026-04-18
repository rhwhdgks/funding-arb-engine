#!/usr/bin/env python3
import sys
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

env_file = os.getenv("STRATEGY_ENV_FILE")
if env_file:
    load_dotenv(env_file, override=True)
else:
    load_dotenv(PROJECT_ROOT / ".env")

from src.ops.persistence.db import DEFAULT_SCHEMA_PATH, ensure_schema, get_db_connection


def main():
    conn = get_db_connection()
    try:
        count = ensure_schema(conn, DEFAULT_SCHEMA_PATH)
        print(f"[SCHEMA] applied_statements={count} path={DEFAULT_SCHEMA_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
