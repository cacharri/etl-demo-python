import sqlite3
from pathlib import Path

def get_sqlite_conn(db_path: str):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(db_path)
