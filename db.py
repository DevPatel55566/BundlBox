import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "contact_center.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS agents (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    status_since TEXT NOT NULL,
    shift_start TEXT NOT NULL,
    french INTEGER NOT NULL DEFAULT 0,
    english INTEGER NOT NULL DEFAULT 0,
    delivery INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS status_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT
);
CREATE TABLE IF NOT EXISTS queue_metrics (
    queue TEXT PRIMARY KEY,
    waiting INTEGER NOT NULL DEFAULT 0,
    longest_wait_seconds INTEGER NOT NULL DEFAULT 0,
    available_agents INTEGER NOT NULL DEFAULT 0,
    logged_in_agents INTEGER NOT NULL DEFAULT 0,
    sla REAL NOT NULL DEFAULT 100.0,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    manager TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    result TEXT NOT NULL
);
"""


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> sqlite3.Connection:
    conn = connect()
    conn.executescript(SCHEMA)
    conn.commit()
    return conn