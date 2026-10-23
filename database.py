import sqlite3
import os
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo(os.getenv("TIMEZONE", "America/Toronto"))
DB_PATH = Path(os.getenv("DB_PATH", str(Path(__file__).parent / "cc.db")))

SCHEMA = """
CREATE TABLE IF NOT EXISTS agents (
    id INTEGER PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL, status_since TEXT NOT NULL, shift_start TEXT NOT NULL,
    french INTEGER NOT NULL DEFAULT 0, english INTEGER NOT NULL DEFAULT 0,
    delivery INTEGER NOT NULL DEFAULT 0,
    break_saved_queues TEXT NOT NULL DEFAULT '',
    break_notified INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS status_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id INTEGER NOT NULL,
    status TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT
);
CREATE TABLE IF NOT EXISTS queue_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id INTEGER NOT NULL,
    queue TEXT NOT NULL, joined INTEGER NOT NULL, at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS queue_metrics (
    queue TEXT PRIMARY KEY, waiting INTEGER NOT NULL DEFAULT 0,
    longest_wait_seconds INTEGER NOT NULL DEFAULT 0, sla REAL NOT NULL DEFAULT 100
);
CREATE TABLE IF NOT EXISTS calls_answered (
    date TEXT NOT NULL, queue TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    missed INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    outbound_answered INTEGER NOT NULL DEFAULT 0,
    outbound_attempts INTEGER NOT NULL DEFAULT 0,
    talk_secs INTEGER NOT NULL DEFAULT 0,
    hold_secs INTEGER NOT NULL DEFAULT 0,
    outbound_talk_secs INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (date, queue)
);
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT NOT NULL,
    agent_id INTEGER,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schedule (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS line_stats (
    date TEXT NOT NULL, line TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    answered INTEGER NOT NULL DEFAULT 0,
    talk_secs INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (date, line)
);
"""

conn: sqlite3.Connection = None

def now_local():
    return datetime.now(TZ).replace(tzinfo=None)

def now_iso():
    return now_local().isoformat(timespec="seconds")

def today_str():
    return now_local().strftime("%Y-%m-%d")

def connect():
    global conn
    if conn is None:
        c = sqlite3.connect(DB_PATH, check_same_thread=False)
        c.row_factory = sqlite3.Row
        conn = c
    return conn

def seed():
    c = connect()
    c.executescript(SCHEMA)
    c.commit()
    
    cols = {r["name"] for r in c.execute("PRAGMA table_info(agents)")}
    if "break_saved_queues" not in cols:
        c.execute("ALTER TABLE agents ADD COLUMN break_saved_queues TEXT NOT NULL DEFAULT ''")
    if "break_notified" not in cols:
        c.execute("ALTER TABLE agents ADD COLUMN break_notified INTEGER NOT NULL DEFAULT 0")
    
    q_cols = {r["name"] for r in c.execute("PRAGMA table_info(calls_answered)")}
    if "outbound_answered" not in q_cols:
        c.execute("ALTER TABLE calls_answered ADD COLUMN outbound_answered INTEGER NOT NULL DEFAULT 0")
        c.execute("ALTER TABLE calls_answered ADD COLUMN outbound_attempts INTEGER NOT NULL DEFAULT 0")
        c.execute("ALTER TABLE calls_answered ADD COLUMN talk_secs INTEGER NOT NULL DEFAULT 0")
        c.execute("ALTER TABLE calls_answered ADD COLUMN hold_secs INTEGER NOT NULL DEFAULT 0")
        c.execute("ALTER TABLE calls_answered ADD COLUMN outbound_talk_secs INTEGER NOT NULL DEFAULT 0")

    c.commit()
    for q in ["French", "English", "Delivery Line"]:
        c.execute("INSERT OR IGNORE INTO queue_metrics (queue) VALUES (?)", (q,))
        
    default_schedule = {
        "hours": {
            "Monday": {"open": True, "start": "09:00", "end": "20:00"},
            "Tuesday": {"open": True, "start": "09:00", "end": "20:00"},
            "Wednesday": {"open": True, "start": "09:00", "end": "20:00"},
            "Thursday": {"open": True, "start": "09:00", "end": "20:00"},
            "Friday": {"open": True, "start": "09:00", "end": "15:00"},
            "Saturday": {"open": False, "start": "09:00", "end": "17:00"},
            "Sunday": {"open": False, "start": "09:00", "end": "17:00"},
        },
        "exceptions": []
    }
    c.execute("INSERT OR IGNORE INTO schedule (id, data) VALUES (1, ?)", (json.dumps(default_schedule),))
    c.commit()
    
    ensure_default_users()

def ensure_default_users():
    c = connect()
    mgr_user = os.getenv("MANAGER_USERNAME", "manager")
    mgr_pass = os.getenv("MANAGER_PASSWORD", "admin")
    wb_user = os.getenv("WALLBOARD_USERNAME", "wallboard")
    wb_pass = os.getenv("WALLBOARD_PASSWORD", "wallboard")

    row = c.execute("SELECT * FROM users WHERE username=?", (mgr_user,)).fetchone()
    if not row:
        c.execute("INSERT INTO users (username, password, role, created_at) VALUES (?,?,?,?)", (mgr_user, mgr_pass, "manager", now_iso()))
    else:
        c.execute("UPDATE users SET password=?, role='manager', agent_id=NULL WHERE username=?", (mgr_pass, mgr_user))
    
    row = c.execute("SELECT * FROM users WHERE username=?", (wb_user,)).fetchone()
    if not row:
        c.execute("INSERT INTO users (username, password, role, created_at) VALUES (?,?,?,?)", (wb_user, wb_pass, "wallboard", now_iso()))
    else:
        c.execute("UPDATE users SET password=?, role='wallboard', agent_id=NULL WHERE username=?", (wb_pass, wb_user))
    
    row = c.execute("SELECT * FROM users WHERE username='agent'").fetchone()
    if not row:
        c.execute("INSERT INTO users (username, password, role, agent_id, created_at) VALUES ('agent', 'pw123456', 'agent', NULL, ?)", (now_iso(),))
    else:
        c.execute("UPDATE users SET password='pw123456', role='agent', agent_id=NULL WHERE username='agent'")
    c.commit()