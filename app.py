import asyncio
import csv
import io
import json
import logging
import os
import secrets
import shutil
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect, File, UploadFile, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel
import uvicorn

import telico_client
from telico_client import QUEUE_IDS, is_configured as telico_is_configured

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("app")
logging.getLogger("telico_client").setLevel(logging.INFO)

# FIXED: Safe timezone loading for Windows
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo(os.getenv("TIMEZONE", "America/Toronto"))
except Exception:
    from datetime import timezone
    TZ = timezone(timedelta(hours=-4))
    log.warning("tzdata not installed! Using fallback -4 offset. Run 'pip install tzdata'")

DB_PATH = Path(os.getenv("DB_PATH", str(Path(__file__).parent / "cc.db")))
SMS_UPLOAD_DIR = Path(os.getenv("SMS_UPLOAD_DIR", str(Path(__file__).parent / "sms_uploads")))
SMS_UPLOAD_DIR.mkdir(exist_ok=True, parents=True)

BREAK_LIMIT_SECONDS = 30 * 60
POLL_INTERVAL_SECONDS = 15
BREAK_CHECK_INTERVAL = 10
TOGGLE_GRACE_SECONDS = 30

ANSWER_WATCH_SECONDS = int(os.getenv("ANSWER_WATCH_SECONDS", "20"))
ANSWER_POLL_INTERVAL = float(os.getenv("ANSWER_POLL_INTERVAL", "0.25"))
ANSWER_FALLBACK_ACK = os.getenv("ANSWER_FALLBACK_ACK", "0") == "1"
CALL_ON_BREAK_EXPIRY = os.getenv("CALL_ON_BREAK_EXPIRY", "1") == "1"

BRAND_NAME = "Bundl Box"
BRAND_LOGO = "BB"

MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "manager")
MANAGER_PASSWORD = os.getenv("MANAGER_PASSWORD", "admin")
WALLBOARD_USERNAME = os.getenv("WALLBOARD_USERNAME", "wallboard")
WALLBOARD_PASSWORD = os.getenv("WALLBOARD_PASSWORD", "wallboard")
AGENT_DEFAULT_PASSWORD = os.getenv("AGENT_DEFAULT_PASSWORD", "")

SMS_API_URL = os.getenv("SMS_API_URL", "https://sms.telico.cloud/api/send_sms")
MMS_API_URL = os.getenv("MMS_API_URL", "https://sms.telico.cloud/api/send_mms")
SMS_USERNAME = os.getenv("SMS_USERNAME", "")
SMS_PASSWORD = os.getenv("SMS_PASSWORD", "")
SMS_SOURCE_DID = os.getenv("SMS_SOURCE_DID", "")
SMS_THROTTLE_SECONDS = float(os.getenv("SMS_THROTTLE_SECONDS", "1.0"))

STATUSES = ["Login / Available", "Logout / Offline", "Admin / Wrap Up", "Break / Lunch", "Outbound"]
QUEUES = ["French", "English", "Delivery Line"]
QUEUE_COL = {"French": "french", "English": "english", "Delivery Line": "delivery"}

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
    PRIMARY KEY (date, queue)
);
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
    manager TEXT NOT NULL, action TEXT NOT NULL, target TEXT NOT NULL,
    result TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT NOT NULL,
    agent_id INTEGER,
    created_at TEXT NOT NULL
);
"""

conn: sqlite3.Connection = None
_last_sync: dict = {"at": None, "ok": False, "queues": {}, "phones": []}
_last_waiting: dict = {}
_sessions: dict[str, dict] = {}
SESSION_TTL = 12 * 3600
_pending_toggles: dict[tuple[int, str], float] = {}

class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        self.lock = asyncio.Lock()
    async def join(self, ws):
        async with self.lock: self.clients.add(ws)
    async def leave(self, ws):
        async with self.lock: self.clients.discard(ws)
    async def broadcast(self, msg):
        text = json.dumps(msg)
        async with self.lock:
            dead = []
            for ws in self.clients:
                try: await ws.send_text(text)
                except Exception: dead.append(ws)
            for ws in dead: self.clients.discard(ws)

hub = Hub()

class StatusBody(BaseModel): status: str
class QueueBody(BaseModel): on: bool
class MassBody(BaseModel): queues: list[str]; on: bool

def make_session(role, agent_id=None):
    tok = secrets.token_urlsafe(32)
    _sessions[tok] = {"role": role, "agent_id": agent_id, "created": time.time()}
    return tok

def get_session(request: Request):
    tok = request.cookies.get("cc_session")
    if not tok: return None
    s = _sessions.get(tok)
    if not s: return None
    if time.time() - s["created"] > SESSION_TTL:
        _sessions.pop(tok, None); return None
    return s

def drop_session(request):
    tok = request.cookies.get("cc_session")
    if tok: _sessions.pop(tok, None)

def home_for_role(role):
    if role == "manager": return "/manager"
    if role == "wallboard": return "/wallboard"
    if role == "agent": return "/agent"
    return "/login"

def now_local():
    return datetime.now(TZ).replace(tzinfo=None)

def now_iso(): return now_local().isoformat(timespec="seconds")
def today_str(): return now_local().strftime("%Y-%m-%d")

def connect():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def seed():
    global conn
    conn = connect()
    conn.executescript(SCHEMA)
    conn.commit()
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(agents)")}
    if "break_saved_queues" not in cols:
        conn.execute("ALTER TABLE agents ADD COLUMN break_saved_queues TEXT NOT NULL DEFAULT ''")
    if "break_notified" not in cols:
        conn.execute("ALTER TABLE agents ADD COLUMN break_notified INTEGER NOT NULL DEFAULT 0")
    q_cols = {r["name"] for r in conn.execute("PRAGMA table_info(calls_answered)")}
    if "missed" not in q_cols:
        conn.execute("ALTER TABLE calls_answered ADD COLUMN missed INTEGER NOT NULL DEFAULT 0")
    if "attempts" not in q_cols:
        conn.execute("ALTER TABLE calls_answered ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")

    conn.commit()
    for q in QUEUES:
        conn.execute("INSERT OR IGNORE INTO queue_metrics (queue, waiting, longest_wait_seconds, sla) VALUES (?,0,0,100)", (q,))
    conn.commit()
    ensure_default_users()

def ensure_default_users():
    row = conn.execute("SELECT * FROM users WHERE username=?", (MANAGER_USERNAME,)).fetchone()
    if not row:
        conn.execute("INSERT INTO users (username, password, role, created_at) VALUES (?,?,?,?)", (MANAGER_USERNAME, MANAGER_PASSWORD, "manager", now_iso()))
    else:
        conn.execute("UPDATE users SET password=?, role='manager', agent_id=NULL WHERE username=?", (MANAGER_PASSWORD, MANAGER_USERNAME))
    
    row = conn.execute("SELECT * FROM users WHERE username=?", (WALLBOARD_USERNAME,)).fetchone()
    if not row:
        conn.execute("INSERT INTO users (username, password, role, created_at) VALUES (?,?,?,?)", (WALLBOARD_USERNAME, WALLBOARD_PASSWORD, "wallboard", now_iso()))
    else:
        conn.execute("UPDATE users SET password=?, role='wallboard', agent_id=NULL WHERE username=?", (WALLBOARD_PASSWORD, WALLBOARD_USERNAME))
    
    row = conn.execute("SELECT * FROM users WHERE username='agent'").fetchone()
    if not row:
        conn.execute("INSERT INTO users (username, password, role, agent_id, created_at) VALUES ('agent', 'pw123456', 'agent', NULL, ?)", (now_iso(),))
    else:
        conn.execute("UPDATE users SET password='pw123456', role='agent', agent_id=NULL WHERE username='agent'")
    conn.commit()

def _set_queue_flag(agent_id: int, queue: str, on: bool):
    col = QUEUE_COL[queue]
    row = conn.execute(f"SELECT {col} FROM agents WHERE id=?", (agent_id,)).fetchone()
    old = bool(row[col]) if row else False
    new = bool(on)
    if old == new: return
    conn.execute(f"UPDATE agents SET {col}=? WHERE id=?", (int(new), agent_id))
    conn.execute("INSERT INTO queue_events (agent_id, queue, joined, at) VALUES (?,?,?,?)", (agent_id, queue, int(new), now_iso()))
    conn.commit()

def _is_pending(agent_id: int, queue: str) -> bool:
    key = (agent_id, queue)
    ts = _pending_toggles.get(key)
    if ts is None: return False
    if time.time() - ts > TOGGLE_GRACE_SECONDS:
        _pending_toggles.pop(key, None); return False
    return True

def _queue_seconds(agent_id: int) -> dict:
    out = {q: 0 for q in QUEUES}
    open_ts: dict[str, datetime] = {}
    for r in conn.execute("SELECT queue, joined, at FROM queue_events WHERE agent_id=? ORDER BY id", (agent_id,)):
        q = r["queue"]
        if q not in QUEUES: continue
        at = datetime.fromisoformat(r["at"])
        if r["joined"] == 1:
            open_ts.setdefault(q, at)
        else:
            if q in open_ts:
                out[q] += int((at - open_ts[q]).total_seconds())
                del open_ts[q]
    now = now_local()
    for q, ts in open_ts.items():
        out[q] += int((now - ts).total_seconds())
    return out

def _calls_answered_today(queue: str) -> tuple[int, int, int]:
    row = conn.execute("SELECT count, missed, attempts FROM calls_answered WHERE date=? AND queue=?", (today_str(), queue)).fetchone()
    return (int(row["count"]), int(row["missed"]), int(row["attempts"])) if row else (0, 0, 0)

def agent_dict(row):
    try: phone = row["phone"] or ""
    except: phone = ""
    try: saved = row["break_saved_queues"] or ""
    except: saved = ""
    try: notified = int(row["break_notified"] or 0)
    except: notified = 0

    status = row["status"]
    shift_start = row["shift_start"]
    
    if status == "Logout / Offline":
        shift_duration = 0
    else:
        start = datetime.fromisoformat(shift_start)
        if status == "Break / Lunch":
            end_time = datetime.fromisoformat(row["status_since"])
        else:
            end_time = now_local()
            
        today_midnight = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
        if start < today_midnight:
            start = today_midnight
            
        if start < end_time:
            shift_duration = int((end_time - start).total_seconds())
        else:
            shift_duration = 0

    status_secs = _status_seconds(row["id"])
    total_break_seconds = status_secs.get("Break / Lunch", 0)

    return {
        "id": row["id"], "name": row["name"], "phone": phone,
        "status": status, "statusSince": row["status_since"],
        "shiftStart": shift_start,
        "shiftDurationSeconds": shift_duration,
        "totalBreakSeconds": total_break_seconds,
        "onBreak": status == "Break / Lunch",
        "breakNotified": bool(notified),
        "breakSavedQueues": [q for q in saved.split(",") if q in QUEUES],
        "queues": {
            "French": bool(row["french"]),
            "English": bool(row["english"]),
            "Delivery Line": bool(row["delivery"]),
        },
    }

def metric_dict(row):
    w, l = row["waiting"], row["longest_wait_seconds"]
    answered, missed, attempts = _calls_answered_today(row["queue"])
    return {
        "queue": row["queue"], "waiting": w,
        "longestWaitSeconds": l,
        "callsAnsweredToday": answered,
        "callsMissedToday": missed,
        "attempts": attempts,
        "queue_id": QUEUE_IDS.get(row["queue"], 0)
    }

def snapshot_full():
    return {
        "agents": [agent_dict(r) for r in conn.execute("SELECT * FROM agents ORDER BY name")],
        "metrics": [metric_dict(r) for r in conn.execute("SELECT * FROM queue_metrics")],
        "telicoConfigured": telico_is_configured(),
        "breakLimitSeconds": BREAK_LIMIT_SECONDS,
        "lastSync": _last_sync,
    }

def apply_status(agent_id, new_status):
    row = conn.execute("SELECT status, shift_start FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: return
    old_status = row["status"]
    ts = now_iso()
    conn.execute("UPDATE status_events SET ended_at=? WHERE agent_id=? AND ended_at IS NULL", (ts, agent_id))
    conn.execute("INSERT INTO status_events (agent_id, status, started_at) VALUES (?,?,?)", (agent_id, new_status, ts))
    
    if old_status == "Logout / Offline" and new_status != "Logout / Offline":
        conn.execute("UPDATE agents SET status=?, status_since=?, shift_start=? WHERE id=?", 
                     (new_status, ts, ts, agent_id))
    else:
        conn.execute("UPDATE agents SET status=?, status_since=? WHERE id=?", 
                     (new_status, ts, agent_id))
    conn.commit()

def _apply_telico_snapshot(snap):
    global _last_sync
    if not snap.get("ok"):
        _last_sync = {"at": now_iso(), "ok": False, "queues": {}, "phones": []}
        return
    now = now_local()
    for p in snap.get("phones", []):
        row = conn.execute("SELECT * FROM agents WHERE phone=?", (p["extension"],)).fetchone()
        if row is None:
            conn.execute("INSERT INTO agents (name, phone, status, status_since, shift_start, french, english, delivery) VALUES (?,?,?,?,?,0,0,0)", (p["name"], p["extension"], "Logout / Offline", now.isoformat(), (now - timedelta(hours=1)).isoformat()))
            conn.execute("INSERT INTO status_events (agent_id, status, started_at) SELECT id, ?, ? FROM agents WHERE phone=?", ("Logout / Offline", now.isoformat(), p["extension"]))
        elif row["name"] != p["name"]:
            conn.execute("UPDATE agents SET name=? WHERE phone=?", (p["name"], p["extension"]))

    on_break_ids = {r["id"] for r in conn.execute("SELECT id FROM agents WHERE status='Break / Lunch'")}

    queue_log = {}
    for q_name, q_data in snap.get("queues", {}).items():
        telico_members = {m["extension"]: m["available"] for m in q_data.get("members", [])}
        
        for r in conn.execute("SELECT id, phone FROM agents WHERE phone != ''"):
            arow_id = r["id"]
            arow_phone = r["phone"]
            
            if arow_id in on_break_ids: continue
            if _is_pending(arow_id, q_name): continue

            if arow_phone in telico_members:
                is_avail = telico_members[arow_phone]
                _set_queue_flag(arow_id, q_name, is_avail)
            else:
                _set_queue_flag(arow_id, q_name, False)

        db_available = []
        for r in conn.execute("SELECT * FROM agents WHERE phone != ''"):
            if r[QUEUE_COL[q_name]] and r["id"] not in on_break_ids:
                db_available.append({"extension": r["phone"], "name": r["name"]})

        w = q_data["waiting"]; lw = q_data["longest_wait_seconds"]
        conn.execute("UPDATE queue_metrics SET waiting=?, longest_wait_seconds=? WHERE queue=?", (w, lw, q_name))
        queue_log[q_name] = {
            "waiting": w, "members": len(q_data.get("members", [])),
            "available_count": len(db_available),
            "available_names": [a["name"] for a in db_available],
            "available_agents": db_available,
            "member_names": [m["name"] for m in q_data.get("members", [])],
        }
        
    cdr = snap.get("cdr", {})
    for q_name, q_stats in cdr.items():
        conn.execute(
            "INSERT INTO calls_answered (date, queue, count, missed, attempts) VALUES (?,?,?,?,?) "
            "ON CONFLICT(date, queue) DO UPDATE SET count=?, missed=?, attempts=?",
            (today_str(), q_name, q_stats["answered"], q_stats["missed"], q_stats["attempts"],
             q_stats["answered"], q_stats["missed"], q_stats["attempts"])
        )
        
    conn.commit()
    _last_sync = {"at": now_iso(), "ok": True, "queues": queue_log, "phones": len(snap.get("phones", []))}

async def _update_agent_status(agent_id: int, new: str, actor: str = "System"):
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: return {"ok": False, "reason": "agent not found"}
    
    prev = row["status"]
    if prev == new: return {"ok": True}

    if new == "Break / Lunch" and prev != "Break / Lunch":
        current = [q for q in QUEUES if row[QUEUE_COL[q]]]
        conn.execute("UPDATE agents SET break_saved_queues=?, break_notified=0 WHERE id=?", (",".join(current), agent_id))
        conn.commit()
        apply_status(agent_id, "Break / Lunch")
        for q_name in current:
            _pending_toggles[(agent_id, q_name)] = time.time()
            _set_queue_flag(agent_id, q_name, False)
            asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], False))
        conn.commit()
        return {"ok": True}

    if prev == "Break / Lunch" and new != "Break / Lunch":
        break_start = datetime.fromisoformat(row["status_since"])
        now = now_local()
        break_duration = now - break_start
        old_shift_start = datetime.fromisoformat(row["shift_start"])
        new_shift_start = old_shift_start + break_duration
        conn.execute("UPDATE agents SET shift_start=? WHERE id=?", (new_shift_start.isoformat(), agent_id))
        conn.commit()

        try: saved_str = row["break_saved_queues"] or ""
        except: saved_str = ""
        
        conn.execute("UPDATE agents SET break_saved_queues='', break_notified=0 WHERE id=?", (agent_id,))
        conn.commit()

        if new == "Logout / Offline":
            apply_status(agent_id, "Logout / Offline")
            for q_name in QUEUES:
                if row[QUEUE_COL[q_name]]:
                    _pending_toggles[(agent_id, q_name)] = time.time()
                    _set_queue_flag(agent_id, q_name, False)
                    asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], False))
            conn.commit()
        else:
            to_restore = [q for q in saved_str.split(",") if q in QUEUES]
            for q_name in to_restore:
                _pending_toggles[(agent_id, q_name)] = time.time()
                _set_queue_flag(agent_id, q_name, True)
                asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], True))
            apply_status(agent_id, new)
        return {"ok": True}

    if new == "Logout / Offline" and prev != "Logout / Offline":
        for q_name in QUEUES:
            if row[QUEUE_COL[q_name]]:
                _pending_toggles[(agent_id, q_name)] = time.time()
                _set_queue_flag(agent_id, q_name, False)
                asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], False))
        conn.commit()

    if new == "Login / Available" and prev == "Logout / Offline":
        for q_name in QUEUES:
            if not row[QUEUE_COL[q_name]]:
                _pending_toggles[(agent_id, q_name)] = time.time()
                _set_queue_flag(agent_id, q_name, True)
                asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], True))
        conn.commit()

    apply_status(agent_id, new)
    return {"ok": True}

async def _fire_call(phone: str, name: str, agent_id: int | None = None):
    try:
        r = await asyncio.to_thread(telico_client.call_extension, phone)
        if not r.get("ok"): log.warning("Call to %s failed: %s", name, r)
    except Exception as e: log.error("Call failed: %s", e)

    if agent_id is None: return
    asyncio.create_task(_watch_for_answer(agent_id, name, phone))

async def _watch_for_answer(agent_id: int, agent_name: str, phone: str):
    deadline = time.time() + ANSWER_WATCH_SECONDS
    while time.time() < deadline:
        await asyncio.sleep(ANSWER_POLL_INTERVAL)
        row = conn.execute("SELECT status FROM agents WHERE id=?", (agent_id,)).fetchone()
        if not row or row["status"] != "Break / Lunch": return
        try:
            info = await asyncio.to_thread(telico_client.fetch_lines_activity, phone)
            if info.get("answered") is True:
                await _update_agent_status(agent_id, "Login / Available", actor="Auto (call answered)")
                await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
                return
        except Exception: continue

    if ANSWER_FALLBACK_ACK:
        await _update_agent_status(agent_id, "Login / Available", actor="Auto (fallback timeout)")
        await hub.broadcast({"type": "snapshot", "data": snapshot_full()})

async def telico_sync_loop():
    while True:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        if not telico_is_configured(): continue
        try:
            snap = await asyncio.to_thread(telico_client.fetch_all)
            _apply_telico_snapshot(snap)
            await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
        except Exception as e:
            log.error("Telico sync error: %s", e)

async def break_expiry_loop():
    while True:
        await asyncio.sleep(BREAK_CHECK_INTERVAL)
        now = now_local()
        try: rows = conn.execute("SELECT * FROM agents WHERE status='Break / Lunch' AND break_notified=0").fetchall()
        except: continue
        
        for row in rows:
            try: since = datetime.fromisoformat(row["status_since"])
            except: continue
            if (now - since).total_seconds() < BREAK_LIMIT_SECONDS: continue
            cur = conn.execute("UPDATE agents SET break_notified=1 WHERE id=? AND break_notified=0 AND status='Break / Lunch'", (row["id"],))
            if cur.rowcount == 0: continue
            conn.commit()
            if CALL_ON_BREAK_EXPIRY and row["phone"]:
                asyncio.create_task(_fire_call(row["phone"], row["name"], row["id"]))
            await hub.broadcast({"type": "break_expired", "agent_id": row["id"], "agent_name": row["name"]})
            await hub.broadcast({"type": "snapshot", "data": snapshot_full()})

def _send_sms_or_mms(destination: str, message: str, media_input: str = "") -> dict:
    if not (SMS_USERNAME and SMS_PASSWORD and SMS_SOURCE_DID):
        return {"ok": False, "channel": "", "error": "SMS not configured"}
    auth = HTTPBasicAuth(SMS_USERNAME, SMS_PASSWORD)
    media_val = (media_input or "").strip()
    mms_error = ""
    if media_val:
        is_local_file = not media_val.startswith("http://") and not media_val.startswith("https://")
        if is_local_file:
            data = {"source_did": SMS_SOURCE_DID, "destination": destination, "message": message}
            try:
                if os.path.exists(media_val):
                    with open(media_val, "rb") as f:
                        response = requests.post(MMS_API_URL, auth=auth, data=data, files={"media_1": f}, timeout=10)
                    if response.status_code in (200, 201, 202): return {"ok": True, "channel": "MMS", "error": ""}
                    mms_error = f"HTTP {response.status_code}"
                else: mms_error = "file not found"
            except Exception as e: mms_error = f"MMS exception: {e}"
        else:
            payload = {"source_did": SMS_SOURCE_DID, "destination": destination, "message": message, "media_urls": [media_val]}
            try:
                response = requests.post(MMS_API_URL, auth=auth, json=payload, timeout=30)
                if response.status_code in (200, 201, 202): return {"ok": True, "channel": "MMS URL", "error": ""}
                mms_error = f"HTTP {response.status_code}"
            except Exception as e: mms_error = f"MMS exception: {e}"
            
    sms_payload = {"source_did": SMS_SOURCE_DID, "destination": destination, "message": message}
    try:
        response = requests.post(SMS_API_URL, auth=auth, json=sms_payload, timeout=30)
        if response.status_code in (200, 201, 202): return {"ok": True, "channel": "SMS", "error": mms_error}
        return {"ok": False, "channel": "SMS", "error": f"HTTP {response.status_code}"}
    except Exception as e: return {"ok": False, "channel": "SMS", "error": f"SMS exception: {e}"}

def _parse_recipients_csv(raw_bytes: bytes) -> list[dict]:
    text = raw_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames: return []
    field_map = {f.lower().strip(): f for f in reader.fieldnames}
    phone_key = field_map.get("phone") or field_map.get("number") or field_map.get("mobile")
    if not phone_key: return []
    out = []
    for row in reader:
        phone = (row.get(phone_key) or "").strip()
        if phone: out.append({"phone": phone, "name": (row.get("name") or "").strip()})
    return out

def _parse_recipients_text(text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line: continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) == 1: out.append({"phone": parts[0], "name": ""}); continue
        a, b = parts[0], parts[1]
        if all(c.isdigit() or c in "+-() " for c in a): out.append({"phone": a, "name": b})
        else: out.append({"phone": b, "name": a})
    return [r for r in out if r["phone"]]

async def _process_sms_blast(recipients: list[dict], template: str, media_path: str):
    results = []
    total = len(recipients)
    await hub.broadcast({"type": "sms_progress", "sent": 0, "total": total, "results": []})

    for r in recipients:
        phone = (r["phone"] or "").strip()
        if not phone: continue
        name = (r.get("name") or "").strip() or "Valued Customer"
        message = template.replace("{name}", name)
        try: result = await asyncio.to_thread(_send_sms_or_mms, phone, message, media_path)
        except Exception as e: result = {"ok": False, "channel": "", "error": str(e)}

        results.append({"phone": phone, "name": name, "ok": bool(result.get("ok")), "channel": result.get("channel", ""), "error": result.get("error", "")})
        sent = sum(1 for x in results if x["ok"])
        await hub.broadcast({"type": "sms_progress", "sent": sent, "total": total, "results": results})
        if SMS_THROTTLE_SECONDS > 0: await asyncio.sleep(SMS_THROTTLE_SECONDS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    seed()
    t1 = asyncio.create_task(telico_sync_loop())
    t2 = asyncio.create_task(break_expiry_loop())
    yield
    t1.cancel(); t2.cancel()

app = FastAPI(lifespan=lifespan)

@app.get("/api/snapshot")
def api_snapshot(): return snapshot_full()

@app.post("/api/agents/{agent_id}/status")
async def api_set_status(agent_id: int, body: StatusBody, request: Request):
    sess = get_session(request)
    if not sess: raise HTTPException(401, "not logged in")
    if sess["role"] == "agent" and sess.get("agent_id") is not None and sess.get("agent_id") != agent_id:
        raise HTTPException(403, "not your agent")
    if sess["role"] == "wallboard": raise HTTPException(403, "read-only")
    
    res = await _update_agent_status(agent_id, body.status, actor=sess["role"])
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return res

@app.post("/api/agents/{agent_id}/queues/{queue}")
async def api_set_agent_queue(agent_id: int, queue: str, body: QueueBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "agent not found")
    
    if body.on:
        members = await asyncio.to_thread(telico_client.fetch_queue_members, QUEUE_IDS[queue])
        is_in_queue = any(m["extension"] == row["phone"] for m in members)
        if not is_in_queue:
            return {"ok": False, "reason": f"Agent is not assigned to {queue} in Telico."}
    
    _pending_toggles[(agent_id, queue)] = time.time()
    _set_queue_flag(agent_id, queue, body.on)
    
    try: r = await asyncio.to_thread(telico_client.set_queue_membership, QUEUE_IDS[queue], row["phone"], body.on)
    except Exception as e: r = {"ok": False, "reason": str(e)}
    ok = bool((r or {}).get("ok"))
    
    if not ok:
        _set_queue_flag(agent_id, queue, not body.on)
        _pending_toggles.pop((agent_id, queue), None)
        
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": ok, "reason": (r or {}).get("reason", "")}

@app.post("/api/agents/{agent_id}/all-queues")
async def api_agent_all_queues(agent_id: int, body: QueueBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "agent not found")
    
    if body.on and row["status"] == "Logout / Offline":
        await _update_agent_status(agent_id, "Login / Available", actor="Manager")
    elif not body.on and row["status"] != "Logout / Offline" and row["status"] != "Break / Lunch":
        await _update_agent_status(agent_id, "Logout / Offline", actor="Manager")

    for q_name in QUEUES:
        _pending_toggles[(agent_id, q_name)] = time.time()
        _set_queue_flag(agent_id, q_name, body.on)
        try: 
            r = await asyncio.to_thread(telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], body.on)
            if not (r or {}).get("ok"):
                _set_queue_flag(agent_id, q_name, not body.on)
                _pending_toggles.pop((agent_id, q_name), None)
        except: 
            _set_queue_flag(agent_id, q_name, not body.on)
            _pending_toggles.pop((agent_id, q_name), None)
        
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True}

@app.post("/api/queues/mass")
async def api_mass_queue(body: MassBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    agents = conn.execute("SELECT * FROM agents WHERE phone != ''").fetchall()
    for a in agents:
        if body.on and a["status"] == "Logout / Offline":
            await _update_agent_status(a["id"], "Login / Available", actor="Manager")
        elif not body.on and a["status"] != "Logout / Offline" and a["status"] != "Break / Lunch":
            await _update_agent_status(a["id"], "Logout / Offline", actor="Manager")
            
        for q_name in body.queues:
            _pending_toggles[(a["id"], q_name)] = time.time()
            _set_queue_flag(a["id"], q_name, body.on)
            try: await asyncio.to_thread(telico_client.set_queue_membership, QUEUE_IDS[q_name], a["phone"], body.on)
            except: 
                _set_queue_flag(a["id"], q_name, not body.on)
                _pending_toggles.pop((a["id"], q_name), None)
    conn.commit()
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True}

@app.post("/api/debug/telico/simulate-break")
async def api_debug_simulate_break(request: Request, agent_id: int):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "agent not found")

    await _update_agent_status(agent_id, "Break / Lunch", actor="Manager")
    if row["phone"]: asyncio.create_task(_fire_call(row["phone"], row["name"], row["id"]))
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True, "agent": row["name"]}

@app.get("/api/debug/telico/try-call")
async def api_debug_try_call(request: Request, phone: str):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    return await asyncio.to_thread(telico_client.call_extension, phone)

@app.post("/api/sms/send")
async def api_sms_send(
    request: Request, message_template: str = Form(...), media_mode: str = Form("none"),
    media_path: str = Form(""), media_file: UploadFile | None = File(None),
    recip_mode: str = Form("csv"), recipients_text: str = Form(""), recipients_csv: UploadFile | None = File(None),
):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    recipients = _parse_recipients_csv(await recipients_csv.read()) if recip_mode == "csv" else _parse_recipients_text(recipients_text)
    
    resolved_media = ""
    if media_mode == "upload" and media_file and media_file.filename:
        safe_name = f"{secrets.token_hex(6)}_{Path(media_file.filename).name}"
        saved_path = SMS_UPLOAD_DIR / safe_name
        with open(saved_path, "wb") as f: shutil.copyfileobj(media_file.file, f)
        resolved_media = str(saved_path)
    elif media_mode == "path" and media_path.strip(): resolved_media = media_path.strip()

    asyncio.create_task(_process_sms_blast(recipients, message_template, resolved_media))
    return {"ok": True, "status": "started"}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    await hub.join(ws)
    try:
        await ws.send_text(json.dumps({"type": "snapshot", "data": snapshot_full()}))
        while True: await ws.receive_text()
    except WebSocketDisconnect: pass
    finally: await hub.leave(ws)

def _status_seconds(agent_id):
    start_today = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    end_today = start_today + timedelta(days=1)
    out = {}
    for r in conn.execute("SELECT status,started_at,ended_at FROM status_events WHERE agent_id=?", (agent_id,)):
        start = datetime.fromisoformat(r["started_at"])
        end = datetime.fromisoformat(r["ended_at"]) if r["ended_at"] else now_local()
        start = max(start, start_today)
        end = min(end, end_today)
        if start >= end: continue
        dur = int((end - start).total_seconds())
        out[r["status"]] = out.get(r["status"], 0) + dur
    return out

def _queue_seconds(agent_id):
    start_today = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    end_today = start_today + timedelta(days=1)
    out = {q: 0 for q in QUEUES}
    open_ts = {}
    for r in conn.execute("SELECT queue, joined, at FROM queue_events WHERE agent_id=? ORDER BY id", (agent_id,)):
        q = r["queue"]
        if q not in QUEUES: continue
        at = datetime.fromisoformat(r["at"])
        if r["joined"] == 1:
            open_ts.setdefault(q, at)
        else:
            if q in open_ts:
                start = open_ts[q]
                end = at
                start = max(start, start_today)
                end = min(end, end_today)
                if start < end:
                    out[q] += int((end - start).total_seconds())
                del open_ts[q]
    now = now_local()
    for q, start in open_ts.items():
        end = now
        start = max(start, start_today)
        end = min(end, end_today)
        if start < end:
            out[q] += int((end - start).total_seconds())
    return out

@app.get("/api/reports/shift-summary")
def api_shift_summary(request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    rows = []
    for a in conn.execute("SELECT * FROM agents ORDER BY name"):
        s = _status_seconds(a["id"])
        q = _queue_seconds(a["id"])
        rows.append({
            "agent": a["name"], "shiftStart": a["shift_start"],
            "available": s.get("Login / Available", 0), "break": s.get("Break / Lunch", 0),
            "admin": s.get("Admin / Wrap Up", 0), "outbound": s.get("Outbound", 0),
            "french": q.get("French", 0), "english": q.get("English", 0), "delivery": q.get("Delivery Line", 0),
            "queueTotal": sum(q.values()), "total": sum(s.values()),
        })
    return rows

# ---------------------------------------------------------------- HTML

BASE_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; }
body { background: #0b1120; color: #f1f5f9; font-family:'Space Grotesk',system-ui,sans-serif; font-size:14px; }
a { color:inherit; text-decoration:none; }
button { font-family:inherit; cursor:pointer; border:none; background:none; color:inherit; }
.mono { font-family:'JetBrains Mono',ui-monospace,monospace; }

:root {
  --bg: #0b1120; --panel: #0f172a; --elevated: #1e293b; --hover: #334155;
  --border: #1e293b; --border-strong: #334155;
  --text: #f1f5f9; --text-muted: #94a3b8;
  --accent: #22d3ee; --orange: #f59e0b;
}
body, button, input, textarea, select {
  font-family: 'Space Grotesk', system-ui, sans-serif;
}
.mono, .clock, .wb-time, .wb-stat-big, .wb-stat-sm-val, .agent-select, .status-card .time {
  font-family: 'JetBrains Mono', ui-monospace, monospace;
}
.topbar { display:flex; align-items:center; justify-content:space-between; gap:20px; padding:10px 20px; background:var(--panel); border-bottom:1px solid var(--border); flex-shrink:0; }
.topbar-left { display:flex; align-items:center; gap:18px; }
.brand { display:flex; align-items:center; gap:10px; }
.brand-logo { width:28px; height:28px; border-radius:8px; background:linear-gradient(135deg, #22d3ee, #0891b2); display:flex; align-items:center; justify-content:center; font-weight:800; font-size:11px; color:#ffffff; }
.brand-title { font-weight:600; font-size:15px; color:var(--text); }
.top-nav { display:flex; gap:2px; }
.top-nav a { padding:8px 14px; font-size:13.5px; font-weight:500; color:var(--text-muted); border-radius:8px; transition:all .15s; }
.top-nav a:hover { color:var(--text); }
.top-nav a.active { color:var(--text); background:var(--elevated); }

.user-select { padding:7px 12px; background:var(--elevated); border:1px solid var(--border-strong); border-radius:10px; font-size:12.5px; color:var(--text); cursor:pointer; }
.clock-block { text-align:right; padding-left:10px; }
.clock { font-family:'JetBrains Mono',monospace; font-size:20px; font-weight:600; color:var(--text); }
.clock-date { color:var(--text-muted); font-size:11px; font-family:'JetBrains Mono',monospace; }

.view { flex:1; overflow:auto; padding:22px 24px 28px; }

.wb-top { display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 1px solid var(--border); padding-bottom: 20px; margin-bottom: 30px; }
.wb-brand { display: flex; align-items: center; gap: 16px; }
.wb-brand-logo { width: 48px; height: 48px; fill: var(--text); }
.wb-brand-text { display: flex; flex-direction: column; }
.wb-brand-live { color: var(--accent); font-size: 11px; font-weight: 700; letter-spacing: 0.15em; text-transform: uppercase; }
.wb-brand-title { color: var(--text); font-size: 28px; font-weight: 700; letter-spacing: 0.02em; text-transform: uppercase; }
.wb-time-block { text-align: right; }
.wb-time { color: var(--text); font-size: 36px; font-weight: 700; font-family: 'JetBrains Mono', monospace; line-height: 1; }
.wb-date { color: var(--text-muted); font-size: 12px; letter-spacing: 0.1em; text-transform: uppercase; margin-top: 4px; font-weight: 600; }

.queues { display:grid; grid-template-columns:repeat(3,1fr); gap:24px; margin-bottom: 30px; }
@media (max-width: 1100px) { .queues { grid-template-columns:1fr; } }

.wb-card { background: var(--elevated); border-radius: 4px; padding: 24px 28px; display: flex; flex-direction: column; position: relative; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.2); }
.wb-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 4px; }
.wb-card:nth-child(1)::before { background: #3b82f6; }
.wb-card:nth-child(2)::before { background: #f59e0b; }
.wb-card:nth-child(3)::before { background: #06b6d4; }

.wb-head { display: flex; justify-content: space-between; margin-bottom: 20px; }
.wb-q-id { color: var(--text-muted); font-size: 12px; font-weight: 600; letter-spacing: 0.1em; }
.wb-range { color: #10b981; font-size: 11px; font-weight: 700; letter-spacing: 0.05em; }
.wb-title { font-size: 32px; font-weight: 700; color: var(--text); margin-bottom: 24px; letter-spacing: -0.02em; }

.wb-big-stats { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
.wb-stat-lbl { color: var(--text-muted); font-size: 11px; font-weight: 600; letter-spacing: 0.08em; margin-bottom: 4px; }
.wb-stat-big { font-size: 64px; font-weight: 700; line-height: 0.9; font-family: 'JetBrains Mono', monospace; }
.wb-stat-big.white { color: var(--text); }
.wb-stat-big.orange { color: var(--orange); }

.wb-small-stats { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; border-top: 1px solid var(--border-strong); padding-top: 16px; }
.wb-stat-sm-val { font-size: 24px; font-weight: 600; color: var(--text); font-family: 'JetBrains Mono', monospace; margin-top: 4px; }

/* --- LOGGED IN NOW PILLS --- */
.logged-in-now {
  margin-top: 20px;
  border-top: 1px solid var(--border-strong);
  padding-top: 16px;
}
.lin-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  color: var(--text-muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.05em;
  margin-bottom: 12px;
}
.lin-header span:last-child {
  color: #10b981; /* Green */
}
.lin-pills {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.agent-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 12px;
  border: 1px solid #10b981; /* Green border */
  border-radius: 50px;
  font-size: 12px;
  color: var(--text);
  background: rgba(16, 185, 129, 0.05);
}
.agent-pill .dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: #10b981;
}

.wb-bottom { margin-top: 30px; padding-top: 20px; border-top: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; }
.wb-all-queues { display: flex; align-items: center; gap: 16px; color: var(--text-muted); font-size: 13px; font-weight: 600; letter-spacing: 0.1em; }
.wb-all-queues .count { font-size: 18px; color: var(--text); font-family: 'JetBrains Mono', monospace; }
.wb-footer-right { display: flex; align-items: center; gap: 20px; }
.wb-live { display: flex; align-items: center; gap: 8px; color: var(--text-muted); font-size: 12px; font-family: 'JetBrains Mono', monospace; }
.wb-live::before { content: ''; width: 8px; height: 8px; border-radius: 50%; background: var(--accent); }
.wb-btn-group { display: flex; gap: 10px; }
.wb-btn { padding: 8px 16px; border: 1px solid var(--border-strong); border-radius: 4px; background: transparent; color: var(--text); font-size: 12px; font-weight: 600; cursor: pointer; text-transform: uppercase; letter-spacing: 0.05em; }
.wb-btn:hover { background: var(--hover); }

.section { background:var(--panel); border:1px solid var(--border); border-radius:14px; padding:18px 20px; margin-bottom:18px; box-shadow:0 4px 12px rgba(0,0,0,0.2); }
.section-title { font-size:13px; font-weight:700; color:var(--text); margin-bottom:14px; }

.mass-top { display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; }
.mass-btn { padding:10px 16px; border-radius:9px; font-size:12.5px; font-weight:700; display:inline-flex; align-items:center; gap:8px; background: var(--elevated); border: 1px solid var(--border-strong); color: var(--text); }
.mass-btn:hover { background: var(--hover); }

.queue-cards { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; }
@media (max-width: 900px) { .queue-cards { grid-template-columns:1fr; } }
.queue-card { padding:14px 16px; background:var(--elevated); border:1px solid var(--border); border-radius:11px; }
.queue-card .name { font-size:12.5px; font-weight:700; color:var(--text); margin-bottom:10px; }
.queue-card .row { display:flex; gap:8px; }
.q-btn { padding:7px 12px; border-radius:7px; font-size:11.5px; font-weight:600; background:var(--panel); border:1px solid var(--border-strong); color:var(--text-muted); }
.q-btn:hover { background:var(--hover); color: var(--text); }

table { width:100%; border-collapse:collapse; }
thead th { text-align:left; font-size:11px; color:var(--text-muted); padding:12px 12px; border-bottom:1px solid var(--border); font-weight:500; }
tbody td { padding:14px 12px; border-bottom:1px solid var(--border); font-size:13px; color:var(--text); }
tbody tr:last-child td { border-bottom:none; }

.status-pill-select { appearance: none; padding: 5px 26px 5px 12px; border-radius: 8px; font-size: 12.5px; font-weight: 600; border: 1px solid var(--border-strong); cursor: pointer; background-color: var(--panel); background-image: url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 6'%3e%3cpath fill='%23cbd5e1' d='M0 0l5 6 5-6H0z'/%3e%3c/svg%3e"); background-repeat: no-repeat; background-position: right 8px center; background-size: 10px 6px; }
.status-pill-select.st-available { color: var(--accent); border-color: var(--accent); }
.status-pill-select.st-break { color: var(--orange); border-color: var(--orange); }
.status-pill-select.st-admin { color: #a855f7; border-color: #a855f7; }
.status-pill-select.st-offline { color: var(--text-muted); border-color: var(--border-strong); }
.status-pill-select option { background: var(--panel); color: var(--text); }

.switch { width:44px; height:24px; border-radius:12px; background:var(--border-strong); position:relative; cursor:pointer; transition:background .18s; display:inline-block; vertical-align:middle; padding:0; border:none; }
.switch::after { content:''; position:absolute; top:2px; left:2px; width:20px; height:20px; border-radius:50%; background:#fff; transition:left .18s; }
.switch.on { background:var(--accent); }
.switch.on::after { left:22px; }

.icon-btn { width:30px; height:30px; border-radius:7px; display:inline-flex; align-items:center; justify-content:center; background:var(--elevated); border:1px solid var(--border-strong); color:var(--text-muted); font-size:12px; margin-right:4px; }
.icon-btn:hover { background:var(--hover); color:var(--text); }

.tab-nav { display:flex; gap:2px; border-bottom:1px solid var(--border); margin-bottom:20px; }
.tab { padding:10px 18px; font-size:13px; font-weight:500; color:var(--text-muted); border-bottom:2px solid transparent; cursor:pointer; }
.tab.active { color:var(--accent); border-bottom-color:var(--accent); }
.tab-panel { display:none; }
.tab-panel.active { display:block; }

/* --- AGENT PAGE SPECIFIC --- */
.agent-wrap { max-width: 480px; margin: 40px auto; padding: 0 16px; }
.agent-select { width: 100%; padding: 16px; font-size: 16px; border-radius: 12px;
                border: 1px solid var(--border-strong); background: var(--elevated); color: var(--text);
                font-family: inherit; outline: none; appearance: none; margin-bottom: 24px;
                background-image: url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 6'%3e%3cpath fill='%23cbd5e1' d='M0 0l5 6 5-6H0z'/%3e%3c/svg%3e");
                background-repeat: no-repeat; background-position: right 16px center; background-size: 12px 8px;
                cursor: pointer; transition: border-color 0.2s;
}
.agent-select:focus { border-color: var(--accent); }
.agent-hello { text-align:center; padding: 0 0 24px; }
.agent-hello .name { font-size: 28px; font-weight: 700; color: var(--text); }
.agent-hello .sub { font-size: 14px; color: var(--text-muted); font-family: 'JetBrains Mono', monospace; margin-top: 4px; }
.status-card { padding: 28px; text-align: center; border-radius: 20px; background: var(--elevated); border: 2px solid var(--border-strong); margin-bottom: 24px; position: relative; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.2); transition: border-color 0.3s; }
.status-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 4px; background: var(--accent); }
.status-card.on-break { border-color: var(--orange); }
.status-card.on-break::before { background: var(--orange); }
.status-card.on-break .big { color: var(--orange); }
.status-card .lbl { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.15em; font-weight: 700; margin-bottom: 8px; }
.status-card .big { font-size: 28px; font-weight: 700; color: var(--accent); margin-bottom: 8px; }

/* --- NEW SPLIT TIMERS --- */
.status-card .timers { display: flex; justify-content: space-around; margin-top: 20px; border-top: 1px solid var(--border-strong); padding-top: 16px; }
.status-card .timer-item { display: flex; flex-direction: column; align-items: center; }
.status-card .timer-lbl { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.1em; font-weight: 700; margin-bottom: 6px; }
.status-card .time { font-family: 'JetBrains Mono', monospace; font-size: 24px; font-weight: 700; color: var(--text); line-height: 1; }
.status-card .time.orange { color: var(--orange); }

.action-btn { width: 100%; padding: 20px 16px; border-radius: 16px; font-size: 18px; font-weight: 700; border: 2px solid var(--border-strong); background: var(--elevated); color: var(--text); display: flex; align-items: center; justify-content: center; gap: 12px; margin-bottom: 12px; transition: all 0.2s; }
.action-btn:hover:not([disabled]) { background: var(--hover); border-color: var(--text-muted); }
.action-btn:active:not([disabled]) { transform: scale(0.98); }
.action-btn[disabled] { opacity: 0.3; cursor: not-allowed; border-color: transparent; background: transparent; }
.btn-clockin { border-color: #10b981; color: #10b981; background: rgba(16,185,129,0.05); }
.btn-clockin:hover:not([disabled]) { background: rgba(16,185,129,0.15); border-color: #10b981; }
.btn-clockout { color: var(--text-muted); }
.btn-break { border-color: var(--orange); color: var(--orange); background: rgba(245,158,11,0.05); }
.btn-break:hover:not([disabled]) { background: rgba(245,158,11,0.15); border-color: var(--orange); }
.btn-login { border-color: var(--accent); color: var(--accent); background: rgba(34,211,238,0.05); }
.btn-login:hover:not([disabled]) { background: rgba(34,211,238,0.15); border-color: var(--accent); }

.divider { height: 1px; background: var(--border-strong); margin: 24px 0; opacity: 0.5; }

/* --- SMS, ALERT OVERLAY, LOGIN --- */
.sms-grid { display:grid; grid-template-columns: 1fr 1fr; gap:20px; align-items:flex-start; }
@media (max-width: 1000px) { .sms-grid { grid-template-columns:1fr; } }
.sms-field { margin-bottom:16px; }
.sms-field textarea, .sms-field input[type="text"] { width:100%; padding:12px 14px; font-size:13.5px; border-radius:10px; border:1px solid var(--border-strong); background:var(--elevated); color:var(--text); resize:vertical; }
.sms-seg { display:inline-flex; background:var(--elevated); border:1px solid var(--border-strong); border-radius:10px; padding:3px; gap:3px; margin-bottom:10px; }
.sms-seg button { padding:7px 14px; font-size:12px; font-weight:600; color:var(--text-muted); border-radius:7px; }
.sms-seg button.active { background:var(--panel); color:var(--text); }
.sms-drop { display:flex; flex-direction:column; align-items:center; justify-content:center; padding:22px 16px; border:2px dashed var(--border-strong); border-radius:12px; background:var(--elevated); color:var(--text-muted); cursor:pointer; }
.sms-drop:hover { border-color:var(--accent); color:var(--accent); }
.sms-row { display:grid; grid-template-columns: 1fr 1fr 90px 1fr; gap:10px; padding:9px 12px; border-bottom:1px solid var(--border); font-size:12.5px; }
.toast { position: fixed; left: 50%; bottom: 24px; transform: translateX(-50%); background: var(--panel); border: 1px solid var(--border-strong); padding: 12px 20px; border-radius: 10px; font-size: 13px; z-index: 999; box-shadow:0 4px 12px rgba(0,0,0,0.4); }
.toast.error { border-color: var(--orange); color: var(--orange); }
.toast.ok { border-color: var(--accent); color: var(--accent); }
.login-wrap { max-width: 400px; margin: 80px auto; padding: 32px; background: var(--panel); border: 1px solid var(--border); border-radius: 16px; box-shadow: 0 4px 12px rgba(0,0,0,0.4); }
.login-wrap h1 { font-size: 22px; margin-bottom: 8px; color: var(--text); }
.login-wrap p { color: var(--text-muted); font-size: 13px; margin-bottom: 24px; }
.login-wrap input { width: 100%; padding: 14px; font-size: 15px; border-radius: 10px; border: 1px solid var(--border-strong); background: var(--elevated); color: var(--text); margin-bottom: 12px; }
.login-wrap button { width: 100%; padding: 14px; border-radius: 10px; background: var(--accent); color: #0b1120; font-weight: 700; font-size: 15px; }
.login-error { color: var(--orange); background: var(--elevated); padding: 10px 14px; border-radius: 8px; font-size: 13px; margin-bottom: 14px; }

.alert-overlay {
  position: fixed; inset: 0; z-index: 9999;
  background: radial-gradient(circle at center, rgba(30,41,59,0.98), rgba(11,17,32,0.98));
  display: none; align-items: center; justify-content: center;
  padding: 24px; text-align: center;
}
.alert-overlay.show { display: flex; }
.alert-overlay .inner {
  max-width: 480px; width: 100%; background: var(--panel);
  border: 2px solid var(--orange); border-radius: 24px;
  padding: 32px 24px; box-shadow: 0 30px 80px rgba(0,0,0,0.7);
}
.alert-overlay .icon { font-size: 64px; line-height: 1; margin-bottom: 12px; }
.alert-overlay .title { font-size: 28px; font-weight: 800; color: var(--orange); margin-bottom: 12px; }
.alert-overlay .msg { font-size: 16px; color: var(--text); line-height: 1.6; margin-bottom: 24px; }
.alert-overlay .btn-dismiss {
  background: var(--orange); color: #0b1120; padding: 18px 32px; border-radius: 12px;
  font-size: 18px; font-weight: 800; letter-spacing: .03em; width: 100%;
}
.alert-overlay .btn-dismiss[disabled] { opacity: 0.6; cursor: wait; }

/* --- SMS PAGE ENHANCEMENTS --- */
.sms-label { color: var(--text); font-size: 13px; font-weight: 500; margin-bottom: 8px; display: block; }
.hint { color: var(--text-muted); font-size: 12px; margin-top: 6px; line-height: 1.4; }
.hint code { color: var(--accent); background: var(--elevated); padding: 2px 4px; border-radius: 4px; font-family: 'JetBrains Mono', monospace; }

.sms-drop input[type="file"] { display: none; }
.filename { color: var(--accent); font-size: 13px; margin-top: 8px; font-family: 'JetBrains Mono', monospace; }

.sms-actions { display: flex; gap: 10px; border-top: 1px solid var(--border); padding-top: 20px; }
.sms-actions .mass-btn { background: var(--accent); color: #0b1120; border: none; font-weight: 700; padding: 12px 24px; border-radius: 9px; transition: background 0.2s; }
.sms-actions .mass-btn:hover { background: #06b6d4; }
.sms-actions .reset-btn { padding: 12px 24px; border-radius: 9px; font-size: 12.5px; font-weight: 600; background: transparent; border: 1px solid var(--border-strong); color: var(--text-muted); cursor: pointer; transition: all 0.2s; }
.sms-actions .reset-btn:hover { background: var(--elevated); color: var(--text); }

#sms-test-btn { background: var(--elevated); border: 1px solid var(--border-strong); color: var(--text); border-radius: 9px; font-weight: 600; padding: 0 20px; cursor: pointer; }
#sms-test-btn:hover { background: var(--hover); }
"""

def page_shell(title, role_label, top_right_html, body_html, script, show_clock=True, topbar_minimal=False, active_nav=""):
    if topbar_minimal:
        topbar = f'<header class="topbar"><div class="topbar-left"><div class="brand"><div class="brand-logo">{BRAND_LOGO}</div><div class="brand-title">{BRAND_NAME}</div></div></div></header>'
    else:
        def navcls(k): return "active" if k == active_nav else ""
        nav_html = ""
        if role_label.startswith("Manager"):
            nav_html = f'<nav class="top-nav"><a href="/wallboard" class="{navcls("wallboard")}">Wallboard</a><a href="/manager" class="{navcls("manager")}">Manager</a><a href="/sms" class="{navcls("sms")}">SMS</a><a href="/reports" class="{navcls("reports")}">Reports</a></nav>'
        clock_html = '<div class="clock-block"><div class="clock" id="clock">--:--:--</div><div class="clock-date" id="clock-date">—</div></div>' if show_clock else ""
        user_html = f'<select class="user-select" onchange="if(this.value)location.href=this.value"><option>{role_label.split(" · ")[0]}</option><option value="/logout">Log out</option></select>'
        topbar = f'<header class="topbar"><div class="topbar-left"><div class="brand"><div class="brand-logo">{BRAND_LOGO}</div><div class="brand-title">{BRAND_NAME}</div></div>{nav_html}</div><div class="topbar-right">{user_html}{clock_html}</div></header>'

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>{title} · {BRAND_NAME}</title><style>{BASE_CSS}</style>
</head><body>
<div class="app" style="display:flex;flex-direction:column;min-height:100vh">
{topbar}
<main class="view">
{body_html}
</main></div>
<script>
const STATUSES = {json.dumps(STATUSES)}; const QUEUES = {json.dumps(QUEUES)};
const STATUS_CLASS = {{ 'Login / Available':'st-available','Logout / Offline':'st-offline','Admin / Wrap Up':'st-admin','Break / Lunch':'st-break','Outbound':'st-outbound' }};
const state = {{ agents: [], metrics: [], telicoConfigured: false, breakLimitSeconds: 1800, lastSync: null, lastUpdate: Date.now() }};
const listeners = [];
function onUpdate(fn){{ listeners.push(fn); }}
function notify(){{ for (const fn of listeners) fn(state); }}
function startClock(){{ const t = document.getElementById('clock'); const d = document.getElementById('clock-date'); if (!t) return; const tick = () => {{ const n=new Date(); t.textContent=n.toLocaleTimeString('en-GB'); d.textContent=n.toLocaleDateString('en-GB',{{weekday:'long',day:'numeric',month:'short',year:'numeric'}}); }}; tick(); setInterval(tick,1000); }}
function fmtDuration(from){{ const s = Math.max(0, Math.floor((Date.now()-new Date(from).getTime())/1000)); const h=Math.floor(s/3600), m=Math.floor((s%3600)/60), sec=s%60; return String(h).padStart(2,'0')+':'+String(m).padStart(2,'0')+':'+String(sec).padStart(2,'0'); }}
function fmtHM(sec){{ const h=Math.floor(sec/3600), m=Math.floor((sec%3600)/60); return String(h).padStart(2,'0')+':'+String(m).padStart(2,'0'); }}
function fmtWait(sec){{ const m=Math.floor(sec/60), s=sec%60; return String(m).padStart(2,'0')+':'+String(s).padStart(2,'0'); }}
function fmtHHMM(iso){{ if (!iso) return '--:--'; const d = new Date(iso); return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0'); }}
function fmtHMS(sec){{
  sec = Math.max(0, Math.floor(Number(sec) || 0));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  return String(h).padStart(2,'0') + ':' + String(m).padStart(2,'0') + ':' + String(s).padStart(2,'0');
}}
function showToast(msg, kind=''){{ const el = document.createElement('div'); el.className = 'toast ' + kind; el.textContent = msg; document.body.appendChild(el); setTimeout(() => el.remove(), 5000); }}
(function connectWS(){{ const proto = location.protocol === 'https:' ? 'wss' : 'ws'; const ws = new WebSocket(proto+'://'+location.host+'/ws'); ws.onclose = () => {{ setTimeout(connectWS, 2000); }}; ws.onmessage = (e) => {{ const msg = JSON.parse(e.data); if (msg.type === 'snapshot') {{ state.agents = msg.data.agents; state.metrics = msg.data.metrics; state.telicoConfigured = !!msg.data.telicoConfigured; state.lastSync = msg.data.lastSync || null; state.lastUpdate = Date.now(); notify(); }} else if (msg.type === 'break_expired') {{ if (typeof window.handleBreakExpired === 'function') window.handleBreakExpired(msg); }} else if (msg.type === 'sms_progress') {{ if (typeof window.handleSmsProgress === 'function') window.handleSmsProgress(msg); }} }}; }})();
startClock();
{script}
</script></body></html>"""

@app.get("/")
def page_root(request: Request):
    sess = get_session(request)
    if sess: return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    return RedirectResponse("/login", status_code=302)

@app.get("/login", response_class=HTMLResponse)
def page_login(request: Request, error: str = ""):
    sess = get_session(request)
    if sess: return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    err = '<div class="login-error">Invalid credentials.</div>' if error else ""
    body = f"""<div class="login-wrap"><h1>Sign in</h1><p>{BRAND_NAME} Control Center</p>{err}<form method="post" action="/login"><input name="username" placeholder="Username" autofocus><input name="password" type="password" placeholder="Password"><button type="submit">Sign in</button></form><div class="hint" style="text-align:center; margin-top:20px; font-size:11px;">Agents: Use username <b>agent</b> and password <b>pw123456</b></div></div>"""
    return page_shell("Login", "Guest", "", body, "", show_clock=False, topbar_minimal=True)

@app.post("/login")
async def page_login_submit(request: Request):
    form = await request.form()
    username = (form.get("username") or "").strip()
    password = form.get("password") or ""
    
    row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if row and row["password"] == password: 
        tok = make_session(row["role"], row["agent_id"])
        resp = RedirectResponse(home_for_role(row["role"]), status_code=302)
        resp.set_cookie("cc_session", tok, httponly=True, samesite="lax", max_age=SESSION_TTL)
        return resp

    return RedirectResponse("/login?error=1", status_code=302)

@app.get("/logout")
def page_logout(request: Request):
    drop_session(request)
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("cc_session")
    return resp

WALLBOARD_SCRIPT = r"""
function renderQueues(){
  const wrap = document.getElementById('queues');
  if (!wrap) return;
  
  const totalWaiting = state.metrics.reduce((acc, m) => acc + (m.waiting || 0), 0);
  const twEl = document.getElementById('wb-total-waiting');
  if (twEl) twEl.textContent = totalWaiting;

  const now = new Date();
  const timeEl = document.getElementById('wb-time');
  if (timeEl) timeEl.textContent = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  const dateEl = document.getElementById('wb-date');
  if (dateEl) dateEl.textContent = now.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'short', day: 'numeric' }).toUpperCase();
  
  const updEl = document.getElementById('wb-updated');
  if (updEl && state.lastSync && state.lastSync.at) updEl.textContent = new Date(state.lastSync.at).toLocaleTimeString();

  wrap.innerHTML = state.metrics.map(m => {
    const info = state.lastSync && state.lastSync.queues ? state.lastSync.queues[m.queue] : null;
    const avail = info ? info.available_count : 0;
    const availAgents = info && info.available_agents ? info.available_agents : [];
    const oldestHold = m.waiting ? fmtWait(m.longestWaitSeconds) : '—';
    
    const pillsHtml = availAgents.map(a => `
      <div class="agent-pill">
        <span class="dot"></span>
        ${a.extension} "${a.name}"
      </div>
    `).join('');

    return `
    <div class="wb-card">
      <div class="wb-head"><div class="wb-q-id">QUEUE ${m.queue_id || '---'}</div><div class="wb-range">IN RANGE</div></div>
      <div class="wb-title">${m.queue.replace(' Line', '')}</div>
      
      <div class="wb-big-stats">
        <div><div class="wb-stat-lbl">TOTAL TODAY</div><div class="wb-stat-big white">${m.callsAnsweredToday || 0}</div></div>
        <div><div class="wb-stat-lbl">MISSED TODAY</div><div class="wb-stat-big orange">${m.callsMissedToday || 0}</div></div>
      </div>
      
      <div class="wb-small-stats">
        <div><div class="wb-stat-lbl">CALLS<br>HOLDING</div><div class="wb-stat-sm-val">${m.waiting}</div></div>
        <div><div class="wb-stat-lbl">OLDEST HOLD</div><div class="wb-stat-sm-val">${oldestHold}</div></div>
        <div><div class="wb-stat-lbl">AGENTS<br>AVAILABLE</div><div class="wb-stat-sm-val">${avail}</div></div>
      </div>

      <div class="logged-in-now">
        <div class="lin-header">
          <span>LOGGED IN NOW</span>
          <span>${avail} AGENTS</span>
        </div>
        <div class="lin-pills">
          ${pillsHtml || '<span style="font-size:12px;color:var(--text-muted)">No agents available</span>'}
        </div>
      </div>
    </div>`;
  }).join('');
}
onUpdate(renderQueues);
setInterval(renderQueues, 1000);
"""

WALLBOARD_BODY = """
<div class="wb-shell">
  <div class="wb-top">
    <div class="wb-brand"><div class="brand-logo" style="width:48px;height:48px;font-size:18px;border-radius:12px;">BB</div>
      <div class="wb-brand-text"><div class="wb-brand-live">LIVE OPERATIONS</div><div class="wb-brand-title">Queue Wallboard</div></div>
    </div>
    <div class="wb-time-block"><div class="wb-time" id="wb-time">00:00:00 AM</div><div class="wb-date" id="wb-date">—</div></div>
  </div>
  <div class="queues" id="queues"></div>
  <div class="wb-bottom">
    <div class="wb-all-queues"><span>ALL QUEUES</span><span class="count" id="wb-total-waiting">0</span><span>CALLS HOLDING</span></div>
    <div class="wb-footer-right">
      <div class="wb-live">LIVE • Updated <span id="wb-updated">--:--:--</span></div>
      <div class="wb-btn-group">
        <button class="wb-btn" onclick="window.location.reload()">↻ REFRESH</button>
        <button class="wb-btn" onclick="location.href='/manager'">MANAGER VIEW</button>
        <button class="wb-btn" onclick="location.href='/logout'">SIGN OUT</button>
      </div>
    </div>
  </div>
</div>
"""

@app.get("/wallboard", response_class=HTMLResponse)
def page_wallboard(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] not in ("manager", "wallboard"): return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    return page_shell("Wallboard", "Manager" if sess["role"] == "manager" else "Wallboard", "", WALLBOARD_BODY, WALLBOARD_SCRIPT, active_nav="wallboard")

MANAGER_SCRIPT = r"""
async function api(path, body) {
  const r = await fetch(path, { method: 'POST', credentials: 'include', headers: {'Content-Type':'application/json'}, body: body?JSON.stringify(body):undefined });
  return r.json().catch(()=>null);
}
window.changeStatus = async (id, s) => { 
    const res = await api(`/api/agents/${id}/status`, {status: s}); 
    if (!res || !res.ok) {
        showToast(res?.reason || 'Failed to update status', 'error');
    }
};
window.toggleQueue = async (ev, id, q, on) => { 
    ev.preventDefault(); ev.stopPropagation(); 
    const r = await api(`/api/agents/${id}/queues/${encodeURIComponent(q)}`, {on}); 
    if (!r || !r.ok) { 
        showToast(r?.reason || 'Failed to update queue', 'error');
    } 
};
window.toggleAllQueues = async (id, on) => await api(`/api/agents/${id}/all-queues`, {on});
window.massQueue = async (queues, on) => { await api('/api/queues/mass', {queues, on}); showToast((on ? 'Login' : 'Logout') + ' sent', 'ok'); };
window.simulateBreak = async (agentId) => { const r = await fetch(`/api/debug/telico/simulate-break?agent_id=${agentId}`, {method: 'POST', credentials: 'include'}).then(x=>x.json()); if(r.ok) showToast('Simulating break...', 'ok'); };

function renderQueues(){
  const wrap = document.getElementById('queues');
  if (!wrap) return;
  
  const totalWaiting = state.metrics.reduce((acc, m) => acc + (m.waiting || 0), 0);
  const twEl = document.getElementById('wb-total-waiting');
  if (twEl) twEl.textContent = totalWaiting;

  const now = new Date();
  const timeEl = document.getElementById('wb-time');
  if (timeEl) timeEl.textContent = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  const dateEl = document.getElementById('wb-date');
  if (dateEl) dateEl.textContent = now.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'short', day: 'numeric' }).toUpperCase();
  
  const updEl = document.getElementById('wb-updated');
  if (updEl && state.lastSync && state.lastSync.at) updEl.textContent = new Date(state.lastSync.at).toLocaleTimeString();

  wrap.innerHTML = state.metrics.map(m => {
    const info = state.lastSync && state.lastSync.queues ? state.lastSync.queues[m.queue] : null;
    const avail = info ? info.available_count : 0;
    const availAgents = info && info.available_agents ? info.available_agents : [];
    const oldestHold = m.waiting ? fmtWait(m.longestWaitSeconds) : '—';
    
    const pillsHtml = availAgents.map(a => `
      <div class="agent-pill">
        <span class="dot"></span>
        ${a.extension} "${a.name}"
      </div>
    `).join('');

    return `
    <div class="wb-card">
      <div class="wb-head"><div class="wb-q-id">QUEUE ${m.queue_id || '---'}</div><div class="wb-range">IN RANGE</div></div>
      <div class="wb-title">${m.queue.replace(' Line', '')}</div>
      
      <div class="wb-big-stats">
        <div><div class="wb-stat-lbl">TOTAL TODAY</div><div class="wb-stat-big white">${m.callsAnsweredToday || 0}</div></div>
        <div><div class="wb-stat-lbl">MISSED TODAY</div><div class="wb-stat-big orange">${m.callsMissedToday || 0}</div></div>
      </div>
      
      <div class="wb-small-stats">
        <div><div class="wb-stat-lbl">CALLS<br>HOLDING</div><div class="wb-stat-sm-val">${m.waiting}</div></div>
        <div><div class="wb-stat-lbl">OLDEST HOLD</div><div class="wb-stat-sm-val">${oldestHold}</div></div>
        <div><div class="wb-stat-lbl">AGENTS<br>AVAILABLE</div><div class="wb-stat-sm-val">${avail}</div></div>
      </div>

      <div class="logged-in-now">
        <div class="lin-header">
          <span>LOGGED IN NOW</span>
          <span>${avail} AGENTS</span>
        </div>
        <div class="lin-pills">
          ${pillsHtml || '<span style="font-size:12px;color:var(--text-muted)">No agents available</span>'}
        </div>
      </div>
    </div>`;
  }).join('');
}

function renderAgents(){
  const tb = document.getElementById('mgr-tbody');
  if (!tb) return;
  if (document.activeElement && document.activeElement.tagName === 'SELECT') return;
  
  tb.innerHTML = state.agents.map(a=>{
    const statusCls = STATUS_CLASS[a.status] || 'st-offline';
    return `
    <tr>
      <td style="font-weight:600">${a.name}</td>
      <td class="mono" style="font-size:12.5px; color:var(--text-muted)">${a.phone}</td>
      <td><select class="status-pill-select ${statusCls}" onchange="changeStatus(${a.id}, this.value)">${STATUSES.map(s=>`<option ${s===a.status?'selected':''}>${s}</option>`).join('')}</select></td>
      <td class="mono" style="font-size:12.5px">${fmtHHMM(a.statusSince)}</td>
      <td class="mono" style="font-size:12.5px; font-weight: 600;">${fmtHMS(a.shiftDurationSeconds)}</td>
      ${['French','English','Delivery Line'].map(q=>`<td><button type="button" class="switch ${a.queues[q]?'on':''}" onclick="toggleQueue(event, ${a.id}, '${q}', ${!a.queues[q]})"></button></td>`).join('')}
      <td>
        <button class="icon-btn" onclick="toggleAllQueues(${a.id}, true)">➜]</button>
        <button class="icon-btn" onclick="toggleAllQueues(${a.id}, false)">⇦]</button>
        <button class="icon-btn" onclick="simulateBreak(${a.id})">🧪</button>
      </td>
    </tr>`;
  }).join('');
}
onUpdate(()=>{ renderQueues(); renderAgents(); });
setInterval(renderAgents, 2000);
setInterval(renderQueues, 1000);
"""

MANAGER_BODY = """
<div class="wb-shell">
  <div class="wb-top">
    <div class="wb-brand"><div class="brand-logo" style="width:48px;height:48px;font-size:18px;border-radius:12px;">BB</div>
      <div class="wb-brand-text"><div class="wb-brand-live">LIVE OPERATIONS</div><div class="wb-brand-title">Manager Dashboard</div></div>
    </div>
    <div class="wb-time-block"><div class="wb-time" id="wb-time">00:00:00 AM</div><div class="wb-date" id="wb-date">—</div></div>
  </div>
  <div class="queues" id="queues"></div>
  <div class="section">
    <div class="section-title">Mass actions</div>
    <div class="mass-top">
      <button class="mass-btn" onclick="massQueue(['French','English','Delivery Line'], true)">⇥ Login All Agents to All Queues</button>
      <button class="mass-btn" onclick="massQueue(['French','English','Delivery Line'], false)">⇤ Logout All Agents from All Queues</button>
    </div>
    <div class="queue-cards">
      <div class="queue-card"><div class="name">French</div><div class="row"><button class="q-btn" onclick="massQueue(['French'], true)">Login All</button><button class="q-btn" onclick="massQueue(['French'], false)">Logout All</button></div></div>
      <div class="queue-card"><div class="name">English</div><div class="row"><button class="q-btn" onclick="massQueue(['English'], true)">Login All</button><button class="q-btn" onclick="massQueue(['English'], false)">Logout All</button></div></div>
      <div class="queue-card"><div class="name">Delivery Line</div><div class="row"><button class="q-btn" onclick="massQueue(['Delivery Line'], true)">Login All</button><button class="q-btn" onclick="massQueue(['Delivery Line'], false)">Logout All</button></div></div>
    </div>
  </div>
  <div class="section">
    <div class="section-title">Agents</div>
    <table>
      <thead><tr><th>Agent</th><th>Ext</th><th>Status</th><th>Status Since</th><th>Duration</th><th>French</th><th>English</th><th>Delivery</th><th>All queues</th></tr></thead>
      <tbody id="mgr-tbody"></tbody>
    </table>
  </div>
</div>
"""

@app.get("/manager", response_class=HTMLResponse)
def page_manager(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] != "manager": return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    return page_shell("Manager", "Manager", "", MANAGER_BODY, MANAGER_SCRIPT, active_nav="manager")

DEFAULT_SMS_TEMPLATE = "Hello {name}, this is a test message from Bundl Box."

SMS_SCRIPT = r"""
window.smsMediaMode = 'none';
window.smsRecipMode = 'csv';
window.smsMediaFile = null;
window.smsCsvFile = null;

window.setMediaMode = (m) => {
  window.smsMediaMode = m;
  document.querySelectorAll('#sms-media-seg button').forEach(b =>
    b.classList.toggle('active', b.dataset.mode === m));
  document.getElementById('sms-media-none-panel').style.display = (m === 'none') ? 'block' : 'none';
  document.getElementById('sms-media-upload-panel').style.display = (m === 'upload') ? 'block' : 'none';
  document.getElementById('sms-media-path-panel').style.display = (m === 'path') ? 'block' : 'none';
};

window.setRecipMode = (m) => {
  window.smsRecipMode = m;
  document.querySelectorAll('#sms-recip-seg button').forEach(b =>
    b.classList.toggle('active', b.dataset.mode === m));
  document.getElementById('sms-recip-csv-panel').style.display = (m === 'csv') ? 'block' : 'none';
  document.getElementById('sms-recip-paste-panel').style.display = (m === 'paste') ? 'block' : 'none';
};

window.onMediaFilePicked = (input) => {
  const f = input.files && input.files[0];
  window.smsMediaFile = f || null;
  const lbl = document.getElementById('sms-media-filename');
  lbl.textContent = f ? f.name + ' (' + Math.round(f.size / 1024) + ' KB)' : '';
};

window.onCsvFilePicked = (input) => {
  const f = input.files && input.files[0];
  window.smsCsvFile = f || null;
  const lbl = document.getElementById('sms-csv-filename');
  lbl.textContent = f ? f.name + ' (' + Math.round(f.size / 1024) + ' KB)' : '';
  if (f) previewCsv(f);
};

async function previewCsv(file) {
  try {
    const text = await file.text();
    const lines = text.split(/\r?\n/).filter(l => l.trim()).slice(0, 5);
    document.getElementById('sms-csv-preview').innerHTML =
      '<div style="font-family:ui-monospace,monospace;font-size:11.5px;color:var(--text-muted);margin-top:8px">' +
      lines.map(l => escapeHtml(l)).join('<br>') + '</div>';
  } catch(e) {}
}
function escapeHtml(s){ return (s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function buildFormData(message, recipientsText, isTest = false) {
  const fd = new FormData();
  fd.append('message_template', message);
  fd.append('media_mode', window.smsMediaMode);
  
  if (isTest) {
    fd.append('recip_mode', 'paste');
    fd.append('recipients_text', recipientsText);
  } else {
    fd.append('recip_mode', window.smsRecipMode);
    if (window.smsRecipMode === 'csv') {
      fd.append('recipients_csv', window.smsCsvFile, window.smsCsvFile.name);
    } else {
      fd.append('recipients_text', recipientsText);
    }
  }

  if (window.smsMediaMode === 'upload') {
    fd.append('media_file', window.smsMediaFile, window.smsMediaFile.name);
  } else if (window.smsMediaMode === 'path') {
    fd.append('media_path', document.getElementById('sms-media-path').value.trim());
  }
  return fd;
}

window.sendSmsBlast = async () => {
  const btn = document.getElementById('sms-send-btn');
  const template = document.getElementById('sms-template').value;

  if (!template.trim()) { showToast('Message template is empty', 'error'); return; }
  if (window.smsMediaMode === 'upload' && !window.smsMediaFile) { showToast('Please choose a media file', 'error'); return; }
  if (window.smsMediaMode === 'path' && !document.getElementById('sms-media-path').value.trim()) { showToast('Please enter a media URL or server path', 'error'); return; }
  if (window.smsRecipMode === 'csv' && !window.smsCsvFile) { showToast('Please choose a CSV file', 'error'); return; }

  if (!confirm('Send this blast now?')) return;

  btn.disabled = true;
  btn.textContent = 'Starting...';

  try {
    const fd = buildFormData(template, document.getElementById('sms-recipients-text').value, false);
    const r = await fetch('/api/sms/send', { method: 'POST', credentials: 'include', body: fd });
    if (!r.ok) {
      const err = await r.json().catch(()=>({detail:r.statusText}));
      showToast('Send failed: ' + (err.detail || r.statusText), 'error');
      btn.disabled = false;
      btn.textContent = 'Send blast';
      return;
    }
    showToast('Blast started in background!', 'ok');
    document.getElementById('sms-summary').style.display = 'none';
    document.getElementById('sms-results').innerHTML = '<div class="sms-empty">Sending messages...</div>';

  } catch (e) {
    showToast('Send failed: ' + e.message, 'error');
    btn.disabled = false;
    btn.textContent = 'Send blast';
  }
};

window.sendTestSms = async () => {
  const btn = document.getElementById('sms-test-btn');
  const phone = document.getElementById('sms-test-phone').value.trim();
  const template = document.getElementById('sms-template').value;

  if (!phone) { showToast('Please enter a test phone number', 'error'); return; }
  if (!template.trim()) { showToast('Message template is empty', 'error'); return; }
  if (window.smsMediaMode === 'upload' && !window.smsMediaFile) { showToast('Please choose a media file', 'error'); return; }
  if (window.smsMediaMode === 'path' && !document.getElementById('sms-media-path').value.trim()) { showToast('Please enter a media URL or server path', 'error'); return; }

  btn.disabled = true;
  btn.textContent = 'Sending...';

  try {
    const fd = buildFormData(template, phone, true);
    const r = await fetch('/api/sms/send', { method: 'POST', credentials: 'include', body: fd });
    if (!r.ok) {
      const err = await r.json().catch(()=>({detail:r.statusText}));
      showToast('Test failed: ' + (err.detail || r.statusText), 'error');
      return;
    }
    showToast(`Test started for ${phone}`, 'ok');
  } catch (e) {
    showToast('Test failed: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Send Test';
  }
};

window.clearSmsForm = () => {
  window.smsMediaFile = null;
  window.smsCsvFile = null;
  document.getElementById('sms-media-file').value = '';
  document.getElementById('sms-csv-file').value = '';
  document.getElementById('sms-media-filename').textContent = '';
  document.getElementById('sms-csv-filename').textContent = '';
  document.getElementById('sms-csv-preview').innerHTML = '';
  document.getElementById('sms-media-path').value = '';
  document.getElementById('sms-recipients-text').value = '';
  document.getElementById('sms-test-phone').value = '';
  document.getElementById('sms-results').innerHTML = '<div class="sms-empty">No results yet.</div>';
  document.getElementById('sms-summary').style.display = 'none';
  setMediaMode('none');
  setRecipMode('csv');
  document.getElementById('sms-send-btn').disabled = false;
  document.getElementById('sms-send-btn').textContent = 'Send blast';
};

window.handleSmsProgress = (msg) => {
  renderSmsResults(msg);
  const btn = document.getElementById('sms-send-btn');
  if (msg.results.length >= msg.total) {
    btn.disabled = false;
    btn.textContent = 'Send blast';
    showToast(`Blast finished: ${msg.sent} sent`, msg.sent === msg.total ? 'ok' : 'error');
  } else {
    btn.disabled = true;
    btn.textContent = `Sending (${msg.results.length}/${msg.total})...`;
  }
};

function renderSmsResults(data) {
  const wrap = document.getElementById('sms-results');
  const summary = document.getElementById('sms-summary');
  summary.style.display = 'flex';
  summary.innerHTML = `
    <div><div class="lbl">Sent</div><div class="big" style="color:var(--accent)">${data.sent}</div></div>
    <div><div class="lbl">Processed</div><div class="big">${data.results.length} / ${data.total}</div></div>
  `;
  const reversed = [...data.results].reverse();
  wrap.innerHTML = reversed.map(r => `
    <div class="sms-row">
      <div class="phone">${escapeHtml(r.phone)}</div>
      <div>${escapeHtml(r.name) || '—'}</div>
      <div>${escapeHtml(r.channel) || '—'}</div>
      <div class="${r.ok ? 'ok' : 'err'}">${r.ok ? '✓ sent' : '✗ ' + escapeHtml(r.error || 'failed')}</div>
    </div>
  `).join('') || '<div class="sms-empty">Processing...</div>';
}

document.addEventListener('DOMContentLoaded', () => {
  setMediaMode('none');
  setRecipMode('csv');
});
"""

SMS_BODY = f"""
<div class="sms-grid">
  <div class="section">
    <div class="section-title">1 · Message</div>
    <div class="sms-field">
      <label class="sms-label">Message template</label>
      <textarea id="sms-template" rows="5" class="mono" placeholder="Type your message here...">{DEFAULT_SMS_TEMPLATE}</textarea>
      <div class="hint">Use <code>{{name}}</code> to insert each recipient's name.</div>
    </div>

    <div class="section-title" style="margin-top:22px">2 · Media (single — same for every recipient)</div>
    <div class="sms-field">
      <div class="sms-seg" id="sms-media-seg">
        <button type="button" data-mode="none"   class="active" onclick="setMediaMode('none')">No media</button>
        <button type="button" data-mode="upload"               onclick="setMediaMode('upload')">Upload file</button>
        <button type="button" data-mode="path"                 onclick="setMediaMode('path')">URL / server path</button>
      </div>

      <div id="sms-media-none-panel">
        <div class="hint">Plain SMS only — no MMS attachment.</div>
      </div>

      <div id="sms-media-upload-panel" style="display:none">
        <label class="sms-drop" for="sms-media-file">
          <div class="big-icon">🖼️</div>
          <div>Click to choose an image / video / PDF</div>
          <div class="hint">Uploaded once and reused for every recipient.</div>
          <div class="filename" id="sms-media-filename"></div>
          <input type="file" id="sms-media-file" accept="image/*,video/*,application/pdf" onchange="onMediaFilePicked(this)">
        </label>
      </div>

      <div id="sms-media-path-panel" style="display:none">
        <input type="text" id="sms-media-path" placeholder="/content/BILLY.jpg  or  https://example.com/pic.jpg">
        <div class="hint">Server file path (like <code>/content/BILLY.jpg</code>) or a public HTTPS URL.</div>
      </div>
    </div>

    <div class="section-title" style="margin-top:22px">3 · Bulk Recipients</div>
    <div class="sms-field">
      <div class="sms-seg" id="sms-recip-seg">
        <button type="button" data-mode="csv"   class="active" onclick="setRecipMode('csv')">Upload CSV</button>
        <button type="button" data-mode="paste"               onclick="setRecipMode('paste')">Paste list</button>
      </div>

      <div id="sms-recip-csv-panel">
        <label class="sms-drop" for="sms-csv-file">
          <div class="big-icon">📄</div>
          <div>Click to choose a CSV file</div>
          <div class="hint">Columns needed: <code>name,phone</code> (extra columns are ignored)</div>
          <div class="filename" id="sms-csv-filename"></div>
          <input type="file" id="sms-csv-file" accept=".csv,text/csv" onchange="onCsvFilePicked(this)">
        </label>
        <div id="sms-csv-preview"></div>
      </div>

      <div id="sms-recip-paste-panel" style="display:none">
        <textarea id="sms-recipients-text" rows="5" class="mono"
          placeholder="Alice Smith,6473959350&#10;Bob Jones,5145550102"></textarea>
        <div class="hint">One per line: <code>name,phone</code> or <code>phone,name</code> or just <code>phone</code>.</div>
      </div>
    </div>

    <div class="section-title" style="margin-top:22px">4 · Quick Test</div>
    <div class="sms-field">
      <div style="display:flex; gap:10px;">
        <input type="text" id="sms-test-phone" placeholder="Phone number (e.g. 4165551234)" style="flex:1;">
        <button id="sms-test-btn" class="mass-btn" onclick="sendTestSms()" style="padding:0 20px; white-space:nowrap;">Send Test</button>
      </div>
      <div class="hint">Sends a single message using the template and media above.</div>
    </div>

    <div class="sms-actions">
      <button id="sms-send-btn" class="mass-btn" onclick="sendSmsBlast()">Send Bulk Blast</button>
      <button class="reset-btn" onclick="clearSmsForm()">Clear Form</button>
    </div>
  </div>

  <div class="section">
    <div class="section-title">Results</div>
    <div id="sms-summary" class="sms-summary" style="display:none"></div>
    <div id="sms-results" class="sms-results">
      <div class="sms-empty">No results yet.</div>
    </div>
  </div>
</div>
"""

@app.get("/sms", response_class=HTMLResponse)
def page_sms(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] != "manager": return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    return page_shell("SMS", "Manager", "", SMS_BODY, SMS_SCRIPT, active_nav="sms")

# --- AGENT SCRIPT WITH EXACT DESIGN MATCHING ---
AGENT_SCRIPT_TEMPLATE = r"""
const SESSION_AGENT_ID = {agent_id};
let myAgentId = SESSION_AGENT_ID || parseInt(localStorage.getItem('cc_agent_id') || '0', 10) || null;
let audioCtx = null;
let pendingStatus = null;
let overlayShownFor = null;

async function api(path, body, method='POST'){
  const r = await fetch(path, { method, credentials: 'include', headers: {'Content-Type':'application/json'},
    body: body?JSON.stringify(body):undefined });
  return r.json().catch(()=>null);
}
function beep(){
  try {
    audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();
    if (audioCtx.state === 'suspended') audioCtx.resume();
    const o = audioCtx.createOscillator(); const g = audioCtx.createGain();
    o.connect(g); g.connect(audioCtx.destination); o.frequency.value = 880;
    g.gain.setValueAtTime(0.001, audioCtx.currentTime);
    g.gain.exponentialRampToValueAtTime(0.35, audioCtx.currentTime + 0.05);
    g.gain.exponentialRampToValueAtTime(0.001, audioCtx.currentTime + 0.8);
    o.start(); o.stop(audioCtx.currentTime + 0.9);
  } catch(e) {}
}
function ensureOverlay(){
  let ov = document.getElementById('alert-overlay');
  if (ov) return ov;
  ov = document.createElement('div'); ov.id = 'alert-overlay'; ov.className = 'alert-overlay';
  ov.innerHTML = `
    <div class="inner"><div class="icon">⏰</div>
      <div class="title" id="alert-overlay-title">Break is over</div>
      <div class="msg" id="alert-overlay-msg">Your desk phone is ringing. Answer it, then tap OK below.</div>
      <button class="btn-dismiss" id="alert-overlay-btn" onclick="acknowledgeBreakOver()">OK — I'm back</button>
    </div>`;
  document.body.appendChild(ov); return ov;
}
function showBreakOverlay(){
  const ov = ensureOverlay(); ov.classList.add('show');
  if (navigator.vibrate) navigator.vibrate([500, 200, 500, 200, 500]);
  beep();
  if ('Notification' in window && Notification.permission === 'granted') {
    try {
      new Notification('Break is over',
        { body: 'Your desk phone is ringing. Answer it, then tap OK in the app.',
          tag: 'cc-break-over', requireInteraction: true });
    } catch (e) {}
  }
}
window.acknowledgeBreakOver = async () => {
  const btn = document.getElementById('alert-overlay-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Returning to Available…'; }
  try { await api(`/api/agents/${myAgentId}/status`, {status: 'Login / Available'}); }
  catch (e) {}
  if (btn) { btn.disabled = false; btn.textContent = "OK — I'm back"; }
  const ov = document.getElementById('alert-overlay');
  if (ov) ov.classList.remove('show');
  overlayShownFor = null;
  render();
};
window.handleBreakExpired = (msg) => {
  if (msg.agent_id !== myAgentId) return;
  const key = String(msg.agent_id);
  if (overlayShownFor === key) return;
  overlayShownFor = key;
  showBreakOverlay();
};
document.addEventListener('visibilitychange', () => {
  if (document.hidden) return;
  const a = state.agents.find(x => x.id === myAgentId);
  if (a && a.status === 'Break / Lunch' && a.breakNotified && overlayShownFor !== String(a.id)) {
    overlayShownFor = String(a.id);
    showBreakOverlay();
  }
});
function renderNamePicker(){
  const sel = document.getElementById('agent-pick');
  if (!sel) return;
  if (SESSION_AGENT_ID) { sel.style.display = 'none'; return; }
  sel.innerHTML = '<option value="">— Select your name —</option>' +
    state.agents.map(a=>`<option value="${a.id}" ${a.id===myAgentId?'selected':''}>
      ${a.name} (${a.phone||''})</option>`).join('');
  sel.onchange = () => {
    myAgentId = parseInt(sel.value, 10) || null;
    if (myAgentId) localStorage.setItem('cc_agent_id', myAgentId);
    else localStorage.removeItem('cc_agent_id');
    render();
  };
}
function me(){ return state.agents.find(a => a.id === myAgentId); }
function render(){
  renderNamePicker();
  const a = me();
  const main = document.getElementById('agent-main');
  if (!a) {
    main.style.display = 'none';
    document.getElementById('agent-hello').innerHTML =
      '<div class="name">Pick your name</div><div class="sub">This phone will remember it</div>';
    return;
  }
  main.style.display = 'block';
  document.getElementById('agent-hello').innerHTML =
    `<div class="name">${a.name}</div><div class="sub">ext ${a.phone||'—'}</div>`;
  const sc = document.getElementById('status-card');
  sc.querySelector('.big').textContent = a.status;
  sc.querySelector('.big').className = 'big ' + (STATUS_CLASS[a.status]||'');
  
  // Live updating of both timers
  const elapsed = (Date.now() - (state.lastUpdate || Date.now())) / 1000;
  let shiftSecs = a.shiftDurationSeconds || 0;
  let breakSecs = a.totalBreakSeconds || 0;
  
  if (a.status !== 'Logout / Offline' && a.status !== 'Break / Lunch') {
      shiftSecs += elapsed;
  }
  if (a.status === 'Break / Lunch') {
      breakSecs += elapsed;
  }
  
  sc.querySelector('#shift-time').textContent = fmtHMS(shiftSecs);
  sc.querySelector('#break-time').textContent = fmtHMS(breakSecs);
  
  const onBreak = (a.status === 'Break / Lunch');
  const isOffline = (a.status === 'Logout / Offline');
  sc.classList.toggle('on-break', onBreak);
  
  document.getElementById('btn-clockin').disabled = !isOffline || (pendingStatus !== null);
  document.getElementById('btn-clockout').disabled = isOffline || (pendingStatus !== null);
  document.getElementById('btn-break').disabled = onBreak || isOffline || (pendingStatus !== null);
  document.getElementById('btn-login').disabled = !onBreak || (pendingStatus !== null);
  
  if (onBreak && a.breakNotified && overlayShownFor !== String(a.id)) {
    overlayShownFor = String(a.id);
    showBreakOverlay();
  }
}
window.startMyBreak = async () => {
  if (!myAgentId || pendingStatus !== null) return;
  pendingStatus = 'Break / Lunch'; render();
  try { await api(`/api/agents/${myAgentId}/status`, {status: 'Break / Lunch'}); }
  catch (e) {}
  finally { pendingStatus = null; }
  render();
};
window.logBackIn = async () => {
  if (!myAgentId || pendingStatus !== null) return;
  await window.acknowledgeBreakOver();
};
window.clockIn = async () => {
  if (!myAgentId || pendingStatus !== null) return;
  pendingStatus = 'Login / Available'; render();
  try { await api(`/api/agents/${myAgentId}/status`, {status: 'Login / Available'}); }
  catch (e) {}
  finally { pendingStatus = null; }
  render();
};
window.clockOut = async () => {
  if (!myAgentId || pendingStatus !== null) return;
  pendingStatus = 'Logout / Offline'; render();
  try { await api(`/api/agents/${myAgentId}/status`, {status: 'Logout / Offline'}); }
  catch (e) {}
  finally { pendingStatus = null; }
  render();
};
onUpdate(render);
setInterval(render, 1000);
render();
"""

# --- NEW AGENT BODY WITH WALLBOARD THEME ---
AGENT_BODY = """
<div class="agent-wrap">
  <select class="agent-select" id="agent-pick"></select>
  <div id="agent-hello" class="agent-hello" style="margin-top:20px"></div>
  
  <div id="agent-main" style="display:none">
    <div class="status-card" id="status-card">
      <div class="lbl">Current status</div>
      <div class="big">—</div>
      
      <div class="timers">
        <div class="timer-item">
          <div class="timer-lbl">Shift Time</div>
          <div class="time mono" id="shift-time">00:00:00</div>
        </div>
        <div class="timer-item">
          <div class="timer-lbl">Break Time</div>
          <div class="time mono orange" id="break-time">00:00:00</div>
        </div>
      </div>
    </div>
    
    <button type="button" id="btn-clockin" class="action-btn btn-clockin" onclick="clockIn()">
      <span style="font-size: 22px;">🟢</span> Clock In
    </button>
    <button type="button" id="btn-clockout" class="action-btn btn-clockout" onclick="clockOut()">
      <span style="font-size: 22px;">🔴</span> Clock Out
    </button>
    
    <div class="divider"></div>

    <button type="button" id="btn-break" class="action-btn btn-break" onclick="startMyBreak()">
      <span style="font-size: 22px;">☕</span> Start Break
    </button>
    <button type="button" id="btn-login" class="action-btn btn-login" onclick="logBackIn()">
      <span style="font-size: 22px;">✓</span> Log Back In
    </button>
  </div>
</div>
"""

@app.get("/agent", response_class=HTMLResponse)
def page_agent(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] not in ("manager", "agent"): return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    agent_id = sess.get("agent_id") if sess["role"] == "agent" else None
    script = AGENT_SCRIPT_TEMPLATE.replace("{agent_id}", str(agent_id) if agent_id else "null")
    role_label = "Agent" if sess["role"] == "agent" else "Manager"
    return page_shell("Agent", role_label, "", AGENT_BODY, script, active_nav="agent")

REPORTS_SCRIPT = r"""
let tab = 'summary';

window.switchTab = (t) => {
  tab = t;
  document.querySelectorAll('.tab').forEach(el => {
    el.classList.toggle('active', el.dataset.tab === t);
  });
  document.querySelectorAll('.tab-panel').forEach(el => {
    el.classList.toggle('active', el.id === 'panel-' + t);
  });
  if (t === 'summary') loadSummary();
  if (t === 'activity') loadActivity();
};

async function loadSummary(){
  const rows = await fetch('/api/reports/shift-summary', {credentials: 'include'}).then(r=>r.json()).catch(()=>[]);
  const tb = document.getElementById('sum-tbody');
  if (!tb) return;
  tb.innerHTML = rows.map(r=>`
    <tr>
      <td style="font-weight:600">${r.agent}</td>
      <td class="mono" style="color:var(--text-muted)">
        ${new Date(r.shiftStart).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})}</td>
      <td class="mono">${fmtHM(r.available)}</td>
      <td class="mono">${fmtHM(r.break)}</td>
      <td class="mono">${fmtHM(r.admin)}</td>
      <td class="mono">${fmtHM(r.outbound)}</td>
      <td class="mono" style="color:var(--accent);font-weight:600">${fmtHM(r.french || 0)}</td>
      <td class="mono" style="color:var(--accent);font-weight:600">${fmtHM(r.english || 0)}</td>
      <td class="mono" style="color:var(--accent);font-weight:600">${fmtHM(r.delivery || 0)}</td>
      <td class="mono" style="font-weight:700">${fmtHM(r.total)}</td>
    </tr>`).join('');
}

async function loadActivity(){
  const rows = await fetch('/api/reports/shift-summary', {credentials: 'include'}).then(r=>r.json()).catch(()=>[]);
  const tb = document.getElementById('act-tbody');
  if (!tb) return;
  if (!rows.length) {
    tb.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:24px">No data yet.</td></tr>';
    return;
  }
  tb.innerHTML = rows.map(r=>`
    <tr>
      <td style="font-weight:600">${r.agent}</td>
      <td class="mono">${fmtHM(r.french || 0)}</td>
      <td class="mono">${fmtHM(r.english || 0)}</td>
      <td class="mono">${fmtHM(r.delivery || 0)}</td>
      <td class="mono" style="font-weight:600;color:var(--accent)">${fmtHM(r.queueTotal || 0)}</td>
      <td class="mono">${fmtHM(r.total)}</td>
      <td style="font-size:12.5px">${(state.agents.find(a => a.name === r.agent)||{}).status || '—'}</td>
    </tr>`).join('');
}

onUpdate(()=>{ if (tab === 'activity') loadActivity(); });
loadSummary();
setInterval(() => {
  if (tab === 'summary') loadSummary();
  else loadActivity();
}, 15000);
"""

REPORTS_BODY = """
<div class="section">
  <div class="tab-nav">
    <button class="tab active" data-tab="summary" onclick="switchTab('summary')">Shift Summary</button>
    <button class="tab" data-tab="activity" onclick="switchTab('activity')">Queue Activity</button>
  </div>

  <div id="panel-summary" class="tab-panel active">
    <table>
      <thead><tr>
        <th>Agent</th><th>Shift Start</th>
        <th>Available</th><th>Break</th><th>Admin/Wrap</th><th>Outbound</th>
        <th>French</th><th>English</th><th>Delivery</th>
        <th>Total</th>
      </tr></thead>
      <tbody id="sum-tbody"></tbody>
    </table>
  </div>

  <div id="panel-activity" class="tab-panel">
    <table>
      <thead><tr>
        <th>Agent</th>
        <th>French time</th>
        <th>English time</th>
        <th>Delivery time</th>
        <th>Total queue time</th>
        <th>Shift total</th>
        <th>Current status</th>
      </tr></thead>
      <tbody id="act-tbody"></tbody>
    </table>
  </div>
</div>
"""

@app.get("/reports", response_class=HTMLResponse)
def page_reports(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] != "manager": return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    return page_shell("Reports", "Manager", "", REPORTS_BODY, REPORTS_SCRIPT, active_nav="reports")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)