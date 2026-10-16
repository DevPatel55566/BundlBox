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

DB_PATH = Path(os.getenv("DB_PATH", str(Path(__file__).parent / "cc.db")))
SMS_UPLOAD_DIR = Path(os.getenv("SMS_UPLOAD_DIR", str(Path(__file__).parent / "sms_uploads")))
SMS_UPLOAD_DIR.mkdir(exist_ok=True, parents=True)

BREAK_LIMIT_SECONDS = 30 * 60
POLL_INTERVAL_SECONDS = 8
BREAK_CHECK_INTERVAL = 10
TOGGLE_GRACE_SECONDS = 20

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

# ↓↓↓ THIS IS THE MISSING BLOCK ↓↓↓
SMS_API_URL = os.getenv("SMS_API_URL", "https://sms.telico.cloud/api/send_sms")
MMS_API_URL = os.getenv("MMS_API_URL", "https://sms.telico.cloud/api/send_mms")
SMS_USERNAME = os.getenv("SMS_USERNAME", "patelsanjay147@gmail.com")
SMS_PASSWORD = os.getenv("SMS_PASSWORD", "Pw123456#")
SMS_SOURCE_DID = os.getenv("SMS_SOURCE_DID", "6478123085")
SMS_THROTTLE_SECONDS = float(os.getenv("SMS_THROTTLE_SECONDS", "1.0"))
# ↑↑↑ THIS IS THE MISSING BLOCK ↑↑↑

STATUSES = ["Login / Available", "Logout / Offline", "Admin / Wrap Up",
            "Break / Lunch", "Outbound"]
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

conn: sqlite3.Connection = None  # type: ignore
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

def now_iso(): return datetime.now().isoformat(timespec="seconds")
def today_str(): return datetime.now().strftime("%Y-%m-%d")

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
    conn.commit()
    for q in QUEUES:
        conn.execute("INSERT OR IGNORE INTO queue_metrics (queue, waiting, longest_wait_seconds, sla) "
                     "VALUES (?,0,0,100)", (q,))
    conn.commit()
    ensure_default_users()


def ensure_default_users():
    row = conn.execute("SELECT * FROM users WHERE username=?", (MANAGER_USERNAME,)).fetchone()
    if not row:
        conn.execute("INSERT INTO users (username, password, role, created_at) VALUES (?,?,?,?)",
                     (MANAGER_USERNAME, MANAGER_PASSWORD, "manager", now_iso()))
    else:
        conn.execute("UPDATE users SET password=?, role='manager', agent_id=NULL WHERE username=?",
                     (MANAGER_PASSWORD, MANAGER_USERNAME))
    row = conn.execute("SELECT * FROM users WHERE username=?", (WALLBOARD_USERNAME,)).fetchone()
    if not row:
        conn.execute("INSERT INTO users (username, password, role, created_at) VALUES (?,?,?,?)",
                     (WALLBOARD_USERNAME, WALLBOARD_PASSWORD, "wallboard", now_iso()))
    else:
        conn.execute("UPDATE users SET password=?, role='wallboard', agent_id=NULL WHERE username=?",
                     (WALLBOARD_PASSWORD, WALLBOARD_USERNAME))
    conn.commit()


def ensure_agent_users():
    pwd_default = AGENT_DEFAULT_PASSWORD
    for a in conn.execute("SELECT id, name, phone FROM agents"):
        ext = (a["phone"] or "").strip()
        if not ext: continue
        existing = conn.execute("SELECT * FROM users WHERE role='agent' AND agent_id=?", (a["id"],)).fetchone()
        if not existing:
            base = ext; uname = base; n = 1
            while conn.execute("SELECT 1 FROM users WHERE username=?", (uname,)).fetchone():
                n += 1; uname = f"{base}-{n}"
            password = pwd_default or ext
            conn.execute("INSERT INTO users (username, password, role, agent_id, created_at) "
                         "VALUES (?,?,?,?,?)",
                         (uname, password, "agent", a["id"], now_iso()))
        else:
            newname = ext
            if existing["username"] != newname and not \
               conn.execute("SELECT 1 FROM users WHERE username=?", (newname,)).fetchone():
                conn.execute("UPDATE users SET username=? WHERE id=?", (newname, existing["id"]))
    conn.commit()


def _set_queue_flag(agent_id: int, queue: str, on: bool):
    col = QUEUE_COL[queue]
    row = conn.execute(f"SELECT {col} FROM agents WHERE id=?", (agent_id,)).fetchone()
    old = bool(row[col]) if row else False
    new = bool(on)
    if old == new:
        return
    conn.execute(f"UPDATE agents SET {col}=? WHERE id=?", (int(new), agent_id))
    conn.execute("INSERT INTO queue_events (agent_id, queue, joined, at) VALUES (?,?,?,?)",
                 (agent_id, queue, int(new), datetime.now().isoformat()))
    conn.commit()


def _is_pending(agent_id: int, queue: str) -> bool:
    key = (agent_id, queue)
    ts = _pending_toggles.get(key)
    if ts is None:
        return False
    if time.time() - ts > TOGGLE_GRACE_SECONDS:
        _pending_toggles.pop(key, None)
        return False
    return True


def _queue_seconds(agent_id: int) -> dict:
    out = {q: 0 for q in QUEUES}
    open_ts: dict[str, datetime] = {}
    for r in conn.execute(
        "SELECT queue, joined, at FROM queue_events WHERE agent_id=? ORDER BY id",
        (agent_id,),
    ):
        q = r["queue"]
        if q not in QUEUES:
            continue
        at = datetime.fromisoformat(r["at"])
        if r["joined"] == 1:
            open_ts.setdefault(q, at)
        else:
            if q in open_ts:
                out[q] += int((at - open_ts[q]).total_seconds())
                del open_ts[q]
    now = datetime.now()
    for q, ts in open_ts.items():
        out[q] += int((now - ts).total_seconds())
    return out


def _calls_answered_today(queue: str) -> int:
    row = conn.execute("SELECT count FROM calls_answered WHERE date=? AND queue=?",
                       (today_str(), queue)).fetchone()
    return int(row["count"]) if row else 0


def agent_dict(row):
    try: phone = row["phone"] or ""
    except Exception: phone = ""
    try: saved = row["break_saved_queues"] or ""
    except Exception: saved = ""
    try: notified = int(row["break_notified"] or 0)
    except Exception: notified = 0
    return {
        "id": row["id"], "name": row["name"], "phone": phone,
        "status": row["status"], "statusSince": row["status_since"],
        "shiftStart": row["shift_start"],
        "onBreak": row["status"] == "Break / Lunch",
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
    level = "normal"
    if w >= 5 or l >= 90: level = "critical"
    elif w >= 2 or l >= 45: level = "warning"
    return {
        "queue": row["queue"], "waiting": w,
        "longestWaitSeconds": l, "sla": row["sla"], "level": level,
        "callsAnsweredToday": _calls_answered_today(row["queue"]),
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
    ts = datetime.now().isoformat()
    conn.execute("UPDATE status_events SET ended_at=? WHERE agent_id=? AND ended_at IS NULL", (ts, agent_id))
    conn.execute("INSERT INTO status_events (agent_id, status, started_at) VALUES (?,?,?)", (agent_id, new_status, ts))
    conn.execute("UPDATE agents SET status=?, status_since=? WHERE id=?", (new_status, ts, agent_id))
    conn.commit()


def _apply_telico_snapshot(snap):
    global _last_sync
    if not snap.get("ok"):
        _last_sync = {"at": now_iso(), "ok": False, "queues": {}, "phones": []}
        return
    now = datetime.now()
    for p in snap.get("phones", []):
        row = conn.execute("SELECT * FROM agents WHERE phone=?", (p["extension"],)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO agents (name, phone, status, status_since, shift_start, "
                "french, english, delivery) VALUES (?,?,?,?,?,0,0,0)",
                (p["name"], p["extension"], "Logout / Offline",
                 now.isoformat(), (now - timedelta(hours=1)).isoformat()))
            conn.execute(
                "INSERT INTO status_events (agent_id, status, started_at) "
                "SELECT id, ?, ? FROM agents WHERE phone=?",
                ("Logout / Offline", now.isoformat(), p["extension"]))
        elif row["name"] != p["name"]:
            conn.execute("UPDATE agents SET name=? WHERE phone=?", (p["name"], p["extension"]))

    on_break_ids = {r["id"] for r in
                    conn.execute("SELECT id FROM agents WHERE status='Break / Lunch'")}

    queue_log = {}
    for q_name, q_data in snap.get("queues", {}).items():
        enabled_names = []
        for m in q_data.get("members", []):
            arow = conn.execute("SELECT * FROM agents WHERE phone=?", (m["extension"],)).fetchone()
            if not arow: continue
            if arow["id"] in on_break_ids: continue
            if _is_pending(arow["id"], q_name): continue
            if m["available"]:
                _set_queue_flag(arow["id"], q_name, True)
                enabled_names.append(m["name"])

        db_available = []
        for r in conn.execute("SELECT * FROM agents WHERE phone != ''"):
            if r[QUEUE_COL[q_name]] and r["id"] not in on_break_ids:
                db_available.append(r["name"])

        w = q_data["waiting"]; lw = q_data["longest_wait_seconds"]
        prev_w = _last_waiting.get(q_name)
        if prev_w is not None and w < prev_w:
            answered = prev_w - w
            conn.execute(
                "INSERT INTO calls_answered (date, queue, count) VALUES (?,?,?) "
                "ON CONFLICT(date, queue) DO UPDATE SET count = count + ?",
                (today_str(), q_name, answered, answered))
        _last_waiting[q_name] = w
        conn.execute("UPDATE queue_metrics SET waiting=?, longest_wait_seconds=?, sla=? WHERE queue=?",
                     (w, lw, max(40.0, 100.0 - w * 4.5), q_name))
        queue_log[q_name] = {
            "waiting": w, "members": len(q_data.get("members", [])),
            "available_count": len(db_available),
            "available_names": db_available,
            "member_names": [m["name"] for m in q_data.get("members", [])],
        }
    conn.commit()
    _last_sync = {"at": now_iso(), "ok": True, "queues": queue_log,
                  "phones": len(snap.get("phones", []))}
    ensure_agent_users()


async def _do_acknowledge_break(agent_id: int, who: str = "Agent"):
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row:
        return {"ok": False, "reason": "agent not found"}
    if row["status"] != "Break / Lunch":
        return {"ok": True, "already": True, "status": row["status"]}
    try: saved_str = row["break_saved_queues"] or ""
    except Exception: saved_str = ""
    to_restore = [q for q in saved_str.split(",") if q in QUEUES]
    failed = []
    for q_name in to_restore:
        try:
            r = await asyncio.to_thread(
                telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], True)
            if not (r or {}).get("ok"): failed.append(q_name)
        except Exception:
            failed.append(q_name)
    for q in to_restore:
        _set_queue_flag(agent_id, q, True)
    conn.execute("UPDATE agents SET break_saved_queues='', break_notified=0 WHERE id=?", (agent_id,))
    conn.commit()
    apply_status(agent_id, "Login / Available")
    log.info("Break acknowledged for %s by %s — restored %s (failed: %s)",
             row["name"], who, to_restore or "none", failed or "none")
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True, "restored": to_restore, "failed": failed}


async def _fire_call(phone: str, name: str, agent_id: int | None = None):
    try:
        r = await asyncio.to_thread(telico_client.call_extension, phone)
        if r.get("ok"):
            log.info("Rang %s (%s) — watching for answer (max %ds)",
                     name, phone, ANSWER_WATCH_SECONDS)
        else:
            log.warning("Call to %s (%s) failed: %s", name, phone, r)
    except Exception as e:
        log.error("Call to %s (%s) crashed: %s", name, phone, e)

    if agent_id is None:
        return
    asyncio.create_task(_watch_for_answer(agent_id, name, phone))


async def _watch_for_answer(agent_id: int, agent_name: str, phone: str):
    deadline = time.time() + ANSWER_WATCH_SECONDS
    attempts = 0

    while time.time() < deadline:
        await asyncio.sleep(ANSWER_POLL_INTERVAL)
        attempts += 1

        row = conn.execute("SELECT status FROM agents WHERE id=?", (agent_id,)).fetchone()
        if not row or row["status"] != "Break / Lunch":
            log.info("Watch for %s stopped — no longer on break", agent_name)
            return

        try:
            info = await asyncio.to_thread(telico_client.fetch_lines_activity, phone)
            if info.get("answered") is True:
                log.info("✅ CONFIRMED ANSWER for %s (ext %s) after %d polls — restoring queues",
                         agent_name, phone, attempts)
                await _do_acknowledge_break(agent_id, who="Auto (call answered)")
                return
        except Exception as e:
            log.warning("Watch poll failed for %s: %s", agent_name, e)
            continue

    if ANSWER_FALLBACK_ACK:
        log.info("No confirmed answer for %s within %ds — fallback ON, acknowledging anyway.",
                 agent_name, ANSWER_WATCH_SECONDS)
        await _do_acknowledge_break(agent_id, who="Auto (fallback timeout)")
    else:
        log.info("No confirmed answer for %s within %ds — leaving on break",
                 agent_name, ANSWER_WATCH_SECONDS)


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
        now = datetime.now()
        try:
            rows = conn.execute(
                "SELECT * FROM agents WHERE status='Break / Lunch' AND break_notified=0"
            ).fetchall()
        except Exception:
            continue
        for row in rows:
            try: since = datetime.fromisoformat(row["status_since"])
            except Exception: continue
            if (now - since).total_seconds() < BREAK_LIMIT_SECONDS: continue
            cur = conn.execute(
                "UPDATE agents SET break_notified=1 "
                "WHERE id=? AND break_notified=0 AND status='Break / Lunch'",
                (row["id"],))
            if cur.rowcount == 0: continue
            conn.commit()
            log.info("Break expired for %s — ringing phone", row["name"])
            if CALL_ON_BREAK_EXPIRY and row["phone"]:
                asyncio.create_task(_fire_call(row["phone"], row["name"], row["id"]))
            await hub.broadcast({"type": "break_expired",
                                 "agent_id": row["id"], "agent_name": row["name"]})
            await hub.broadcast({"type": "snapshot", "data": snapshot_full()})


# ---------------------------------------------------------------- SMS sender
# This mirrors the logic from mms.py — same timeouts, same fallback behaviour.

def _send_sms_or_mms(destination: str, message: str, media_input: str = "") -> dict:
    """
    Send SMS or MMS via Telico.
    - If `media_input` is a local file path → uploads it as MMS (10s timeout)
    - If `media_input` is an http(s) URL    → sends it as MMS (30s timeout)
    - On any MMS failure → falls back to plain SMS (30s timeout)
    Returns: {"ok": bool, "channel": str, "error": str}
    """
    if not (SMS_USERNAME and SMS_PASSWORD and SMS_SOURCE_DID):
        return {"ok": False, "channel": "", "error": "SMS not configured"}

    auth = HTTPBasicAuth(SMS_USERNAME, SMS_PASSWORD)
    media_val = (media_input or "").strip()
    mms_success = False
    mms_error = ""

    # ---- Try MMS first if media provided
    if media_val:
        is_local_file = not media_val.startswith("http://") and not media_val.startswith("https://")

        if is_local_file:
            data = {
                "source_did": SMS_SOURCE_DID,
                "destination": destination,
                "message": message,
            }
            try:
                if os.path.exists(media_val):
                    with open(media_val, "rb") as f:
                        files = {"media_1": f}
                        response = requests.post(MMS_API_URL, auth=auth, data=data,
                                                 files=files, timeout=10)
                    if response.status_code in (200, 201, 202):
                        return {"ok": True, "channel": "MMS (file)", "error": ""}
                    mms_error = f"HTTP {response.status_code}: {response.text[:200]}"
                else:
                    mms_error = f"global media file '{media_val}' not found on disk"
            except requests.exceptions.Timeout:
                mms_error = "MMS (file) timeout"
            except Exception as e:
                mms_error = f"MMS (file) exception: {e}"
        else:
            mms_payload = {
                "source_did": SMS_SOURCE_DID,
                "destination": destination,
                "message": message,
                "media_urls": [media_val],
            }
            try:
                response = requests.post(MMS_API_URL, auth=auth, json=mms_payload, timeout=30)
                if response.status_code in (200, 201, 202):
                    return {"ok": True, "channel": "MMS (URL)", "error": ""}
                mms_error = f"HTTP {response.status_code}: {response.text[:200]}"
            except requests.exceptions.Timeout:
                mms_error = "MMS (URL) timeout"
            except Exception as e:
                mms_error = f"MMS (URL) exception: {e}"

    # ---- SMS fallback (also the primary channel if no media)
    sms_payload = {
        "source_did": SMS_SOURCE_DID,
        "destination": destination,
        "message": message,
    }
    try:
        response = requests.post(SMS_API_URL, auth=auth, json=sms_payload, timeout=30)
        if response.status_code in (200, 201, 202):
            return {"ok": True, "channel": "SMS", "error": mms_error}
        return {"ok": False, "channel": "SMS",
                "error": f"HTTP {response.status_code}: {response.text[:200]}"}
    except requests.exceptions.Timeout:
        return {"ok": False, "channel": "SMS", "error": "SMS timeout"}
    except Exception as e:
        return {"ok": False, "channel": "SMS", "error": f"SMS exception: {e}"}


def _parse_recipients_csv(raw_bytes: bytes) -> list[dict]:
    """
    Parse uploaded CSV. Expects `name` and `phone` columns (case-insensitive).
    Any other columns are ignored.
    """
    text = raw_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return []
    field_map = {f.lower().strip(): f for f in reader.fieldnames}
    name_key = field_map.get("name")
    phone_key = field_map.get("phone") or field_map.get("number") or field_map.get("mobile")
    if not phone_key:
        return []
    out = []
    for row in reader:
        phone = (row.get(phone_key) or "").strip()
        name = (row.get(name_key) or "").strip() if name_key else ""
        if phone:
            out.append({"phone": phone, "name": name})
    return out


def _parse_recipients_text(text: str) -> list[dict]:
    """Parse pasted lines: 'name,phone' | 'phone,name' | 'phone'."""
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) == 1:
            out.append({"phone": parts[0], "name": ""})
            continue
        a, b = parts[0], parts[1]
        a_is_phone = bool(a) and all(c.isdigit() or c in "+-() " for c in a)
        if a_is_phone:
            out.append({"phone": a, "name": b})
        else:
            out.append({"phone": b, "name": a})
    return [r for r in out if r["phone"]]


@asynccontextmanager
async def lifespan(app: FastAPI):
    seed()
    log.info("Watch window: %ds, fallback-ack=%s",
             ANSWER_WATCH_SECONDS, ANSWER_FALLBACK_ACK)
    t1 = asyncio.create_task(telico_sync_loop())
    t2 = asyncio.create_task(break_expiry_loop())
    yield
    t1.cancel(); t2.cancel()

app = FastAPI(lifespan=lifespan)


@app.get("/.well-known/appspecific/com.chrome.devtools.json")
def chrome_devtools_noise():
    return Response(status_code=204)


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


LOGIN_BODY = """
<div class="login-wrap">
  <h1>Sign in to Bundl Box</h1>
  <p>Use your assigned credentials.</p>
  {error}
  <form method="post" action="/login">
    <input type="text" name="username" placeholder="Username" autofocus autocomplete="username">
    <input type="password" name="password" placeholder="Password" autocomplete="current-password">
    <button type="submit">Sign in</button>
  </form>
</div>
"""

@app.get("/login", response_class=HTMLResponse)
def page_login(request: Request, error: str = ""):
    sess = get_session(request)
    if sess: return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    err_html = '<div class="login-error">Invalid username or password.</div>' if error else ""
    body = LOGIN_BODY.replace("{error}", err_html)
    return page_shell("Sign in", "Guest", "", "", body, "", show_clock=False, topbar_minimal=True)


@app.post("/login")
async def do_login(request: Request):
    form = await request.form()
    username = (form.get("username") or "").strip()
    password = form.get("password") or ""
    row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if not row or row["password"] != password:
        return RedirectResponse("/login?error=1", status_code=302)
    role = row["role"]
    agent_id = row["agent_id"] if role == "agent" else None
    tok = make_session(role, agent_id)
    resp = RedirectResponse(home_for_role(role), status_code=302)
    resp.set_cookie("cc_session", tok, httponly=True, samesite="lax", max_age=SESSION_TTL)
    return resp


@app.get("/logout")
def do_logout(request: Request):
    drop_session(request)
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("cc_session")
    return resp


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    return RedirectResponse(home_for_role(sess["role"]), status_code=302)


@app.get("/api/snapshot")
def api_snapshot():
    return snapshot_full()


@app.get("/api/audit")
def api_audit(request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    rows = conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 200").fetchall()
    return [dict(r) for r in rows]


@app.post("/api/agents/{agent_id}/status")
async def api_set_status(agent_id: int, body: StatusBody, request: Request):
    sess = get_session(request)
    if not sess: raise HTTPException(401, "not logged in")
    if sess["role"] == "agent" and sess.get("agent_id") != agent_id:
        raise HTTPException(403, "not your agent")
    if sess["role"] == "wallboard": raise HTTPException(403, "read-only")
    if body.status not in STATUSES: raise HTTPException(400, "invalid status")
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "agent not found")

    prev = row["status"]
    now_break = (body.status == "Break / Lunch")
    was_break = (prev == "Break / Lunch")

    if now_break and was_break:
        return {"ok": True, "queue_effects": [], "failed": [], "warn": None}

    queue_effects = []

    if now_break and not was_break:
        current = [q for q in QUEUES if row[QUEUE_COL[q]]]
        conn.execute("UPDATE agents SET break_saved_queues=?, break_notified=0 WHERE id=?",
                     (",".join(current), agent_id))
        conn.commit()
        apply_status(agent_id, "Break / Lunch")
        all_ok = True
        for q_name in current:
            try:
                r = await asyncio.to_thread(
                    telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], False)
            except Exception as e:
                r = {"ok": False, "reason": str(e)}
            queue_effects.append({"queue": q_name, "available": False, "result": r})
            if not (r or {}).get("ok"):
                all_ok = False
        for q in current:
            _set_queue_flag(agent_id, q, False)
        conn.commit()
        failed = [e for e in queue_effects if not (e.get("result") or {}).get("ok")]
        await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
        return {"ok": True, "queue_effects": queue_effects, "failed": failed,
                "warn": None if all_ok else "Some queues could not be cleared."}

    if was_break and not now_break:
        res = await _do_acknowledge_break(agent_id, who=sess["role"])
        return {"ok": True, "queue_effects": [], "failed": res.get("failed", [])}

    apply_status(agent_id, body.status)
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True, "queue_effects": [], "failed": []}


@app.post("/api/agents/{agent_id}/acknowledge_break")
async def api_acknowledge_break(agent_id: int, request: Request):
    sess = get_session(request)
    if not sess: raise HTTPException(401, "not logged in")
    if sess["role"] == "agent" and sess.get("agent_id") != agent_id:
        raise HTTPException(403, "not your agent")
    if sess["role"] == "wallboard": raise HTTPException(403, "read-only")
    return await _do_acknowledge_break(agent_id, who=sess["role"])


@app.post("/api/agents/{agent_id}/queues/{queue}")
async def api_set_agent_queue(agent_id: int, queue: str, body: QueueBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    if queue not in QUEUES: raise HTTPException(400, "invalid queue")
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "agent not found")

    _pending_toggles[(agent_id, queue)] = time.time()
    _set_queue_flag(agent_id, queue, body.on)
    conn.commit()
    try:
        r = await asyncio.to_thread(
            telico_client.set_queue_membership, QUEUE_IDS[queue], row["phone"], body.on)
    except Exception as e:
        r = {"ok": False, "reason": str(e)}
    ok = bool((r or {}).get("ok"))
    log.info("TOGGLE: agent=%s queue=%s on=%s telico_ok=%s resp=%s",
             row["name"], queue, body.on, ok, r)
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": ok, "queue": queue, "on": body.on, "telico": r}


@app.post("/api/agents/{agent_id}/all-queues")
async def api_agent_all_queues(agent_id: int, body: QueueBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "agent not found")
    results = []
    for q_name in QUEUES:
        _pending_toggles[(agent_id, q_name)] = time.time()
        _set_queue_flag(agent_id, q_name, body.on)
        conn.commit()
        try:
            r = await asyncio.to_thread(
                telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], body.on)
            ok = bool((r or {}).get("ok"))
        except Exception as e:
            r = {"ok": False, "reason": str(e)}
            ok = False
        results.append({"queue": q_name, "ok": ok, "telico": r})
    fresh = conn.execute("SELECT french, english, delivery FROM agents WHERE id=?", (agent_id,)).fetchone()
    log.info("ALL-QUEUES: agent=%s on=%s → db flags french=%s english=%s delivery=%s",
             row["name"], body.on, fresh["french"], fresh["english"], fresh["delivery"])
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True, "results": results}


@app.post("/api/queues/mass")
async def api_mass_queue(body: MassBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    for q in body.queues:
        if q not in QUEUES: raise HTTPException(400, f"invalid queue {q}")
    rows = conn.execute("SELECT * FROM agents").fetchall()
    for a in rows:
        for q_name in body.queues:
            _pending_toggles[(a["id"], q_name)] = time.time()
            _set_queue_flag(a["id"], q_name, body.on)
            try:
                await asyncio.to_thread(
                    telico_client.set_queue_membership, QUEUE_IDS[q_name], a["phone"], body.on)
            except Exception as e:
                log.error("Mass queue update failed for %s/%s: %s", a["name"], q_name, e)
    conn.commit()
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True}


@app.post("/api/telico/refresh")
async def api_telico_refresh(request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    snap = await asyncio.to_thread(telico_client.fetch_all)
    if not snap.get("ok"):
        return {"ok": False}
    _apply_telico_snapshot(snap)
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True}


@app.get("/api/debug/telico/try-call")
async def api_debug_try_call(request: Request, phone: str = "2193831"):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    r = await asyncio.to_thread(telico_client.call_extension, phone)
    return r


@app.post("/api/debug/telico/simulate-break")
async def api_debug_simulate_break(request: Request, agent_id: int):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")

    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row:
        raise HTTPException(404, "agent not found")

    log.info("SIMULATE BREAK DB STATE: name=%s status=%s french=%s english=%s delivery=%s saved=%s",
             row["name"], row["status"], row["french"], row["english"], row["delivery"],
             row["break_saved_queues"])

    already_on_break = (row["status"] == "Break / Lunch")

    if already_on_break:
        current = [q for q in (row["break_saved_queues"] or "").split(",") if q in QUEUES]
        log.info("SIMULATE BREAK (already on break): %s — keeping saved queues %s, re-ringing phone",
                 row["name"], current or "none")
    else:
        current = [q for q in QUEUES if row[QUEUE_COL[q]]]
        conn.execute("UPDATE agents SET break_saved_queues=?, break_notified=0 WHERE id=?",
                     (",".join(current), agent_id))
        conn.commit()
        apply_status(agent_id, "Break / Lunch")
        for q_name in current:
            _pending_toggles[(agent_id, q_name)] = time.time()
            try:
                await asyncio.to_thread(
                    telico_client.set_queue_membership, QUEUE_IDS[q_name], row["phone"], False)
            except Exception as e:
                log.warning("simulate-break Telico clear failed for %s: %s", q_name, e)
        for q in current:
            _set_queue_flag(agent_id, q, False)
        conn.commit()
        log.info("SIMULATE BREAK: %s — saved queues %s, ringing phone",
                 row["name"], current or "none")

    if row["phone"]:
        asyncio.create_task(_fire_call(row["phone"], row["name"], row["id"]))

    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {
        "ok": True,
        "agent": row["name"],
        "phone": row["phone"],
        "queues_cleared": current,
        "watch_seconds": ANSWER_WATCH_SECONDS,
    }


@app.get("/api/debug/telinta/active-lines")
async def api_debug_active_lines(request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    rows = await asyncio.to_thread(telico_client.fetch_phone_lines)
    return {"rows": rows}


@app.get("/api/debug/agent/{agent_id}")
async def api_debug_agent(agent_id: int, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "not found")
    return {
        "id": row["id"], "name": row["name"], "phone": row["phone"],
        "status": row["status"],
        "french": bool(row["french"]),
        "english": bool(row["english"]),
        "delivery": bool(row["delivery"]),
        "break_saved_queues": row["break_saved_queues"],
    }


# ---------------------------------------------------------------- SMS API

@app.post("/api/sms/send")
async def api_sms_send(
    request: Request,
    message_template: str = Form(...),
    media_mode: str = Form("none"),        # "none" | "upload" | "path"
    media_path: str = Form(""),            # used when media_mode == "path" (URL or server path)
    media_file: UploadFile | None = File(None),
    recip_mode: str = Form("csv"),         # "csv" | "paste"
    recipients_text: str = Form(""),
    recipients_csv: UploadFile | None = File(None),
):
    sess = get_session(request)
    if not sess or sess["role"] != "manager":
        raise HTTPException(403, "manager only")

    if not message_template.strip():
        raise HTTPException(400, "Message template is empty")

    # --- Resolve recipients
    recipients: list[dict] = []
    if recip_mode == "csv":
        if not recipients_csv or not recipients_csv.filename:
            raise HTTPException(400, "No CSV file uploaded")
        raw = await recipients_csv.read()
        recipients = _parse_recipients_csv(raw)
        if not recipients:
            raise HTTPException(400, "CSV parsed but no recipients found (need a 'phone' column)")
    else:
        recipients = _parse_recipients_text(recipients_text)

    if not recipients:
        raise HTTPException(400, "No valid recipients")

    # --- Resolve media (single global media for everyone)
    resolved_media = ""
    if media_mode == "upload" and media_file and media_file.filename:
        safe_name = f"{secrets.token_hex(6)}_{Path(media_file.filename).name}"
        saved_path = SMS_UPLOAD_DIR / safe_name
        with open(saved_path, "wb") as f:
            shutil.copyfileobj(media_file.file, f)
        resolved_media = str(saved_path)
        log.info("SMS BLAST: uploaded media saved -> %s", resolved_media)
    elif media_mode == "path" and media_path.strip():
        resolved_media = media_path.strip()

    log.info("SMS BLAST: %d recipients, media=%s, template=%dB",
             len(recipients), resolved_media or "none", len(message_template))

    results = []
    for r in recipients:
        phone = (r["phone"] or "").strip()
        if not phone:
            continue
        name = (r.get("name") or "").strip() or "Valued Customer"
        message = message_template.replace("{name}", name)

        try:
            result = await asyncio.to_thread(_send_sms_or_mms, phone, message, resolved_media)
        except Exception as e:
            result = {"ok": False, "channel": "", "error": str(e)}

        results.append({
            "phone": phone,
            "name": name,
            "ok": bool(result.get("ok")),
            "channel": result.get("channel", ""),
            "error": result.get("error", ""),
        })
        log.info("SMS -> %s (%s): ok=%s channel=%s err=%s",
                 phone, name, result.get("ok"), result.get("channel"), result.get("error"))

        if SMS_THROTTLE_SECONDS > 0:
            await asyncio.sleep(SMS_THROTTLE_SECONDS)

    sent = sum(1 for x in results if x["ok"])
    log.info("SMS BLAST complete: %d/%d sent", sent, len(results))
    return {"ok": True, "sent": sent, "total": len(results), "results": results,
            "media_used": resolved_media}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    await hub.join(ws)
    try:
        await ws.send_text(json.dumps({"type": "snapshot", "data": snapshot_full()}))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await hub.leave(ws)


def _status_seconds(agent_id):
    out = {}
    for r in conn.execute("SELECT status,started_at,ended_at FROM status_events WHERE agent_id=?", (agent_id,)):
        start = datetime.fromisoformat(r["started_at"])
        end = datetime.fromisoformat(r["ended_at"]) if r["ended_at"] else datetime.now()
        out[r["status"]] = out.get(r["status"], 0) + int((end - start).total_seconds())
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
            "available": s.get("Login / Available", 0),
            "break": s.get("Break / Lunch", 0),
            "admin": s.get("Admin / Wrap Up", 0),
            "outbound": s.get("Outbound", 0),
            "french": q.get("French", 0),
            "english": q.get("English", 0),
            "delivery": q.get("Delivery Line", 0),
            "queueTotal": sum(q.values()),
            "total": sum(s.values()),
        })
    return rows


# ---------------------------------------------------------------- HTML

BASE_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; }
body { background:var(--bg); color:var(--text);
       font-family:'Inter',system-ui,sans-serif; font-size:14px;
       transition:background .15s, color .15s; }
a { color:inherit; text-decoration:none; }
button { font-family:inherit; cursor:pointer; border:none; background:none; color:inherit; }
.mono { font-family:ui-monospace,'JetBrains Mono',monospace; }

:root {
  --bg: #f1f5f9; --panel: #ffffff; --elevated: #f8fafc; --hover: #f1f5f9;
  --border: #e2e8f0; --border-strong: #cbd5e1;
  --text: #0f172a; --text-dim: #475569; --text-muted: #94a3b8;
  --accent: #0891b2; --accent-bg: #cffafe; --accent-border: #67e8f9;
  --success: #16a34a; --success-bg: #dcfce7; --success-border: #86efac;
  --danger: #dc2626; --danger-bg: #fee2e2; --danger-border: #fca5a5;
  --warning: #d97706; --warning-bg: #fef3c7; --warning-border: #fcd34d;
  --outbound: #9333ea; --outbound-bg: #f3e8ff; --outbound-border: #d8b4fe;
  --shadow-sm: 0 1px 2px rgba(15,23,42,0.06);
  --shadow-md: 0 4px 12px rgba(15,23,42,0.08);
  --topbar-bg: #ffffff;
}
[data-theme="dark"] {
  --bg: #0b1120; --panel: #0f172a; --elevated: #1e293b; --hover: #1e293b;
  --border: #1e293b; --border-strong: #334155;
  --text: #f1f5f9; --text-dim: #cbd5e1; --text-muted: #64748b;
  --accent: #22d3ee; --accent-bg: rgba(34,211,238,0.12); --accent-border: rgba(34,211,238,0.4);
  --success: #22c55e; --success-bg: rgba(34,197,94,0.12); --success-border: rgba(34,197,94,0.4);
  --danger: #ef4444; --danger-bg: rgba(239,68,68,0.12); --danger-border: rgba(239,68,68,0.4);
  --warning: #f59e0b; --warning-bg: rgba(245,158,11,0.12); --warning-border: rgba(245,158,11,0.4);
  --outbound: #a855f7; --outbound-bg: rgba(168,85,247,0.12); --outbound-border: rgba(168,85,247,0.4);
  --shadow-sm: 0 1px 2px rgba(0,0,0,0.3); --shadow-md: 0 4px 12px rgba(0,0,0,0.4);
  --topbar-bg: #0f172a;
}

.topbar { display:flex; align-items:center; justify-content:space-between; gap:20px;
          padding:10px 20px; background:var(--topbar-bg);
          border-bottom:1px solid var(--border); flex-shrink:0; }
.topbar-left { display:flex; align-items:center; gap:18px; }
.hamburger { width:36px; height:36px; display:flex; align-items:center; justify-content:center;
             border-radius:8px; color:var(--text-muted); font-size:18px; }
.hamburger:hover { background:var(--hover); color:var(--text); }
.brand { display:flex; align-items:center; gap:10px; }
.brand-logo { width:28px; height:28px; border-radius:8px;
              background:linear-gradient(135deg, #22d3ee, #0891b2);
              display:flex; align-items:center; justify-content:center;
              font-weight:800; font-size:11px; color:#ffffff; letter-spacing:-.03em; }
.brand-title { font-weight:600; font-size:15px; color:var(--text); }
.top-nav { display:flex; gap:2px; }
.top-nav a { padding:8px 14px; font-size:13.5px; font-weight:500; color:var(--text-muted);
             border-radius:8px; transition:all .15s; }
.top-nav a:hover { color:var(--text); }
.top-nav a.active { color:var(--text); background:var(--elevated); }

.topbar-right { display:flex; align-items:center; gap:10px; }
.theme-btn { display:inline-flex; align-items:center; gap:6px; padding:7px 12px;
             background:var(--elevated); border:1px solid var(--border-strong);
             border-radius:10px; font-size:12.5px; color:var(--text-dim); }
.theme-btn:hover { background:var(--hover); }
.user-select { padding:7px 12px; background:var(--elevated); border:1px solid var(--border-strong);
               border-radius:10px; font-size:12.5px; color:var(--text);
               font-family:inherit; cursor:pointer; }
.clock-block { text-align:right; padding-left:10px; }
.clock { font-family:ui-monospace,monospace; font-size:20px; font-weight:600;
         color:var(--text); letter-spacing:.02em; }
.clock-date { color:var(--text-muted); font-size:11px; font-family:ui-monospace,monospace; }

.view { flex:1; overflow:auto; padding:22px 24px 28px; }

.break-banner { display:none; padding:12px 22px;
                background:linear-gradient(90deg, var(--danger-bg) 0%, color-mix(in srgb, var(--danger-bg) 40%, transparent) 100%);
                border-bottom:1px solid var(--danger-border);
                color:var(--danger); font-weight:600; font-size:13.5px;
                align-items:center; gap:12px; }
.break-banner.show { display:flex; }
.break-banner .icon { width:24px; height:24px; border-radius:50%; background:var(--danger);
                      color:white; display:flex; align-items:center; justify-content:center;
                      font-weight:800; font-size:13px; }
.break-banner .names { color:var(--text); font-weight:500; }

.queues { display:grid; grid-template-columns:repeat(3,1fr); gap:18px; }
@media (max-width: 900px) { .queues { grid-template-columns:1fr; } }
.qcard { background:var(--panel); border:2px solid var(--border); border-radius:16px; padding:22px;
         box-shadow:var(--shadow-sm); transition:border-color .3s, box-shadow .3s; }
.qcard.warning { border-color:var(--warning); animation:pulse-warning 2.5s ease-in-out infinite; }
.qcard.critical { border-color:var(--danger); animation:pulse-critical 1s ease-in-out infinite; }
@keyframes pulse-warning { 0%,100% { box-shadow:0 0 0 0 transparent; } 50% { box-shadow:0 0 24px 2px color-mix(in srgb, var(--warning) 35%, transparent); } }
@keyframes pulse-critical { 0%,100% { box-shadow:0 0 0 0 transparent; } 50% { box-shadow:0 0 36px 8px color-mix(in srgb, var(--danger) 45%, transparent); } }
@media (prefers-reduced-motion:reduce) { .qcard.warning,.qcard.critical { animation:none; } }
.qhead { display:flex; align-items:center; justify-content:space-between; margin-bottom:16px; }
.qname { text-transform:uppercase; letter-spacing:.09em; font-size:12.5px; font-weight:700; color:var(--text-muted); }
.badge { font-size:10px; font-weight:700; padding:4px 10px; border-radius:999px;
         text-transform:uppercase; letter-spacing:.06em; }
.badge.normal { background:var(--success-bg); color:var(--success); }
.badge.warning { background:var(--warning-bg); color:var(--warning); }
.badge.critical { background:var(--danger-bg); color:var(--danger); }
.qwaiting { font-family:ui-monospace,monospace; font-size:64px; font-weight:700; line-height:1; color:var(--text); }
.qwaiting.warning { color:var(--warning); } .qwaiting.critical { color:var(--danger); }
.qwaiting-label { font-size:11px; color:var(--text-muted); text-transform:uppercase; letter-spacing:.12em; margin-left:10px; }
.qstats { display:grid; grid-template-columns:1fr 1fr 1fr 1fr; gap:12px; padding-top:16px; margin-top:16px;
          border-top:1px solid var(--border); }
.stat-label { font-size:10.5px; color:var(--text-muted); text-transform:uppercase; letter-spacing:.06em; }
.stat-value { font-family:ui-monospace,monospace; font-weight:600; margin-top:3px; font-size:14px; color:var(--text); }
.stat-value.ok { color:var(--success); }
.stat-value.accent { color:var(--accent); }
.qagents { margin-top:16px; padding-top:14px; border-top:1px solid var(--border); }
.qagents-label { font-size:10.5px; color:var(--text-muted); text-transform:uppercase;
                 letter-spacing:.08em; font-weight:600; margin-bottom:10px;
                 display:flex; justify-content:space-between; }
.qagents-label .count { color:var(--success); font-family:ui-monospace,monospace; letter-spacing:0; }
.qagents-list { display:flex; flex-wrap:wrap; gap:6px; max-height:120px; overflow-y:auto; }
.agent-chip { display:inline-flex; align-items:center; gap:6px; padding:5px 11px;
              border-radius:7px; font-size:12px; font-weight:500;
              background:var(--success-bg); color:var(--success);
              border:1px solid var(--success-border); }
.agent-chip::before { content:''; width:6px; height:6px; border-radius:50%; background:var(--success); }
.qagents-empty { color:var(--text-muted); font-size:12px; font-style:italic; }

.section { background:var(--panel); border:1px solid var(--border); border-radius:14px;
           padding:18px 20px; margin-bottom:18px; box-shadow:var(--shadow-sm); }
.section-title { font-size:13px; font-weight:700; color:var(--text); margin-bottom:14px; }

.mgr-grid { display:grid; grid-template-columns: 1fr; gap:20px; align-items:flex-start; }
.mass-top { display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; }
.mass-btn { padding:10px 16px; border-radius:9px; font-size:12.5px; font-weight:700;
            display:inline-flex; align-items:center; gap:8px; transition:filter .12s; }
.mass-btn:hover { filter:brightness(1.08); }
.mass-btn.green { background:linear-gradient(180deg, #22c55e, #16a34a); color:white; }
.mass-btn.red { background:linear-gradient(180deg, #ef4444, #dc2626); color:white; }
.mass-btn:disabled { opacity:0.5; cursor:not-allowed; filter:none; }

.queue-cards { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; }
@media (max-width: 900px) { .queue-cards { grid-template-columns:1fr; } }
.queue-card { padding:14px 16px; background:var(--elevated); border:1px solid var(--border); border-radius:11px; }
.queue-card .name { font-size:12.5px; font-weight:700; color:var(--text); margin-bottom:10px; }
.queue-card .row { display:flex; gap:8px; }
.q-btn { padding:7px 12px; border-radius:7px; font-size:11.5px; font-weight:600;
         background:var(--panel); border:1px solid var(--border-strong); color:var(--text-dim); }
.q-btn:hover { background:var(--hover); }
.q-btn.login:hover { background:var(--success-bg); color:var(--success); border-color:var(--success-border); }
.q-btn.logout:hover { background:var(--danger-bg); color:var(--danger); border-color:var(--danger-border); }

.agents-head { display:flex; align-items:center; justify-content:space-between; margin-bottom:12px; }
.reset-btn { padding:7px 12px; border-radius:8px; font-size:12px; color:var(--text-dim);
             background:var(--elevated); border:1px solid var(--border-strong);
             display:inline-flex; align-items:center; gap:6px; }
.reset-btn:hover { background:var(--hover); }

table { width:100%; border-collapse:collapse; }
thead th { text-align:left; font-size:11px; color:var(--text-muted); text-transform:none;
           letter-spacing:0; padding:12px 12px; border-bottom:1px solid var(--border);
           font-weight:500; }
tbody td { padding:14px 12px; border-bottom:1px solid var(--border); font-size:13px; color:var(--text); }
tbody tr:last-child td { border-bottom:none; }

.status-pill-select {
  appearance: none; -webkit-appearance: none; -moz-appearance: none;
  padding: 5px 26px 5px 12px; border-radius: 8px; font-size: 12.5px; font-weight: 600;
  border: none; cursor: pointer; font-family: inherit; outline: none;
  background-repeat: no-repeat; background-position: right 8px center; background-size: 10px 6px;
  background-image: url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 6'%3e%3cpath fill='currentColor' d='M0 0l5 6 5-6H0z'/%3e%3c/svg%3e");
}
.status-pill-select.st-available { background-color: var(--success-bg); color: var(--success); }
.status-pill-select.st-break     { background-color: var(--danger-bg); color: var(--danger); }
.status-pill-select.st-admin     { background-color: var(--accent-bg); color: var(--accent); }
.status-pill-select.st-outbound  { background-color: var(--outbound-bg); color: var(--outbound); }
.status-pill-select.st-offline   { background-color: var(--elevated); color: var(--text-muted); }
.status-pill-select:hover { filter: brightness(0.97); }
.status-pill-select option { background: var(--panel); color: var(--text); }

.switch {
  width:44px; height:24px; border-radius:12px; background:var(--border-strong);
  position:relative; cursor:pointer; transition:background .18s;
  display:inline-block; vertical-align:middle; padding:0; border:none; pointer-events:auto;
}
.switch::after {
  content:''; position:absolute; top:2px; left:2px;
  width:20px; height:20px; border-radius:50%; background:#fff;
  transition:left .18s; box-shadow:0 1px 2px rgba(0,0,0,0.25); pointer-events:none;
}
.switch:hover { filter:brightness(0.95); }
.switch.on { background:linear-gradient(135deg, #22d3ee, #0891b2); }
.switch.on::after { left:22px; }

.icon-btn { width:30px; height:30px; border-radius:7px; display:inline-flex;
            align-items:center; justify-content:center; background:var(--elevated);
            border:1px solid var(--border-strong); color:var(--text-dim); font-size:12px; margin-right:4px; }
.icon-btn:hover { background:var(--hover); color:var(--text); }
.icon-btn.in:hover { background:var(--success-bg); color:var(--success); border-color:var(--success-border); }
.icon-btn.out:hover { background:var(--danger-bg); color:var(--danger); border-color:var(--danger-border); }

.tab-nav { display:flex; gap:2px; border-bottom:1px solid var(--border); margin-bottom:20px; }
.tab { padding:10px 18px; font-size:13px; font-weight:500; color:var(--text-muted);
       border-bottom:2px solid transparent; background:none; cursor:pointer;
       transition:color .15s, border-color .15s; }
.tab:hover { color:var(--text); }
.tab.active { color:var(--accent); border-bottom-color:var(--accent); }
.tab-panel { display:none; }
.tab-panel.active { display:block; }

.agent-wrap { max-width: 480px; margin: 0 auto; padding: 16px; }
.agent-hello { text-align:center; padding: 8px 0 20px; }
.agent-hello .name { font-size: 22px; font-weight: 700; color:var(--text); }
.agent-hello .sub { font-size: 12px; color: var(--text-muted); font-family: ui-monospace, monospace; margin-top: 4px; }
.agent-select { width: 100%; padding: 14px; font-size: 16px; border-radius: 12px;
                border: 1px solid var(--border-strong); background: var(--panel); color: var(--text);
                font-family: inherit; }
.status-card { padding: 22px; text-align: center; border-radius: 18px;
               background: var(--panel); border: 2px solid var(--border); margin-bottom: 16px;
               box-shadow: var(--shadow-sm); transition: border-color .2s, background .2s; }
.status-card.on-break { border-color: var(--danger); background: var(--danger-bg);
                        animation: pulse-critical 1.4s ease-in-out infinite; }
.status-card.on-break .big { color: var(--danger); }
.status-card .lbl { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: .12em; font-weight: 600; }
.status-card .big { font-size: 24px; font-weight: 700; margin-top: 6px; color:var(--text); }
.status-card .time { font-family: ui-monospace, monospace; font-size: 34px; font-weight: 700; margin-top: 4px; color:var(--text); }
.agent-btn { width: 100%; padding: 22px 16px; border-radius: 16px; font-size: 18px; font-weight: 700;
             border: 3px solid var(--border); background: var(--panel); color: var(--text-dim);
             display: flex; align-items: center; justify-content: center; text-align: center;
             margin-bottom: 12px; cursor: pointer; transition: transform .1s, background .15s; }
.agent-btn:active { transform: scale(0.97); }
.agent-btn[disabled] { opacity: 0.35; cursor: not-allowed; }
.agent-btn.break-active { background: var(--danger-bg); border-color: var(--danger); color: var(--danger); }
.agent-btn.break-idle:hover { background: var(--warning-bg); border-color: var(--warning); color: var(--warning); }
.agent-btn.login-active { background: var(--success-bg); border-color: var(--success); color: var(--success); }
.agent-btn.login-idle:hover { background: var(--success-bg); border-color: var(--success); color: var(--success); }

/* ---- SMS page ---- */
.sms-grid { display:grid; grid-template-columns: 1fr 1fr; gap:20px; align-items:flex-start; }
@media (max-width: 1000px) { .sms-grid { grid-template-columns:1fr; } }
.sms-field { margin-bottom:16px; }
.sms-field > label.sms-label { display:block; font-size:11.5px; font-weight:600; color:var(--text-muted);
                   text-transform:uppercase; letter-spacing:.06em; margin-bottom:8px; }
.sms-field textarea, .sms-field input[type="text"] {
  width:100%; padding:12px 14px; font-size:13.5px; border-radius:10px;
  border:1px solid var(--border-strong); background:var(--elevated); color:var(--text);
  font-family:inherit; resize:vertical; outline:none;
}
.sms-field textarea:focus, .sms-field input[type="text"]:focus { border-color:var(--accent); }
.sms-field textarea.mono { font-family:ui-monospace,monospace; font-size:12.5px; }
.sms-field .hint { font-size:11px; color:var(--text-muted); margin-top:6px; }

.sms-seg { display:inline-flex; background:var(--elevated); border:1px solid var(--border-strong);
           border-radius:10px; padding:3px; gap:3px; margin-bottom:10px; }
.sms-seg button { padding:7px 14px; font-size:12px; font-weight:600; color:var(--text-dim);
                  border-radius:7px; transition:all .15s; }
.sms-seg button.active { background:var(--panel); color:var(--text); box-shadow:var(--shadow-sm); }

.sms-drop {
  display:flex; flex-direction:column; align-items:center; justify-content:center;
  padding:22px 16px; border:2px dashed var(--border-strong); border-radius:12px;
  background:var(--elevated); color:var(--text-muted); font-size:12.5px;
  cursor:pointer; transition:all .15s; text-align:center;
}
.sms-drop:hover, .sms-drop.dragover { border-color:var(--accent); background:var(--accent-bg); color:var(--accent); }
.sms-drop input[type="file"] { display:none; }
.sms-drop .big-icon { font-size:28px; margin-bottom:6px; }
.sms-drop .filename { font-family:ui-monospace,monospace; font-size:12px;
                      color:var(--text); margin-top:6px; font-weight:600; }

.sms-actions { display:flex; gap:10px; align-items:center; margin-top:16px; }
.sms-results { max-height:520px; overflow-y:auto; }
.sms-row { display:grid; grid-template-columns: 1fr 1fr 90px 1fr; gap:10px;
           padding:9px 12px; border-bottom:1px solid var(--border); font-size:12.5px; }
.sms-row:last-child { border-bottom:none; }
.sms-row .phone { font-family:ui-monospace,monospace; }
.sms-row .ok { color:var(--success); font-weight:600; }
.sms-row .err { color:var(--danger); font-weight:600; font-size:11.5px; word-break:break-word; }
.sms-summary { display:flex; gap:16px; padding:12px 14px; background:var(--elevated);
               border:1px solid var(--border); border-radius:10px; margin-bottom:12px; }
.sms-summary .big { font-size:22px; font-weight:700; font-family:ui-monospace,monospace; }
.sms-summary .lbl { font-size:10.5px; color:var(--text-muted); text-transform:uppercase;
                    letter-spacing:.06em; }
.sms-empty { color:var(--text-muted); font-size:12.5px; font-style:italic; padding:14px; }

.toast { position: fixed; left: 50%; bottom: 24px; transform: translateX(-50%);
         background: var(--panel); border: 1px solid var(--border-strong); padding: 12px 20px;
         border-radius: 10px; font-size: 13px; max-width: 460px; z-index: 999;
         color: var(--text); box-shadow: var(--shadow-md); }
.toast.error { border-color: var(--danger); color: var(--danger); background: var(--danger-bg); }
.toast.ok { border-color: var(--success); color: var(--success); background: var(--success-bg); }

.alert-overlay {
  position: fixed; inset: 0; z-index: 9999;
  background: radial-gradient(circle at center, rgba(239,68,68,0.95), rgba(127,29,29,0.98));
  display: none; align-items: center; justify-content: center;
  padding: 24px; text-align: center;
  animation: overlayPulse 1.2s ease-in-out infinite;
}
.alert-overlay.show { display: flex; }
@keyframes overlayPulse {
  0%,100% { box-shadow: inset 0 0 60px rgba(0,0,0,0.5); }
  50%     { box-shadow: inset 0 0 120px rgba(0,0,0,0.75); }
}
.alert-overlay .inner {
  max-width: 480px; width: 100%;
  background: rgba(15,23,42,0.9);
  border: 4px solid #fecaca; border-radius: 24px;
  padding: 32px 24px; box-shadow: 0 30px 80px rgba(0,0,0,0.7);
}
.alert-overlay .icon { font-size: 64px; line-height: 1; margin-bottom: 12px; }
.alert-overlay .title { font-size: 28px; font-weight: 800; color: #fecaca; margin-bottom: 12px; }
.alert-overlay .msg { font-size: 16px; color: #fee2e2; line-height: 1.6; margin-bottom: 24px; }
.alert-overlay .btn-dismiss {
  background: #fecaca; color: #7f1d1d; padding: 18px 32px; border-radius: 12px;
  font-size: 18px; font-weight: 800; letter-spacing: .03em; width: 100%;
}
.alert-overlay .btn-dismiss[disabled] { opacity: 0.6; cursor: wait; }
.login-wrap { max-width: 400px; margin: 80px auto; padding: 32px;
              background: var(--panel); border: 1px solid var(--border); border-radius: 16px;
              box-shadow: var(--shadow-md); }
.login-wrap h1 { font-size: 22px; margin-bottom: 8px; color: var(--text); }
.login-wrap p { color: var(--text-muted); font-size: 13px; margin-bottom: 24px; }
.login-wrap input { width: 100%; padding: 14px; font-size: 15px; border-radius: 10px;
                    border: 1px solid var(--border-strong); background: var(--elevated);
                    color: var(--text); margin-bottom: 12px; font-family: inherit; }
.login-wrap input:focus { outline: 2px solid var(--accent); outline-offset: 1px; }
.login-wrap button { width: 100%; padding: 14px; border-radius: 10px;
                     background: var(--accent); color: white; font-weight: 700; font-size: 15px; }
.login-error { color: var(--danger); background: var(--danger-bg); padding: 10px 14px;
               border-radius: 8px; font-size: 13px; margin-bottom: 14px; }
"""


def page_shell(title, role_label, role_class, top_right_html, body_html, script,
               show_clock=True, topbar_minimal=False, active_nav="", extra_css=""):
    if topbar_minimal:
        topbar = f"""
        <header class="topbar">
          <div class="topbar-left">
            <div class="brand"><div class="brand-logo">{BRAND_LOGO}</div>
              <div class="brand-title">{BRAND_NAME}</div></div>
          </div>
          <div class="topbar-right">
            <button class="theme-btn" id="theme-btn" onclick="toggleTheme()">🌙</button>
          </div>
        </header>"""
    else:
        def navcls(k): return "active" if k == active_nav else ""
        nav_html = ""
        if role_label.startswith("Manager"):
            nav_html = f"""
              <nav class="top-nav">
                <a href="/wallboard" class="{navcls('wallboard')}">Wallboard</a>
                <a href="/manager" class="{navcls('manager')}">Manager</a>
                <a href="/sms" class="{navcls('sms')}">SMS</a>
                <a href="/reports" class="{navcls('reports')}">Reports</a>
              </nav>"""
        clock_html = ""
        if show_clock:
            clock_html = """
              <div class="clock-block">
                <div class="clock" id="clock">--:--:--</div>
                <div class="clock-date" id="clock-date">—</div>
              </div>"""
        user_html = f"""
              <select class="user-select" onchange="if(this.value)location.href=this.value">
                <option>{role_label.split(' · ')[0]}</option>
                <option value="/logout">Log out</option>
              </select>"""
        topbar = f"""
        <header class="topbar">
          <div class="topbar-left">
            <button class="hamburger">☰</button>
            <div class="brand"><div class="brand-logo">{BRAND_LOGO}</div>
              <div class="brand-title">{BRAND_NAME}</div></div>
            {nav_html}
          </div>
          <div class="topbar-right">
            <button class="theme-btn" id="theme-btn" onclick="toggleTheme()" title="Toggle theme">🌙</button>
            {user_html}
            {clock_html}
          </div>
        </header>"""

    break_banner_html = """
    <div class="break-banner" id="break-banner">
      <span class="icon">!</span>
      <span>On break:</span>
      <span class="names" id="break-names">—</span>
    </div>"""

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="theme-color" content="#ffffff">
<title>{title} · {BRAND_NAME}</title><style>{BASE_CSS}{extra_css}</style>
<script>
  (function() {{
    var t = localStorage.getItem('cc_theme') || 'light';
    document.documentElement.dataset.theme = t;
  }})();
</script>
</head><body>
<div class="app" style="display:flex;flex-direction:column;min-height:100vh">
{topbar}
{break_banner_html}
<main class="view">
{body_html}
</main></div>
<script>
const STATUSES = {json.dumps(STATUSES)}; const QUEUES = {json.dumps(QUEUES)};
const STATUS_CLASS = {{
  'Login / Available':'st-available','Logout / Offline':'st-offline',
  'Admin / Wrap Up':'st-admin','Break / Lunch':'st-break','Outbound':'st-outbound'}};
const state = {{ agents: [], metrics: [], telicoConfigured: false, breakLimitSeconds: 1800, lastSync: null }};
const listeners = [];
function onUpdate(fn){{ listeners.push(fn); }}
function notify(){{ for (const fn of listeners) fn(state); }}
function startClock(){{
  const t = document.getElementById('clock'); const d = document.getElementById('clock-date');
  if (!t) return;
  const tick = () => {{ const n=new Date();
    t.textContent=n.toLocaleTimeString('en-GB');
    d.textContent=n.toLocaleDateString('en-GB',{{weekday:'long',day:'numeric',month:'short',year:'numeric'}}); }};
  tick(); setInterval(tick,1000);
}}
function fmtDuration(from){{
  const s = Math.max(0, Math.floor((Date.now()-new Date(from).getTime())/1000));
  const h=Math.floor(s/3600), m=Math.floor((s%3600)/60), sec=s%60;
  return String(h).padStart(2,'0')+':'+String(m).padStart(2,'0')+':'+String(sec).padStart(2,'0');
}}
function fmtHM(sec){{ const h=Math.floor(sec/3600), m=Math.floor((sec%3600)/60);
  return String(h).padStart(2,'0')+':'+String(m).padStart(2,'0'); }}
function fmtWait(sec){{ const m=Math.floor(sec/60), s=sec%60;
  return String(m).padStart(2,'0')+':'+String(s).padStart(2,'0'); }}
function fmtHHMM(iso){{
  if (!iso) return '--:--';
  const d = new Date(iso);
  return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');
}}
function showToast(msg, kind=''){{
  const el = document.createElement('div'); el.className = 'toast ' + kind;
  el.textContent = msg; document.body.appendChild(el);
  setTimeout(() => el.remove(), 5000);
}}
function updateBreakBanner(){{
  const el = document.getElementById('break-banner');
  const namesEl = document.getElementById('break-names');
  if (!el || !namesEl) return;
  const onBreak = state.agents.filter(a => a.onBreak);
  if (!onBreak.length) {{ el.classList.remove('show'); return; }}
  el.classList.add('show');
  namesEl.textContent = onBreak.map(a => a.name).join(', ');
}}
function applyThemeButton(){{
  const btn = document.getElementById('theme-btn');
  if (!btn) return;
  const cur = document.documentElement.dataset.theme || 'light';
  btn.textContent = cur === 'dark' ? '☀️' : '🌙';
  btn.title = cur === 'dark' ? 'Switch to light' : 'Switch to dark';
}}
window.toggleTheme = () => {{
  const cur = document.documentElement.dataset.theme || 'light';
  const next = cur === 'dark' ? 'light' : 'dark';
  document.documentElement.dataset.theme = next;
  localStorage.setItem('cc_theme', next);
  const meta = document.querySelector('meta[name=theme-color]');
  if (meta) meta.setAttribute('content', next === 'dark' ? '#0b1120' : '#ffffff');
  applyThemeButton();
}};
document.addEventListener('DOMContentLoaded', applyThemeButton);
(function connectWS(){{
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(proto+'://'+location.host+'/ws');
  ws.onclose = () => {{ setTimeout(connectWS, 2000); }};
  ws.onmessage = (e) => {{
    const msg = JSON.parse(e.data);
    if (msg.type === 'snapshot') {{
      state.agents = msg.data.agents; state.metrics = msg.data.metrics;
      state.telicoConfigured = !!msg.data.telicoConfigured;
      state.breakLimitSeconds = msg.data.breakLimitSeconds || 1800;
      state.lastSync = msg.data.lastSync || null;
      updateBreakBanner(); notify();
    }} else if (msg.type === 'break_expired') {{
      if (typeof window.handleBreakExpired === 'function') window.handleBreakExpired(msg);
    }}
  }};
}})();
startClock();
{script}
</script></body></html>"""


# ---------------------------------------------------------------- wallboard

WALLBOARD_CSS = """
.wb-shell { padding: 0; }
.wb-shell .queues {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 18px;
}
@media (max-width: 900px) {
  .wb-shell .queues { grid-template-columns: 1fr; }
}
"""

WALLBOARD_SCRIPT = r"""
function queueInfoFor(name){
  if (!state.lastSync || !state.lastSync.queues) return null;
  return state.lastSync.queues[name] || null;
}
function renderQueues(){
  const wrap = document.getElementById('queues');
  if (!wrap) return;
  wrap.innerHTML = state.metrics.map(m => {
    const info = queueInfoFor(m.queue);
    const avail = info ? info.available_count : 0;
    const total = info ? info.members : 0;
    const names = info ? (info.available_names || []) : [];
    const agentsHtml = names.length === 0
      ? '<div class="qagents-empty">Nobody logged in</div>'
      : '<div class="qagents-list">' + names.map(n => `<span class="agent-chip">${n}</span>`).join('') + '</div>';
    return `
    <div class="qcard ${m.waiting>0 ? m.level : ''}">
      <div class="qhead"><div class="qname">${m.queue}</div>
        <span class="badge ${m.waiting>0 ? m.level : 'normal'}">${m.waiting>0 ? m.level : 'idle'}</span></div>
      <div style="display:flex; align-items:baseline">
        <div class="qwaiting ${m.waiting>0 ? m.level : ''}">${m.waiting}</div>
        <div class="qwaiting-label">waiting</div></div>
      <div class="qstats">
        <div><div class="stat-label">Longest</div>
             <div class="stat-value">${m.waiting ? fmtWait(m.longestWaitSeconds) : '00:00'}</div></div>
        <div><div class="stat-label">SLA</div>
             <div class="stat-value">${m.waiting ? m.sla.toFixed(0)+'%' : '100%'}</div></div>
        <div><div class="stat-label">Agents IN</div>
             <div class="stat-value ok">${avail} / ${total}</div></div>
        <div><div class="stat-label">Answered today</div>
             <div class="stat-value accent">${m.callsAnsweredToday || 0}</div></div></div>
      <div class="qagents"><div class="qagents-label">
        <span>Logged in now</span>
        <span class="count">${avail} agent${avail===1?'':'s'}</span></div>
        ${agentsHtml}</div></div>`;
  }).join('');
}
onUpdate(renderQueues);
setInterval(renderQueues, 2000);
"""

WALLBOARD_BODY = """
<div class="wb-shell">
  <div class="queues" id="queues"></div>
</div>
"""

@app.get("/wallboard", response_class=HTMLResponse)
def page_wallboard(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] not in ("manager", "wallboard"):
        return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    role_label = "Manager" if sess["role"] == "manager" else "Wallboard"
    return page_shell("Wallboard", role_label, "", "", WALLBOARD_BODY,
                      WALLBOARD_SCRIPT, active_nav="wallboard", extra_css=WALLBOARD_CSS)


# ---------------------------------------------------------------- manager

MANAGER_SCRIPT = r"""
async function api(path, body, method='POST'){
  const r = await fetch(path, { method, headers: {'Content-Type':'application/json'},
    body: body?JSON.stringify(body):undefined });
  return r.json().catch(()=>null);
}
window.changeStatus = async (id, s) => {
  await api(`/api/agents/${id}/status`, {status: s});
};
window.toggleQueue = async (ev, id, q, on) => {
  if (ev) { ev.preventDefault(); ev.stopPropagation(); }
  const agent = state.agents.find(a => a.id === id);
  if (agent) { agent.queues[q] = on; renderAgents(); }
  const r = await api(`/api/agents/${id}/queues/${encodeURIComponent(q)}`, {on});
  if (!r || !r.ok) {
    if (agent) agent.queues[q] = !on;
    renderAgents();
  }
};
window.toggleAllQueues = async (id, on) =>
  await api(`/api/agents/${id}/all-queues`, {on});
window.massQueue = async (queues, on) => {
  await api('/api/queues/mass', {queues, on});
  showToast((on ? 'Login' : 'Logout') + ' sent for ' + queues.join(', '), 'ok');
};
window.tryCall = async () => {
  const phone = prompt('Extension to ring (e.g. 2193838):');
  if (!phone) return;
  const r = await fetch('/api/debug/telico/try-call?phone=' + encodeURIComponent(phone),
                        {credentials: 'include'}).then(x => x.json()).catch(() => null);
  if (r && r.ok) showToast('Ringing ' + phone + '…', 'ok');
  else showToast('Call failed — check server log', 'error');
};
window.simulateBreak = async (agentId) => {
  const r = await fetch('/api/debug/telico/simulate-break?agent_id=' + agentId,
                        {method: 'POST', credentials: 'include'})
            .then(x => x.json()).catch(() => null);
  if (r && r.ok) showToast('Simulating break for ' + r.agent + ' — watching for answer', 'ok');
  else showToast('Failed — check server log', 'error');
};

function queueInfoFor(name){
  if (!state.lastSync || !state.lastSync.queues) return null;
  return state.lastSync.queues[name] || null;
}
function renderQueues(){
  const wrap = document.getElementById('mgr-queues');
  if (!wrap) return;
  wrap.innerHTML = state.metrics.map(m=>{
    const info = queueInfoFor(m.queue);
    const avail = info ? info.available_count : 0;
    const total = info ? info.members : 0;
    const names = info ? (info.available_names || []) : [];
    const agentsHtml = names.length === 0
      ? '<span class="qagents-empty">Nobody logged in</span>'
      : names.map(n => `<span class="agent-chip">${n}</span>`).join('');
    return `
    <div class="qcard ${m.waiting>0 ? m.level : ''}">
      <div class="qhead"><div class="qname">${m.queue}</div>
        <span class="badge ${m.waiting>0 ? m.level : 'normal'}">${m.waiting>0 ? m.level : 'idle'}</span></div>
      <div style="display:flex; align-items:baseline">
        <div class="qwaiting ${m.waiting>0 ? m.level : ''}">${m.waiting}</div>
        <div class="qwaiting-label">waiting</div></div>
      <div class="qstats">
        <div><div class="stat-label">Longest</div>
             <div class="stat-value">${m.waiting ? fmtWait(m.longestWaitSeconds) : '00:00'}</div></div>
        <div><div class="stat-label">SLA</div>
             <div class="stat-value">${m.waiting ? m.sla.toFixed(0)+'%' : '100%'}</div></div>
        <div><div class="stat-label">Agents IN</div>
             <div class="stat-value ok">${avail} / ${total}</div></div>
        <div><div class="stat-label">Answered today</div>
             <div class="stat-value accent">${m.callsAnsweredToday || 0}</div></div></div>
      <div class="qagents"><div class="qagents-label">
        <span>Logged in now</span>
        <span class="count">${avail} agent${avail===1?'':'s'}</span></div>
        <div class="qagents-list">${agentsHtml}</div></div></div>`;
  }).join('');
}

function renderAgents(){
  const tb = document.getElementById('mgr-tbody');
  if (!tb) return;
  const focused = document.activeElement;
  if (focused && focused.tagName === 'SELECT' && tb.contains(focused)) return;

  tb.innerHTML = state.agents.map(a=>{
    const statusCls = STATUS_CLASS[a.status] || 'st-offline';
    const restoreHint = a.onBreak
      ? `<div style="font-size:11px;color:var(--text-muted);font-weight:400;margin-top:2px">
           restoring: ${(a.breakSavedQueues && a.breakSavedQueues.length)
                          ? a.breakSavedQueues.join(', ') : 'none'}</div>`
      : '';
    return `
    <tr>
      <td style="font-weight:600">${a.name}${a.onBreak ? ' <span style="color:var(--danger);font-size:11px">●</span>' : ''}
        ${restoreHint}
      </td>
      <td>
        <select class="status-pill-select ${statusCls}"
                onchange="changeStatus(${a.id}, this.value)">
          ${STATUSES.map(s=>`<option ${s===a.status?'selected':''}>${s}</option>`).join('')}
        </select>
      </td>
      <td class="mono" style="color:var(--text-muted);font-size:12.5px">${fmtHHMM(a.statusSince)}</td>
      <td class="mono" style="font-size:12.5px">${fmtDuration(a.statusSince)}</td>
      ${['French','English','Delivery Line'].map(q=>`
        <td><button type="button" class="switch ${a.queues[q]?'on':''}"
             onclick="toggleQueue(event, ${a.id}, '${q}', ${!a.queues[q]})"></button></td>`).join('')}
      <td>
        <button class="icon-btn in" title="Login all queues" onclick="toggleAllQueues(${a.id}, true)">➜]</button>
        <button class="icon-btn out" title="Logout all queues" onclick="toggleAllQueues(${a.id}, false)">[➜</button>
        <button class="icon-btn" title="Simulate full break → ring phone" onclick="simulateBreak(${a.id})">🧪</button>
      </td>
    </tr>`;
  }).join('');
}

onUpdate(()=>{ renderQueues(); renderAgents(); });
setInterval(renderAgents, 1000);
setInterval(renderQueues, 2000);
"""

MANAGER_BODY = """
<div class="mgr-grid">
  <div>
    <div class="section">
      <div class="section-title">Queues</div>
      <div class="queues" id="mgr-queues"></div>
    </div>

    <div class="section">
      <div class="section-title">Mass actions</div>
      <div class="mass-top">
        <button class="mass-btn green" onclick="massQueue(['French','English','Delivery Line'], true)">
          ⇥ Login All Agents to All Queues
        </button>
        <button class="mass-btn red" onclick="massQueue(['French','English','Delivery Line'], false)">
          ⇤ Logout All Agents from All Queues
        </button>
      </div>
      <div class="queue-cards">
        <div class="queue-card">
          <div class="name">French</div>
          <div class="row">
            <button class="q-btn login" onclick="massQueue(['French'], true)">Login All</button>
            <button class="q-btn logout" onclick="massQueue(['French'], false)">Logout All</button>
          </div>
        </div>
        <div class="queue-card">
          <div class="name">English</div>
          <div class="row">
            <button class="q-btn login" onclick="massQueue(['English'], true)">Login All</button>
            <button class="q-btn logout" onclick="massQueue(['English'], false)">Logout All</button>
          </div>
        </div>
        <div class="queue-card">
          <div class="name">Delivery Line</div>
          <div class="row">
            <button class="q-btn login" onclick="massQueue(['Delivery Line'], true)">Login All</button>
            <button class="q-btn logout" onclick="massQueue(['Delivery Line'], false)">Logout All</button>
          </div>
        </div>
      </div>
    </div>

    <div class="section">
      <div class="agents-head">
        <div class="section-title" style="margin-bottom:0">Agents</div>
        <div>
          <button class="reset-btn" onclick="tryCall()" title="Ring an extension without changing state">📞 Test call</button>
        </div>
      </div>
      <div style="overflow-x:auto">
        <table>
          <thead><tr>
            <th style="min-width:200px">Agent</th>
            <th style="min-width:180px">Status</th>
            <th style="min-width:100px">Status Since</th>
            <th style="min-width:110px">Duration</th>
            <th style="min-width:80px">French</th>
            <th style="min-width:80px">English</th>
            <th style="min-width:80px">Delivery</th>
            <th style="min-width:140px">All queues</th>
          </tr></thead>
          <tbody id="mgr-tbody"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>
"""

@app.get("/manager", response_class=HTMLResponse)
def page_manager(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] != "manager":
        return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    return page_shell("Manager", "Manager", "", "", MANAGER_BODY, MANAGER_SCRIPT,
                      active_nav="manager")


# ---------------------------------------------------------------- SMS page

DEFAULT_SMS_TEMPLATE = (
    "Merry Christmas and Happy Holidays {name}! "
    "We are beyond excited to present to you our brand new, highly anticipated Holiday & Christmas Catalog! "
    "This season, we have curated an extraordinary collection of exclusive gifts, festive decor, seasonal treats, "
    "and limited-edition bundles designed to make your celebrations truly magical and unforgettable. "
    "Whether you are looking for the perfect surprise for your loved ones, cozy winter upgrades for your home, "
    "or special corporate holiday packages, our catalog has everything you need to spread joy and warmth. "
    "Take a moment to browse through the attached digital catalog to discover incredible seasonal discounts, "
    "early-bird specials, and VIP perks available only to our valued community members. "
    "If you have any questions about availability, shipping deadlines, or custom gift wrapping, "
    "please feel free to reply directly to this message or call our holiday hotline anytime. "
    "Thank you for being such an important part of our family this year. "
    "Wishing you and your family a wonderful holiday season filled with peace, love, and joy!"
)

SMS_SCRIPT = r"""
// ---------------------------------------------------------------- state
window.smsMediaMode = 'none';   // "none" | "upload" | "path"
window.smsRecipMode = 'csv';    // "csv"  | "paste"
window.smsMediaFile = null;
window.smsCsvFile = null;

// ---------------------------------------------------------------- segmented buttons
window.setMediaMode = (m) => {
  window.smsMediaMode = m;
  document.querySelectorAll('#sms-media-seg button').forEach(b =>
    b.classList.toggle('active', b.dataset.mode === m));
  document.getElementById('sms-media-none-panel').style.display = (m === 'none')   ? 'block' : 'none';
  document.getElementById('sms-media-upload-panel').style.display = (m === 'upload') ? 'block' : 'none';
  document.getElementById('sms-media-path-panel').style.display = (m === 'path')   ? 'block' : 'none';
};

window.setRecipMode = (m) => {
  window.smsRecipMode = m;
  document.querySelectorAll('#sms-recip-seg button').forEach(b =>
    b.classList.toggle('active', b.dataset.mode === m));
  document.getElementById('sms-recip-csv-panel').style.display = (m === 'csv')   ? 'block' : 'none';
  document.getElementById('sms-recip-paste-panel').style.display = (m === 'paste') ? 'block' : 'none';
};

// ---------------------------------------------------------------- file pickers
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

// ---------------------------------------------------------------- send
window.sendSmsBlast = async () => {
  const btn = document.getElementById('sms-send-btn');
  const template = document.getElementById('sms-template').value;

  if (!template.trim()) { showToast('Message template is empty', 'error'); return; }

  const fd = new FormData();
  fd.append('message_template', template);
  fd.append('media_mode', window.smsMediaMode);
  fd.append('recip_mode', window.smsRecipMode);

  // Media
  if (window.smsMediaMode === 'upload') {
    if (!window.smsMediaFile) { showToast('Please choose a media file', 'error'); return; }
    fd.append('media_file', window.smsMediaFile, window.smsMediaFile.name);
  } else if (window.smsMediaMode === 'path') {
    const p = document.getElementById('sms-media-path').value.trim();
    if (!p) { showToast('Please enter a media URL or server path', 'error'); return; }
    fd.append('media_path', p);
  }

  // Recipients
  if (window.smsRecipMode === 'csv') {
    if (!window.smsCsvFile) { showToast('Please choose a CSV file', 'error'); return; }
    fd.append('recipients_csv', window.smsCsvFile, window.smsCsvFile.name);
  } else {
    fd.append('recipients_text', document.getElementById('sms-recipients-text').value);
  }

  if (!confirm('Send this blast now?')) return;

  btn.disabled = true;
  btn.textContent = 'Sending…';

  try {
    const r = await fetch('/api/sms/send', {
      method: 'POST', credentials: 'include', body: fd,
    });
    if (!r.ok) {
      const err = await r.json().catch(()=>({detail:r.statusText}));
      showToast('Send failed: ' + (err.detail || r.statusText), 'error');
      return;
    }
    const data = await r.json();
    renderSmsResults(data);
    showToast(`Sent ${data.sent} / ${data.total}`, data.sent === data.total ? 'ok' : 'error');
  } catch (e) {
    showToast('Send failed: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Send blast';
  }
};

// ---------------------------------------------------------------- reset
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
  document.getElementById('sms-results').innerHTML = '<div class="sms-empty">No results yet.</div>';
  document.getElementById('sms-summary').style.display = 'none';
  setMediaMode('none');
  setRecipMode('csv');
};

// ---------------------------------------------------------------- results
function renderSmsResults(data) {
  const wrap = document.getElementById('sms-results');
  const summary = document.getElementById('sms-summary');
  summary.style.display = 'flex';
  summary.innerHTML = `
    <div><div class="lbl">Sent</div><div class="big" style="color:var(--success)">${data.sent}</div></div>
    <div><div class="lbl">Failed</div><div class="big" style="color:var(--danger)">${data.total - data.sent}</div></div>
    <div><div class="lbl">Total</div><div class="big">${data.total}</div></div>
  `;
  wrap.innerHTML = data.results.map(r => `
    <div class="sms-row">
      <div class="phone">${escapeHtml(r.phone)}</div>
      <div>${escapeHtml(r.name) || '—'}</div>
      <div>${escapeHtml(r.channel) || '—'}</div>
      <div class="${r.ok ? 'ok' : 'err'}">${r.ok ? '✓ sent' : '✗ ' + escapeHtml(r.error || 'failed')}</div>
    </div>
  `).join('') || '<div class="sms-empty">No recipients processed.</div>';
}

// ---------------------------------------------------------------- init
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
      <textarea id="sms-template" rows="9" class="mono">{DEFAULT_SMS_TEMPLATE}</textarea>
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

    <div class="section-title" style="margin-top:22px">3 · Recipients</div>
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
        <textarea id="sms-recipients-text" rows="8" class="mono"
          placeholder="Alice Smith,6473959350&#10;Bob Jones,5145550102&#10;4378305930"></textarea>
        <div class="hint">One per line: <code>name,phone</code> or <code>phone,name</code> or just <code>phone</code>.</div>
      </div>
    </div>

    <div class="sms-actions">
      <button id="sms-send-btn" class="mass-btn green" onclick="sendSmsBlast()">Send blast</button>
      <button class="reset-btn" onclick="clearSmsForm()">Clear</button>
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
    if sess["role"] != "manager":
        return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    return page_shell("SMS", "Manager", "", "", SMS_BODY, SMS_SCRIPT, active_nav="sms")


# ---------------------------------------------------------------- agent

AGENT_SCRIPT_TEMPLATE = r"""
const SESSION_AGENT_ID = {agent_id};
let myAgentId = SESSION_AGENT_ID || parseInt(localStorage.getItem('cc_agent_id') || '0', 10) || null;
let audioCtx = null;
let pendingStatus = null;
let overlayShownFor = null;

async function api(path, body, method='POST'){
  const r = await fetch(path, { method, headers: {'Content-Type':'application/json'},
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
  try { await api(`/api/agents/${myAgentId}/acknowledge_break`, {}); }
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
  sc.querySelector('.time').textContent = fmtDuration(a.statusSince);
  const onBreak = (a.status === 'Break / Lunch');
  sc.classList.toggle('on-break', onBreak);
  const breakBtn = document.getElementById('btn-break');
  const loginBtn = document.getElementById('btn-login');
  breakBtn.disabled = onBreak || (pendingStatus !== null);
  breakBtn.className = 'agent-btn ' + (onBreak ? 'break-active' : 'break-idle');
  loginBtn.disabled = !onBreak || (pendingStatus !== null);
  loginBtn.className = 'agent-btn ' + (onBreak ? 'login-active' : 'login-idle');
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
onUpdate(render);
setInterval(render, 1000);
render();
"""

AGENT_BODY = """
<div class="agent-wrap">
  <select class="agent-select" id="agent-pick"></select>
  <div id="agent-hello" class="agent-hello" style="margin-top:20px"></div>
  <div id="agent-main" style="display:none">
    <div class="status-card" id="status-card">
      <div class="lbl">Current status</div>
      <div class="big">—</div>
      <div class="time mono">00:00:00</div>
    </div>
    <button type="button" id="btn-break" class="agent-btn break-idle" onclick="startMyBreak()">
      ☕ Start Break
    </button>
    <button type="button" id="btn-login" class="agent-btn login-idle" onclick="logBackIn()">
      ✓ Log back in
    </button>
  </div>
</div>
"""

@app.get("/agent", response_class=HTMLResponse)
def page_agent(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] not in ("manager", "agent"):
        return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    agent_id = sess.get("agent_id") if sess["role"] == "agent" else None
    script = AGENT_SCRIPT_TEMPLATE.replace("{agent_id}", str(agent_id) if agent_id else "null")
    role_label = "Agent" if sess["role"] == "agent" else "Manager"
    return page_shell("Agent", role_label, "", "", AGENT_BODY, script, active_nav="agent")


# ---------------------------------------------------------------- reports

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
  const rows = await fetch('/api/reports/shift-summary').then(r=>r.json()).catch(()=>[]);
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
  const rows = await fetch('/api/reports/shift-summary').then(r=>r.json()).catch(()=>[]);
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
    if sess["role"] != "manager":
        return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    return page_shell("Reports", "Manager", "", "", REPORTS_BODY, REPORTS_SCRIPT,
                      active_nav="reports")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)