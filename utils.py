import asyncio
import json
import secrets
import time
import os
from datetime import datetime, timedelta
from fastapi import Request, WebSocket
from pydantic import BaseModel
import telico_client
from database import connect, now_local, now_iso, today_str, TZ

QUEUES = ["French", "English", "Delivery Line"]
QUEUE_COL = {"French": "french", "English": "english", "Delivery Line": "delivery"}
STATUSES = ["Login / Available", "Logout / Offline", "Admin / Wrap Up", "Break / Lunch", "Outbound"]

BREAK_LIMIT_SECONDS = 30 * 60
ANSWER_WATCH_SECONDS = int(os.getenv("ANSWER_WATCH_SECONDS", "20"))
ANSWER_POLL_INTERVAL = float(os.getenv("ANSWER_POLL_INTERVAL", "0.25"))
ANSWER_FALLBACK_ACK = os.getenv("ANSWER_FALLBACK_ACK", "0") == "1"
CALL_ON_BREAK_EXPIRY = os.getenv("CALL_ON_BREAK_EXPIRY", "1") == "1"

_sessions: dict[str, dict] = {}
SESSION_TTL = 12 * 3600
_pending_toggles: dict[tuple[int, str], float] = {}
_last_sync: dict = {"at": None, "ok": False, "queues": {}, "phones": [], "recent_calls": []}

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
class ScheduleBody(BaseModel): data: dict

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

def drop_session(request: Request):
    tok = request.cookies.get("cc_session")
    if tok: _sessions.pop(tok, None)

def home_for_role(role):
    if role == "manager": return "/manager"
    if role == "wallboard": return "/wallboard"
    if role == "agent": return "/agent"
    return "/login"

def _set_queue_flag(agent_id: int, queue: str, on: bool):
    c = connect()
    col = QUEUE_COL[queue]
    row = c.execute(f"SELECT {col} FROM agents WHERE id=?", (agent_id,)).fetchone()
    old = bool(row[col]) if row else False
    new = bool(on)
    if old == new: return
    c.execute(f"UPDATE agents SET {col}=? WHERE id=?", (int(new), agent_id))
    c.execute("INSERT INTO queue_events (agent_id, queue, joined, at) VALUES (?,?,?,?)", (agent_id, queue, int(new), now_iso()))
    c.commit()

def _is_pending(agent_id: int, queue: str) -> bool:
    key = (agent_id, queue)
    ts = _pending_toggles.get(key)
    if ts is None: return False
    if time.time() - ts > 30:
        _pending_toggles.pop(key, None); return False
    return True

def _queue_seconds(agent_id: int) -> dict:
    c = connect()
    out = {q: 0 for q in QUEUES}
    open_ts: dict[str, datetime] = {}
    for r in c.execute("SELECT queue, joined, at FROM queue_events WHERE agent_id=? ORDER BY id", (agent_id,)):
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

def _calls_answered_today(queue: str) -> dict:
    c = connect()
    row = c.execute("SELECT * FROM calls_answered WHERE date=? AND queue=?", (today_str(), queue)).fetchone()
    if not row: return {"count": 0, "missed": 0, "attempts": 0, "outbound_answered": 0, "outbound_attempts": 0, "talk_secs": 0, "hold_secs": 0, "outbound_talk_secs": 0}
    return dict(row)

def _status_seconds(agent_id):
    c = connect()
    start_today = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    end_today = start_today + timedelta(days=1)
    out = {}
    for r in c.execute("SELECT status,started_at,ended_at FROM status_events WHERE agent_id=?", (agent_id,)):
        start = datetime.fromisoformat(r["started_at"])
        end = datetime.fromisoformat(r["ended_at"]) if r["ended_at"] else now_local()
        start = max(start, start_today)
        end = min(end, end_today)
        if start >= end: continue
        dur = int((end - start).total_seconds())
        out[r["status"]] = out.get(r["status"], 0) + dur
    return out

def _get_login_logout_times(agent_id: int):
    c = connect()
    start_today = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    end_today = start_today + timedelta(days=1)
    
    status_row = c.execute("SELECT status, shift_start FROM agents WHERE id=?", (agent_id,)).fetchone()
    current_status = status_row["status"] if status_row else "Logout / Offline"
    shift_start = status_row["shift_start"] if status_row else None
    
    login_time = "—"
    if shift_start:
        dt = datetime.fromisoformat(shift_start)
        if dt >= start_today:
            login_time = dt.strftime("%I:%M %p").lstrip("0")

    logout_time = "—"
    if current_status == "Logout / Offline":
        logout_row = c.execute(
            "SELECT started_at FROM status_events WHERE agent_id=? AND status='Logout / Offline' AND started_at >= ? AND started_at < ? ORDER BY started_at DESC LIMIT 1",
            (agent_id, start_today.isoformat(), end_today.isoformat())
        ).fetchone()
        if logout_row:
            dt = datetime.fromisoformat(logout_row["started_at"])
            logout_time = dt.strftime("%I:%M %p").lstrip("0")
    
    return login_time, logout_time

def agent_dict(row):
    try: phone = row["phone"] or ""
    except: phone = ""
    try: saved = row["break_saved_queues"] or ""
    except: saved = ""
    try: notified = int(row["break_notified"] or 0)
    except: notified = 0

    status = row["status"]
    shift_start = row["shift_start"]
    
    # FIX: Calculate shift duration up to the moment they log out or go on break
    if not shift_start:
        shift_duration = 0
    else:
        start = datetime.fromisoformat(shift_start)
        if status == "Logout / Offline" or status == "Break / Lunch":
            end_time = datetime.fromisoformat(row["status_since"])
        else:
            end_time = now_local()
            
        today_midnight = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
        if start < today_midnight: start = today_midnight
        if start < end_time: shift_duration = int((end_time - start).total_seconds())
        else: shift_duration = 0

    total_break_seconds = _status_seconds(row["id"]).get("Break / Lunch", 0)
    login_time, logout_time = _get_login_logout_times(row["id"])

    return {
        "id": row["id"], "name": row["name"], "phone": phone,
        "status": status, 
        "statusSince": row["status_since"],
        "shiftStart": shift_start,
        "shiftDurationSeconds": shift_duration, # <--- ADDED THIS LINE BACK
        "totalBreakSeconds": total_break_seconds,
        "onBreak": status == "Break / Lunch",
        "breakNotified": bool(notified),
        "breakSavedQueues": [q for q in saved.split(",") if q in QUEUES],
        "loginTime": login_time,
        "logoutTime": logout_time,
        "queues": {
            "French": bool(row["french"]),
            "English": bool(row["english"]),
            "Delivery Line": bool(row["delivery"]),
        },
    }

def metric_dict(row):
    stats = _calls_answered_today(row["queue"])
    stats["queue"] = row["queue"]
    stats["waiting"] = row["waiting"]
    stats["longestWaitSeconds"] = row["longest_wait_seconds"]
    stats["queue_id"] = telico_client.QUEUE_IDS.get(row["queue"], 0)
    return stats

def snapshot_full():
    c = connect()
    sched_row = c.execute("SELECT data FROM schedule WHERE id=1").fetchone()
    sched_data = json.loads(sched_row["data"]) if sched_row else {}
    
    ls_rows = c.execute("SELECT * FROM line_stats WHERE date=?", (today_str(),)).fetchall()
    line_stats = {r["line"]: {"attempts": r["attempts"], "answered": r["answered"], "talk": r["talk_secs"]} for r in ls_rows}
    
    return {
        "agents": [agent_dict(r) for r in c.execute("SELECT * FROM agents ORDER BY name")],
        "metrics": [metric_dict(r) for r in c.execute("SELECT * FROM queue_metrics")],
        "telicoConfigured": telico_client.is_configured(),
        "breakLimitSeconds": BREAK_LIMIT_SECONDS,
        "lastSync": _last_sync,
        "schedule": sched_data,
        "lineStats": line_stats,
        "recentCalls": _last_sync.get("recent_calls", [])
    }

def apply_status(agent_id, new_status):
    c = connect()
    row = c.execute("SELECT status, shift_start FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: return
    old_status = row["status"]
    ts = now_iso()
    c.execute("UPDATE status_events SET ended_at=? WHERE agent_id=? AND ended_at IS NULL", (ts, agent_id))
    c.execute("INSERT INTO status_events (agent_id, status, started_at) VALUES (?,?,?)", (agent_id, new_status, ts))
    
    if old_status == "Logout / Offline" and new_status != "Logout / Offline":
        # Update shift_start so the new login time overrides the old one
        c.execute("UPDATE agents SET status=?, status_since=?, shift_start=? WHERE id=?", (new_status, ts, ts, agent_id))
    else:
        c.execute("UPDATE agents SET status=?, status_since=? WHERE id=?", (new_status, ts, agent_id))
    c.commit()

async def _update_agent_status(agent_id: int, new: str, actor: str = "System"):
    c = connect()
    row = c.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: return {"ok": False, "reason": "agent not found"}
    
    prev = row["status"]
    if prev == new: return {"ok": True}

    if new == "Break / Lunch" and prev != "Break / Lunch":
        current = [q for q in QUEUES if row[QUEUE_COL[q]]]
        c.execute("UPDATE agents SET break_saved_queues=?, break_notified=0 WHERE id=?", (",".join(current), agent_id))
        c.commit()
        apply_status(agent_id, "Break / Lunch")
        for q_name in current:
            _pending_toggles[(agent_id, q_name)] = time.time()
            _set_queue_flag(agent_id, q_name, False)
            asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, telico_client.QUEUE_IDS[q_name], row["phone"], False))
        c.commit()
        return {"ok": True}

    if prev == "Break / Lunch" and new != "Break / Lunch":
        break_start = datetime.fromisoformat(row["status_since"])
        now = now_local()
        break_duration = now - break_start
        old_shift_start = datetime.fromisoformat(row["shift_start"])
        new_shift_start = old_shift_start + break_duration
        c.execute("UPDATE agents SET shift_start=? WHERE id=?", (new_shift_start.isoformat(), agent_id))
        c.commit()

        try: saved_str = row["break_saved_queues"] or ""
        except: saved_str = ""
        
        c.execute("UPDATE agents SET break_saved_queues='', break_notified=0 WHERE id=?", (agent_id,))
        c.commit()

        if new == "Logout / Offline":
            apply_status(agent_id, "Logout / Offline")
            for q_name in QUEUES:
                if row[QUEUE_COL[q_name]]:
                    _pending_toggles[(agent_id, q_name)] = time.time()
                    _set_queue_flag(agent_id, q_name, False)
                    asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, telico_client.QUEUE_IDS[q_name], row["phone"], False))
            c.commit()
        else:
            to_restore = [q for q in saved_str.split(",") if q in QUEUES]
            for q_name in to_restore:
                _pending_toggles[(agent_id, q_name)] = time.time()
                _set_queue_flag(agent_id, q_name, True)
                asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, telico_client.QUEUE_IDS[q_name], row["phone"], True))
            apply_status(agent_id, new)
        return {"ok": True}

    if new == "Logout / Offline" and prev != "Logout / Offline":
        for q_name in QUEUES:
            if row[QUEUE_COL[q_name]]:
                _pending_toggles[(agent_id, q_name)] = time.time()
                _set_queue_flag(agent_id, q_name, False)
                asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, telico_client.QUEUE_IDS[q_name], row["phone"], False))
        c.commit()

    if new == "Login / Available" and prev == "Logout / Offline":
        for q_name in QUEUES:
            if not row[QUEUE_COL[q_name]]:
                _pending_toggles[(agent_id, q_name)] = time.time()
                _set_queue_flag(agent_id, q_name, True)
                asyncio.create_task(asyncio.to_thread(telico_client.set_queue_membership, telico_client.QUEUE_IDS[q_name], row["phone"], True))
        c.commit()

    apply_status(agent_id, new)
    return {"ok": True}

async def _fire_call(phone: str, name: str, agent_id: int | None = None):
    try:
        r = await asyncio.to_thread(telico_client.call_extension, phone)
    except Exception as e: pass

    if agent_id is None: return
    asyncio.create_task(_watch_for_answer(agent_id, name, phone))

async def _watch_for_answer(agent_id: int, agent_name: str, phone: str):
    deadline = time.time() + ANSWER_WATCH_SECONDS
    while time.time() < deadline:
        await asyncio.sleep(ANSWER_POLL_INTERVAL)
        c = connect()
        row = c.execute("SELECT status FROM agents WHERE id=?", (agent_id,)).fetchone()
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

def _apply_telico_snapshot(snap):
    global _last_sync
    if not snap.get("ok"):
        _last_sync = {"at": now_iso(), "ok": False, "queues": {}, "phones": [], "recent_calls": []}
        return
    c = connect()
    now = now_local()
    
    for p in snap.get("phones", []):
        row = c.execute("SELECT * FROM agents WHERE phone=?", (p["extension"],)).fetchone()
        if row is None:
            c.execute("INSERT INTO agents (name, phone, status, status_since, shift_start, french, english, delivery) VALUES (?,?,?,?,?,0,0,0)", (p["name"], p["extension"], "Logout / Offline", now.isoformat(), (now - timedelta(hours=1)).isoformat()))
            c.execute("INSERT INTO status_events (agent_id, status, started_at) SELECT id, ?, ? FROM agents WHERE phone=?", ("Logout / Offline", now.isoformat(), p["extension"]))
        elif row["name"] != p["name"]:
            c.execute("UPDATE agents SET name=? WHERE phone=?", (p["name"], p["extension"]))

    on_break_ids = {r["id"] for r in c.execute("SELECT id FROM agents WHERE status='Break / Lunch'")}

    queue_log = {}
    for q_name, q_data in snap.get("queues", {}).items():
        telico_members = {m["extension"]: m["available"] for m in q_data.get("members", [])}
        
        for r in c.execute("SELECT id, phone FROM agents WHERE phone != ''"):
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
        for r in c.execute("SELECT * FROM agents WHERE phone != ''"):
            if r[QUEUE_COL[q_name]] and r["id"] not in on_break_ids:
                db_available.append({"extension": r["phone"], "name": r["name"]})

        w = q_data["waiting"]; lw = q_data["longest_wait_seconds"]
        c.execute("UPDATE queue_metrics SET waiting=?, longest_wait_seconds=? WHERE queue=?", (w, lw, q_name))
        queue_log[q_name] = {
            "waiting": w, "members": len(q_data.get("members", [])),
            "available_count": len(db_available),
            "available_names": [a["name"] for a in db_available],
            "available_agents": db_available,
            "member_names": [m["name"] for m in q_data.get("members", [])],
        }
        
    cdr = snap.get("cdr", {})
    for q_name, q_stats in cdr.items():
        c.execute(
            "INSERT INTO calls_answered (date, queue, count, missed, attempts, outbound_answered, outbound_attempts, talk_secs, hold_secs, outbound_talk_secs) "
            "VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(date, queue) DO UPDATE SET count=?, missed=?, attempts=?, outbound_answered=?, outbound_attempts=?, talk_secs=?, hold_secs=?, outbound_talk_secs=?",
            (today_str(), q_name, q_stats["answered"], q_stats["missed"], q_stats["attempts"], q_stats["outbound_answered"], q_stats["outbound_attempts"], q_stats["talk_secs"], q_stats["hold_secs"], q_stats["outbound_talk_secs"],
             q_stats["answered"], q_stats["missed"], q_stats["attempts"], q_stats["outbound_answered"], q_stats["outbound_attempts"], q_stats["talk_secs"], q_stats["hold_secs"], q_stats["outbound_talk_secs"])
        )
        
    line_stats = snap.get("line_stats", {})
    for line, stats in line_stats.items():
        c.execute(
            "INSERT INTO line_stats (date, line, attempts, answered, talk_secs) VALUES (?,?,?,?,?) "
            "ON CONFLICT(date, line) DO UPDATE SET attempts=?, answered=?, talk_secs=?",
            (today_str(), line, stats["attempts"], stats["answered"], stats["talk"],
             stats["attempts"], stats["answered"], stats["talk"])
        )
        
    recent_calls = snap.get("recent_calls", [])
        
    c.commit()
    _last_sync = {"at": now_iso(), "ok": True, "queues": queue_log, "phones": len(snap.get("phones", [])), "recent_calls": recent_calls}

async def telico_sync_loop():
    while True:
        await asyncio.sleep(15)
        if not telico_client.is_configured(): continue
        try:
            snap = await asyncio.to_thread(telico_client.fetch_all)
            _apply_telico_snapshot(snap)
            await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
        except Exception as e:
            pass

async def break_expiry_loop():
    while True:
        await asyncio.sleep(10)
        now = now_local()
        c = connect()
        try: rows = c.execute("SELECT * FROM agents WHERE status='Break / Lunch' AND break_notified=0").fetchall()
        except: continue
        
        for row in rows:
            try: since = datetime.fromisoformat(row["status_since"])
            except: continue
            if (now - since).total_seconds() < BREAK_LIMIT_SECONDS: continue
            cur = c.execute("UPDATE agents SET break_notified=1 WHERE id=? AND break_notified=0 AND status='Break / Lunch'", (row["id"],))
            if cur.rowcount == 0: continue
            c.commit()
            if CALL_ON_BREAK_EXPIRY and row["phone"]:
                asyncio.create_task(_fire_call(row["phone"], row["name"], row["id"]))
            await hub.broadcast({"type": "break_expired", "agent_id": row["id"], "agent_name": row["name"]})
            await hub.broadcast({"type": "snapshot", "data": snapshot_full()})