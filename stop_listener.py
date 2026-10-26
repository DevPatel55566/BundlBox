import asyncio
import csv
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
OPT_OUT_FILE = Path(os.getenv("OPT_OUT_FILE", str(BASE_DIR / "opt_out.csv")))
CONTACTS_FILE = Path(os.getenv("CONTACTS_FILE", str(BASE_DIR / "contacts.csv")))
STATE_FILE = BASE_DIR / "stop_listener_state.json"

CONV_API_URL = os.getenv("CONV_API_URL", "https://sms.telico.cloud/api/conversations")
MSG_API_URL = os.getenv("MSG_API_URL", "https://sms.telico.cloud/api/messages")

API_TOKEN = os.getenv("TELICO_API_TOKEN", "")
SMS_USERNAME = os.getenv("SMS_USERNAME", "")
SMS_PASSWORD = os.getenv("SMS_PASSWORD", "")
SOURCE_DID = os.getenv("SMS_SOURCE_DID", "")

POLL_SECONDS = float(os.getenv("STOP_POLL_SECONDS", "30"))
WORKERS = int(os.getenv("STOP_WORKERS", "8"))
FIRST_SCAN_LIMIT = 50
REPEAT_SCAN_LIMIT = 10

EXACT_WORDS = {
    "stop", "stopall", "stop all", "unsubscribe", "optout", "opt out",
    "cancel", "end", "quit", "remove", "revoke",
    "no", "n", "nope", "no thanks", "no thank you",
}
START_WORDS = {"stop", "stopall", "unsubscribe", "optout"}
INBOUND_VALUES = {"in", "inbound", "incoming", "received", "rx", "mo"}
OUTBOUND_VALUES = {"out", "outbound", "outgoing", "sent", "tx", "mt"}

log = logging.getLogger("stop_listener")


def norm_phone(p) -> str:
    return re.sub(r"\D", "", str(p or ""))[-10:]


def is_opt_out(text: str) -> bool:
    t = " ".join(re.sub(r"[^a-z0-9 ]", " ", (text or "").lower()).split())
    if not t:
        return False
    if t in EXACT_WORDS:
        return True
    first = t.split(" ", 1)[0]
    return first in START_WORDS or t.startswith("opt out")


def message_text(m: dict) -> str:
    for key in ("message", "body", "text", "content"):
        v = m.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return ""


def is_inbound(m: dict) -> bool:
    for key in ("direction", "message_direction", "type"):
        v = str(m.get(key) or "").strip().lower()
        if v in INBOUND_VALUES:
            return True
        if v in OUTBOUND_VALUES:
            return False
    for key in ("is_inbound", "inbound", "incoming"):
        if isinstance(m.get(key), bool):
            return m[key]
    for key in ("is_outbound", "outbound", "outgoing"):
        if isinstance(m.get(key), bool):
            return not m[key]
    for key in ("from", "source", "sender", "source_did", "from_number"):
        v = m.get(key)
        if v and SOURCE_DID and norm_phone(v) == norm_phone(SOURCE_DID):
            return False
    return True


def make_session() -> requests.Session:
    s = requests.Session()
    if API_TOKEN:
        s.headers["Authorization"] = f"Bearer {API_TOKEN}"
    else:
        s.auth = HTTPBasicAuth(SMS_USERNAME, SMS_PASSWORD)
    return s


def api_get(s: requests.Session, url: str, params: dict):
    try:
        r = s.get(url, params=params, timeout=15)
    except requests.RequestException as e:
        log.warning("Request failed: %s", e)
        return None
    if r.status_code != 200:
        log.warning("GET %s -> HTTP %s: %s", url, r.status_code, r.text[:120])
        return None
    try:
        return r.json()
    except ValueError:
        log.warning("GET %s returned non-JSON", url)
        return None


def list_conversations(s: requests.Session) -> list:
    out, offset = [], 0
    while True:
        data = api_get(s, CONV_API_URL, {"did": SOURCE_DID, "limit": 100, "offset": offset})
        if data is None:
            break
        items = data if isinstance(data, list) else (data.get("conversations") or [])
        out.extend(items)
        offset += len(items)
        total = data.get("total") if isinstance(data, dict) else None
        if not items or (total is not None and offset >= total):
            break
    return out


def conv_phone(conv: dict) -> str:
    for key in ("phone_number", "phone", "contact_number", "number"):
        if conv.get(key):
            return str(conv[key])
    return ""


def conv_name(conv: dict) -> str:
    for key in ("name", "contact_name", "display_name"):
        v = conv.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def check_conversation(s: requests.Session, conv: dict, last_id: int):
    cid = conv.get("id")
    limit = REPEAT_SCAN_LIMIT if last_id else FIRST_SCAN_LIMIT
    data = api_get(s, MSG_API_URL, {"conversation_id": cid, "order": "desc", "limit": limit})
    if data is None:
        return cid, last_id, None
    msgs = data if isinstance(data, list) else (data.get("messages") or [])
    max_id, hit = last_id, None
    for m in msgs:
        mid = m.get("id")
        if isinstance(mid, int):
            max_id = max(max_id, mid)
            if mid <= last_id:
                continue
        if hit is None and is_inbound(m) and is_opt_out(message_text(m)):
            hit = message_text(m).strip()
    return cid, max_id, hit


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict):
    try:
        STATE_FILE.write_text(json.dumps(state), encoding="utf-8")
    except Exception as e:
        log.warning("Could not save state: %s", e)


def load_opt_outs() -> set:
    out = set()
    if not OPT_OUT_FILE.exists():
        return out
    with open(OPT_OUT_FILE, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            p = norm_phone(row.get("phone"))
            if p:
                out.add(p)
    return out


def load_contact_names() -> dict:
    names = {}
    if not CONTACTS_FILE.exists():
        return names
    try:
        with open(CONTACTS_FILE, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                p = norm_phone(row.get("phone"))
                if p and (row.get("name") or "").strip():
                    names[p] = row["name"].strip()
    except Exception as e:
        log.warning("Could not read %s: %s", CONTACTS_FILE, e)
    return names


def append_opt_out(name: str, phone: str):
    new_file = not OPT_OUT_FILE.exists() or OPT_OUT_FILE.stat().st_size == 0
    with open(OPT_OUT_FILE, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["name", "phone"])
        w.writerow([name, phone])


def poll_once(s: requests.Session, state: dict) -> int:
    opt_outs = load_opt_outs()
    names = load_contact_names()
    convs = [c for c in list_conversations(s) if c.get("id") is not None]
    added = 0

    def work(c):
        return c, check_conversation(s, c, int(state.get(str(c["id"]), 0)))

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for conv, (cid, max_id, hit) in pool.map(work, convs):
            state[str(cid)] = max_id
            phone = norm_phone(conv_phone(conv))
            if not hit or not phone or phone in opt_outs:
                continue
            name = names.get(phone) or conv_name(conv) or "Unknown"
            append_opt_out(name, phone)
            opt_outs.add(phone)
            added += 1
            log.info("OPT-OUT: %s (%s) replied %r", name, phone, hit)
    save_state(state)
    return added


# ── ASYNC LOOP FOR FASTAPI ──────────────────────────────────────────────────
async def stop_listener_loop():
    """Runs the stop listener in the background without blocking FastAPI."""
    if not SOURCE_DID or not (API_TOKEN or (SMS_USERNAME and SMS_PASSWORD)):
        log.warning("Stop Listener disabled: missing SMS_SOURCE_DID or credentials in .env")
        return

    s = make_session()
    state = load_state()
    log.info("Background Stop Listener started on %s every %ss -> %s", SOURCE_DID, POLL_SECONDS, OPT_OUT_FILE)
    
    while True:
        try:
            added = await asyncio.to_thread(poll_once, s, state)
            if added:
                log.info("Stop Listener added %d new opt-out(s)", added)
        except Exception as e:
            log.error("Stop Listener poll failed: %s", e)
        
        await asyncio.sleep(POLL_SECONDS)