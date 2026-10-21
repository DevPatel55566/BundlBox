"""Telico admin panel client (as2.telico.ca — Telinta white-label)."""
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
import csv
import io
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("telico_client")

BASE_URL = os.getenv("TELICO_BASE_URL", "https://as2.telico.ca").rstrip("/")
USERNAME = os.getenv("TELICO_USERNAME", "")
PASSWORD = os.getenv("TELICO_PASSWORD", "")
CUSTOMER_ID = os.getenv("TELICO_CUSTOMER_ID", "2131")

QUEUE_IDS = {"French": 440, "English": 439, "Delivery Line": 441}
TELICO_TO_OUR = {"French CSR": "French", "English CSR": "English",
                 "Delivery Line": "Delivery Line"}

CALLER_ID = os.getenv("TELICO_CALLER_ID", "Reminder : Break Over")
TIMEOUT = 15
_session: requests.Session | None = None

# CDR Rules
ATTEMPT_STATUSES = {"busy", "cancel", "congestion", "chanunavail", "no answer"}
ANSWER_STATUS = "answer"
CALLID_RE = re.compile(r"[?&;]callid=([0-9]+\.[0-9]+)")

def is_configured() -> bool:
    return bool(USERNAME and PASSWORD)

def _session_get() -> requests.Session:
    global _session
    if _session is not None:
        return _session
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Referer": BASE_URL + "/",
    })
    
    try:
        r = s.post(BASE_URL + "/", data={
            "username": USERNAME,
            "password": PASSWORD,
            "submit": "Log in"
        }, timeout=TIMEOUT)
        
        if r.status_code == 200 and "Welcome to" in r.text:
            logger.info("Telico login successful")
        else:
            logger.warning("Telico login response: %s (Check credentials)", r.status_code)
    except Exception as e:
        logger.error("Telico login network error: %s", e)
        
    _session = s
    return s

def _get(path: str) -> str | None:
    try:
        r = _session_get().get(BASE_URL + path, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            logger.warning("GET %s -> %s", path, r.status_code)
            return None
        return r.text
    except Exception as e:
        logger.warning("GET %s error: %s", path, e)
        return None

def _parse_duration(s: str) -> int:
    if not s: return 0
    nums = [int(x) for x in re.findall(r"\d+", s)]
    if len(nums) == 3: return nums[0] * 3600 + nums[1] * 60 + nums[2]
    if len(nums) == 2: return nums[0] * 60 + nums[1]
    return nums[0] if nums else 0



def fetch_queue_members(queue_id: int) -> list[dict]:
    html = _get(f"/features/queues/edit/?id={queue_id}")
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    
    for sel in soup.find_all("select"):
        name = sel.get("name") or ""
        m = re.match(r"available_(\d+)", name)
        if not m: continue
        
        dest_id = m.group(1)
        row = sel.find_parent("tr")
        if not row: continue
        
        row_text = row.get_text(" ", strip=True)
        em = re.search(r"Telephone line:\s*(\d+)\s+(.+?)(?:\s+Available|\s+Unavailable)?$", row_text)
        if not em: em = re.search(r"Telephone line:\s*(\d+)\s+(.+)", row_text)
        if not em: continue
        
        ext = em.group(1)
        if ext in seen: continue
        
        name_txt = re.sub(r"\s+", " ", em.group(2).strip()).strip(" -")
        
        # THE FIX: Accurately parse the <select> dropdown for Available/Unavailable
        opt = sel.find("option", selected=True)
        available = False
        if opt:
            val = (opt.get("value") or "").strip()
            text = opt.get_text(strip=True).lower()
            if val == "1" or ("available" in text and "unavailable" not in text):
                available = True
                
        out.append({"extension": ext, "name": name_txt, "available": available, "dest_id": dest_id})
        seen.add(ext)
    return out

def fetch_phone_lines() -> list[dict]:
    xml = _get("/panel/state/")
    if not xml: return []
    
    def _entity_replacer(match):
        entity = match.group(0)
        if entity in ("&amp;", "&lt;", "&gt;", "&quot;", "&apos;"): return entity
        name = entity[1:]
        import html.entities
        if name in html.entities.html5: return html.entities.html5[name]
        return entity
        
    xml = re.sub(r"&[a-zA-Z]+;", _entity_replacer, xml)
    
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as e:
        logger.error("Failed to parse /panel/state/ XML: %s", e)
        return []

    out = []
    for p in root.findall(".//phones/phone"):
        ext = p.get("name") or p.get("dnumber")
        if not ext: continue
        direction = (p.get("call_direction") or "").strip().lower()
        party = (p.get("call_party") or "").strip().lower()
        label = p.get("label") or p.get("description") or ""
        registered = p.get("registered") == "1"
        is_answered = "play sound" in party
        out.append({
            "extension": ext, "name": label, "registered": registered,
            "on_call": bool(direction), "answered": is_answered,
            "call_direction": direction, "call_party": party,
        })
    return out

def fetch_lines_activity(extension: str) -> dict:
    for r in fetch_phone_lines():
        if r["extension"] == extension:
            return {"found": True, "on_call": r.get("on_call", False), "answered": r.get("answered", False), "registered": r["registered"]}
    return {"found": False, "on_call": False, "answered": False, "registered": False}

def set_queue_membership(queue_id: int, extension: str, available: bool) -> dict:
    s = _session_get()
    edit_html = _get(f"/features/queues/edit/?id={queue_id}")
    if not edit_html: return {"ok": False, "reason": "edit page fetch failed"}
    
    soup = BeautifulSoup(edit_html, "html.parser")
    target_dest_id = None
    for sel in soup.find_all("select"):
        name = sel.get("name") or ""
        m = re.match(r"available_(\d+)", name)
        if not m: continue
        row = sel.find_parent("tr")
        if not row: continue
        if not re.search(rf"Telephone line:\s*{re.escape(extension)}\b", row.get_text(" ", strip=True)): continue
        target_dest_id = m.group(1)
        break
        
    if not target_dest_id: return {"ok": False, "reason": f"extension {extension} not in queue {queue_id}"}

    url = BASE_URL + "/features/queues/destinations/update/"
    payload = {
        "queue": str(queue_id),
        f"select_{target_dest_id}": "on",
        f"available_{target_dest_id}": "1" if available else "0",
        "update": "Update selected",
    }
    try:
        r = s.post(url, data=payload, timeout=TIMEOUT, allow_redirects=True)
        return {"ok": r.status_code in (200, 302), "status": r.status_code}
    except Exception as e:
        return {"ok": False, "reason": str(e)}

def call_extension(phone_number: str, message_path: str = "") -> dict:
    s = _session_get()
    url = f"{BASE_URL}/utils/play/"
    params = {
        "feature": "queues", "id": message_path, "number": phone_number,
        "callerid": CALLER_ID, "callername": CALLER_ID,
    }
    try:
        r = s.get(url, params=params, timeout=TIMEOUT, allow_redirects=True)
        ok = r.status_code in (200, 302)
        return {"ok": ok, "status": r.status_code, "url": r.url, "sent": params}
    except Exception as e:
        return {"ok": False, "reason": str(e), "sent": params}
def fetch_active_calls() -> dict:
    out = {name: {"waiting": 0, "longest_wait_seconds": 0} for name in QUEUE_IDS}
    html = _get("/features/queues/active/?new_page=0;hide_options=1;queue=0;sort=queue_start;descending=0;refresh=60")
    if not html: return out
    soup = BeautifulSoup(html, "html.parser")
    if "No calls found" in soup.get_text(): return out
    
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3: continue
        
        # Look at the whole row to find the queue name
        row_text = row.get_text(" ", strip=True).replace('\xa0', ' ')
        
        q_name = None
        if "French CSR" in row_text: q_name = "French"
        elif "English CSR" in row_text: q_name = "English"
        elif "Delivery Line" in row_text: q_name = "Delivery Line"
        
        if not q_name: continue
        
        # The duration is always the last cell
        secs = _parse_duration(cells[-1].get_text(strip=True))
        out[q_name]["waiting"] += 1
        if secs > out[q_name]["longest_wait_seconds"]:
            out[q_name]["longest_wait_seconds"] = secs
    return out


def fetch_today_calls() -> dict:
    """
    Fetches today's CDR accurately and groups by Unique Call ID.
    Parses the HTML table instead of CSV to extract the hidden `callid`.
    """
    out = {
        "English": {"answered": 0, "missed": 0, "attempts": 0},
        "French": {"answered": 0, "missed": 0, "attempts": 0},
        "Delivery Line": {"answered": 0, "missed": 0, "attempts": 0}
    }
    
    now = datetime.now()
    tomorrow = now + timedelta(days=1)
    today_start = now.strftime("%Y-%m-%d-0-0-0")
    tomorrow_start = tomorrow.strftime("%Y-%m-%d-0-0-0")
    
    params = {
        "start_year": now.year, "start_month": now.month, "start_day": now.day,
        "start_hour": 0, "start_minute": 0, "start_second": 0, "start_buffer": today_start,
        "end_year": tomorrow.year, "end_month": tomorrow.month, "end_day": tomorrow.day,
        "end_hour": 0, "end_minute": 0, "end_second": 0, "end_buffer": tomorrow_start,
        "customer": CUSTOMER_ID, "recurse": "0", "status": "", # Empty status gets ALL calls (answered & missed)
        "direction": "", "caller_match": "contains", "caller_number": "",
        "callername_match": "contains", "callername_number": "", "called_match": "contains",
        "called_number": "", "phone": "", "talktime_minimum": "0", "talktime_maximum": "No limit",
        "cost_minimum": "No limit", "cost_maximum": "No limit", "cost_customer": "scustomer",
        "include_tax": "1", "sort": "start", "descending": "0", "simple": "0", "stype": "",
        "archive": "0"
        # Removed 'format': 'csv' so we get the HTML table with the links
    }

    try:
        r = _session_get().get(BASE_URL + "/reports/cdrs/list/", params=params, timeout=20)
        if r.status_code != 200:
            logger.warning("CDR fetch failed with status %s", r.status_code)
            return out
            
        soup = BeautifulSoup(r.text, "html.parser")
        calls = {}
        
        # Telinta marks table rows with class "row1" or "row2"
        for row in soup.find_all("tr", class_=["row1", "row2"]):
            tds = row.find_all("td")
            if len(tds) < 3: continue
            
            # 1. Extract the Unique Call ID from the link in the first column
            a_tag = tds[0].find("a")
            if not a_tag: continue
            m = CALLID_RE.search(a_tag.get("href", ""))
            if not m: continue
            callid = m.group(1)
            
            # 2. Identify the Queue by scanning the row text
            row_text = row.get_text(" ", strip=True).lower()
            q_name = None
            if "english csr" in row_text: q_name = "English"
            elif "french csr" in row_text: q_name = "French"
            elif "delivery line" in row_text: q_name = "Delivery Line"
            
            if not q_name: continue
            
            if callid not in calls:
                calls[callid] = {"queue": q_name, "answered": False, "attempts": 0}
                
            # 3. Extract Status and Direction
            # Col 1 looks like: "Answer<br>Inbound" or "Busy<br>Internal"
            status_html = tds[1].decode_contents()
            parts = re.sub(r'<[^>]+>', '|', status_html).split("|")
            status = parts[0].strip().lower() if len(parts) > 0 else ""
            
            # 4. Extract Called Number
            # Col 2 looks like: "6047990926 "EnBrit. Columbia"<br>8008470236"
            numbers_html = tds[2].decode_contents()
            num_parts = re.sub(r'<[^>]+>', '|', numbers_html).split("|")
            called_text = num_parts[1].strip().lower() if len(num_parts) > 1 else ""
            
            # A queue leg is just ringing the queue. An agent leg rings an extension.
            is_queue_leg = "csr" in called_text or "delivery" in called_text
            
            # If an agent answered
            if status == "answer" and not is_queue_leg:
                calls[callid]["answered"] = True
            # If an agent rejected or missed
            elif status in ATTEMPT_STATUSES and not is_queue_leg:
                calls[callid]["attempts"] += 1
                
        for c in calls.values():
            q = c["queue"]
            if c["answered"]:
                out[q]["answered"] += 1
            elif c["attempts"] > 0:
                out[q]["missed"] += 1
            out[q]["attempts"] += c["attempts"]
            
        logger.info("CDR Sync: %s", out)
        return out
        
    except Exception as e:
        logger.error("CDR stats error: %s", e)
        return out
def fetch_all() -> dict:
    if not is_configured():
        return {"ok": False, "reason": "not configured"}
    active = fetch_active_calls()
    queues = {}
    for our_name, qid in QUEUE_IDS.items():
        members = fetch_queue_members(qid)
        queues[our_name] = {
            "waiting": active[our_name]["waiting"],
            "longest_wait_seconds": active[our_name]["longest_wait_seconds"],
            "members": members,
        }
    phones = fetch_phone_lines()
    cdr_stats = fetch_today_calls()
    return {"ok": True, "queues": queues, "phones": phones, "cdr": cdr_stats}