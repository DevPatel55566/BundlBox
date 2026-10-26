"""Telico admin panel client (as2.telico.ca — Telinta white-label)."""
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
import csv
import io
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
load_dotenv()
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("telico_client")

try:
    TZ = ZoneInfo(os.getenv("TIMEZONE", "America/Toronto"))
except Exception:
    from datetime import timezone
    TZ = timezone(timedelta(hours=-4))

BASE_URL = os.getenv("TELICO_BASE_URL", "https://as2.telico.ca").rstrip("/")
USERNAME = os.getenv("TELICO_USERNAME", "")
PASSWORD = os.getenv("TELICO_PASSWORD", "")
CUSTOMER_ID = os.getenv("TELICO_CUSTOMER_ID", "2131")

QUEUE_IDS = {"French": 440, "English": 439, "Delivery Line": 441}
CALLER_ID = os.getenv("TELICO_CALLER_ID", "Reminder : Break Over")
TIMEOUT = 15
_session: requests.Session | None = None

ATTEMPT_STATUSES = {"busy", "cancel", "congestion", "chanunavail", "no answer"}
CALLID_RE = re.compile(r"[?&;]callid=([0-9]+\.[0-9]+)")


def is_configured() -> bool:
    return bool(USERNAME and PASSWORD)


def _session_get() -> requests.Session:
    global _session
    if _session is not None: return _session

    if not USERNAME or not PASSWORD:
        logger.error("TELICO_USERNAME / TELICO_PASSWORD not set — cannot log in")
        raise RuntimeError("Telico credentials missing")

    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": BASE_URL + "/",
    })
    try:
        r = s.post(BASE_URL + "/", data={
            "username": USERNAME,
            "password": PASSWORD,
            "submit": "Log in",
        }, timeout=TIMEOUT, allow_redirects=True)

        still_login_form = 'type="password"' in (r.text or "")[:8000]
        if still_login_form or r.url.rstrip("/").endswith("/login"):
            logger.error("Telico login FAILED — still on login form. Check creds.")
            raise RuntimeError("Telico login failed")
        logger.info("Telico login successful (landed on %s)", r.url)
    except requests.RequestException as e:
        logger.error("Telico login network error: %s", e)
        raise
    _session = s
    return s


def _get(path: str) -> str | None:
    try:
        r = _session_get().get(BASE_URL + path, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            logger.warning("GET %s -> %s", path, r.status_code)
            return None
        if 'type="password"' in (r.text or "")[:8000]:
            logger.error("GET %s returned login page — session expired or not logged in", path)
            global _session
            _session = None
            r = _session_get().get(BASE_URL + path, timeout=TIMEOUT, allow_redirects=True)
            if 'type="password"' in (r.text or "")[:8000]:
                logger.error("GET %s STILL returning login page after re-login", path)
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


# -------------------------------------------------------------------------
# TIME GROUP / SCHEDULE MANAGEMENT
# -------------------------------------------------------------------------

def fetch_time_groups() -> dict:
    html = _get("/features/times/groups/list/")
    if not html or len(html) < 500:
        html = _get("/features/times/groups/")
        
    if not html:
        logger.error("Could not fetch time groups list.")
        return {"business hours": "2303"}

    soup = BeautifulSoup(html, "html.parser")
    groups = {}
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "id=" in href:
            m = re.search(r"id=(\d+)", href)
            if m:
                gid = m.group(1)
                name = a.get_text(strip=True)
                if not name: continue
                name_lower = name.lower()
                if name_lower in ("edit", "save", "delete", "select all", "new", "make copy"):
                    continue
                groups[name_lower] = gid

    logger.info(f"Parsed Time Groups: {groups}")
    if "business hours" not in groups:
        groups["business hours"] = "2303"
        
    return groups


def fetch_time_periods(group_id: int) -> list[dict]:
    html = _get(f"/features/times/groups/edit/?id={group_id}")
    if not html:
        logger.error(f"Could not fetch time group edit page for ID {group_id}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    periods = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "periods/edit/" in href and "id=" in href:
            m = re.search(r"id=(\d+)", href)
            if m:
                pid = m.group(1)
                name = a.get_text(strip=True)
                if name:
                    periods.append({"id": pid, "name": name})

    logger.info(f"Parsed Periods for Group {group_id}: {periods}")
    return periods


def _form_to_dict(form) -> dict:
    out: dict = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name: continue
        typ = (inp.get("type") or "text").lower()
        if typ in ("submit", "button", "reset", "image"): continue
        if typ in ("checkbox", "radio"):
            if inp.get("checked") is not None:
                out.setdefault(name, []).append(inp.get("value") or "on")
        else:
            out[name] = inp.get("value") or ""

    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name: continue
        opts = sel.find_all("option")
        selected = [
            (o.get("value") if o.get("value") is not None else o.get_text(strip=True))
            for o in opts if o.get("selected") is not None
        ]
        if not selected and opts:
            first = opts[0]
            selected = [first.get("value") if first.get("value") is not None
                        else first.get_text(strip=True)]
        if sel.get("multiple") is not None:
            out[name] = selected
        else:
            out[name] = selected[0]

    for ta in form.find_all("textarea"):
        name = ta.get("name")
        if name:
            out[name] = ta.get_text()

    return out


def save_time_period(group_id: int, period_id: str | None, data: dict) -> bool:
    from urllib.parse import urljoin

    s = _session_get()

    if period_id:
        form_path = f"/features/times/periods/edit/?id={period_id}"
    else:
        form_path = f"/features/times/periods/new/?timegroup={group_id}"

    html = _get(form_path)
    if not html:
        logger.error(f"save_time_period {data['name']}: cannot fetch {form_path}")
        return False

    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", action=re.compile(r"save"))
    if not form:
        logger.error(f"save_time_period {data['name']}: no save form on {form_path}")
        return False

    action_attr = form.get("action", "save/")
    post_url = urljoin(BASE_URL + form_path, action_attr)
    logger.info(f"save_time_period {data['name']}: POST → {post_url}")

    payload = _form_to_dict(form)

    payload["name"] = data["name"]
    payload["timegroup"] = str(group_id)
    if period_id:
        payload["id"] = str(period_id)
    payload["repeats"] = "1" if data.get("repeats") else "0"

    sh, sm = data["start"].split(":")
    eh, em = data["end"].split(":")
    payload["starthour"]   = str(int(sh))
    payload["startminute"] = str(int(sm))
    payload["startsecond"] = "0"
    payload["endhour"]     = str(int(eh))
    payload["endminute"]   = str(int(em))
    payload["endsecond"]   = "59"

    now = datetime.now()
    if data.get("repeats"):
        end_dt = now.replace(year=now.year + 20)
        payload["start_year"]  = str(now.year)
        payload["start_month"] = f"{now.month:02d}"
        payload["start_day"]   = f"{now.day:02d}"
        payload["end_year"]    = str(end_dt.year)
        payload["end_month"]   = f"{end_dt.month:02d}"
        payload["end_day"]     = f"{end_dt.day:02d}"
        payload["days_week"]   = data.get("days_week", [])
    elif data.get("date"):
        d = datetime.strptime(data["date"], "%Y-%m-%d")
        payload["start_year"]  = str(d.year)
        payload["start_month"] = f"{d.month:02d}"
        payload["start_day"]   = f"{d.day:02d}"
        payload["end_year"]    = str(d.year)
        payload["end_month"]   = f"{d.month:02d}"
        payload["end_day"]     = f"{d.day:02d}"
        payload.pop("days_week", None)

    submit = form.find("input", {"type": "submit"})
    if submit and submit.get("name"):
        payload[submit["name"]] = submit.get("value") or "Save"

    try:
        r = s.post(post_url, data=payload, timeout=TIMEOUT, allow_redirects=True)
    except Exception as e:
        logger.error(f"save_time_period {data['name']}: exception {e}")
        return False

    final_url = r.url or ""
    body_lc   = (r.text or "").lower()

    if (
        "time period saved" in body_lc
        or "has been saved" in body_lc
        or "groups/edit" in final_url
    ):
        logger.info(f"save_time_period {data['name']}: OK → {final_url}")
        return True

    body = r.text or ""
    soup2 = BeautifulSoup(body, "html.parser")
    err_node = (
        soup2.find(class_=re.compile(r"\berror\b", re.I))
        or soup2.find(id=re.compile(r"error", re.I))
        or soup2.find(class_=re.compile(r"alert|danger", re.I))
    )
    if err_node:
        logger.error(f"save_time_period {data['name']}: REJECTED — "
                     f"{err_node.get_text(' ', strip=True)[:400]}")
    else:
        logger.error(f"save_time_period {data['name']}: rejected, "
                     f"url={final_url}, status={r.status_code}")
        try:
            dbg = f"telico_reject_{data['name'].replace(' ', '_')}.html"
            with open(dbg, "w", encoding="utf-8") as f:
                f.write(body)
            logger.error(f"  saved response → {dbg}")
        except Exception:
            pass

    return False


def delete_time_period(period_id: str, group_id: str) -> bool:
    s = _session_get()
    url = f"{BASE_URL}/features/times/periods/delete/"
    payload = {
        f"select_{period_id}": "on",
        "timegroup": str(group_id),
        "delete": "Delete selected",
    }
    try:
        r = s.post(url, data=payload, timeout=TIMEOUT, allow_redirects=True)
        ok = r.status_code in (200, 302) and 'type="password"' not in (r.text or "")[:8000].lower()
        logger.info(f"delete_time_period {period_id}: {'OK' if ok else 'FAILED'} "
                    f"(status={r.status_code}, url={r.url})")
        return ok
    except Exception as e:
        logger.error(f"delete_time_period {period_id}: {e}")
        return False


# -------------------------------------------------------------------------
# QUEUE & CDR MANAGEMENT
# -------------------------------------------------------------------------

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
        opt = sel.find("option", selected=True)
        available = False
        if opt:
            val = (opt.get("value") or "").strip()
            text = opt.get_text(strip=True).lower()
            if val == "1" or ("available" in text and "unavailable" not in text): available = True
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
    try: root = ET.fromstring(xml)
    except ET.ParseError: return []
    out = []
    for p in root.findall(".//phones/phone"):
        ext = p.get("name") or p.get("dnumber")
        if not ext: continue
        direction = (p.get("call_direction") or "").strip().lower()
        party = (p.get("call_party") or "").strip().lower()
        label = p.get("label") or p.get("description") or ""
        
        feature_code = p.get("feature_code") or ""
        formatted_name = f'{feature_code} "{label}"' if feature_code else label

        registered = p.get("registered") == "1"
        is_answered = "play sound" in party
        out.append({
            "extension": ext, 
            "name": formatted_name, 
            "registered": registered, 
            "on_call": bool(direction), 
            "answered": is_answered, 
            "call_direction": direction, 
            "call_party": party
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
    payload = {"queue": str(queue_id), f"select_{target_dest_id}": "on", f"available_{target_dest_id}": "1" if available else "0", "update": "Update selected"}
    try:
        r = s.post(url, data=payload, timeout=TIMEOUT, allow_redirects=True)
        return {"ok": r.status_code in (200, 302), "status": r.status_code}
    except Exception as e: return {"ok": False, "reason": str(e)}


def call_extension(phone_number: str, message_path: str = "") -> dict:
    s = _session_get()
    url = f"{BASE_URL}/utils/play/"
    params = {"feature": "queues", "id": message_path, "number": phone_number, "callerid": CALLER_ID, "callername": CALLER_ID}
    try:
        r = s.get(url, params=params, timeout=TIMEOUT, allow_redirects=True)
        return {"ok": r.status_code in (200, 302), "status": r.status_code, "url": r.url, "sent": params}
    except Exception as e: return {"ok": False, "reason": str(e), "sent": params}


def fetch_active_calls() -> dict:
    out = {name: {"waiting": 0, "longest_wait_seconds": 0} for name in QUEUE_IDS}
    html = _get("/features/queues/active/?new_page=0;hide_options=1;queue=0;sort=queue_start;descending=0;refresh=60")
    if not html: return out
    soup = BeautifulSoup(html, "html.parser")
    if "No calls found" in soup.get_text(): return out
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3: continue
        row_text = row.get_text(" ", strip=True).replace('\xa0', ' ')
        q_name = None
        if "French CSR" in row_text: q_name = "French"
        elif "English CSR" in row_text: q_name = "English"
        elif "Delivery Line" in row_text: q_name = "Delivery Line"
        if not q_name: continue
        
        secs = 0
        for td in reversed(cells):
            text = td.get_text(strip=True)
            if re.match(r"^\d{1,2}:\d{2}(:\d{2})?$", text):
                secs = _parse_duration(text)
                break
                
        out[q_name]["waiting"] += 1
        if secs > out[q_name]["longest_wait_seconds"]: out[q_name]["longest_wait_seconds"] = secs
    return out


def fetch_today_calls(queue_members: dict | None = None) -> tuple[dict, dict, list]:
    out = {
        "English": {"answered": 0, "missed": 0, "attempts": 0, "outbound_answered": 0, "outbound_attempts": 0, "talk_secs": 0, "hold_secs": 0, "outbound_talk_secs": 0},
        "French": {"answered": 0, "missed": 0, "attempts": 0, "outbound_answered": 0, "outbound_attempts": 0, "talk_secs": 0, "hold_secs": 0, "outbound_talk_secs": 0},
        "Delivery Line": {"answered": 0, "missed": 0, "attempts": 0, "outbound_answered": 0, "outbound_attempts": 0, "talk_secs": 0, "hold_secs": 0, "outbound_talk_secs": 0}
    }
    line_stats = {}
    recent_calls = []
    now = datetime.now(TZ)
    tomorrow = now + timedelta(days=1)
    today_start = now.strftime("%Y-%m-%d-0-0-0")
    tomorrow_start = tomorrow.strftime("%Y-%m-%d-0-0-0")
    params = {
        "start_year": now.year, "start_month": now.month, "start_day": now.day,
        "start_hour": 0, "start_minute": 0, "start_second": 0, "start_buffer": today_start,
        "end_year": tomorrow.year, "end_month": tomorrow.month, "end_day": tomorrow.day,
        "end_hour": 0, "end_minute": 0, "end_second": 0, "end_buffer": tomorrow_start,
        "customer": CUSTOMER_ID, "recurse": "0", "status": "", "direction": "", "caller_match": "contains", "caller_number": "",
        "callername_match": "contains", "callername_number": "", "called_match": "contains", "called_number": "", "phone": "",
        "talktime_minimum": "0", "talktime_maximum": "No limit", "cost_minimum": "No limit", "cost_maximum": "No limit",
        "cost_customer": "scustomer", "include_tax": "1", "sort": "start", "descending": "0", "simple": "0", "stype": "", "archive": "0"
    }
    try:
        r = _session_get().get(BASE_URL + "/reports/cdrs/list/", params=params, timeout=20)
        if r.status_code != 200: return out, line_stats, recent_calls
        soup = BeautifulSoup(r.text, "html.parser")
        calls = {}
        
        ext_map = {}
        code_map = {}
        if queue_members:
            for q_name, q_data in queue_members.items():
                for mem in q_data["members"]:
                    ext_map[mem["extension"]] = mem["name"]
                    m_code = re.match(r'^(\d+)\s+', mem["name"])
                    if m_code:
                        code_map[m_code.group(1)] = mem["name"]
        
        for row in soup.find_all("tr", class_=["row1", "row2"]):
            tds = row.find_all("td")
            if len(tds) < 5: continue
            a_tag = tds[0].find("a")
            if not a_tag: continue
            m = CALLID_RE.search(a_tag.get("href", ""))
            if not m: continue
            callid = m.group(1)
            
            status_raw = tds[1].get_text(strip=True)
            status_lc = status_raw.lower()
            if "answer" in status_lc: status = "answer"
            elif "busy" in status_lc: status = "busy"
            elif "cancel" in status_lc: status = "cancel"
            elif "congestion" in status_lc: status = "congestion"
            elif "chanunavail" in status_lc: status = "chanunavail"
            elif "no answer" in status_lc: status = "no answer"
            else: status = status_lc
            
            direction = "outbound" if "outbound" in status_lc else "inbound"
            
            col2 = tds[2].get_text(strip=True)
            if '"' in col2:
                parts = col2.split('"')
                source_line = parts[0].strip()
                called_text = parts[-1].strip()
            else:
                source_line = col2
                called_text = col2

            col3 = tds[3].get_text(strip=True)
            time_str = col3[:19] if len(col3) >= 19 else col3
            
            col4 = tds[4].get_text(strip=True)
            dur_match = re.search(r"(\d+:\d+(?::\d+)?)", col4)
            dur = _parse_duration(dur_match.group(1)) if dur_match else 0

            row_text = row.get_text(" ", strip=True).lower()
            
            # --- FIX: Robust queue matching ---
            q_name = None
            if "english csr" in row_text or "english" in source_line.lower() or '"en' in row_text:
                q_name = "English"
            elif "french csr" in row_text or "french" in source_line.lower() or '"fr' in row_text:
                q_name = "French"
            elif "delivery line" in row_text or "delivery" in source_line.lower():
                q_name = "Delivery Line"

            if not q_name and direction == "outbound" and queue_members:
                m_ext = re.match(r'^(\d+)', source_line)
                if m_ext:
                    ext = m_ext.group(1)
                    for q_iter, q_data in queue_members.items():
                        if any(mem["extension"] == ext for mem in q_data["members"]):
                            q_name = q_iter
                            break
                            
            if not q_name and direction == "outbound":
                q_name = "English"

            if not q_name:
                continue

            if callid not in calls:
                calls[callid] = {
                    "queue": q_name, "answered": False, "attempts": 0,
                    "outbound": (direction == "outbound"), "talk": 0, "hold": 0,
                    "source_line": source_line, "called_text": called_text,
                    "external_source": "", "agent_dest": "", "time_str": time_str
                }
            else:
                if q_name and not calls[callid]["queue"]:
                    calls[callid]["queue"] = q_name
                    
            c = calls[callid]

            if direction == "inbound":
                if "csr" not in source_line.lower() and "delivery" not in source_line.lower():
                    c["external_source"] = source_line
            if "csr" not in called_text.lower() and "delivery" not in called_text.lower():
                c["agent_dest"] = called_text

            if status == "answer":
                c["answered"] = True
                c["talk"] = max(c["talk"], dur)
            elif status in ATTEMPT_STATUSES:
                c["attempts"] += 1
                c["hold"] += dur

        for c in calls.values():
            if not c["queue"]: continue

            q = c["queue"]
            if c["outbound"]:
                dest_str = c.get("called_text") or ""
                digits = re.sub(r'\D', '', dest_str)
                if len(digits) != 10:
                    continue

                if c["answered"]:
                    out[q]["outbound_answered"] += 1
                    out[q]["outbound_talk_secs"] += c["talk"]
                out[q]["outbound_attempts"] += 1
                
                src_digits = re.sub(r'\D', '', c.get("source_line", ""))
                final_agent_name = ext_map.get(src_digits, "")
                if not final_agent_name:
                    m_src = re.match(r'^(\d+)', c.get("source_line", ""))
                    if m_src: final_agent_name = code_map.get(m_src.group(1), "")
                if not final_agent_name and src_digits:
                    final_agent_name = src_digits

            else:
                ext_src = c.get("external_source", "")
                if "8008470236" in ext_src or "9059015353" in ext_src:
                    continue

                if c["answered"]:
                    out[q]["answered"] += 1
                    out[q]["talk_secs"] += c["talk"]
                elif c["attempts"] > 0:
                    out[q]["missed"] += 1
                out[q]["attempts"] += c["attempts"]
                out[q]["hold_secs"] += c["hold"]

                dest_raw = c.get("agent_dest") or c.get("called_text") or ""
                dest_digits = re.sub(r'\D', '', dest_raw)
                final_agent_name = ext_map.get(dest_digits, "")
                
                if not final_agent_name:
                    m_dest = re.match(r'^(\d+)', dest_raw)
                    if m_dest: 
                        final_agent_name = code_map.get(m_dest.group(1), "")
                        
                if not final_agent_name and dest_raw:
                    final_agent_name = dest_raw

                c["agent_dest"] = final_agent_name

            dest_display = final_agent_name if final_agent_name else (c.get("agent_dest") or c["called_text"])
            src_display = c.get("external_source") or c["source_line"]
            
            recent_calls.append({
                "time": c["time_str"], "queue": q,
                "direction": "Outbound" if c["outbound"] else "Inbound",
                "status": "Answer" if c["answered"] else c["status"].capitalize() if "status" in c else "Missed",
                "source": src_display, "destination": dest_display,
                "duration": c["talk"]
            })

        recent_calls.sort(key=lambda x: x["time"], reverse=True)
        recent_calls = recent_calls[:100]

        return out, line_stats, recent_calls
    except Exception as e:
        logger.error("CDR stats error: %s", e)
        return out, line_stats, recent_calls


def fetch_all() -> dict:
    if not is_configured(): return {"ok": False, "reason": "not configured"}
    active = fetch_active_calls()
    queues = {}
    for our_name, qid in QUEUE_IDS.items():
        members = fetch_queue_members(qid)
        queues[our_name] = {"waiting": active[our_name]["waiting"], "longest_wait_seconds": active[our_name]["longest_wait_seconds"], "members": members}
    phones = fetch_phone_lines()
    
    cdr_stats, line_stats, recent_calls = fetch_today_calls(queues)
    
    return {"ok": True, "queues": queues, "phones": phones, "cdr": cdr_stats, "line_stats": line_stats, "recent_calls": recent_calls}