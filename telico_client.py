"""Telico admin panel client (as2.telico.ca — Telinta white-label)."""
import logging
import os
import re
import time
import xml.etree.ElementTree as ET

import requests  # pyright: ignore[reportMissingModuleSource]
from bs4 import BeautifulSoup

logger = logging.getLogger("telico_client")

BASE_URL = os.getenv("TELICO_BASE_URL", "https://as2.telico.ca").rstrip("/")
USERNAME = os.getenv("TELICO_USERNAME", "")
PASSWORD = os.getenv("TELICO_PASSWORD", "")

QUEUE_IDS = {"French": 440, "English": 439, "Delivery Line": 441}
TELICO_TO_OUR = {"French CSR": "French", "English CSR": "English",
                 "Delivery Line": "Delivery Line"}

CALLER_ID = os.getenv("TELICO_CALLER_ID", "Reminder : Break Over")

TIMEOUT = 12
_session: requests.Session | None = None


def is_configured() -> bool:
    return bool(USERNAME and PASSWORD)


def _session_get() -> requests.Session:
    global _session
    if _session is not None:
        return _session
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (CC-Wallboard)",
        "Accept": "text/html,application/xhtml+xml,application/xml",
        "Referer": BASE_URL + "/",
    })
    s.cookies.set("_username", USERNAME, domain="as2.telico.ca", path="/")
    s.cookies.set("_password", PASSWORD, domain="as2.telico.ca", path="/")
    s.cookies.set("_users",    USERNAME, domain="as2.telico.ca", path="/")
    s.cookies.set("_pages",    "codes,0,0", domain="as2.telico.ca", path="/")
    _session = s
    return s


def _get(path: str) -> str | None:
    try:
        r = _session_get().get(BASE_URL + path, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            logger.warning("GET %s -> %s", path, r.status_code)
            return None
        return r.text
    except requests.exceptions.RequestException as e:
        logger.warning("Network error on GET %s (server dropped connection): %s", path, e)
        return None
    except Exception as e:
        logger.error("GET %s failed: %s", path, e)
        return None


def fetch_static(path: str) -> dict:
    s = _session_get()
    try:
        r = s.get(BASE_URL + path, timeout=TIMEOUT)
        return {"status": r.status_code, "body": r.text[:5000]}
    except Exception as e:
        return {"error": str(e)}


def _parse_duration(s: str) -> int:
    if not s:
        return 0
    nums = [int(x) for x in re.findall(r"\d+", s)]
    if len(nums) == 3:
        return nums[0] * 3600 + nums[1] * 60 + nums[2]
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    return nums[0] if nums else 0


def fetch_active_calls() -> dict:
    out = {name: {"waiting": 0, "longest_wait_seconds": 0} for name in QUEUE_IDS}
    html = _get("/features/queues/active/?new_page=0;hide_options=1;queue=0;"
                "sort=queue_start;descending=0;refresh=60")
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")
    if "No calls found" in soup.get_text():
        return out
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        our_name = TELICO_TO_OUR.get(cells[0].get_text(strip=True))
        if not our_name:
            continue
        secs = _parse_duration(cells[-1].get_text(strip=True))
        out[our_name]["waiting"] += 1
        if secs > out[our_name]["longest_wait_seconds"]:
            out[our_name]["longest_wait_seconds"] = secs
    return out


def fetch_queue_members(queue_id: int) -> list[dict]:
    html = _get(f"/features/queues/edit/?id={queue_id}")
    if not html:
        logger.warning("Queue %s: no HTML", queue_id)
        return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    seen = set()
    for sel in soup.find_all("select"):
        name = sel.get("name") or ""
        m = re.match(r"available_(\d+)", name)
        if not m:
            continue
        dest_id = m.group(1)
        row = sel.find_parent("tr")
        if not row:
            continue
        row_text = row.get_text(" ", strip=True)
        em = re.search(r"Telephone line:\s*(\d+)\s+(.+?)(?:\s+Available|\s+Unavailable)?$",
                       row_text)
        if not em:
            em = re.search(r"Telephone line:\s*(\d+)\s+(.+)", row_text)
        if not em:
            continue
        ext = em.group(1)
        if ext in seen:
            continue
        name_txt = em.group(2).strip()
        name_txt = re.sub(r"\s+", " ", name_txt).strip(" -")
        opt = sel.find("option", selected=True) or sel.find("option")
        available = False
        if opt:
            v = (opt.get("value") or opt.get_text(strip=True)).lower()
            available = v == "1"
        out.append({"extension": ext, "name": name_txt,
                    "available": available, "dest_id": dest_id})
        seen.add(ext)
    logger.debug("Queue %s: parsed %d members, %d available",
                 queue_id, len(out), sum(1 for x in out if x["available"]))
    return out


def fetch_phone_lines() -> list[dict]:
    """
    Fetch live phone states from /panel/state/ (XML), not /panel/ HTML.
    The HTML page is a static shell; the XML endpoint updates live.

    States observed:
      Idle:      call_direction=""         call_party=""
      Ringing:   call_direction="internal" call_party="Withheld"
      ANSWERED:  call_direction="internal" call_party contains "Play sound"
    """
    xml = _get("/panel/state/")
    if not xml:
        return []

    try:
        root = ET.fromstring(xml)
    except ET.ParseError as e:
        logger.error("Failed to parse /panel/state/ XML: %s", e)
        return []

    out = []
    for p in root.findall(".//phones/phone"):
        ext = p.get("name") or p.get("dnumber")
        if not ext:
            continue

        direction = (p.get("call_direction") or "").strip().lower()
        party = (p.get("call_party") or "").strip().lower()
        label = p.get("label") or p.get("description") or ""
        registered = p.get("registered") == "1"

        is_ringing = bool(direction) and "play sound" not in party
        is_answered = "play sound" in party

        out.append({
            "extension": ext,
            "name": label,
            "registered": registered,
            "on_call": bool(direction),
            "answered": is_answered,
            "call_direction": direction,
            "call_party": party,
        })

    logger.debug("Fetched %d phones from /panel/state/", len(out))
    return out


def fetch_lines_activity(extension: str) -> dict:
    """Return strict answered state for one extension from the live XML feed."""
    for r in fetch_phone_lines():
        if r["extension"] == extension:
            return {
                "found": True,
                "on_call": r.get("on_call", False),
                "answered": r.get("answered", False),
                "registered": r["registered"],
                "call_direction": r.get("call_direction", ""),
                "call_party": r.get("call_party", ""),
            }
    return {
        "found": False,
        "on_call": False,
        "answered": False,
        "registered": False,
        "call_direction": "",
        "call_party": "",
    }


def set_queue_membership(queue_id: int, extension: str, available: bool) -> dict:
    s = _session_get()
    edit_html = _get(f"/features/queues/edit/?id={queue_id}")
    if not edit_html:
        return {"ok": False, "reason": "edit page fetch failed"}
    soup = BeautifulSoup(edit_html, "html.parser")
    target_dest_id = None
    for sel in soup.find_all("select"):
        name = sel.get("name") or ""
        m = re.match(r"available_(\d+)", name)
        if not m:
            continue
        row = sel.find_parent("tr")
        if not row:
            continue
        row_text = row.get_text(" ", strip=True)
        if not re.search(rf"Telephone line:\s*{re.escape(extension)}\b", row_text):
            continue
        target_dest_id = m.group(1)
        break
    if not target_dest_id:
        # Log a snippet to help diagnose the parser
        logger.warning("Queue %s: ext %s NOT FOUND in edit page. First 800 chars: %s",
                       queue_id, extension, edit_html[:800])
        return {"ok": False, "reason": f"extension {extension} not in queue {queue_id}"}

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
        logger.error("POST failed: %s", e)
        return {"ok": False, "reason": str(e)}


def call_extension(phone_number: str, message_path: str = "") -> dict:
    """Ring an extension via /utils/play/."""
    s = _session_get()
    url = f"{BASE_URL}/utils/play/"
    params = {
        "feature": "queues",
        "id": message_path,
        "number": phone_number,
        "callerid": CALLER_ID,
        "callername": CALLER_ID,
    }
    try:
        r = s.get(url, params=params, timeout=TIMEOUT, allow_redirects=True)
        ok = r.status_code in (200, 302)
        logger.info("Call to %s -> %s (%s)", phone_number, r.url, r.status_code)
        return {
            "ok": ok, "status": r.status_code, "url": r.url, "sent": params,
            "response_snippet": r.text[:300] if not ok else "",
        }
    except Exception as e:
        logger.error("Call to %s failed: %s", phone_number, e)
        return {"ok": False, "reason": str(e), "sent": params}


def fetch_all() -> dict:
    if not is_configured():
        return {"ok": False, "reason": "not configured"}
    t0 = time.time()
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
    logger.debug("Telico poll OK in %.2fs — queues=%s phones=%d",
                 time.time() - t0,
                 {k: v["waiting"] for k, v in queues.items()},
                 len(phones))
    return {"ok": True, "queues": queues, "phones": phones}