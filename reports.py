import logging
from fastapi import APIRouter, Request, HTTPException
from database import connect
from utils import get_session

log = logging.getLogger("app")
router = APIRouter()

@router.get("/api/reports/metrics")
async def api_get_reports(request: Request, start: str, end: str):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": 
        raise HTTPException(403, "manager only")

    c = connect()
    
    query = """
        SELECT 
            queue,
            SUM(count) as count,
            SUM(missed) as missed,
            SUM(attempts) as attempts,
            SUM(outbound_answered) as outbound_answered,
            SUM(outbound_attempts) as outbound_attempts,
            SUM(talk_secs) as talk_secs,
            SUM(hold_secs) as hold_secs,
            SUM(outbound_talk_secs) as outbound_talk_secs
        FROM calls_answered
        WHERE date >= ? AND date <= ?
        GROUP BY queue
    """
    
    rows = c.execute(query, (start, end)).fetchall()
    
    metrics = []
    for r in rows:
        metrics.append({
            "queue": r["queue"],
            "count": r["count"] or 0,
            "missed": r["missed"] or 0,
            "attempts": r["attempts"] or 0,
            "outbound_answered": r["outbound_answered"] or 0,
            "outbound_attempts": r["outbound_attempts"] or 0,
            "talk_secs": r["talk_secs"] or 0,
            "hold_secs": r["hold_secs"] or 0,
            "outbound_talk_secs": r["outbound_talk_secs"] or 0,
            "waiting": 0, 
            "longestWaitSeconds": 0
        })

    return {"ok": True, "metrics": metrics}