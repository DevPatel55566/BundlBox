import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form
from fastapi.responses import HTMLResponse, RedirectResponse
import uvicorn
import json
import reports

from database import seed
from utils import make_session, get_session, drop_session, home_for_role, hub, snapshot_full, telico_sync_loop, break_expiry_loop, ensure_data_fresh, ensure_schedule_fresh
import telico_client
from utils import _apply_telico_snapshot
import wallboard
import manager
import agent
import sms
import call_history

# Import the background task for stop listener
from stop_listener import stop_listener_loop

ON_VERCEL = bool(os.getenv("VERCEL"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    seed()

    # Vercel serverless functions freeze between requests, so long-running
    # background loops (Telico sync, break expiry, STOP listener) cannot run
    # there. They only start on a long-lived host (e.g. local / Render).
    # On Vercel use GET /api/cron/sync (Vercel Cron) for a single sync pass.
    tasks = []
    if not ON_VERCEL:
        tasks = [
            asyncio.create_task(telico_sync_loop()),
            asyncio.create_task(break_expiry_loop()),
            asyncio.create_task(stop_listener_loop()),
        ]

    yield

    for task in tasks:
        task.cancel()

app = FastAPI(lifespan=lifespan)

# The Render deployment (bundlbox.onrender.com) is retired: everything now
# runs on Vercel (bundlbox.vercel.app). Render sets RENDER=true on its own
# services, Vercel does not, so this redirect only ever fires on Render —
# on Vercel (and locally) the app serves normally. Same path and query are
# kept, so a Render link to /wallboard lands on Vercel's /wallboard.
@app.middleware("http")
async def _render_to_vercel_redirect(request, call_next):
    if os.getenv("RENDER") == "true":
        dest = "https://bundlbox.vercel.app" + request.url.path
        if request.url.query:
            dest += "?" + request.url.query
        return RedirectResponse(dest, status_code=301)
    return await call_next(request)

# Include Routers
app.include_router(wallboard.router)
app.include_router(manager.router)
app.include_router(agent.router)
app.include_router(sms.router)
app.include_router(reports.router)
app.include_router(call_history.router)

LOGIN_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');
* { box-sizing: border-box; margin: 0; padding: 0; }
body { height: 100vh; background: #0b1120; color: #f1f5f9; font-family:'Space Grotesk',system-ui,sans-serif; display: flex; align-items: center; justify-content: center; }
.login-wrap { max-width: 400px; width: 100%; padding: 32px; background: #0f172a; border: 1px solid #1e293b; border-radius: 16px; box-shadow: 0 4px 12px rgba(0,0,0,0.4); }
.login-wrap h1 { font-size: 26px; margin-bottom: 8px; color: #f1f5f9; }
.login-wrap p { color: #94a3b8; font-size: 15px; margin-bottom: 24px; }
.login-wrap input { width: 100%; padding: 14px; font-size: 16px; border-radius: 10px; border: 1px solid #334155; background: #1e293b; color: #f1f5f9; margin-bottom: 12px; outline: none; }
.login-wrap button { width: 100%; padding: 14px; border-radius: 10px; background: #22d3ee; color: #0b1120; font-weight: 700; font-size: 17px; cursor: pointer; border: none; }
.login-error { color: #f59e0b; background: #1e293b; padding: 10px 14px; border-radius: 8px; font-size: 15px; margin-bottom: 14px; }
"""

@app.get("/api/health")
def api_health():
    return {"ok": True, "vercel": ON_VERCEL, "telicoConfigured": telico_client.is_configured()}

@app.get("/api/snapshot")
def api_snapshot(request: Request):
    # HTTP fallback for /ws: Vercel serverless cannot hold WebSockets, so the
    # wallboard / manager / agent pages poll this every 5s instead.
    if not get_session(request):
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="not signed in")
    # Fresh Vercel instances start with an empty DB; pull Telico data now
    # instead of waiting for the daily cron sync, so the wallboard isn't blank.
    ensure_data_fresh()
    # Same for the schedule: a fresh instance's /tmp DB only has the defaults,
    # so read the real hours back from Telico instead of showing stale ones.
    ensure_schedule_fresh()
    return snapshot_full()

@app.get("/api/cron/sync")
async def api_cron_sync(request: Request):
    # Single Telico sync pass, for Vercel Cron (or a manual hit). Protected
    # by CRON_SECRET when set; without it, only allow it when Telico is
    # configured, matching the old loop's behaviour.
    secret = os.getenv("CRON_SECRET", "")
    if secret and request.headers.get("Authorization") != f"Bearer {secret}":
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="bad cron secret")
    if not telico_client.is_configured():
        return {"ok": False, "reason": "telico not configured"}
    try:
        snap = await asyncio.to_thread(telico_client.fetch_all)
        _apply_telico_snapshot(snap)
        await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "reason": str(e)}

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
    body = f"""<div class="login-wrap"><h1>Sign in</h1><p>Bundl Box Control Center</p>{err}<form method="post" action="/login"><input name="username" placeholder="Username" autofocus><input name="password" type="password" placeholder="Password"><button type="submit">Sign in</button></form><div style="text-align:center; margin-top:20px; font-size:13px; color:#94a3b8;"></b></div></div>"""
    return f"""<!doctype html><html><head><title>Login · Bundl Box</title><style>{LOGIN_CSS}</style></head><body>{body}</body></html>"""

@app.post("/login")
async def page_login_submit(request: Request):
    from database import connect
    form = await request.form()
    username = (form.get("username") or "").strip()
    password = form.get("password") or ""
    
    conn = connect()
    row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if row and row["password"] == password: 
        tok = make_session(row["role"], row["agent_id"])
        resp = RedirectResponse(home_for_role(row["role"]), status_code=302)
        resp.set_cookie("cc_session", tok, httponly=True, samesite="lax", max_age=12*3600)
        return resp

    return RedirectResponse("/login?error=1", status_code=302)

@app.get("/logout")
def page_logout(request: Request):
    drop_session(request)
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("cc_session")
    return resp

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    await hub.join(ws)
    try:
        await ws.send_text(json.dumps({"type": "snapshot", "data": snapshot_full()}))
        while True: await ws.receive_text()
    except WebSocketDisconnect: pass
    finally: await hub.leave(ws)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 