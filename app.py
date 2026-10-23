import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form
from fastapi.responses import HTMLResponse, RedirectResponse
import uvicorn
import json
import reports
from database import seed
from utils import make_session, get_session, drop_session, home_for_role, hub, snapshot_full, telico_sync_loop, break_expiry_loop
import wallboard
import manager
import agent
import sms
import call_history

@asynccontextmanager
async def lifespan(app: FastAPI):
    seed()
    t1 = asyncio.create_task(telico_sync_loop())
    t2 = asyncio.create_task(break_expiry_loop())
    yield
    t1.cancel(); t2.cancel()

app = FastAPI(lifespan=lifespan)

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