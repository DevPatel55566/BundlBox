from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from utils import get_session, home_for_role
import json

router = APIRouter()

AGENT_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; background: #f3f4f6; color: #1f2937; font-family:'Space Grotesk',system-ui,sans-serif; font-size:15px; }
a { color:inherit; text-decoration:none; }
button { font-family:inherit; cursor:pointer; border:none; background:none; color:inherit; }
.mono { font-family:'JetBrains Mono',ui-monospace,monospace; }

.topbar { display:flex; align-items:center; justify-content:space-between; gap:20px; padding:10px 20px; background:#ffffff; border-bottom:1px solid #e5e7eb; flex-shrink:0; }
.topbar-left { display:flex; align-items:center; gap:18px; }
.brand { display:flex; align-items:center; gap:10px; }
.brand-logo { width:32px; height:32px; border-radius:8px; background:#111827; display:flex; align-items:center; justify-content:center; font-weight:800; font-size:12px; color:#ffffff; }
.brand-title { font-weight:600; font-size:16px; color:#111827; }
.top-nav { display:flex; gap:2px; }
.top-nav a { padding:8px 14px; font-size:14px; font-weight:500; color:#6b7280; border-radius:8px; transition:all .15s; }
.top-nav a:hover { color:#111827; }
.top-nav a.active { color:#111827; background:#f3f4f6; }

.user-select { padding:7px 12px; background:#f9fafb; border:1px solid #d1d5db; border-radius:10px; font-size:13.5px; color:#111827; cursor:pointer; }
.clock-block { text-align:right; padding-left:10px; }
.clock { font-family:'JetBrains Mono',monospace; font-size:22px; font-weight:600; color:#111827; }
.clock-date { color:#6b7280; font-size:12px; font-family:'JetBrains Mono',monospace; }

.view { flex:1; overflow:auto; padding:22px 24px 28px; display: flex; flex-direction: column; align-items: center; }
.agent-layout { width: 100%; max-width: 1400px; display: flex; flex-direction: column; gap: 24px; }

/* Top Panel (Status + Actions) */
.top-panel { display: flex; gap: 24px; align-items: stretch; }
@media (max-width: 900px) { .top-panel { flex-direction: column; } }

.profile-card { flex: 1; background: #ffffff; border-radius: 12px; border: 1px solid #e5e7eb; padding: 24px; display: flex; flex-direction: column; justify-content: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
.agent-select { width: 100%; padding: 12px 16px; font-size: 16px; border-radius: 8px; border: 1px solid #d1d5db; background: #ffffff; color: #111827; font-family: inherit; outline: none; appearance: none; margin-bottom: 16px; background-image: url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 6'%3e%3cpath fill='%236b7280' d='M0 0l5 6 5-6H0z'/%3e%3c/svg%3e"); background-repeat: no-repeat; background-position: right 16px center; background-size: 12px 8px; cursor: pointer; transition: border-color 0.2s; }|
.agent-select:focus { border-color: #111827; }
.agent-hello .name { font-size: 32px; font-weight: 700; color: #111827; }
.agent-hello .sub { font-size: 15px; color: #6b7280; font-family: 'JetBrains Mono', monospace; margin-top: 4px; }

.status-card { flex: 1.5; padding: 24px; text-align: center; border-radius: 12px; background: #ffffff; border: 2px solid #e5e7eb; position: relative; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.05); transition: border-color 0.3s; display: flex; flex-direction: column; justify-content: center;}
.status-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 4px; background: #111827; }
.status-card.on-break { border-color: #ea580c; }
.status-card.on-break::before { background: #ea580c; }
.status-card.on-break .big { color: #ea580c; }
.status-card .lbl { font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.15em; font-weight: 700; margin-bottom: 6px; }
.status-card .big { font-size: 32px; font-weight: 700; color: #111827; margin-bottom: 8px; }
.status-card .timers { display: flex; justify-content: center; gap: 40px; margin-top: 16px; border-top: 1px solid #e5e7eb; padding-top: 16px; }
.status-card .timer-item { display: flex; flex-direction: column; align-items: center; }
.status-card .timer-lbl { font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.1em; font-weight: 700; margin-bottom: 4px; }
.status-card .time { font-family: 'JetBrains Mono', monospace; font-size: 28px; font-weight: 700; color: #111827; line-height: 1; }
.status-card .time.orange { color: #ea580c; }

.actions-card { flex: 1.5; background: #ffffff; border-radius: 12px; border: 1px solid #e5e7eb; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); display: flex; flex-direction: column; gap: 12px; }
.action-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.action-btn { padding: 16px 12px; border-radius: 8px; font-size: 15px; font-weight: 700; border: 2px solid #e5e7eb; background: #ffffff; color: #111827; display: flex; align-items: center; justify-content: center; gap: 10px; transition: all 0.2s; box-shadow: 0 1px 3px rgba(0,0,0,0.05); width: 100%; }
.action-btn:hover:not([disabled]) { background: #f9fafb; border-color: #d1d5db; }
.action-btn:active:not([disabled]) { transform: scale(0.98); }
.action-btn[disabled] { opacity: 0.3; cursor: not-allowed; border-color: transparent; background: transparent; box-shadow: none; }
.btn-clockin { border-color: #10b981; color: #10b981; background: rgba(16,185,129,0.05); }
.btn-clockin:hover:not([disabled]) { background: rgba(16,185,129,0.1); border-color: #10b981; }
.btn-clockout { color: #6b7280; }
.btn-break { border-color: #ea580c; color: #ea580c; background: rgba(234,88,12,0.05); }
.btn-break:hover:not([disabled]) { background: rgba(234,88,12,0.1); border-color: #ea580c; }
.btn-login { border-color: #111827; color: #111827; background: rgba(17,24,39,0.05); }
.btn-login:hover:not([disabled]) { background: rgba(17,24,39,0.1); border-color: #111827; }

/* Manager Style Table */
.section { background:#ffffff; border:1px solid #e5e7eb; border-radius:12px; padding:24px; box-shadow:0 1px 3px rgba(0,0,0,0.05); }
.section-title { font-size:20px; font-weight:700; color:#111827; margin-bottom:16px; }

table { width:100%; border-collapse:collapse; background: #ffffff; border-radius: 8px; overflow: hidden; }
thead th { background:#f9fafb; text-align:left; font-size:13px; color:#6b7280; padding:14px 16px; border-bottom:1px solid #e5e7eb; font-weight:600; text-transform: uppercase; letter-spacing: 0.05em;}
tbody td { padding:16px 16px; border-bottom:1px solid #e5e7eb; font-size:15px; color:#111827; }
tbody tr:last-child td { border-bottom:none; }
tbody tr:hover { background: #f9fafb; }

.agents-table th { padding: 16px; font-size: 14px; }
.agents-table td { padding: 18px 16px; font-size: 15.5px; }
.agent-name-cell { font-weight: 700; color: #111827; display: flex; align-items: center; gap: 12px; }
.avatar { width: 32px; height: 32px; border-radius: 50%; background: #e5e7eb; display: flex; align-items: center; justify-content: center; font-size: 14px; color: #6b7280; font-weight: 700; flex-shrink: 0; }

.status-pill { display: inline-block; padding: 6px 16px; border-radius: 20px; font-size: 14px; font-weight: 700; border: 1px solid transparent; }
.st-available { color: #166534; border-color: #bbf7d0; background-color: #dcfce7; }
.st-break { color: #ea580c; border-color: #fed7aa; background-color: #ffedd5; }
.st-admin { color: #a855f7; border-color: #e9d5ff; background-color: #f3e8ff; }
.st-offline { color: #6b7280; border-color: #e5e7eb; background-color: #f3f4f6; }
.st-outbound { color: #1d4ed8; border-color: #bfdbfe; background-color: #dbeafe; }

/* Overlay */
.alert-overlay { position: fixed; inset: 0; z-index: 9999; background: rgba(243,244,246,0.95); display: none; align-items: center; justify-content: center; padding: 24px; text-align: center; backdrop-filter: blur(4px); }
.alert-overlay.show { display: flex; }
.alert-overlay .inner { max-width: 480px; width: 100%; background: #ffffff; border: 2px solid #ea580c; border-radius: 24px; padding: 32px 24px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); }
.alert-overlay .icon { font-size: 64px; line-height: 1; margin-bottom: 12px; }
.alert-overlay .title { font-size: 28px; font-weight: 800; color: #ea580c; margin-bottom: 12px; }
.alert-overlay .msg { font-size: 16px; color: #1f2937; line-height: 1.6; margin-bottom: 24px; }
.alert-overlay .btn-dismiss { background: #ea580c; color: #ffffff; padding: 18px 32px; border-radius: 12px; font-size: 18px; font-weight: 800; letter-spacing: .03em; width: 100%; border: none; cursor: pointer; }
.alert-overlay .btn-dismiss[disabled] { opacity: 0.6; cursor: wait; }
"""

AGENT_BODY = """
<div class="agent-layout">
  <div class="top-panel">
    <!-- Profile -->
    <div class="profile-card">
      <select class="agent-select" id="agent-pick"></select>
      <div id="agent-hello" class="agent-hello" style="text-align: center;"></div>
    </div>

    <!-- Status -->
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

    <!-- Actions -->
    <div class="actions-card" id="agent-actions">
      <div class="action-grid">
        <button type="button" id="btn-clockin" class="action-btn btn-clockin" onclick="clockIn()">
          <span style="font-size: 18px;">🟢</span> Clock In
        </button>
        <button type="button" id="btn-clockout" class="action-btn btn-clockout" onclick="clockOut()">
          <span style="font-size: 18px;">🔴</span> Clock Out
        </button>
        <button type="button" id="btn-break" class="action-btn btn-break" onclick="startMyBreak()">
          <span style="font-size: 18px;">☕</span> Start Break
        </button>
        <button type="button" id="btn-login" class="action-btn btn-login" onclick="logBackIn()">
          <span style="font-size: 18px;">✓</span> Log Back In
        </button>
      </div>
    </div>
  </div>

  <!-- Table -->
  <div class="section">
    <div class="section-title">Telephone Lines</div>
    <div style="border:1px solid #e5e7eb; border-radius:8px; overflow:hidden;">
        <table class="agents-table" style="margin:0;">
        <thead style="background:#f9fafb;"><tr><th>Agent</th><th>Ext</th><th>Login</th><th>Logout</th><th>Status</th><th>Status Since</th><th>Duration</th></tr></thead>
        <tbody id="lines-tbody"></tbody>
        </table>
    </div>
  </div>
</div>
"""

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
      new Notification('Break is over', { body: 'Your desk phone is ringing. Answer it, then tap OK in the app.', tag: 'cc-break-over', requireInteraction: true });
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
    state.agents.map(a=>`<option value="${a.id}" ${a.id===myAgentId?'selected':''}>${a.name} (${a.phone||''})</option>`).join('');
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
  
  if (!a) {
    document.getElementById('agent-hello').innerHTML = '<div class="name">Pick your name</div><div class="sub">This phone will remember it</div>';
    document.querySelector('.status-card').style.opacity = '0.3';
    document.querySelector('.actions-card').style.opacity = '0.3';
    return;
  }
  
  document.querySelector('.status-card').style.opacity = '1';
  document.querySelector('.actions-card').style.opacity = '1';
  
  document.getElementById('agent-hello').innerHTML = `<div class="name">${a.name}</div><div class="sub">ext ${a.phone||'—'}</div>`;
  const sc = document.getElementById('status-card');
  sc.querySelector('.big').textContent = a.status;
  sc.querySelector('.big').className = 'big ' + (STATUS_CLASS[a.status]||'');
  
  const elapsed = (Date.now() - (state.lastUpdate || Date.now())) / 1000;
  let shiftSecs = a.shiftDurationSeconds || 0;
  let breakSecs = a.totalBreakSeconds || 0;
  
  if (a.status !== 'Logout / Offline' && a.status !== 'Break / Lunch') shiftSecs += elapsed;
  if (a.status === 'Break / Lunch') breakSecs += elapsed;
  
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
  try { await api(`/api/agents/${myAgentId}/status`, {status: 'Break / Lunch'}); } catch (e) {}
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
  try { await api(`/api/agents/${myAgentId}/status`, {status: 'Login / Available'}); } catch (e) {}
  finally { pendingStatus = null; }
  render();
};
window.clockOut = async () => {
  if (!myAgentId || pendingStatus !== null) return;
  pendingStatus = 'Logout / Offline'; render();
  try { await api(`/api/agents/${myAgentId}/status`, {status: 'Logout / Offline'}); } catch (e) {}
  finally { pendingStatus = null; }
  render();
};

function renderLines(){
  const tb = document.getElementById('lines-tbody');
  if (!tb) return;

  if (!state.agents || state.agents.length === 0) {
      tb.innerHTML = '<tr><td colspan="7" style="text-align:center; color:#6b7280; padding:24px;">Loading lines...</td></tr>';
      return;
  }

  tb.innerHTML = state.agents.map((a, index) => {
    let badgeCls = 'st-offline';
    if (a.status === 'Login / Available') badgeCls = 'st-available';
    else if (a.status === 'Break / Lunch') badgeCls = 'st-break';
    else if (a.status === 'Admin / Wrap Up') badgeCls = 'st-admin';
    else if (a.status === 'Outbound') badgeCls = 'st-outbound';

    return `
    <tr>
      <td class="agent-name-cell">
        <div class="avatar">${index + 1}</div>
        ${a.name}
      </td>
      <td class="mono" style="color:#6b7280;">${a.phone || '—'}</td>
      <td class="mono" style="color:#6b7280;">${a.loginTime || '—'}</td>
      <td class="mono" style="color:#6b7280;">${a.logoutTime || '—'}</td>
      <td><span class="status-pill ${badgeCls}">${a.status}</span></td>
      <td class="mono">${fmtHHMM(a.statusSince)}</td>
      <td class="mono" style="font-weight: 700;">${fmtHMS(a.shiftDurationSeconds)}</td>
    </tr>
  `}).join('');
}

onUpdate(()=>{ render(); renderLines(); });
setInterval(() => { render(); renderLines(); }, 1000);
document.addEventListener('DOMContentLoaded', () => { render(); renderLines(); });
"""

@router.get("/agent", response_class=HTMLResponse)
def page_agent(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] not in ("manager", "agent"): return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    
    agent_id = sess.get("agent_id") if sess["role"] == "agent" else None
    script = AGENT_SCRIPT_TEMPLATE.replace("{agent_id}", str(agent_id) if agent_id else "null")
    
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Agent · Bundl Box</title>
<style>{AGENT_CSS}</style>
</head><body style="display:flex;flex-direction:column;min-height:100vh;">
<header class="topbar">
  <div class="topbar-left">
    <div class="brand"><div class="brand-logo">BB</div><div class="brand-title">Bundl Box</div></div>
    <nav class="top-nav">
      <a href="/wallboard">Wallboard</a>
      <a href="/manager">Manager</a>
      <a href="/agent" class="active">Agent View</a>
      <a href="/call-history">Call History</a>
      <a href="/sms">SMS</a>
    </nav>
  </div>
  <div style="display:flex; align-items:center; gap:20px;">
    <select class="user-select" onchange="if(this.value)location.href=this.value">
      <option>Agent</option>
      <option value="/logout">Log out</option>
    </select>
    <div class="clock-block">
      <div class="clock" id="clock">--:--:--</div>
      <div class="clock-date" id="clock-date">—</div>
    </div>
  </div>
</header>
<main class="view">
{AGENT_BODY}
</main>
<script>
const STATUSES = {json.dumps(STATUSES) if 'STATUSES' in globals() else '[]'}; const QUEUES = {json.dumps(QUEUES) if 'QUEUES' in globals() else '[]'};
const STATUS_CLASS = {{ 'Login / Available':'st-available','Logout / Offline':'st-offline','Admin / Wrap Up':'st-admin','Break / Lunch':'st-break','Outbound':'st-outbound' }};
const state = {{ agents: [], metrics: [], telicoConfigured: false, breakLimitSeconds: 1800, lastSync: null, lastUpdate: Date.now(), schedule: null, recentCalls: [] }};
const listeners = [];
function onUpdate(fn){{ listeners.push(fn); }}
function notify(){{ for (const fn of listeners) fn(state); }}
function startClock(){{ const t = document.getElementById('clock'); const d = document.getElementById('clock-date'); if (!t) return; const tick = () => {{ const n=new Date(); t.textContent=n.toLocaleTimeString('en-GB'); d.textContent=n.toLocaleDateString('en-GB',{{weekday:'long',day:'numeric',month:'short',year:'numeric'}}); }}; tick(); setInterval(tick,1000); }}
function fmtHMS(sec){{ sec = Math.max(0, Math.floor(Number(sec) || 0)); const h = Math.floor(sec / 3600); const m = Math.floor((sec % 3600) / 60); const s = sec % 60; return String(h).padStart(2,'0') + ':' + String(m).padStart(2,'0') + ':' + String(s).padStart(2,'0'); }}
function fmtHHMM(iso){{ if (!iso) return '--:--'; const d = new Date(iso); return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0'); }}
(function connectWS(){{ const proto = location.protocol === 'https:' ? 'wss' : 'ws'; const ws = new WebSocket(proto+'://'+location.host+'/ws'); ws.onclose = () => {{ setTimeout(connectWS, 2000); }}; ws.onmessage = (e) => {{ const msg = JSON.parse(e.data); if (msg.type === 'snapshot') {{ state.agents = msg.data.agents; state.metrics = msg.data.metrics; state.telicoConfigured = !!msg.data.telicoConfigured; state.lastSync = msg.data.lastSync || null; state.schedule = msg.data.schedule || null; state.recentCalls = msg.data.recentCalls || []; state.lastUpdate = Date.now(); notify(); }} else if (msg.type === 'break_expired') {{ if (typeof window.handleBreakExpired === 'function') window.handleBreakExpired(msg); }} }}; }})();
startClock();
{script}
</script></body></html>"""