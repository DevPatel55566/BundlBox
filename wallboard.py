from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from utils import get_session, home_for_role
import json

router = APIRouter()

WALLBOARD_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; background: #0b1120; color: #f1f5f9; font-family:'Space Grotesk',system-ui,sans-serif; font-size:15px; }
.wb-top { display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 1px solid #1e293b; padding-bottom: 20px; margin-bottom: 30px; }
.wb-brand { display: flex; align-items: center; gap: 16px; }
.wb-brand-text { display: flex; flex-direction: column; }
.wb-brand-live { color: #22d3ee; font-size: 14px; font-weight: 700; letter-spacing: 0.15em; text-transform: uppercase; }
.wb-brand-title { color: #f1f5f9; font-size: 56px; font-weight: 700; letter-spacing: 0.02em; text-transform: uppercase; }
.wb-time-block { text-align: right; }
.wb-time { color: #f1f5f9; font-size: 64px; font-weight: 700; font-family: 'JetBrains Mono', monospace; line-height: 1; }
.wb-date { color: #94a3b8; font-size: 16px; letter-spacing: 0.1em; text-transform: uppercase; margin-top: 6px; font-weight: 600; }

.queues { display:grid; grid-template-columns:repeat(3,1fr); gap:24px; margin-bottom: 30px; }
@media (max-width: 1100px) { .queues { grid-template-columns:1fr; } }

.wb-card { background: #1e293b; border-radius: 8px; padding: 30px 36px; display: flex; flex-direction: column; position: relative; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.2); }
.wb-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 6px; }
.wb-card:nth-child(1)::before { background: #3b82f6; }
.wb-card:nth-child(2)::before { background: #f59e0b; }
.wb-card:nth-child(3)::before { background: #06b6d4; }

.wb-head { display: flex; justify-content: space-between; margin-bottom: 20px; }
.wb-q-id { color: #94a3b8; font-size: 18px; font-weight: 600; letter-spacing: 0.1em; }
.wb-range { color: #10b981; font-size: 16px; font-weight: 700; letter-spacing: 0.05em; }
.wb-title { font-size: 64px; font-weight: 700; color: #f1f5f9; margin-bottom: 30px; letter-spacing: -0.02em; }

.wb-big-stats { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 30px; }
.wb-stat-lbl { color: #94a3b8; font-size: 16px; font-weight: 600; letter-spacing: 0.08em; margin-bottom: 8px; }
.wb-stat-big { font-size: 120px; font-weight: 700; line-height: 0.9; font-family: 'JetBrains Mono', monospace; }
.wb-stat-big.white { color: #f1f5f9; }
.wb-stat-big.orange { color: #f59e0b; }

.wb-small-stats { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; border-top: 2px solid #334155; padding-top: 24px; }
.wb-stat-sm-val { font-size: 48px; font-weight: 600; color: #f1f5f9; font-family: 'JetBrains Mono', monospace; margin-top: 8px; }

.blink-red { animation: blinker 1s linear infinite; color: #ef4444 !important; }
@keyframes blinker { 50% { opacity: 0; } }

.wb-bottom { margin-top: 30px; padding-top: 20px; border-top: 1px solid #1e293b; display: flex; justify-content: space-between; align-items: center; }
.wb-all-queues { display: flex; align-items: center; gap: 16px; color: #94a3b8; font-size: 18px; font-weight: 600; letter-spacing: 0.1em; }
.wb-all-queues .count { font-size: 32px; color: #f1f5f9; font-family: 'JetBrains Mono', monospace; }
.wb-footer-right { display: flex; align-items: center; gap: 20px; }
.wb-live { display: flex; align-items: center; gap: 8px; color: #94a3b8; font-size: 16px; font-family: 'JetBrains Mono', monospace; }
.wb-live::before { content: ''; width: 10px; height: 10px; border-radius: 50%; background: #22d3ee; }
.wb-btn-group { display: flex; gap: 10px; }
.wb-btn { padding: 12px 20px; border: 1px solid #334155; border-radius: 4px; background: transparent; color: #f1f5f9; font-size: 16px; font-weight: 600; cursor: pointer; text-transform: uppercase; letter-spacing: 0.05em; }
.wb-btn:hover { background: #334155; }
"""

WALLBOARD_BODY = """
<div style="padding: 22px 24px 28px;">
  <div class="wb-top">
    <div class="wb-brand"><div style="width:64px;height:64px;background:linear-gradient(135deg, #22d3ee, #0891b2);border-radius:12px;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:24px;color:#fff;">BB</div>
      <div class="wb-brand-text"><div class="wb-brand-live">LIVE OPERATIONS</div><div class="wb-brand-title">Queue Wallboard</div></div>
    </div>
    <div class="wb-time-block"><div class="wb-time" id="wb-time">00:00:00 AM</div><div class="wb-date" id="wb-date">—</div></div>
  </div>
  <div class="queues" id="queues"></div>
  <div class="wb-bottom">
    <div class="wb-all-queues"><span>ALL QUEUES</span><span class="count" id="wb-total-waiting">0</span><span>CALLS HOLDING</span></div>
    <div class="wb-footer-right">
      <div class="wb-live">LIVE • Updated <span id="wb-updated">--:--:--</span></div>
      <div class="wb-btn-group">
        <button class="wb-btn" onclick="window.location.reload()">↻ REFRESH</button>
        <button class="wb-btn" onclick="location.href='/manager'">MANAGER VIEW</button>
        <button class="wb-btn" onclick="location.href='/logout'">SIGN OUT</button>
      </div>
    </div>
  </div>
</div>
"""

WALLBOARD_SCRIPT = r"""
function renderQueues(){
  const wrap = document.getElementById('queues');
  if (!wrap) return;

  const totalWaiting = state.metrics.reduce((acc, m) => acc + (m.waiting || 0), 0);
  const twEl = document.getElementById('wb-total-waiting');
  if (twEl) twEl.textContent = totalWaiting;

  const now = new Date();
  const timeEl = document.getElementById('wb-time');
  if (timeEl) timeEl.textContent = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  const dateEl = document.getElementById('wb-date');
  if (dateEl) dateEl.textContent = now.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'short', day: 'numeric' }).toUpperCase();
  
  const updEl = document.getElementById('wb-updated');
  if (updEl && state.lastSync && state.lastSync.at) updEl.textContent = new Date(state.lastSync.at).toLocaleTimeString();

  wrap.innerHTML = state.metrics.map(m => {
    const info = state.lastSync && state.lastSync.queues ? state.lastSync.queues[m.queue] : null;
    const avail = info ? info.available_count : 0;
    const oldestHold = m.waiting ? fmtWait(m.longestWaitSeconds) : '—';
    const waitClass = m.waiting > 0 ? 'blink-red' : '';

    // Use accurate backend totals
    const displayCount = m.count || 0;
    const displayMissed = m.missed || 0;

    return `
    <div class="wb-card">
      <div class="wb-head"><div class="wb-q-id">QUEUE ${m.queue_id || '---'}</div><div class="wb-range">IN RANGE</div></div>
      <div class="wb-title">${m.queue.replace(' Line', '')}</div>
      
      <div class="wb-big-stats">
        <div><div class="wb-stat-lbl">TOTAL TODAY</div><div class="wb-stat-big white">${displayCount}</div></div>
        <div><div class="wb-stat-lbl">MISSED TODAY</div><div class="wb-stat-big orange">${displayMissed}</div></div>
      </div>
      
      <div class="wb-small-stats">
        <div><div class="wb-stat-lbl">CALLS<br>HOLDING</div><div class="wb-stat-sm-val ${waitClass}">${m.waiting}</div></div>
        <div><div class="wb-stat-lbl">OLDEST HOLD</div><div class="wb-stat-sm-val">${oldestHold}</div></div>
        <div><div class="wb-stat-lbl">AGENTS<br>ONLINE</div><div class="wb-stat-sm-val">${avail}</div></div>
      </div>
    </div>`;
  }).join('');
}
onUpdate(renderQueues);
setInterval(renderQueues, 1000);
"""

@router.get("/wallboard", response_class=HTMLResponse)
def page_wallboard(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] not in ("manager", "wallboard"): return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Wallboard · Bundl Box</title>
<style>{WALLBOARD_CSS}</style>
</head><body>
{WALLBOARD_BODY}
<script>
const STATUSES = {json.dumps(STATUSES) if 'STATUSES' in globals() else '[]'}; const QUEUES = {json.dumps(QUEUES) if 'QUEUES' in globals() else '[]'};
const state = {{ agents: [], metrics: [], telicoConfigured: false, breakLimitSeconds: 1800, lastSync: null, lastUpdate: Date.now(), schedule: null, recentCalls: [] }};
const listeners = [];
function onUpdate(fn){{ listeners.push(fn); }}
function notify(){{ for (const fn of listeners) fn(state); }}
function fmtWait(sec){{ const m=Math.floor(sec/60), s=sec%60; return String(m).padStart(2,'0')+':'+String(s).padStart(2,'0'); }}
(function connectWS(){{ const proto = location.protocol === 'https:' ? 'wss' : 'ws'; const ws = new WebSocket(proto+'://'+location.host+'/ws'); ws.onclose = () => {{ setTimeout(connectWS, 2000); }}; ws.onmessage = (e) => {{ const msg = JSON.parse(e.data); if (msg.type === 'snapshot') {{ state.agents = msg.data.agents; state.metrics = msg.data.metrics; state.telicoConfigured = !!msg.data.telicoConfigured; state.lastSync = msg.data.lastSync || null; state.schedule = msg.data.schedule || null; state.recentCalls = msg.data.recentCalls || []; state.lastUpdate = Date.now(); notify(); }} }}; }})();
{WALLBOARD_SCRIPT}
</script></body></html>"""