from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from utils import get_session, home_for_role

router = APIRouter()

CALL_HISTORY_CSS = """
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

.layout { width: 100%; max-width: 1400px; }
.section { background:#ffffff; border:1px solid #e5e7eb; border-radius:12px; padding:24px; margin-bottom:24px; box-shadow:0 1px 3px rgba(0,0,0,0.05); }
.section-title { font-size:24px; font-weight:800; color:#111827; margin-bottom:20px; }
table { width:100%; border-collapse:collapse; background: #ffffff; border-radius: 8px; overflow: hidden; }
thead th { background:#f9fafb; text-align:left; font-size:13px; color:#6b7280; padding:14px 16px; border-bottom:1px solid #e5e7eb; font-weight:600; text-transform: uppercase; letter-spacing: 0.05em;}
tbody td { padding:16px 16px; border-bottom:1px solid #e5e7eb; font-size:15px; color:#111827; }
tbody tr:last-child td { border-bottom:none; }
tbody tr:hover { background: #f9fafb; }

.dir-badge { padding:4px 8px; border-radius:6px; font-size:12px; font-weight:700; }
.dir-in { background:#dcfce7; color:#166534; }
.dir-out { background:#dbeafe; color:#1d4ed8; }
.status-ans { color:#166534; font-weight:600; }
.status-miss { color:#ea580c; font-weight:600; }
"""

@router.get("/call-history", response_class=HTMLResponse)
def page_call_history(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] not in ("manager", "wallboard"): return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    
    body = """
    <div class="section">
      <div class="section-title">Call History (Today)</div>
      <div style="border:1px solid #e5e7eb; border-radius:8px; overflow:hidden;">
          <table style="margin:0;">
          <thead style="background:#f9fafb;"><tr><th>Time</th><th>Direction</th><th>Queue</th><th>Source</th><th>Destination</th><th>Status</th><th>Duration</th></tr></thead>
          <tbody id="history-tbody">
              <tr><td colspan="7" style="text-align:center; color:#6b7280; padding:24px;">Loading call history...</td></tr>
          </tbody>
          </table>
      </div>
    </div>
    """
    
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Call History · Bundl Box</title>
<style>{CALL_HISTORY_CSS}</style>
</head><body style="display:flex;flex-direction:column;min-height:100vh;">
<header class="topbar">
  <div class="topbar-left">
    <div class="brand"><div class="brand-logo">BB</div><div class="brand-title">Bundl Box</div></div>
    <nav class="top-nav">
      <a href="/wallboard">Wallboard</a>
      <a href="/manager">Manager</a>
      <a href="/agent">Agent View</a>
      <a href="/call-history" class="active">Call History</a>
      <a href="/sms">SMS</a>
    </nav>
  </div>
  <div style="display:flex; align-items:center; gap:20px;">
    <select class="user-select" onchange="if(this.value)location.href=this.value">
      <option>Manager</option>
      <option value="/logout">Log out</option>
    </select>
    <div class="clock-block">
      <div class="clock" id="clock">--:--:--</div>
      <div class="clock-date" id="clock-date">—</div>
    </div>
  </div>
</header>
<main class="view">
<div class="layout">
{body}
</div>
</main>
<script>
const state = {{ recentCalls: [] }};
const listeners = [];
function onUpdate(fn){{ listeners.push(fn); }}
function notify(){{ for (const fn of listeners) fn(state); }}
function startClock(){{ const t = document.getElementById('clock'); const d = document.getElementById('clock-date'); if (!t) return; const tick = () => {{ const n=new Date(); t.textContent=n.toLocaleTimeString('en-GB'); d.textContent=n.toLocaleDateString('en-GB',{{weekday:'long',day:'numeric',month:'short',year:'numeric'}}); }}; tick(); setInterval(tick,1000); }}
function fmtHMS(sec){{ sec = Math.max(0, Math.floor(Number(sec) || 0)); const h = Math.floor(sec / 3600); const m = Math.floor((sec % 3600) / 60); const s = sec % 60; return String(h).padStart(2,'0') + ':' + String(m).padStart(2,'0') + ':' + String(s).padStart(2,'0'); }}
(function connectWS(){{ const proto = location.protocol === 'https:' ? 'wss' : 'ws'; const ws = new WebSocket(proto+'://'+location.host+'/ws'); ws.onclose = () => {{ setTimeout(connectWS, 2000); }}; ws.onmessage = (e) => {{ const msg = JSON.parse(e.data); if (msg.type === 'snapshot') {{ state.recentCalls = msg.data.recentCalls || []; notify(); }} }}; }})();
startClock();

function renderHistory(){{
  const tb = document.getElementById('history-tbody');
  if (!tb) return;

  if (!state.recentCalls || state.recentCalls.length === 0) {{
      tb.innerHTML = '<tr><td colspan="7" style="text-align:center; color:#6b7280; padding:24px;">No calls logged today.</td></tr>';
      return;
  }}

  tb.innerHTML = state.recentCalls.map(c => {{
    const isOutbound = c.direction.toLowerCase() === 'outbound';
    const dirBadge = isOutbound 
        ? '<span class="dir-badge dir-out">OUTBOUND</span>'
        : '<span class="dir-badge dir-in">INBOUND</span>';
    
    const isAnswer = c.status === 'Answer';
    const statusBadge = isAnswer
        ? '<span class="status-ans">Answer</span>'
        : `<span class="status-miss">${{c.status}}</span>`;

    return `
      <tr>
        <td class="mono" style="color:#6b7280;">${{c.time.split(' ')[1] || c.time}}</td>
        <td>${{dirBadge}}</td>
        <td style="font-weight:600;">${{c.queue}}</td>
        <td class="mono">${{c.source}}</td>
        <td class="mono" style="font-weight:600;">${{c.destination}}</td>
        <td>${{statusBadge}}</td>
        <td class="mono">${{fmtHMS(c.duration)}}</td>
      </tr>
    `;
  }}).join('');
}}

onUpdate(renderHistory);
document.addEventListener('DOMContentLoaded', renderHistory);
</script></body></html>"""