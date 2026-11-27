import json
import asyncio
import time
import logging
from datetime import date as _date, datetime as _datetime, timedelta
from zoneinfo import ZoneInfo
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from database import connect
from utils import (
    get_session, home_for_role, QUEUES, STATUSES,
    _update_agent_status, _set_queue_flag, _pending_toggles,
    hub, snapshot_full, ScheduleBody, QueueBody, MassBody, StatusBody,
    _fire_call
)
import telico_client
import telico_exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s :: %(message)s",
)
logging.getLogger("telico_client").setLevel(logging.INFO)
logging.getLogger("telico_exceptions").setLevel(logging.INFO)
logging.getLogger("app").setLevel(logging.INFO)

log = logging.getLogger("app")
router = APIRouter()

MANAGER_CSS = """
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
.manager-layout { width: 100%; max-width: 1400px; display: flex; flex-direction: column; gap: 24px; }

/* Top Panel (Header style) */
.top-panel { display: flex; gap: 24px; align-items: stretch; }
@media (max-width: 900px) { .top-panel { flex-direction: column; } }

.wb-brand { flex: 1; background: #ffffff; border-radius: 12px; border: 1px solid #e5e7eb; padding: 24px; display: flex; flex-direction: column; justify-content: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
.wb-brand-text { display: flex; flex-direction: column; }
.wb-brand-live { color: #6b7280; font-size: 12px; font-weight: 700; letter-spacing: 0.15em; text-transform: uppercase; margin-bottom: 4px; }
.wb-brand-title { color: #111827; font-size: 42px; font-weight: 800; letter-spacing: 0.02em; text-transform: uppercase; }
.wb-brand-title span { color: #ea580c; }

/* Sections & Cards */
.section { background:#ffffff; border:1px solid #e5e7eb; border-radius:12px; padding:24px; box-shadow:0 1px 3px rgba(0,0,0,0.05); }
.section-title { font-size:20px; font-weight:700; color:#111827; margin-bottom:16px; }
.sub { color: #6b7280; font-size: 14px; margin-bottom: 16px; }

.btn-primary { background:#111827; color:#ffffff; padding:10px 20px; border-radius:8px; font-weight:600; font-size:14px; cursor:pointer; transition: background 0.2s; }
.btn-primary:hover { background:#374151; }
.btn-primary:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-outline { background:#ffffff; color:#111827; border:1px solid #d1d5db; padding:10px 20px; border-radius:8px; font-weight:600; font-size:14px; cursor:pointer; transition: all 0.2s; }
.btn-outline:hover { background:#f9fafb; border-color: #9ca3af; }

/* Hours Container */
.hours-container { display: grid; grid-template-columns: 1fr 1fr; gap: 40px; }
@media (max-width: 900px) { .hours-container { grid-template-columns: 1fr; } }
.days-list { display: flex; flex-direction: column; gap: 12px; margin-top: 20px; }
.day-row { display: flex; align-items: center; justify-content: space-between; padding-bottom: 12px; border-bottom: 1px solid #e5e7eb; }
.day-row .day-name { font-weight: 600; font-size: 14px; width: 150px; display: flex; align-items: center; gap: 8px; }
.day-row .time-inputs { display: flex; align-items: center; gap: 12px; }
.day-row input[type="time"] { padding: 8px; width: 110px; background:#ffffff; border: 1px solid #d1d5db; color:#111827; border-radius: 4px; }
.exceptions-list { margin-top: 20px; background:#ffffff; border: 1px solid #e5e7eb; border-radius: 8px; }
.exception-item { border-bottom: 1px solid #e5e7eb; padding: 18px 20px; display: flex; justify-content: space-between; align-items: center; }
.exception-item:last-child { border-bottom: none; }
.exception-name { font-weight: 700; color: #111827; font-size: 17px; margin-bottom: 4px; }
.exception-meta { font-size: 14px; color: #6b7280; }

/* Stats */
.stats-grid { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 24px; }
.stat-card { flex: 1 1 180px; background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 20px; display: flex; flex-direction: column; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
.stat-card.row { flex: 1 1 100%; flex-direction: row; justify-content: space-between; align-items: center; padding: 20px 32px; }
.stat-card.highlight { background: #1e293b; color: #ffffff; border-color: #1e293b; }
.stat-card.highlight .label, .stat-card.highlight .sub { color: #94a3b8; }
.stat-card.highlight .value { color: #ffffff; }
.stat-card .label { font-size: 12px; font-weight: 600; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
.stat-card .value { font-size: 36px; font-weight: 700; color: #111827; font-family: 'JetBrains Mono', monospace; line-height: 1; }
.stat-card .sub { font-size: 12px; color: #6b7280; margin-top: 8px; }

/* Charts */
.charts-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
@media (max-width: 1000px) { .charts-grid { grid-template-columns: 1fr; } }
.chart-box { background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 20px; height: 320px; position: relative; }
.chart-box.wide { grid-column: 1 / -1; height: 200px; }
.chart-box h3 { font-size: 16px; font-weight: 700; color: #111827; margin-bottom: 4px; }
.chart-box .sub { font-size: 12px; color: #6b7280; margin-bottom: 16px; }
.chart-box canvas { max-height: 220px; width: 100% !important; }
.chart-box.wide canvas { max-height: 120px; }

/* Table Style matching Agent */
table { width:100%; border-collapse:collapse; background: #ffffff; border-radius: 8px; overflow: hidden; }
thead th { background:#f9fafb; text-align:left; font-size:13px; color:#6b7280; padding:14px 16px; border-bottom:1px solid #e5e7eb; font-weight:600; text-transform: uppercase; letter-spacing: 0.05em;}
tbody td { padding:16px 16px; border-bottom:1px solid #e5e7eb; font-size:15px; color:#111827; }
tbody tr:last-child td { border-bottom:none; }
tbody tr:hover { background: #f9fafb; }

.agents-table th { padding: 16px; font-size: 14px; }
.agents-table td { padding: 18px 16px; font-size: 15.5px; }
.agent-name-cell { font-weight: 700; color: #111827; display: flex; align-items: center; gap: 12px; }
.avatar { width: 32px; height: 32px; border-radius: 50%; background: #e5e7eb; display: flex; align-items: center; justify-content: center; font-size: 14px; color: #6b7280; font-weight: 700; flex-shrink: 0; }

.status-pill-select { appearance: none; padding: 8px 32px 8px 16px; border-radius: 20px; font-size: 14px; font-weight: 700; border: 1px solid #d1d5db; cursor: pointer; background-color: #ffffff; background-image: url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 6'%3e%3cpath fill='%236b7280' d='M0 0l5 6 5-6H0z'/%3e%3c/svg%3e"); background-repeat: no-repeat; background-position: right 12px center; background-size: 10px 6px; transition: all 0.2s; }
.status-pill-select.st-available { color: #166534; border-color: #bbf7d0; background-color: #dcfce7; }
.status-pill-select.st-break { color: #ea580c; border-color: #fed7aa; background-color: #ffedd5; }
.status-pill-select.st-admin { color: #a855f7; border-color: #e9d5ff; background-color: #f3e8ff; }
.status-pill-select.st-offline { color: #6b7280; border-color: #e5e7eb; background-color: #f3f4f6; }
.status-pill-select.st-outbound { color: #1d4ed8; border-color: #bfdbfe; background-color: #dbeafe; }

/* Toggles & Buttons */
.switch { width:50px; height:28px; border-radius:14px; background:#e5e7eb; position:relative; cursor:pointer; transition:background .2s; display:inline-block; vertical-align:middle; padding:0; border:none; }
.switch::after { content:''; position:absolute; top:3px; left:3px; width:22px; height:22px; border-radius:50%; background:#fff; transition:left .2s; box-shadow: 0 1px 3px rgba(0,0,0,0.2); }
.switch.on { background:#111827; }
.switch.on::after { left:25px; }

.icon-btn { width:36px; height:36px; border-radius:8px; display:inline-flex; align-items:center; justify-content:center; background:#ffffff; border:1px solid #d1d5db; color:#6b7280; font-size:15px; margin-right:6px; cursor:pointer; transition: all 0.2s; }
.icon-btn:hover { background:#f3f4f6; color:#111827; border-color: #9ca3af; transform: translateY(-1px); }

input[type="text"], input[type="date"], select { background: #ffffff; border: 1px solid #d1d5db; color: #111827; padding: 10px; border-radius: 4px; font-family: inherit; font-size: 14px;}
.toast { position: fixed; left: 50%; bottom: 24px; transform: translateX(-50%); background: #111827; border: 1px solid #374151; padding: 12px 20px; border-radius: 8px; font-size: 15px; z-index: 999; box-shadow:0 4px 12px rgba(0,0,0,0.1); color: #ffffff; }
.toast.error { border-color: #ea580c; color: #ea580c; }
.toast.ok { border-color: #10b981; color: #10b981; }
"""

MANAGER_BODY = """
<div class="manager-layout">
  <div class="top-panel">
    <div class="wb-brand">
      <div class="wb-brand-text">
        <div class="wb-brand-live">OPERATIONS INTELLIGENCE</div>
        <div class="wb-brand-title">MANAGER <span>DESK</span></div>
      </div>
    </div>
  </div>

  <div class="section">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
        <div>
            <div style="font-size:12px; font-weight:700; color:#6b7280; letter-spacing:0.05em; text-transform:uppercase;">ACCOUNT CONTROLS</div>
            <div style="font-size:24px; font-weight:800; color:#111827;">Hours & closures</div>
        </div>
        <button class="btn-outline" onclick="window.location.reload()">↻ REFRESH SCHEDULE</button>
    </div>
    <div style="background:#f8fafc; padding:12px 20px; border-radius:6px; border:1px solid #e2e8f0; font-size:13px; color:#475569; margin-bottom:20px; display:flex; align-items:center; gap:8px;">
        <span>⏱</span> Times use America/Toronto. Changes update the live Enswitch account after validation.
    </div>

    <div class="hours-container">
        <div>
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div>
                    <div style="font-size:18px; font-weight:700; color:#111827;">Weekly hours</div>
                    <div style="font-size:13px; color:#6b7280;">The regular week — the same days every time. To change one date, use the box below.</div>
                </div>
                <button id="save-week-btn" class="btn-primary" onclick="saveWeek(this)" disabled>SAVE WEEK</button>
            </div>
            <div class="days-list" id="weekly-hours"></div>
            <div id="schedule-sync-status" style="margin-top:12px; font-size:13px; font-weight:600;"></div>

            <div style="margin-top:24px; background:#f9fafb; padding:20px; border-radius:8px; border:1px solid #e5e7eb;">
                <div style="font-size:16px; font-weight:700; color:#111827;">Change one date</div>
                <div style="font-size:13px; color:#6b7280; margin-bottom:12px;">Pick a date and set the hours for that date only. The regular week stays the same.</div>
                <div style="display:flex; gap:12px; flex-wrap:wrap; align-items:flex-end;">
                    <div style="display:flex; flex-direction:column;">
                        <label style="font-size:11px; font-weight:700; color:#6b7280; margin-bottom:4px;">DATE</label>
                        <input type="date" id="one_date" onchange="prefillOneDate()">
                    </div>
                    <div style="display:flex; flex-direction:column; justify-content:flex-end; padding-bottom:6px;">
                        <label style="font-size:13px; color:#111827; display:flex; align-items:center; gap:6px;"><input type="checkbox" id="one_open" checked> Open</label>
                    </div>
                    <div style="display:flex; flex-direction:column;">
                        <label style="font-size:11px; font-weight:700; color:#6b7280; margin-bottom:4px;">OPEN AT</label>
                        <input type="time" id="one_start" value="09:00">
                    </div>
                    <div style="display:flex; flex-direction:column;">
                        <label style="font-size:11px; font-weight:700; color:#6b7280; margin-bottom:4px;">CLOSE AT</label>
                        <input type="time" id="one_end" value="17:00">
                    </div>
                    <div style="flex:1; display:flex; flex-direction:column; min-width:160px;">
                        <label style="font-size:11px; font-weight:700; color:#6b7280; margin-bottom:4px;">NAME (OPTIONAL)</label>
                        <input type="text" id="one_name" placeholder="e.g. Labour Day" style="width:100%;">
                    </div>
                </div>
                <div id="one_summary" style="font-size:13px; color:#6b7280; margin-top:10px;"></div>
                <button id="save-one-date-btn" class="btn-primary" onclick="saveOneDate(this)" style="margin-top:12px;" disabled>SAVE THIS DATE</button>
            </div>
        </div>
        <div>
            <div style="font-size:18px; font-weight:700; color:#111827;">Scheduled exceptions</div>
            <div style="font-size:13px; color:#6b7280; margin-bottom:16px;">Dates that are different from the regular week.</div>
            <div class="exceptions-list" id="exceptions-list"></div>
        </div>
    </div>
  </div>

  <div class="section">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px; background:#f9fafb; padding:15px 20px; border-radius:8px; border:1px solid #e5e7eb;">
        <div style="display:flex; gap:10px; align-items:center;">
            <span style="font-size:12px; font-weight:700; color:#6b7280; letter-spacing:0.1em;">REPORTING RANGE</span>
            <button class="btn-primary" onclick="setRange('today')">TODAY</button>
            <button class="btn-outline" onclick="setRange('7')">LAST 7 DAYS</button>
            <button class="btn-outline" onclick="setRange('30')">LAST 30 DAYS</button>
        </div>
        <div style="display:flex; gap:10px; align-items:center;">
            <div style="display:flex; flex-direction:column;">
                <span style="font-size:10px; font-weight:700; color:#6b7280;">FROM</span>
                <input type="date" id="report-start">
            </div>
            <div style="display:flex; flex-direction:column;">
                <span style="font-size:10px; font-weight:700; color:#6b7280;">TO</span>
                <input type="date" id="report-end">
            </div>
            <button class="btn-primary" style="margin-top:14px;" onclick="applyCustomRange()">APPLY RANGE</button>
        </div>
    </div>

    <div class="stats-grid" id="live-stats-cards"></div>

    <div class="charts-grid">
      <div class="chart-box">
        <h3>Volume by queue</h3>
        <div class="sub">Offered / answered / abandoned</div>
        <canvas id="chartVolume"></canvas>
      </div>
      <div class="chart-box">
        <h3>Talk and hold time</h3>
        <div class="sub">Seconds, queue averages</div>
        <canvas id="chartTalk"></canvas>
      </div>
    </div>

    <div class="chart-box wide">
      <h3>Calls Answered by Agent</h3>
      <div class="sub">Total answered calls per employee (Today only)</div>
      <canvas id="chartAttempts"></canvas>
    </div>
  </div>

  <div class="section">
    <div class="section-title" style="font-size:24px; font-weight:800; margin-bottom:20px;">Outbound Reports</div>
    <div class="stats-grid" id="outbound-stats-cards"></div>
    <div class="chart-box">
      <h3>Outbound Attempts by Queue</h3>
      <div class="sub">Answered vs Unanswered</div>
      <canvas id="chartOutbound"></canvas>
    </div>
  </div>

  <div class="section">
    <div class="section-title" style="font-size:24px; font-weight:800; margin-bottom:20px;">Agents</div>
    <div style="border:1px solid #e5e7eb; border-radius:8px; overflow:hidden;">
        <table class="agents-table" style="margin:0;">
        <thead style="background:#f9fafb;"><tr><th>Agent</th><th>Ext</th><th>Login</th><th>Logout</th><th>Status</th><th>Status Since</th><th>Duration</th><th>French</th><th>English</th><th>Delivery</th><th>All queues</th></tr></thead>
        <tbody id="mgr-tbody"></tbody>
        </table>
    </div>
  </div>
</div>
"""

MANAGER_SCRIPT = r"""
let chartVolume, chartTalk, chartAttempts, chartOutbound;
let currentMetrics = [];

// The 5s snapshot poll re-renders the weekly-hours form from server state.
// Without this flag it wipes a ticked checkbox before SAVE WEEK is clicked,
// so the save posts the old (closed) values and Saturday never opens.
let hoursDirty = false;
document.getElementById('weekly-hours')?.addEventListener('input', () => { hoursDirty = true; });
document.getElementById('weekly-hours')?.addEventListener('change', () => { hoursDirty = true; });

// A fresh Vercel instance first serves the untouched default schedule
// (Saturday closed, Friday 15:00), not the real hours — the real hours
// arrive from Telico about a minute later. Until they have arrived, the
// form must not be saved: a save in that window stores the defaults and
// Telico drops the Saturday period. A schedule counts as real once it
// carries a sync status, a save stamp, or a Telico rebuild stamp.
let scheduleReady = false;
function scheduleIsReal(s) {
    return !!(s && (s.sync_status || s.user_saved || s.from_telico_at));
}

// The single-date form is set up with the one-date code below (default date,
// pre-fill from the hours that apply, and its summary).

async function api(path, body) {
  const r = await fetch(path, { method: 'POST', credentials: 'include', headers: {'Content-Type':'application/json'}, body: body?JSON.stringify(body):undefined });
  return r.json().catch(()=>null);
}

window.setRange = async (range) => {
    const now = new Date();
    const end = now.toISOString().split('T')[0];
    let start = end;
    
    if (range === '7') {
        const d = new Date(now); d.setDate(d.getDate() - 7);
        start = d.toISOString().split('T')[0];
    } else if (range === '30') {
        const d = new Date(now); d.setDate(d.getDate() - 30);
        start = d.toISOString().split('T')[0];
    }
    
    document.getElementById('report-start').value = start;
    document.getElementById('report-end').value = end;
    
    await fetchReports(start, end);
};

window.applyCustomRange = async () => {
    const start = document.getElementById('report-start').value;
    const end = document.getElementById('report-end').value;
    if (!start || !end) { showToast('Select valid dates', 'error'); return; }
    await fetchReports(start, end);
};

async function fetchReports(start, end) {
    const today = new Date().toISOString().split('T')[0];
    if (start === today && end === today) {
        currentMetrics = [];
        renderStats(); 
        return;
    }
    
    try {
        const r = await fetch(`/api/reports/metrics?start=${start}&end=${end}`, { credentials: 'include' });
        const data = await r.json();
        if (data.ok) {
            currentMetrics = data.metrics;
            renderStats();
        } else {
            showToast('Failed to load reports', 'error');
        }
    } catch (e) {
        showToast('Failed to load reports', 'error');
    }
}

window.changeStatus = async (id, s) => {
    if(!s) return;
    const res = await api(`/api/agents/${id}/status`, {status: s});
    if (!res || !res.ok) showToast(res?.reason || 'Failed to update status', 'error');
};
window.toggleQueue = async (ev, id, q, on) => {
    ev.preventDefault(); ev.stopPropagation();
    const r = await api(`/api/agents/${id}/queues/${encodeURIComponent(q)}`, {on});
    if (!r || !r.ok) showToast(r?.reason || 'Failed to update queue', 'error');
};
window.toggleAllQueues = async (id, on) => await api(`/api/agents/${id}/all-queues`, {on});
window.massQueue = async (queues, on) => { await api('/api/queues/mass', {queues, on}); showToast((on ? 'Login' : 'Logout') + ' sent', 'ok'); };
window.simulateBreak = async (agentId) => { const r = await fetch(`/api/debug/telico/simulate-break?agent_id=${agentId}`, {method: 'POST', credentials: 'include'}).then(x=>x.json()); if(r.ok) showToast('Simulating break...', 'ok'); };

window.saveWeek = async (btn) => {
    if (!scheduleReady) { showToast('Hours are still loading from Telico — wait a moment and try again.', 'error'); return; }
    if (btn) { btn.disabled = true; btn.textContent = 'SAVING...'; }
    try {
        const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
        const newHours = {};
        for (const d of days) {
            const open  = document.getElementById('chk_' + d).checked;
            const start = document.getElementById('start_' + d).value;
            const end   = document.getElementById('end_' + d).value;
            if (open && start >= end) {
                showToast(`${d}: end time must be after start time`, 'error');
                return;
            }
            newHours[d] = { open, start, end };
        }
        const data = { ...(state.schedule || {}), hours: newHours };
        const r = await api('/api/settings/schedule', { data });
        if (r && r.ok) { showToast('Business hours saved — pushing to Telico…', 'ok'); hoursDirty = false; }
        else           showToast(r?.reason || 'Save failed', 'error');
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = 'SAVE WEEK'; }
    }
};

window.removeException = async (btn, idx) => {
    if (!scheduleReady) { showToast('Hours are still loading from Telico — wait a moment and try again.', 'error'); return; }
    if (btn) { btn.disabled = true; }
    try {
        const exs = [...((state.schedule?.exceptions) || [])];
        const removed = exs.splice(idx, 1)[0];
        const data = { ...(state.schedule || {}), exceptions: exs };
        const r = await api('/api/settings/schedule', { data });
        if (r && r.ok) showToast(`Exception "${removed?.name || ''}" removed`, 'ok');
        else           showToast(r?.reason || 'Remove failed', 'error');
    } finally {
        if (btn) { btn.disabled = false; }
    }
};

// --- Change one date (a single date-specific override) ---
// One table of regular weekday rows, plus one date picker below it. Picking a
// date pre-fills the hours that already apply that day (the regular hours for
// that weekday, or an existing exception). Saving stores only that date as a
// date exception, which the existing Telico push already sends as a one-off
// period. The regular week is never touched by this.
const WEEK_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
let oneDateDirty = false; // true once the user has edited the single-date form

function _oneEffective(dateISO){
    const hours = (state.schedule && state.schedule.hours) || {};
    const exs = (state.schedule && state.schedule.exceptions) || [];
    const dt = new Date(dateISO + 'T12:00:00');
    const wd = WEEK_DAYS[(dt.getDay() + 6) % 7];
    const rec = hours[wd] || { open: false, start: '09:00', end: '17:00' };
    const ex = exs.find(e => e.date === dateISO);
    if (ex) return { open: ex.type !== 'closed', start: ex.start || rec.start, end: ex.end || rec.end, name: ex.name || '', regular: rec };
    return { open: !!rec.open, start: rec.start, end: rec.end, name: '', regular: rec };
}

window.prefillOneDate = () => {
    const dateEl = document.getElementById('one_date');
    if (!dateEl || !dateEl.value) return;
    const eff = _oneEffective(dateEl.value);
    document.getElementById('one_open').checked = eff.open;
    document.getElementById('one_start').value = eff.start;
    document.getElementById('one_end').value = eff.end;
    // Show the saved name only when it is a real name, not the auto-generated one.
    const nameEl = document.getElementById('one_name');
    nameEl.value = (eff.name && eff.name !== dateEl.value + ' hours' && eff.name !== dateEl.value + ' closed') ? eff.name : '';
    oneDateDirty = false;
    updateOneDateSummary();
};

// True when the edited hours sit entirely inside the regular hours for that
// day: a one-off period can only add hours, so such a change would do nothing.
function _oneIsSubset(open, start, end, rec){
    return open && !!rec.open && start >= rec.start && end <= rec.end && (start !== rec.start || end !== rec.end);
}

window.updateOneDateSummary = () => {
    const el = document.getElementById('one_summary');
    if (!el) return;
    const dateEl = document.getElementById('one_date');
    if (!dateEl || !dateEl.value) { el.textContent = ''; return; }
    const open = document.getElementById('one_open').checked;
    const start = document.getElementById('one_start').value;
    const end = document.getElementById('one_end').value;
    document.getElementById('one_start').disabled = !open;
    document.getElementById('one_end').disabled = !open;
    const eff = _oneEffective(dateEl.value);
    const rec = eff.regular;
    let text;
    if (open === eff.open && start === eff.start && end === eff.end) {
        text = 'No change for ' + dateEl.value + ' — it already ' + (eff.open ? 'opens ' + eff.start + '–' + eff.end : 'is closed') + '.';
    } else if (!open) {
        text = dateEl.value + ' will be closed all day' + (rec.open ? ' (regular: open ' + rec.start + '–' + rec.end + ')' : '') + '.';
    } else {
        text = dateEl.value + ' will open ' + start + '–' + end + (rec.open ? ' (regular: ' + rec.start + '–' + rec.end + ')' : ' (regular: closed)') + '.';
    }
    if (open && _oneIsSubset(open, start, end, rec)) text += ' Note: those hours sit inside the regular ' + rec.start + '–' + rec.end + ' — the regular hours will still apply, so it will not be saved.';
    el.textContent = text;
};

window.saveOneDate = async (btn) => {
    if (!scheduleReady) { showToast('Hours are still loading from Telico — wait a moment and try again.', 'error'); return; }
    const dateEl = document.getElementById('one_date');
    if (!dateEl || !dateEl.value) { showToast('Pick a date first', 'error'); return; }
    const date = dateEl.value;
    const open = document.getElementById('one_open').checked;
    const start = document.getElementById('one_start').value;
    const end = document.getElementById('one_end').value;
    const name = (document.getElementById('one_name').value || '').trim();
    if (open && start >= end) { showToast(date + ': end time must be after start time', 'error'); return; }
    const eff = _oneEffective(date);
    const rec = eff.regular;
    // Nothing different from what already applies that day: nothing to store.
    if (open === eff.open && start === eff.start && end === eff.end) {
        showToast('Nothing changed for ' + date, 'ok'); return;
    }
    // Inside the regular hours: a one-off period would change nothing, so skip it.
    if (_oneIsSubset(open, start, end, rec)) {
        showToast('Nothing saved — those hours sit inside the regular ' + rec.start + '–' + rec.end + ', so the regular hours still apply', 'error'); return;
    }
    if (btn) { btn.disabled = true; btn.textContent = 'SAVING...'; }
    try {
        let exs = [...((state.schedule && state.schedule.exceptions) || [])];
        exs = exs.filter(e => e.date !== date);
        // Back to the regular hours: the override is removed, nothing new is
        // stored. A closed day whose regular hours are also closed is back
        // to regular whatever the time fields still hold, so it is not
        // stored as a closed exception.
        const backToRegular = (!open && !rec.open) || (open === !!rec.open && start === rec.start && end === rec.end);
        if (!backToRegular) {
            if (!open) exs.push({ type: 'closed', date, name: name || date + ' closed', start: '00:00', end: '23:59' });
            else exs.push({ type: 'open', date, name: name || date + ' hours', start, end });
        }
        const data = { ...(state.schedule || {}), exceptions: exs };
        const r = await api('/api/settings/schedule', { data });
        if (r && r.ok) {
            showToast((open ? date + ' will open ' + start + '–' + end : date + ' will be closed') + ' — pushing to Telico…', 'ok');
            oneDateDirty = false;
            updateOneDateSummary();
        } else {
            showToast(r?.reason || 'Save failed', 'error');
        }
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = 'SAVE THIS DATE'; }
    }
};

// Default the date picker to today, fill it, and keep its summary live.
(function(){
    const el = document.getElementById('one_date');
    if (el && !el.value) {
        const n = new Date();
        el.value = n.getFullYear() + '-' + String(n.getMonth() + 1).padStart(2, '0') + '-' + String(n.getDate()).padStart(2, '0');
    }
    prefillOneDate();
})();
['one_open', 'one_start', 'one_end', 'one_name'].forEach(id => {
    document.getElementById(id)?.addEventListener('input', () => { oneDateDirty = true; updateOneDateSummary(); });
    document.getElementById(id)?.addEventListener('change', () => { oneDateDirty = true; updateOneDateSummary(); });
});

function renderAgents(){
  const tb = document.getElementById('mgr-tbody');
  if (!tb) return;
  if (document.activeElement && document.activeElement.tagName === 'SELECT') return;

  tb.innerHTML = state.agents.map((a, index)=>{
    const statusCls = STATUS_CLASS[a.status] || 'st-offline';
    return `
    <tr>
      <td class="agent-name-cell">
        <div class="avatar">${index + 1}</div>
        ${a.name}
      </td>
      <td class="mono" style="color:#6b7280">${a.phone}</td>
      <td class="mono">${a.loginTime || '—'}</td>
      <td class="mono">${a.logoutTime || '—'}</td>
      <td><select class="status-pill-select ${statusCls}" onchange="changeStatus(${a.id}, this.value)">
          ${STATUSES.map(s=>`<option value="${s}" ${s===a.status?'selected':''}>${s}</option>`).join('')}
      </select></td>
      <td class="mono">${fmtHHMM(a.statusSince)}</td>
      <td class="mono" style="font-weight: 700;">${fmtHMS(a.shiftDurationSeconds)}</td>
      ${['French','English','Delivery Line'].map(q=>`<td><button type="button" class="switch ${a.queues[q]?'on':''}" onclick="toggleQueue(event, ${a.id}, '${q}', ${!a.queues[q]})"></button></td>`).join('')}
      <td style="white-space:nowrap;">
        <button class="icon-btn" onclick="toggleAllQueues(${a.id}, true)" title="Join All">➜]</button>
        <button class="icon-btn" onclick="toggleAllQueues(${a.id}, false)" title="Leave All">⇦]</button>
        <button class="icon-btn" onclick="simulateBreak(${a.id})" title="Simulate Break">🧪</button>
      </td>
    </tr>`;
  }).join('');
}

function renderSchedule(){
  const wrap = document.getElementById('weekly-hours');
  if (!wrap || !state.schedule) return;
  if (scheduleIsReal(state.schedule)) scheduleReady = true;

  const saveWeekBtn = document.getElementById('save-week-btn');
  const saveDateBtn = document.getElementById('save-one-date-btn');
  // No hours yet (a fresh instance still holding the untouched default,
  // which stores `week` not `hours`) is the same as not ready: show the
  // loading note instead of a blank section, and keep the saves blocked.
  if (!scheduleReady || !state.schedule.hours) {
      wrap.innerHTML = '<div style="padding:12px; font-size:13px; color:#6b7280;">Loading hours from Telico… please wait before changing anything.</div>';
      const stEl = document.getElementById('schedule-sync-status');
      if (stEl) { stEl.textContent = 'Loading hours from Telico…'; stEl.style.color = '#6b7280'; }
      if (saveWeekBtn) saveWeekBtn.disabled = true;
      if (saveDateBtn) saveDateBtn.disabled = true;
      return;
  }
  if (saveWeekBtn && saveWeekBtn.textContent === 'SAVE WEEK') saveWeekBtn.disabled = false;
  if (saveDateBtn && saveDateBtn.textContent === 'SAVE THIS DATE') saveDateBtn.disabled = false;

  const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  if (!hoursDirty) wrap.innerHTML = days.map(d => {
      const h = state.schedule.hours[d] || { open: false, start: "09:00", end: "17:00" };
      return `
      <div class="day-row">
        <div class="day-name"><input type="checkbox" id="chk_${d}" ${h.open?'checked':''}> ${d.toUpperCase()}</div>
        <div class="time-inputs">
            <div style="display:flex; flex-direction:column; gap:4px;">
                <span style="font-size:10px; color:#6b7280; font-weight:600;">OPEN</span>
                <input type="time" id="start_${d}" value="${h.start}">
            </div>
            <div style="display:flex; flex-direction:column; gap:4px;">
                <span style="font-size:10px; color:#6b7280; font-weight:600;">CLOSE</span>
                <input type="time" id="end_${d}" value="${h.end}">
            </div>
        </div>
      </div>
      `;
  }).join('');

  const stEl = document.getElementById('schedule-sync-status');
  if (stEl) {
      const st = state.schedule.sync_status;
      if (!st) { stEl.textContent = 'Not pushed to Telico yet — save the week to sync.'; stEl.style.color = '#6b7280'; }
      else if (st.ok) { stEl.textContent = '✓ Synced to Telico — ' + st.at; stEl.style.color = '#15803d'; }
      else { stEl.textContent = '⚠ Telico sync failed: ' + (st.detail || 'rejected'); stEl.style.color = '#b91c1c'; }
  }

  const exWrap = document.getElementById('exceptions-list');
  if (exWrap) {
      const exs = state.schedule.exceptions || [];
      exWrap.innerHTML = `<div style="font-size:13px; color:#6b7280; margin-bottom:12px;">${exs.length} entries in Open Late and Closed Early.</div>` + exs.map((ex, i) => `
          <div class="exception-item">
              <div>
                  <div class="exception-name">${ex.name}</div>
                  <div class="exception-meta">${ex.date} · ${ex.type === 'closed' ? 'Closed' : 'Open late'} · ${ex.start}-${ex.end}</div>
              </div>
              <button class="icon-btn" style="color:#ef4444; background:transparent; border:none;" onclick="removeException(this, ${i})">🗑️</button>
          </div>
      `).join('');
  }

  // Keep the single-date form filled from the hours that apply, until the user edits it.
  if (!oneDateDirty && typeof prefillOneDate === 'function') prefillOneDate();
}

function calculateFrontendStats() {
    let totOffered = 0, totAnswered = 0, totAbandoned = 0, totHolding = 0, totTalk = 0, totHold = 0;
    let totOutAttempts = 0, totOutAnswered = 0, totOutTalk = 0;
    
    const queueStats = {
        "French": { offered: 0, answered: 0, abandoned: 0, talk: 0, hold: 0, out_attempts: 0, out_answered: 0 },
        "English": { offered: 0, answered: 0, abandoned: 0, talk: 0, hold: 0, out_attempts: 0, out_answered: 0 },
        "Delivery Line": { offered: 0, answered: 0, abandoned: 0, talk: 0, hold: 0, out_attempts: 0, out_answered: 0 }
    };
    const agentCounts = {};

    const extToName = {};
    const codeToName = {};
    const rawNameToName = {};

    state.agents.forEach(a => {
        if (a.phone) extToName[a.phone] = a.name;
        if (a.name) {
            const codeMatch = a.name.match(/^(\d+)\s+/);
            if (codeMatch) {
                codeToName[codeMatch[1]] = a.name;
                const rawName = a.name.replace(/^\d+\s+/, '').replace(/"/g, '').trim().toLowerCase();
                rawNameToName[rawName] = a.name;
            }
        }
    });

    function formatDest(dest) {
        if (!dest) return dest;
        dest = dest.trim();
        
        const match = dest.match(/^(\d+)\s+(.+)$/);
        if (match) {
            const code = match[1];
            if (codeToName[code]) return codeToName[code];
        }
        
        if (/^\d+$/.test(dest) && extToName[dest]) {
            return extToName[dest];
        }
        
        const cleanDest = dest.replace(/"/g, '').trim().toLowerCase();
        if (rawNameToName[cleanDest]) {
            return rawNameToName[cleanDest];
        }

        return dest.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
    }

    (state.recentCalls || []).forEach(c => {
        const q = c.queue;
        if (!queueStats[q]) return;

        const dur = c.duration;

        if (c.direction.toLowerCase() === 'outbound') {
            totOutAttempts++;
            if (c.status === 'Answer') {
                totOutAnswered++;
                totOutTalk += dur;
            }
        } else {
            if (c.status === 'Answer') {
                totAnswered++;
                totTalk += dur;
                queueStats[q].answered++;
                queueStats[q].talk += dur;

                let dest = formatDest(c.destination);
                if (dest) {
                    const lower = dest.toLowerCase();
                    if (lower !== 'english csr' && lower !== 'french csr' && lower !== 'delivery line' && lower !== 'english' && lower !== 'french') {
                        agentCounts[dest] = (agentCounts[dest] || 0) + 1;
                    }
                }
            } else {
                totAbandoned++;
                queueStats[q].abandoned++;
            }
        }
    });

    if (state.metrics && state.metrics.length > 0) {
        totOutAttempts = 0; totOutAnswered = 0; totOutTalk = 0;
        state.metrics.forEach(m => {
            totOutAttempts += (m.outbound_attempts || 0);
            totOutAnswered += (m.outbound_answered || 0);
            totOutTalk += (m.outbound_talk_secs || 0);
        });
    }

    if (state.recentCalls.length === 0 && currentMetrics.length > 0) {
        currentMetrics.forEach(m => {
            const q = m.queue;
            if (!queueStats[q]) return;
            totOffered += (m.count + m.missed);
            totAnswered += m.count;
            totAbandoned += m.missed;
            totHolding += m.waiting;
            totTalk += m.talk_secs;
            totHold += m.hold_secs;
            totOutAttempts += m.outbound_attempts;
            totOutAnswered += m.outbound_answered;
            totOutTalk += m.outbound_talk_secs;
            
            queueStats[q].offered += (m.count + m.missed);
            queueStats[q].answered += m.count;
            queueStats[q].abandoned += m.missed;
            queueStats[q].talk += m.talk_secs;
            queueStats[q].hold += m.hold_secs;
        });
    } else {
        totOffered = totAnswered + totAbandoned;
        Object.keys(queueStats).forEach(q => {
            queueStats[q].offered = queueStats[q].answered + queueStats[q].abandoned;
        });
    }

    return { totOffered, totAnswered, totAbandoned, totHolding, totTalk, totHold, totOutAttempts, totOutAnswered, totOutTalk, queueStats, agentCounts };
}

function renderStats(){
  const cards = document.getElementById('live-stats-cards');
  const outCards = document.getElementById('outbound-stats-cards');
  if (!cards || !outCards) return;

  const startVal = document.getElementById('report-start').value;
  const endVal = document.getElementById('report-end').value;
  const today = new Date().toISOString().split('T')[0];
  const isToday = (!startVal || startVal === today) && (!endVal || endVal === today);

  let stats;
  if (isToday) {
      stats = calculateFrontendStats();
  } else {
      let totOffered = 0, totAnswered = 0, totAbandoned = 0, totHolding = 0, totTalk = 0, totHold = 0;
      let totOutAttempts = 0, totOutAnswered = 0, totOutTalk = 0;
      const queueStats = {
          "French": { offered: 0, answered: 0, abandoned: 0, talk: 0, hold: 0, out_attempts: 0, out_answered: 0 },
          "English": { offered: 0, answered: 0, abandoned: 0, talk: 0, hold: 0, out_attempts: 0, out_answered: 0 },
          "Delivery Line": { offered: 0, answered: 0, abandoned: 0, talk: 0, hold: 0, out_attempts: 0, out_answered: 0 }
      };
      currentMetrics.forEach(m => {
          const q = m.queue;
          if (!queueStats[q]) return;
          totOffered += (m.count + m.missed);
          totAnswered += m.count;
          totAbandoned += m.missed;
          totHolding += m.waiting;
          totTalk += m.talk_secs;
          totHold += m.hold_secs;
          totOutAttempts += m.outbound_attempts;
          totOutAnswered += m.outbound_answered;
          totOutTalk += m.outbound_talk_secs;
          
          queueStats[q].offered = m.count + m.missed;
          queueStats[q].answered = m.count;
          queueStats[q].abandoned = m.missed;
          queueStats[q].talk = m.talk_secs;
          queueStats[q].hold = m.hold_secs;
          queueStats[q].out_attempts = m.outbound_attempts;
          queueStats[q].out_answered = m.outbound_answered;
      });
      stats = { totOffered, totAnswered, totAbandoned, totHolding, totTalk, totHold, totOutAttempts, totOutAnswered, totOutTalk, queueStats, agentCounts: {} };
  }

  const { totOffered, totAnswered, totAbandoned, totHolding, totTalk, totHold, totOutAttempts, totOutAnswered, totOutTalk, queueStats, agentCounts } = stats;

  const answerRate = totOffered > 0 ? ((totAnswered / totOffered) * 100).toFixed(1) : '0.0';
  const abandonRate = totOffered > 0 ? ((totAbandoned / totOffered) * 100).toFixed(1) : '0.0';
  const avgTalk = totAnswered > 0 ? Math.floor(totTalk / totAnswered) : 0;
  const avgHold = totAnswered > 0 ? Math.floor(totHold / totAnswered) : 0;
  const outAnswerRate = totOutAttempts > 0 ? ((totOutAnswered / totOutAttempts) * 100).toFixed(1) : '0.0';

  cards.innerHTML = `
    <div class="stat-card highlight">
      <div class="label">Offered Calls</div>
      <div class="value">${totOffered}</div>
      <div class="sub">Delivery 441 • English 439 • French 440</div>
    </div>
    <div class="stat-card">
      <div class="label">Answered</div>
      <div class="value">${totAnswered}</div>
      <div class="sub">${answerRate}% answer rate</div>
    </div>
    <div class="stat-card">
      <div class="label">Abandoned</div>
      <div class="value">${totAbandoned}</div>
      <div class="sub">${abandonRate}% abandonment</div>
    </div>
    <div class="stat-card">
      <div class="label">Current Holding</div>
      <div class="value">${totHolding}</div>
      <div class="sub">Across all queues</div>
    </div>
    <div class="stat-card">
      <div class="label">Weighted Talk</div>
      <div class="value" style="color:#1e293b">${fmtHMS(avgTalk)}</div>
      <div class="sub">Average answered call</div>
    </div>
    <div class="stat-card">
      <div class="label">Weighted Hold</div>
      <div class="value" style="color:#1e293b">${fmtHMS(avgHold)}</div>
      <div class="sub">Average wait before outcome</div>
    </div>

    <div class="stat-card row">
        <div style="flex:1;">
          <div class="label">Inbound Calls</div>
          <div class="value">${totOffered}</div>
          <div class="sub">Selected reporting range</div>
        </div>
        <div style="flex:1;">
          <div class="label">Inbound Answered</div>
          <div class="value">${totAnswered}</div>
          <div class="sub">Successful connections</div>
        </div>
        <div style="flex:1;">
          <div class="label">Inbound Answer Rate</div>
          <div class="value">${answerRate}%</div>
          <div class="sub">Answered / offered</div>
        </div>
        <div style="flex:1;">
          <div class="label">Total Inbound Talk</div>
          <div class="value">${fmtHMS(totTalk)}</div>
          <div class="sub">Across telephone lines</div>
        </div>
    </div>
  `;

  outCards.innerHTML = `
    <div class="stat-card">
      <div class="label">Outbound Attempts</div>
      <div class="value">${totOutAttempts}</div>
      <div class="sub">Total dials</div>
    </div>
    <div class="stat-card">
      <div class="label">Answered Outbound</div>
      <div class="value">${totOutAnswered}</div>
      <div class="sub">Successful connections</div>
    </div>
    <div class="stat-card">
      <div class="label">Outbound Answer Rate</div>
      <div class="value">${outAnswerRate}%</div>
      <div class="sub">Answered / attempts</div>
    </div>
    <div class="stat-card">
      <div class="label">Total Outbound Talk</div>
      <div class="value" style="color:#1e293b">${fmtHMS(totOutTalk)}</div>
      <div class="sub">Across all lines</div>
    </div>
  `;

  if (typeof Chart !== 'undefined') {
    const qLabels = ["French", "English", "Delivery Line"];
    const offered = [];
    const answered = [];
    const abandoned = [];
    const talk = [];
    const hold = [];

    qLabels.forEach(q => {
        const m = queueStats[q] || { offered: 0, answered: 0, abandoned: 0, talk: 0, hold: 0 };
        offered.push(m.offered);
        answered.push(m.answered);
        abandoned.push(m.abandoned);
        talk.push(m.talk);
        hold.push(m.hold);
    });

    const tickColor = '#6b7280';
    
    const chartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { labels: { color: tickColor } } },
        scales: { x: { ticks: { color: tickColor } }, y: { ticks: { color: tickColor }, beginAtZero: true, max: Math.max(1, ...offered, ...talk) } }
    };

    if (!chartVolume) {
      chartVolume = new Chart(document.getElementById('chartVolume'), {
        type: 'bar',
        data: { labels: qLabels, datasets: [
          { label: 'Offered', data: offered, backgroundColor: '#1e293b' },
          { label: 'Answered', data: answered, backgroundColor: '#94a3b8' },
          { label: 'Abandoned', data: abandoned, backgroundColor: '#ea580c' }
        ]},
        options: chartOptions
      });
    } else {
      chartVolume.data.datasets[0].data = offered;
      chartVolume.data.datasets[1].data = answered;
      chartVolume.data.datasets[2].data = abandoned;
      chartVolume.options.scales.y.max = Math.max(1, ...offered, ...talk);
      chartVolume.update();
    }

    if (!chartTalk) {
      chartTalk = new Chart(document.getElementById('chartTalk'), {
        type: 'bar',
        data: { labels: qLabels, datasets: [
          { label: 'Talk', data: talk, backgroundColor: '#1e293b' },
          { label: 'Hold', data: hold, backgroundColor: '#ea580c' }
        ]},
        options: chartOptions
      });
    } else {
      chartTalk.data.datasets[0].data = talk;
      chartTalk.data.datasets[1].data = hold;
      chartTalk.options.scales.y.max = Math.max(1, ...talk, ...hold);
      chartTalk.update();
    }

    const agentLabels = Object.keys(agentCounts);
    const agentData = Object.values(agentCounts);

    if (!chartAttempts) {
      chartAttempts = new Chart(document.getElementById('chartAttempts'), {
        type: 'bar',
        data: { labels: agentLabels, datasets: [
          { label: 'Calls Answered', data: agentData, backgroundColor: '#1e293b' }
        ]},
        options: chartOptions
      });
    } else {
      chartAttempts.data.labels = agentLabels;
      chartAttempts.data.datasets[0].data = agentData;
      chartAttempts.update();
    }

    const outAttemptsData = [];
    const outAnsweredData = [];
    
    qLabels.forEach(q => {
        const m = queueStats[q] || { out_attempts: 0, out_answered: 0 };
        outAttemptsData.push(m.out_attempts || 0);
        outAnsweredData.push(m.out_answered || 0);
    });

    if (!chartOutbound) {
      chartOutbound = new Chart(document.getElementById('chartOutbound'), {
        type: 'bar',
        data: { labels: qLabels, datasets: [
          { label: 'Answered', data: outAnsweredData, backgroundColor: '#1e293b' },
          { label: 'Unanswered', data: outAttemptsData.map((a, i) => Math.max(0, a - outAnsweredData[i])), backgroundColor: '#ea580c' }
        ]},
        options: chartOptions
      });
    } else {
      chartOutbound.data.datasets[0].data = outAnsweredData;
      chartOutbound.data.datasets[1].data = outAttemptsData.map((a, i) => Math.max(0, a - outAnsweredData[i]));
      chartOutbound.options.scales.y.max = Math.max(1, ...outAttemptsData);
      chartOutbound.update();
    }
  }
}

onUpdate(()=>{ renderAgents(); renderSchedule(); renderStats(); });
setInterval(renderAgents, 2000);

document.addEventListener('DOMContentLoaded', () => {
  setRange('today');
  if(document.getElementById('weekly-hours')) renderSchedule();
  renderStats();
});
"""

@router.get("/manager", response_class=HTMLResponse)
def page_manager(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] != "manager": return RedirectResponse(home_for_role(sess["role"]), status_code=302)

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Manager · Bundl Box</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>{MANAGER_CSS}</style>
</head><body style="display:flex;flex-direction:column;min-height:100vh;">
<header class="topbar">
  <div class="topbar-left">
    <div class="brand"><div class="brand-logo">BB</div><div class="brand-title">Bundl Box</div></div>
    <nav class="top-nav">
      <a href="/wallboard">Wallboard</a>
      <a href="/manager" class="active">Manager</a>
      <a href="/agent">Agent View</a>
      <a href="/call-history">Call History</a>
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
{MANAGER_BODY}
</main>
<script>
const STATUSES = {json.dumps(STATUSES)}; const QUEUES = {json.dumps(QUEUES)};
const STATUS_CLASS = {{ 'Login / Available':'st-available','Logout / Offline':'st-offline','Admin / Wrap Up':'st-admin','Break / Lunch':'st-break','Outbound':'st-outbound' }};
const state = {{ agents: [], metrics: [], telicoConfigured: false, breakLimitSeconds: 1800, lastSync: null, lastUpdate: Date.now(), schedule: null, recentCalls: [] }};
const listeners = [];
function onUpdate(fn){{ listeners.push(fn); }}
function notify(){{ for (const fn of listeners) fn(state); }}
function startClock(){{ const t = document.getElementById('clock'); const d = document.getElementById('clock-date'); if (!t) return; const tick = () => {{ const n=new Date(); t.textContent=n.toLocaleTimeString('en-GB'); d.textContent=n.toLocaleDateString('en-GB',{{weekday:'long',day:'numeric',month:'short',year:'numeric'}}); }}; tick(); setInterval(tick,1000); }}
function fmtHMS(sec){{ sec = Math.max(0, Math.floor(Number(sec) || 0)); const h = Math.floor(sec / 3600); const m = Math.floor((sec % 3600) / 60); const s = sec % 60; return String(h).padStart(2,'0') + ':' + String(m).padStart(2,'0') + ':' + String(s).padStart(2,'0'); }}
function fmtHHMM(iso){{ if (!iso) return '--:--'; const d = new Date(iso); return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0'); }}
function showToast(msg, kind=''){{ const el = document.createElement('div'); el.className = 'toast ' + kind; el.textContent = msg; document.body.appendChild(el); setTimeout(() => el.remove(), 5000); }}
(function connectWS(){{ const proto = location.protocol === 'https:' ? 'wss' : 'ws'; const ws = new WebSocket(proto+'://'+location.host+'/ws'); ws.onclose = () => {{ setTimeout(connectWS, 2000); }}; ws.onmessage = (e) => {{ const msg = JSON.parse(e.data); if (msg.type === 'snapshot') {{ state.agents = msg.data.agents; state.metrics = msg.data.metrics; state.telicoConfigured = !!msg.data.telicoConfigured; state.lastSync = msg.data.lastSync || null; state.schedule = msg.data.schedule || null; state.recentCalls = msg.data.recentCalls || []; state.lastUpdate = Date.now(); notify(); }} }}; }})();
// Vercel: serverless cannot hold a WebSocket, so poll /api/snapshot too.
setInterval(async () => {{ try {{ const r = await fetch('/api/snapshot', {{credentials:'include'}}); if (!r.ok) return; const d = await r.json(); state.agents = d.agents; state.metrics = d.metrics; state.telicoConfigured = !!d.telicoConfigured; state.lastSync = d.lastSync || null; state.schedule = d.schedule || null; state.recentCalls = d.recentCalls || []; state.lastUpdate = Date.now(); notify(); }} catch (e) {{}} }}, 5000);

startClock();
{MANAGER_SCRIPT}
</script></body></html>"""

@router.post("/api/agents/{agent_id}/status")
async def api_set_status(agent_id: int, body: StatusBody, request: Request):
    sess = get_session(request)
    if not sess: raise HTTPException(401, "not logged in")
    if sess["role"] == "agent" and sess.get("agent_id") is not None and sess.get("agent_id") != agent_id: raise HTTPException(403, "not your agent")
    if sess["role"] == "wallboard": raise HTTPException(403, "read-only")
    res = await _update_agent_status(agent_id, body.status, actor=sess["role"])
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return res

@router.post("/api/agents/{agent_id}/queues/{queue}")
async def api_set_agent_queue(agent_id: int, queue: str, body: QueueBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    c = connect()
    row = c.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "agent not found")
    if body.on:
        members = await asyncio.to_thread(telico_client.fetch_queue_members, telico_client.QUEUE_IDS[queue])
        is_in_queue = any(m["extension"] == row["phone"] for m in members)
        if not is_in_queue: return {"ok": False, "reason": f"Agent is not assigned to {queue} in Telico."}
    _pending_toggles[(agent_id, queue)] = time.time()
    _set_queue_flag(agent_id, queue, body.on)
    try: r = await asyncio.to_thread(telico_client.set_queue_membership, telico_client.QUEUE_IDS[queue], row["phone"], body.on)
    except Exception as e: r = {"ok": False, "reason": str(e)}
    ok = bool((r or {}).get("ok"))
    if not ok:
        _set_queue_flag(agent_id, queue, not body.on)
        _pending_toggles.pop((agent_id, queue), None)
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": ok, "reason": (r or {}).get("reason", "")}

@router.post("/api/agents/{agent_id}/all-queues")
async def api_agent_all_queues(agent_id: int, body: QueueBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    c = connect()
    row = c.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "agent not found")
    if body.on and row["status"] == "Logout / Offline": await _update_agent_status(agent_id, "Login / Available", actor="Manager")
    elif not body.on and row["status"] != "Logout / Offline" and row["status"] != "Break / Lunch": await _update_agent_status(agent_id, "Logout / Offline", actor="Manager")
    for q_name in QUEUES:
        _pending_toggles[(agent_id, q_name)] = time.time()
        _set_queue_flag(agent_id, q_name, body.on)
        try:
            r = await asyncio.to_thread(telico_client.set_queue_membership, telico_client.QUEUE_IDS[q_name], row["phone"], body.on)
            if not (r or {}).get("ok"):
                _set_queue_flag(agent_id, q_name, not body.on)
                _pending_toggles.pop((agent_id, q_name), None)
        except:
            _set_queue_flag(agent_id, q_name, not body.on)
            _pending_toggles.pop((agent_id, q_name), None)
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True}

@router.post("/api/queues/mass")
async def api_mass_queue(body: MassBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    c = connect()
    agents = c.execute("SELECT * FROM agents WHERE phone != ''").fetchall()
    for a in agents:
        if body.on and a["status"] == "Logout / Offline": await _update_agent_status(a["id"], "Login / Available", actor="Manager")
        elif not body.on and a["status"] != "Logout / Offline" and a["status"] != "Break / Lunch": await _update_agent_status(a["id"], "Logout / Offline", actor="Manager")
        for q_name in body.queues:
            _pending_toggles[(a["id"], q_name)] = time.time()
            _set_queue_flag(a["id"], q_name, body.on)
            try: await asyncio.to_thread(telico_client.set_queue_membership, telico_client.QUEUE_IDS[q_name], a["phone"], body.on)
            except:
                _set_queue_flag(a["id"], q_name, not body.on)
                _pending_toggles.pop((a["id"], q_name), None)
    c.commit()
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True}

@router.post("/api/settings/schedule")
async def api_save_schedule(body: ScheduleBody, request: Request):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")

    # Mark the save as the manager's own, so a later Telico rebuild on a fresh
    # instance never overwrites hours the manager set here.
    body.data["user_saved"] = True
    body.data["saved_at"] = time.time()
    body.data.pop("from_telico_at", None)
    c = connect()
    c.execute("UPDATE schedule SET data=? WHERE id=1", (json.dumps(body.data),))
    c.commit()

    async def _record_schedule_sync(failures: list):
        # Persist the Telico push outcome on the schedule row so the Hours
        # page can show whether the save actually reached Telico (a failed
        # push used to be silent — the page said "saved" and nothing changed).
        try:
            c2 = connect()
            row = c2.execute("SELECT data FROM schedule WHERE id=1").fetchone()
            data2 = json.loads(row["data"]) if row else {}
            data2["sync_status"] = {
                "ok": not failures,
                "at": _datetime.now(ZoneInfo("America/Toronto")).strftime("%b %d, %I:%M %p"),
                "detail": "; ".join(failures) if failures else "All periods pushed to Telico",
            }
            c2.execute("UPDATE schedule SET data=? WHERE id=1", (json.dumps(data2),))
            c2.commit()
            await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
        except Exception as e:
            log.error(f"Could not record schedule sync status: {e}")

    async def _push_to_telico(data: dict):
        log.info("--- STARTING TELICO SCHEDULE SYNC ---")
        failures: list = []
        try:
            groups = await asyncio.to_thread(telico_client.fetch_time_groups)
            
            biz_group_id = groups.get("business hours", "2303")
            
            exc_group_id_str = None
            for k, v in groups.items():
                if any(kw in k for kw in ("closed early", "holiday", "closure", "exception")):
                    exc_group_id_str = v
                    log.info(f"Found Exception group '{k}' with ID {v}")
                    break
                    
            exc_group_id = int(exc_group_id_str) if exc_group_id_str else None

            if not biz_group_id:
                log.error("No business-hours group in Telico — aborting sync")
                return

            biz_periods = await asyncio.to_thread(telico_client.fetch_time_periods, int(biz_group_id))
            biz_period_map = {p["name"].lower(): p["id"] for p in biz_periods}
            
            exc_periods = []
            exc_period_map = {}
            if exc_group_id:
                exc_periods = await asyncio.to_thread(telico_client.fetch_time_periods, exc_group_id)
                exc_period_map = {p["name"].lower(): p["id"] for p in exc_periods}

            # Telico allows several periods with the same name. A save that
            # could not see the existing periods once created a second full
            # set of weekday periods; later saves only ever update one copy
            # of each pair, so the extra copies stay forever. Drop the
            # extras here, keeping the copy the name maps above point at
            # (the last one listed), so a duplicate can never survive a save.
            async def _dedupe_periods(periods, group_id, label):
                seen = {}
                for p in periods:
                    seen.setdefault(p["name"].strip().lower(), []).append(p["id"])
                for name_lc, ids in seen.items():
                    for extra_id in ids[:-1]:
                        log.info(f"Deleting duplicate period '{name_lc}' (id {extra_id}) from {label}")
                        await asyncio.to_thread(telico_client.delete_time_period, extra_id, group_id)

            await _dedupe_periods(biz_periods, int(biz_group_id), "Business Hours")
            if exc_group_id:
                await _dedupe_periods(exc_periods, exc_group_id, "Exceptions")

            log.info(f"Business periods: {list(biz_period_map.keys())}")
            log.info(f"Exception periods: {list(exc_period_map.keys())}")

            day_map = {
                "Monday":    {"short": "Mon", "dow": "1"},
                "Tuesday":   {"short": "Tue", "dow": "2"},
                "Wednesday": {"short": "Wed", "dow": "3"},
                "Thursday":  {"short": "Thu", "dow": "4"},
                "Friday":    {"short": "Fri", "dow": "5"},
                "Saturday":  {"short": "Sat", "dow": "6"},
                "Sunday":    {"short": "Sun", "dow": "7"},
            }

            # ---- 1. WEEKLY HOURS ----
            for full_day, day_data in (data.get("hours") or {}).items():
                mapping = day_map.get(full_day)
                if not mapping:
                    continue

                short_day = mapping["short"]
                existing_id = biz_period_map.get(short_day.lower())

                if day_data.get("open"):
                    log.info(f"Pushing {short_day}  {day_data['start']} → {day_data['end']}")
                    ok = await asyncio.to_thread(
                        telico_client.save_time_period,
                        int(biz_group_id), existing_id,
                        {
                            "name": short_day,
                            "start": day_data["start"],
                            "end":   day_data["end"],
                            "repeats": True,
                            "days_week": [mapping["dow"]],
                        },
                    )
                    if not ok:
                        failures.append(f"{short_day}: {telico_client.last_error or 'Telico rejected the save'}")
                elif existing_id:
                    log.info(f"Deleting {short_day} (day closed)")
                    ok = await asyncio.to_thread(
                        telico_client.delete_time_period, existing_id, biz_group_id
                    )
                    if not ok:
                        failures.append(f"{short_day}: could not delete the old period in Telico")

            # ---- 2. EXCEPTIONS ----
            today = _date.today()
            
            db_exception_names = {
                (ex.get("name") or "").strip().lower() 
                for ex in (data.get("exceptions") or []) 
                if ex.get("name")
            }

            for ex in (data.get("exceptions") or []):
                name = (ex.get("name") or "").strip()
                date_str = (ex.get("date") or "").strip()
                ex_type = ex.get("type")
                if not name or not date_str:
                    continue

                try:
                    ex_date = _date.fromisoformat(date_str)
                except ValueError:
                    continue

                if ex_date < today:
                    log.info(f"Skipping past exception '{name}' ({date_str}) — keeping in Telico")
                    continue

                if ex_type == "open":
                    existing_id = biz_period_map.get(name.lower())
                    log.info(f"Pushing open-late {name} on {date_str}")
                    ok = await asyncio.to_thread(
                        telico_client.save_time_period,
                        int(biz_group_id), existing_id,
                        {
                            "name": name,
                            "start": ex.get("start", "18:00"),
                            "end":   ex.get("end",   "23:59"),
                            "repeats": False,
                            "date": date_str,
                        },
                    )
                    if not ok:
                        failures.append(f"{name}: {telico_client.last_error or 'Telico rejected the save'}")

                elif ex_type == "closed" and exc_group_id:
                    existing_id = exc_period_map.get(name.lower())
                    ok = await asyncio.to_thread(
                        telico_exceptions.sync_closed_exception,
                        exc_group_id, name, date_str, existing_id
                    )
                    if not ok:
                        failures.append(f"{name}: {telico_client.last_error or 'Telico rejected the save'}")

            # ---- 3. RECONCILE ORPHANED PERIODS ----
            weekdays = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
            
            for p in biz_periods:
                name_lc = p["name"].strip().lower()
                if name_lc in weekdays or name_lc in db_exception_names:
                    continue
                log.info(f"Deleting orphaned period '{p['name']}' from Business Hours")
                await asyncio.to_thread(telico_client.delete_time_period, p["id"], biz_group_id)

            if exc_group_id:
                for p in exc_periods:
                    name_lc = p["name"].strip().lower()
                    if name_lc in db_exception_names:
                        continue
                    log.info(f"Deleting orphaned period '{p['name']}' from Exceptions Group")
                    await asyncio.to_thread(telico_client.delete_time_period, p["id"], exc_group_id)

            log.info("--- FINISHED TELICO SCHEDULE SYNC ---")
            await _record_schedule_sync(failures)
        except Exception as e:
            log.error(f"FAILED to sync schedule to Telico: {e}", exc_info=True)
            await _record_schedule_sync([f"Sync failed: {e}"])

    asyncio.create_task(_push_to_telico(body.data))
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True}


@router.post("/api/debug/telico/simulate-break")
async def api_debug_simulate_break(request: Request, agent_id: int):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    c = connect()
    row = c.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row: raise HTTPException(404, "agent not found")
    await _update_agent_status(agent_id, "Break / Lunch", actor="Manager")
    if row["phone"]: asyncio.create_task(_fire_call(row["phone"], row["name"], row["id"]))
    await hub.broadcast({"type": "snapshot", "data": snapshot_full()})
    return {"ok": True, "agent": row["name"]}
