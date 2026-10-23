from fastapi import APIRouter, Request, File, UploadFile, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from utils import get_session, home_for_role, hub
import os
import secrets
import shutil
from pathlib import Path
import csv
import io
import asyncio
import requests
from requests.auth import HTTPBasicAuth
import logging

from dotenv import load_dotenv
load_dotenv()

router = APIRouter()
log = logging.getLogger("app")

SMS_UPLOAD_DIR = Path(os.getenv("SMS_UPLOAD_DIR", str(Path(__file__).parent / "sms_uploads")))
SMS_UPLOAD_DIR.mkdir(exist_ok=True, parents=True)

SMS_API_URL = os.getenv("SMS_API_URL", "https://sms.telico.cloud/api/send_sms")
MMS_API_URL = os.getenv("MMS_API_URL", "https://sms.telico.cloud/api/send_mms")
SMS_USERNAME = os.getenv("SMS_USERNAME", "")
SMS_PASSWORD = os.getenv("SMS_PASSWORD", "")
SMS_SOURCE_DID = os.getenv("SMS_SOURCE_DID", "")
SMS_THROTTLE_SECONDS = float(os.getenv("SMS_THROTTLE_SECONDS", "1.0"))

# White theme matching the Manager Desk
SMS_CSS = """
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

.view { flex:1; overflow:auto; padding:22px 24px 28px; display: flex; justify-content: center; }
.sms-wrapper { width: 100%; max-width: 1200px; }

.sms-grid { display:grid; grid-template-columns: 1fr 1fr; gap:24px; align-items:flex-start; }
@media (max-width: 1000px) { .sms-grid { grid-template-columns:1fr; } }

.section { background:#ffffff; border:1px solid #e5e7eb; border-radius:8px; padding:24px; margin-bottom:24px; box-shadow:0 1px 3px rgba(0,0,0,0.05); }
.section-title { font-size:18px; font-weight:700; color:#111827; margin-bottom:16px; border-bottom: 2px solid #f3f4f6; padding-bottom: 10px; }

.sms-field { margin-bottom:20px; }
.sms-field textarea, .sms-field input[type="text"] { width:100%; padding:12px 14px; font-size:15px; border-radius:8px; border:1px solid #d1d5db; background:#ffffff; color:#111827; resize:vertical; transition: border-color 0.2s; }
.sms-field textarea:focus, .sms-field input[type="text"]:focus { border-color: #111827; outline: none; }

.sms-seg { display:inline-flex; background:#f3f4f6; border:1px solid #e5e7eb; border-radius:8px; padding:4px; gap:4px; margin-bottom:12px; }
.sms-seg button { padding:8px 16px; font-size:13px; font-weight:600; color:#6b7280; border-radius:6px; transition: all 0.2s; }
.sms-seg button.active { background:#ffffff; color:#111827; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }

.sms-drop { display:flex; flex-direction:column; align-items:center; justify-content:center; padding:24px 16px; border:2px dashed #d1d5db; border-radius:12px; background:#f9fafb; color:#6b7280; cursor:pointer; transition: all 0.2s; }
.sms-drop:hover { border-color:#111827; color:#111827; background: #ffffff; }
.sms-drop input[type="file"] { display: none; }
.sms-drop .big-icon { font-size: 28px; margin-bottom: 8px; }

.sms-row { display:grid; grid-template-columns: 1fr 1fr 90px 1fr; gap:10px; padding:12px 16px; border-bottom:1px solid #e5e7eb; font-size:14px; align-items: center; }
.sms-row:last-child { border-bottom: none; }
.sms-row:nth-child(even) { background: #f9fafb; }

.filename { color: #1d4ed8; font-weight: 600; font-size: 14px; margin-top: 8px; font-family: 'JetBrains Mono', monospace; }
.sms-label { color: #111827; font-size: 14px; font-weight: 700; margin-bottom: 8px; display: block; text-transform: uppercase; letter-spacing: 0.05em; }
.hint { color: #6b7280; font-size: 13px; margin-top: 8px; line-height: 1.5; }
.hint code { color: #111827; background: #f3f4f6; padding: 2px 6px; border-radius: 4px; font-family: 'JetBrains Mono', monospace; font-weight: 600; border: 1px solid #e5e7eb; }

.sms-actions { display: flex; gap: 12px; border-top: 1px solid #e5e7eb; padding-top: 24px; margin-top: 24px; }
.btn-primary { background:#111827; color:#ffffff; padding:14px 24px; border-radius:8px; font-weight:700; font-size:15px; cursor:pointer; transition: background 0.2s; }
.btn-primary:hover { background:#374151; }
.btn-primary:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-outline { background:#ffffff; color:#111827; border:1px solid #d1d5db; padding:14px 24px; border-radius:8px; font-weight:600; font-size:15px; cursor:pointer; transition: all 0.2s; }
.btn-outline:hover { background:#f9fafb; border-color: #9ca3af; }

.sms-summary { display: flex; gap: 30px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 20px; margin-bottom: 20px; }
.sms-summary .lbl { font-size: 12px; font-weight: 700; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
.sms-summary .big { font-size: 32px; font-weight: 800; font-family: 'JetBrains Mono', monospace; color: #111827; line-height: 1.1; }

.sms-empty { text-align: center; color: #9ca3af; padding: 40px 0; font-size: 15px; }
.sms-row .ok { color: #166534; font-weight: 700; }
.sms-row .err { color: #ea580c; font-weight: 700; }
.sms-row .phone { font-family: 'JetBrains Mono', monospace; font-weight: 600; color: #111827; }

.toast { position: fixed; left: 50%; bottom: 24px; transform: translateX(-50%); background: #111827; border: 1px solid #374151; padding: 12px 20px; border-radius: 8px; font-size: 15px; z-index: 999; box-shadow:0 4px 12px rgba(0,0,0,0.1); color: #ffffff; }
.toast.error { border-color: #ea580c; color: #ea580c; }
.toast.ok { border-color: #10b981; color: #10b981; }
"""

DEFAULT_SMS_TEMPLATE = "Hello {name}, this is a test message from Bundl Box."

SMS_BODY = f"""
<div class="sms-wrapper">
  <div class="sms-grid">
    <div>
      <div class="section">
        <div class="section-title">1 · Message</div>
        <div class="sms-field">
          <label class="sms-label">Message template</label>
          <textarea id="sms-template" rows="5" class="mono" placeholder="Type your message here...">{DEFAULT_SMS_TEMPLATE}</textarea>
          <div class="hint">Use <code>{{name}}</code> to insert each recipient's name.</div>
        </div>

        <div class="section-title" style="margin-top:22px">2 · Media</div>
        <div class="sms-field">
          <div class="sms-seg" id="sms-media-seg">
            <button type="button" data-mode="none"   class="active" onclick="setMediaMode('none')">No media</button>
            <button type="button" data-mode="upload"               onclick="setMediaMode('upload')">Upload file</button>
            <button type="button" data-mode="path"                 onclick="setMediaMode('path')">URL / path</button>
          </div>

          <div id="sms-media-none-panel">
            <div class="hint">Plain SMS only — no MMS attachment.</div>
          </div>

          <div id="sms-media-upload-panel" style="display:none">
            <label class="sms-drop" for="sms-media-file">
              <div class="big-icon">🖼️</div>
              <div style="font-weight:600;">Click to choose an image / video / PDF</div>
              <div class="hint">Uploaded once and reused for every recipient.</div>
              <div class="filename" id="sms-media-filename"></div>
              <input type="file" id="sms-media-file" accept="image/*,video/*,application/pdf" onchange="onMediaFilePicked(this)">
            </label>
          </div>

          <div id="sms-media-path-panel" style="display:none">
            <input type="text" id="sms-media-path" placeholder="/content/BILLY.jpg  or  https://example.com/pic.jpg">
            <div class="hint">Server file path (like <code>/content/BILLY.jpg</code>) or a public HTTPS URL.</div>
          </div>
        </div>

        <div class="section-title" style="margin-top:22px">3 · Bulk Recipients</div>
        <div class="sms-field">
          <div class="sms-seg" id="sms-recip-seg">
            <button type="button" data-mode="csv"   class="active" onclick="setRecipMode('csv')">Upload CSV</button>
            <button type="button" data-mode="paste"               onclick="setRecipMode('paste')">Paste list</button>
          </div>

          <div id="sms-recip-csv-panel">
            <label class="sms-drop" for="sms-csv-file">
              <div class="big-icon">📄</div>
              <div style="font-weight:600;">Click to choose a CSV file</div>
              <div class="hint">Columns needed: <code>name,phone</code></div>
              <div class="filename" id="sms-csv-filename"></div>
              <input type="file" id="sms-csv-file" accept=".csv,text/csv" onchange="onCsvFilePicked(this)">
            </label>
            <div id="sms-csv-preview"></div>
          </div>

          <div id="sms-recip-paste-panel" style="display:none">
            <textarea id="sms-recipients-text" rows="5" class="mono" placeholder="Alice Smith,6473959350&#10;Bob Jones,5145550102"></textarea>
            <div class="hint">One per line: <code>name,phone</code> or <code>phone,name</code> or just <code>phone</code>.</div>
          </div>
        </div>

        <div class="section-title" style="margin-top:22px">4 · Quick Test</div>
        <div class="sms-field">
          <div style="display:flex; gap:10px;">
            <input type="text" id="sms-test-phone" placeholder="Phone number (e.g. 4165551234)" style="flex:1;">
            <button id="sms-test-btn" class="btn-outline" onclick="sendTestSms()" style="white-space:nowrap;">Send Test</button>
          </div>
          <div class="hint">Sends a single message using the template and media above.</div>
        </div>

        <div class="sms-actions">
          <button id="sms-send-btn" class="btn-primary" onclick="sendSmsBlast()">Send Bulk Blast</button>
          <button class="btn-outline" onclick="clearSmsForm()">Clear Form</button>
        </div>
      </div>
    </div>

    <div class="section">
      <div class="section-title">Results</div>
      <div id="sms-summary" class="sms-summary" style="display:none"></div>
      <div id="sms-results" class="sms-results">
        <div class="sms-empty">No results yet.</div>
      </div>
    </div>
  </div>
</div>
"""

SMS_SCRIPT = r"""
window.smsMediaMode = 'none';
window.smsRecipMode = 'csv';
window.smsMediaFile = null;
window.smsCsvFile = null;

window.setMediaMode = (m) => {
  window.smsMediaMode = m;
  document.querySelectorAll('#sms-media-seg button').forEach(b =>
    b.classList.toggle('active', b.dataset.mode === m));
  document.getElementById('sms-media-none-panel').style.display = (m === 'none') ? 'block' : 'none';
  document.getElementById('sms-media-upload-panel').style.display = (m === 'upload') ? 'block' : 'none';
  document.getElementById('sms-media-path-panel').style.display = (m === 'path') ? 'block' : 'none';
};

window.setRecipMode = (m) => {
  window.smsRecipMode = m;
  document.querySelectorAll('#sms-recip-seg button').forEach(b =>
    b.classList.toggle('active', b.dataset.mode === m));
  document.getElementById('sms-recip-csv-panel').style.display = (m === 'csv') ? 'block' : 'none';
  document.getElementById('sms-recip-paste-panel').style.display = (m === 'paste') ? 'block' : 'none';
};

window.onMediaFilePicked = (input) => {
  const f = input.files && input.files[0];
  window.smsMediaFile = f || null;
  const lbl = document.getElementById('sms-media-filename');
  lbl.textContent = f ? f.name + ' (' + Math.round(f.size / 1024) + ' KB)' : '';
};

window.onCsvFilePicked = (input) => {
  const f = input.files && input.files[0];
  window.smsCsvFile = f || null;
  const lbl = document.getElementById('sms-csv-filename');
  lbl.textContent = f ? f.name + ' (' + Math.round(f.size / 1024) + ' KB)' : '';
  if (f) previewCsv(f);
};

async function previewCsv(file) {
  try {
    const text = await file.text();
    const lines = text.split(/\r?\n/).filter(l => l.trim()).slice(0, 5);
    document.getElementById('sms-csv-preview').innerHTML =
      '<div style="font-family:ui-monospace,monospace;font-size:13.5px;color:#6b7280;margin-top:12px; background:#f9fafb; padding:10px; border-radius:6px; border:1px solid #e5e7eb;">' +
      lines.map(l => escapeHtml(l)).join('<br>') + '</div>';
  } catch(e) {}
}
function escapeHtml(s){ return (s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function buildFormData(message, recipientsText, isTest = false) {
  const fd = new FormData();
  fd.append('message_template', message);
  fd.append('media_mode', window.smsMediaMode);
  
  if (isTest) {
    fd.append('recip_mode', 'paste');
    fd.append('recipients_text', recipientsText);
  } else {
    fd.append('recip_mode', window.smsRecipMode);
    if (window.smsRecipMode === 'csv') {
      fd.append('recipients_csv', window.smsCsvFile, window.smsCsvFile.name);
    } else {
      fd.append('recipients_text', recipientsText);
    }
  }

  if (window.smsMediaMode === 'upload') {
    fd.append('media_file', window.smsMediaFile, window.smsMediaFile.name);
  } else if (window.smsMediaMode === 'path') {
    fd.append('media_path', document.getElementById('sms-media-path').value.trim());
  }
  return fd;
}

window.sendSmsBlast = async () => {
  const btn = document.getElementById('sms-send-btn');
  const template = document.getElementById('sms-template').value;

  if (!template.trim()) { showToast('Message template is empty', 'error'); return; }
  if (window.smsMediaMode === 'upload' && !window.smsMediaFile) { showToast('Please choose a media file', 'error'); return; }
  if (window.smsMediaMode === 'path' && !document.getElementById('sms-media-path').value.trim()) { showToast('Please enter a media URL or server path', 'error'); return; }
  if (window.smsRecipMode === 'csv' && !window.smsCsvFile) { showToast('Please choose a CSV file', 'error'); return; }

  if (!confirm('Send this blast now?')) return;

  btn.disabled = true;
  btn.textContent = 'Starting...';

  try {
    const fd = buildFormData(template, document.getElementById('sms-recipients-text').value, false);
    const r = await fetch('/api/sms/send', { method: 'POST', credentials: 'include', body: fd });
    if (!r.ok) {
      const err = await r.json().catch(()=>({detail:r.statusText}));
      showToast('Send failed: ' + (err.detail || r.statusText), 'error');
      btn.disabled = false;
      btn.textContent = 'Send Bulk Blast';
      return;
    }
    showToast('Blast started in background!', 'ok');
    document.getElementById('sms-summary').style.display = 'none';
    document.getElementById('sms-results').innerHTML = '<div class="sms-empty">Sending messages...</div>';

  } catch (e) {
    showToast('Send failed: ' + e.message, 'error');
    btn.disabled = false;
    btn.textContent = 'Send Bulk Blast';
  }
};

window.sendTestSms = async () => {
  const btn = document.getElementById('sms-test-btn');
  const phone = document.getElementById('sms-test-phone').value.trim();
  const template = document.getElementById('sms-template').value;

  if (!phone) { showToast('Please enter a test phone number', 'error'); return; }
  if (!template.trim()) { showToast('Message template is empty', 'error'); return; }
  if (window.smsMediaMode === 'upload' && !window.smsMediaFile) { showToast('Please choose a media file', 'error'); return; }
  if (window.smsMediaMode === 'path' && !document.getElementById('sms-media-path').value.trim()) { showToast('Please enter a media URL or server path', 'error'); return; }

  btn.disabled = true;
  btn.textContent = 'Sending...';

  try {
    const fd = buildFormData(template, phone, true);
    const r = await fetch('/api/sms/send', { method: 'POST', credentials: 'include', body: fd });
    if (!r.ok) {
      const err = await r.json().catch(()=>({detail:r.statusText}));
      showToast('Test failed: ' + (err.detail || r.statusText), 'error');
      return;
    }
    showToast(`Test started for ${phone}`, 'ok');
  } catch (e) {
    showToast('Test failed: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Send Test';
  }
};

window.clearSmsForm = () => {
  window.smsMediaFile = null;
  window.smsCsvFile = null;
  document.getElementById('sms-media-file').value = '';
  document.getElementById('sms-csv-file').value = '';
  document.getElementById('sms-media-filename').textContent = '';
  document.getElementById('sms-csv-filename').textContent = '';
  document.getElementById('sms-csv-preview').innerHTML = '';
  document.getElementById('sms-media-path').value = '';
  document.getElementById('sms-recipients-text').value = '';
  document.getElementById('sms-test-phone').value = '';
  document.getElementById('sms-results').innerHTML = '<div class="sms-empty">No results yet.</div>';
  document.getElementById('sms-summary').style.display = 'none';
  setMediaMode('none');
  setRecipMode('csv');
  document.getElementById('sms-send-btn').disabled = false;
  document.getElementById('sms-send-btn').textContent = 'Send Bulk Blast';
};

window.handleSmsProgress = (msg) => {
  renderSmsResults(msg);
  const btn = document.getElementById('sms-send-btn');
  if (msg.results.length >= msg.total) {
    btn.disabled = false;
    btn.textContent = 'Send Bulk Blast';
    showToast(`Blast finished: ${msg.sent} sent`, msg.sent === msg.total ? 'ok' : 'error');
  } else {
    btn.disabled = true;
    btn.textContent = `Sending (${msg.results.length}/${msg.total})...`;
  }
};

function renderSmsResults(data) {
  const wrap = document.getElementById('sms-results');
  const summary = document.getElementById('sms-summary');
  summary.style.display = 'flex';
  summary.innerHTML = `
    <div><div class="lbl">Sent</div><div class="big" style="color:#166534">${data.sent}</div></div>
    <div><div class="lbl">Processed</div><div class="big">${data.results.length} / ${data.total}</div></div>
  `;
  const reversed = [...data.results].reverse();
  wrap.innerHTML = reversed.map(r => `
    <div class="sms-row">
      <div class="phone">${escapeHtml(r.phone)}</div>
      <div style="font-weight:600;">${escapeHtml(r.name) || '—'}</div>
      <div style="color:#6b7280;">${escapeHtml(r.channel) || '—'}</div>
      <div class="${r.ok ? 'ok' : 'err'}">${r.ok ? '✓ sent' : '✗ ' + escapeHtml(r.error || 'failed')}</div>
    </div>
  `).join('') || '<div class="sms-empty">Processing...</div>';
}

document.addEventListener('DOMContentLoaded', () => {
  setMediaMode('none');
  setRecipMode('csv');
});
"""

def _send_sms_or_mms(destination: str, message: str, media_input: str = "") -> dict:
    if not (SMS_USERNAME and SMS_PASSWORD and SMS_SOURCE_DID):
        log.error("SMS failed: SMS_USERNAME, SMS_PASSWORD, or SMS_SOURCE_DID missing from .env")
        return {"ok": False, "channel": "", "error": "SMS not configured in .env"}
        
    auth = HTTPBasicAuth(SMS_USERNAME, SMS_PASSWORD)
    media_val = (media_input or "").strip()
    mms_error = ""
    
    # Try MMS first if media is provided
    if media_val:
        is_local_file = not media_val.startswith("http://") and not media_val.startswith("https://")
        if is_local_file:
            data = {"source_did": SMS_SOURCE_DID, "destination": destination, "message": message}
            try:
                if os.path.exists(media_val):
                    with open(media_val, "rb") as f:
                        response = requests.post(MMS_API_URL, auth=auth, data=data, files={"media_1": f}, timeout=15)
                    if response.status_code in (200, 201, 202): 
                        return {"ok": True, "channel": "MMS", "error": ""}
                    mms_error = f"HTTP {response.status_code}: {response.text[:100]}"
                    log.error(f"MMS Failed: {mms_error}")
                else: 
                    mms_error = "file not found"
                    log.error(f"MMS Failed: {mms_error}")
            except Exception as e: 
                mms_error = f"MMS exception: {e}"
                log.error(f"MMS Failed: {mms_error}")
        else:
            payload = {"source_did": SMS_SOURCE_DID, "destination": destination, "message": message, "media_urls": [media_val]}
            try:
                response = requests.post(MMS_API_URL, auth=auth, json=payload, timeout=30)
                if response.status_code in (200, 201, 202): 
                    return {"ok": True, "channel": "MMS URL", "error": ""}
                mms_error = f"HTTP {response.status_code}: {response.text[:100]}"
                log.error(f"MMS URL Failed: {mms_error}")
            except Exception as e: 
                mms_error = f"MMS exception: {e}"
                log.error(f"MMS URL Failed: {mms_error}")
                
    # Fallback to SMS
    sms_payload = {"source_did": SMS_SOURCE_DID, "destination": destination, "message": message}
    try:
        response = requests.post(SMS_API_URL, auth=auth, json=sms_payload, timeout=30)
        if response.status_code in (200, 201, 202): 
            return {"ok": True, "channel": "SMS", "error": mms_error}
            
        err_msg = f"HTTP {response.status_code}: {response.text[:100]}"
        log.error(f"SMS Failed to {destination}: {err_msg}")
        return {"ok": False, "channel": "SMS", "error": err_msg}
    except Exception as e: 
        log.error(f"SMS Exception to {destination}: {e}")
        return {"ok": False, "channel": "SMS", "error": f"SMS exception: {e}"}

def _parse_recipients_csv(raw_bytes: bytes) -> list[dict]:
    text = raw_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames: return []
    field_map = {f.lower().strip(): f for f in reader.fieldnames}
    phone_key = field_map.get("phone") or field_map.get("number") or field_map.get("mobile")
    if not phone_key: return []
    out = []
    for row in reader:
        phone = (row.get(phone_key) or "").strip()
        if phone: out.append({"phone": phone, "name": (row.get("name") or "").strip()})
    return out

def _parse_recipients_text(text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line: continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) == 1: out.append({"phone": parts[0], "name": ""}); continue
        a, b = parts[0], parts[1]
        if all(c.isdigit() or c in "+-() " for c in a): out.append({"phone": a, "name": b})
        else: out.append({"phone": b, "name": a})
    return [r for r in out if r["phone"]]

async def _process_sms_blast(recipients: list[dict], template: str, media_path: str):
    results = []
    total = len(recipients)
    await hub.broadcast({"type": "sms_progress", "sent": 0, "total": total, "results": []})

    for r in recipients:
        phone = (r["phone"] or "").strip()
        if not phone: continue
        name = (r.get("name") or "").strip() or "Valued Customer"
        message = template.replace("{name}", name)
        try: result = await asyncio.to_thread(_send_sms_or_mms, phone, message, media_path)
        except Exception as e: result = {"ok": False, "channel": "", "error": str(e)}

        results.append({"phone": phone, "name": name, "ok": bool(result.get("ok")), "channel": result.get("channel", ""), "error": result.get("error", "")})
        sent = sum(1 for x in results if x["ok"])
        await hub.broadcast({"type": "sms_progress", "sent": sent, "total": total, "results": results})
        if SMS_THROTTLE_SECONDS > 0: await asyncio.sleep(SMS_THROTTLE_SECONDS)

@router.get("/sms", response_class=HTMLResponse)
def page_sms(request: Request):
    sess = get_session(request)
    if not sess: return RedirectResponse("/login", status_code=302)
    if sess["role"] != "manager": return RedirectResponse(home_for_role(sess["role"]), status_code=302)
    
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SMS · Bundl Box</title>
<style>{SMS_CSS}</style>
</head><body style="display:flex;flex-direction:column;min-height:100vh;">
<header class="topbar">
  <div class="topbar-left">
    <div class="brand"><div class="brand-logo">BB</div><div class="brand-title">Bundl Box</div></div>
        <nav class="top-nav">
      <a href="/wallboard">Wallboard</a>
      <a href="/manager">Manager</a>
      <a href="/agent">Agent View</a>
      <a href="/call-history">Call History</a>
      <a href="/sms" class="active">SMS</a>
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
{SMS_BODY}
</main>
<script>
function startClock(){{ const t = document.getElementById('clock'); const d = document.getElementById('clock-date'); if (!t) return; const tick = () => {{ const n=new Date(); t.textContent=n.toLocaleTimeString('en-GB'); d.textContent=n.toLocaleDateString('en-GB',{{weekday:'long',day:'numeric',month:'short',year:'numeric'}}); }}; tick(); setInterval(tick,1000); }}
function showToast(msg, kind=''){{ const el = document.createElement('div'); el.className = 'toast ' + kind; el.textContent = msg; document.body.appendChild(el); setTimeout(() => el.remove(), 5000); }}
(function connectWS(){{ const proto = location.protocol === 'https:' ? 'wss' : 'ws'; const ws = new WebSocket(proto+'://'+location.host+'/ws'); ws.onclose = () => {{ setTimeout(connectWS, 2000); }}; ws.onmessage = (e) => {{ const msg = JSON.parse(e.data); if (msg.type === 'sms_progress') {{ if (typeof window.handleSmsProgress === 'function') window.handleSmsProgress(msg); }} }}; }})();
startClock();
{SMS_SCRIPT}
</script></body></html>"""

@router.post("/api/sms/send")
async def api_sms_send(
    request: Request, message_template: str = Form(...), media_mode: str = Form("none"),
    media_path: str = Form(""), media_file: UploadFile | None = File(None),
    recip_mode: str = Form("csv"), recipients_text: str = Form(""), recipients_csv: UploadFile | None = File(None)
):
    sess = get_session(request)
    if not sess or sess["role"] != "manager": raise HTTPException(403, "manager only")
    
    # DEBUG
    log.info(f"Received SMS request. mode={recip_mode}, media={media_mode}, template_len={len(message_template)}")
    
    recipients = _parse_recipients_csv(await recipients_csv.read()) if recip_mode == "csv" else _parse_recipients_text(recipients_text)
    log.info(f"Parsed {len(recipients)} recipients.")
    
    if len(recipients) == 0:
        raise HTTPException(400, "No valid recipients found.")

    resolved_media = ""
    if media_mode == "upload" and media_file and media_file.filename:
        safe_name = f"{secrets.token_hex(6)}_{Path(media_file.filename).name}"
        saved_path = SMS_UPLOAD_DIR / safe_name
        with open(saved_path, "wb") as f: shutil.copyfileobj(media_file.file, f)
        resolved_media = str(saved_path)
    elif media_mode == "path" and media_path.strip(): resolved_media = media_path.strip()

    asyncio.create_task(_process_sms_blast(recipients, message_template, resolved_media))
    return {"ok": True, "status": "started"}