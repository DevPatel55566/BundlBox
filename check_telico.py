"""One-shot Telico diagnostic. Run: python check_telico.py"""
import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s :: %(message)s")

from dotenv import load_dotenv
load_dotenv()

import telico_client as tc

print("=" * 60)
print("CONFIG")
print("=" * 60)
print(f"  BASE_URL   : {tc.BASE_URL}")
print(f"  USERNAME   : {'<' + tc.USERNAME + '>' if tc.USERNAME else '<MISSING>'}")
print(f"  PASSWORD   : {'*' * len(tc.PASSWORD) if tc.PASSWORD else '<MISSING>'}")
print(f"  CUSTOMER_ID: {tc.CUSTOMER_ID}")

if not tc.is_configured():
    print("\n❌ Credentials missing — set TELICO_USERNAME / TELICO_PASSWORD in .env")
    raise SystemExit(1)

print("\n" + "=" * 60)
print("LOGIN")
print("=" * 60)
try:
    s = tc._session_get()
    print("  ✓ session established")
except Exception as e:
    print(f"  ❌ {e}")
    raise SystemExit(1)

print("\n" + "=" * 60)
print("PROTECTED PAGE PROBE")
print("=" * 60)
html = tc._get("/panel/state/")
print(f"  /panel/state/  -> {len(html) if html else 0} chars")
if html and "<phones>" in html:
    import re
    n_phones = len(re.findall(r"<phone\b", html))
    print(f"    looks authenticated — {n_phones} phones parsed")
else:
    print("    ⚠ unexpected response, dumping head:")
    print(f"    {(html or '')[:300]!r}")

print("\n" + "=" * 60)
print("GROUPS PAGE")
print("=" * 60)
html = tc._get("/features/times/groups/")
print(f"  /features/times/groups/  -> {len(html) if html else 0} chars")
if html:
    import re
    links = re.findall(r'href="([^"]*groups/edit/[^"]*)"', html)
    print(f"  edit links found: {links[:10] or 'NONE'}")
    # dump a slice so we can eyeball the structure
    i = html.find("groups/edit")
    print(f"  sample around first 'groups/edit': {html[max(0,i-200):i+200]!r}")

print("\n" + "=" * 60)
print("GROUP 2303 PERIODS")
print("=" * 60)
html = tc._get("/features/times/groups/edit/?id=2303")
print(f"  /features/times/groups/edit/?id=2303  -> {len(html) if html else 0} chars")
if html:
    import re
    plinks = re.findall(r'href="([^"]*periods/edit/[^"]*)"', html)
    print(f"  period links found: {plinks[:20] or 'NONE'}")
    i = html.find("periods/edit")
    if i >= 0:
        print(f"  sample: {html[max(0,i-200):i+200]!r}")

print("\n" + "=" * 60)
print("RAW SAVE ATTEMPT (Monday 10:00–11:00, repeats)")
print("=" * 60)
ok = tc.save_time_period(2303, None, {
    "name": "Mon", "start": "10:00", "end": "11:00",
    "repeats": True, "days_week": ["1"],
})
print(f"  save_time_period returned: {ok}")
print("\nIf this returned True but nothing changed in Telico's UI → the form")
print("payload or field names are wrong. Capture the POST body and compare")
print("against what Telico's own form sends (DevTools → Network while saving")
print("a period manually).")