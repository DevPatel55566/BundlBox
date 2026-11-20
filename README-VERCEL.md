# BundlBox on Vercel

FastAPI app deployed as a Vercel Python project: the root `app.py` (with
`app = FastAPI()`) is the entrypoint and serves every route directly.
`vercel.json` only defines the daily cron — do NOT add a rewrite to
`/api/index.py`: rewrites in a Python project map to paths *inside* the
app, so rewriting everything to `/api/index.py` makes every route 404.
There is no `api/` directory anymore.

## Deploy

Connect this repo in Vercel (import `DevPatel55566/BundlBox`), no build
command needed. Python deps come from `requirements.txt`.

## Env vars to set in Vercel

- `SESSION_SECRET` — long random string (signs login cookies; required in
  production, otherwise logins use the dev default).
- `MANAGER_USERNAME` / `MANAGER_PASSWORD` (default colleen / pw123456)
- `WALLBOARD_USERNAME` / `WALLBOARD_PASSWORD` (default wallboard / wallboard)
- `CRON_SECRET` — optional; if set, `/api/cron/sync` needs
  `Authorization: Bearer <secret>`. Vercel Cron calls it once a day (13:00 UTC, 8am Toronto) — the Hobby plan only allows daily crons, so the every-5-minutes schedule breaks the build.
- Telico / SMS (only if you want live sync): `TELICO_USERNAME`,
  `TELICO_PASSWORD`, `TELICO_CUSTOMER_ID`, `TELICO_BASE_URL`,
  `SMS_USERNAME`, `SMS_PASSWORD`, `SMS_SOURCE_DID`, `SMS_API_TOKEN`.

## Known Vercel limits (by design, not bugs)

- **Database is ephemeral.** SQLite lives at `/tmp/cc.db` on Vercel, so
  agents, statuses and history reset on cold start / redeploy. For real
  persistence this needs an external DB (e.g. Postgres).
- **No live WebSocket.** `/ws` cannot stay open on serverless, so the
  wallboard / manager / agent pages poll `GET /api/snapshot` every 5s.
- **No background loops.** The Telico sync, break-expiry and STOP-listener
  loops only run on a long-lived host. On Vercel, Vercel Cron hits
  `GET /api/cron/sync` once a day for a single Telico sync pass (Hobby plan limit; upgrade to Pro for more frequent crons). Because the
  database is ephemeral, the first wallboard/reports read on a fresh
  instance also triggers one sync pass automatically (at most once every
  5 minutes), so the pages refill themselves instead of staying blank.
- **Reports only cover today on Vercel.** Past days' call data is lost
  whenever the instance restarts, so date ranges before today come back
  empty. For real report history this needs an external DB (e.g. Postgres).
- **Logins are stateless signed cookies** (HMAC, `SESSION_SECRET`), so they
  survive across serverless instances.
