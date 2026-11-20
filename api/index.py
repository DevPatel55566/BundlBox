"""Vercel Python entrypoint: re-export the FastAPI app.

Temporary diagnostic: a raw ASGI wrapper answers /__probe__ with the exact
ASGI scope (path, raw_path, root_path, headers) the function receives, so we
can see whether Vercel's rewrite hands FastAPI the original path or the
rewritten /api/index.py path. Everything else is passed straight to the app.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import app as _fastapi_app  # noqa: E402,F401


async def app(scope, receive, send):
    if scope["type"] == "http" and scope.get("path") in ("/__probe__", "/api/__probe__"):
        raw = scope.get("raw_path", b"")
        info = {
            "path": scope.get("path"),
            "raw_path": raw.decode("latin-1", "ignore") if isinstance(raw, bytes) else str(raw),
            "root_path": scope.get("root_path", ""),
            "method": scope.get("method"),
            "query_string": scope.get("query_string", b"").decode("latin-1", "ignore"),
            "headers": {
                k.decode("latin-1", "ignore"): v.decode("latin-1", "ignore")
                for k, v in scope.get("headers", [])
            },
        }
        body = json.dumps(info).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body})
        return
    await _fastapi_app(scope, receive, send)
