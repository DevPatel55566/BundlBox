"""Vercel Python entrypoint: re-export the FastAPI app."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import app  # noqa: E402,F401
from starlette.middleware.base import BaseHTTPMiddleware  # noqa: E402


class _ScopeDebugMiddleware(BaseHTTPMiddleware):
    """Temporary diagnostic: echo the ASGI scope path back in response headers.

    Vercel rewrites every request to /api/index.py; if the runtime hands
    FastAPI the rewritten path instead of the original one, every route 404s.
    These headers show what the app actually sees, so the fix can target it.
    """

    async def dispatch(self, request, call_next):
        response = await call_next(request)
        scope = request.scope
        response.headers["x-scope-path"] = scope.get("path", "")
        raw = scope.get("raw_path", b"")
        response.headers["x-scope-raw-path"] = (
            raw.decode("latin-1", "ignore") if isinstance(raw, bytes) else str(raw)
        )
        response.headers["x-scope-root-path"] = scope.get("root_path", "")
        return response


app.add_middleware(_ScopeDebugMiddleware)
