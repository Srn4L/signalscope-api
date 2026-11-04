"""
replay_service.py - Dev replay for Yelhao (owner test requests only).

While building and testing, the same businesses get analyzed again and again,
and every run pays for the same outside calls (Google, Exa, SerpAPI, page
fetches, GPT, Claude). Replay saves the first successful reply to each call
and reuses it for later identical calls.

Who gets replay: nobody by default. YELHAO_DEV_REPLAY=1 on the server only
ALLOWS it. A request then opts in with the header  X-Replay: all  or
X-Replay: ai , and only when it carries the owner (master) code. Every other
request, including every request from other users, runs fully live. The
choice is carried into the background threads a request starts (deep
analysis, batch, parallel fetches), so a whole run uses one mode.

Modes (per request):
  all  searches, Places, page fetches, DuckDuckGo and AI replies are reused.
       Use it to test a code change on frozen inputs.
  ai   only GPT and Claude replies are reused; searches and fetches are live.
       Use it to measure search quality while not paying again for an
       identical AI prompt. Changed evidence makes a new prompt, so the AI
       call is live then too.
  off  (no header) fully live.

What can be replayed:
- GET requests made with `requests` (search APIs, Google Places, web pages)
- POST requests to search and scrape APIs only (Exa, Firecrawl, Tavily)
- GPT and Claude replies (by model + full prompt + settings)
- DuckDuckGo results (by query)

Never replayed: CRM writes (HubSpot, Salesforce, Airtable) or any other POST,
failed replies (4xx/5xx), and /health requests.

Storage: memory first, then a small Postgres table (dev_replay_cache) created
on first use, so replays survive deploys. If the database is unavailable,
memory only.
"""

from __future__ import annotations

import base64
import contextvars
import hashlib
import json
import os
import re
import threading
import time
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

_ALLOW_RAW = (os.environ.get("YELHAO_DEV_REPLAY") or "").strip().lower()
# Server-level permission only. Nothing is replayed unless a request opts in.
REPLAY_ON = _ALLOW_RAW in ("1", "true", "on", "allow", "all", "ai")
REPLAY_MODE = "owner_opt_in" if REPLAY_ON else "off"

# Per-request mode: "off", "ai" or "all". Set for every request by
# set_request_mode(); copied into background threads by carry().
_REQ_MODE: contextvars.ContextVar = contextvars.ContextVar("yelhao_replay_mode", default="off")


def mode_from_header(header_value, is_owner: bool) -> str:
    v = (header_value or "").strip().lower()
    if not REPLAY_ON or not is_owner or v not in ("ai", "all"):
        return "off"
    return v


def set_request_mode(mode: str):
    """Call at the start of EVERY request (worker threads are reused)."""
    _REQ_MODE.set(mode if mode in ("ai", "all") else "off")


def current_mode() -> str:
    return _REQ_MODE.get()


def carry(fn):
    """
    Wrap fn so it runs with the caller's replay mode, in any thread.
    Threads and thread pools do not inherit context variables on their own.
    """
    ctx = contextvars.copy_context()

    def run(*args, **kwargs):
        return ctx.copy().run(fn, *args, **kwargs)
    return run


def _http_active() -> bool:
    return REPLAY_ON and _REQ_MODE.get() == "all" and not _bypass()


def _ai_active() -> bool:
    return REPLAY_ON and _REQ_MODE.get() in ("ai", "all") and not _bypass()
try:
    REPLAY_TTL_SECONDS = int(os.environ.get("REPLAY_TTL_DAYS") or 14) * 86400
except Exception:
    REPLAY_TTL_SECONDS = 14 * 86400
MAX_BODY_BYTES = 2_000_000

STATS = {"hits": 0, "misses": 0, "saved": 0, "errors": 0}
_MEM: dict = {}            # key -> (payload, saved_at)
_LOCK = threading.Lock()
_TABLE_READY = False

# Called with the request URL after a replayed HTTP hit, so the caller can
# give back the daily API budget it reserved before the call.
on_http_hit = None

_SECRET_NAMES = {"api_key", "apikey", "key", "token", "access_token", "x-api-key", "authorization"}
_POST_HOSTS = ("api.exa.ai", "api.firecrawl.dev", "api.tavily.com")
_NEVER_HOSTS = ("hubapi.com", "hubspot.com", "salesforce.com", "force.com",
                "airtable.com", "api.openai.com", "api.anthropic.com")


# ── keys ────────────────────────────────────────────────────────────────────

def _strip_secrets(obj):
    if isinstance(obj, dict):
        return {k: _strip_secrets(v) for k, v in obj.items()
                if str(k).lower() not in _SECRET_NAMES}
    if isinstance(obj, (list, tuple)):
        return [_strip_secrets(v) for v in obj]
    return obj


def _clean_url(url: str) -> str:
    try:
        p = urlparse(url)
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if k.lower() not in _SECRET_NAMES]
        # ScraperAPI carries the target page in ?url=; the key is already gone
        return urlunparse(p._replace(query=urlencode(sorted(q))))
    except Exception:
        return re.sub(r"(api_key|key|token)=[^&]+", "", url)


def make_key(kind: str, *parts) -> str:
    raw = json.dumps([kind, _strip_secrets(list(parts))], sort_keys=True, default=str)
    return kind + ":" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]


# ── store ───────────────────────────────────────────────────────────────────

def _db_session():
    from db import get_db_session  # noqa: WPS433 (optional dependency)
    return get_db_session()


def _ensure_table(session):
    global _TABLE_READY
    if _TABLE_READY:
        return
    from sqlalchemy import text
    session.execute(text("""
        CREATE TABLE IF NOT EXISTS dev_replay_cache (
            cache_key  TEXT PRIMARY KEY,
            kind       TEXT,
            payload    JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """))
    _TABLE_READY = True


def get(key: str):
    now = time.time()
    with _LOCK:
        hit = _MEM.get(key)
    if hit and now - hit[1] < REPLAY_TTL_SECONDS:
        return hit[0]
    try:
        from sqlalchemy import text
        with _db_session() as s:
            _ensure_table(s)
            row = s.execute(text("""
                SELECT payload, EXTRACT(EPOCH FROM created_at) FROM dev_replay_cache
                WHERE cache_key = :k
            """), {"k": key}).fetchone()
        if row and now - float(row[1] or 0) < REPLAY_TTL_SECONDS:
            payload = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            with _LOCK:
                _MEM[key] = (payload, float(row[1]))
            return payload
    except Exception as e:
        STATS["errors"] += 1
        if STATS["errors"] in (1, 10, 100):
            print(f"[Replay] store read unavailable ({str(e)[:80]}); memory only", flush=True)
    return None


def put(key: str, payload: dict):
    with _LOCK:
        _MEM[key] = (payload, time.time())
    STATS["saved"] += 1
    try:
        from sqlalchemy import text
        with _db_session() as s:
            _ensure_table(s)
            s.execute(text("""
                INSERT INTO dev_replay_cache (cache_key, kind, payload, created_at)
                VALUES (:k, :kind, CAST(:p AS JSONB), NOW())
                ON CONFLICT (cache_key) DO UPDATE
                    SET payload = EXCLUDED.payload, created_at = NOW()
            """), {"k": key, "kind": key.split(":", 1)[0], "p": json.dumps(payload)})
    except Exception as e:
        STATS["errors"] += 1
        if STATS["errors"] in (1, 10, 100):
            print(f"[Replay] store write unavailable ({str(e)[:80]}); memory only", flush=True)


def clear() -> int:
    with _LOCK:
        n = len(_MEM)
        _MEM.clear()
    try:
        from sqlalchemy import text
        with _db_session() as s:
            _ensure_table(s)
            r = s.execute(text("DELETE FROM dev_replay_cache"))
            n = max(n, r.rowcount or 0)
    except Exception:
        pass
    return n


def status() -> dict:
    return {"allowed": REPLAY_ON, "who": "owner requests with X-Replay: ai|all",
            "ttl_days": REPLAY_TTL_SECONDS // 86400, **STATS,
            "in_memory": len(_MEM)}


def _bypass() -> bool:
    """Provider health checks always reach the real service."""
    try:
        from flask import has_request_context, request
        return has_request_context() and request.path.startswith("/health")
    except Exception:
        return False


# ── HTTP (requests) ─────────────────────────────────────────────────────────

def _replayable(method: str, url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    if any(host == h or host.endswith("." + h) for h in _NEVER_HOSTS):
        return False
    if "/account" in urlparse(url).path:
        return False
    if method == "GET":
        return True
    return method == "POST" and host in _POST_HOSTS


def _build_response(saved: dict, url: str):
    import requests
    from requests.structures import CaseInsensitiveDict
    r = requests.models.Response()
    r.status_code = int(saved.get("status", 200))
    r._content = base64.b64decode(saved.get("body_b64", ""))
    r.headers = CaseInsensitiveDict(saved.get("headers") or {})
    r.encoding = saved.get("encoding")
    r.url = url
    r.reason = "OK (replay)"
    return r


def install_http_replay():
    """Patch requests once. Safe to call more than once."""
    import requests
    sess = requests.sessions.Session
    if getattr(sess.request, "_yelhao_replay", False):
        return
    original = sess.request

    def request(self, method, url, params=None, data=None, headers=None, cookies=None,
                files=None, auth=None, timeout=None, allow_redirects=True, proxies=None,
                hooks=None, stream=None, verify=None, cert=None, json=None):
        kwargs = dict(params=params, data=data, headers=headers, cookies=cookies, files=files,
                      auth=auth, timeout=timeout, allow_redirects=allow_redirects,
                      proxies=proxies, hooks=hooks, stream=stream, verify=verify,
                      cert=cert, json=json)
        m = (method or "GET").upper()
        if files or stream or not _http_active() or not _replayable(m, url):
            return original(self, method, url, **kwargs)

        body = data if isinstance(data, (str, bytes, type(None))) else _strip_secrets(data)
        if isinstance(body, bytes):
            body = hashlib.sha256(body).hexdigest()
        key = make_key("http", m, _clean_url(url),
                       _strip_secrets(params) if isinstance(params, dict) else params,
                       body, json)
        saved = get(key)
        if saved is not None:
            STATS["hits"] += 1
            if on_http_hit:
                try:
                    on_http_hit(url)
                except Exception:
                    pass
            return _build_response(saved, url)

        STATS["misses"] += 1
        resp = original(self, method, url, **kwargs)
        try:
            if resp.status_code < 400 and len(resp.content or b"") <= MAX_BODY_BYTES:
                put(key, {
                    "status": resp.status_code,
                    "headers": {"Content-Type": resp.headers.get("Content-Type", "")},
                    "encoding": resp.encoding,
                    "body_b64": base64.b64encode(resp.content or b"").decode("ascii"),
                })
        except Exception:
            pass
        return resp

    request._yelhao_replay = True
    sess.request = request


# ── AI clients ──────────────────────────────────────────────────────────────

def wrap_ai_clients(gpt_client=None, claude_client=None):
    """Save and reuse GPT and Claude replies keyed by the full request."""
    if not REPLAY_ON:
        return
    if gpt_client is not None:
        _wrap(gpt_client.chat.completions, "openai", _openai_type())
    if claude_client is not None:
        _wrap(claude_client.messages, "anthropic", _anthropic_type())


def _openai_type():
    try:
        from openai.types.chat import ChatCompletion
        return ChatCompletion
    except Exception:
        return None


def _anthropic_type():
    try:
        from anthropic.types import Message
        return Message
    except Exception:
        return None


def _wrap(resource, kind, model_type):
    if model_type is None or getattr(resource.create, "_yelhao_replay", False):
        return
    original = resource.create

    def create(*args, **kwargs):
        if not _ai_active() or kwargs.get("stream"):
            return original(*args, **kwargs)
        key = make_key(kind, kwargs)
        saved = get(key)
        if saved is not None:
            try:
                STATS["hits"] += 1
                return model_type.model_validate(saved)
            except Exception:
                STATS["hits"] -= 1
        STATS["misses"] += 1
        out = original(*args, **kwargs)
        try:
            put(key, out.model_dump(mode="json"))
        except Exception:
            pass
        return out

    create._yelhao_replay = True
    resource.create = create


# ── DuckDuckGo ──────────────────────────────────────────────────────────────

def ddg_get(query: str, max_results: int):
    if not _http_active():
        return None
    saved = get(make_key("ddg", query, max_results))
    if saved is not None:
        STATS["hits"] += 1
        return saved.get("results") or []
    return None


def ddg_put(query: str, max_results: int, results: list):
    if results and _http_active():
        put(make_key("ddg", query, max_results), {"results": results})
