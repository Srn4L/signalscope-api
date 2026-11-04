"""
replay_service.py - Dev replay for Yelhao (owner test requests only).

Goal: test Yelhao repeatedly without surprise API usage.

Who gets replay: nobody by default. YELHAO_DEV_REPLAY=1 on the server only
ALLOWS it. A request opts in with the header  X-Replay: <mode>  and only when
it carries the owner (master) code. Every other request, including every
request from other users, runs fully live. The mode is carried into the
background threads a request starts (deep analysis, batch, parallel fetches),
so a whole run uses one mode.

Modes (per request):
  all    Capture. Reuse saved successful replies; make every other call live
         and save its outcome (successes and failures), so a later "saved"
         run can follow the same path.
  saved  Saved data only. Every outside call is answered from saved data,
         including saved failures. If anything is missing, the call is
         blocked (never sent), the item is listed, and the run ends with a
         "saved data missing" result instead of a report. Live calls: 0.
  ai     Only GPT and Claude replies are reused; searches and fetches are
         live. For measuring search quality. Changed evidence makes a new
         prompt, so the AI call is live then too.
  off    (no header) fully live.

Each opted-in run counts its live provider calls, replayed calls and missing
items; the app returns that summary with the result.

What can be saved and replayed:
- GET requests made with `requests` (search APIs, Google Places, web pages)
- POST requests to search and scrape APIs only (Exa, Firecrawl, Tavily)
- GPT and Claude replies (by model + full prompt + settings)
- DuckDuckGo results (by query), and search steps skipped during capture
Never saved: CRM writes (HubSpot, Salesforce, Airtable) and any other POST.
In "saved" mode those are blocked and listed as missing. /health requests are
always live.

Storage: memory first, then a Postgres table (dev_replay_cache) created on
first use, so saved data survives deploys. If the database is unavailable,
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
REPLAY_ON = _ALLOW_RAW in ("1", "true", "on", "allow", "all", "ai", "saved")
REPLAY_MODE = "owner_opt_in" if REPLAY_ON else "off"
MODES = ("ai", "all", "saved")

try:
    REPLAY_TTL_SECONDS = int(os.environ.get("REPLAY_TTL_DAYS") or 14) * 86400
except Exception:
    REPLAY_TTL_SECONDS = 14 * 86400
MAX_BODY_BYTES = 2_000_000

STATS = {"hits": 0, "misses": 0, "saved": 0, "errors": 0}
_MEM: dict = {}            # key -> (payload, saved_at)
_LOCK = threading.Lock()
_TABLE_READY = False

# Called with the request URL after a replayed HTTP hit in "all"/"ai" mode,
# so the caller can give back the daily API budget it reserved.
on_http_hit = None

_SECRET_NAMES = {"api_key", "apikey", "key", "token", "access_token", "x-api-key", "authorization"}
_POST_HOSTS = ("api.exa.ai", "api.firecrawl.dev", "api.tavily.com")
_NEVER_HOSTS = ("hubapi.com", "hubspot.com", "salesforce.com", "force.com",
                "airtable.com", "api.openai.com", "api.anthropic.com")


class ReplayMiss(Exception):
    """Raised instead of a live call when saved data is missing ("saved" mode)."""


def _http_miss_class():
    # Callers that catch request errors should treat a blocked call like a
    # failed network call, so the HTTP version subclasses RequestException.
    import requests

    class ReplayHTTPMiss(ReplayMiss, requests.exceptions.RequestException):
        pass
    return ReplayHTTPMiss


# ── per-request mode and run summary ────────────────────────────────────────

class RunState:
    def __init__(self, mode: str):
        self.mode = mode
        self.started = time.time()
        self.live = {}          # provider/host -> count
        self.replayed = 0
        self.missing = []       # [{"kind", "what"}]
        self._seen_missing = set()
        self.lock = threading.Lock()

    def add_live(self, what: str):
        with self.lock:
            self.live[what] = self.live.get(what, 0) + 1

    def add_replayed(self):
        with self.lock:
            self.replayed += 1

    def add_missing(self, kind: str, what: str):
        with self.lock:
            if (kind, what) not in self._seen_missing:
                self._seen_missing.add((kind, what))
                self.missing.append({"kind": kind, "what": what[:300]})

    def summary(self) -> dict:
        with self.lock:
            return {
                "mode":           self.mode,
                "live_calls":     sum(self.live.values()),
                "live_by_target": dict(sorted(self.live.items())),
                "replayed_calls": self.replayed,
                "missing_count":  len(self.missing),
                "missing":        list(self.missing[:50]),
                "seconds":        round(time.time() - self.started, 1),
            }

    def has_activity(self) -> bool:
        with self.lock:
            return bool(self.live or self.replayed or self.missing)


_REQ_MODE: contextvars.ContextVar = contextvars.ContextVar("yelhao_replay_mode", default="off")
_RUN: contextvars.ContextVar = contextvars.ContextVar("yelhao_replay_run", default=None)


def mode_from_header(header_value, is_owner: bool) -> str:
    v = (header_value or "").strip().lower()
    if not REPLAY_ON or not is_owner or v not in MODES:
        return "off"
    return v


def set_request_mode(mode: str):
    """Call at the start of EVERY request (worker threads are reused)."""
    mode = mode if mode in MODES else "off"
    _REQ_MODE.set(mode)
    _RUN.set(RunState(mode) if mode != "off" else None)


def current_mode() -> str:
    return _REQ_MODE.get()


def run_state():
    return _RUN.get()


def carry(fn):
    """
    Wrap fn so it runs with the caller's replay mode and run summary, in any
    thread. Threads and thread pools do not inherit context variables.
    """
    ctx = contextvars.copy_context()

    def run(*args, **kwargs):
        return ctx.copy().run(fn, *args, **kwargs)
    return run


def _bypass() -> bool:
    """Provider health checks always reach the real service."""
    try:
        from flask import has_request_context, request
        return has_request_context() and request.path.startswith("/health")
    except Exception:
        return False


def _effective_mode() -> str:
    if not REPLAY_ON or _bypass():
        return "off"
    return _REQ_MODE.get()


def _note_live(what: str):
    rs = _RUN.get()
    if rs is not None:
        rs.add_live(what)


def _note_replayed():
    STATS["hits"] += 1
    rs = _RUN.get()
    if rs is not None:
        rs.add_replayed()


def _note_missing(kind: str, what: str):
    STATS["misses"] += 1
    rs = _RUN.get()
    if rs is not None:
        rs.add_missing(kind, what)
    print(f"[Replay] saved data missing ({kind}): {what[:120]} - call blocked", flush=True)


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
    from db import get_db_session  # optional dependency
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
    return {"allowed": REPLAY_ON, "who": "owner requests with X-Replay: all|saved|ai",
            "ttl_days": REPLAY_TTL_SECONDS // 86400, **STATS,
            "in_memory": len(_MEM)}


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


def _target(url: str) -> str:
    p = urlparse(url)
    return (p.hostname or "") + (p.path or "")


def _describe(url: str, params=None) -> str:
    """Host, path and non-secret parameters, so each missing call is distinct."""
    p = urlparse(_clean_url(url))
    q = dict(parse_qsl(p.query))
    if isinstance(params, dict):
        q.update({k: v for k, v in _strip_secrets(params).items() if v is not None})
    qs = "&".join(f"{k}={str(v)[:60]}" for k, v in sorted(q.items()))
    return (p.hostname or "") + (p.path or "") + (("?" + qs) if qs else "")


def _build_response(saved: dict, url: str):
    import requests
    from requests.structures import CaseInsensitiveDict
    r = requests.models.Response()
    r.status_code = int(saved.get("status", 200))
    r._content = base64.b64decode(saved.get("body_b64", ""))
    r.headers = CaseInsensitiveDict(saved.get("headers") or {})
    r.encoding = saved.get("encoding")
    r.url = url
    r.reason = "replay"
    return r


def _replay_saved(saved: dict, url: str):
    """Reproduce a saved outcome: a response, or the saved network error."""
    if saved.get("exception"):
        import requests
        cls = getattr(requests.exceptions, saved["exception"], None)
        if not (isinstance(cls, type) and issubclass(cls, Exception)):
            cls = requests.exceptions.RequestException
        raise cls(saved.get("message") or "saved network error (replay)")
    return _build_response(saved, url)


def install_http_replay():
    """Patch requests once. Safe to call more than once."""
    import requests
    sess = requests.sessions.Session
    if getattr(sess.request, "_yelhao_replay", False):
        return
    original = sess.request
    HTTPMiss = _http_miss_class()

    def request(self, method, url, params=None, data=None, headers=None, cookies=None,
                files=None, auth=None, timeout=None, allow_redirects=True, proxies=None,
                hooks=None, stream=None, verify=None, cert=None, json=None):
        kwargs = dict(params=params, data=data, headers=headers, cookies=cookies, files=files,
                      auth=auth, timeout=timeout, allow_redirects=allow_redirects,
                      proxies=proxies, hooks=hooks, stream=stream, verify=verify,
                      cert=cert, json=json)
        m = (method or "GET").upper()
        mode = _effective_mode()
        if mode == "off":
            return original(self, method, url, **kwargs)

        can_save = not files and not stream and _replayable(m, url)
        key = None
        if can_save:
            body = data if isinstance(data, (str, bytes, type(None))) else _strip_secrets(data)
            if isinstance(body, bytes):
                body = hashlib.sha256(body).hexdigest()
            key = make_key("http", m, _clean_url(url),
                           _strip_secrets(params) if isinstance(params, dict) else params,
                           body, json)

        if mode == "saved":
            saved = get(key) if key else None
            if saved is None:
                why = "" if can_save else " (this kind of call is never saved)"
                _note_missing("http", f"{m} {_describe(url, params)}{why}")
                raise HTTPMiss(f"saved data missing: {m} {_target(url)}")
            _note_replayed()
            return _replay_saved(saved, url)

        if mode == "all" and key:
            saved = get(key)
            if saved is not None and saved.get("ok", True) and not saved.get("exception"):
                _note_replayed()
                if on_http_hit:
                    try:
                        on_http_hit(url)
                    except Exception:
                        pass
                return _build_response(saved, url)

        # Live call ("ai" mode, unsaveable calls, or capture of a new call)
        _note_live((urlparse(url).hostname or "unknown").lower())
        if mode != "all" or not key:
            return original(self, method, url, **kwargs)
        STATS["misses"] += 1
        try:
            resp = original(self, method, url, **kwargs)
        except requests.exceptions.RequestException as e:
            # Save the failure so a "saved" run takes the same fallback path.
            put(key, {"ok": False, "exception": type(e).__name__, "message": str(e)[:300]})
            raise
        try:
            if len(resp.content or b"") <= MAX_BODY_BYTES:
                put(key, {
                    "ok": resp.status_code < 400,
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
        mode = _effective_mode()
        if mode == "off":
            return original(*args, **kwargs)
        key = make_key(kind, kwargs)
        saved = None if kwargs.get("stream") else get(key)
        if saved is not None:
            try:
                out = model_type.model_validate(saved)
                _note_replayed()
                return out
            except Exception:
                saved = None
        if mode == "saved":
            _note_missing(kind, f"{kind} {kwargs.get('model', '')} reply for this prompt")
            raise ReplayMiss(f"saved data missing: {kind} reply")
        _note_live(kind)
        STATS["misses"] += 1
        out = original(*args, **kwargs)
        if not kwargs.get("stream"):
            try:
                put(key, out.model_dump(mode="json"))
            except Exception:
                pass
        return out

    create._yelhao_replay = True
    resource.create = create


# ── Inputs that are not provider calls ──────────────────────────────────────

def replay_input(name: str, key_parts, value):
    """
    Freeze a non-provider input that changes run to run (for example Scout's
    exposure history and the clock it is compared against).
    capture ("all"): save `value` and return it.
    saved: return the captured value; if none was captured, list it as
    missing and return None (the caller uses `value` and the run reports
    the gap).
    other modes: return `value`.
    """
    mode = _effective_mode()
    if mode not in ("all", "saved"):
        return value
    key = make_key("input", name, key_parts)
    if mode == "all":
        try:
            put(key, json.loads(json.dumps(value, default=str)))
        except Exception:
            pass
        return value
    saved = get(key)
    if saved is None:
        _note_missing("input", f"{name} (not in the capture)")
        return None
    _note_replayed()
    return saved


def is_saved_run() -> bool:
    return _effective_mode() == "saved"


# ── DuckDuckGo and skipped search steps ─────────────────────────────────────

def ddg_get(query: str, max_results: int):
    """
    None when replay does not apply. Otherwise a saved list. In "saved" mode
    a missing entry is listed and [] is returned (no live call).
    """
    mode = _effective_mode()
    if mode not in ("all", "saved"):
        return None
    saved = get(make_key("ddg", query, max_results))
    if saved is not None:
        _note_replayed()
        return saved.get("results") or []
    if mode == "saved":
        _note_missing("duckduckgo", query)
        return []
    return None


def ddg_put(query: str, max_results: int, results: list):
    """Capture a DuckDuckGo outcome, including empty results and skips."""
    if _effective_mode() == "all":
        put(make_key("ddg", query, max_results), {"results": results or []})


def note_live(what: str):
    """For live calls made outside `requests` (DuckDuckGo)."""
    if _effective_mode() != "off":
        _note_live(what)


def note_skip(provider: str, query: str, max_results: int):
    """Capture: this search step was skipped (no key or daily cap)."""
    if _effective_mode() == "all":
        put(make_key("skip", provider, query, max_results), {"skipped": True})


def was_skipped(provider: str, query: str, max_results: int) -> bool:
    """Saved mode: the capture skipped this step, so skip it again."""
    if _effective_mode() != "saved":
        return False
    if get(make_key("skip", provider, query, max_results)) is not None:
        _note_replayed()
        return True
    return False
