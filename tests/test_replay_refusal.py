"""A request that explicitly asks for X-Replay: saved is refused before any
work when saved mode can't be honored: zero jobs, zero provider calls.
Requests without the header behave as before.
"""
import json
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402
import requests  # noqa: E402

import app  # noqa: E402
import replay_service as rs  # noqa: E402


class Recorder:
    """Records every outbound HTTP request and AI client call."""

    def __init__(self):
        self.http = []
        self.ai = []
        self.threads = []


@pytest.fixture
def rec(monkeypatch):
    r = Recorder()

    def send(self, request, **kw):
        r.http.append((request.method, request.url))
        resp = requests.models.Response()
        resp.status_code = 200
        resp._content = json.dumps({"results": [], "data": [], "organic_results": []}).encode()
        resp.headers["Content-Type"] = "application/json"
        resp.url = request.url
        resp.request = request
        return resp

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)

    class _Models:
        def list(self):
            r.ai.append("openai.models.list")
            return []

        def retrieve(self, *a, **k):
            r.ai.append("anthropic.models.retrieve")
            return type("M", (), {"id": "x"})()

    class _Completions:
        def create(self, **kw):
            r.ai.append("openai.create")
            raise RuntimeError("should not be called")

    class _Chat:
        completions = _Completions()

    class _GPT:
        chat = _Chat()
        models = _Models()

    class _Messages:
        def create(self, **kw):
            r.ai.append("anthropic.create")
            raise RuntimeError("should not be called")

    class _Claude:
        messages = _Messages()
        models = _Models()

    monkeypatch.setattr(app, "gpt_client", _GPT())
    monkeypatch.setattr(app, "claude_client", _Claude())
    monkeypatch.setattr(app, "init_clients", lambda: None)

    class _Thread:
        def __init__(self, target=None, **kw):
            self.target = target

        def start(self):
            r.threads.append(self.target)

    monkeypatch.setattr(app, "Thread", _Thread)
    monkeypatch.setattr(app, "EXA_API_KEY", "exa-test")
    monkeypatch.setattr(app, "FIRECRAWL_API_KEY", "fc-test")
    monkeypatch.setattr(app, "SERP_API_KEY", "serp-test")
    monkeypatch.setattr(app, "get_cached", lambda ck: None)
    monkeypatch.setattr(app, "api_budget_ok", lambda provider, amount=1: True)
    monkeypatch.setattr(rs, "_db_session", lambda: (_ for _ in ()).throw(RuntimeError("no db")))
    rs.install_http_replay()
    yield r
    rs.set_request_mode("off")


OWNER = {"X-Agent-Token": None}   # filled per test (MASTER_CODE read at runtime)

AGENT_BODY = {"business_name": "Union Cafe", "location": "New York, NY",
              "website": "https://unioncafe.example", "industry": "cafe"}
SCOUT_BODY = {"niche": "cafe", "location": "Harlem, NY", "user_query": "cafes in Harlem"}


def _post(path, body, token, replay="saved"):
    h = {"X-Agent-Token": token}
    if replay:
        h["X-Replay"] = replay
    return app.app.test_client().post(path, json=body, headers=h)


def _assert_nothing_happened(rec, jobs_before):
    assert rec.http == [], rec.http
    assert rec.ai == [], rec.ai
    assert rec.threads == []
    assert len(app.JOBS) == jobs_before


@pytest.mark.parametrize("path,body", [("/agent", AGENT_BODY), ("/prospect", SCOUT_BODY)])
def test_missing_owner_code(rec, monkeypatch, path, body):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    n = len(app.JOBS)
    for token in ("not-a-code", ""):
        r = _post(path, body, token)
        assert r.status_code == 409 and r.get_json()["replay_refused"] == "owner_code_required"
    _assert_nothing_happened(rec, n)


@pytest.mark.parametrize("path,body", [("/agent", AGENT_BODY), ("/prospect", SCOUT_BODY)])
def test_replay_disabled_on_the_server(rec, monkeypatch, path, body):
    monkeypatch.setattr(rs, "REPLAY_ON", False)
    n = len(app.JOBS)
    r = _post(path, body, app.MASTER_CODE)
    assert r.status_code == 409 and r.get_json()["replay_refused"] == "replay_not_enabled"
    assert "Nothing was started or sent" in r.get_json()["error"]
    _assert_nothing_happened(rec, n)


def test_replay_module_unavailable(rec, monkeypatch):
    monkeypatch.setattr(app, "replay_service", None)
    n = len(app.JOBS)
    r = _post("/agent", AGENT_BODY, app.MASTER_CODE)
    assert r.status_code == 409 and r.get_json()["replay_refused"] == "replay_not_enabled"
    _assert_nothing_happened(rec, n)


@pytest.mark.parametrize("method,path,body", [
    ("post", "/dev/search/compare", {"queries": ["cafe Harlem"]}),
    ("get", "/health/providers", None),
])
def test_always_live_routes(rec, monkeypatch, method, path, body):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    n = len(app.JOBS)
    c = app.app.test_client()
    h = {"X-Agent-Token": app.MASTER_CODE, "X-Replay": "saved"}
    r = c.post(path, json=body, headers=h) if method == "post" else c.get(path, headers=h)
    assert r.status_code == 409 and r.get_json()["replay_refused"] == "always_live_route"
    _assert_nothing_happened(rec, n)


def test_honorable_saved_request_is_not_refused(rec, monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    r = _post("/dev/replay/clear", {}, app.MASTER_CODE)
    assert r.status_code == 200 and "replay_refused" not in r.get_json()
    with app.app.test_request_context("/agent", headers={"X-Replay": "saved", "X-Agent-Token": app.MASTER_CODE}):
        assert app._saved_replay_refusal() is None


def test_cors_preflight_is_not_refused(rec, monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", False)
    r = app.app.test_client().options("/agent", headers={
        "Origin": "https://yelhao.com", "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "x-replay, x-agent-token, content-type"})
    assert r.status_code in (200, 204)


# ── ordinary requests without the header are unchanged ─────────────────────

def test_no_header_compare_still_runs_live(rec, monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    r = _post("/dev/search/compare", {"queries": ["cafe Harlem"]}, app.MASTER_CODE, replay=None)
    assert r.status_code == 200 and len(rec.http) == 3          # exa, firecrawl, serpapi


def test_no_header_bad_code_gets_the_usual_401(rec, monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    r = _post("/agent", AGENT_BODY, "not-a-code", replay=None)
    assert r.status_code == 401 and "replay_refused" not in r.get_json()


@pytest.mark.parametrize("replay", ["ai", "all", "off", "garbage"])
def test_other_header_values_are_not_refused(rec, monkeypatch, replay):
    monkeypatch.setattr(rs, "REPLAY_ON", False)
    r = _post("/agent", AGENT_BODY, "not-a-code", replay=replay)
    assert r.status_code == 401                                  # same as no header
