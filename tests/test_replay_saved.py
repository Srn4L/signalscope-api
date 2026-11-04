"""Owner "saved data only" replay: capture once, rerun with zero live calls,
stop and list anything missing.

Offline: the network is replaced by a fake transport.
"""
import json
import os
import threading

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402
import requests  # noqa: E402

import app  # noqa: E402
import replay_service as rs  # noqa: E402


class FakeNet:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def send(self, request):
        self.calls.append((request.method, request.url))
        out = self.handler(request)
        status, payload = out
        r = requests.models.Response()
        r.status_code = status
        r._content = json.dumps(payload).encode()
        r.headers["Content-Type"] = "application/json"
        r.url = request.url
        r.request = request
        r.encoding = "utf-8"
        return r


@pytest.fixture
def net(monkeypatch):
    def install(handler):
        fake = FakeNet(handler)
        monkeypatch.setattr(requests.adapters.HTTPAdapter, "send",
                            lambda self, request, **kw: fake.send(request))
        return fake
    return install


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    monkeypatch.setattr(rs, "_db_session", lambda: (_ for _ in ()).throw(RuntimeError("no db")))
    monkeypatch.setattr(rs, "on_http_hit", None)
    rs._MEM.clear()
    rs.install_http_replay()
    monkeypatch.setattr(app, "EXA_API_KEY", "exa-test")
    monkeypatch.setattr(app, "FIRECRAWL_API_KEY", "fc-test")
    monkeypatch.setattr(app, "SERP_API_KEY", "")
    monkeypatch.setattr(app, "DDGS", None)
    monkeypatch.setattr(app, "api_budget_ok", lambda provider, amount=1: True)
    yield
    rs.set_request_mode("off")
    rs._MEM.clear()


def _exa(request):
    if "exa.ai" in request.url:
        return 200, {"results": [{"title": "Shiny Nails", "url": "https://www.instagram.com/shinynails/",
                                  "highlights": ["Gel nails in Astoria."]}]}
    return 200, {"data": [{"title": "F", "url": "https://f.com", "description": "f"}]}


def _capture_then_saved(fn):
    rs.set_request_mode("all")
    captured = fn()
    cap = rs.run_state().summary()
    rs.set_request_mode("saved")
    again = fn()
    return captured, again, cap, rs.run_state().summary()


def test_saved_run_reproduces_the_capture_with_zero_live_calls(net):
    fake = net(_exa)
    captured, again, cap, saved = _capture_then_saved(lambda: app.search_web("nail salon Astoria"))
    assert again == captured
    assert cap["live_calls"] == 1 and len(fake.calls) == 1
    assert saved["live_calls"] == 0 and saved["missing_count"] == 0 and saved["replayed_calls"] == 1


def test_missing_saved_data_blocks_the_call_and_lists_it(net):
    fake = net(_exa)
    rs.set_request_mode("saved")
    out = app.search_web("a query that was never captured")
    s = rs.run_state().summary()
    assert fake.calls == []                              # nothing sent
    assert out == []
    assert s["live_calls"] == 0 and s["missing_count"] >= 1
    assert s["missing"][0]["kind"] == "http" and "api.exa.ai/search" in s["missing"][0]["what"]


def test_saved_failures_take_the_same_fallback_path(net):
    # During capture Exa failed (402), so search used Firecrawl. The saved
    # run must replay that failure, not report Exa as missing.
    def handler(request):
        if "exa.ai" in request.url:
            return 402, {"error": "credits"}
        return 200, {"data": [{"title": "F", "url": "https://f.com", "description": "f"}]}

    fake = net(handler)
    captured, again, cap, saved = _capture_then_saved(lambda: app.search_web("cafe Harlem"))
    assert captured[0]["provider"] == "firecrawl" and again == captured
    assert len(fake.calls) == 2
    assert saved["missing_count"] == 0 and saved["live_calls"] == 0


def test_saved_network_errors_are_replayed_as_errors(net):
    def handler(request):
        if "exa.ai" in request.url:
            raise requests.exceptions.ReadTimeout("read timed out")
        return 200, {"data": [{"title": "F", "url": "https://f.com", "description": "f"}]}

    net(handler)
    captured, again, cap, saved = _capture_then_saved(lambda: app.search_web("cafe Harlem"))
    assert again == captured and saved["missing_count"] == 0 and saved["live_calls"] == 0


def test_steps_skipped_during_capture_are_skipped_again(net, monkeypatch):
    # Exa was over its daily cap during the capture.
    monkeypatch.setattr(app, "api_budget_ok", lambda provider, amount=1: provider != "exa")
    fake = net(_exa)
    rs.set_request_mode("all")
    captured = app.search_web("cafe Harlem")
    monkeypatch.setattr(app, "api_budget_ok", lambda provider, amount=1: True)
    rs.set_request_mode("saved")
    again = app.search_web("cafe Harlem")
    s = rs.run_state().summary()
    assert captured == again and captured[0]["provider"] == "firecrawl"
    assert s["missing_count"] == 0 and s["live_calls"] == 0
    assert not any("exa.ai" in c[1] for c in fake.calls)


def test_crm_writes_are_blocked_in_saved_mode(net):
    fake = net(lambda r: (200, {"id": 1}))
    rs.set_request_mode("saved")
    with pytest.raises(requests.exceptions.RequestException):
        requests.post("https://api.hubapi.com/crm/v3/objects/contacts", json={"email": "a@b.c"})
    s = rs.run_state().summary()
    assert fake.calls == [] and s["live_calls"] == 0
    assert "never saved" in s["missing"][0]["what"]


def test_missing_ai_reply_stops_instead_of_calling(monkeypatch):
    from openai.types.chat import ChatCompletion
    calls = []

    class Completions:
        def create(self, **kw):
            calls.append(kw)
            return ChatCompletion.model_validate({
                "id": "c", "object": "chat.completion", "created": 1, "model": kw["model"],
                "choices": [{"index": 0, "finish_reason": "stop",
                             "message": {"role": "assistant", "content": "{}"}}]})

    class Chat:
        completions = Completions()

    class Client:
        chat = Chat()

    c = Client()
    rs.wrap_ai_clients(gpt_client=c)
    req1 = dict(model="gpt-4o", messages=[{"role": "user", "content": "Analyze Union Cafe"}])
    rs.set_request_mode("all")
    c.chat.completions.create(**req1)
    rs.set_request_mode("saved")
    c.chat.completions.create(**req1)                    # saved: replayed
    with pytest.raises(rs.ReplayMiss):
        c.chat.completions.create(**dict(req1, messages=[{"role": "user", "content": "new prompt"}]))
    s = rs.run_state().summary()
    assert len(calls) == 1
    assert s["live_calls"] == 0 and s["replayed_calls"] == 1 and s["missing"][0]["kind"] == "openai"


def test_duckduckgo_saved_mode(monkeypatch):
    calls = []

    class D:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def text(self, q, max_results=6):
            calls.append(q)
            return [{"title": "t", "href": "https://d.com", "body": "b"}]

    monkeypatch.setattr(app, "DDGS", D)
    monkeypatch.setattr(app, "EXA_API_KEY", "")
    monkeypatch.setattr(app, "FIRECRAWL_API_KEY", "")
    monkeypatch.setitem(app._DDG_STATE, "until", 0.0)
    rs.set_request_mode("all")
    first = app.search_web("q1")
    assert rs.run_state().summary()["live_by_target"] == {"duckduckgo": 1}
    rs.set_request_mode("saved")
    assert app.search_web("q1") == first
    assert app.search_web("never captured") == []
    s = rs.run_state().summary()
    assert calls == ["q1"] and s["live_calls"] == 0 and s["missing"][0]["kind"] == "duckduckgo"


def test_background_threads_share_one_run_summary(net):
    net(_exa)
    rs.set_request_mode("all")
    app.search_web("q-main")
    t = threading.Thread(target=app._carry(lambda: app.search_web("q-thread")))
    t.start(); t.join()
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as ex:
        list(ex.map(app._carry(lambda q: app.search_web(q)), ["p1", "p2", "p3"]))
    assert rs.run_state().summary()["live_calls"] == 5


def test_saved_mode_reserves_no_budget(monkeypatch):
    monkeypatch.undo()
    used = []
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "increment_usage", lambda *a, **k: used.append(a) or 1, raising=False)
    rs.set_request_mode("saved")
    assert app.api_budget_ok("exa") and app.api_budget_ok("google_details")
    assert used == []
    rs.set_request_mode("off")


# ── what the owner sees ────────────────────────────────────────────────────

def _json_response(body, status=200):
    return app.app.response_class(json.dumps(body), status=status, mimetype="application/json")


def test_saved_run_with_missing_data_returns_the_list_not_a_result():
    with app.app.test_request_context("/prospect"):
        rs.set_request_mode("saved")
        rs.run_state().add_missing("http", "GET maps.googleapis.com/maps/api/place/textsearch/json")
        resp = app._replay_report(_json_response({"results": [{"business_name": "X"}]}))
    body = resp.get_json()
    assert resp.status_code == 409 and "results" not in body
    assert body["replay_missing"][0]["what"].startswith("GET maps.googleapis.com")
    assert "nothing was sent live" in body["error"]
    assert resp.headers["X-Replay-Effective"] == "saved"
    assert resp.headers["X-Replay-Live-Calls"] == "0"


def test_complete_runs_carry_the_live_call_count():
    with app.app.test_request_context("/prospect"):
        rs.set_request_mode("saved")
        rs.run_state().add_replayed()
        resp = app._replay_report(_json_response({"results": []}))
    body = resp.get_json()
    assert resp.status_code == 200
    assert body["replay_run"]["live_calls"] == 0 and body["replay_run"]["replayed_calls"] == 1


def test_polling_requests_do_not_get_a_fake_summary():
    with app.app.test_request_context("/job/abc"):
        rs.set_request_mode("saved")
        resp = app._replay_report(_json_response({"status": "running"}))
    assert "replay_run" not in resp.get_json()


def test_background_job_ends_with_missing_list_or_summary():
    rs.set_request_mode("saved")
    rs.run_state().add_missing("anthropic", "anthropic claude reply for this prompt")
    app.JOBS["j1"] = {"status": "done", "result": {"report": {}}}
    app._finish_replay_job("j1")
    j = app.JOBS.pop("j1")
    assert j["status"] == "error" and j["replay_missing"][0]["kind"] == "anthropic"

    rs.set_request_mode("saved")
    rs.run_state().add_replayed()
    app.JOBS["j2"] = {"status": "done", "result": {"report": {}}}
    app._finish_replay_job("j2")
    j = app.JOBS.pop("j2")
    assert j["status"] == "done" and j["result"]["replay_run"]["live_calls"] == 0


def test_saved_header_needs_the_owner_code():
    with app.app.test_request_context("/prospect", headers={"X-Replay": "saved", "X-Agent-Token": "someone-else"}):
        app.app.preprocess_request()
        assert rs.current_mode() == "off" and rs.run_state() is None
    with app.app.test_request_context("/prospect", headers={"X-Replay": "saved", "X-Agent-Token": app.MASTER_CODE}):
        app.app.preprocess_request()
        assert rs.current_mode() == "saved"
