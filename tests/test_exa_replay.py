"""Exa as main search, Firecrawl search fallback, and dev replay.

Offline: the network is replaced by a fake transport.
"""
import json
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402
import requests  # noqa: E402

import app  # noqa: E402
import replay_service as rs  # noqa: E402


class FakeNet:
    """Stands in for the HTTP transport; records every real request."""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def send(self, adapter, request, **kw):
        self.calls.append((request.method, request.url, request.body))
        status, payload = self.handler(request)
        r = requests.models.Response()
        r.status_code = status
        r._content = json.dumps(payload).encode() if not isinstance(payload, bytes) else payload
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
                            lambda self, request, **kw: fake.send(self, request, **kw))
        return fake
    return install


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setattr(app, "EXA_API_KEY", "exa-test")
    monkeypatch.setattr(app, "FIRECRAWL_API_KEY", "")
    monkeypatch.setattr(app, "SERP_API_KEY", "")
    monkeypatch.setattr(app, "DDGS", None)
    monkeypatch.setattr(app, "api_budget_ok", lambda provider, amount=1: True)
    monkeypatch.setattr(rs, "REPLAY_ON", False)
    monkeypatch.setattr(rs, "HTTP_REPLAY_ON", False)
    yield


def _exa_ok(request):
    return 200, {"results": [{"title": "Shiny Nails (@shinynails)", "url": "https://www.instagram.com/shinynails/",
                              "highlights": ["Gel and chrome nails in Astoria.", "Book via link in bio."]}]}


# ── Exa ────────────────────────────────────────────────────────────────────

def test_exa_is_tried_first_and_site_operators_become_domains(net):
    fake = net(_exa_ok)
    out = app.search_web('site:instagram.com "nail salon" "Astoria"', 4)
    assert out[0]["provider"] == "exa"
    assert out[0]["body"] == "Gel and chrome nails in Astoria. Book via link in bio."
    method, url, body = fake.calls[0]
    sent = json.loads(body)
    assert url == "https://api.exa.ai/search" and method == "POST"
    assert sent["includeDomains"] == ["instagram.com"]
    assert sent["query"] == '"nail salon" "Astoria"'
    assert sent["type"] == "lite" and sent["numResults"] == 4


def test_negative_site_operator_becomes_exclude(net):
    fake = net(_exa_ok)
    app.search_web('"Angel of Your Nails" official site -site:yelp.com')
    sent = json.loads(fake.calls[0][2])
    assert sent["excludeDomains"] == ["yelp.com"] and "includeDomains" not in sent


def test_exa_failure_falls_back_to_firecrawl_then_serpapi(net, monkeypatch):
    monkeypatch.setattr(app, "FIRECRAWL_API_KEY", "fc-test")
    monkeypatch.setattr(app, "SERP_API_KEY", "serp-test")

    def handler(request):
        if "exa.ai" in request.url:
            return 402, {"error": "credits"}
        if "firecrawl.dev/v1/search" in request.url:
            return 200, {"success": True, "data": [{"title": "Shiny", "url": "https://shiny.com", "description": "nails"}]}
        return 200, {"organic_results": [{"title": "s", "link": "https://serp.com", "snippet": "x"}]}

    fake = net(handler)
    out = app.search_web("nail salon Astoria")
    assert out[0]["provider"] == "firecrawl"
    assert [c[1].split("?")[0] for c in fake.calls] == ["https://api.exa.ai/search", "https://api.firecrawl.dev/v1/search"]


def test_capped_providers_are_skipped(net, monkeypatch):
    monkeypatch.setattr(app, "SERP_API_KEY", "serp-test")
    monkeypatch.setattr(app, "FIRECRAWL_API_KEY", "fc-test")
    monkeypatch.setattr(app, "api_budget_ok", lambda provider, amount=1: provider == "serpapi")
    fake = net(lambda r: (200, {"organic_results": [{"title": "s", "link": "https://serp.com", "snippet": "x"}]}))
    out = app.search_web("nail salon Astoria")
    assert out[0]["url"] == "https://serp.com"
    assert all("serpapi.com" in c[1] for c in fake.calls)


def test_no_exa_key_means_no_exa_call(net, monkeypatch):
    monkeypatch.setattr(app, "EXA_API_KEY", "")
    fake = net(_exa_ok)
    assert app.search_web("anything") == []
    assert fake.calls == []


# ── Dev replay ─────────────────────────────────────────────────────────────

@pytest.fixture
def replay(monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    monkeypatch.setattr(rs, "HTTP_REPLAY_ON", True)
    monkeypatch.setattr(rs, "_db_session", lambda: (_ for _ in ()).throw(RuntimeError("no db")))
    rs._MEM.clear()
    for k in rs.STATS:
        rs.STATS[k] = 0
    rs.install_http_replay()
    refunds = []
    monkeypatch.setattr(rs, "on_http_hit", lambda url: refunds.append(url))
    yield refunds
    rs._MEM.clear()


def test_second_identical_search_is_replayed_and_refunded(net, replay):
    fake = net(_exa_ok)
    first = app.search_web("nail salon Astoria")
    second = app.search_web("nail salon Astoria")
    assert first == second
    assert len(fake.calls) == 1
    assert replay == ["https://api.exa.ai/search"]       # budget given back for the free call


def test_api_keys_do_not_split_or_leak_into_the_cache(net, replay):
    fake = net(lambda r: (200, {"ok": True}))
    requests.get("https://serpapi.com/search", params={"q": "x", "api_key": "AAA"})
    requests.get("https://serpapi.com/search", params={"q": "x", "api_key": "BBB"})
    assert len(fake.calls) == 1
    assert "AAA" not in json.dumps({k: v[0] for k, v in rs._MEM.items()})
    assert not any("AAA" in k for k in rs._MEM)


def test_crm_writes_and_other_posts_are_never_replayed(net, replay):
    fake = net(lambda r: (200, {"id": 1}))
    for _ in range(2):
        requests.post("https://api.hubapi.com/crm/v3/objects/contacts", json={"email": "a@b.c"})
        requests.post("https://example.com/form", json={"a": 1})
        requests.get("https://api.hubapi.com/crm/v3/objects/contacts")
    assert len(fake.calls) == 6


def test_failed_replies_are_not_saved(net, replay):
    fake = net(lambda r: (429, {"error": "rate"}))
    requests.get("https://maps.googleapis.com/maps/api/place/details/json", params={"place_id": "p"})
    requests.get("https://maps.googleapis.com/maps/api/place/details/json", params={"place_id": "p"})
    assert len(fake.calls) == 2


def test_health_checks_always_reach_the_real_service(net, replay):
    fake = net(lambda r: (200, {"ok": True}))
    with app.app.test_request_context("/health/providers"):
        requests.get("https://serpapi.com/search", params={"q": "y"})
        requests.get("https://serpapi.com/search", params={"q": "y"})
    assert len(fake.calls) == 2


def test_replay_off_changes_nothing(net, monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", False)
    monkeypatch.setattr(rs, "HTTP_REPLAY_ON", False)
    rs.install_http_replay()
    fake = net(_exa_ok)
    app.search_web("nail salon Astoria")
    app.search_web("nail salon Astoria")
    assert len(fake.calls) == 2


def test_ai_mode_keeps_searches_live(net, replay, monkeypatch):
    monkeypatch.setattr(rs, "HTTP_REPLAY_ON", False)     # YELHAO_DEV_REPLAY=ai
    fake = net(_exa_ok)
    app.search_web("nail salon Astoria")
    app.search_web("nail salon Astoria")
    assert len(fake.calls) == 2


def test_x_replay_off_header_forces_a_live_run(net, replay):
    fake = net(_exa_ok)
    app.search_web("nail salon Astoria")                 # saved
    with app.app.test_request_context("/agent", headers={"X-Replay": "off"}):
        app.search_web("nail salon Astoria")             # live despite the saved reply
    assert len(fake.calls) == 2


@pytest.mark.parametrize("raw,mode", [("1", "all"), ("all", "all"), ("ai", "ai"), ("", "off"), ("0", "off")])
def test_mode_parsing(monkeypatch, raw, mode):
    import importlib
    monkeypatch.setenv("YELHAO_DEV_REPLAY", raw)
    m = importlib.reload(rs)
    try:
        assert m.REPLAY_MODE == mode
        assert m.HTTP_REPLAY_ON == (mode == "all")
    finally:
        monkeypatch.delenv("YELHAO_DEV_REPLAY")
        importlib.reload(rs)


def test_ai_replies_are_replayed_by_full_request(replay):
    from openai.types.chat import ChatCompletion

    calls = []

    class Completions:
        def create(self, **kw):
            calls.append(kw)
            return ChatCompletion.model_validate({
                "id": "c1", "object": "chat.completion", "created": 1, "model": kw["model"],
                "choices": [{"index": 0, "finish_reason": "stop",
                             "message": {"role": "assistant", "content": '{"overall_score": 64}'}}]})

    class Chat:
        completions = Completions()

    class Client:
        chat = Chat()

    client = Client()
    rs.wrap_ai_clients(gpt_client=client)
    req1 = dict(model="gpt-4o", messages=[{"role": "user", "content": "Analyze Union Cafe"}], temperature=0.2)
    a = client.chat.completions.create(**req1)
    b = client.chat.completions.create(**req1)
    c = client.chat.completions.create(**dict(req1, messages=[{"role": "user", "content": "Analyze Other Cafe"}]))
    assert len(calls) == 2                               # identical prompt replayed, new prompt called
    assert b.choices[0].message.content == a.choices[0].message.content


# ── Deterministic trend keywords ───────────────────────────────────────────

def test_niche_keywords_keep_a_stable_order():
    out = app.detect_niche_keywords("nail salon", "Angel of Your Nails", {})
    expected = []
    for niche, kws in app.NICHE_KEYWORDS.items():
        if niche in "nail salon angel of your nails ":
            expected.extend(kws)
    assert out == list(dict.fromkeys(expected))[:5]
