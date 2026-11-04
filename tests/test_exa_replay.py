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
    rs.set_request_mode("off")
    yield
    rs.set_request_mode("off")


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
    rs.set_request_mode("all")
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
    rs.install_http_replay()
    fake = net(_exa_ok)
    app.search_web("nail salon Astoria")
    app.search_web("nail salon Astoria")
    assert len(fake.calls) == 2


def test_ai_mode_keeps_searches_live(net, replay):
    rs.set_request_mode("ai")
    fake = net(_exa_ok)
    app.search_web("nail salon Astoria")
    app.search_web("nail salon Astoria")
    assert len(fake.calls) == 2


def test_allowed_on_server_but_no_header_means_live(net, replay):
    rs.set_request_mode("off")                           # what a request without X-Replay gets
    fake = net(_exa_ok)
    app.search_web("nail salon Astoria")
    app.search_web("nail salon Astoria")
    assert len(fake.calls) == 2


@pytest.mark.parametrize("header,owner,allowed,mode", [
    ("ai", True, True, "ai"),
    ("all", True, True, "all"),
    ("ai", False, True, "off"),      # other users: header ignored
    ("all", False, True, "off"),
    ("ai", True, False, "off"),      # server does not allow replay
    ("", True, True, "off"),
    ("off", True, True, "off"),
    ("garbage", True, True, "off"),
])
def test_only_the_owner_can_opt_in(monkeypatch, header, owner, allowed, mode):
    monkeypatch.setattr(rs, "REPLAY_ON", allowed)
    assert rs.mode_from_header(header, owner) == mode


def test_before_request_sets_mode_per_request(monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    with app.app.test_request_context("/prospect", headers={"X-Replay": "ai", "X-Agent-Token": app.MASTER_CODE}):
        app.app.preprocess_request()
        assert rs.current_mode() == "ai"
    with app.app.test_request_context("/prospect", headers={"X-Replay": "ai", "X-Agent-Token": "not-the-owner"}):
        app.app.preprocess_request()
        assert rs.current_mode() == "off"
    with app.app.test_request_context("/prospect", headers={"X-Agent-Token": app.MASTER_CODE}):
        app.app.preprocess_request()
        assert rs.current_mode() == "off"                # previous request's mode does not stick


def test_background_threads_keep_the_request_mode(replay):
    import threading
    rs.set_request_mode("ai")
    seen = {}

    def work(tag):
        seen[tag] = rs.current_mode()

    t1 = threading.Thread(target=app._carry(work), args=("carried",))
    t2 = threading.Thread(target=work, args=("plain",))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert seen == {"carried": "ai", "plain": "off"}

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as ex:
        modes = list(ex.map(app._carry(lambda _: rs.current_mode()), range(6)))
    assert modes == ["ai"] * 6


@pytest.mark.parametrize("raw,allowed", [("1", True), ("allow", True), ("ai", True), ("", False), ("0", False)])
def test_server_permission_parsing(monkeypatch, raw, allowed):
    import importlib
    monkeypatch.setenv("YELHAO_DEV_REPLAY", raw)
    m = importlib.reload(rs)
    try:
        assert m.REPLAY_ON is allowed
        assert m.current_mode() == "off"                 # permission alone replays nothing
    finally:
        monkeypatch.delenv("YELHAO_DEV_REPLAY")
        importlib.reload(rs)


# ── Exa cap ────────────────────────────────────────────────────────────────

def test_exa_request_is_lite_and_never_over_ten_results(net):
    fake = net(_exa_ok)
    app.exa_search("cafe Harlem", 25)
    sent = json.loads(fake.calls[0][2])
    assert sent["type"] == "lite" and sent["numResults"] == 10
    assert set(sent["contents"]) == {"highlights"}       # no paid extras (summary)


def test_exa_cap_holds_without_the_database(monkeypatch):
    monkeypatch.undo()                                   # use the real api_budget_ok
    monkeypatch.setattr(app, "_DB_AVAILABLE", False)
    monkeypatch.setitem(app.API_CALL_LIMITS, "exa", 3)
    app._LOCAL_BUDGET.clear()
    got = [app.api_budget_ok("exa") for _ in range(5)]
    assert got == [True, True, True, False, False]
    # other providers keep today's behavior when the counter is down
    assert all(app.api_budget_ok("serpapi") for _ in range(20))


def test_default_exa_cap_fits_the_monthly_credit():
    assert app.API_CALL_LIMITS["exa"] * 31 * 4 / 1000 <= 10.0


# ── Provider comparison ────────────────────────────────────────────────────

def test_compare_runs_each_provider_on_the_same_query(net, monkeypatch):
    monkeypatch.setattr(app, "FIRECRAWL_API_KEY", "fc-test")
    monkeypatch.setattr(app, "SERP_API_KEY", "serp-test")

    def handler(request):
        if "exa.ai" in request.url:
            return 200, {"results": [{"title": "A", "url": "https://a.com", "highlights": ["a"]},
                                     {"title": "B", "url": "https://b.com", "highlights": ["b"]}]}
        if "firecrawl.dev" in request.url:
            return 200, {"data": [{"title": "B", "url": "https://b.com", "description": "b"}]}
        return 200, {"organic_results": [{"title": "C", "link": "https://c.com", "snippet": "c"}]}

    net(handler)
    c = app.app.test_client()
    assert c.post("/dev/search/compare", json={"queries": ["x"]}).status_code == 403
    r = c.post("/dev/search/compare", json={"queries": ["cafe Harlem"]},
               headers={"X-Agent-Token": app.MASTER_CODE}).get_json()
    row = r["comparisons"][0]
    assert set(row["providers"]) == {"exa", "firecrawl", "serpapi"}
    assert row["overlap"]["exa&firecrawl"] == 1
    assert row["only_in"]["exa"] == ["https://a.com"] and row["only_in"]["serpapi"] == ["https://c.com"]


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
