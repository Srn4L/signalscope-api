"""Search Part A: skip DuckDuckGo after a block, fewer duplicate searches.

Offline: DuckDuckGo, SerpAPI and page fetches are all mocked.
"""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402

import app  # noqa: E402


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(app, "SERP_API_KEY", "")
    monkeypatch.setitem(app._DDG_STATE, "until", 0.0)
    monkeypatch.setitem(app._DDG_STATE, "skipped", 0)
    monkeypatch.setattr(app.time, "sleep", lambda *_: None)
    app._TREND_CACHE.clear()
    yield
    app._TREND_CACHE.clear()


def fake_ddgs(behavior, calls):
    """A DDGS stand-in. behavior(query) returns results or raises."""
    class _D:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def text(self, query, max_results=6):
            calls.append(query)
            return behavior(query)
    return _D


# ── DuckDuckGo cooldown ────────────────────────────────────────────────────

@pytest.mark.parametrize("err", [
    "https://duckduckgo.com/?q=x 202 Ratelimit",
    "TimeoutError: error sending request for url > operation timed out",
])
def test_block_skips_duckduckgo_for_the_rest_of_the_cooldown(monkeypatch, err):
    calls = []

    def boom(q):
        raise RuntimeError(err)

    monkeypatch.setattr(app, "DDGS", fake_ddgs(boom, calls))
    assert app.search_web("first") == []
    for q in ("second", "third", "fourth"):
        assert app.search_web(q) == []
    assert calls == ["first"]          # only the first search waited on DuckDuckGo
    assert app._DDG_STATE["skipped"] == 3


def test_duckduckgo_is_tried_again_after_cooldown(monkeypatch):
    calls = []
    monkeypatch.setattr(app, "DDGS", fake_ddgs(lambda q: [{"title": "t", "href": "https://x.com", "body": "b"}], calls))
    app._DDG_STATE["until"] = app.time.time() - 1   # cooldown already over
    out = app.search_web("again")
    assert calls == ["again"] and out[0]["url"] == "https://x.com"


def test_other_errors_do_not_start_a_cooldown(monkeypatch):
    calls = []

    def parse_error(q):
        raise ValueError("unexpected html layout")

    monkeypatch.setattr(app, "DDGS", fake_ddgs(parse_error, calls))
    app.search_web("one")
    app.search_web("two")
    assert calls == ["one", "two"]
    assert app._DDG_STATE["until"] == 0.0


def test_query_text_with_202_is_not_mistaken_for_a_block(monkeypatch):
    calls = []

    def other(q):
        raise ValueError(f"bad response for {q}")

    monkeypatch.setattr(app, "DDGS", fake_ddgs(other, calls))
    app.search_web("nail tutorial 2025 OR 2026")
    assert app._DDG_STATE["until"] == 0.0


# ── Booking platforms: looser query only when the exact one found nothing ──

def _booking_run(monkeypatch, found):
    queries = []

    def fake_search(q, max_results=6):
        queries.append(q)
        if not found:
            return []
        dom = q.split("site:")[1].split()[0]
        return [{"title": "Shiny Nails | Booksy", "url": f"https://{dom}/shiny-nails",
                 "body": "Shiny Nails in Astoria, 4.8 stars, gel manicure from $35, open late most days."}]

    monkeypatch.setattr(app, "search_web", fake_search)
    monkeypatch.setattr(app, "safe_get", lambda url, timeout=10: (None, 403))
    cards = app.scrape_booking_competitors("Angel of Your Nails", "nail salon", "Astoria, NY", limit=10)
    return queries, cards


def test_booking_runs_one_query_per_platform_when_the_first_finds_results(monkeypatch):
    queries, cards = _booking_run(monkeypatch, found=True)
    assert len(queries) == len(app.BOOKING_PLATFORMS)
    assert len(cards) == len(app.BOOKING_PLATFORMS)


def test_booking_still_tries_the_looser_query_when_the_first_finds_nothing(monkeypatch):
    queries, cards = _booking_run(monkeypatch, found=False)
    assert len(queries) == 2 * len(app.BOOKING_PLATFORMS)
    assert cards == []


# ── Trend cache ────────────────────────────────────────────────────────────

def test_trends_are_reused_for_the_same_niche(monkeypatch):
    queries = []

    def fake_search(q, max_results=6):
        queries.append(q)
        return [{"title": "Chrome nails are everywhere", "url": "https://www.tiktok.com/@x/video/1",
                 "body": "Chrome nails #chromenails 1.2M views and counting this week across NYC salons."}]

    monkeypatch.setattr(app, "search_web", fake_search)
    first = app.fetch_trend_intelligence("Angel of Your Nails", "nail salon", "Astoria, NY", {})
    n = len(queries)
    second = app.fetch_trend_intelligence("Another Nail Spot", "nail salon", "Queens, NY", {})
    assert n > 0 and len(queries) == n      # no new searches the second time
    assert second == first


def test_empty_trends_are_not_cached(monkeypatch):
    queries = []

    def nothing(q, max_results=6):
        queries.append(q)
        return []

    monkeypatch.setattr(app, "search_web", nothing)
    app.fetch_trend_intelligence("A", "nail salon", "Astoria, NY", {})
    n = len(queries)
    app.fetch_trend_intelligence("B", "nail salon", "Astoria, NY", {})
    assert len(queries) == 2 * n            # tried again: a failure isn't remembered


# ── Website lookup: one "official site" search, not two ────────────────────

def test_discovery_marks_that_it_ran_the_website_search(monkeypatch):
    monkeypatch.setattr(app, "search_google_places", lambda *a, **k: [])
    monkeypatch.setattr(app, "search_web", lambda q, n=6: [])
    monkeypatch.setattr(app, "search_context", lambda q, n=6: [])
    out = app.discover_business_signals("Angel of Your Nails", "Astoria, NY", "nail salon")
    assert out.get("website_searched") is True
