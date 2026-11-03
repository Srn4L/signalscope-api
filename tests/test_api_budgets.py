"""Daily caps on paid outside APIs: over the cap, the provider is skipped."""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import app  # noqa: E402


class FakeCounter:
    def __init__(self):
        self.totals = {}

    def __call__(self, day, scope, key, kind, amount):
        k = (scope, key, kind)
        self.totals[k] = self.totals.get(k, 0) + amount
        return self.totals[k]


def _setup(monkeypatch, limits):
    counter = FakeCounter()
    monkeypatch.setattr(app, "increment_usage", counter)
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "API_CALL_LIMITS", dict(limits))
    return counter


def test_cap_blocks_after_limit_and_undoes_reservation(monkeypatch):
    counter = _setup(monkeypatch, {"google_search": 2})
    assert app.api_budget_ok("google_search")
    assert app.api_budget_ok("google_search")
    assert not app.api_budget_ok("google_search")
    assert counter.totals[("global", "all", "api_google_search")] == 2


def test_multi_call_reservation(monkeypatch):
    _setup(monkeypatch, {"geoapify": 3})
    assert app.api_budget_ok("geoapify", 2)
    assert not app.api_budget_ok("geoapify", 2)


def test_zero_turns_provider_off(monkeypatch):
    _setup(monkeypatch, {"serpapi": 0})
    assert not app.api_budget_ok("serpapi")


def test_fails_open_without_database(monkeypatch):
    _setup(monkeypatch, {"tavily": 1})
    monkeypatch.setattr(app, "_DB_AVAILABLE", False)
    assert app.api_budget_ok("tavily")
    assert app.api_budget_ok("tavily")


def test_google_places_skipped_over_cap(monkeypatch):
    _setup(monkeypatch, {"google_search": 0})
    monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "x")

    def boom(*a, **k):
        raise AssertionError("Google must not be called over the cap")

    import requests
    monkeypatch.setattr(requests, "get", boom)
    assert app.search_google_places("nail salon", "Astoria, NY") == []


def test_defaults_present():
    for name in ("google_search", "google_details", "google_geocode", "serpapi", "tavily", "scraperapi", "geoapify"):
        assert name in app.API_CALL_LIMITS
