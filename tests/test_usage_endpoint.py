"""Owner-only usage endpoint: auth, caps, and no per-visitor keys."""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import app  # noqa: E402


def test_requires_master(monkeypatch):
    monkeypatch.setattr(app, "MASTER_CODE", "owner123")
    assert app.app.test_client().get("/health/usage").status_code == 403


def test_reports_usage(monkeypatch):
    monkeypatch.setattr(app, "MASTER_CODE", "owner123")
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "SERP_API_KEY", "")
    monkeypatch.setattr(app, "get_usage_summary", lambda day: {
        "global": {"search": 3, "analysis": 1, "api_google_search": 4, "api_tavily": 9},
        "demo_ip_total": {"search": 2}, "codes_total": {"search": 1}})
    r = app.app.test_client().get("/health/usage", headers={"X-Agent-Token": "owner123"})
    d = r.get_json()
    assert r.status_code == 200 and d["ok"]
    assert d["actions"]["searches"]["used"] == 3
    assert d["apis"]["google_search"] == {"used": 4, "cap": app.API_CALL_LIMITS["google_search"]}
    assert d["apis"]["serpapi"]["used"] == 0
    assert 0 <= d["resets_in_minutes"] <= 1440


def test_database_offline(monkeypatch):
    monkeypatch.setattr(app, "MASTER_CODE", "owner123")
    monkeypatch.setattr(app, "get_usage_summary", lambda day: None)
    r = app.app.test_client().get("/health/usage", headers={"X-Agent-Token": "owner123"})
    assert r.status_code == 503
