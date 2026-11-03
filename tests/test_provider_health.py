"""Owner-only provider check: auth, shape, and no key values in output."""
import os
from types import SimpleNamespace as NS

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import app  # noqa: E402


class Resp:
    def __init__(self, status, data, ctype="application/json"):
        self.status_code, self._d, self.text = status, data, str(data)
        self.headers = {"content-type": ctype}

    def json(self):
        return self._d


def test_requires_master(monkeypatch):
    monkeypatch.setattr(app, "MASTER_CODE", "owner123")
    c = app.app.test_client()
    assert c.get("/health/providers").status_code == 403
    assert c.get("/health/providers", headers={"X-Agent-Token": "guest"}).status_code == 403


def test_reports_each_provider(monkeypatch):
    import requests
    monkeypatch.setattr(app, "MASTER_CODE", "owner123")
    monkeypatch.setattr(app, "SERP_API_KEY", "serp-secret")
    monkeypatch.setattr(app, "SCRAPER_API_KEY", "")
    monkeypatch.setattr(app, "TAVILY_API_KEY", "tav-secret")
    monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "g-secret")
    monkeypatch.setenv("GEOAPIFY_KEY", "geo-secret")
    monkeypatch.setattr(app, "api_budget_ok", lambda *a, **k: True)

    def fake_get(url, params=None, timeout=None, **k):
        if "serpapi" in url:
            return Resp(200, {"plan_name": "Free", "searches_per_month": 100, "total_searches_left": 0})
        if "googleapis" in url:
            return Resp(200, {"status": "OK"})
        if "geoapify" in url:
            return Resp(200, {"features": [{}]})
        raise AssertionError(url)

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(requests, "post", lambda *a, **k: Resp(200, {"results": [{"url": "x"}]}))
    monkeypatch.setattr(app, "gpt_client", NS(models=NS(list=lambda: [NS(id="gpt-4o")])))
    monkeypatch.setattr(app, "claude_client", NS(models=NS(retrieve=lambda m: NS(id=m))))

    r = app.app.test_client().get("/health/providers", headers={"X-Agent-Token": "owner123"})
    assert r.status_code == 200
    p = r.get_json()["providers"]
    assert p["serpapi"]["ok"] is False and p["serpapi"]["searches_left"] == 0
    assert p["scraperapi"]["configured"] is False
    assert p["tavily"]["ok"] is True
    assert p["google_geocoding"]["ok"] is True
    assert p["geoapify"]["ok"] is True
    assert p["openai"]["ok"] is True and p["anthropic"]["ok"] is True
    body = r.get_data(as_text=True)
    for secret in ("serp-secret", "tav-secret", "g-secret", "geo-secret"):
        assert secret not in body
