"""Geocoding uses Geoapify first and falls back to Google."""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import app  # noqa: E402


def test_query_skips_missing_parts():
    assert app._geocode_query(None, None, "Astoria, NY") == "Astoria, NY"
    assert app._geocode_query("Glow", None, "Brooklyn") == "Glow Brooklyn"
    assert app._geocode_query("Glow", "1 Main St", "Brooklyn") == "1 Main St"


def test_geoapify_first(monkeypatch):
    monkeypatch.setattr(app, "_geocode_geoapify", lambda q: (40.7, -73.9))
    monkeypatch.setattr(app, "_geocode_google", lambda *a, **k: (_ for _ in ()).throw(AssertionError("google called")))
    assert app.geocode_business(None, None, "Astoria, NY") == (40.7, -73.9)


def test_falls_back_to_google(monkeypatch):
    monkeypatch.setattr(app, "_geocode_geoapify", lambda q: (None, None))
    monkeypatch.setattr(app, "_geocode_google", lambda *a, **k: (1.0, 2.0))
    assert app.geocode_business("Glow", None, "Brooklyn") == (1.0, 2.0)
