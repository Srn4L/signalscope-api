"""Scout research keeps a stable order (2026-11-04).

The 8:03 PM and 8:41 PM Astoria captures replayed the same saved Google
search but showed different top 10s: exposure history from each live run
pushed the businesses already shown down in the next one. Research is now
stable: the same search gives the same order, businesses this code was
shown or saved are labeled, not moved. Rotation stays on Refresh.

All Google calls are mocked; nothing here touches the network or a database.
"""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import app  # noqa: E402
from tests.test_scout_factual_m1 import FakeGoogle, MASTER, _setup, card  # noqa: E402

LOCATION = "Astoria, NY"


def _places():
    names = ["Chris Spa & Nails", "Is A Nails Spa", "Russian Nails Astoria", "U + Nail",
             "Ying Nails & Spa", "Allure Nail Salon", "Naespa Nails", "Silvias Nails",
             "Aura Nails", "DSS Astoria Nails", "QQ Nails & Spa", "Nail'd it Astoria"]
    out = []
    for i, n in enumerate(names):
        out.append({"name": n, "place_id": f"pid-{i}", "formatted_address": f"{i} 31st St, Astoria, NY 11102",
                    "rating": round(4.9 - i * 0.05, 2), "user_ratings_total": 100 + i * 10,
                    "business_status": "OPERATIONAL", "types": ["beauty_salon"],
                    "geometry": {"location": {"lat": 40.764 + i * 0.001, "lng": -73.923}}})
    return out


def _key(name):
    import re
    return re.sub(r"[^a-z0-9]", "", name.lower())[:20] + "|" + LOCATION.lower()


def _run(monkeypatch, stats, **extra):
    _setup(monkeypatch, FakeGoogle(_places()))
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "save_search", lambda d: 1)
    monkeypatch.setattr(app, "upsert_business", lambda d: None)
    monkeypatch.setattr(app, "_scout_exposure_inputs", lambda *a, **k: (stats, None))
    body = {"niche": "nail salon", "location": LOCATION, "limit": 10}
    body.update(extra)
    r = app.app.test_client().post("/prospect", json=body, headers={"X-Agent-Token": MASTER})
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()


def _names(data):
    return [c["business_name"] for c in data["results"]]


SEEN_TOP = {_key(n): {"seen_count_for_token": 2, "seen_count_global": 2,
                      "last_seen_at_for_token": "2026-11-05T01:03:00+00:00",
                      "already_saved_by_token": False, "already_contacted_by_token": False}
            for n in ["Chris Spa & Nails", "Is A Nails Spa", "Russian Nails Astoria", "U + Nail", "Ying Nails & Spa"]}


def test_same_search_same_order_after_being_shown(monkeypatch):
    first = _names(_run(monkeypatch, {}))
    again = _names(_run(monkeypatch, SEEN_TOP))
    assert first == again


def test_seen_businesses_are_labeled_not_moved(monkeypatch):
    data = _run(monkeypatch, SEEN_TOP)
    chris = card(data, "Chris Spa & Nails")
    assert chris["seen_before"]["count"] == 2
    assert chris["seen_before"]["last_seen_at"] == "2026-11-05T01:03:00+00:00"
    assert card(data, "Allure Nail Salon")["seen_before"] is None
    assert data["ranking_strategy"]["type"] == "stable"
    assert data["ranking_strategy"]["repeat_suppression_active"] is False


def test_saved_business_keeps_already_saved_flag(monkeypatch):
    stats = {_key("Is A Nails Spa"): {"seen_count_for_token": 1, "already_saved_by_token": True}}
    data = _run(monkeypatch, stats)
    c = card(data, "Is A Nails Spa")
    assert c["already_saved"] is True and c["seen_before"]["saved"] is True


def test_refresh_still_rotates(monkeypatch):
    plain = _names(_run(monkeypatch, SEEN_TOP))
    refreshed = _run(monkeypatch, SEEN_TOP, refresh=True)
    assert refreshed["ranking_strategy"]["type"] == "saturation_aware"
    assert _names(refreshed) != plain


def test_global_exposure_does_not_hide_research(monkeypatch):
    # Other users' views never move this user's results
    busy = {_key(n): {"seen_count_global": 50} for n in ["Chris Spa & Nails", "Is A Nails Spa"]}
    assert _names(_run(monkeypatch, busy)) == _names(_run(monkeypatch, {}))
