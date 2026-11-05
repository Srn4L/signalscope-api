"""Scout hands every run and its cards, in order, to the journey record."""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import app  # noqa: E402
import journey_service as js  # noqa: E402
from tests.test_scout_factual_m1 import FakeGoogle, MASTER, _setup  # noqa: E402


def _places(n=4):
    return [{"name": f"Spot {i} Nails", "place_id": f"pid-{i}", "formatted_address": f"{i} 31st St, Astoria, NY",
             "rating": 4.9 - i * 0.1, "user_ratings_total": 50 + i, "business_status": "OPERATIONAL",
             "types": ["beauty_salon"], "geometry": {"location": {"lat": 40.76, "lng": -73.92}}}
            for i in range(n)]


def test_scout_logs_the_run_and_returns_its_id(monkeypatch):
    got = {}
    _setup(monkeypatch, FakeGoogle(_places()))
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "save_search", lambda d: 42)
    ids = iter([101, 102, None, 104])
    monkeypatch.setattr(app, "upsert_business", lambda d: next(ids))
    monkeypatch.setattr(app, "log_discovery_event", lambda **k: None)
    monkeypatch.setattr(app, "_scout_exposure_inputs", lambda *a, **k: ({}, None))

    def fake_log(**kw):
        got.update(kw)
        return 9001
    monkeypatch.setattr(js, "log_scout_run", fake_log)
    r = app.app.test_client().post("/prospect", json={
        "niche": "nail salon", "location": "Astoria, NY", "user_query": "nail salons in Astoria"},
        headers={"X-Agent-Token": MASTER})
    data = r.get_json()
    assert data["run_id"] == 9001
    assert got["search_id"] == 42 and got["user_query"] == "nail salons in Astoria"
    assert [c["business_name"] for c in got["cards"]] == [c["business_name"] for c in data["results"]]
    assert got["business_ids"] == [101, 102, None, 104]
    assert got["replay_mode"] == "off" and got["ranking"]["type"] == "stable"


def test_no_database_means_no_run_id(monkeypatch):
    _setup(monkeypatch, FakeGoogle(_places(2)))
    r = app.app.test_client().post("/prospect", json={"niche": "nail salon", "location": "Astoria, NY"},
                                   headers={"X-Agent-Token": MASTER})
    assert r.get_json()["run_id"] is None


def test_logging_failure_never_breaks_a_search(monkeypatch):
    _setup(monkeypatch, FakeGoogle(_places(2)))
    monkeypatch.setattr(js, "_db_session", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    assert js.log_scout_run(search_id=1, token_hash=None, replay_mode="off", mode="outreach", niche="x",
                            location="y", user_query="", meaning=None, ranking=None, is_refresh=False,
                            cards=[{"business_name": "A"}], business_ids=[None]) is None
