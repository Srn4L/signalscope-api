"""Saved-data replay of a Scout run must rank exactly like the capture.

Regression for the first live test: the capture logged its 10 businesses as
shown, so the saved rerun's exposure rerank picked different cards and asked
for Places Details the capture never made.
"""
import datetime as dt
import json
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402
import requests  # noqa: E402

import app  # noqa: E402
import replay_service as rs  # noqa: E402
import signal_service as ss  # noqa: E402

UTC = dt.timezone.utc


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    monkeypatch.setattr(rs, "_db_session", lambda: (_ for _ in ()).throw(RuntimeError("no db")))
    monkeypatch.setattr(rs, "on_http_hit", None)
    rs._MEM.clear()
    rs.install_http_replay()
    monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "g-test")
    monkeypatch.setattr(app, "api_budget_ok", lambda provider, amount=1: True)
    yield
    rs.set_request_mode("off")
    rs._MEM.clear()


@pytest.fixture
def net(monkeypatch):
    calls = []

    def send(self, request, **kw):
        calls.append(request.url)
        pid = request.url.split("place_id=")[1].split("&")[0] if "place_id=" in request.url else "x"
        r = requests.models.Response()
        r.status_code = 200
        r._content = json.dumps({"status": "OK", "result": {"website": f"https://{pid}.com",
                                                              "formatted_phone_number": "1"}}).encode()
        r.headers["Content-Type"] = "application/json"
        r.url = request.url
        r.request = request
        return r

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    return calls


def _cards():
    # Same Google search response both runs (it is replayed); scores close
    return [
        {"business_name": f"Nail Spot {i}", "location": "Astoria, NY", "opportunity_score": 70 - i,
         "source": "google_text", "google_place_id": f"p{i}", "website_checked": False, "score_reasons": []}
        for i in range(6)
    ]


def _rank_and_enrich(monkeypatch, stats, now_real, limit=3):
    monkeypatch.setattr(app, "get_candidate_exposure_stats", lambda scored, th: stats)
    scored = _cards()
    expo, now = app._scout_exposure_inputs(scored, "tok", "nail salon", "Astoria, NY")
    ranked = ss.apply_saturation_rerank(scored, expo, {}, now=now or now_real)
    ranked.sort(key=lambda c: -c["reranked_score"])
    top = ranked[:limit]
    app.enrich_shown_google_cards(top, max_calls=limit)
    return [c["business_name"] for c in top]


def test_saved_rerun_ranks_like_the_capture_after_exposure_changed(monkeypatch, net):
    t0 = dt.datetime(2026, 11, 4, 16, 20, tzinfo=UTC)
    rs.set_request_mode("all")
    captured = _rank_and_enrich(monkeypatch, {}, t0)
    cap = rs.run_state().summary()

    # The capture logged its top 3 as shown: the live DB now penalizes them
    after = {f"nailspot{i}|astoria, ny": {"seen_count_for_token": 1,
                                         "last_seen_at_for_token": t0.isoformat()} for i in range(3)}
    rs.set_request_mode("off")
    live_now = _rank_and_enrich(monkeypatch, after, t0 + dt.timedelta(minutes=5))
    assert live_now != captured                          # the bug: a different top 3

    rs.set_request_mode("saved")
    saved = _rank_and_enrich(monkeypatch, after, t0 + dt.timedelta(minutes=5))
    s = rs.run_state().summary()
    assert saved == captured
    assert s["live_calls"] == 0 and s["missing_count"] == 0
    assert cap["live_calls"] == 3


def test_rerank_clock_can_be_fixed():
    t0 = dt.datetime(2026, 11, 4, 16, 0, tzinfo=UTC)
    cards = _cards()
    stats = {"nailspot0|astoria, ny": {"last_seen_at_for_token": (t0 - dt.timedelta(hours=2)).isoformat()}}
    within = ss.apply_saturation_rerank([dict(c) for c in cards], stats, {}, now=t0)
    later = ss.apply_saturation_rerank([dict(c) for c in cards], stats, {}, now=t0 + dt.timedelta(hours=30))
    as_text = ss.apply_saturation_rerank([dict(c) for c in cards], stats, {}, now=t0.isoformat())
    def spot0(res):
        return next(c for c in res if c["business_name"] == "Nail Spot 0")
    assert "seen_within_24h" in spot0(within)["rerank_reasons"]
    assert "seen_within_24h" not in spot0(later)["rerank_reasons"]
    assert [c["reranked_score"] for c in as_text] == [c["reranked_score"] for c in within]


def test_saved_run_without_captured_exposure_is_listed_missing(monkeypatch):
    monkeypatch.setattr(app, "get_candidate_exposure_stats", lambda scored, th: {"x": 1})
    rs.set_request_mode("saved")
    stats, now = app._scout_exposure_inputs(_cards(), "tok", "nail salon", "Astoria, NY")
    s = rs.run_state().summary()
    assert stats == {"x": 1} and now is None
    assert s["missing"][0]["kind"] == "input" and "scout_exposure" in s["missing"][0]["what"]


def test_saved_rerun_does_not_add_exposure_history():
    rs.set_request_mode("saved")
    assert app._replay_is_saved_run() is True
    rs.set_request_mode("all")
    assert app._replay_is_saved_run() is False
    rs.set_request_mode("off")
    assert app._replay_is_saved_run() is False


def test_each_missing_call_is_listed_separately(net):
    rs.set_request_mode("saved")
    for pid in ("p1", "p2", "p3"):
        app.fetch_google_place_details(pid)
    s = rs.run_state().summary()
    assert s["missing_count"] == 3
    assert {m["what"].split("place_id=")[1][:2] for m in s["missing"]} == {"p1", "p2", "p3"}
    assert all("g-test" not in m["what"] for m in s["missing"])     # key never shown
    assert net == []


def test_details_skipped_by_cap_in_capture_are_skipped_again(monkeypatch, net):
    cards = _cards()[:3]
    # capture: the cap ran out after one Details call
    left = {"n": 1}

    def capped(provider, amount=1):
        if provider != "google_details":
            return True
        left["n"] -= 1
        return left["n"] >= 0

    monkeypatch.setattr(app, "api_budget_ok", capped)
    rs.set_request_mode("all")
    app.enrich_shown_google_cards([dict(c) for c in cards], max_calls=3)
    monkeypatch.setattr(app, "api_budget_ok", lambda provider, amount=1: True)
    rs.set_request_mode("saved")
    again = [dict(c) for c in cards]
    app.enrich_shown_google_cards(again, max_calls=3)
    s = rs.run_state().summary()
    assert s["missing_count"] == 0 and s["live_calls"] == 0
    assert [c["website_checked"] for c in again] == [True, False, False]
    assert len(net) == 1
