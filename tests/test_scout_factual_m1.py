"""Milestone 1: Scout ranks on search-response fields, enriches shown cards
afterward, gates closed businesses, and never states unchecked facts.

All Google calls are mocked; nothing here touches the network or a database.
"""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import requests  # noqa: E402

import app  # noqa: E402

MASTER = "m1-test-master"
_ORIGINAL_LIMITS = dict(app.API_CALL_LIMITS)

# Phrases that state an absence or weakness of website, booking or social.
BANNED = [
    "no website found", "no website confirmed", "no website -", "no web presence",
    "no online booking", "no booking path", "booking path not clearly surfaced",
    "weak or no social presence", "no social profiles detected", "little social media presence",
    "barely visible online", "weak social", "minimal digital footprint",
    "limited contact signals", "no website or booking", "missing an online booking",
    "matches queried problem",
]


def place(i, name, rating, reviews, status="OPERATIONAL"):
    p = {
        "name": name,
        "place_id": f"pid-{i}",
        "formatted_address": f"{i} Main St, Brooklyn, NY",
        "vicinity": f"{i} Main St",
        "rating": rating,
        "user_ratings_total": reviews,
    }
    if status is not None:
        p["business_status"] = status
    return p


TARGET = place(99, "Golden Crumb Bakery", 4.6, 140)

OTHERS = [
    place(1, "Sunrise Pastry House", 4.2, 60),
    place(2, "Corner Loaf Company", 4.8, 300),
    place(3, "Maple Street Bakeshop", 3.9, 12),
    place(4, "Brooklyn Rye Works", 4.5, 520),
    place(5, "Little Oven Cafe", 4.7, 35),
    place(6, "Crescent Moon Bakery", 4.1, 8),
    place(7, "Flour and Fig Studio", 4.4, 95),
    place(8, "Harbor Bread Collective", 4.0, 210),
    place(9, "Sweet Ember Patisserie", 4.9, 22),
]


def with_target_at(position):
    """List of places with TARGET at a 1-based position."""
    rows = list(OTHERS)
    rows.insert(position - 1, TARGET)
    return rows


class FakeGoogle:
    """Routes requests.get by URL; records every call in order."""

    def __init__(self, places, websites=None, failing=()):
        self.places = places
        self.websites = websites or {}
        self.failing = set(failing)
        self.calls = []          # ("text"|"nearby"|"details", place_id)

    def __call__(self, url, params=None, **kw):
        params = params or {}
        if "textsearch" in url:
            self.calls.append(("text", None))
            return _Resp({"status": "OK", "results": self.places})
        if "nearbysearch" in url:
            self.calls.append(("nearby", None))
            return _Resp({"status": "OK", "results": self.places})
        if "details" in url:
            pid = params.get("place_id")
            self.calls.append(("details", pid))
            if pid in self.failing:
                raise requests.ConnectionError("boom")
            return _Resp({"status": "OK", "result": {
                "website": self.websites.get(pid, ""),
                "formatted_phone_number": f"(718) 555-{pid[-2:].zfill(4)}",
            }})
        raise AssertionError(f"unexpected outbound call: {url}")

    def details_ids(self):
        return [pid for kind, pid in self.calls if kind == "details"]


class _Resp:
    status_code = 200

    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def _setup(monkeypatch, fake, details_limit=None):
    monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "x")
    monkeypatch.setattr(app, "MASTER_CODE", MASTER)
    monkeypatch.setattr(app, "_DB_AVAILABLE", False)
    monkeypatch.setattr(requests, "get", fake)
    limits = dict(_ORIGINAL_LIMITS)
    if details_limit is not None:
        limits["google_details"] = details_limit
    monkeypatch.setattr(app, "API_CALL_LIMITS", limits)
    monkeypatch.setattr(app, "geocode_business", lambda *a, **k: (40.68, -73.94))


def scout(radius=0, mode="outreach", limit=10, **extra):
    body = {"niche": "bakery", "location": "Brooklyn, NY", "mode": mode,
            "limit": limit, "radius": radius}
    body.update(extra)
    r = app.app.test_client().post("/prospect", json=body, headers={"X-Agent-Token": MASTER})
    assert r.status_code == 200, r.get_data(as_text=True)
    return r.get_json()


def card(data, name):
    for c in data["results"]:
        if c["business_name"] == name:
            return c
    raise AssertionError(f"{name} not in results")


def score_view(c):
    ns = c.get("need_score") or {}
    return (c["opportunity_score"], c["score_confidence"],
            ns.get("score"), ns.get("level"), tuple(ns.get("reasons") or []),
            c.get("opportunity_lane"))


def ranking_view(data):
    return [(c["business_name"],) + score_view(c) for c in data["results"]]


# ── Position independence ──────────────────────────────────────────────────

def test_same_business_same_score_at_position_4_and_7_text_search(monkeypatch):
    views = []
    for pos in (4, 7):
        _setup(monkeypatch, FakeGoogle(with_target_at(pos), websites={"pid-99": "https://goldencrumbbakery.com"}))
        views.append(score_view(card(scout(), "Golden Crumb Bakery")))
    assert views[0] == views[1]


def test_same_business_same_score_at_position_4_and_7_nearby_search(monkeypatch):
    views = []
    for pos in (4, 7):
        _setup(monkeypatch, FakeGoogle(with_target_at(pos), websites={"pid-99": "https://goldencrumbbakery.com"}))
        data = scout(radius=3)
        assert all(c["source"] == "google_nearby" for c in data["results"])
        views.append(score_view(card(data, "Golden Crumb Bakery")))
    assert views[0] == views[1]


# ── Rank first, enrich second ──────────────────────────────────────────────

def test_no_details_calls_before_ranking(monkeypatch):
    fake = FakeGoogle(with_target_at(4))
    _setup(monkeypatch, fake)
    seen = {}
    real = app.score_and_explain

    def spy(*a, **k):
        seen.setdefault("details_before_first_score", len(fake.details_ids()))
        return real(*a, **k)

    monkeypatch.setattr(app, "score_and_explain", spy)
    scout()
    assert seen["details_before_first_score"] == 0


def test_enrichment_covers_every_shown_google_card_in_rank_order(monkeypatch):
    fake = FakeGoogle(with_target_at(7))
    _setup(monkeypatch, fake)
    data = scout(limit=10)
    shown = [c["business_name"] for c in data["results"]]
    by_name = {p["name"]: p["place_id"] for p in with_target_at(7)}
    assert fake.details_ids() == [by_name[n] for n in shown]
    for c in data["results"]:
        assert c["website_checked"] is True and c["phone"]


def test_enrichment_stops_at_the_shown_limit(monkeypatch):
    fake = FakeGoogle(with_target_at(7))
    _setup(monkeypatch, fake)
    data = scout(limit=4)
    assert len(data["results"]) == 4
    assert len(fake.details_ids()) == 4


def test_daily_details_cap_leaves_the_rest_not_checked(monkeypatch):
    fake = FakeGoogle(with_target_at(7))
    _setup(monkeypatch, fake)
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    used = {"n": 0}

    def counter(day, scope, key, kind, amount):
        if kind == "api_google_details":
            used["n"] += amount
            return used["n"]
        return 1

    monkeypatch.setattr(app, "increment_usage", counter)
    limits = dict(app.API_CALL_LIMITS, google_details=3)
    monkeypatch.setattr(app, "API_CALL_LIMITS", limits)
    for name in ("save_search", "upsert_business", "log_discovery_event"):
        monkeypatch.setattr(app, name, lambda *a, **k: None)
    monkeypatch.setattr(app, "get_candidate_exposure_stats", lambda *a, **k: {})
    monkeypatch.setattr(app, "apply_saturation_rerank", lambda scored, *a, **k: scored)
    data = scout(limit=10)
    assert len(fake.details_ids()) == 3
    assert [c["website_checked"] for c in data["results"]][:4] == [True, True, True, False]


def test_budget_guard_off_means_no_enrichment(monkeypatch):
    fake = FakeGoogle(with_target_at(4))
    _setup(monkeypatch, fake, details_limit=0)
    data = scout()
    assert fake.details_ids() == []
    assert all(c["website_checked"] is False for c in data["results"])


def test_enrichment_never_changes_scores_or_order(monkeypatch):
    websites = {"pid-2": "https://cornerloaf.com", "pid-99": "https://goldencrumbbakery.com"}
    _setup(monkeypatch, FakeGoogle(with_target_at(4), websites=websites), details_limit=0)
    without = ranking_view(scout())
    _setup(monkeypatch, FakeGoogle(with_target_at(4), websites=websites))
    enriched = scout()
    assert ranking_view(enriched) == without
    assert any(c["website"] for c in enriched["results"])  # enrichment did happen


def test_failed_details_call_stays_not_checked(monkeypatch):
    fake = FakeGoogle(with_target_at(4), failing={"pid-99", "pid-2", "pid-5", "pid-9", "pid-1", "pid-7"})
    _setup(monkeypatch, fake)
    data = scout()
    for c in data["results"]:
        if c["business_name"] in ("Golden Crumb Bakery",):
            assert c["website_checked"] is False and c["website_fact"] == ""


def test_checked_absent_website_is_a_display_fact_only(monkeypatch):
    # Every listing returns no website from Details
    _setup(monkeypatch, FakeGoogle(with_target_at(4)), details_limit=0)
    before = ranking_view(scout())
    _setup(monkeypatch, FakeGoogle(with_target_at(4)))
    data = scout()
    assert ranking_view(data) == before
    for c in data["results"]:
        assert c["website_checked"] is True
        assert c["website_fact"] == "No website on the Google listing"


# ── Operating status gate ──────────────────────────────────────────────────

def test_operating_status_gate(monkeypatch):
    rows = list(OTHERS[:5]) + [
        place(50, "Shuttered Bread Co", 4.9, 400, status="CLOSED_PERMANENTLY"),
        place(51, "Paused Pie Shop", 4.7, 150, status="CLOSED_TEMPORARILY"),
        place(52, "Mystery Muffin Bar", 4.3, 40, status=None),
    ]
    _setup(monkeypatch, FakeGoogle(rows))
    data = scout()
    names = [c["business_name"] for c in data["results"]]
    held = data["held_back"]
    assert "Shuttered Bread Co" not in names
    assert all(h["business_name"] != "Shuttered Bread Co" for h in held)
    assert "Paused Pie Shop" not in names
    assert held == [{
        "business_name": "Paused Pie Shop",
        "address": "51 Main St, Brooklyn, NY",
        "source": "google_text",
        "business_status": "CLOSED_TEMPORARILY",
        "reason": "Google lists this business as temporarily closed.",
    }]
    assert "Mystery Muffin Bar" in names


def test_gate_function_directly():
    kept, held = app.apply_operating_status_gate([
        {"business_name": "A", "business_status": "OPERATIONAL"},
        {"business_name": "B", "business_status": "CLOSED_PERMANENTLY"},
        {"business_name": "C", "business_status": "closed_temporarily"},
        {"business_name": "D"},
    ])
    assert [c["business_name"] for c in kept] == ["A", "D"]
    assert [h["business_name"] for h in held] == ["C"]


# ── No absence claims for unchecked fields ─────────────────────────────────

def _texts(c):
    out = [r.get("signal", "") for r in (c.get("score_reasons") or [])]
    out += list((c.get("need_score") or {}).get("reasons") or [])
    return out


def test_no_absence_reasons_on_scout_cards(monkeypatch):
    for mode in ("outreach", "partnership", "feature", "market"):
        _setup(monkeypatch, FakeGoogle(with_target_at(4)), details_limit=0)
        data = scout(mode=mode)
        for c in data["results"]:
            for text in _texts(c):
                low = text.lower()
                assert not any(b in low for b in BANNED), (mode, c["business_name"], text)


def test_no_absence_wording_in_summary_top_signal_or_insight():
    cand = {
        "business_name": "Golden Crumb Bakery", "industry": "bakery",
        "rating": 4.6, "review_count": 140, "website": "", "platform": "google_places",
        "website_checked": False, "social_checked": False, "booking_checked": False,
    }
    for mode in ("outreach", "partnership", "feature", "market"):
        out = app.score_and_explain(dict(cand), mode)
        texts = [out["opportunity_summary"], out["top_signal"], out["quick_insight"]]
        texts += [r["signal"] for r in out["score_reasons"]]
        for text in texts:
            assert not any(b in text.lower() for b in BANNED), (mode, text)


def test_need_score_skips_unchecked_website(monkeypatch):
    cand = {"business_name": "X", "rating": 4.6, "review_count": 140, "website": "",
            "website_checked": False, "social_checked": False, "booking_checked": False}
    import signal_service
    ns = signal_service.compute_need_score(cand, {"problem_signal": "website booking social"})
    joined = " ".join(ns["reasons"]).lower()
    assert "website" not in joined and "social" not in joined and "booking" not in joined
    unchecked_plain = signal_service.compute_need_score(dict(cand), {})
    checked = signal_service.compute_need_score(dict(cand, website_checked=True), {})
    assert "No website confirmed" in checked["reasons"]
    assert checked["score"] - unchecked_plain["score"] == 25  # website term only when checked


def test_unchecked_website_never_beats_checked_present():
    base = {"business_name": "Golden Crumb Bakery", "industry": "bakery",
            "rating": 4.6, "review_count": 140, "platform": "google_places"}
    for mode in ("outreach", "partnership", "feature", "market"):
        unchecked = app.score_and_explain(dict(base, website="", website_checked=False), mode)
        present = app.score_and_explain(dict(base, website="https://goldencrumbbakery.com"), mode)
        assert unchecked["opportunity_score"] <= present["opportunity_score"], mode


def test_hard_filter_does_not_reject_unchecked_website():
    cand = {"business_name": "X", "website": "", "platform": "google_places",
            "website_checked": False, "booking_checked": False}
    ok, _ = app.apply_hard_filters(cand, {"requires_website": "yes", "requires_booking": "yes"})
    assert ok
    ok2, _ = app.apply_hard_filters(dict(cand, website_checked=True), {"requires_website": "yes"})
    assert not ok2


# ── Phone stored ───────────────────────────────────────────────────────────

def test_phone_kept_and_passed_to_upsert(monkeypatch):
    fake = FakeGoogle(with_target_at(4))
    _setup(monkeypatch, fake)
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "increment_usage", lambda *a, **k: 1)
    monkeypatch.setattr(app, "save_search", lambda *a, **k: 1)
    saved = []
    monkeypatch.setattr(app, "upsert_business", lambda d: saved.append(d) or len(saved))
    monkeypatch.setattr(app, "log_discovery_event", lambda **k: None)
    monkeypatch.setattr(app, "get_candidate_exposure_stats", lambda *a, **k: {})
    monkeypatch.setattr(app, "apply_saturation_rerank", lambda scored, *a, **k: scored)
    data = scout()
    enriched = [c for c in data["results"] if c["website_checked"]]
    assert enriched and all(c["phone"].startswith("(718) 555-") for c in enriched)
    by_name = {d["name"]: d for d in saved}
    for c in enriched:
        assert by_name[c["business_name"]]["phone"] == c["phone"]
        assert by_name[c["business_name"]]["raw_data"]["website_checked"] is True


# ── Other callers keep today's behavior ────────────────────────────────────

def test_default_text_search_still_fetches_details_inline(monkeypatch):
    fake = FakeGoogle(with_target_at(4), websites={"pid-1": "https://sunrisepastry.com"})
    _setup(monkeypatch, fake)
    rows = app.search_google_places("bakery", "Brooklyn, NY", limit=10)
    assert len(fake.details_ids()) == 5
    assert rows[0]["website"] == "https://sunrisepastry.com"
    assert all("website_checked" not in r for r in rows)


def test_default_nearby_still_fetches_details_inline(monkeypatch):
    fake = FakeGoogle(with_target_at(4), websites={"pid-1": "https://sunrisepastry.com"})
    _setup(monkeypatch, fake)
    cards = app.nearby_competitors_google(40.68, -73.94, "bakery", 1500, limit=8)
    assert len(fake.details_ids()) == 4
    assert cards[0]["website"] == "https://sunrisepastry.com"


def test_scout_text_search_passes_fetch_details_off(monkeypatch):
    fake = FakeGoogle(with_target_at(4))
    _setup(monkeypatch, fake)
    rows = app.search_google_places("bakery", "Brooklyn, NY", limit=10, fetch_details=False)
    assert fake.details_ids() == []
    assert all(r["website_checked"] is False and r["google_place_id"] for r in rows)
