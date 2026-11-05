"""Scout quality pass 1 (2026-11-04).

- Free Google search-response fields kept: price level, types, open now, coordinates.
- Nearby price labels fixed (they were corrupted to "20", "20$", "2020").
- Google cards get the type match Geoapify cards already had.
- Area check: text-search results far from the rest are labeled and lowered, never removed.
- Duplicates matched by place ID, then full name plus street (not a 20-letter prefix).
- The search log keeps what the user typed.

All Google calls are mocked; nothing here touches the network or a database.
"""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402
import requests  # noqa: E402

import app  # noqa: E402
from tests.test_scout_factual_m1 import FakeGoogle, _setup, card  # noqa: E402

MASTER = "m1-test-master"


# ── Price labels ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("level,label", [
    (None, ""), ("", ""), (0, "Free"), (1, "$"), (2, "$$"), (3, "$$$"), (4, "$$$$"), (7, ""), ("x", ""),
])
def test_price_label(level, label):
    assert app.google_price_label(level) == label


def _gplace(i, name, *, types=None, price=None, lat=40.7644, lng=-73.9235,
            address=None, open_now=None, reviews=40):
    p = {"name": name, "place_id": f"pid-{i}",
         "formatted_address": address or f"{i} 31st St, Astoria, NY 11102",
         "vicinity": f"{i} 31st St", "rating": 4.5, "user_ratings_total": reviews,
         "business_status": "OPERATIONAL", "types": types or ["beauty_salon", "establishment"],
         "geometry": {"location": {"lat": lat, "lng": lng}}}
    if price is not None:
        p["price_level"] = price
    if open_now is not None:
        p["opening_hours"] = {"open_now": open_now}
    return p


def test_nearby_price_labels_are_fixed(monkeypatch):
    fake = FakeGoogle([_gplace(1, "Two Dollar Spot", price=2), _gplace(2, "No Price Spot")])
    _setup(monkeypatch, fake)
    cards = app.nearby_competitors_google(40.76, -73.92, "nail salon", fetch_details=False)
    assert [c["price_level"] for c in cards] == ["$$", "unknown"]
    assert cards[0]["types"] == ["beauty_salon", "establishment"]
    assert cards[0]["lat"] == 40.7644


def test_text_search_keeps_free_fields(monkeypatch):
    fake = FakeGoogle([_gplace(1, "Camila Nails", price=1, open_now=True)])
    _setup(monkeypatch, fake)
    rows = app.search_google_places("nail salon", "Astoria, NY", fetch_details=False)
    r = rows[0]
    assert (r["price_level"], r["open_now"], r["lat"], r["lng"]) == ("$", True, 40.7644, -73.9235)
    assert r["google_types"] == ["beauty_salon", "establishment"]


# ── Google type match ──────────────────────────────────────────────────────

def _gcard(name, types, source="google_text"):
    return {"business_name": name, "source": source, "google_types": types,
            "opportunity_score": 60, "score_reasons": []}


def test_google_barber_ranks_below_in_nail_search():
    barber = _gcard("Boris Cuts", ["hair_care", "establishment"])
    app.apply_geoapify_query_match([barber], "nail salon", "nail salons in Astoria")
    assert barber["query_match"]["level"] == "other_type"
    assert "Google type hair_care" in barber["query_match"]["reason"]
    assert barber["opportunity_score"] == 48
    assert barber["score_reasons"][0]["impact"] == "negative"


def test_broad_beauty_salon_is_not_ruled_out():
    # Many nail salons are listed by Google as beauty_salon plus hair_care
    salon = _gcard("Glamour Studio", ["beauty_salon", "hair_care", "establishment"])
    app.apply_geoapify_query_match([salon], "nail salon", "nail salons in Astoria")
    assert salon["query_match"]["level"] == "unknown"
    assert salon["opportunity_score"] == 60
    assert salon["score_reasons"] == []          # no noise line on Google cards


def test_google_type_confirms_match():
    # Google searched with a broad word ("salon"): the type adds evidence
    shop = _gcard("Blonde Me", ["hair_care", "beauty_salon"], source="google_nearby")
    app.apply_geoapify_query_match([shop], "salon", "hair salons in Astoria")
    assert shop["query_match"]["level"] == "tag"
    assert shop["opportunity_score"] == 72


def test_name_still_counts_on_google_cards():
    c = _gcard("Lucky Nails & Hair", ["hair_care"])
    app.apply_geoapify_query_match([c], "salon", "nail salons")
    assert c["query_match"]["level"] == "name"
    assert c["opportunity_score"] == 70


def test_match_on_the_searched_type_adds_nothing_on_google_cards():
    # Google was searched "nail salon": a name saying nail only repeats Google's pick
    c = _gcard("Lucky Nails", ["beauty_salon"])
    app.apply_geoapify_query_match([c], "nail salon", "nail salons in Astoria")
    assert c["query_match"]["level"] == "name"
    assert c["opportunity_score"] == 60 and c["score_reasons"] == []
    # A different type is still ranked down
    barber = _gcard("Boris Cuts", ["hair_care"])
    app.apply_geoapify_query_match([barber], "nail salon", "nail salons in Astoria")
    assert barber["opportunity_score"] == 48


# ── Area check ─────────────────────────────────────────────────────────────

def _acard(name, lat, lng, address, source="google_text"):
    return {"business_name": name, "source": source, "lat": lat, "lng": lng,
            "address": address, "opportunity_score": 60, "score_reasons": []}


ASTORIA_PTS = [(40.7644, -73.9235), (40.7660, -73.9200), (40.7620, -73.9260),
               (40.7700, -73.9180), (40.7590, -73.9150)]


def _astoria_cards(far_address="200 W 57th St, New York, NY 10019"):
    cards = [_acard(f"Near {i}", lat, lng, f"{i} Ditmars Blvd, Queens, NY")
             for i, (lat, lng) in enumerate(ASTORIA_PTS)]
    cards.append(_acard("Midtown Nails", 40.7651, -73.9799, far_address))
    return cards


def test_far_result_is_labeled_and_lowered_not_removed():
    cards = _astoria_cards()
    app.apply_area_check(cards, "Astoria, Queens, NY")
    far = cards[-1]
    assert far["area_check"]["level"] == "far"
    assert far["area_check"]["km"] > 4
    assert "possibly outside Astoria" in far["area_check"]["reason"]
    assert far["opportunity_score"] == 50
    assert len(cards) == 6
    assert all("area_check" not in c for c in cards[:-1])
    assert all(c["opportunity_score"] == 60 for c in cards[:-1])


def test_area_name_in_address_wins():
    cards = _astoria_cards(far_address="1 Steinway St, Astoria, NY 11103")
    app.apply_area_check(cards, "Astoria, Queens, NY")
    assert cards[-1]["area_check"]["level"] == "address"
    assert cards[-1]["opportunity_score"] == 60


def test_too_few_points_means_no_far_label():
    cards = _astoria_cards()[-3:]
    app.apply_area_check(cards, "Astoria, Queens, NY")
    assert all("area_check" not in c for c in cards)


def test_nearby_and_geoapify_cards_are_not_area_checked():
    cards = [_acard(f"N{i}", lat, lng, "x", source="google_nearby") for i, (lat, lng) in enumerate(ASTORIA_PTS)]
    cards.append(_acard("Far", 40.7651, -73.9799, "y", source="google_nearby"))
    app.apply_area_check(cards, "Astoria, NY")
    assert all("area_check" not in c for c in cards)


# ── Dedup ──────────────────────────────────────────────────────────────────

def test_dedup_key():
    k = app.scout_dedup_key
    assert k({"business_name": "A", "google_place_id": "p1"}) == k({"business_name": "B", "google_place_id": "p1"})
    long_a = {"business_name": "Brooklyn Nail Spa", "address": "1 Main St, Brooklyn, NY"}
    long_b = {"business_name": "Brooklyn Nail Spa & Lounge", "address": "1 Main St, Brooklyn, NY"}
    assert k(long_a) != k(long_b)
    branch2 = {"business_name": "Brooklyn Nail Spa", "address": "88 Court St, Brooklyn, NY"}
    assert k(long_a) != k(branch2)
    same = {"business_name": "brooklyn nail spa", "address": "1 Main St., Brooklyn"}
    assert k(long_a) == k(same)
    assert k({"business_name": "x"}) == ""


def test_scout_keeps_two_branches_and_long_shared_names(monkeypatch):
    places = [
        _gplace(1, "Brooklyn Nail Spa", address="1 Main St, Brooklyn, NY"),
        _gplace(2, "Brooklyn Nail Spa & Lounge", address="5 Main St, Brooklyn, NY"),
        _gplace(3, "Brooklyn Nail Spa", address="88 Court St, Brooklyn, NY"),
        _gplace(1, "Brooklyn Nail Spa", address="1 Main St, Brooklyn, NY"),   # true duplicate
    ]
    _setup(monkeypatch, FakeGoogle(places))
    r = app.app.test_client().post("/prospect", json={"niche": "nail salon", "location": "Brooklyn, NY"},
                                   headers={"X-Agent-Token": MASTER})
    data = r.get_json()
    assert len(data["results"]) == 3


# ── Scout response and search log ──────────────────────────────────────────

def test_cards_carry_new_fields(monkeypatch):
    places = [_gplace(i, f"Spot {i}", price=2, open_now=False,
                      lat=ASTORIA_PTS[i][0], lng=ASTORIA_PTS[i][1]) for i in range(5)]
    places.append(_gplace(9, "Midtown Glow", lat=40.7651, lng=-73.9799,
                          address="200 W 57th St, New York, NY 10019"))
    _setup(monkeypatch, FakeGoogle(places))
    r = app.app.test_client().post("/prospect", json={"niche": "nail salon", "location": "Astoria, NY"},
                                   headers={"X-Agent-Token": MASTER})
    data = r.get_json()
    c = card(data, "Spot 0")
    assert c["price_level"] == "$$" and c["open_now"] is False
    assert c["area_check"]["level"] == "address"
    m = card(data, "Midtown Glow")
    assert m["area_check"]["level"] == "far"
    assert data["results"][-1]["business_name"] == "Midtown Glow"
    # Old fields still present
    for f in ("opportunity_score", "score_reasons", "website_checked", "query_match"):
        assert f in c


def test_search_log_keeps_user_words(monkeypatch):
    saved = {}
    _setup(monkeypatch, FakeGoogle([_gplace(1, "Camila Nails")]))
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "save_search", lambda d: saved.update(d) or 1)
    monkeypatch.setattr(app, "upsert_business", lambda d: None)
    monkeypatch.setattr(app, "_scout_exposure_inputs", lambda *a, **k: ({}, None))
    app.app.test_client().post("/prospect", json={
        "niche": "nail salon", "location": "Astoria, NY",
        "user_query": "cute nail spots in Astoria for a creator video"},
        headers={"X-Agent-Token": MASTER})
    assert saved["user_query"] == "cute nail spots in Astoria for a creator video"
    assert saved["target_business_type"] == "nail salon"
