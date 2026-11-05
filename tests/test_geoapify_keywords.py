"""Geoapify fallback: the user's words decide which places fit.

Regression for the 2026-11-04 Astoria test: a "nail salons in Astoria"
Scout fell back to Geoapify (Google cap reached), which returned every
beauty business with no ratings, so the cards tied and came out
alphabetically: barbers and hair studios ahead of nail salons.
"""
import json
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402
import requests  # noqa: E402

import app  # noqa: E402

QUERY = "nail salons in Astoria, Queens, NY with weak digital presence"


def geo(name, cats=("service", "service.beauty"), **tags):
    return {"business_name": name, "source": "geoapify", "platform": "geoapify",
            "geo_categories": list(cats), "osm_tags": tags, "opportunity_score": 60,
            "rating": 0, "review_count": 0, "score_reasons": []}


# The ten cards from the test, with the tags OpenStreetMap typically has
ASTORIA = [
    geo("Alex VIP"),
    geo("Ascend Day Spa", cats=("service.beauty", "service.beauty.spa"), leisure="spa"),
    geo("Barber Shop Beauty Salon", cats=("service.beauty", "service.beauty.hairdresser"), shop="barber"),
    geo("Blonde Me NYC", cats=("service.beauty", "service.beauty.hairdresser"), shop="hairdresser"),
    geo("Boris Barber Shop", shop="barber"),
    geo("Breeze Day Spa", cats=("service.beauty", "service.beauty.spa")),
    geo("C&K Nail Spa", shop="beauty"),
    geo("Camila Nails Salon", shop="beauty", beauty="nails"),
    geo("Desi First Hair Cut", cats=("service.beauty", "service.beauty.hairdresser")),
    geo("Estelle Hair Studio", shop="hairdresser"),
    geo("Lucky Beauty", shop="beauty", beauty="nails;waxing"),
]


def test_type_words_come_from_the_search_not_the_noise():
    assert app.query_type_terms("nail salon", QUERY) == {"nail"}
    assert app.query_type_terms("salon", "salons in Astoria") == set()      # too broad alone
    assert app.query_type_terms("", "day spa and massage near me") == {"spa", "massage"}
    assert app.query_type_terms("coffee shop", "") == {"cafe"}


@pytest.mark.parametrize("name,level", [
    ("Camila Nails Salon", "tag"),       # beauty=nails
    ("Lucky Beauty", "tag"),             # beauty=nails;waxing (multi-value tag)
    ("C&K Nail Spa", "name"),            # name says nail, tags don't
    ("Boris Barber Shop", "other_type"),
    ("Estelle Hair Studio", "other_type"),
    ("Desi First Hair Cut", "other_type"),
    ("Ascend Day Spa", "other_type"),
    ("Alex VIP", "unknown"),             # broad category only
])
def test_match_levels(name, level):
    c = next(x for x in ASTORIA if x["business_name"] == name)
    m = app.geoapify_query_match(c, {"nail"})
    assert m["level"] == level
    if level == "name":
        assert "inferred" in m["reason"]


def _rank(cards, niche="nail salon", query=QUERY):
    cards = [dict(c, score_reasons=list(c["score_reasons"])) for c in cards]
    app.apply_geoapify_query_match(cards, niche, query)
    cards.sort(key=lambda x: (
        -x["opportunity_score"],
        app._MATCH_TIER.get((x.get("query_match") or {}).get("level"), 1),
        -(x.get("rating") or 0), -(x.get("review_count") or 0),
        (x.get("business_name") or "").lower()))
    return cards


def test_nail_places_rank_first_and_nothing_is_removed():
    ranked = _rank(ASTORIA)
    names = [c["business_name"] for c in ranked]
    assert len(names) == len(ASTORIA)                                # boost, not a filter
    assert set(names[:3]) == {"Camila Nails Salon", "Lucky Beauty", "C&K Nail Spa"}
    assert names[3] == "Alex VIP"                                    # unknown in the middle
    assert set(names[4:]) == {"Ascend Day Spa", "Barber Shop Beauty Salon", "Blonde Me NYC",
                              "Boris Barber Shop", "Breeze Day Spa", "Desi First Hair Cut",
                              "Estelle Hair Studio"}


def test_reasons_use_the_card_format():
    ranked = {c["business_name"]: c for c in _rank(ASTORIA)}
    r = ranked["Camila Nails Salon"]["score_reasons"][0]
    assert r["impact"] == "positive" and r["key"] == "query_match"
    assert r["signal"] == "Listed as nail (OpenStreetMap beauty=nails), matching your search"
    assert ranked["Boris Barber Shop"]["score_reasons"][0]["impact"] == "negative"
    assert ranked["Alex VIP"]["score_reasons"][0]["impact"] == "neutral"


def test_no_type_words_changes_nothing():
    ranked = _rank(ASTORIA, niche="salon", query="salons in Astoria")
    assert all("query_match" not in c for c in ranked)
    assert all(c["opportunity_score"] == 60 for c in ranked)


def test_google_cards_use_google_types():
    # 2026-11-04: Google cards now get the same match, from Google's own types
    barber = {"business_name": "Boris Barber Shop", "source": "google_text", "platform": None,
              "google_types": ["hair_care", "point_of_interest"], "opportunity_score": 60,
              "score_reasons": []}
    app.apply_geoapify_query_match([barber], "nail salon", QUERY)
    assert barber["query_match"]["level"] == "other_type"
    assert barber["opportunity_score"] == 48


def test_search_geoapify_keeps_type_evidence(monkeypatch):
    monkeypatch.setenv("GEOAPIFY_KEY", "k")
    monkeypatch.setattr(app, "api_budget_ok", lambda provider, amount=1: True)

    def send(self, request, **kw):
        if "geocode" in request.url:
            body = {"features": [{"geometry": {"coordinates": [-73.92, 40.76]}}]}
        else:
            body = {"features": [{"properties": {
                "name": "Camila Nails Salon", "formatted": "31st St, Astoria",
                "categories": ["service", "service.beauty"],
                "datasource": {"raw": {"shop": "beauty", "beauty": "nails", "phone": "+1 718", "opening_hours": "Mo-Su"}}}}]}
        r = requests.models.Response()
        r.status_code = 200
        r._content = json.dumps(body).encode()
        r.headers["Content-Type"] = "application/json"
        r.url = request.url
        r.request = request
        return r

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    out = app.search_geoapify("nail salon", "Astoria, NY")
    assert out[0]["geo_categories"] == ["service", "service.beauty"]
    assert out[0]["osm_tags"] == {"shop": "beauty", "beauty": "nails"}    # only type tags kept


def test_reason_names_the_exact_evidence():
    by = {c["business_name"]: c for c in ASTORIA}
    m = app.geoapify_query_match(by["Lucky Beauty"], {"nail"})          # beauty=nails;waxing
    assert m["reason"] == "Listed as nail (OpenStreetMap beauty=nails), matching your search"
    m = app.geoapify_query_match(by["Boris Barber Shop"], {"nail"})
    assert m["reason"] == "Listed as barber (OpenStreetMap shop=barber), not nail"
    # category-only evidence is credited to Geoapify, not OpenStreetMap
    m = app.geoapify_query_match(by["Desi First Hair Cut"], {"nail"})
    assert "Geoapify category service.beauty.hairdresser" in m["reason"]
    assert "OpenStreetMap" not in m["reason"]
    # an OpenStreetMap tag wins over a Geoapify category for the same type
    m = app.geoapify_query_match(by["Blonde Me NYC"], {"nail"})
    assert "OpenStreetMap shop=hairdresser" in m["reason"]
