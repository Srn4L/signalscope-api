"""Meaning match (embeddings), first version: ranking only.

Offline: a fake embedder stands in for OpenAI. Each text is embedded as a
bag of concept words so similarities are predictable.
"""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402

import app  # noqa: E402
import embedding_service as es  # noqa: E402
import replay_service as rs  # noqa: E402

# Concepts: each vector dimension is one concept
CONCEPTS = {
    "westafrica": ("jollof", "waakye", "ghanaian", "nigerian", "west african", "suya", "fufu"),
    "caribbean": ("jerk", "jamaican", "haitian", "caribbean", "oxtail"),
    "nails": ("nail", "manicure", "pedicure"),
    "food": ("restaurant", "food", "spot", "eat"),
}


def fake_vec(text):
    """Concept counts, plus one text-specific dimension so texts without any
    concept are unrelated to each other (as real embeddings would be)."""
    import hashlib
    t = text.lower()
    vec = [float(sum(1 for w in words if w in t)) for words in CONCEPTS.values()]
    other = [0.0] * 64
    other[int(hashlib.sha256(t.encode()).hexdigest(), 16) % 64] = 0.5
    return vec + other


class Embedder:
    def __init__(self):
        self.calls = []

    def __call__(self, texts, budget_ok=None):
        if budget_ok is not None and not budget_ok("openai_embed"):
            return None
        self.calls.append(list(texts))
        return [fake_vec(t) for t in texts]


@pytest.fixture
def emb(monkeypatch):
    e = Embedder()
    monkeypatch.setattr(es, "_live_embed", e)
    monkeypatch.setattr(es, "MATCH_MIN", 0.6)
    store = {}
    monkeypatch.setattr(es, "_db_get", lambda hashes: {h: store[h] for h in hashes if h in store})
    monkeypatch.setattr(es, "_db_put", lambda rows: store.update({h: v for h, _, _, v in rows}))
    e.store = store
    return e


def test_jollof_means_west_african(emb):
    m = es.match_query("where can I get good jollof in the Bronx")
    assert m["status"] == "matched"
    assert m["terms"][0] == "west_african"
    assert m["top"][0][0] == "west_african"


def test_unrelated_text_matches_nothing(emb, monkeypatch):
    monkeypatch.setattr(es, "MATCH_MIN", 0.95)
    m = es.match_query("something fun to do this weekend")
    assert m["status"] == "no_match" and m["terms"] == []


def test_same_text_is_never_embedded_twice(emb):
    es.match_query("waakye spots in Harlem")
    first = len(emb.calls)
    es._MEM.clear()                       # new process: table still has it
    es.match_query("waakye spots in Harlem")
    assert len(emb.calls) == first == 1   # catalog + query in one call, then none


def test_no_key_or_cap_means_keyword_only(emb, monkeypatch):
    m = es.match_query("jollof in the Bronx", budget_ok=lambda p: False)
    assert m["status"] == "unavailable" and m["terms"] == []


def test_saved_rerun_never_calls_and_lists_missing(emb, monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    rs.set_request_mode("saved")
    try:
        m = es.match_query("suya near me")
        s = rs.run_state().summary()
    finally:
        rs.set_request_mode("off")
    assert emb.calls == []
    assert m["status"] == "unavailable"
    assert s["live_calls"] == 0 and s["missing_count"] >= 1
    assert s["missing"][0]["kind"] == "embedding"


def test_capture_then_saved_rerun_matches_with_zero_live(emb, monkeypatch):
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    rs.set_request_mode("all")
    cap = es.match_query("ghanaian waakye and suya in the Bronx")
    cap_sum = rs.run_state().summary()
    es._MEM.clear()
    rs.set_request_mode("saved")
    again = es.match_query("ghanaian waakye and suya in the Bronx")
    s = rs.run_state().summary()
    rs.set_request_mode("off")
    assert cap_sum["live_calls"] == 1
    assert again["terms"] == cap["terms"] == ["west_african"]
    assert s["live_calls"] == 0 and s["missing_count"] == 0 and s["replayed_calls"] == 1


# ── Scout ranking ──────────────────────────────────────────────────────────

def _gcard(name, score=60):
    return {"business_name": name, "source": "google_text", "google_types": ["restaurant"],
            "opportunity_score": score, "score_reasons": []}


def test_meaning_terms_boost_matching_names_when_google_was_searched_broadly():
    cards = [_gcard("Golden Krust Caribbean"), _gcard("Accra Ghanaian Kitchen"), _gcard("Joe's Diner")]
    app.apply_geoapify_query_match(cards, "restaurant", "jollof in the Bronx", extra_terms=["west_african"])
    by = {c["business_name"]: c for c in cards}
    assert by["Accra Ghanaian Kitchen"]["opportunity_score"] == 70
    assert by["Golden Krust Caribbean"]["opportunity_score"] == 60
    assert by["Joe's Diner"]["opportunity_score"] == 60


def test_cuisine_searched_with_google_is_not_reshuffled_by_names():
    # Google already searched "West African restaurant": "Accra" must not lose to names saying African
    cards = [_gcard("Accra"), _gcard("Adom African Cuisine")]
    app.apply_geoapify_query_match(cards, "West African restaurant", "West African restaurants in the Bronx")
    assert [c["opportunity_score"] for c in cards] == [60, 60]


def test_unknown_meaning_terms_are_ignored():
    cards = [_gcard("Accra")]
    app.apply_geoapify_query_match(cards, "restaurant", "x", extra_terms=["not_a_type"])
    assert "query_match" not in cards[0]


def test_scout_response_carries_query_meaning(monkeypatch, emb):
    from tests.test_scout_factual_m1 import FakeGoogle, MASTER, _setup
    places = [{"name": "Ghanaian Kitchen", "place_id": "p1", "formatted_address": "1 Ave, Bronx, NY",
               "rating": 4.5, "user_ratings_total": 40, "business_status": "OPERATIONAL",
               "types": ["restaurant"], "geometry": {"location": {"lat": 40.85, "lng": -73.89}}}]
    _setup(monkeypatch, FakeGoogle(places))
    # Authenticated queries are embedded only after their key is registered
    import journey_service as js
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(js, "register_query_key", lambda *a, **k: True)
    monkeypatch.setattr(js, "log_scout_run", lambda **k: None)
    monkeypatch.setattr(app, "save_search", lambda d: 1)
    monkeypatch.setattr(app, "upsert_business", lambda d: None)
    monkeypatch.setattr(app, "_scout_exposure_inputs", lambda *a, **k: ({}, None))
    r = app.app.test_client().post("/prospect", json={
        "niche": "restaurant", "location": "Bronx, NY", "user_query": "jollof spots in the Bronx"},
        headers={"X-Agent-Token": MASTER})
    data = r.get_json()
    assert data["query_meaning"]["terms"] == ["west_african"]
    assert data["results"][0]["query_match"]["level"] == "name"


def test_searched_type_match_gives_no_tiebreak_edge():
    cards = [_gcard("Accra"), _gcard("Mama G. African Kitchen")]
    app.apply_geoapify_query_match(cards, "West African restaurant", "West African restaurants in the Bronx")
    assert [app.match_tier(c) for c in cards] == [1, 1]
