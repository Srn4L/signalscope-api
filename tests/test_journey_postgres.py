"""Journey records (scout_runs, shown_results) and query_embeddings against a real Postgres.

Runs only when JOURNEY_TEST_DATABASE_URL points at a disposable database
(never production). Skipped otherwise.
"""
import json
import os

import pytest

URL = os.environ.get("JOURNEY_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="set JOURNEY_TEST_DATABASE_URL to a disposable Postgres")


@pytest.fixture(autouse=True)
def _restore_db_module():
    """Point db back at the original settings afterwards, so later tests run offline."""
    before = os.environ.get("DATABASE_URL")
    yield
    import importlib
    import db
    if before is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = before
    importlib.reload(db)
    import journey_service as js
    import embedding_service as es
    js._TABLES_READY = False
    es._TABLE_READY = False


@pytest.fixture
def pg(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", URL)
    import importlib
    import db
    importlib.reload(db)
    import journey_service as js
    js._TABLES_READY = False
    from sqlalchemy import text
    with db.get_db_session() as s:
        s.execute(text("DROP TABLE IF EXISTS shown_results"))
        s.execute(text("DROP TABLE IF EXISTS scout_runs"))
    yield js, db, text


CARDS = [
    {"business_name": "Chris Spa & Nails", "google_place_id": "pid-1", "opportunity_score": 53,
     "score_reasons": [{"key": "query_match", "signal": "Name mentions nail", "impact": "positive", "weight": "high"}],
     "source": "google_text", "rating": 4.9, "review_count": 187, "price_level": "$$", "open_now": True,
     "query_match": {"level": "name", "terms": ["nail"], "searched": True},
     "area_check": {"level": "address", "reason": "Address is in Astoria"},
     "seen_before": {"count": 2}},
    {"business_name": "Is A Nails Spa", "google_place_id": "pid-2", "opportunity_score": 53,
     "score_reasons": [], "source": "google_text", "rating": 4.6, "review_count": 592},
]


def test_run_and_cards_are_recorded_in_order(pg):
    js, db, text = pg
    rid = js.log_scout_run(search_id=7, token_hash="tok", replay_mode="all", mode="outreach",
                           niche="nail salon", location="Astoria, NY", user_query="nail salons in Astoria",
                           meaning={"terms": ["nail"], "top": [["nail", 0.61]]},
                           ranking={"type": "stable"}, is_refresh=False, cards=CARDS, business_ids=[11, None])
    assert rid
    with db.get_db_session() as s:
        run = s.execute(text("SELECT search_id, run_kind, user_query, meaning, ranking, result_count "
                             "FROM scout_runs WHERE id = :r"), {"r": rid}).fetchone()
        rows = s.execute(text("SELECT position, business_id, google_place_id, business_name, score, facts "
                              "FROM shown_results WHERE run_id = :r ORDER BY position"), {"r": rid}).fetchall()
    assert run[0] == 7 and run[1] == "capture" and run[2] == "nail salons in Astoria"
    assert run[3]["terms"] == ["nail"] and run[4]["type"] == "stable" and run[5] == 2
    assert [(r[0], r[1], r[2], r[3], r[4]) for r in rows] == [
        (1, 11, "pid-1", "Chris Spa & Nails", 53), (2, None, "pid-2", "Is A Nails Spa", 53)]
    f = rows[0][5]
    assert f["price_level"] == "$$" and f["open_now"] is True and f["seen_before"] == 2
    assert f["query_match"] == {"level": "name", "terms": ["nail"], "searched": True}
    assert f["area_check"]["level"] == "address"


def test_run_kinds(pg):
    js, db, text = pg
    kinds = []
    for mode in ("off", "all", "saved", "ai"):
        rid = js.log_scout_run(search_id=None, token_hash=None, replay_mode=mode, mode="feature",
                               niche="bakery", location="Harlem, NY", user_query="", meaning=None,
                               ranking=None, is_refresh=True, cards=[], business_ids=[])
        with db.get_db_session() as s:
            kinds.append(s.execute(text("SELECT run_kind FROM scout_runs WHERE id = :r"), {"r": rid}).scalar())
    assert kinds == ["live", "capture", "saved", "ai"]


def test_query_embeddings_table_round_trip(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", URL)
    import importlib
    import db
    importlib.reload(db)
    import embedding_service as es
    from sqlalchemy import text
    with db.get_db_session() as s:
        s.execute(text("DROP TABLE IF EXISTS query_embeddings"))
    es._TABLE_READY = False
    # conftest stubs these for offline tests; use the real ones here
    importlib.reload(es)
    es._TABLE_READY = False
    h = es._hash("jollof in the bronx")
    es._db_put([(h, "query", "jollof in the bronx", [0.1, 0.2, 0.3])])
    es._db_put([(h, "query", "jollof in the bronx", [9.0, 9.0, 9.0])])     # never overwritten
    assert es._db_get([h, "missing"]) == {h: [0.1, 0.2, 0.3]}
    es.record_match("jollof in the bronx", {"terms": ["west_african"]})
    with db.get_db_session() as s:
        row = s.execute(text("SELECT matches, seen_count, catalog_version FROM query_embeddings "
                             "WHERE text_hash = :h"), {"h": h}).fetchone()
    assert row[0] == {"terms": ["west_african"]} and row[1] == 2 and row[2] == es.CATALOG_VERSION
    json.dumps(row[0])
