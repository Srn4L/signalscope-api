"""Journey records (scout_runs, returned_results) and query_embeddings against a real Postgres.

Runs only when JOURNEY_TEST_DATABASE_URL points at a disposable database
(never production). Skipped otherwise.
"""
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
        s.execute(text("DROP TABLE IF EXISTS returned_results"))
        s.execute(text("DROP TABLE IF EXISTS scout_runs"))
    yield js, db, text


GOOGLE_CARD = {
    "business_name": "Chris Spa & Nails", "google_place_id": "pid-1", "opportunity_score": 53,
    "score_reasons": [{"key": "demand", "signal": "4.9 stars from 187 reviews", "impact": "positive", "weight": "high"}],
    "source": "google_text", "rating": 4.9, "review_count": 187, "price_level": "$$", "open_now": True,
    "address": "21-34 Broadway, Astoria, NY",
    "query_match": {"level": "name", "terms": ["nail"], "searched": True},
    "area_check": {"level": "address", "reason": "Address is in Astoria"},
    "seen_before": {"count": 2},
}
OSM_CARD = {
    "business_name": "Camila Nails Salon", "opportunity_score": 60, "source": "geoapify",
    "address": "30-01 36th Ave, Astoria, NY", "rating": 0, "review_count": 0,
    "score_reasons": [{"key": "query_match", "signal": "Listed as nail (OpenStreetMap beauty=nails)", "impact": "positive"}],
}


def _log(js, **kw):
    args = dict(client_run_id=None, search_id=7, token_hash="tok", actor="owner", is_test=False,
                replay_mode="off", mode="outreach", niche="nail salon", location="Astoria, NY",
                user_query="nail salons in Astoria", meaning={"terms": ["nail"]},
                ranking={"type": "stable"}, is_refresh=False, cards=[GOOGLE_CARD, OSM_CARD],
                business_ids=[11, None])
    args.update(kw)
    return js.log_scout_run(**args)


def test_run_and_cards_are_recorded_in_order_without_google_content(pg):
    js, db, text = pg
    rid = _log(js, replay_mode="all", actor="owner", is_test=True)
    with db.get_db_session() as s:
        run = s.execute(text("SELECT search_id, run_kind, actor, is_test, user_query, query_hash, meaning, "
                             "ranking, result_count FROM scout_runs WHERE id = :r"), {"r": rid}).fetchone()
        rows = s.execute(text("SELECT position, business_id, google_place_id, source, business_name, score, "
                              "reasons, facts FROM returned_results WHERE run_id = :r ORDER BY position"),
                         {"r": rid}).fetchall()
    assert run[0] == 7 and run[1] == "capture" and run[2] == "owner" and run[3] is True
    assert run[4] == "nail salons in Astoria" and run[5] == js.query_hash("nail salons in Astoria")
    assert run[6]["terms"] == ["nail"] and run[7]["type"] == "stable" and run[8] == 2
    g, o = rows
    # Google card: place id, Yelhao's ids, labels and reason keys only
    assert (g[0], g[1], g[2], g[3], g[4], g[5]) == (1, 11, "pid-1", "google_text", None, 53)
    assert g[6] == [{"key": "demand", "impact": "positive", "weight": "high"}]
    assert g[7]["query_match"] == {"level": "name", "terms": ["nail"], "searched": True}
    assert g[7]["area_check"]["level"] == "address" and g[7]["seen_before"] == 2
    for banned in ("rating", "review_count", "price_level", "open_now", "address"):
        assert banned not in g[7]
    assert "4.9" not in str(g[6]) and "Broadway" not in str(g)
    # OpenStreetMap card keeps name, address and reason text with its source
    assert o[3] == "geoapify" and o[4] == "Camila Nails Salon"
    assert o[7]["address"] == "30-01 36th Ave, Astoria, NY" and "OpenStreetMap" in o[6][0]["signal"]


def test_same_client_run_id_is_recorded_once(pg):
    js, db, text = pg
    first = _log(js, client_run_id="run-abc-12345")
    again = _log(js, client_run_id="run-abc-12345", cards=[OSM_CARD], business_ids=[None])
    other = _log(js, client_run_id="run-xyz-67890")
    assert first == again and other != first
    with db.get_db_session() as s:
        n_runs = s.execute(text("SELECT COUNT(*) FROM scout_runs WHERE client_run_id = 'run-abc-12345'")).scalar()
        n_rows = s.execute(text("SELECT COUNT(*) FROM returned_results WHERE run_id = :r"), {"r": first}).scalar()
    assert n_runs == 1 and n_rows == 2


def test_zero_cards_still_get_a_run(pg):
    js, db, text = pg
    rid = _log(js, cards=[], business_ids=[])
    with db.get_db_session() as s:
        assert s.execute(text("SELECT result_count FROM scout_runs WHERE id = :r"), {"r": rid}).scalar() == 0


def test_run_kinds(pg):
    js, db, text = pg
    kinds = []
    for mode in ("off", "all", "saved", "ai"):
        rid = _log(js, replay_mode=mode, cards=[], business_ids=[])
        with db.get_db_session() as s:
            kinds.append(s.execute(text("SELECT run_kind FROM scout_runs WHERE id = :r"), {"r": rid}).scalar())
    assert kinds == ["live", "capture", "saved", "ai"]


def test_failed_first_write_keeps_tables_and_recovers(pg):
    # Regression: the tables used to be marked ready inside the write's own
    # transaction; a failing write rolled back their creation and the process
    # kept believing they existed.
    js, db, text = pg
    assert _log(js, search_id="not-a-number") is None            # write fails, rolled back
    assert js._TABLES_READY is False
    with db.get_db_session() as s:
        assert s.execute(text("SELECT to_regclass('public.scout_runs')")).scalar() is not None
        assert s.execute(text("SELECT to_regclass('public.returned_results')")).scalar() is not None
    assert _log(js) is not None                                  # next request works


def test_delete_for_token_removes_runs_results_and_cached_queries(pg, monkeypatch):
    js, db, text = pg
    import embedding_service as es
    import importlib
    importlib.reload(es)
    es._TABLE_READY = False
    with db.get_db_session() as s:
        s.execute(text("DROP TABLE IF EXISTS query_embeddings"))
    es._db_put([(es._hash("nail salons in Astoria"), "query", "nail salons in Astoria", [0.1, 0.2])])
    es._db_put([(es._hash(next(iter(es.CATALOG.values()))), "catalog", next(iter(es.CATALOG.values())), [0.3])])
    _log(js, token_hash="tok-del")
    _log(js, token_hash="tok-keep", user_query="bakeries in Harlem")
    counts = js.delete_for_token("tok-del")
    assert counts == {"runs": 1, "returned_results": 2, "embeddings": 1}
    with db.get_db_session() as s:
        assert s.execute(text("SELECT COUNT(*) FROM scout_runs WHERE token_hash = 'tok-del'")).scalar() == 0
        assert s.execute(text("SELECT COUNT(*) FROM scout_runs WHERE token_hash = 'tok-keep'")).scalar() == 1
        assert s.execute(text("SELECT COUNT(*) FROM query_embeddings WHERE kind = 'catalog'")).scalar() == 1


def test_query_embeddings_table_round_trip_and_recovery(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", URL)
    import importlib
    import db
    importlib.reload(db)
    import embedding_service as es
    from sqlalchemy import text
    with db.get_db_session() as s:
        s.execute(text("DROP TABLE IF EXISTS query_embeddings"))
    importlib.reload(es)                  # the real store functions, not the offline stubs
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
    # Table dropped behind the process's back: the next call recreates it
    with db.get_db_session() as s:
        s.execute(text("DROP TABLE query_embeddings"))
    es._db_put([(h, "query", "jollof in the bronx", [0.5])])          # fails, resets the flag
    es._db_put([(h, "query", "jollof in the bronx", [0.5])])          # recreates and writes
    assert es._db_get([h]) == {h: [0.5]}
