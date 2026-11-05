"""Journey records (scout_runs, returned_results) and query_embeddings against a real Postgres.

Runs only when JOURNEY_TEST_DATABASE_URL points at a disposable database
(never production). Skipped otherwise.
"""
import os
import time

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
        s.execute(text("DROP TABLE IF EXISTS journey_deleted_tokens"))
    yield js, db, text


GOOGLE_CARD = {
    "business_name": "Chris Spa & Nails", "google_place_id": "pid-1", "opportunity_score": 53,
    "score_reasons": [{"key": "demand", "signal": "4.9 stars from 187 reviews", "impact": "positive"}],
    "source": "google_text", "rating": 4.9, "review_count": 187, "price_level": "$$", "open_now": True,
    "address": "21-34 Broadway, Astoria, NY", "lat": 40.7645, "lng": -73.93,
    "query_match": {"level": "name", "terms": ["nail"]},
    "area_check": {"level": "far", "km": 4.2}, "opportunity_lane": "hidden_gap",
    "seen_before": {"count": 2}, "website_checked": True,
}
OSM_CARD = {"business_name": "Camila Nails Salon", "opportunity_score": 60, "source": "geoapify",
            "address": "30-01 36th Ave, Astoria, NY", "google_place_id": "should-not-be-kept"}
BOOKING_CARD = {"business_name": "Glow Nails", "source": "booking_platform", "website": "https://booksy.com/x"}
UNKNOWN_CARD = {"business_name": "Mystery", "source": "something_new", "google_place_id": "pid-x",
                "address": "1 Main St"}


def _log(js, **kw):
    args = dict(client_run_id=None, search_id=7, token_hash="tok", actor="owner", is_test=False,
                replay_mode="off", mode="outreach", niche="nail salon", location="Astoria, NY",
                user_query="nail salons in Astoria", meaning={"terms": ["nail"]},
                ranking={"type": "stable"}, is_refresh=False,
                cards=[GOOGLE_CARD, OSM_CARD, BOOKING_CARD, UNKNOWN_CARD], business_ids=[11, None, None, 14])
    args.update(kw)
    return js.log_scout_run(**args)


def _rows(db, text, rid):
    with db.get_db_session() as s:
        return s.execute(text("SELECT position, business_id, source, google_place_id, content, facts "
                              "FROM returned_results WHERE run_id = :r ORDER BY position"), {"r": rid}).fetchall()


def test_cards_in_order_storing_only_what_source_rules_allow(pg):
    js, db, text = pg
    rid = _log(js, replay_mode="all", is_test=True)
    with db.get_db_session() as s:
        run = s.execute(text("SELECT run_kind, actor, is_test, result_count FROM scout_runs WHERE id = :r"),
                        {"r": rid}).fetchone()
    assert tuple(run) == ("capture", "owner", True, 4)
    g, o, b, u = _rows(db, text, rid)
    assert tuple(g) == (1, 11, "google_text", "pid-1", None, {"seen_before": 2, "website_checked": True})
    assert tuple(o) == (2, None, "geoapify", None, None, None)          # not approved: nothing kept
    assert tuple(b) == (3, None, "booking_platform", None, None, None)
    assert tuple(u) == (4, 14, "something_new", None, None, None)       # unknown source: nothing
    with db.get_db_session() as s:
        cols = [r[0] for r in s.execute(text("SELECT column_name FROM information_schema.columns "
                                             "WHERE table_name = 'returned_results'")).fetchall()]
    for derived in ("score", "reasons", "business_name"):
        assert derived not in cols


def test_retried_click_makes_separate_runs_grouped_by_code_and_click(pg):
    js, db, text = pg
    a1 = _log(js, client_run_id="click-0001-aaaa", cards=[GOOGLE_CARD], business_ids=[11])
    a2 = _log(js, client_run_id="click-0001-aaaa", cards=[], business_ids=[])
    other_user = _log(js, client_run_id="click-0001-aaaa", token_hash="tok-2", cards=[OSM_CARD], business_ids=[None])
    assert len({a1, a2, other_user}) == 3
    assert len(_rows(db, text, a1)) == 1 and len(_rows(db, text, a2)) == 0
    with db.get_db_session() as s:
        groups = s.execute(text("SELECT token_hash, COUNT(*) FROM scout_runs WHERE client_run_id = 'click-0001-aaaa' "
                                "GROUP BY token_hash ORDER BY token_hash")).fetchall()
    assert [tuple(g) for g in groups] == [("tok", 2), ("tok-2", 1)]


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
    js, db, text = pg
    assert _log(js, search_id="not-a-number") is None            # write fails, rolled back
    assert js._TABLES_READY is False
    with db.get_db_session() as s:
        assert s.execute(text("SELECT to_regclass('public.scout_runs')")).scalar() is not None
        assert s.execute(text("SELECT to_regclass('public.returned_results')")).scalar() is not None
    assert _log(js) is not None


# ── deletion ────────────────────────────────────────────────────────────────

@pytest.fixture
def pg_es(pg):
    js, db, text = pg
    import importlib
    import embedding_service as es
    importlib.reload(es)                  # real store functions, not the offline stubs
    es._TABLE_READY = False
    with db.get_db_session() as s:
        s.execute(text("DROP TABLE IF EXISTS query_embeddings"))
        s.execute(text("DROP TABLE IF EXISTS journey_deleted_tokens"))
    js._TABLES_READY = False
    yield js, es, db, text


LONG_QUERY = "quiet affordable date night spot " * 30          # about 1,000 characters


def test_delete_removes_runs_results_and_exact_embeddings_including_long_queries(pg_es):
    js, es, db, text = pg_es
    catalog_text = next(iter(es.CATALOG.values()))
    es._db_put([(es.cache_key(LONG_QUERY), "query", LONG_QUERY, [0.1]),
                (es.cache_key("bakeries in Harlem"), "query", "bakeries in Harlem", [0.2]),
                (es.cache_key(catalog_text), "catalog", catalog_text, [0.3])])
    _log(js, token_hash="tok-del", user_query=LONG_QUERY)
    _log(js, token_hash="tok-keep", user_query="bakeries in Harlem")
    counts = js.delete_for_token("tok-del")
    assert counts == {"ok": True, "runs": 1, "returned_results": 4, "embeddings": 1}
    es._MEM.clear()
    assert es._db_get([es.cache_key(LONG_QUERY)]) == {}
    assert es._db_get([es.cache_key("bakeries in Harlem")]) != {}
    assert es._db_get([es.cache_key(catalog_text)]) != {}
    with db.get_db_session() as s:
        row = s.execute(text("SELECT text, embedding, deleted_at FROM query_embeddings WHERE text_hash = :h"),
                        {"h": es.cache_key(LONG_QUERY)}).fetchone()
        assert row[0] is None and row[1] is None and row[2] is not None
        assert s.execute(text("SELECT COUNT(*) FROM scout_runs WHERE token_hash = 'tok-del'")).scalar() == 0


def test_failed_cleanup_deletes_nothing_and_a_retry_finishes(pg_es, monkeypatch):
    js, es, db, text = pg_es
    es._db_put([(es.cache_key(LONG_QUERY), "query", LONG_QUERY, [0.1])])
    _log(js, token_hash="tok-del", user_query=LONG_QUERY)
    real = es.tombstone_keys

    def boom(session, keys):
        real(session, keys)
        raise RuntimeError("embedding cleanup failed")
    monkeypatch.setattr(es, "tombstone_keys", boom)
    assert js.delete_for_token("tok-del")["ok"] is False
    with db.get_db_session() as s:                         # everything rolled back
        assert s.execute(text("SELECT COUNT(*) FROM scout_runs WHERE token_hash = 'tok-del'")).scalar() == 1
        assert s.execute(text("SELECT COUNT(*) FROM journey_deleted_tokens")).scalar() == 0
    es._MEM.clear()
    assert es._db_get([es.cache_key(LONG_QUERY)]) != {}
    monkeypatch.setattr(es, "tombstone_keys", real)
    assert js.delete_for_token("tok-del") == {"ok": True, "runs": 1, "returned_results": 4, "embeddings": 1}


def test_in_flight_writes_cannot_restore_deleted_data(pg_es):
    js, es, db, text = pg_es
    key = es.cache_key(LONG_QUERY)
    es._db_put([(key, "query", LONG_QUERY, [0.1])])
    started = time.time() - 5                              # request began before the deletion
    _log(js, token_hash="tok-del", user_query=LONG_QUERY)
    js.delete_for_token("tok-del")
    # The in-flight request finishes after the deletion
    assert _log(js, token_hash="tok-del", user_query=LONG_QUERY, started_at=started) is None
    es._db_put([(key, "query", LONG_QUERY, [9.9])])        # tries to store the vector again
    es._MEM.clear()
    assert es._db_get([key]) == {}
    assert key in es._recently_deleted([key])
    # A new request for the same code after the deletion is a new search: recorded
    assert _log(js, token_hash="tok-del", user_query="x", started_at=time.time() + 1) is not None
    # After the tombstone window, the text can be cached again
    with db.get_db_session() as s:
        s.execute(text("UPDATE query_embeddings SET deleted_at = NOW() - INTERVAL '11 minutes' "
                       "WHERE text_hash = :h"), {"h": key})
    es._db_put([(key, "query", LONG_QUERY, [0.7])])
    assert es._db_get([key]) == {key: [0.7]}


def test_other_workers_memory_copies_expire(pg_es, monkeypatch):
    js, es, db, text = pg_es
    key = es.cache_key("waakye in Harlem")
    # Another worker cached the vector before the deletion
    es._MEM[key] = ([0.4], time.time() - es.MEM_TTL_SECONDS - 1)
    calls = []
    monkeypatch.setattr(es, "_live_embed", lambda texts, budget_ok=None: calls.append(texts) or None)
    got = es.embed_many([("query", "waakye in Harlem")])
    assert got == {} and key not in es._MEM               # expired copy not served
    es._MEM[key] = ([0.4], time.time())
    assert es.embed_many([("query", "waakye in Harlem")]) == {"waakye in Harlem": [0.4]}


def test_query_embeddings_round_trip_and_recovery(pg_es):
    js, es, db, text = pg_es
    h = es.cache_key("jollof in the bronx")
    es._db_put([(h, "query", "jollof in the bronx", [0.1, 0.2, 0.3])])
    es._db_put([(h, "query", "jollof in the bronx", [9.0, 9.0, 9.0])])     # active rows never overwritten
    assert es._db_get([h, "missing"]) == {h: [0.1, 0.2, 0.3]}
    es.record_match("jollof in the bronx", {"terms": ["west_african"]})
    with db.get_db_session() as s:
        row = s.execute(text("SELECT matches, seen_count FROM query_embeddings WHERE text_hash = :h"),
                        {"h": h}).fetchone()
        s.execute(text("DROP TABLE query_embeddings"))
    assert row[0] == {"terms": ["west_african"]} and row[1] == 2
    es._db_put([(h, "query", "jollof in the bronx", [0.5])])          # fails, resets the flag
    es._db_put([(h, "query", "jollof in the bronx", [0.5])])          # recreates and writes
    assert es._db_get([h]) == {h: [0.5]}


# ── concurrency: logging and deletion for one access code are serialized ──

import threading  # noqa: E402


def _count_runs(db, text, token):
    with db.get_db_session() as s:
        return s.execute(text("SELECT COUNT(*) FROM scout_runs WHERE token_hash = :t"), {"t": token}).scalar()


def test_run_that_waits_on_an_uncommitted_deletion_is_not_left_behind(pg_es, monkeypatch):
    js, es, db, text = pg_es
    _log(js, token_hash="tok-race", user_query="first search")
    started = time.time() - 5                               # this request began before the deletion
    in_delete, release = threading.Event(), threading.Event()
    real = es.tombstone_keys

    def slow(session, keys):                                # deletion holds its transaction open
        n = real(session, keys)
        in_delete.set()
        release.wait(5)
        return n
    monkeypatch.setattr(es, "tombstone_keys", slow)
    out = {}
    d = threading.Thread(target=lambda: out.setdefault("del", js.delete_for_token("tok-race")))
    d.start()
    assert in_delete.wait(5)
    w = threading.Thread(target=lambda: out.setdefault("run", _log(js, token_hash="tok-race",
                                                                    user_query="second", started_at=started)))
    w.start()
    w.join(0.5)
    assert w.is_alive()                                      # blocked on the per-code lock
    release.set()
    d.join(5)
    w.join(5)
    assert out["del"]["ok"] is True and out["run"] is None
    assert _count_runs(db, text, "tok-race") == 0


def test_deletion_that_waits_on_an_uncommitted_run_removes_it(pg_es, monkeypatch):
    js, es, db, text = pg_es
    in_log, release = threading.Event(), threading.Event()
    real = js.stored_card

    def slow(card, bid):                                    # the run holds its transaction open
        in_log.set()
        release.wait(5)
        return real(card, bid)
    monkeypatch.setattr(js, "stored_card", slow)
    out = {}
    w = threading.Thread(target=lambda: out.setdefault("run", _log(js, token_hash="tok-race2",
                                                                    cards=[GOOGLE_CARD], business_ids=[1])))
    w.start()
    assert in_log.wait(5)
    d = threading.Thread(target=lambda: out.setdefault("del", js.delete_for_token("tok-race2")))
    d.start()
    d.join(0.5)
    assert d.is_alive()                                      # waits for the run to commit
    release.set()
    w.join(5)
    d.join(5)
    assert out["run"] is not None and out["del"] == {"ok": True, "runs": 1, "returned_results": 1,
                                                     "embeddings": 1}
    assert _count_runs(db, text, "tok-race2") == 0


def test_deletion_during_a_first_ever_search_catches_its_embedding(pg_es):
    js, es, db, text = pg_es
    q = "where can I find suya near me tonight"
    key = es.cache_key(q)
    started = time.time() - 2
    assert js.register_query_key("tok-new", q, started_at=started) is True   # search begins, no run yet
    counts = js.delete_for_token("tok-new")
    assert counts == {"ok": True, "runs": 0, "returned_results": 0, "embeddings": 1}
    # The search finishes after the deletion: its embedding can't be kept
    es._db_put([(key, "query", q, [0.3])])
    es._MEM.clear()
    assert es._db_get([key]) == {} and key in es._recently_deleted([key])
    with db.get_db_session() as s:
        assert s.execute(text("SELECT COUNT(*) FROM journey_embed_keys WHERE token_hash = 'tok-new'")).scalar() == 0
        assert s.execute(text("SELECT text FROM query_embeddings WHERE text_hash = :h"), {"h": key}).scalar() is None
    # ...and its run isn't recorded either; a search started later is blocked from nothing
    assert _log(js, token_hash="tok-new", user_query=q, started_at=started) is None
    assert js.register_query_key("tok-new", q, started_at=started) is False


def test_upgrade_from_the_pr38_table_adds_deleted_at(pg_es):
    js, es, db, text = pg_es
    with db.get_db_session() as s:                          # the table as #38 created it
        s.execute(text("""
            CREATE TABLE query_embeddings (
                text_hash TEXT PRIMARY KEY, kind TEXT, model TEXT, text TEXT, embedding JSONB,
                matches JSONB, catalog_version TEXT, seen_count INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT NOW(), last_seen_at TIMESTAMP DEFAULT NOW())
        """))
        s.execute(text("INSERT INTO query_embeddings (text_hash, kind, model, text, embedding) "
                       "VALUES ('k1', 'query', 'm', 'old text', CAST('[0.1]' AS JSONB))"))
    es._TABLE_READY = False
    es._ensure_table()
    es._TABLE_READY = False
    es._ensure_table()                                      # idempotent
    with db.get_db_session() as s:
        cols = [r[0] for r in s.execute(text("SELECT column_name FROM information_schema.columns "
                                             "WHERE table_name = 'query_embeddings'")).fetchall()]
    assert "deleted_at" in cols
    assert es._db_get(["k1"]) == {"k1": [0.1]}              # old rows still readable
