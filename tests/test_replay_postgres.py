"""Replay cache write/read against a real Postgres.

Runs only when REPLAY_TEST_DATABASE_URL points at a disposable database
(never production). Skipped otherwise.
"""
import os
import time

import pytest

URL = os.environ.get("REPLAY_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="set REPLAY_TEST_DATABASE_URL to a disposable Postgres")


@pytest.fixture(scope="module")
def pg():
    before = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = URL
    import importlib
    import db
    importlib.reload(db)
    import replay_service as rs
    rs._TABLE_READY = False
    from sqlalchemy import text
    with db.get_db_session() as s:
        s.execute(text("DROP TABLE IF EXISTS dev_replay_cache"))
    yield rs, db, text
    # Later test files must not count API calls or read replay data in this database
    if before is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = before
    importlib.reload(db)
    rs._TABLE_READY = False


def test_table_is_created_on_first_use_and_survives_a_restart(pg):
    rs, db, text = pg
    rs._MEM.clear()
    key = rs.make_key("http", "GET", "https://example.com/menu", None, None, None)
    payload = {"ok": True, "status": 200, "headers": {"Content-Type": "text/html"},
               "encoding": "utf-8", "body_b64": "PGgxPk1lbnU8L2gxPg=="}
    rs.put(key, payload)
    rs._MEM.clear()                                      # like a new process after a deploy
    rs._TABLE_READY = False
    assert rs.get(key) == payload
    with db.get_db_session() as s:
        cols = s.execute(text("""SELECT column_name, data_type FROM information_schema.columns
                                 WHERE table_name = 'dev_replay_cache' ORDER BY ordinal_position""")).fetchall()
        kind = s.execute(text("SELECT kind FROM dev_replay_cache WHERE cache_key = :k"), {"k": key}).scalar()
    assert [c[0] for c in cols] == ["cache_key", "kind", "payload", "created_at"]
    assert dict(cols)["payload"] == "jsonb" and kind == "http"


def test_upsert_replaces_and_ttl_expires(pg):
    rs, db, text = pg
    key = rs.make_key("openai", {"model": "gpt-4o", "messages": [{"role": "user", "content": "x"}]})
    rs.put(key, {"v": 1})
    rs.put(key, {"v": 2})
    rs._MEM.clear()
    assert rs.get(key) == {"v": 2}
    with db.get_db_session() as s:
        assert s.execute(text("SELECT count(*) FROM dev_replay_cache WHERE cache_key = :k"), {"k": key}).scalar() == 1
        s.execute(text("UPDATE dev_replay_cache SET created_at = NOW() - INTERVAL '30 days' WHERE cache_key = :k"), {"k": key})
    rs._MEM.clear()
    assert rs.get(key) is None                           # older than REPLAY_TTL_DAYS (14)


def test_secrets_never_reach_the_table(pg):
    rs, db, text = pg
    key = rs.make_key("http", "GET", rs._clean_url("https://serpapi.com/search?q=cafe&api_key=SECRET123"),
                      {"q": "cafe", "api_key": "SECRET123"}, None, None)
    rs.put(key, {"ok": True, "status": 200, "body_b64": ""})
    with db.get_db_session() as s:
        dump = s.execute(text("SELECT string_agg(cache_key || payload::text, '') FROM dev_replay_cache")).scalar()
    assert "SECRET123" not in dump


def test_clear_empties_the_table(pg):
    rs, db, text = pg
    rs.put(rs.make_key("ddg", "q", 6), {"results": []})
    assert rs.clear() >= 1
    rs._MEM.clear()
    with db.get_db_session() as s:
        assert s.execute(text("SELECT count(*) FROM dev_replay_cache")).scalar() == 0
