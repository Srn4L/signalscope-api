"""
Focused tests for database_service.init_db().

Integration tests (1-3, 5) need a DISPOSABLE local PostgreSQL server:

    YELHAO_TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/postgres \
        python -m pytest tests/test_init_db.py -v

Each integration test creates its own throwaway database on that server and
drops it afterwards. The URL must point at localhost; anything else is refused.
Without the variable, integration tests are skipped and only the unit test runs.
DATABASE_URL is never read or modified by these tests.
"""

import os
import re
import uuid
from pathlib import Path
from urllib.parse import urlparse

import pytest
from sqlalchemy import create_engine, inspect, text

import db
import database_service

REPO_ROOT = Path(database_service.__file__).resolve().parent
SCHEMA_SQL = (REPO_ROOT / "schema.sql").read_text(encoding="utf-8")
EXPECTED_TABLES = set(re.findall(r"^CREATE TABLE IF NOT EXISTS\s+(\w+)", SCHEMA_SQL, re.M))
EXPECTED_INDEXES = set(re.findall(r"^CREATE INDEX IF NOT EXISTS\s+(\w+)", SCHEMA_SQL, re.M))
EXPECTED_ADDED_COLUMNS = set(
    re.findall(r"^ALTER TABLE\s+(\w+)\s+ADD COLUMN IF NOT EXISTS\s+(\w+)", SCHEMA_SQL, re.M)
)

TEST_URL = os.environ.get("YELHAO_TEST_DATABASE_URL", "")
needs_pg = pytest.mark.skipif(not TEST_URL, reason="YELHAO_TEST_DATABASE_URL not set")


def _assert_local(url):
    host = urlparse(url).hostname
    if host not in (None, "", "localhost", "127.0.0.1", "::1"):
        pytest.fail(f"Refusing to run against non-local database host {host!r}")


@pytest.fixture
def fresh_engine(monkeypatch):
    """A brand-new, empty database, dropped after the test."""
    _assert_local(TEST_URL)
    name = f"yelhao_init_test_{uuid.uuid4().hex[:10]}"
    admin = create_engine(TEST_URL, isolation_level="AUTOCOMMIT")
    with admin.connect() as c:
        c.execute(text(f'CREATE DATABASE "{name}"'))
    engine = create_engine(urlparse(TEST_URL)._replace(path=f"/{name}").geturl())
    monkeypatch.setattr(db, "_get_engine", lambda: engine)
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.connect() as c:
            c.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
        admin.dispose()


def _use_schema(monkeypatch, tmp_path, content):
    """Point init_db() at a schema.sql in tmp_path instead of the repo copy."""
    (tmp_path / "schema.sql").write_text(content, encoding="utf-8")
    monkeypatch.setattr(database_service, "__file__", str(tmp_path / "database_service.py"))


# 1 - fresh database receives every table, index and added column
@needs_pg
def test_fresh_db_gets_full_schema(fresh_engine):
    assert len(EXPECTED_TABLES) == 9  # guards the regex itself

    assert database_service.init_db() == (True, None)

    insp = inspect(fresh_engine)
    assert EXPECTED_TABLES <= set(insp.get_table_names())
    indexes = {ix["name"] for t in EXPECTED_TABLES for ix in insp.get_indexes(t)}
    assert EXPECTED_INDEXES <= indexes
    for table, column in EXPECTED_ADDED_COLUMNS:
        assert column in {c["name"] for c in insp.get_columns(table)}, (table, column)


# 2 - second run succeeds and keeps existing data
@needs_pg
def test_rerun_is_idempotent_and_preserves_data(fresh_engine):
    assert database_service.init_db() == (True, None)
    with fresh_engine.begin() as c:
        c.execute(text("INSERT INTO businesses (name, google_place_id) VALUES ('Keep Me', 'gp-keep')"))

    assert database_service.init_db() == (True, None)

    with fresh_engine.connect() as c:
        rows = c.execute(text("SELECT name FROM businesses WHERE google_place_id = 'gp-keep'")).all()
    assert rows == [("Keep Me",)]


# 3 - an invalid statement fails the whole run and rolls back earlier DDL
@needs_pg
def test_invalid_statement_rolls_back(fresh_engine, monkeypatch, tmp_path):
    _use_schema(monkeypatch, tmp_path, SCHEMA_SQL + "\nCREATE TABLE deliberately broken (;\n")

    ok, err = database_service.init_db()

    assert ok is False and err
    assert set(inspect(fresh_engine).get_table_names()) == set()


# 4 - missing schema file keeps the existing failure result (unit; no database)
def test_missing_schema_file(monkeypatch, tmp_path):
    def _no_engine():
        raise AssertionError("engine must not be touched when schema.sql is missing")

    monkeypatch.setattr(db, "_get_engine", _no_engine)
    monkeypatch.setattr(database_service, "__file__", str(tmp_path / "database_service.py"))

    assert database_service.init_db() == (
        False,
        f"schema.sql not found at {tmp_path / 'schema.sql'}",
    )


# 5 - semicolons/colons/percent signs inside comments and strings are left to PostgreSQL
@needs_pg
def test_server_side_parsing_of_comments_and_strings(fresh_engine, monkeypatch, tmp_path):
    _use_schema(monkeypatch, tmp_path, (
        "-- leading comment; with a semicolon :param and 100%\n"
        "CREATE TABLE IF NOT EXISTS t (note TEXT DEFAULT 'a;b :x 5%');\n"
        "/* block; comment */ INSERT INTO t DEFAULT VALUES;\n"
    ))

    assert database_service.init_db() == (True, None)
    with fresh_engine.connect() as c:
        assert c.execute(text("SELECT note FROM t")).scalar_one() == "a;b :x 5%"
