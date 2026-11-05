"""Provider-content rules on businesses / Networks saves, and the cleanup scripts.

Runs only when JOURNEY_TEST_DATABASE_URL points at a disposable database
(never production). Skipped otherwise. Tables mirror the live schema.
"""
import os
import re

import pytest

URL = os.environ.get("JOURNEY_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="set JOURNEY_TEST_DATABASE_URL to a disposable Postgres")

HERE = os.path.dirname(__file__)
SQL_DIR = os.path.join(HERE, "..", "sql")

SCHEMA = [
    "DROP TABLE IF EXISTS opportunity_states, opportunities, discovery_events, businesses CASCADE",
    """CREATE TABLE businesses (
        id SERIAL PRIMARY KEY, name TEXT NOT NULL, category TEXT, location TEXT, address TEXT,
        phone TEXT, website TEXT, google_place_id TEXT UNIQUE, google_rating NUMERIC,
        google_review_count INTEGER, instagram_url TEXT, facebook_url TEXT, source TEXT, raw_data JSONB,
        created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())""",
    """CREATE TABLE discovery_events (
        id SERIAL PRIMARY KEY, token_hash TEXT, search_id INTEGER, business_id INTEGER REFERENCES businesses(id),
        opportunity_id INTEGER, mode TEXT, niche TEXT, location TEXT, shown_at TIMESTAMP DEFAULT NOW())""",
    """CREATE TABLE opportunities (
        id SERIAL PRIMARY KEY, business_id INTEGER REFERENCES businesses(id), search_id INTEGER,
        opportunity_type TEXT, matched_offer TEXT, problem_detected TEXT, why_it_fits TEXT, why_now TEXT,
        opportunity_fit_score INTEGER, saturation_score INTEGER, contactability_score INTEGER,
        source_diversity_score INTEGER, signals JSONB, risk_flags JSONB, recommended_offer TEXT,
        best_contact_path TEXT, suggested_message TEXT, suggested_follow_up TEXT,
        created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW(), service_angle TEXT,
        user_role TEXT, contact_target TEXT, contact_confidence TEXT, contact_reason TEXT)""",
    """CREATE TABLE opportunity_states (
        id SERIAL PRIMARY KEY, opportunity_id INTEGER REFERENCES opportunities(id), status TEXT,
        contact_method TEXT, last_action TEXT, next_action TEXT, next_action_date TIMESTAMP, notes TEXT,
        message_used TEXT, response_status TEXT, outcome TEXT, created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(), token_hash TEXT, contact_target TEXT, service_angle TEXT)""",
]


@pytest.fixture
def pg(monkeypatch):
    before = os.environ.get("DATABASE_URL")
    monkeypatch.setenv("DATABASE_URL", URL)
    import importlib
    import db
    importlib.reload(db)
    import database_service as ds
    importlib.reload(ds)
    from sqlalchemy import text
    with db.get_db_session() as s:
        for stmt in SCHEMA:
            s.execute(text(stmt))
    yield ds, db, text
    if before is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = before
    importlib.reload(db)
    importlib.reload(ds)


def _legacy_rows(db, text):
    """Rows as the old code wrote them (Google content stored)."""
    with db.get_db_session() as s:
        ids = {}
        for key, sql, params in [
            ("g_pid", "INSERT INTO businesses (name, category, location, address, phone, website, google_place_id, "
                      "google_rating, google_review_count, source, raw_data) VALUES ('Chris Spa & Nails', 'nail salon', "
                      "'Astoria, NY', '21-34 Broadway', '(718) 555-0101', 'https://chris.example', 'pid-1', 4.9, 187, "
                      "'google_text', CAST(:raw AS JSONB)) RETURNING id",
             {"raw": '{"platform": "google_places", "score_reasons": [{"signal": "4.9 stars"}], "score_confidence": "high"}'}),
            ("g_nopid", "INSERT INTO businesses (name, category, location, address, google_rating, google_review_count, "
                        "source, raw_data) VALUES ('Mama G. African Kitchen', 'restaurant', 'Bronx, NY', '1322A E Gun Hill Rd', "
                        "4.2, 466, 'google_text', CAST(:raw AS JSONB)) RETURNING id",
             {"raw": '{"platform": "google_places", "score_reasons": [{"signal": "466 reviews"}]}'}),
            ("geo", "INSERT INTO businesses (name, category, location, address, google_rating, google_review_count, source, "
                    "raw_data) VALUES ('Camila Nails Salon', 'nail salon', 'Astoria, NY', '30-01 36th Ave', 0, 0, 'geoapify', "
                    "CAST(:raw AS JSONB)) RETURNING id", {"raw": '{"platform": "geoapify"}'}),
            ("booking", "INSERT INTO businesses (name, location, website, google_rating, google_review_count, source, raw_data) "
                        "VALUES ('Glow Nails', 'Astoria, NY', 'https://fresha.com/glow', 0, 0, 'booking_platform', "
                        "CAST(:raw AS JSONB)) RETURNING id", {"raw": '{"platform": "fresha"}'}),
        ]:
            ids[key] = s.execute(text(sql), params).scalar()
        s.execute(text("INSERT INTO discovery_events (business_id, token_hash) VALUES (:a, 't'), (:b, 't')"),
                  {"a": ids["g_pid"], "b": ids["g_nopid"]})
        opp = s.execute(text("INSERT INTO opportunities (business_id, problem_detected, why_it_fits, signals, "
                             "opportunity_fit_score, best_contact_path, contact_reason) VALUES (:b, '466 reviews', "
                             "'4.2 stars', CAST('{\"score_reasons\": []}' AS JSONB), 61, 'phone', 'Call (718)') RETURNING id"),
                        {"b": ids["g_nopid"]}).scalar()
        s.execute(text("INSERT INTO opportunity_states (opportunity_id, status, token_hash) VALUES (:o, 'saved', 't')"),
                  {"o": opp})
        ids["opp"] = opp
    return ids


def _run_sql(db, name):
    sql = open(os.path.join(SQL_DIR, name)).read()
    from sqlalchemy import text
    with db.get_db_session() as s:
        if "apply" in name:
            body = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
            body = body.replace("BEGIN;", "").replace("COMMIT;", "")
            for stmt in [x.strip() for x in re.split(r";\s*\n", body) if x.strip()]:
                s.execute(text(stmt))
            return None
        return [dict(r._mapping) for r in s.execute(text(sql)).fetchall()]


def test_dry_run_lists_fields_provenance_and_kept_links(pg):
    ds, db, text = pg
    ids = _legacy_rows(db, text)
    rows = {r["id"]: r for r in _run_sql(db, "provider_cleanup_dry_run.sql")}
    g, n, geo, bk = rows[ids["g_pid"]], rows[ids["g_nopid"]], rows[ids["geo"]], rows[ids["booking"]]
    assert g["google_touched"] and "place_id set" in g["evidence"] and g["identity_after"] == "place_id"
    assert g["fields_to_clear"] == ("name, address, phone, website, google_rating, google_review_count, "
                                    "raw_data.score_reasons, raw_data.score_confidence")
    assert n["identity_after"] == "identity_key" and n["discovery_events_kept"] == 1 and n["opportunities_kept"] == 1
    import source_rules
    assert n["identity_key_to_add"] == source_rules.identity_key("Mama G. African Kitchen", "Bronx, NY")
    assert not geo["google_touched"] and geo["fields_to_clear"] is None
    assert not bk["google_touched"] and bk["identity_after"] == "name+location (unchanged)"


def test_apply_clears_google_fields_keeps_ids_links_and_independent_rows(pg):
    ds, db, text = pg
    ids = _legacy_rows(db, text)
    _run_sql(db, "provider_cleanup_apply.sql")
    with db.get_db_session() as s:
        g = dict(s.execute(text("SELECT * FROM businesses WHERE id = :i"), {"i": ids["g_pid"]}).mappings().one())
        n = dict(s.execute(text("SELECT * FROM businesses WHERE id = :i"), {"i": ids["g_nopid"]}).mappings().one())
        geo = dict(s.execute(text("SELECT * FROM businesses WHERE id = :i"), {"i": ids["geo"]}).mappings().one())
        opp = dict(s.execute(text("SELECT * FROM opportunities WHERE id = :i"), {"i": ids["opp"]}).mappings().one())
        de = s.execute(text("SELECT COUNT(*) FROM discovery_events")).scalar()
        st = s.execute(text("SELECT COUNT(*) FROM opportunity_states WHERE opportunity_id = :o"), {"o": ids["opp"]}).scalar()
    for row in (g, n):
        assert row["name"] == "" and row["address"] is None and row["phone"] is None and row["website"] is None
        assert row["google_rating"] is None and row["google_review_count"] is None
        assert "score_reasons" not in row["raw_data"] and "score_confidence" not in row["raw_data"]
        assert row["category"] and row["location"]
    assert g["google_place_id"] == "pid-1"
    import source_rules
    assert n["raw_data"]["identity_key"] == source_rules.identity_key("Mama G. African Kitchen", "Bronx, NY")
    assert geo["name"] == "Camila Nails Salon" and geo["address"] == "30-01 36th Ave"     # independent: kept
    assert opp["business_id"] == ids["g_nopid"] and opp["why_it_fits"] is None and opp["problem_detected"] is None
    assert opp["opportunity_fit_score"] is None and opp["contact_reason"] is None
    assert de == 2 and st == 1                                                            # relationships intact


def _scout_write(ds, **over):
    data = {"name": "Mama G. African Kitchen", "category": "restaurant", "location": "Bronx, NY",
            "address": "1322A E Gun Hill Rd", "phone": "(718) 555-0199", "website": "https://mamag.example",
            "google_place_id": None, "google_rating": 4.2, "google_review_count": 466, "source": "google_text",
            "raw_data": {"platform": "google_places", "score_reasons": [{"signal": "466 reviews"}],
                         "score_confidence": "high", "website_checked": True}}
    data.update(over)
    return ds.upsert_business(data)


def test_later_scout_and_networks_saves_cannot_repopulate(pg):
    ds, db, text = pg
    ids = _legacy_rows(db, text)
    _run_sql(db, "provider_cleanup_apply.sql")
    # Scout write without a place id finds the same row by identity key
    assert _scout_write(ds) == ids["g_nopid"]
    # Scout write with a place id updates the place-id row
    assert _scout_write(ds, name="Chris Spa & Nails", location="Astoria, NY", google_place_id="pid-1") == ids["g_pid"]
    # Networks save of a full Google card
    out = ds.save_scout_result_to_network({
        "business_name": "Mama G. African Kitchen", "location": "Bronx, NY", "category": "restaurant",
        "address": "1322A E Gun Hill Rd", "phone": "(718) 555-0199", "website": "https://mamag.example",
        "rating": 4.2, "review_count": 466, "source": "google_text", "platform": "google_places",
        "mode": "outreach", "opportunity_score": 61, "why_it_fits": "4.2 stars from 466 reviews",
        "score_reasons": [{"key": "demand", "signal": "466 reviews", "impact": "positive"}]}, "t2")
    assert out["ok"] and out["business_id"] == ids["g_nopid"]
    with db.get_db_session() as s:
        for bid in (ids["g_nopid"], ids["g_pid"]):
            row = dict(s.execute(text("SELECT * FROM businesses WHERE id = :i"), {"i": bid}).mappings().one())
            assert row["name"] == "" and row["address"] is None and row["phone"] is None and row["website"] is None
            assert row["google_rating"] is None and row["google_review_count"] is None
            assert "score_reasons" not in (row["raw_data"] or {})
        new_opp = dict(s.execute(text("SELECT * FROM opportunities WHERE id = :o"),
                                 {"o": out["opportunity_id"]}).mappings().one())
    assert new_opp["why_it_fits"] in (None, "") and new_opp["opportunity_fit_score"] is None
    assert "466" not in str(new_opp["signals"]) and "466" not in str(new_opp["problem_detected"])


def test_new_google_business_is_stored_without_content(pg):
    ds, db, text = pg
    bid = _scout_write(ds, name="Brand New Spot", google_place_id="pid-new")
    nid = _scout_write(ds, name="No Pid Spot", location="Harlem, NY")
    with db.get_db_session() as s:
        for i in (bid, nid):
            row = dict(s.execute(text("SELECT * FROM businesses WHERE id = :i"), {"i": i}).mappings().one())
            assert row["name"] == "" and row["address"] is None and row["google_rating"] is None
    assert _scout_write(ds, name="No Pid Spot", location="Harlem, NY") == nid       # same business again
